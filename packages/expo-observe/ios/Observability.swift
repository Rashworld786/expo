import EASClient
import ExpoAppMetrics
import ExpoModulesCore

@AppMetricsActor
internal struct ObservabilityManager {
  private static let easClientId = EASClientID.uuid().uuidString
  private static var metricsEndpointUrl: URL? = nil
  private static var logsEndpointUrl: URL? = nil
  private static var projectId: String? = nil

  /// Default deferred-dispatch delay when no explicit value is configured (30 minutes).
  private static let defaultDeferredDispatchDelaySeconds: TimeInterval = 1800

  /// Polling cadence (in seconds) for checking the metrics DB for new rows. `nil` keeps the loop
  /// idle. Updated via `setPollingIntervalSeconds(_:)` from
  /// `Observe.configure({ scheduledDispatchPollingInterval })`. The loop reads this on each wake,
  /// so a `configure(...)` change takes effect on the next pass.
  private static var pollingIntervalSeconds: TimeInterval?

  /// Delay (in seconds) between detecting new rows and the deferred dispatch firing. `nil` falls
  /// back to `defaultDeferredDispatchDelaySeconds`. Updated via
  /// `setDeferredDispatchDelaySeconds(_:)` from `Observe.configure({ scheduledDispatchDelay })`.
  private static var deferredDispatchDelaySeconds: TimeInterval?

  private static var pollingLoopStarted = false

  /// The currently-armed deferred dispatch, if any. Set when polling detects new rows; replaced
  /// (the old task is cancelled) when polling fires again with new rows; cancelled when any other
  /// `dispatch()` runs first.
  private static var deferredDispatchTask: Task<Void, Never>?

  /// When the currently-armed deferred dispatch is scheduled to fire (wall clock). `nil` when no
  /// dispatch is armed. Re-arms read this to compute the next fire time as `existing + delay/2`.
  private static var deferredDispatchFireTime: Date?

  /// When the *first* arm in the current deferral window happened. Cleared after dispatch (or
  /// cancellation). Combined with `deferredDispatchDelaySeconds` to enforce the hard cap on how
  /// far re-arms can push out — at most `2 × delay` past this point.
  private static var deferredDispatchOriginalArmTime: Date?

  /// Sets the polling interval for the metrics-DB polling loop. Passing a positive number of
  /// seconds starts the loop on its first call. Subsequent calls update the interval in place; the
  /// next wake uses the new value. Passing `nil` (or `0`) leaves the loop idle.
  internal nonisolated static func setPollingIntervalSeconds(_ intervalSeconds: TimeInterval?) {
    AppMetricsActor.isolated {
      self.pollingIntervalSeconds = intervalSeconds
      if !pollingLoopStarted, let intervalSeconds, intervalSeconds > 0 {
        pollingLoopStarted = true
        startPollingLoop()
      }
    }
  }

  /// Sets the delay between detecting new rows and the deferred dispatch firing.
  internal nonisolated static func setDeferredDispatchDelaySeconds(_ delaySeconds: TimeInterval?) {
    AppMetricsActor.isolated {
      self.deferredDispatchDelaySeconds = delaySeconds
    }
  }

  /// Runs the repeating poll on `AppMetricsActor`. The poll checks the metrics and logs DB cursors
  /// for new rows; when either has new rows, it (re)arms a one-shot deferred dispatch — bursty
  /// writes batch into a single dispatch at the end of the window. The poll itself does not send
  /// anything. The loop reads `pollingIntervalSeconds` at each wake — if cleared, it idles in a
  /// 1-minute heartbeat until a positive value is restored.
  private static func startPollingLoop() {
    AppMetricsActor.isolated {
      while !Task.isCancelled {
        let interval = pollingIntervalSeconds.map { max($0, 1) } ?? 60
        try? await Task.sleep(nanoseconds: UInt64(interval) * 1_000_000_000)
        guard let configured = pollingIntervalSeconds, configured > 0 else {
          continue
        }
        pollOnceAndMaybeArmDispatch()
      }
    }
  }

  /// One pass of the polling loop. Compares the max metric/log ids against the persisted dispatch
  /// cursors — if either has new rows, (re)arms the deferred dispatch timer.
  private static func pollOnceAndMaybeArmDispatch() {
    let hasNewMetrics: Bool = {
      do {
        guard let maxId = try AppMetrics.getMaxMetricId() else { return false }
        return maxId > ObserveUserDefaults.lastDispatchedMetricId
      } catch {
        observeLogger.warn("[EAS Observe] Polling failed to read max metric id: \(error.localizedDescription)")
        return false
      }
    }()
    let hasNewLogs: Bool = {
      do {
        guard let maxId = try AppMetrics.getMaxLogId() else { return false }
        return maxId > ObserveUserDefaults.lastDispatchedLogId
      } catch {
        observeLogger.warn("[EAS Observe] Polling failed to read max log id: \(error.localizedDescription)")
        return false
      }
    }()
    if hasNewMetrics || hasNewLogs {
      armDeferredDispatch()
    }
  }

  /// Arms — or re-arms — the deferred dispatch timer.
  ///
  /// - First arm in a deferral window: schedule for `now + delay`.
  /// - Subsequent re-arms (timer already pending): push the existing fire time out by `delay / 2`,
  ///   capped so re-arms can never push beyond `original arm time + 2 × delay`. The cap bounds
  ///   the worst-case starvation in a chatty app: once at the cap, further polls don't extend the
  ///   timer, and it fires on schedule.
  private static func armDeferredDispatch() {
    let delay = max((deferredDispatchDelaySeconds ?? defaultDeferredDispatchDelaySeconds), 0)
    let now = Date()

    let newFireTime: Date
    if let existingFireTime = deferredDispatchFireTime,
      let originalArmTime = deferredDispatchOriginalArmTime,
      existingFireTime > now
    {
      // Re-arm: push existing fire time out by half the delay, capped at original + 2 × delay.
      let pushed = existingFireTime.addingTimeInterval(delay / 2)
      let cap = originalArmTime.addingTimeInterval(2 * delay)
      newFireTime = min(pushed, cap)
      observeLogger.debug(
        "[EAS Observe] Re-arming deferred dispatch: pushed to \(newFireTime) (cap \(cap))"
      )
    } else {
      // First arm in a fresh window.
      newFireTime = now.addingTimeInterval(delay)
      deferredDispatchOriginalArmTime = now
      observeLogger.debug("[EAS Observe] Arming deferred dispatch for \(newFireTime)")
    }

    deferredDispatchTask?.cancel()
    deferredDispatchFireTime = newFireTime
    let sleepInterval = max(newFireTime.timeIntervalSince(now), 0)
    let nanoseconds = UInt64(sleepInterval.rounded()) * 1_000_000_000
    deferredDispatchTask = Task { @AppMetricsActor in
      try? await Task.sleep(nanoseconds: nanoseconds)
      if Task.isCancelled { return }
      deferredDispatchTask = nil
      deferredDispatchFireTime = nil
      deferredDispatchOriginalArmTime = nil
      await dispatch(cancelDeferred: false)
    }
  }

  /// Cancels any armed deferred dispatch. Called from `dispatch(cancelDeferred:)` so any other
  /// dispatch path (lifecycle, manual) supersedes the deferred timer.
  private static func cancelDeferredDispatch() {
    deferredDispatchTask?.cancel()
    deferredDispatchTask = nil
    deferredDispatchFireTime = nil
    deferredDispatchOriginalArmTime = nil
  }

  internal static func dispatch() async {
    await dispatch(cancelDeferred: true)
  }

  /// `cancelDeferred` is `true` from every entry point except the deferred timer itself — the timer
  /// already nilled the task out before calling, so cancelling there would just no-op.
  private static func dispatch(cancelDeferred: Bool) async {
    if cancelDeferred {
      cancelDeferredDispatch()
    }
    // Compute once and reuse for both signals — `shouldDispatch()` reads the persisted config, the
    // bundle defaults, and computes a sample-rate hash. Both halves of dispatch want the same answer.
    let shouldDispatch = Self.shouldDispatch()

    await dispatchMetrics(shouldDispatch: shouldDispatch)
    await dispatchLogs(shouldDispatch: shouldDispatch)
  }

  private static func dispatchMetrics(shouldDispatch: Bool) async {
    repairMetricCursorIfStale()

    let cursor = ObserveUserDefaults.lastDispatchedMetricId
    let pendingMetrics: [MetricRow]
    do {
      pendingMetrics = try AppMetrics.getMetrics(afterId: cursor)
    } catch {
      observeLogger.warn("[EAS Observe] Failed to read pending metrics: \(error.localizedDescription)")
      return
    }
    guard !pendingMetrics.isEmpty, let endpointUrl = metricsEndpointUrl else {
      observeLogger.debug("[EAS Observe] No new metrics to dispatch")
      return
    }
    let highestId = pendingMetrics.last?.id ?? cursor
    if !shouldDispatch {
      ObserveUserDefaults.lastDispatchedMetricId = highestId
      return
    }
    let events: [Event]
    do {
      events = try buildEvents(forMetrics: pendingMetrics)
    } catch {
      observeLogger.warn("[EAS Observe] Failed to assemble metric events: \(error.localizedDescription)")
      return
    }
    if events.isEmpty {
      ObserveUserDefaults.lastDispatchedMetricId = highestId
      return
    }
    do {
      let body = OTRequestBody(resourceMetrics: events.map { $0.toOTEvent(easClientId) })
      let success = try await sendRequest(to: endpointUrl, body: body)
      if success {
        ObserveUserDefaults.lastDispatchDate = Date.now
        ObserveUserDefaults.lastDispatchedMetricId = highestId
      }
    } catch {
      observeLogger.warn("[EAS Observe] Dispatching the metrics has thrown an error: \(error)")
    }
  }

  private static func dispatchLogs(shouldDispatch: Bool) async {
    repairLogCursorIfStale()

    let cursor = ObserveUserDefaults.lastDispatchedLogId
    let pendingLogs: [LogRow]
    do {
      pendingLogs = try AppMetrics.getLogs(afterId: cursor)
    } catch {
      observeLogger.warn("[EAS Observe] Failed to read pending logs: \(error.localizedDescription)")
      return
    }
    guard !pendingLogs.isEmpty, let endpointUrl = logsEndpointUrl else {
      observeLogger.debug("[EAS Observe] No new logs to dispatch")
      return
    }
    let highestId = pendingLogs.last?.id ?? cursor
    if !shouldDispatch {
      ObserveUserDefaults.lastDispatchedLogId = highestId
      return
    }
    let events: [Event]
    do {
      events = try buildEvents(forLogs: pendingLogs)
    } catch {
      observeLogger.warn("[EAS Observe] Failed to assemble log events: \(error.localizedDescription)")
      return
    }
    let resourceLogs = events.compactMap { event -> OTResourceLogs? in
      guard !event.logs.isEmpty else {
        return nil
      }
      return event.toOTResourceLogs(easClientId)
    }
    if resourceLogs.isEmpty {
      ObserveUserDefaults.lastDispatchedLogId = highestId
      return
    }
    do {
      let body = OTLogsRequestBody(resourceLogs: resourceLogs)
      let success = try await sendRequest(to: endpointUrl, body: body)
      if success {
        ObserveUserDefaults.lastDispatchedLogId = highestId
      }
    } catch {
      observeLogger.warn("[EAS Observe] Dispatching the logs has thrown an error: \(error)")
    }
  }

  /// Groups `metrics` by `sessionId`, hydrates the matching session rows, and emits one `Event` per
  /// session in the same shape Android dispatches: each event carries the session's metadata and only
  /// the metrics that belong to it.
  private static func buildEvents(forMetrics metrics: [MetricRow]) throws -> [Event] {
    let metricsBySession = Dictionary(grouping: metrics, by: \.sessionId)
    let sessionIds = Array(metricsBySession.keys)
    let sessions = try AppMetrics.getSessions(ids: sessionIds)
    return sessions.compactMap { session in
      guard let sessionMetrics = metricsBySession[session.id] else {
        return nil
      }
      return Event.from(session: session, metrics: sessionMetrics, logs: [])
    }
  }

  private static func buildEvents(forLogs logs: [LogRow]) throws -> [Event] {
    let logsBySession = Dictionary(grouping: logs, by: \.sessionId)
    let sessionIds = Array(logsBySession.keys)
    let sessions = try AppMetrics.getSessions(ids: sessionIds)
    return sessions.compactMap { session in
      guard let sessionLogs = logsBySession[session.id] else {
        return nil
      }
      return Event.from(session: session, metrics: [], logs: sessionLogs)
    }
  }

  private static func sendRequest(to endpointUrl: URL, body: any Encodable) async throws -> Bool {
    var request = URLRequest(url: endpointUrl)
    request.httpMethod = "POST"
    request.allHTTPHeaderFields = [
      "Content-Type": "application/json",
      // Tells `NetworkRequestURLProtocol` to skip observation so our own telemetry uploads don't
      // get logged back into the network-request stream. The header reaches o.expo.dev unchanged
      // (we control that endpoint, so the harmless overhead is fine). The name is duplicated here
      // rather than imported: expo-observe must not depend on expo-app-metrics internals. Keep it
      // in sync with `NetworkRequestURLProtocol.internalHeaderName` in expo-app-metrics.
      "Expo-AppMetrics-Skip": "1",
    ]
    request.httpBody = try body.toJSONData([])

    #if DEBUG
    observeLogger.debug("[EAS Observe] Sending the request to \(endpointUrl) with body:")
    // Use `print` so the JSON can be copied without including the log level emojis. Wrapped in
    // `#if DEBUG` so release builds don't pay for a second pretty-printed encode of the payload.
    print(try body.toJSONString(.prettyPrinted))
    #endif

    let (responseData, urlResponse) = try await URLSession.shared.data(for: request)

    guard let urlResponse = urlResponse as? HTTPURLResponse else {
      return false
    }
    guard (200...299).contains(urlResponse.statusCode) else {
      observeLogger.warn(
        "[EAS Observe] Server responded with \(urlResponse.statusCode) status code and data: \(String(data: responseData, encoding: .utf8) ?? "<unreadable>")"
      )
      return false
    }
    observeLogger.debug(
      "[EAS Observe] Server responded successfully with \(urlResponse.statusCode) status code and data: \(String(data: responseData, encoding: .utf8) ?? "<unreadable>")"
    )
    return true
  }

  internal nonisolated static func setEndpointUrl(_ urlString: String?, projectId: String) {
    let defaultUrl = "https://o.expo.dev"
    let urlString = urlString ?? defaultUrl

    guard let url = URL(string: urlString) else {
      observeLogger.warn("[EAS Observe] Unable to set the endpoint url with string: \(urlString)")
      return
    }
    AppMetricsActor.isolated {
      self.metricsEndpointUrl = url.appendingPathComponent("\(projectId)/v1/metrics")
      self.logsEndpointUrl = url.appendingPathComponent("\(projectId)/v1/logs")
    }
  }

  // Static function extracted for testability
  internal nonisolated static func shouldDispatch(
    config: PersistedConfig?, isDev: Bool, isInSample: Bool
  ) -> Bool {
    let dispatchingEnabled = config?.dispatchingEnabled ?? true
    let dispatchInDebug = config?.dispatchInDebug ?? false
    return dispatchingEnabled && isInSample && (!isDev || dispatchInDebug)
  }

  private static func shouldDispatch() -> Bool {
    let isJsDev = ObserveUserDefaults.bundleDefaults?.isJsDev ?? false
    let isDev = EXAppDefines.APP_DEBUG || isJsDev
    return Self.shouldDispatch(
      config: ObserveUserDefaults.config, isDev: isDev, isInSample: isInSample()
    )
  }

  private static func isInSample() -> Bool {
    guard let rate = ObserveUserDefaults.config?.sampleRate else {
      return true
    }
    let clamped = min(max(rate, 0.0), 1.0)
    return EASClientID.deterministicUniformValue(EASClientID.uuid()) < clamped
  }
}
