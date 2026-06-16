package expo.modules.observe

import android.content.Context
import android.util.Log
import expo.modules.easclient.EASClientID
import expo.modules.observe.storage.PendingLogsManager
import expo.modules.observe.storage.PendingMetricsManager
import expo.modules.appmetrics.storage.SessionManager
import expo.modules.interfaces.constants.ConstantsInterface
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

class ObservabilityManager(
  // TODO(@lukmccall): Consider saving context as weak reference to avoid potential memory leaks
  private val context: Context,
  constants: ConstantsInterface?,
  val sessionManager: SessionManager
) {
  private val baseManager: BaseObservabilityManager

  // TODO: Can this information change during expo module lifecycle?
  init {
    val manifest = getManifest(constants)
    checkNotNull(manifest) {
      "Manifest is required to initialize ObservabilityManager."
    }

    val projectId = manifest.projectId
    checkNotNull(projectId) {
      "Project ID is required to send observability metrics. Make sure you have configured it correctly in app.json."
    }
    val baseUrl = manifest.baseUrl ?: OBSERVE_DEFAULT_BASE_URL

    val pendingMetricsManager = PendingMetricsManager(context)
    val pendingLogsManager = PendingLogsManager(context)

    baseManager = BaseObservabilityManager(
      context = context,
      sessionManager = sessionManager,
      pendingMetricsManager = pendingMetricsManager,
      pendingLogsManager = pendingLogsManager,
      projectId = projectId,
      baseUrl = baseUrl,
      isDebugBuild = BuildConfig.DEBUG
    )

    sessionManager.addMetricsInsertListener { metricIds ->
      pendingMetricsManager.addPendingMetrics(metricIds)
    }
    sessionManager.addLogsInsertListener { logIds ->
      pendingLogsManager.addPendingLogs(logIds)
    }
  }

  suspend fun dispatchUnsentMetrics() {
    cancelDeferredDispatch()
    baseManager.dispatchUnsentMetrics()
  }

  suspend fun dispatchUnsentLogs() {
    cancelDeferredDispatch()
    baseManager.dispatchUnsentLogs()
  }

  fun scheduleBackgroundDispatch() {
    cancelDeferredDispatch()
    ObservabilityBackgroundWorker.scheduleBackgroundDispatch(
      context = context,
      projectId = baseManager.projectId,
      baseUrl = baseManager.baseUrl
    )
  }

  // Manager-owned scope so the polling + deferred-dispatch tasks have a lifecycle tied to module
  // destroy rather than to a single JS-call dispatch (`modulesQueue`, which is per-call).
  // `Dispatchers.IO` is the right pool: each wake reads from the metrics DB and may POST.
  private val dispatchScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
  private var pollingJob: Job? = null
  private var deferredDispatchJob: Job? = null

  /** Default deferred-dispatch delay when no explicit value is configured (30 minutes). */
  private val defaultDeferredDispatchDelaySeconds: Long = 1800

  @Volatile
  private var pollingIntervalSeconds: Long? = null

  @Volatile
  private var deferredDispatchDelaySeconds: Long? = null

  /**
   * Wall-clock millis of the currently-armed deferred dispatch's fire time. Re-arms read this to
   * push the existing fire time out by `delay / 2`, capped at `originalArmTime + 2 × delay`.
   */
  private var deferredDispatchFireTimeMs: Long? = null

  /**
   * Wall-clock millis when the *first* arm in the current deferral window happened. Cleared after
   * dispatch (or cancellation). Combined with `deferredDispatchDelaySeconds` to enforce the hard
   * cap on how far re-arms can push out — at most `2 × delay` past this point.
   */
  private var deferredDispatchOriginalArmTimeMs: Long? = null

  /**
   * Configures the polling loop. A positive `intervalSeconds` starts the loop on its first call;
   * subsequent calls update the interval in place. Each wake reads the metrics/logs DB cursors,
   * and if either has new rows, (re)arms the deferred-dispatch timer. The poll itself does not
   * send anything. `null` or `0` leaves the loop idle.
   */
  fun setPollingIntervalSeconds(intervalSeconds: Long?) {
    pollingIntervalSeconds = intervalSeconds
    if (pollingJob == null && intervalSeconds != null && intervalSeconds > 0) {
      pollingJob = dispatchScope.launch {
        while (true) {
          // Idle in a 1-minute heartbeat while the interval is cleared so a later re-configure
          // resumes promptly.
          val interval = pollingIntervalSeconds?.coerceAtLeast(1) ?: 60
          delay(interval * 1000)
          val configured = pollingIntervalSeconds ?: continue
          if (configured <= 0) {
            continue
          }
          try {
            pollOnceAndMaybeArmDispatch()
          } catch (e: Exception) {
            Log.w(OBSERVE_TAG, "Polling pass failed: ${e.message}")
          }
        }
      }
    }
  }

  /** Sets the delay between detecting new rows and the deferred dispatch firing. */
  fun setDeferredDispatchDelaySeconds(delaySeconds: Long?) {
    deferredDispatchDelaySeconds = delaySeconds
  }

  /**
   * One pass of the polling loop. Checks for new pending metrics/logs and (re)arms the deferred
   * dispatch timer if any are found.
   */
  private suspend fun pollOnceAndMaybeArmDispatch() {
    val hasNewMetrics = baseManager.hasPendingMetrics()
    val hasNewLogs = baseManager.hasPendingLogs()
    if (hasNewMetrics || hasNewLogs) {
      armDeferredDispatch()
    }
  }

  /**
   * Arms — or re-arms — the deferred dispatch timer.
   *
   * - First arm in a deferral window: schedule for `now + delay`.
   * - Subsequent re-arms (timer already pending): push the existing fire time out by `delay / 2`,
   *   capped at `originalArmTime + 2 × delay`. The cap bounds the worst-case starvation: once at
   *   the cap, further polls don't extend the timer and it fires on schedule.
   */
  private fun armDeferredDispatch() {
    val delayMs = ((deferredDispatchDelaySeconds ?: defaultDeferredDispatchDelaySeconds).coerceAtLeast(0)) * 1000
    val now = System.currentTimeMillis()

    val newFireTimeMs: Long
    val existingFire = deferredDispatchFireTimeMs
    val originalArm = deferredDispatchOriginalArmTimeMs
    if (existingFire != null && originalArm != null && existingFire > now) {
      // Re-arm: push out by half the delay, capped at original + 2 × delay.
      val pushed = existingFire + delayMs / 2
      val cap = originalArm + 2 * delayMs
      newFireTimeMs = minOf(pushed, cap)
      Log.d(OBSERVE_TAG, "Re-arming deferred dispatch: pushed fire time to $newFireTimeMs (cap $cap)")
    } else {
      newFireTimeMs = now + delayMs
      deferredDispatchOriginalArmTimeMs = now
      Log.d(OBSERVE_TAG, "Arming deferred dispatch for $newFireTimeMs")
    }

    deferredDispatchJob?.cancel()
    deferredDispatchFireTimeMs = newFireTimeMs
    val sleepMs = (newFireTimeMs - now).coerceAtLeast(0)
    deferredDispatchJob = dispatchScope.launch {
      delay(sleepMs)
      deferredDispatchJob = null
      deferredDispatchFireTimeMs = null
      deferredDispatchOriginalArmTimeMs = null
      try {
        baseManager.dispatchUnsentMetrics()
        baseManager.dispatchUnsentLogs()
      } catch (e: Exception) {
        Log.w(OBSERVE_TAG, "Deferred dispatch failed: ${e.message}")
      }
    }
  }

  /**
   * Cancels any armed deferred dispatch. Called from every other dispatch path so the lifecycle /
   * manual dispatches supersede the deferred timer.
   */
  private fun cancelDeferredDispatch() {
    deferredDispatchJob?.cancel()
    deferredDispatchJob = null
    deferredDispatchFireTimeMs = null
    deferredDispatchOriginalArmTimeMs = null
  }

  /**
   * Cancels the polling and deferred-dispatch jobs. Called from `ObserveModule.OnDestroy` so they
   * don't try to hit the network or DB after the module's lifecycle has ended.
   */
  fun destroy() {
    dispatchScope.cancel()
    pollingJob = null
    deferredDispatchJob = null
    deferredDispatchFireTimeMs = null
    deferredDispatchOriginalArmTimeMs = null
  }
}

class BaseObservabilityManager(
  private val context: Context,
  private val sessionManager: SessionManager,
  private val pendingMetricsManager: PendingMetricsManager,
  private val pendingLogsManager: PendingLogsManager,
  val projectId: String,
  val baseUrl: String,
  private val isDebugBuild: Boolean = false,
  private val deterministicUniformValueProvider: () -> Double = {
    EASClientID.deterministicUniformValue(EASClientID(context).uuid)
  }
) {
  private val eventDispatcher = EventDispatcher(
    context = context,
    projectId = projectId,
    baseUrl = baseUrl
  )

  /** Whether the pending-metrics table has any rows. Used by the polling pass. */
  suspend fun hasPendingMetrics(): Boolean = pendingMetricsManager.getAllPendingMetricIds().isNotEmpty()

  /** Whether the pending-logs table has any rows. Used by the polling pass. */
  suspend fun hasPendingLogs(): Boolean = pendingLogsManager.getAllPendingLogIds().isNotEmpty()

  suspend fun dispatchUnsentMetrics() {
    val pendingIds = pendingMetricsManager.getAllPendingMetricIds()
    if (pendingIds.isEmpty()) {
      return
    }

    if (!shouldDispatch()) {
      pendingMetricsManager.removePendingMetrics(pendingIds)
      return
    }

    val sessionsWithPendingMetrics = sessionManager.getSessionsWithMetrics(pendingIds)

    // Clean up orphaned pending IDs (metrics deleted from MetricsDatabase but still in pending table)
    val resolvedMetricIds = sessionsWithPendingMetrics.flatMap { it.metrics }.map { it.metricId }.toSet()
    val orphanedIds = pendingIds.filter { it !in resolvedMetricIds }
    if (orphanedIds.isNotEmpty()) {
      pendingMetricsManager.removePendingMetrics(orphanedIds)
    }

    if (sessionsWithPendingMetrics.isEmpty()) {
      return
    }

    val events = sessionsWithPendingMetrics.map { sessionWithMetrics ->
      Event(
        metadata = Metadata.fromSessionMetadata(sessionWithMetrics.session),
        metrics = sessionWithMetrics.metrics.map { EASMetric.fromMetric(it) }
      )
    }

    if (eventDispatcher.dispatch(events)) {
      val dispatchedMetricIds = sessionsWithPendingMetrics.flatMap { it.metrics }.map { it.metricId }
      pendingMetricsManager.removePendingMetrics(dispatchedMetricIds)
    }
  }

  /**
   * Dispatches log events to `/v1/logs`. Independent from the metrics path —
   * a logs failure doesn't affect the metrics pending table and vice versa.
   */
  suspend fun dispatchUnsentLogs() {
    val pendingIds = pendingLogsManager.getAllPendingLogIds()
    if (pendingIds.isEmpty()) {
      return
    }

    if (!shouldDispatch()) {
      pendingLogsManager.removePendingLogs(pendingIds)
      return
    }

    val sessionsWithPendingLogs = sessionManager.getSessionsWithLogs(pendingIds)

    // Clean up orphaned pending IDs (logs deleted from the `logs` table but
    // still tracked in `pending_logs`).
    val resolvedLogIds = sessionsWithPendingLogs.flatMap { it.logs }.map { it.logId }.toSet()
    val orphanedIds = pendingIds.filter { it !in resolvedLogIds }
    if (orphanedIds.isNotEmpty()) {
      pendingLogsManager.removePendingLogs(orphanedIds)
    }

    if (sessionsWithPendingLogs.isEmpty()) {
      return
    }

    val events = sessionsWithPendingLogs.map { sessionWithLogs ->
      Event(
        metadata = Metadata.fromSessionMetadata(sessionWithLogs.session),
        metrics = emptyList(),
        logs = sessionWithLogs.logs.map { LogEvent.fromLogRecord(it) }
      )
    }

    if (eventDispatcher.dispatchLogs(events)) {
      val dispatchedLogIds = sessionsWithPendingLogs.flatMap { it.logs }.map { it.logId }
      pendingLogsManager.removePendingLogs(dispatchedLogIds)
    }
  }

  private fun isInSample(): Boolean {
    val rate = ObservePreferences.getConfig(context)?.sampleRate ?: return true
    val clamped = rate.coerceIn(0.0, 1.0)
    return deterministicUniformValueProvider() < clamped
  }

  private fun shouldDispatch(): Boolean {
    val config = ObservePreferences.getConfig(context)
    val dispatchingEnabled = config?.dispatchingEnabled ?: true
    val dispatchInDebug = config?.dispatchInDebug ?: false
    // `isDev` is the OR of the JS-bundle dev flag (pushed via `setBundleDefaults` on JS
    // package import) and the native build's debug flag. Either being true means the
    // bundle should be treated as dev for dispatch-gating.
    val isJsDev = ObservePreferences.getBundleDefaults(context)?.isJsDev ?: false
    val isDev = isDebugBuild || isJsDev
    return dispatchingEnabled && isInSample() && (!isDev || dispatchInDebug)
  }

  suspend fun cleanup() {
    pendingMetricsManager.cleanupOldPendingMetrics()
    pendingLogsManager.cleanupOldPendingLogs()
    // TODO(@ubax): Move sessionManager.cleanupOldSessions out of eas observe
    sessionManager.cleanupOldSessions()
    sessionManager.cleanupOldLogs()
  }
}
