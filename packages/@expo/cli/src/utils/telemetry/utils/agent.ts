import { detectAgent } from 'agent-cli-detector';

const debug = require('debug')('expo:telemetry:agent') as typeof console.log;

export type AgentTelemetryContext = {
  id: string | undefined;
  sessionId: string | undefined;
};

let agentTelemetryContext: AgentTelemetryContext | null = null;

export function getAgentTelemetryContext(): AgentTelemetryContext {
  agentTelemetryContext ??= resolveAgentTelemetryContext();
  return agentTelemetryContext;
}

function resolveAgentTelemetryContext(): AgentTelemetryContext {
  try {
    const { agent } = detectAgent();
    return { id: agent?.id, sessionId: agent?.sessionId };
  } catch (error: any) {
    debug('Failed to detect coding agent: %s', error?.message ?? error);
    return { id: undefined, sessionId: undefined };
  }
}
