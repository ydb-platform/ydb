/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * DO NOT EDIT, this is an Auto-generated file from:
 * buildscripts/semantic-convention/templates/registry/semantic_metrics-h.j2
 */

#pragma once

#include "opentelemetry/common/macros.h"
#include "opentelemetry/metrics/meter.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace semconv
{
namespace mcp
{

/**
  The duration of the MCP request or notification as observed on the sender from the time it was
  sent until the response or ack is received.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> histogram
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kMetricMcpClientOperationDuration =
    "mcp.client.operation.duration";
OPENTELEMETRY_DEPRECATED static constexpr const char *descrMetricMcpClientOperationDuration =
    "The duration of the MCP request or notification as observed on the sender from the time it was sent until the response or ack is received.
    ";
    OPENTELEMETRY_DEPRECATED static constexpr const char *unitMetricMcpClientOperationDuration =
        "s";

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<uint64_t>>
CreateSyncInt64MetricMcpClientOperationDuration(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateUInt64Histogram(kMetricMcpClientOperationDuration,
                                      descrMetricMcpClientOperationDuration,
                                      unitMetricMcpClientOperationDuration);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<double>>
CreateSyncDoubleMetricMcpClientOperationDuration(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateDoubleHistogram(kMetricMcpClientOperationDuration,
                                      descrMetricMcpClientOperationDuration,
                                      unitMetricMcpClientOperationDuration);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

/**
  The duration of the MCP session as observed on the MCP client.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> histogram
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kMetricMcpClientSessionDuration =
    "mcp.client.session.duration";
OPENTELEMETRY_DEPRECATED static constexpr const char *descrMetricMcpClientSessionDuration =
    "The duration of the MCP session as observed on the MCP client.";
OPENTELEMETRY_DEPRECATED static constexpr const char *unitMetricMcpClientSessionDuration = "s";

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<uint64_t>>
CreateSyncInt64MetricMcpClientSessionDuration(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateUInt64Histogram(kMetricMcpClientSessionDuration,
                                      descrMetricMcpClientSessionDuration,
                                      unitMetricMcpClientSessionDuration);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<double>>
CreateSyncDoubleMetricMcpClientSessionDuration(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateDoubleHistogram(kMetricMcpClientSessionDuration,
                                      descrMetricMcpClientSessionDuration,
                                      unitMetricMcpClientSessionDuration);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

/**
  MCP request or notification duration as observed on the receiver from the time it was received
  until the result or ack is sent.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> histogram
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kMetricMcpServerOperationDuration =
    "mcp.server.operation.duration";
OPENTELEMETRY_DEPRECATED static constexpr const char *descrMetricMcpServerOperationDuration =
    "MCP request or notification duration as observed on the receiver from the time it was received until the result or ack is sent.
    ";
    OPENTELEMETRY_DEPRECATED static constexpr const char *unitMetricMcpServerOperationDuration =
        "s";

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<uint64_t>>
CreateSyncInt64MetricMcpServerOperationDuration(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateUInt64Histogram(kMetricMcpServerOperationDuration,
                                      descrMetricMcpServerOperationDuration,
                                      unitMetricMcpServerOperationDuration);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<double>>
CreateSyncDoubleMetricMcpServerOperationDuration(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateDoubleHistogram(kMetricMcpServerOperationDuration,
                                      descrMetricMcpServerOperationDuration,
                                      unitMetricMcpServerOperationDuration);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

/**
  The duration of the MCP session as observed on the MCP server.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> histogram
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kMetricMcpServerSessionDuration =
    "mcp.server.session.duration";
OPENTELEMETRY_DEPRECATED static constexpr const char *descrMetricMcpServerSessionDuration =
    "The duration of the MCP session as observed on the MCP server.";
OPENTELEMETRY_DEPRECATED static constexpr const char *unitMetricMcpServerSessionDuration = "s";

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<uint64_t>>
CreateSyncInt64MetricMcpServerSessionDuration(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateUInt64Histogram(kMetricMcpServerSessionDuration,
                                      descrMetricMcpServerSessionDuration,
                                      unitMetricMcpServerSessionDuration);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<double>>
CreateSyncDoubleMetricMcpServerSessionDuration(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateDoubleHistogram(kMetricMcpServerSessionDuration,
                                      descrMetricMcpServerSessionDuration,
                                      unitMetricMcpServerSessionDuration);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

}  // namespace mcp
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
