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
namespace gen_ai
{

/**
  GenAI operation duration.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> histogram
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kMetricGenAiClientOperationDuration =
    "gen_ai.client.operation.duration";
OPENTELEMETRY_DEPRECATED static constexpr const char *descrMetricGenAiClientOperationDuration =
    "GenAI operation duration.";
OPENTELEMETRY_DEPRECATED static constexpr const char *unitMetricGenAiClientOperationDuration = "s";

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<uint64_t>>
CreateSyncInt64MetricGenAiClientOperationDuration(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateUInt64Histogram(kMetricGenAiClientOperationDuration,
                                      descrMetricGenAiClientOperationDuration,
                                      unitMetricGenAiClientOperationDuration);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<double>>
CreateSyncDoubleMetricGenAiClientOperationDuration(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateDoubleHistogram(kMetricGenAiClientOperationDuration,
                                      descrMetricGenAiClientOperationDuration,
                                      unitMetricGenAiClientOperationDuration);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

/**
  Time per output chunk, recorded for each chunk received after the first one, measured as the time
  elapsed from the end of the previous chunk to the end of the current chunk.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> This metrics SHOULD be reported for
  streaming calls and SHOULD NOT be reported otherwise. <p> histogram
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *
    kMetricGenAiClientOperationTimePerOutputChunk = "gen_ai.client.operation.time_per_output_chunk";
OPENTELEMETRY_DEPRECATED static constexpr const char
    *descrMetricGenAiClientOperationTimePerOutputChunk =
        "Time per output chunk, recorded for each chunk received after the first one, measured as the time elapsed from the end of the previous chunk to the end of the current chunk.
        ";
    OPENTELEMETRY_DEPRECATED static constexpr const char
        *unitMetricGenAiClientOperationTimePerOutputChunk = "s";

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<uint64_t>>
CreateSyncInt64MetricGenAiClientOperationTimePerOutputChunk(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateUInt64Histogram(kMetricGenAiClientOperationTimePerOutputChunk,
                                      descrMetricGenAiClientOperationTimePerOutputChunk,
                                      unitMetricGenAiClientOperationTimePerOutputChunk);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<double>>
CreateSyncDoubleMetricGenAiClientOperationTimePerOutputChunk(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateDoubleHistogram(kMetricGenAiClientOperationTimePerOutputChunk,
                                      descrMetricGenAiClientOperationTimePerOutputChunk,
                                      unitMetricGenAiClientOperationTimePerOutputChunk);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

/**
  Time to receive the first chunk, measured from when the client issues the generation request to
  when the first chunk is received in the response stream.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> This metrics SHOULD be reported for
  streaming calls and SHOULD NOT be reported otherwise. <p> histogram
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kMetricGenAiClientOperationTimeToFirstChunk =
    "gen_ai.client.operation.time_to_first_chunk";
OPENTELEMETRY_DEPRECATED static constexpr const char
    *descrMetricGenAiClientOperationTimeToFirstChunk =
        "Time to receive the first chunk, measured from when the client issues the generation "
        "request to when the first chunk is received in the response stream.";
OPENTELEMETRY_DEPRECATED static constexpr const char
    *unitMetricGenAiClientOperationTimeToFirstChunk = "s";

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<uint64_t>>
CreateSyncInt64MetricGenAiClientOperationTimeToFirstChunk(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateUInt64Histogram(kMetricGenAiClientOperationTimeToFirstChunk,
                                      descrMetricGenAiClientOperationTimeToFirstChunk,
                                      unitMetricGenAiClientOperationTimeToFirstChunk);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<double>>
CreateSyncDoubleMetricGenAiClientOperationTimeToFirstChunk(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateDoubleHistogram(kMetricGenAiClientOperationTimeToFirstChunk,
                                      descrMetricGenAiClientOperationTimeToFirstChunk,
                                      unitMetricGenAiClientOperationTimeToFirstChunk);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

/**
  Number of input and output tokens used.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> histogram
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kMetricGenAiClientTokenUsage =
    "gen_ai.client.token.usage";
OPENTELEMETRY_DEPRECATED static constexpr const char *descrMetricGenAiClientTokenUsage =
    "Number of input and output tokens used.";
OPENTELEMETRY_DEPRECATED static constexpr const char *unitMetricGenAiClientTokenUsage = "{token}";

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<uint64_t>>
CreateSyncInt64MetricGenAiClientTokenUsage(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateUInt64Histogram(kMetricGenAiClientTokenUsage,
                                      descrMetricGenAiClientTokenUsage,
                                      unitMetricGenAiClientTokenUsage);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<double>>
CreateSyncDoubleMetricGenAiClientTokenUsage(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateDoubleHistogram(kMetricGenAiClientTokenUsage,
                                      descrMetricGenAiClientTokenUsage,
                                      unitMetricGenAiClientTokenUsage);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

/**
  Generative AI server request duration such as time-to-last byte or last output token.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> histogram
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kMetricGenAiServerRequestDuration =
    "gen_ai.server.request.duration";
OPENTELEMETRY_DEPRECATED static constexpr const char *descrMetricGenAiServerRequestDuration =
    "Generative AI server request duration such as time-to-last byte or last output token.";
OPENTELEMETRY_DEPRECATED static constexpr const char *unitMetricGenAiServerRequestDuration = "s";

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<uint64_t>>
CreateSyncInt64MetricGenAiServerRequestDuration(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateUInt64Histogram(kMetricGenAiServerRequestDuration,
                                      descrMetricGenAiServerRequestDuration,
                                      unitMetricGenAiServerRequestDuration);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<double>>
CreateSyncDoubleMetricGenAiServerRequestDuration(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateDoubleHistogram(kMetricGenAiServerRequestDuration,
                                      descrMetricGenAiServerRequestDuration,
                                      unitMetricGenAiServerRequestDuration);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

/**
  Time per output token generated after the first token for successful responses.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> histogram
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kMetricGenAiServerTimePerOutputToken =
    "gen_ai.server.time_per_output_token";
OPENTELEMETRY_DEPRECATED static constexpr const char *descrMetricGenAiServerTimePerOutputToken =
    "Time per output token generated after the first token for successful responses.";
OPENTELEMETRY_DEPRECATED static constexpr const char *unitMetricGenAiServerTimePerOutputToken = "s";

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<uint64_t>>
CreateSyncInt64MetricGenAiServerTimePerOutputToken(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateUInt64Histogram(kMetricGenAiServerTimePerOutputToken,
                                      descrMetricGenAiServerTimePerOutputToken,
                                      unitMetricGenAiServerTimePerOutputToken);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<double>>
CreateSyncDoubleMetricGenAiServerTimePerOutputToken(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateDoubleHistogram(kMetricGenAiServerTimePerOutputToken,
                                      descrMetricGenAiServerTimePerOutputToken,
                                      unitMetricGenAiServerTimePerOutputToken);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

/**
  Time to generate first token for successful responses.

  @deprecated
  {"note": "Moved to the <a
  href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI semantic
  conventions repository</a>.\n", "reason": "uncategorized"} <p> histogram
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kMetricGenAiServerTimeToFirstToken =
    "gen_ai.server.time_to_first_token";
OPENTELEMETRY_DEPRECATED static constexpr const char *descrMetricGenAiServerTimeToFirstToken =
    "Time to generate first token for successful responses.";
OPENTELEMETRY_DEPRECATED static constexpr const char *unitMetricGenAiServerTimeToFirstToken = "s";

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<uint64_t>>
CreateSyncInt64MetricGenAiServerTimeToFirstToken(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateUInt64Histogram(kMetricGenAiServerTimeToFirstToken,
                                      descrMetricGenAiServerTimeToFirstToken,
                                      unitMetricGenAiServerTimeToFirstToken);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

OPENTELEMETRY_DEPRECATED static inline nostd::unique_ptr<metrics::Histogram<double>>
CreateSyncDoubleMetricGenAiServerTimeToFirstToken(metrics::Meter *meter)
{
  OPENTELEMETRY_SUPPRESS_DEPRECATED_BEGIN
  return meter->CreateDoubleHistogram(kMetricGenAiServerTimeToFirstToken,
                                      descrMetricGenAiServerTimeToFirstToken,
                                      unitMetricGenAiServerTimeToFirstToken);
  OPENTELEMETRY_SUPPRESS_DEPRECATED_END
}

}  // namespace gen_ai
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
