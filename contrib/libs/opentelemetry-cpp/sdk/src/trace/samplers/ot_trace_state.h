// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

// Key of the OpenTelemetry sub-state inside the W3C tracestate.
static constexpr const char *kOtTraceStateKey = "ot";

// Number of bits used for randomness and thresholds in consistent probability
// sampling (W3C Trace Context Level 2 / OpenTelemetry tracestate "ot").
static constexpr int kRandomValueBits = 56;

// threshold == kMaxThreshold corresponds to 0% sampling probability.
// threshold == 0 corresponds to 100% sampling probability. A span is kept when
// its randomness R satisfies R >= threshold.
static constexpr uint64_t kMaxThreshold   = static_cast<uint64_t>(1) << kRandomValueBits;
static constexpr uint64_t kMaxRandomValue = kMaxThreshold - 1;

// Maps a sampling probability in [0, 1] to a 56-bit rejection threshold in the
// range [0, kMaxThreshold].
uint64_t CalculateThreshold(double sampling_probability) noexcept;

// Extracts the 56-bit randomness value from the least significant 56 bits of
// the trace id, as specified by W3C Trace Context Level 2.
uint64_t GetRandomnessFromTraceId(const opentelemetry::trace::TraceId &trace_id) noexcept;

// Parsed view of the OpenTelemetry "ot" tracestate value. Holds the "th"
// (threshold) and "rv" (randomness) sub-keys; any other sub-keys are preserved
// verbatim and re-emitted on serialization.
struct OtelTraceState
{
  bool has_threshold = false;
  uint64_t threshold = 0;  // valid in [0, kMaxThreshold]

  bool has_random_value = false;
  uint64_t random_value = 0;  // valid in [0, kMaxRandomValue]

  std::vector<std::string> other_subkeys;

  // Parses the value of the "ot" tracestate key. On any parse error the
  // offending sub-key is dropped; the parser never throws.
  static OtelTraceState Parse(const std::string &ot_value) noexcept;

  // Serializes back to the "ot" tracestate value. Returns an empty string when
  // there is nothing to emit.
  std::string Serialize() const;
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
