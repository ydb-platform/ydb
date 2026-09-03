// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "ot_trace_state.h"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>

#include "opentelemetry/nostd/span.h"
#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/version.h"

namespace
{

constexpr char kHexDigits[] = "0123456789abcdef";

// Number of contiguous zero bits at the least significant end of value.
// value is guaranteed non-zero by the callers.
int CountTrailingZeroBits(uint64_t value) noexcept
{
  int count = 0;
  while ((value & 1U) == 0U)
  {
    value >>= 1;
    ++count;
  }
  return count;
}

// Appends the 56-bit threshold as most-significant-first hex digits, dropping
// trailing zero nibbles. A threshold of 0 is emitted as a single "0".
void AppendThresholdHex(std::string &out, uint64_t threshold)
{
  // Setting bit 55 guarantees at least one significant digit (covers the
  // threshold == 0 case) and bounds the loop below.
  uint64_t marked    = threshold | (static_cast<uint64_t>(1) << 55);
  int trailing_zeros = CountTrailingZeroBits(marked);
  for (int shift = 52; shift >= trailing_zeros - 3; shift -= 4)
  {
    out.push_back(kHexDigits[(threshold >> shift) & 0xFU]);
  }
}

// Appends the 56-bit randomness value as exactly 14 zero-padded hex digits.
void AppendRandomHex(std::string &out, uint64_t value)
{
  for (int shift = 52; shift >= 0; shift -= 4)
  {
    out.push_back(kHexDigits[(value >> shift) & 0xFU]);
  }
}

// Parses len lowercase hex digits starting at s[start] into a left-aligned
// 56-bit value. Returns false on any non-hex character.
bool ParseHex(const std::string &s, std::size_t start, std::size_t len, uint64_t &result) noexcept
{
  uint64_t r = 0;
  for (std::size_t i = 0; i < len; ++i)
  {
    char c     = s[start + i];
    int nibble = 0;
    if (c >= '0' && c <= '9')
    {
      nibble = c - '0';
    }
    else if (c >= 'a' && c <= 'f')
    {
      nibble = c - 'a' + 10;
    }
    else
    {
      return false;
    }
    int shift = 52 - static_cast<int>(i) * 4;
    r |= static_cast<uint64_t>(nibble) << shift;
  }
  result = r;
  return true;
}

}  // namespace

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

uint64_t CalculateThreshold(double sampling_probability) noexcept
{
  if (sampling_probability <= 0.0)
  {
    return kMaxThreshold;
  }
  if (sampling_probability >= 1.0)
  {
    return 0;
  }
  // 0x1p56 == 2^56. sampling_probability is in (0, 1) here, so the rounded
  // product is in [0, kMaxThreshold] and the subtraction never underflows.
  uint64_t kept = static_cast<uint64_t>(std::llround(sampling_probability * 0x1p56));
  return kMaxThreshold - kept;
}

uint64_t GetRandomnessFromTraceId(const opentelemetry::trace::TraceId &trace_id) noexcept
{
  const uint8_t *data = trace_id.Id().data();
  uint64_t randomness = 0;
  for (std::size_t i = opentelemetry::trace::TraceId::kSize - 7;
       i < opentelemetry::trace::TraceId::kSize; ++i)
  {
    randomness = (randomness << 8) | data[i];
  }
  return randomness;
}

OtelTraceState OtelTraceState::Parse(const std::string &ot_value) noexcept
{
  OtelTraceState state;
  const std::size_t len = ot_value.size();
  if (len == 0 || len > 256)
  {
    return state;
  }

  std::size_t pos = 0;
  while (pos < len)
  {
    std::size_t sep   = ot_value.find(';', pos);
    std::size_t end   = (sep == std::string::npos) ? len : sep;
    std::size_t colon = ot_value.find(':', pos);

    if (colon != std::string::npos && colon < end)
    {
      std::size_t value_start = colon + 1;
      std::size_t value_len   = end - value_start;
      if (ot_value.compare(pos, colon - pos, "th") == 0)
      {
        uint64_t threshold_value;
        if (value_len <= 14 && ParseHex(ot_value, value_start, value_len, threshold_value))
        {
          state.has_threshold = true;
          state.threshold     = threshold_value;
        }
      }
      else if (ot_value.compare(pos, colon - pos, "rv") == 0)
      {
        uint64_t random_value;
        if (value_len == 14 && ParseHex(ot_value, value_start, value_len, random_value))
        {
          state.has_random_value = true;
          state.random_value     = random_value;
        }
      }
      else
      {
        state.other_subkeys.push_back(ot_value.substr(pos, end - pos));
      }
    }

    if (sep == std::string::npos)
    {
      break;
    }
    pos = sep + 1;
  }
  return state;
}

std::string OtelTraceState::Serialize() const
{
  std::string out;
  if (has_threshold && threshold < kMaxThreshold)
  {
    out.append("th:");
    AppendThresholdHex(out, threshold);
  }
  if (has_random_value)
  {
    if (!out.empty())
    {
      out.push_back(';');
    }
    out.append("rv:");
    AppendRandomHex(out, random_value);
  }
  for (const auto &pair : other_subkeys)
  {
    std::size_t extra = out.empty() ? pair.size() : pair.size() + 1;
    if (out.size() + extra > 256)
    {
      break;
    }
    if (!out.empty())
    {
      out.push_back(';');
    }
    out.append(pair);
  }
  return out;
}

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
