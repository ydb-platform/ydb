// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <cstddef>
#include <cstdint>

#include "opentelemetry/sdk/common/env_variables.h"
#include "opentelemetry/sdk/trace/span_limits.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{
namespace span_limits_env
{

// Environment variable names, see
// https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/#span-limits
static constexpr const char *kAttributeValueLengthLimitEnv = "OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT";
static constexpr const char *kAttributeCountLimitEnv       = "OTEL_ATTRIBUTE_COUNT_LIMIT";
static constexpr const char *kSpanAttributeValueLengthLimitEnv =
    "OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT";
static constexpr const char *kSpanAttributeCountLimitEnv  = "OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT";
static constexpr const char *kSpanEventCountLimitEnv      = "OTEL_SPAN_EVENT_COUNT_LIMIT";
static constexpr const char *kSpanLinkCountLimitEnv       = "OTEL_SPAN_LINK_COUNT_LIMIT";
static constexpr const char *kEventAttributeCountLimitEnv = "OTEL_EVENT_ATTRIBUTE_COUNT_LIMIT";
static constexpr const char *kLinkAttributeCountLimitEnv  = "OTEL_LINK_ATTRIBUTE_COUNT_LIMIT";

namespace
{

void UpdateFromEnv(const char *env_var_name, std::uint32_t &limit)
{
  std::uint32_t value{};
  if (opentelemetry::sdk::common::GetUintEnvironmentVariable(env_var_name, value))
  {
    limit = value;
  }
}

void UpdateFromEnv(const char *env_var_name, std::size_t &limit)
{
  std::uint32_t value{};
  if (opentelemetry::sdk::common::GetUintEnvironmentVariable(env_var_name, value))
  {
    limit = static_cast<std::size_t>(value);
  }
}

}  // namespace

SpanLimits GetSpanLimitsFromEnv(const SpanLimits &defaults)
{
  SpanLimits limits = defaults;

  // General attribute limits first; the span-specific variables below take precedence.
  UpdateFromEnv(kAttributeValueLengthLimitEnv, limits.attribute_value_length_limit);
  UpdateFromEnv(kAttributeCountLimitEnv, limits.attribute_count_limit);

  UpdateFromEnv(kSpanAttributeValueLengthLimitEnv, limits.attribute_value_length_limit);
  UpdateFromEnv(kSpanAttributeCountLimitEnv, limits.attribute_count_limit);
  UpdateFromEnv(kSpanEventCountLimitEnv, limits.event_count_limit);
  UpdateFromEnv(kSpanLinkCountLimitEnv, limits.link_count_limit);
  UpdateFromEnv(kEventAttributeCountLimitEnv, limits.event_attribute_count_limit);
  UpdateFromEnv(kLinkAttributeCountLimitEnv, limits.link_attribute_count_limit);

  return limits;
}

}  // namespace span_limits_env
}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
