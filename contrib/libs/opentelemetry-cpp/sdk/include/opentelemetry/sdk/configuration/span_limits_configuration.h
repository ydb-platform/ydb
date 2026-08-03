// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/tracer_provider.json
// YAML-NODE: SpanLimits
class SpanLimitsConfiguration
{
public:
  // TODO: spec default is no limit, using 4096 to preserve original behavior
  static constexpr std::size_t kDefaultAttributeValueLengthLimit  = 4096;
  static constexpr std::uint32_t kDefaultAttributeCountLimit      = 128;
  static constexpr std::uint32_t kDefaultEventCountLimit          = 128;
  static constexpr std::uint32_t kDefaultLinkCountLimit           = 128;
  static constexpr std::uint32_t kDefaultEventAttributeCountLimit = 128;
  static constexpr std::uint32_t kDefaultLinkAttributeCountLimit  = 128;

  std::size_t attribute_value_length_limit{kDefaultAttributeValueLengthLimit};
  std::uint32_t attribute_count_limit{kDefaultAttributeCountLimit};
  std::uint32_t event_count_limit{kDefaultEventCountLimit};
  std::uint32_t link_count_limit{kDefaultLinkCountLimit};
  std::uint32_t event_attribute_count_limit{kDefaultEventAttributeCountLimit};
  std::uint32_t link_attribute_count_limit{kDefaultLinkAttributeCountLimit};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
