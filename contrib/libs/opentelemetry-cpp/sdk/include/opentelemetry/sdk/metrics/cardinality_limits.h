// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

/**
 * Cardinality limits per instrument type for a MetricReader.
 *
 * All fields default to kDefaultCardinalityLimit (2000), which is the
 * OpenTelemetry Metrics SDK specification default.
 *
 * To configure a reader-level limit, set the relevant fields and pass the
 * struct to MetricReader::SetCardinalityLimits().  The sdk_builder converts
 * the declarative CardinalityLimitsConfiguration (where 0 means "unset, use
 * default_limit") into a fully-resolved CardinalityLimits before calling
 * SetCardinalityLimits(), so SetCardinalityLimits() itself is a plain setter
 * with no sentinel logic.
 */
struct CardinalityLimits
{
  std::size_t default_limit              = 2000;
  std::size_t counter                    = 2000;
  std::size_t gauge                      = 2000;
  std::size_t histogram                  = 2000;
  std::size_t observable_counter         = 2000;
  std::size_t observable_gauge           = 2000;
  std::size_t observable_up_down_counter = 2000;
  std::size_t up_down_counter            = 2000;
};

/// The OpenTelemetry Metrics SDK spec default cardinality limit.
constexpr std::size_t kDefaultCardinalityLimit = 2000;

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
