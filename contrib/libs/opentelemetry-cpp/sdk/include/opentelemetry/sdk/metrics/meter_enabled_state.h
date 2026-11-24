// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

/**
 * A Meter's enabled flag, shared with every instrument it creates so a configurator update
 * reaches instruments that already exist. Relaxed ordering: no dependent data, only visibility.
 */
class MeterEnabledState
{
public:
  explicit MeterEnabledState(bool enabled = true) noexcept : enabled_(enabled) {}

  ~MeterEnabledState()                                    = default;
  MeterEnabledState(const MeterEnabledState &)            = delete;
  MeterEnabledState(MeterEnabledState &&)                 = delete;
  MeterEnabledState &operator=(const MeterEnabledState &) = delete;
  MeterEnabledState &operator=(MeterEnabledState &&)      = delete;

  bool IsEnabled() const noexcept { return enabled_.load(std::memory_order_relaxed); }

  void SetEnabled(bool enabled) noexcept { enabled_.store(enabled, std::memory_order_relaxed); }

private:
  std::atomic<bool> enabled_;
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
