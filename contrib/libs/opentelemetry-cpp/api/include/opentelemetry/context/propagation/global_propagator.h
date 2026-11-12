// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <mutex>

#include "opentelemetry/context/propagation/noop_propagator.h"

#include "opentelemetry/common/macros.h"
#include "opentelemetry/common/spin_lock_mutex.h"
#include "opentelemetry/nostd/shared_ptr.h"

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace context
{
namespace propagation
{

class TextMapPropagator;

/* Stores the singleton TextMapPropagator */

class OPENTELEMETRY_EXPORT GlobalTextMapPropagator
{
public:
  /**
   * Returns the singleton TextMapPropagator.
   *
   * By default, a no-op TextMapPropagator is returned. This will never return a
   * nullptr TextMapPropagator.
   */
  static nostd::shared_ptr<TextMapPropagator> GetGlobalPropagator() noexcept
  {
    std::lock_guard<common::SpinLockMutex> guard(GetLock());
    return nostd::shared_ptr<TextMapPropagator>(GetPropagator());
  }

  /**
   * Changes the singleton TextMapPropagator.
   *
   * Passing a nullptr TextMapPropagator installs a no-op TextMapPropagator, so
   * that GetGlobalPropagator() never returns a nullptr TextMapPropagator.
   */
  static void SetGlobalPropagator(const nostd::shared_ptr<TextMapPropagator> &prop) noexcept
  {
    std::lock_guard<common::SpinLockMutex> guard(GetLock());
    if (prop)
    {
      GetPropagator() = prop;
    }
    else
    {
      GetPropagator() = nostd::shared_ptr<TextMapPropagator>(new NoOpPropagator());
    }
  }

private:
  OPENTELEMETRY_API_SINGLETON static nostd::shared_ptr<TextMapPropagator> &GetPropagator() noexcept
  {
    static nostd::shared_ptr<TextMapPropagator> propagator(new NoOpPropagator());
    return propagator;
  }

  OPENTELEMETRY_API_SINGLETON static common::SpinLockMutex &GetLock() noexcept
  {
    static common::SpinLockMutex lock;
    return lock;
  }
};

}  // namespace propagation
}  // namespace context
OPENTELEMETRY_END_NAMESPACE
