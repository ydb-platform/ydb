// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/logs/log_record.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace resource
{
class Resource;
}  // namespace resource

namespace instrumentationscope
{
class InstrumentationScope;
}  // namespace instrumentationscope

namespace logs
{

struct LogRecordLimits;

/**
 * Maintains a representation of a log in a format that can be processed by a recorder.
 *
 * This class is thread-compatible.
 */

class Recordable : public opentelemetry::logs::LogRecord
{
public:
  /**
   * Set Resource of this log
   * @param resource the resource to set
   */
  virtual void SetResource(const opentelemetry::sdk::resource::Resource &resource) noexcept = 0;

  /**
   * Set instrumentation_scope for this log.
   * @param instrumentation_scope the instrumentation scope to set
   */
  virtual void SetInstrumentationScope(
      const opentelemetry::sdk::instrumentationscope::InstrumentationScope
          &instrumentation_scope) noexcept = 0;

  /**
   * Apply attribute count and value length limits to this log. The default
   * implementation is a no-op; concrete recordables that wish to enforce
   * limits override this and copy the supplied limits before any
   * SetAttribute call is observed, so the caller does not need to keep the
   * supplied object alive.
   */
  virtual void SetLogRecordLimits(const LogRecordLimits & /* limits */) noexcept {}
};

}  // namespace logs
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
