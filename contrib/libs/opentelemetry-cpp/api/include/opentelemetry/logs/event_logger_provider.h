// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace logs
{

class EventLogger;
class Logger;

#if OPENTELEMETRY_ABI_VERSION_NO < 2
#  if defined(_MSC_VER)
#    pragma warning(push)
#    pragma warning(disable : 4996)
#  elif defined(__GNUC__) && !defined(__clang__) && !defined(__apple_build_version__)
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#  elif defined(__clang__) || defined(__apple_build_version__)
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wdeprecated-declarations"
#  endif

/**
 * Creates new EventLogger instances.
 */
class OPENTELEMETRY_DEPRECATED EventLoggerProvider
{
public:
  virtual ~EventLoggerProvider() = default;

  /**
   * Creates a named EventLogger instance.
   *
   */

  virtual nostd::shared_ptr<EventLogger> CreateEventLogger(
      nostd::shared_ptr<Logger> delegate_logger,
      nostd::string_view event_domain) noexcept = 0;
};

#  if defined(_MSC_VER)
#    pragma warning(pop)
#  elif defined(__GNUC__) && !defined(__clang__) && !defined(__apple_build_version__)
#    pragma GCC diagnostic pop
#  elif defined(__clang__) || defined(__apple_build_version__)
#    pragma clang diagnostic pop
#  endif
#endif
}  // namespace logs
OPENTELEMETRY_END_NAMESPACE
