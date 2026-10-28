// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <ctime>

#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace ext
{
namespace http
{
namespace client
{
namespace curl
{

/**
 * @brief Parsing helpers for RFC 7231 value formats (HTTP-date, section
 * 7.1.1.1, and delay-seconds, section 7.1.3) that appear in headers such
 * as Retry-After, Date, Last-Modified, and Expires.
 */
class HttpTimeUtil
{
public:
  // Parse a delay-seconds value (RFC 7231 section 7.1.3) into a duration.
  // Returns false on malformed input or overflow.
  static bool ParseDelaySeconds(nostd::string_view value, std::chrono::seconds &delay) noexcept;

  // Parse an HTTP-date value (RFC 7231 section 7.1.1.1) into a time point.
  // All three legal formats (IMF-fixdate, RFC 850, asctime) are accepted and
  // interpreted as UTC. Returns false if the value does not match any format.
  static bool ParseHttpDate(nostd::string_view value, std::chrono::system_clock::time_point &date);

private:
  // Portable, timezone-independent conversion of a UTC broken-down time to a
  // Unix time_t. Avoids mktime() (which uses local time) and platform-specific
  // timegm().
  static std::time_t PortableTimegm(std::tm *tm) noexcept;
};

}  // namespace curl
}  // namespace client
}  // namespace http
}  // namespace ext
OPENTELEMETRY_END_NAMESPACE
