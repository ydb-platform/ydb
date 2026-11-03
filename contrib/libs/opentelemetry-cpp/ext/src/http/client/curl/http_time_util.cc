// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "opentelemetry/ext/http/client/curl/http_time_util.h"

#include <cctype>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <limits>
#include <locale>
#include <sstream>
#include <string>

#include "opentelemetry/common/string_util.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace ext
{
namespace http
{
namespace client
{
namespace curl
{

bool HttpTimeUtil::ParseDelaySeconds(nostd::string_view value, std::chrono::seconds &delay) noexcept
{
  value = opentelemetry::common::StringUtil::Trim(value);
  if (value.empty())
  {
    return false;
  }

  std::chrono::seconds::rep result = 0;
  for (const char c : value)
  {
    if (!std::isdigit(static_cast<int>(static_cast<unsigned char>(c))))
    {
      return false;
    }
    auto digit = c - '0';
    if (result > ((std::numeric_limits<decltype(result)>::max)() - digit) / 10)
    {
      return false;
    }
    result = result * 10 + digit;
  }

  delay = std::chrono::seconds(result);
  return true;
}

bool HttpTimeUtil::ParseHttpDate(nostd::string_view value,
                                 std::chrono::system_clock::time_point &date)
{
  value = opentelemetry::common::StringUtil::Trim(value);
  std::string str(value.data(), value.size());
  std::tm tm = {};
  std::istringstream ss(str);
  ss.imbue(std::locale::classic());

  ss >> std::get_time(&tm, "%a, %d %b %Y %H:%M:%S");
  if (!ss.fail())
  {
    date = std::chrono::system_clock::from_time_t(PortableTimegm(&tm));
    return true;
  }

  ss.clear();
  ss.str(str);
  tm = {};
  ss >> std::get_time(&tm, "%A, %d-%b-%y %H:%M:%S");
  if (!ss.fail())
  {
    std::time_t now_t   = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    int current_year    = 1970 + static_cast<int>(now_t / 31556952);
    int full_year       = 1900 + tm.tm_year;
    int centuries_ahead = (full_year - current_year + 50) / 100;
    if (centuries_ahead > 0)
    {
      full_year -= 100 * centuries_ahead;
    }
    tm.tm_year = full_year - 1900;
    date       = std::chrono::system_clock::from_time_t(PortableTimegm(&tm));
    return true;
  }

  ss.clear();
  ss.str(str);
  tm = {};
  ss >> std::get_time(&tm, "%a %b %d %H:%M:%S %Y");
  if (!ss.fail())
  {
    date = std::chrono::system_clock::from_time_t(PortableTimegm(&tm));
    return true;
  }

  return false;
}

std::time_t HttpTimeUtil::PortableTimegm(std::tm *tm) noexcept
{
  int year  = tm->tm_year + 1900;
  int month = tm->tm_mon + 1;
  if (month <= 2)
  {
    year -= 1;
    month += 12;
  }
  int day = tm->tm_mday;
  int days =
      365 * year + year / 4 - year / 100 + year / 400 + (153 * (month - 3) + 2) / 5 + day - 719469;
  return static_cast<std::time_t>(days) * 86400 + tm->tm_hour * 3600 + tm->tm_min * 60 + tm->tm_sec;
}

}  // namespace curl
}  // namespace client
}  // namespace http
}  // namespace ext
OPENTELEMETRY_END_NAMESPACE
