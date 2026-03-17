//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_UTIL_TIME_HPP
#define PC_UTIL_TIME_HPP

#include <string>
#include "Python/Common.hpp"

#ifdef _WIN32
#include <algorithm>
#endif

namespace sf
{

namespace internal
{
/** python datetime.time has only support 6 bit precision, which is formated
 * double in this file */

constexpr int SECONDS_PER_MINUTE = 60;
constexpr int MINUTES_PER_HOUR = 60;
constexpr int HOURS_PER_DAY = 24;
constexpr int SECONDS_PER_HOUR = MINUTES_PER_HOUR * SECONDS_PER_MINUTE;

constexpr int32_t PYTHON_DATETIME_TIME_MICROSEC_DIGIT = 6;
constexpr int32_t NANOSEC_DIGIT = 9;

constexpr int powTenSB4[]{1,      10,      100,      1000,      10000,
                          100000, 1000000, 10000000, 100000000, 1000000000};

/** if we use c++17 some day in the future, we can use 'constexpr
 * std::string_view sv = "hello, world";' to replace this */
const std::string FIELD_NAME_EPOCH = "epoch";
const std::string FIELD_NAME_TIME_ZONE = "timezone";
const std::string FIELD_NAME_FRACTION = "fraction";

/** pow10Int means the return value should be int type. So user needs to take
 * care not to cause int overflow by a huge parameter n.
 * And since we are using -stdc++11 now, we can only use constexpr in this way.
 * When we move to -stdc++14, we can have a more elegant way, e.g., loop. */
constexpr int pow10Int(int n)
{
  return n == 0 ? 1 : 10 * pow10Int(n - 1);
}

struct TimeSpec {
    int64_t seconds;
    int64_t microseconds;
    TimeSpec(int64_t units, int32_t scale);
};

// TODO: I think we can just keep int64_t version, since we can call the
//  function with implicit conversion from int32 to int64
int32_t getHourFromSeconds(int64_t seconds, int32_t scale);

int32_t getMinuteFromSeconds(int64_t seconds, int32_t scale);

int32_t getSecondFromSeconds(int64_t seconds, int32_t scale);

int32_t getMicrosecondFromSeconds(int64_t seconds, int32_t scale);

int32_t getHourFromSeconds(int32_t seconds, int32_t scale);

int32_t getMinuteFromSeconds(int32_t seconds, int32_t scale);

int32_t getSecondFromSeconds(int32_t seconds, int32_t scale);

int32_t getMicrosecondFromSeconds(int32_t seconds, int32_t scale);

}  // namespace internal
}  // namespace sf

#endif  // PC_UTIL_TIME_HPP
