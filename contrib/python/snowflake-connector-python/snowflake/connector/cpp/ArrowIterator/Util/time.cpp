//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#include "time.hpp"

namespace sf
{

namespace internal
{

int32_t getHourFromSeconds(int64_t seconds, int32_t scale)
{
  return seconds / powTenSB4[scale] / SECONDS_PER_HOUR;
}

int32_t getHourFromSeconds(int32_t seconds, int32_t scale)
{
  return seconds / powTenSB4[scale] / SECONDS_PER_HOUR;
}

int32_t getMinuteFromSeconds(int64_t seconds, int32_t scale)
{
  return seconds / powTenSB4[scale] % SECONDS_PER_HOUR / SECONDS_PER_MINUTE;
}

int32_t getMinuteFromSeconds(int32_t seconds, int32_t scale)
{
  return seconds / powTenSB4[scale] % SECONDS_PER_HOUR / SECONDS_PER_MINUTE;
}

int32_t getSecondFromSeconds(int64_t seconds, int32_t scale)
{
  return seconds / powTenSB4[scale] % SECONDS_PER_MINUTE;
}

int32_t getSecondFromSeconds(int32_t seconds, int32_t scale)
{
  return seconds / powTenSB4[scale] % SECONDS_PER_MINUTE;
}

int32_t getMicrosecondFromSeconds(int64_t seconds, int32_t scale)
{
  int32_t microsec = seconds % powTenSB4[scale];
  return scale > PYTHON_DATETIME_TIME_MICROSEC_DIGIT ? microsec /=
         powTenSB4[scale - PYTHON_DATETIME_TIME_MICROSEC_DIGIT] : microsec *=
         powTenSB4[PYTHON_DATETIME_TIME_MICROSEC_DIGIT - scale];
}

TimeSpec::TimeSpec(int64_t units, int32_t scale) {
      if (scale == 0) {
        seconds = units;
        microseconds = 0;
      } else if (scale == 6) {
        seconds = 0;
        microseconds = units;
      } else if (scale > 6) {
        seconds = 0;
        const int divider = internal::powTenSB4[scale - 6];
        if (units < 0) {
            units -= divider - 1;
        }
        microseconds = units / divider;
      } else {
        seconds = units / internal::powTenSB4[scale];
        int64_t fractions = std::abs(units % internal::powTenSB4[scale]);
        microseconds = fractions * internal::powTenSB4[6 - scale];
        if (units < 0) {
            microseconds = -microseconds;
        }
      }
}

}  // namespace internal
}  // namespace sf
