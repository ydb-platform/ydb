// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fxjs/fx_date_helpers.h"

#include <math.h>
#include <time.h>
#include <wctype.h>

#include <array>
#include <iterator>

#include "build/build_config.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_system.h"
#include "fpdfsdk/cpdfsdk_helpers.h"

namespace fxjs {
namespace {

constexpr std::array<uint16_t, 12> kDaysMonth = {
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334}};

constexpr std::array<uint16_t, 12> kLeapDaysMonth = {
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335}};

double Mod(double x, double y) {
  double r = fmod(x, y);
  if (r < 0)
    r += y;
  return r;
}

double GetLocalTZA() {
  if (!IsPDFSandboxPolicyEnabled(FPDF_POLICY_MACHINETIME_ACCESS))
    return 0;
  time_t t = 0;
  FXSYS_time(&t);
  FXSYS_localtime(&t);
#if BUILDFLAG(IS_WIN)
  // In gcc 'timezone' is a global variable declared in time.h. In VC++, that
  // variable was removed in VC++ 2015, with _get_timezone replacing it.
  long timezone = 0;
  _get_timezone(&timezone);
#endif
  return (double)(-(timezone * 1000));
}

int GetDaylightSavingTA(double d) {
  if (!IsPDFSandboxPolicyEnabled(FPDF_POLICY_MACHINETIME_ACCESS))
    return 0;
  time_t t = (time_t)(d / 1000);
  struct tm* tmp = FXSYS_localtime(&t);
  if (!tmp)
    return 0;
  if (tmp->tm_isdst > 0)
    // One hour.
    return (int)60 * 60 * 1000;
  return 0;
}

bool IsLeapYear(int year) {
  return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 != 0));
}

int DayFromYear(int y) {
  return (int)(365 * (y - 1970.0) + floor((y - 1969.0) / 4) -
               floor((y - 1901.0) / 100) + floor((y - 1601.0) / 400));
}

double TimeFromYear(int y) {
  return 86400000.0 * DayFromYear(y);
}

double TimeFromYearMonth(int y, int m) {
  const uint16_t month = IsLeapYear(y) ? kLeapDaysMonth[m] : kDaysMonth[m];
  return TimeFromYear(y) + static_cast<double>(month) * 86400000;
}

int Day(double t) {
  return static_cast<int>(floor(t / 86400000.0));
}

int YearFromTime(double t) {
  // estimate the time.
  int y = 1970 + static_cast<int>(t / (365.2425 * 86400000.0));
  if (TimeFromYear(y) <= t) {
    while (TimeFromYear(y + 1) <= t)
      y++;
  } else {
    while (TimeFromYear(y) > t)
      y--;
  }
  return y;
}

int DayWithinYear(double t) {
  int year = YearFromTime(t);
  int day = Day(t);
  return day - DayFromYear(year);
}

int MonthFromTime(double t) {
  // Check for negative |day| values and check for January.
  int day = DayWithinYear(t);
  if (day < 0)
    return -1;
  if (day < 31)
    return 0;

  if (IsLeapYear(YearFromTime(t)))
    --day;

  // Check for February onwards.
  static constexpr std::array<int, 11> kCumulativeDaysInMonths = {
      {59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365}};
  for (size_t i = 0; i < std::size(kCumulativeDaysInMonths); ++i) {
    if (day < kCumulativeDaysInMonths[i])
      return static_cast<int>(i) + 1;
  }

  return -1;
}

int DateFromTime(double t) {
  int day = DayWithinYear(t);
  int year = YearFromTime(t);
  int leap = IsLeapYear(year);
  int month = MonthFromTime(t);
  switch (month) {
    case 0:
      return day + 1;
    case 1:
      return day - 30;
    case 2:
      return day - 58 - leap;
    case 3:
      return day - 89 - leap;
    case 4:
      return day - 119 - leap;
    case 5:
      return day - 150 - leap;
    case 6:
      return day - 180 - leap;
    case 7:
      return day - 211 - leap;
    case 8:
      return day - 242 - leap;
    case 9:
      return day - 272 - leap;
    case 10:
      return day - 303 - leap;
    case 11:
      return day - 333 - leap;
    default:
      return 0;
  }
}

size_t FindSubWordLength(const WideString& str, size_t nStart) {
  pdfium::span<const wchar_t> data = str.span();
  size_t i = nStart;
  while (i < data.size() && iswalnum(data[i]))
    ++i;
  return i - nStart;
}

}  // namespace

const std::array<const char*, 12> kMonths = {{"Jan", "Feb", "Mar", "Apr", "May",
                                              "Jun", "Jul", "Aug", "Sep", "Oct",
                                              "Nov", "Dec"}};

const std::array<const char*, 12> kFullMonths = {
    {"January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"}};

static constexpr size_t KMonthAbbreviationLength = 3;  // Anything in |kMonths|.
static constexpr size_t kLongestFullMonthLength = 9;   // September

double FX_GetDateTime() {
  if (!IsPDFSandboxPolicyEnabled(FPDF_POLICY_MACHINETIME_ACCESS))
    return 0;

  time_t t = FXSYS_time(nullptr);
  struct tm* pTm = FXSYS_localtime(&t);
  double t1 = TimeFromYear(pTm->tm_year + 1900);
  return t1 + pTm->tm_yday * 86400000.0 + pTm->tm_hour * 3600000.0 +
         pTm->tm_min * 60000.0 + pTm->tm_sec * 1000.0;
}

int FX_GetYearFromTime(double dt) {
  return YearFromTime(dt);
}

int FX_GetMonthFromTime(double dt) {
  return MonthFromTime(dt);
}

int FX_GetDayFromTime(double dt) {
  return DateFromTime(dt);
}

int FX_GetHourFromTime(double dt) {
  return (int)Mod(floor(dt / (60 * 60 * 1000)), 24);
}

int FX_GetMinFromTime(double dt) {
  return (int)Mod(floor(dt / (60 * 1000)), 60);
}

int FX_GetSecFromTime(double dt) {
  return (int)Mod(floor(dt / 1000), 60);
}

bool FX_IsValidMonth(int m) {
  return m >= 1 && m <= 12;
}

// TODO(thestig): Should this take the month into consideration?
bool FX_IsValidDay(int d) {
  return d >= 1 && d <= 31;
}

// TODO(thestig): Should 24 be allowed? Similarly, 60 for minutes and seconds.
bool FX_IsValid24Hour(int h) {
  return h >= 0 && h <= 24;
}

bool FX_IsValidMinute(int m) {
  return m >= 0 && m <= 60;
}

bool FX_IsValidSecond(int s) {
  return s >= 0 && s <= 60;
}

double FX_LocalTime(double d) {
  return d + GetLocalTZA() + GetDaylightSavingTA(d);
}

double FX_MakeDay(int nYear, int nMonth, int nDate) {
  double y = static_cast<double>(nYear);
  double m = static_cast<double>(nMonth);
  double dt = static_cast<double>(nDate);
  double ym = y + floor(m / 12);
  double mn = Mod(m, 12);
  double t = TimeFromYearMonth(static_cast<int>(ym), static_cast<int>(mn));
  if (YearFromTime(t) != ym || MonthFromTime(t) != mn || DateFromTime(t) != 1)
    return nan("");

  return Day(t) + dt - 1;
}

double FX_MakeTime(int nHour, int nMin, int nSec, int nMs) {
  double h = static_cast<double>(nHour);
  double m = static_cast<double>(nMin);
  double s = static_cast<double>(nSec);
  double milli = static_cast<double>(nMs);
  return h * 3600000 + m * 60000 + s * 1000 + milli;
}

double FX_MakeDate(double day, double time) {
  if (!isfinite(day) || !isfinite(time))
    return nan("");

  return day * 86400000 + time;
}

int FX_ParseStringInteger(const WideString& str,
                          size_t nStart,
                          size_t* pSkip,
                          size_t nMaxStep) {
  int nRet = 0;
  size_t nSkip = 0;
  for (size_t i = nStart; i < str.GetLength(); ++i) {
    if (i - nStart > 10)
      break;

    wchar_t c = str[i];
    if (!FXSYS_IsDecimalDigit(c))
      break;

    nRet = nRet * 10 + FXSYS_DecimalCharToInt(c);
    ++nSkip;
    if (nSkip >= nMaxStep)
      break;
  }

  *pSkip = nSkip;
  return nRet;
}

ConversionStatus FX_ParseDateUsingFormat(const WideString& value,
                                         const WideString& format,
                                         double* result) {
  double dt = FX_GetDateTime();
  if (format.IsEmpty() || value.IsEmpty()) {
    *result = dt;
    return ConversionStatus::kSuccess;
  }

  int nYear = FX_GetYearFromTime(dt);
  int nMonth = FX_GetMonthFromTime(dt) + 1;
  int nDay = FX_GetDayFromTime(dt);
  int nHour = FX_GetHourFromTime(dt);
  int nMin = FX_GetMinFromTime(dt);
  int nSec = FX_GetSecFromTime(dt);
  int nYearSub = 99;  // nYear - 2000;
  bool bPm = false;
  bool bExit = false;
  bool bBadFormat = false;
  size_t i = 0;
  size_t j = 0;

  while (i < format.GetLength()) {
    if (bExit)
      break;

    wchar_t c = format[i];
    switch (c) {
      case ':':
      case '.':
      case '-':
      case '\\':
      case '/':
        i++;
        j++;
        break;

      case 'y':
      case 'm':
      case 'd':
      case 'H':
      case 'h':
      case 'M':
      case 's':
      case 't': {
        size_t oldj = j;
        size_t nSkip = 0;
        size_t remaining = format.GetLength() - i - 1;

        if (remaining == 0 || format[i + 1] != c) {
          switch (c) {
            case 'y':
              i++;
              j++;
              break;
            case 'm':
              nMonth = FX_ParseStringInteger(value, j, &nSkip, 2);
              i++;
              j += nSkip;
              break;
            case 'd':
              nDay = FX_ParseStringInteger(value, j, &nSkip, 2);
              i++;
              j += nSkip;
              break;
            case 'H':
              nHour = FX_ParseStringInteger(value, j, &nSkip, 2);
              i++;
              j += nSkip;
              break;
            case 'h':
              nHour = FX_ParseStringInteger(value, j, &nSkip, 2);
              i++;
              j += nSkip;
              break;
            case 'M':
              nMin = FX_ParseStringInteger(value, j, &nSkip, 2);
              i++;
              j += nSkip;
              break;
            case 's':
              nSec = FX_ParseStringInteger(value, j, &nSkip, 2);
              i++;
              j += nSkip;
              break;
            case 't':
              bPm = (j < value.GetLength() && value[j] == 'p');
              i++;
              j++;
              break;
          }
        } else if (remaining == 1 || format[i + 2] != c) {
          switch (c) {
            case 'y':
              nYear = FX_ParseStringInteger(value, j, &nSkip, 2);
              i += 2;
              j += nSkip;
              break;
            case 'm':
              nMonth = FX_ParseStringInteger(value, j, &nSkip, 2);
              i += 2;
              j += nSkip;
              break;
            case 'd':
              nDay = FX_ParseStringInteger(value, j, &nSkip, 2);
              i += 2;
              j += nSkip;
              break;
            case 'H':
              nHour = FX_ParseStringInteger(value, j, &nSkip, 2);
              i += 2;
              j += nSkip;
              break;
            case 'h':
              nHour = FX_ParseStringInteger(value, j, &nSkip, 2);
              i += 2;
              j += nSkip;
              break;
            case 'M':
              nMin = FX_ParseStringInteger(value, j, &nSkip, 2);
              i += 2;
              j += nSkip;
              break;
            case 's':
              nSec = FX_ParseStringInteger(value, j, &nSkip, 2);
              i += 2;
              j += nSkip;
              break;
            case 't':
              bPm = (j + 1 < value.GetLength() && value[j] == 'p' &&
                     value[j + 1] == 'm');
              i += 2;
              j += 2;
              break;
          }
        } else if (remaining == 2 || format[i + 3] != c) {
          switch (c) {
            case 'm': {
              bool bFind = false;
              nSkip = FindSubWordLength(value, j);
              if (nSkip == KMonthAbbreviationLength) {
                WideString sMonth = value.Substr(j, KMonthAbbreviationLength);
                for (size_t m = 0; m < std::size(kMonths); ++m) {
                  if (sMonth.EqualsASCIINoCase(kMonths[m])) {
                    nMonth = static_cast<int>(m) + 1;
                    i += 3;
                    j += nSkip;
                    bFind = true;
                    break;
                  }
                }
              }

              if (!bFind) {
                nMonth = FX_ParseStringInteger(value, j, &nSkip, 3);
                i += 3;
                j += nSkip;
              }
            } break;
            case 'y':
              break;
            default:
              i += 3;
              j += 3;
              break;
          }
        } else if (remaining == 3 || format[i + 4] != c) {
          switch (c) {
            case 'y':
              nYear = FX_ParseStringInteger(value, j, &nSkip, 4);
              j += nSkip;
              i += 4;
              break;
            case 'm': {
              bool bFind = false;
              nSkip = FindSubWordLength(value, j);
              if (nSkip <= kLongestFullMonthLength) {
                WideString sMonth = value.Substr(j, nSkip);
                sMonth.MakeLower();
                for (size_t m = 0; m < std::size(kFullMonths); ++m) {
                  auto sFullMonths = WideString::FromASCII(kFullMonths[m]);
                  sFullMonths.MakeLower();
                  if (sFullMonths.Contains(sMonth.AsStringView())) {
                    nMonth = static_cast<int>(m) + 1;
                    i += 4;
                    j += nSkip;
                    bFind = true;
                    break;
                  }
                }
              }
              if (!bFind) {
                nMonth = FX_ParseStringInteger(value, j, &nSkip, 4);
                i += 4;
                j += nSkip;
              }
            } break;
            default:
              i += 4;
              j += 4;
              break;
          }
        } else {
          if (j >= value.GetLength() || format[i] != value[j]) {
            bBadFormat = true;
            bExit = true;
          }
          i++;
          j++;
        }

        if (oldj == j) {
          bBadFormat = true;
          bExit = true;
        }
        break;
      }

      default:
        if (value.GetLength() <= j) {
          bExit = true;
        } else if (format[i] != value[j]) {
          bBadFormat = true;
          bExit = true;
        }

        i++;
        j++;
        break;
    }
  }

  if (bBadFormat)
    return ConversionStatus::kBadFormat;

  if (bPm)
    nHour += 12;

  if (nYear >= 0 && nYear <= nYearSub)
    nYear += 2000;

  if (!FX_IsValidMonth(nMonth) || !FX_IsValidDay(nDay) ||
      !FX_IsValid24Hour(nHour) || !FX_IsValidMinute(nMin) ||
      !FX_IsValidSecond(nSec)) {
    return ConversionStatus::kBadDate;
  }

  dt = FX_MakeDate(FX_MakeDay(nYear, nMonth - 1, nDay),
                   FX_MakeTime(nHour, nMin, nSec, 0));
  if (isnan(dt))
    return ConversionStatus::kBadDate;

  *result = dt;
  return ConversionStatus::kSuccess;
}

}  // namespace fxjs
