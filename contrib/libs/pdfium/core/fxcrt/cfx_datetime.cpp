// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/cfx_datetime.h"

#include "build/build_config.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/span.h"

namespace {

constexpr uint8_t kDaysPerMonth[12] = {31, 28, 31, 30, 31, 30,
                                       31, 31, 30, 31, 30, 31};
constexpr uint8_t kDaysPerLeapMonth[12] = {31, 29, 31, 30, 31, 30,
                                           31, 31, 30, 31, 30, 31};
constexpr int32_t kDaysBeforeMonth[12] = {0,   31,  59,  90,  120, 151,
                                          181, 212, 243, 273, 304, 334};
constexpr int32_t kDaysBeforeLeapMonth[12] = {0,   31,  60,  91,  121, 152,
                                              182, 213, 244, 274, 305, 335};
constexpr int32_t kDaysPerYear = 365;
constexpr int32_t kDaysPerLeapYear = 366;

int32_t DaysBeforeMonthInYear(int32_t iYear, uint8_t iMonth) {
  DCHECK(iYear != 0);
  pdfium::span<const int32_t> p = FX_IsLeapYear(iYear)
                                      ? pdfium::make_span(kDaysBeforeLeapMonth)
                                      : pdfium::make_span(kDaysBeforeMonth);
  // Note: iMonth is one-based.
  return p[iMonth - 1];
}

int32_t DaysInYear(int32_t iYear) {
  DCHECK(iYear != 0);
  return FX_IsLeapYear(iYear) ? kDaysPerLeapYear : kDaysPerYear;
}

int64_t DateToDays(int32_t iYear,
                   uint8_t iMonth,
                   uint8_t iDay,
                   bool bIncludeThisDay) {
  DCHECK(iYear != 0);
  DCHECK(iMonth >= 1);
  DCHECK(iMonth <= 12);
  DCHECK(iDay >= 1);
  DCHECK(iDay <= FX_DaysInMonth(iYear, iMonth));

  int64_t iDays = DaysBeforeMonthInYear(iYear, iMonth);
  iDays += iDay;
  if (!bIncludeThisDay)
    iDays--;

  if (iYear > 0) {
    iYear--;
  } else {
    iDays -= DaysInYear(iYear);
    iYear++;
  }
  return iDays + static_cast<int64_t>(iYear) * 365 + iYear / 4 - iYear / 100 +
         iYear / 400;
}

}  // namespace

uint8_t FX_DaysInMonth(int32_t iYear, uint8_t iMonth) {
  DCHECK(iYear != 0);
  pdfium::span<const uint8_t> p = FX_IsLeapYear(iYear)
                                      ? pdfium::make_span(kDaysPerLeapMonth)
                                      : pdfium::make_span(kDaysPerMonth);
  // Note: iMonth is one-based.
  return p[iMonth - 1];
}

bool FX_IsLeapYear(int32_t iYear) {
  DCHECK(iYear != 0);
  return ((iYear % 4) == 0 && (iYear % 100) != 0) || (iYear % 400) == 0;
}

// static
CFX_DateTime CFX_DateTime::Now() {
  time_t t = FXSYS_time(nullptr);
  struct tm* pTime = FXSYS_localtime(&t);
  return CFX_DateTime(
      pTime->tm_year + 1900, static_cast<uint8_t>(pTime->tm_mon + 1),
      static_cast<uint8_t>(pTime->tm_mday),
      static_cast<uint8_t>(pTime->tm_hour), static_cast<uint8_t>(pTime->tm_min),
      static_cast<uint8_t>(pTime->tm_sec), 0);
}

int32_t CFX_DateTime::GetDayOfWeek() const {
  int32_t v = static_cast<int32_t>(DateToDays(year_, month_, day_, true) % 7);
  if (v < 0)
    v += 7;
  return v;
}

bool CFX_DateTime::operator==(const CFX_DateTime& other) const {
  return year_ == other.year_ && month_ == other.month_ && day_ == other.day_ &&
         hour_ == other.hour_ && minute_ == other.minute_ &&
         second_ == other.second_ && millisecond_ == other.millisecond_;
}
