// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CFX_DATETIME_H_
#define CORE_FXCRT_CFX_DATETIME_H_

#include <stdint.h>

bool FX_IsLeapYear(int32_t iYear);
uint8_t FX_DaysInMonth(int32_t iYear, uint8_t iMonth);

class CFX_DateTime {
 public:
  static CFX_DateTime Now();  // Accurate to seconds, subject to test overrides.

  CFX_DateTime() = default;
  CFX_DateTime(int32_t year,
               uint8_t month,
               uint8_t day,
               uint8_t hour,
               uint8_t minute,
               uint8_t second,
               uint16_t millisecond)
      : year_(year),
        month_(month),
        day_(day),
        hour_(hour),
        minute_(minute),
        second_(second),
        millisecond_(millisecond) {}

  void Reset() {
    year_ = 0;
    month_ = 0;
    day_ = 0;
    hour_ = 0;
    minute_ = 0;
    second_ = 0;
    millisecond_ = 0;
  }

  bool IsSet() const {
    return year_ != 0 || month_ != 0 || day_ != 0 || hour_ != 0 ||
           minute_ != 0 || second_ != 0 || millisecond_ != 0;
  }

  void SetDate(int32_t year, uint8_t month, uint8_t day) {
    year_ = year;
    month_ = month;
    day_ = day;
  }

  void SetTime(uint8_t hour,
               uint8_t minute,
               uint8_t second,
               uint16_t millisecond) {
    hour_ = hour;
    minute_ = minute;
    second_ = second;
    millisecond_ = millisecond;
  }

  int32_t GetYear() const { return year_; }
  uint8_t GetMonth() const { return month_; }
  uint8_t GetDay() const { return day_; }
  uint8_t GetHour() const { return hour_; }
  uint8_t GetMinute() const { return minute_; }
  uint8_t GetSecond() const { return second_; }
  uint16_t GetMillisecond() const { return millisecond_; }
  int32_t GetDayOfWeek() const;

  bool operator==(const CFX_DateTime& other) const;

 private:
  int32_t year_ = 0;
  uint8_t month_ = 0;
  uint8_t day_ = 0;
  uint8_t hour_ = 0;
  uint8_t minute_ = 0;
  uint8_t second_ = 0;
  uint16_t millisecond_ = 0;
};

#endif  // CORE_FXCRT_CFX_DATETIME_H_
