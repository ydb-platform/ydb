#pragma once

#include <library/cpp/yt/error/error.h>

#include <string>

namespace NYT::NTzTypes {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
using TTZItem = std::pair<T, std::string_view>;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TTZItem<T> ParseTzValue(std::string_view tzString);

template <typename T>
std::string MakeTzString(T timeValue, std::string_view tzName);

template <typename T>
std::string_view MakeTzString(T timeValue, std::string_view tzName, char* buffer, size_t bufferSize);

void ValidateTzName(std::string_view tzName);

std::string_view GetTzName(int tzIndex);

int GetTzIndex(std::string_view tzName);

int GetMaxPossibleTzStringSize();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTzTypes

#define TIMEZONE_HELPER_INL_H_
#include "tz_types-inl.h"
#undef TIMEZONE_HELPER_INL_H_
