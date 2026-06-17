#ifndef TIMEZONE_HELPER_INL_H_
#error "Direct inclusion of this file is not allowed, include timezone_helper.h.h"
#include "tz_types.h"
#endif

#include <library/cpp/type_info/tz/tz.h>

#include <util/system/byteorder.h>

namespace NYT::NTzTypes {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TTzRegistry
{
public:
    TTzRegistry()
    {
        const auto timezones = NTi::GetTimezones();
        TzBuffer_.resize(timezones.size());
        for (int index = 0; index < std::ssize(timezones); ++index) {
            TzBuffer_[index] = std::string(timezones[index]);
            if (!timezones[index].empty()) {
                NameToTimezoneIndex_[std::string_view(TzBuffer_[index])] = index;
            }
        }
    }

    int GetTzIndex(std::string_view timezoneName) const
    {
        auto result = NameToTimezoneIndex_.find(timezoneName);
        if (result == NameToTimezoneIndex_.end()) {
            THROW_ERROR_EXCEPTION("Invalid timezone name")
                << TErrorAttribute("timezone_name", timezoneName);
        }
        return result->second;
    }

    std::string_view GetTzName(int index) const
    {
        if (index < 0 || index >= std::ssize(TzBuffer_)) {
            THROW_ERROR_EXCEPTION("Invalid timezone index, value %v not in range [0:%v]",
                index,
                std::ssize(TzBuffer_) - 1)
                << TErrorAttribute("timezone_index", index);
        }
        if (TzBuffer_[index].empty()) {
            THROW_ERROR_EXCEPTION("Index of an empty timezone is not valid")
                << TErrorAttribute("timezone_index", index);
        }
        return TzBuffer_[index];
    }

    void ValidateTzName(std::string_view timezoneName) const
    {
        auto result = NameToTimezoneIndex_.find(timezoneName);
        if (result == NameToTimezoneIndex_.end()) {
            THROW_ERROR_EXCEPTION("Invalid timezone name")
                << TErrorAttribute("timezone_name", timezoneName);
        }
    }

private:
    THashMap<std::string_view, int> NameToTimezoneIndex_;
    std::vector<std::string> TzBuffer_;
};

////////////////////////////////////////////////////////////////////////////////

inline std::string_view ParseTzNameFromTzString(std::string_view tzString, int byteCount)
{
    YT_VERIFY(byteCount <= std::ssize(tzString));
    return tzString.substr(byteCount);
}

template <typename T>
T FlipHighestBit(T value)
{
    constexpr size_t bitCount = sizeof(T) * 8;
    return value ^ (T(1) << (bitCount - 1));
}

template <typename T>
T ParsePresorted(std::string_view from)
{
    auto value = ReadUnaligned<T>(from.data());
    value = InetToHost(value);

    if constexpr (std::is_signed_v<T>) {
        return FlipHighestBit(value);
    } else {
        return value;
    }
}

// Inplace
template <typename T>
void MakePresorted(T* number)
{
    if constexpr (std::is_signed_v<T>) {
        *number = FlipHighestBit<T>(*number);
    }
    *number = HostToInet(*number);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

constexpr int GetMaxPossibleTzStringSize()
{
    return sizeof(i64) + sizeof(ui16);
}

template <typename T>
constexpr int GetTzStringSize() {
    return sizeof(T) + sizeof(ui16);
}

template <typename T>
TTZItem<T> ParseTzValue(std::string_view from)
{
    constexpr size_t expectedLength = sizeof(T) + sizeof(ui16);
    if (expectedLength != from.size()) {
        THROW_ERROR_EXCEPTION("Invalid length of TzType<%v>: %v", TypeName<T>(), std::ssize(from))
            << TErrorAttribute("actual_length", std::ssize(from))
            << TErrorAttribute("expected_length", expectedLength);
    }
    return TTZItem<T>({ParsePresorted<T>(from), ParsePresorted<ui16>(from.substr(sizeof(T)))});
}

template <typename T>
std::string_view MakeTzString(T timeValue, ui16 tzId, char* buffer, size_t bufferSize)
{
    YT_VERIFY(bufferSize >= sizeof(timeValue) + sizeof(tzId));

    MakePresorted(&timeValue);
    std::memcpy(buffer, &timeValue, sizeof(timeValue));

    MakePresorted(&tzId);
    std::memcpy(buffer + sizeof(timeValue), &tzId, sizeof(tzId));

    return std::string_view(buffer, sizeof(timeValue) + sizeof(tzId));
}

template <typename T>
std::string MakeTzString(T timeValue, ui16 tzId)
{
    std::string buffer;
    buffer.resize(GetTzStringSize<T>());
    MakeTzString<T>(timeValue, tzId, buffer.data(), buffer.size());
    return buffer;
}

template <typename T>
std::string MakeTzString(T timeValue, std::string_view tzName)
{
    return MakeTzString(timeValue, GetTzIndex(tzName));
}

template <typename T>
std::string_view MakeTzString(T timeValue, std::string_view tzName, char* buffer, size_t bufferSize)
{
    return MakeTzString(timeValue, GetTzIndex(tzName), buffer, bufferSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTzTypes
