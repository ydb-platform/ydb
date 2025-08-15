#ifndef TIMEZONE_HELPER_INL_H_
#error "Direct inclusion of this file is not allowed, include timezone_helper.h.h"
#include "tz_types.h"
#endif

#include <library/cpp/type_info/tz/tz.h>

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
        MaxPossibleTzSize_ = 0;
        for (int index = 0; index < std::ssize(timezones); ++index) {
            TzBuffer_[index] = std::string(timezones[index]);
            if (!timezones[index].empty()) {
                MaxPossibleTzSize_ = std::max(MaxPossibleTzSize_, static_cast<int>(std::ssize(timezones[index])));
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

    int GetMaxPossibleTzSize() const
    {
        return MaxPossibleTzSize_;
    }

private:
    THashMap<std::string_view, int> NameToTimezoneIndex_;
    std::vector<std::string> TzBuffer_;
    int MaxPossibleTzSize_;
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
T ParseTimestampFromTzString(std::string_view tzString, bool isSigned)
{
    constexpr size_t byteCount = sizeof(T);
    if (byteCount > tzString.size()) {
        THROW_ERROR_EXCEPTION("The number of bytes in the tz time string is not sufficient")
            << TErrorAttribute("actual_byte_coun", tzString.size())
            << TErrorAttribute("min_expected_bytes_count", byteCount);
    }
    auto byteDate = std::string(std::make_reverse_iterator(tzString.begin() + byteCount),
        std::make_reverse_iterator(tzString.begin()));
    auto value = ReadUnaligned<T>(byteDate.data());
    return isSigned ? FlipHighestBit(value) : value;
}

template <typename T>
T ParseTimestampFromTzString(std::string_view tzString)
{
    if constexpr (std::is_signed_v<T>) {
        return ParseTimestampFromTzString<T>(tzString, true);
    } else {
        return ParseTimestampFromTzString<T>(tzString, false);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TTZItem<T> ParseTzValue(std::string_view tzString)
{
    return TTZItem<T>({ParseTimestampFromTzString<T>(tzString), ParseTzNameFromTzString(tzString, sizeof(T))});
}

inline void ValidateTzName(std::string_view tzName)
{
    Singleton<TTzRegistry>()->ValidateTzName(tzName);
}

template <typename T>
std::string_view MakeTzString(T timeValue, std::string_view tzName, char* buffer, size_t bufferSize)
{
    YT_VERIFY(bufferSize >= sizeof(T) + tzName.size());

    auto converted = timeValue;
    if constexpr (std::is_signed_v<T>) {
        converted = FlipHighestBit<T>(timeValue);
    }
    // Make big-endian representation of numeric value.
    auto* bytes = reinterpret_cast<unsigned char*>(&converted);
    std::reverse(bytes, bytes + sizeof(T));

    std::memcpy(buffer, &converted, sizeof(T));
    std::memcpy(buffer + sizeof(T), tzName.data(), tzName.size());
    return std::string_view(buffer, sizeof(T) + tzName.size());
}

template <typename T>
std::string MakeTzString(T timeValue, std::string_view tzName)
{
    std::string buffer;
    buffer.resize(sizeof(T) + tzName.size());
    Y_UNUSED(MakeTzString<T>(timeValue, tzName, buffer.data(), buffer.size()));
    return buffer;
}

inline std::string_view GetTzName(int tzIndex)
{
    return Singleton<TTzRegistry>()->GetTzName(tzIndex);
}

inline int GetTzIndex(std::string_view tzName)
{
    return Singleton<TTzRegistry>()->GetTzIndex(tzName);
}

inline int GetMaxPossibleTzStringSize()
{
    return Singleton<TTzRegistry>()->GetMaxPossibleTzSize() + sizeof(i64);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTzTypes
