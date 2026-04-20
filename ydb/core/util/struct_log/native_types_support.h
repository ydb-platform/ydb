#pragma once

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <charconv>
#include <cstdint>
#include <cstring>
#include <string>

#include <type_traits>
#include <vector>

namespace NKikimr::NStructLog {

using TBinaryData = std::vector<std::uint8_t>;

template<typename T>
struct TNativeTypeSupport : public std::false_type{};

template <typename T, typename V = typename std::enable_if< std::is_pod_v<T> >::type>
struct TNativePlainTypeSupport : public std::true_type
{
    static inline void Serialize(const T& value, TBinaryData& data) {
        auto from = reinterpret_cast<const std::uint8_t*>(&value);
        auto oldSize = data.size();
        data.resize(oldSize + sizeof(T));
        auto to = data.data() + oldSize;
        std::memcpy(to, from, sizeof(T));
    }

    static bool Deserialize(T& value, const void* data, std::size_t length) {
        if (sizeof(T) != length) {
            return false;
        }
        value =  *(reinterpret_cast<const T*>(data));
        return true;
    }

    static TString ToString(const T& value) {
        return std::to_string(value);
    }

    static void AppendToString(const T& value, TStringBuilder& stringBuffer) {
        char buffer[64];
        auto result = std::to_chars(buffer, buffer + 64, value);
        stringBuffer.append(buffer, result.ptr - buffer);
    }
};

template<> struct TNativeTypeSupport<i8> : public TNativePlainTypeSupport<i8>{};
template<> struct TNativeTypeSupport<ui8> : public TNativePlainTypeSupport<ui8>{};
template<> struct TNativeTypeSupport<i16> : public TNativePlainTypeSupport<i16>{};
template<> struct TNativeTypeSupport<ui16> : public TNativePlainTypeSupport<ui16>{};
template<> struct TNativeTypeSupport<i32> : public TNativePlainTypeSupport<i32>{};
template<> struct TNativeTypeSupport<ui32> : public TNativePlainTypeSupport<ui32>{};
template<> struct TNativeTypeSupport<i64> : public TNativePlainTypeSupport<i64>{};
template<> struct TNativeTypeSupport<ui64> : public TNativePlainTypeSupport<ui64>{};

template<> struct TNativeTypeSupport<bool> : public TNativePlainTypeSupport<bool>
{
    static TString ToString(const bool& value) {
        return value?"true":"false";
    }

    static void AppendToString(const bool& value, TStringBuilder& stringBuffer) {
        stringBuffer.append(value ? "true" : "false");
    }
};

template<> struct TNativeTypeSupport<TString> : public std::true_type
{
    using TLength = std::size_t;

    static void Serialize(const TString& value, TBinaryData& data) {
        // Write size
        TLength size = value.size();
        TNativePlainTypeSupport<TLength>::Serialize(size, data);

        // Write contents
        auto oldSize = data.size();
        data.resize(oldSize + size);
        auto to = data.data() + oldSize;
        std::memcpy(to, value.c_str(), size);
    }

    static bool Deserialize(TString& value, const void* data, std::size_t length) {
        if (sizeof (TLength) > length) {
            return false;
        }

        TLength stringLength =  *(reinterpret_cast<const TLength*>(data));
        if (sizeof(TLength) + stringLength != length) {
            return false;
        }

        auto charPtr = reinterpret_cast<const char*>(data);
        value = TString(charPtr + sizeof(stringLength), stringLength);
        return true;
    }

    static TString ToString(const TString& value) {
        return value;
    }

    static void AppendToString(const TString& value, TStringBuilder& stringBuffer) {
        stringBuffer.append(value);
    }
};

template <typename T, typename V = typename std::enable_if< std::is_floating_point_v<T> >::type>
struct TFloatingTypeSupport : public TNativePlainTypeSupport<T>
{
    static TString ToString(const T& value) {
        return std::to_string(value);
    }

    static void AppendToString(const T& value, TStringBuilder& stringBuffer) {
        stringBuffer.append(ToString<T>(value));
    }
};

template<> struct TNativeTypeSupport<float> : public TFloatingTypeSupport<float>{};
template<> struct TNativeTypeSupport<double> : public TFloatingTypeSupport<double>{};
template<> struct TNativeTypeSupport<long double> : public TFloatingTypeSupport<long double>{};

}
