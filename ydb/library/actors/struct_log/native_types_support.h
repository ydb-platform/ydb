#pragma once

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <vector>

namespace NActors::NStructuredLog {

using TBinaryData = std::vector<std::uint8_t>;

template <typename T>
struct TNativeTypeSupport : public std::false_type {

    static void Serialize(const T&, TBinaryData&) {
        static_assert(false, "Unable to serialize unsupported type");
    }

    static bool Deserialize(T& , const void*, std::size_t) {
        static_assert(false, "Unable to deserialize unsupported type");
    }

    static TString ToString(const T&) {
        static_assert(false, "Unable to convert unsupported type");
    }

    static void AppendToString(const T&, TStringBuilder&) {
        static_assert(false, "Unable to append unsupported type to string");
    }
};

template <>
struct TNativeTypeSupport<TString> : public std::true_type {
    using TLength = std::size_t;

    static void Serialize(const TString& value, TBinaryData& data);

    static bool Deserialize(TString& value, const void* data, std::size_t length);

    static TString ToString(const TString& value);

    static void AppendToString(const TString& value, TStringBuilder& stringBuffer);
};

template <typename T, typename V = typename std::enable_if< std::is_pod_v<T> >::type>
struct TNativePlainTypeSupport : public std::true_type
{
    using TLength = std::size_t;
    static constexpr inline TLength ValueLength = sizeof(T);

    static inline void Serialize(const T& value, TBinaryData& data) {

        // Write contents
        auto oldSize = data.size();
        data.resize(oldSize + ValueLength);

        auto to = data.data() + oldSize;
        memcpy(to, &value, ValueLength);
    }

    static bool Deserialize(T& value, const void* data, std::size_t length) {
        if (ValueLength != length) {
           return false;
        }
        memcpy(&value, data, ValueLength);
        return true;
    }

    static TString ToString(const T& value) {
        return std::to_string(value);
    }

    static void AppendToString(const T& value, TStringBuilder& stringBuffer) {
        stringBuffer << ToString(value);
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
    static TString ToString(const bool& value);

    static void AppendToString(const bool& value, TStringBuilder& stringBuffer);
};

}  // namespace NActors::NStructuredLog
