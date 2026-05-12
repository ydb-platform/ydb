#pragma once

#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/providers/common/codec/yql_codec_buf.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/utils/swap_bytes.h>

#include <library/cpp/yson/detail.h>

namespace NYql {

// yt native format (tuple of timestamp and tzId)
// no validation of value/tzId
template<typename T>
void ReadTzNative(NCommon::TInputBuf& buf, typename NUdf::TDataType<T>::TLayout& value, ui16& tzId) {
    static_assert(NUdf::TDataType<T>::Features & NUdf::EDataTypeFeatures::TzDateType);
    buf.ReadMany((char*)&value, sizeof(value));
    buf.ReadMany((char*)&tzId, sizeof(tzId));
}

template<typename T>
void WriteTzNative(NCommon::TOutputBuf& buf, typename NUdf::TDataType<T>::TLayout value, ui16 tzId) {
    static_assert(NUdf::TDataType<T>::Features & NUdf::EDataTypeFeatures::TzDateType);
    buf.WriteMany((const char*)&value, sizeof(value));
    buf.WriteMany((const char*)&tzId, sizeof(tzId));
}

// yt native table format (string with presorted timestamp concatenated with timezone name)
// only tzId is validated
template<typename T>
bool ReadTzTableNative(NCommon::TInputBuf& buf, typename NUdf::TDataType<T>::TLayout& value, ui16& tzId) {
    static_assert(NUdf::TDataType<T>::Features & NUdf::EDataTypeFeatures::TzDateType);
    using TLayout = NUdf::TDataType<T>::TLayout;

    TStringBuf str = buf.ReadYtString();
    if (str.size() < sizeof(value)) {
        return false;
    }

    using TUnsigned = std::make_unsigned<TLayout>::type;
    TUnsigned data;
    std::memcpy(&data, str.data(), sizeof(data));
    if constexpr (std::is_signed<TLayout>::value) {
        data = 0x80 ^ data;
    }
    value = (TLayout)SwapBytes(data);

    str.Skip(sizeof(data));
    if (auto maybeTzId = NKikimr::NMiniKQL::FindTimezoneId(str)) {
        tzId = *maybeTzId;
        return true;
    }
    return false;
}

template<typename T>
void WriteTzTableNative(NCommon::TOutputBuf& buf, typename NUdf::TDataType<T>::TLayout value, ui16 tzId) {
    static_assert(NUdf::TDataType<T>::Features & NUdf::EDataTypeFeatures::TzDateType);
    using TLayout = NUdf::TDataType<T>::TLayout;
    using TUnsigned = std::make_unsigned<TLayout>::type;
    TUnsigned data = SwapBytes((TUnsigned)value);
    if constexpr (std::is_signed<TLayout>::value) {
        data = 0x80 ^ data;
    }

    auto tzName = NKikimr::NMiniKQL::GetTimezoneIANAName(tzId);

    ui32 size = sizeof(data) + tzName.size();;
    buf.Write(NYson::NDetail::StringMarker);
    buf.WriteVarI32(size);
    buf.WriteMany((const char*)&data, sizeof(data));
    buf.WriteMany(tzName);
}

namespace {

template<typename T>
void ReadTzYqlData(NCommon::TInputBuf& buf, typename NUdf::TDataType<T>::TLayout& value, ui16& tzId) {
    using TLayout = NUdf::TDataType<T>::TLayout;
    using TUnsigned = std::make_unsigned<TLayout>::type;
    TUnsigned data;
    buf.ReadMany((char*)&data, sizeof(data));
    if constexpr (std::is_signed<TLayout>::value) {
        data = 0x80 ^ data;
    }
    value = (TLayout)SwapBytes(data);
    buf.ReadMany((char *)&tzId, sizeof(tzId));
    tzId = SwapBytes(tzId);
}

template<typename T>
void WriteTzYqlData(NCommon::TOutputBuf& buf, typename NUdf::TDataType<T>::TLayout value, ui16 tzId) {
    using TLayout = NUdf::TDataType<T>::TLayout;
    using TUnsigned = std::make_unsigned<TLayout>::type;
    TUnsigned data = SwapBytes((TUnsigned)value);
    if constexpr (std::is_signed<TLayout>::value) {
        data = 0x80 ^ data;
    }
    tzId = SwapBytes(tzId);
    buf.WriteMany((const char*)&data, sizeof(data));
    buf.WriteMany((const char*)&tzId, sizeof(tzId));
}

} // namespace

// YQL format (presorted string of timestamp and tzId)
// table format is exactly the same except for differeng string length encoding
// no validation of value/tzId

template<typename T>
bool ReadTzYql(NCommon::TInputBuf& buf, typename NUdf::TDataType<T>::TLayout& value, ui16& tzId) {
    static_assert(NUdf::TDataType<T>::Features & NUdf::EDataTypeFeatures::TzDateType);
    ui32 len;
    buf.ReadMany((char*)&len, sizeof(len));
    if (len != sizeof(value) + sizeof(tzId)) {
        return false;
    }
    ReadTzYqlData<T>(buf, value, tzId);
    return true;
}

template<typename T>
bool ReadTzYqlTable(NCommon::TInputBuf& buf, typename NUdf::TDataType<T>::TLayout& value, ui16& tzId) {
    static_assert(NUdf::TDataType<T>::Features & NUdf::EDataTypeFeatures::TzDateType);
    i32 len = buf.ReadVarI32();
    if (len != sizeof(value) + sizeof(tzId)) {
        return false;
    }
    ReadTzYqlData<T>(buf, value, tzId);
    return true;
}

template<typename T>
void WriteTzYql(NCommon::TOutputBuf& buf, typename NUdf::TDataType<T>::TLayout value, ui16 tzId) {
    static_assert(NUdf::TDataType<T>::Features & NUdf::EDataTypeFeatures::TzDateType);
    ui32 size = sizeof(value) + sizeof(tzId);
    buf.WriteMany((const char *)&size, sizeof(size));
    WriteTzYqlData<T>(buf, value, tzId);
}

template<typename T>
void WriteTzYqlTable(NCommon::TOutputBuf& buf, typename NUdf::TDataType<T>::TLayout value, ui16 tzId) {
    static_assert(NUdf::TDataType<T>::Features & NUdf::EDataTypeFeatures::TzDateType);
    ui32 size = sizeof(value) + sizeof(tzId);
    buf.Write(NYson::NDetail::StringMarker);
    buf.WriteVarI32(size);
    WriteTzYqlData<T>(buf, value, tzId);
}

}
