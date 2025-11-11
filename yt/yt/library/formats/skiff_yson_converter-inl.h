#ifndef SKIFF_YSON_CONVERTER_INL_H_
#error "Direct inclusion of this file is not allowed; include skiff_yson_converter.h"
#endif

#include <yt/yt/library/tz_types/tz_types.h>

#include <util/system/byteorder.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType wireType>
Y_FORCE_INLINE auto TSimpleSkiffParser<wireType>::operator () (NSkiff::TCheckedInDebugSkiffParser* parser) const
{
    using namespace NSkiff;

    if constexpr (wireType == EWireType::Int8) {
        return parser->ParseInt8();
    } else if constexpr (wireType == EWireType::Int16) {
        return parser->ParseInt16();
    } else if constexpr (wireType == EWireType::Int32) {
        return parser->ParseInt32();
    } else if constexpr (wireType == EWireType::Int64) {
        return parser->ParseInt64();
    } else if constexpr (wireType == EWireType::Uint8) {
        return parser->ParseUint8();
    } else if constexpr (wireType == EWireType::Uint16) {
        return parser->ParseUint16();
    } else if constexpr (wireType == EWireType::Uint32) {
        return parser->ParseUint32();
    } else if constexpr (wireType == EWireType::Uint64) {
        return parser->ParseUint64();
    } else if constexpr (wireType == EWireType::Boolean) {
        return parser->ParseBoolean();
    } else if constexpr (wireType == EWireType::Double) {
        return parser->ParseDouble();
    } else if constexpr (wireType == EWireType::String32) {
        return parser->ParseString32();
    } else if constexpr (wireType == EWireType::Nothing) {
        return nullptr;
    } else {
        static_assert(wireType == EWireType::Int64);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType internalWireType>
TTzSkiffParser<internalWireType>::TTzSkiffParser()
{
    Buffer_.resize(NTzTypes::GetMaxPossibleTzStringSize());
}

template <NSkiff::EWireType internalWireType>
Y_FORCE_INLINE TStringBuf TTzSkiffParser<internalWireType>::operator () (NSkiff::TCheckedInDebugSkiffParser* parser)
{
    using namespace NSkiff;
    using namespace NTzTypes;

    if constexpr (internalWireType == EWireType::Int32) {
        auto value = parser->ParseInt32();
        return MakeTzString<i32>(value, GetTzName(parser->ParseUint16()), Buffer_.data(), Buffer_.size());
    } else if constexpr (internalWireType == EWireType::Int64) {
        auto value = parser->ParseInt64();
        return MakeTzString<i64>(value, GetTzName(parser->ParseUint16()), Buffer_.data(), Buffer_.size());
    } else if constexpr (internalWireType == EWireType::Uint16) {
        auto value = parser->ParseUint16();
        return MakeTzString<ui16>(value, GetTzName(parser->ParseUint16()), Buffer_.data(), Buffer_.size());
    } else if constexpr (internalWireType == EWireType::Uint32) {
        auto value = parser->ParseUint32();
        return MakeTzString<ui32>(value, GetTzName(parser->ParseUint16()), Buffer_.data(), Buffer_.size());
    } else if constexpr (internalWireType == EWireType::Uint64) {
        auto value = parser->ParseUint64();
        return MakeTzString<ui64>(value, GetTzName(parser->ParseUint16()), Buffer_.data(), Buffer_.size());
    } else {
        static_assert(internalWireType == EWireType::Int64);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType internalWireType>
void TTzSkiffWriter<internalWireType>::operator() (TStringBuf value, NSkiff::TCheckedInDebugSkiffWriter* writer) const
{
    using namespace NSkiff;
    using namespace NTzTypes;

    std::string_view resultTzName;
    if constexpr (internalWireType == EWireType::Int32) {
        const auto& [timestamp, tzName] = ParseTzValue<i32>(value);
        writer->WriteInt32(timestamp);
        resultTzName = tzName;
    } else if constexpr (internalWireType == EWireType::Int64) {
        const auto& [timestamp, tzName] = ParseTzValue<i64>(value);
        writer->WriteInt64(timestamp);
        resultTzName = tzName;
    } else if constexpr (internalWireType == EWireType::Uint16) {
        const auto& [timestamp, tzName] = ParseTzValue<ui16>(value);
        writer->WriteUint16(timestamp);
        resultTzName = tzName;
    } else if constexpr (internalWireType == EWireType::Uint32) {
        const auto& [timestamp, tzName] = ParseTzValue<ui32>(value);
        writer->WriteUint32(timestamp);
        resultTzName = tzName;
    } else if constexpr (internalWireType == EWireType::Uint64) {
        const auto& [timestamp, tzName] = ParseTzValue<ui64>(value);
        writer->WriteUint64(timestamp);
        resultTzName = tzName;
    } else {
        static_assert(internalWireType == EWireType::Int64);
    }
    writer->WriteUint16(static_cast<ui16>(GetTzIndex(resultTzName)));
}

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType SkiffWireType>
TDecimalSkiffParser<SkiffWireType>::TDecimalSkiffParser(int precision)
    : Precision_(precision)
{
    CheckSkiffWireTypeForDecimal(precision, SkiffWireType);
}

template <NSkiff::EWireType SkiffWireType>
Y_FORCE_INLINE TStringBuf TDecimalSkiffParser<SkiffWireType>::operator() (NSkiff::TCheckedInDebugSkiffParser* parser) const
{
    using namespace NSkiff;
    using namespace NDecimal;

    if constexpr (SkiffWireType == EWireType::Int32) {
        auto value = parser->ParseInt32();
        return TDecimal::WriteBinary32(Precision_, value, Buffer_, sizeof(Buffer_));
    } else if constexpr (SkiffWireType == EWireType::Int64) {
        auto value = parser->ParseInt64();
        return TDecimal::WriteBinary64(Precision_, value, Buffer_, sizeof(Buffer_));
    } else if constexpr (SkiffWireType == EWireType::Int128) {
        const auto skiffValue = parser->ParseInt128();
        return TDecimal::WriteBinary128(
            Precision_,
            TDecimal::TValue128{skiffValue.Low, skiffValue.High},
            Buffer_,
            sizeof(Buffer_));
    } else if constexpr (SkiffWireType == EWireType::Int256) {
        const auto skiffValue = parser->ParseInt256();
        TDecimal::TValue256 decimalValue;
        static_assert(sizeof(decimalValue) == sizeof(skiffValue));
        std::memcpy(&decimalValue, &skiffValue, sizeof(decimalValue));
        return TDecimal::WriteBinary256(
            Precision_,
            std::move(decimalValue),
            Buffer_,
            sizeof(Buffer_));
    } else {
        static_assert(SkiffWireType == EWireType::Int256);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType SkiffWireType>
TDecimalSkiffWriter<SkiffWireType>::TDecimalSkiffWriter(int precision)
    : Precision_(precision)
{
    CheckSkiffWireTypeForDecimal(precision, SkiffWireType);
}

template <NSkiff::EWireType SkiffWireType>
void TDecimalSkiffWriter<SkiffWireType>::operator()(TStringBuf value, NSkiff::TCheckedInDebugSkiffWriter* writer) const
{
    using namespace NSkiff;
    using namespace NDecimal;

    if constexpr (SkiffWireType == EWireType::Int32) {
        auto intValue = TDecimal::ParseBinary32(Precision_, value);
        writer->WriteInt32(intValue);
    } else if constexpr (SkiffWireType == EWireType::Int64) {
        auto intValue = TDecimal::ParseBinary64(Precision_, value);
        writer->WriteInt64(intValue);
    } else if constexpr (SkiffWireType == EWireType::Int128) {
        auto intValue = TDecimal::ParseBinary128(Precision_, value);
        writer->WriteInt128(TInt128{intValue.Low, intValue.High});
    } else if constexpr (SkiffWireType == EWireType::Int256) {
        auto intValue = TDecimal::ParseBinary256(Precision_, value);
        TInt256 skiffValue;
        static_assert(sizeof(skiffValue) == sizeof(intValue));
        std::memcpy(&skiffValue, &intValue, sizeof(skiffValue));
        writer->WriteInt256(std::move(skiffValue));
    } else {
        // poor man's static_assert(false)
        static_assert(SkiffWireType == EWireType::Int256);
    }
}

////////////////////////////////////////////////////////////////////////////////

TStringBuf TUuidParser::operator()(NSkiff::TCheckedInDebugSkiffParser* parser) const
{
    static_assert(sizeof(Buffer_) == 16);

    auto value = parser->ParseUint128();
    Buffer_[0] = HostToInet(value.High);
    Buffer_[1] = HostToInet(value.Low);
    return TStringBuf(reinterpret_cast<const char*>(Buffer_), sizeof(Buffer_));
}

////////////////////////////////////////////////////////////////////////////////

void TUuidWriter::operator()(TStringBuf value, NSkiff::TCheckedInDebugSkiffWriter* writer) const
{
    constexpr size_t ExpectedSize = 16;
    if (value.size() != ExpectedSize) {
        THROW_ERROR_EXCEPTION("Invalid size of UUID value: expected %v, actual %v",
            ExpectedSize,
            value.size());
    }
    const ui64* array = reinterpret_cast<const ui64*>(value.data());
    writer->WriteUint128(NSkiff::TUint128{InetToHost(array[1]), InetToHost(array[0])});
}

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType wireType, typename TValueType>
void CheckIntSize(TValueType value)
{
    using TIntType = typename NSkiff::TUnderlyingIntegerType<wireType>::TValue;
    bool ok;
    if constexpr (std::is_same_v<TValueType, i64>) {
        ok = std::numeric_limits<TIntType>::min() <= value && value <= std::numeric_limits<TIntType>::max();
    } else if constexpr (std::is_same_v<TValueType, ui64>) {
        ok = value <= std::numeric_limits<TIntType>::max();
    } else {
        static_assert(std::is_same_v<TIntType, i64>, "We expect either i64 or ui64 here");
    }
    if (!ok) {
        THROW_ERROR_EXCEPTION("Value %v is out of range for possible values for skiff type %Qlv",
            value,
            wireType);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
