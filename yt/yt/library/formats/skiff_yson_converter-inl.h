#ifndef SKIFF_YSON_CONVERTER_INL_H_
#error "Direct inclusion of this file is not allowed, include skiff_yson_converter.h"
// For the sake of sane code completion.
#include "skiff_yson_converter.h"
#endif

#include <yt/yt/library/tz_types/tz_types.h>

#include <library/cpp/yt/string/stream.h>

#include <util/system/byteorder.h>

#include <cmath>
#include <limits>
#include <utility>
#include <variant>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TDirectSkiffTypeTraits<NSkiff::EWireType::Nothing>
{
    using TWire = std::monostate;
    using TPhysical = std::monostate;

    static TWire Parse(NSkiff::TCheckedInDebugSkiffParser* /*parser*/)
    {
        return {};
    }

    static void Write(TPhysical /*value*/, NSkiff::TCheckedInDebugSkiffWriter* /*writer*/)
    { }
};

#define XX(WireType, TWireType, TPhysicalType)                                         \
    template <>                                                                        \
    struct TDirectSkiffTypeTraits<NSkiff::EWireType::WireType>                         \
    {                                                                                  \
        using TWire = TWireType;                                                       \
        using TPhysical = TPhysicalType;                                               \
                                                                                       \
        static TWire Parse(NSkiff::TCheckedInDebugSkiffParser* parser)                 \
        {                                                                              \
            return parser->Parse##WireType();                                          \
        }                                                                              \
                                                                                       \
        static void Write(TPhysical value, NSkiff::TCheckedInDebugSkiffWriter* writer) \
        {                                                                              \
            writer->Write##WireType(value);                                            \
        }                                                                              \
    };

XX(Boolean, bool, bool)
XX(Int8, i8, i64)
XX(Int16, i16, i64)
XX(Int32, i32, i64)
XX(Int64, i64, i64)
XX(VarInt32, i32, i64)
XX(VarInt64, i64, i64)
XX(Uint8, ui8, ui64)
XX(Uint16, ui16, ui64)
XX(Uint32, ui32, ui64)
XX(Uint64, ui64, ui64)
XX(Float, float, double)
XX(Double, double, double)
XX(String32, TStringBuf, TStringBuf)
XX(StringVar, TStringBuf, TStringBuf)
#undef XX

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType WireType>
Y_FORCE_INLINE auto TDirectParseFunction<WireType>::operator()(
    NSkiff::TCheckedInDebugSkiffParser* parser) const -> typename TTraits::TPhysical
{
    return TTraits::Parse(parser);
}

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType WireType>
Y_FORCE_INLINE void TDirectWriteFunction<WireType>::operator()(
    typename TTraits::TPhysical value,
    NSkiff::TCheckedInDebugSkiffWriter* writer) const
{
    if (!IsInRange<typename TTraits::TWire>(value)) {
        THROW_ERROR_EXCEPTION("Value %v is out of range of wire type %Qlv",
            value,
            WireType);
    }
    TTraits::Write(value, writer);
}

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType internalWireType>
TTzTupleParseFunction<internalWireType>::TTzTupleParseFunction()
{
    Buffer_.resize(NTzTypes::GetMaxPossibleTzStringSize());
}

template <NSkiff::EWireType internalWireType>
Y_FORCE_INLINE TStringBuf TTzTupleParseFunction<internalWireType>::operator()(NSkiff::TCheckedInDebugSkiffParser* parser)
{
    using namespace NSkiff;
    using namespace NTzTypes;

#define XX(wireType)                                                        \
    if constexpr (internalWireType == EWireType::wireType) {                \
        auto value = parser->Parse##wireType();                           \
        auto tzId = parser->ParseUint16();                                  \
        return MakeTzString(value, tzId, Buffer_.data(), Buffer_.size());   \
    } else

    XX(Int32)
    XX(Int64)
    XX(Uint16)
    XX(Uint32)
    XX(Uint64)
    /*else*/ {
        static_assert(internalWireType == EWireType::Int64);
    }
#undef XX
}

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType internalWireType>
void TTzTupleWriteFunction<internalWireType>::operator()(TStringBuf value, NSkiff::TCheckedInDebugSkiffWriter* writer) const
{
    using namespace NSkiff;
    using namespace NTzTypes;

    ui16 resultTzId;
    if constexpr (internalWireType == EWireType::Int32) {
        const auto& [timestamp, tzId] = ParseTzValue<i32>(value);
        writer->WriteInt32(timestamp);
        resultTzId = tzId;
    } else if constexpr (internalWireType == EWireType::Int64) {
        const auto& [timestamp, tzId] = ParseTzValue<i64>(value);
        writer->WriteInt64(timestamp);
        resultTzId = tzId;
    } else if constexpr (internalWireType == EWireType::Uint16) {
        const auto& [timestamp, tzId] = ParseTzValue<ui16>(value);
        writer->WriteUint16(timestamp);
        resultTzId = tzId;
    } else if constexpr (internalWireType == EWireType::Uint32) {
        const auto& [timestamp, tzId] = ParseTzValue<ui32>(value);
        writer->WriteUint32(timestamp);
        resultTzId = tzId;
    } else if constexpr (internalWireType == EWireType::Uint64) {
        const auto& [timestamp, tzId] = ParseTzValue<ui64>(value);
        writer->WriteUint64(timestamp);
        resultTzId = tzId;
    } else {
        static_assert(internalWireType == EWireType::Int64);
    }
    writer->WriteUint16(resultTzId);
}

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType SkiffWireType>
TDecimalParseFunction<SkiffWireType>::TDecimalParseFunction(int precision)
    : Precision_(precision)
{ }

template <NSkiff::EWireType SkiffWireType>
Y_FORCE_INLINE TStringBuf TDecimalParseFunction<SkiffWireType>::operator()(NSkiff::TCheckedInDebugSkiffParser* parser) const
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
TDecimalWriteFunction<SkiffWireType>::TDecimalWriteFunction(int precision)
    : Precision_(precision)
{ }

template <NSkiff::EWireType SkiffWireType>
void TDecimalWriteFunction<SkiffWireType>::operator()(TStringBuf value, NSkiff::TCheckedInDebugSkiffWriter* writer) const
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

TStringBuf TUuidParseFunction::operator()(NSkiff::TCheckedInDebugSkiffParser* parser) const
{
    static_assert(sizeof(Buffer_) == 16);

    auto value = parser->ParseUint128();
    Buffer_[0] = HostToInet(value.High);
    Buffer_[1] = HostToInet(value.Low);
    return TStringBuf(reinterpret_cast<const char*>(Buffer_), sizeof(Buffer_));
}

////////////////////////////////////////////////////////////////////////////////

void TUuidWriteFunction::operator()(TStringBuf value, NSkiff::TCheckedInDebugSkiffWriter* writer) const
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

TStringFixedParseFunction::TStringFixedParseFunction(i64 size)
    : Size_(size)
{ }

TStringBuf TStringFixedParseFunction::operator()(NSkiff::TCheckedInDebugSkiffParser* parser) const
{
    return parser->ParseStringFixed(Size_);
}

////////////////////////////////////////////////////////////////////////////////

TStringFixedWriteFunction::TStringFixedWriteFunction(i64 size)
    : Size_(size)
{ }

void TStringFixedWriteFunction::operator()(TStringBuf value, NSkiff::TCheckedInDebugSkiffWriter* writer) const
{
    if (std::ssize(value) != Size_) {
        THROW_ERROR_EXCEPTION("Invalid size of wire type %Qlv value: expected %v bytes, got %v",
            NSkiff::EWireType::StringFixed,
            Size_,
            std::ssize(value));
    }
    writer->WriteStringFixed(value);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TCppType>
struct TCppTypeToValueType
{
    static constexpr NTableClient::EValueType Value = [] {
        using namespace NTableClient;

        if constexpr (std::is_same_v<TCppType, std::monostate>) {
            return EValueType::Null;
        } else if constexpr (std::is_same_v<TCppType, i64>) {
            return EValueType::Int64;
        } else if constexpr (std::is_same_v<TCppType, ui64>) {
            return EValueType::Uint64;
        } else if constexpr (std::is_same_v<TCppType, bool>) {
            return EValueType::Boolean;
        } else if constexpr (std::is_same_v<TCppType, double>) {
            return EValueType::Double;
        } else if constexpr (std::is_same_v<TCppType, TStringBuf>) {
            return EValueType::String;
        } else {
            static_assert(false, "Unexpected C++ type");
        }
    }();
};

////////////////////////////////////////////////////////////////////////////////

template <typename TFunction>
auto DispatchNonComplexWireType(NSkiff::EWireType wireType, TFunction&& function)
{
    using namespace NSkiff;

    switch (wireType) {
#define CASE(WireType) \
        case EWireType::WireType: \
            return function.template operator()<EWireType::WireType>();
        CASE(Nothing)
        CASE(Boolean)
        CASE(Int8)
        CASE(Int16)
        CASE(Int32)
        CASE(Int64)
        CASE(Int128)
        CASE(Int256)
        CASE(VarInt32)
        CASE(VarInt64)
        CASE(Uint8)
        CASE(Uint16)
        CASE(Uint32)
        CASE(Uint64)
        CASE(Uint128)
        CASE(Uint256)
        CASE(Float)
        CASE(Double)
        CASE(String32)
        CASE(StringVar)
        CASE(Yson32)
        CASE(StringFixed)
#undef CASE
        case EWireType::Tuple:
        case EWireType::Variant8:
        case EWireType::Variant16:
        case EWireType::VariantVar:
        case EWireType::RepeatedVariant8:
        case EWireType::RepeatedVariant16:
        case EWireType::RepeatedBlockVar:
            break;
    }
    YT_ABORT();
}

template <typename TFunction>
auto DispatchOptionalKind(NSkiffExt::EOptionalKind optionalKind, TFunction&& function)
{
    using namespace NSkiffExt;

    switch (optionalKind) {
        case EOptionalKind::None:
            return function.template operator()<EOptionalKind::None>();
        case EOptionalKind::Variant8:
            return function.template operator()<EOptionalKind::Variant8>();
        case EOptionalKind::VariantVar:
            return function.template operator()<EOptionalKind::VariantVar>();
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

template <
    typename TConverter,
    typename TConverters,
    typename TFunctions,
    typename... TArgs
>
TConverter CreateSimpleConverter(
    const NSkiff::TSkiffSchemaPtr& skiffSchema,
    NTableClient::ESimpleLogicalValueType simpleType,
    TArgs&&... args)
{
    using namespace NSkiff;
    using namespace NTableClient;

    ValidateMatchesSimpleType(skiffSchema, simpleType);

    auto wireType = skiffSchema->GetWireType();
    if (simpleType == ESimpleLogicalValueType::Uuid && wireType == EWireType::Uint128) {
        using TUuidFunction = typename TFunctions::TUuidFunction;
        using TUuidConverter = typename TConverters::template TPhysicalConverter<TStringBuf, TUuidFunction>;
        return TUuidConverter(
            std::forward<TArgs>(args)...,
            TUuidFunction());
    } else if (IsTzType(simpleType) && wireType == EWireType::Tuple) {
        auto timeValueWireType = skiffSchema->GetChildren()[0]->GetWireType();
        return DispatchNonComplexWireType(timeValueWireType, [&args...] <EWireType TimeValueWireType> () -> TConverter {
            if constexpr (
                TimeValueWireType == EWireType::Int32 ||
                TimeValueWireType == EWireType::Int64 ||
                TimeValueWireType == EWireType::Uint16 ||
                TimeValueWireType == EWireType::Uint32 ||
                TimeValueWireType == EWireType::Uint64)
            {
                using TTzTupleFunction = typename TFunctions::template TTzTupleFunction<TimeValueWireType>;
                using TTzTupleConverter = typename TConverters::template TPhysicalConverter<
                    TStringBuf,
                    TTzTupleFunction
                >;
                return TTzTupleConverter(
                    std::forward<TArgs>(args)...,
                    TTzTupleFunction());
            } else {
                YT_ABORT();
            }
        });
    } else {
        return DispatchNonComplexWireType(wireType, [&skiffSchema, &args...] <EWireType WireType> () -> TConverter {
            if constexpr (WireType == EWireType::Yson32) {
                using TYson32Converter = typename TConverters::TYson32Converter;
                return TYson32Converter(std::forward<TArgs>(args)...);
            } else if constexpr (WireType == EWireType::StringFixed) {
                using TStringFixedFunction = typename TFunctions::TStringFixedFunction;
                using TStringFixedConverter = typename TConverters::template TPhysicalConverter<
                    TStringBuf,
                    TStringFixedFunction
                >;
                return TStringFixedConverter(
                    std::forward<TArgs>(args)...,
                    TStringFixedFunction(skiffSchema->GetSize()));
            } else if constexpr (requires { typename TDirectSkiffTypeTraits<WireType>::TPhysical; }) {
                using TPhysical = typename TDirectSkiffTypeTraits<WireType>::TPhysical;
                using TDirectFunction = typename TFunctions::template TDirectFunction<WireType>;
                using TDirectConverter = typename TConverters::template TPhysicalConverter<TPhysical, TDirectFunction>;
                return TDirectConverter(
                    std::forward<TArgs>(args)...,
                    TDirectFunction());
            } else {
                YT_ABORT();
            }
        });
    }
}

template <
    typename TConverter,
    typename TConverters,
    typename TFunctions,
    typename... TArgs
>
TConverter CreateDecimalConverter(
    const NSkiff::TSkiffSchemaPtr& skiffSchema,
    int precision,
    TArgs&&... args)
{
    using namespace NSkiff;

    auto wireType = skiffSchema->GetWireType();
    ValidateSkiffWireTypeForDecimal(precision, wireType);

    return DispatchNonComplexWireType(wireType, [precision, &args...] <EWireType WireType> () -> TConverter {
        if constexpr (WireType == EWireType::Yson32) {
            using TYson32Converter = typename TConverters::TYson32Converter;
            return TYson32Converter(std::forward<TArgs>(args)...);
        } else if constexpr (
            WireType == EWireType::Int32 ||
            WireType == EWireType::Int64 ||
            WireType == EWireType::Int128 ||
            WireType == EWireType::Int256)
        {
            using TDecimalFunction = typename TFunctions::template TDecimalFunction<WireType>;
            using TDecimalConverter = typename TConverters::template TPhysicalConverter<TStringBuf, TDecimalFunction>;
            return TDecimalConverter(
                std::forward<TArgs>(args)...,
                TDecimalFunction(precision));
        } else {
            YT_ABORT();
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

template <typename TWire, typename TValue>
bool IsInRange(TValue value)
{
    if constexpr (std::is_same_v<TValue, i64> || std::is_same_v<TValue, ui64>) {
        return std::in_range<TWire>(value);
    }
    if constexpr (std::is_same_v<TWire, float>) {
        return std::isinf(value) ||
            std::isnan(value) ||
            (std::numeric_limits<TWire>::lowest() <= value && value <= std::numeric_limits<TWire>::max());
    }
    return true;
}

template <typename TContainer>
std::string FormatExpectedTypes(const TContainer& expected)
{
    YT_VERIFY(!std::empty(expected));

    TStdStringStream expectationString;
    if (expected.size() == 1) {
        expectationString << Format("%Qlv", *expected.begin());
    } else {
        expectationString << "one of ";
        bool first = true;
        for (const auto& type : expected) {
            if (!first) {
                expectationString << ", ";
            }
            first = false;
            expectationString << Format("%Qlv", type);
        }
    }
    return std::move(expectationString).Str();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
