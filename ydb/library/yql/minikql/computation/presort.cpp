#include "presort.h"
#include "mkql_computation_node_holders.h"
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/utils/swap_bytes.h>
#include <ydb/library/yql/public/decimal/yql_decimal_serialize.h>

#include <util/system/unaligned_mem.h>
#include <util/string/builder.h> 

namespace NKikimr {
namespace NMiniKQL {

namespace NDetail {

using NYql::SwapBytes;

Y_FORCE_INLINE
void EnsureInputSize(TStringBuf& input, size_t size) {
    MKQL_ENSURE(input.size() >= size, "premature end of input");
}

template <bool Desc>
Y_FORCE_INLINE
void EncodeBool(TVector<ui8>& output, bool value) {
    output.push_back(Desc ? 0xFF ^ ui8(value) : ui8(value));
}

template <bool Desc>
Y_FORCE_INLINE
bool DecodeBool(TStringBuf& input) {
    EnsureInputSize(input, 1);
    auto result = Desc ? bool(0xFF ^ ui8(input[0])) : bool(input[0]);
    input.Skip(1);
    return result;
}

template <typename TUnsigned, bool Desc>
Y_FORCE_INLINE
void EncodeUnsigned(TVector<ui8>& output, TUnsigned value) {
    constexpr size_t size = sizeof(TUnsigned);

    if (Desc) {
        value = ~value;
    }

    output.resize(output.size() + size);
    WriteUnaligned<TUnsigned>(output.end() - size, SwapBytes(value));
}

template <typename TUnsigned, bool Desc>
Y_FORCE_INLINE
TUnsigned DecodeUnsigned(TStringBuf& input) {
    constexpr size_t size = sizeof(TUnsigned);

    EnsureInputSize(input, size);
    auto value = ReadUnaligned<TUnsigned>(input.data());
    input.Skip(size);

    value = SwapBytes(value);
    if (Desc) {
        value = ~value;
    }
    return value;
}

template <typename TSigned, bool Desc>
Y_FORCE_INLINE
void EncodeSigned(TVector<ui8>& output, TSigned value) {
    using TUnsigned = std::make_unsigned_t<TSigned>;
    constexpr size_t size = sizeof(TUnsigned);
    constexpr TUnsigned shift = TUnsigned(1) << (size * 8 - 1);

    EncodeUnsigned<TUnsigned, Desc>(output, TUnsigned(value) + shift);
}

template <typename TSigned, bool Desc>
Y_FORCE_INLINE
TSigned DecodeSigned(TStringBuf& input) {
    using TUnsigned = std::make_unsigned_t<TSigned>;
    constexpr size_t size = sizeof(TUnsigned);
    constexpr TUnsigned shift = TUnsigned(1) << (size * 8 - 1);

    return TSigned(DecodeUnsigned<TUnsigned, Desc>(input) - shift);
}

enum class EFPCode : ui8 {
    NegInf = 0,
    Neg = 1,
    Zero = 2,
    Pos = 3,
    PosInf = 4,
    Nan = 5
};

template <typename TFloat>
struct TFloatToInteger {};

template <>
struct TFloatToInteger<float> {
    using TType = ui32;
};

template <>
struct TFloatToInteger<double> {
    using TType = ui64;
};

static_assert(std::numeric_limits<float>::is_iec559, "float type is not iec559(ieee754)");
static_assert(std::numeric_limits<double>::is_iec559, "double type is not iec559(ieee754)");

template <typename TFloat, bool Desc>
Y_FORCE_INLINE
void EncodeFloating(TVector<ui8>& output, TFloat value) {
    using TInteger = typename TFloatToInteger<TFloat>::TType;
    EFPCode code;

    switch (std::fpclassify(value)) {
    case FP_NORMAL:
    case FP_SUBNORMAL: {
        auto integer = ReadUnaligned<TInteger>(&value);
        if (value < 0) {
            integer = ~integer;
            code = EFPCode::Neg;
        } else {
            code = EFPCode::Pos;
        }
        output.push_back(Desc ? 0xFF ^ ui8(code) : ui8(code));
        EncodeUnsigned<TInteger, Desc>(output, integer);
        return;
    }
    case FP_ZERO:
        code = EFPCode::Zero;
        break;
    case FP_INFINITE:
        code = value < 0 ? EFPCode::NegInf : EFPCode::PosInf;
        break;
    default:
        code = EFPCode::Nan;
        break;
    }
    output.push_back(Desc ? 0xFF ^ ui8(code) : ui8(code));
}

template <typename TFloat, bool Desc>
Y_FORCE_INLINE
TFloat DecodeFloating(TStringBuf& input) {
    using TInteger = typename TFloatToInteger<TFloat>::TType;

    EnsureInputSize(input, 1);
    auto code = EFPCode(Desc ? 0xFF ^ input[0] : input[0]);
    input.Skip(1);

    bool negative;
    switch (code) {
    case EFPCode::Zero:
        return 0;
    case EFPCode::NegInf:
        return -std::numeric_limits<TFloat>::infinity();
    case EFPCode::PosInf:
        return std::numeric_limits<TFloat>::infinity();
    case EFPCode::Nan:
        return std::numeric_limits<TFloat>::quiet_NaN();
    case EFPCode::Neg:
        negative = true;
        break;
    case EFPCode::Pos:
        negative = false;
        break;
    default:
        MKQL_ENSURE(false, "floating point data is corrupted");
    }

    auto integer = DecodeUnsigned<TInteger, Desc>(input);
    if (negative) {
        integer = ~integer;
    }

    return ReadUnaligned<TFloat>(&integer);
}

constexpr ui8 BlockCode = 0x1F;
constexpr size_t BlockSize = 15;
constexpr size_t BlockSizeUi64 = BlockSize / 8 + 1;

template <bool Desc>
Y_FORCE_INLINE
void EncodeString(TVector<ui8>& output, TStringBuf value) {
    size_t part = 0;

    while (!value.empty()) {
        union {
            ui8 buffer[BlockSize + 1];
            ui64 buffer64[BlockSizeUi64];
        };

        part = std::min(value.size(), BlockSize);
        if (part == BlockSize) {
            std::memcpy(buffer + 1, value.data(), BlockSize);
        } else {
            for (size_t i = 0; i < BlockSizeUi64; ++i) {
                buffer64[i] = 0;
            }
            std::memcpy(buffer + 1, value.data(), part);
        }
        value.Skip(part);

        buffer[0] = BlockCode;

        if (Desc) {
            for (size_t i = 0; i < BlockSizeUi64; ++i) {
                buffer64[i] ^= std::numeric_limits<ui64>::max();
            }
        }

        output.insert(output.end(), buffer, buffer + BlockSize + 1);
    }

    auto lastLength = ui8(part);
    output.push_back(Desc ? 0xFF ^ lastLength : lastLength);
}

template <bool Desc>
Y_FORCE_INLINE
TStringBuf DecodeString(TStringBuf& input, TVector<ui8>& value) {
    EnsureInputSize(input, 1);
    ui8 code = Desc ? 0xFF ^ input[0] : input[0];
    input.Skip(1);

    if (code != BlockCode) {
        MKQL_ENSURE(code == 0, TStringBuilder() << "unknown string block code: " << code);
        return TStringBuf();
    }

    while (code == BlockCode) {
        union {
            ui8 buffer[BlockSize + 1];
            ui64 buffer64[BlockSizeUi64];
        };

        EnsureInputSize(input, BlockSize + 1);
        std::memcpy(buffer, input.data(), BlockSize + 1);
        input.Skip(BlockSize + 1);

        if (Desc) {
            for (size_t i = 0; i < BlockSizeUi64; ++i) {
                buffer64[i] ^= std::numeric_limits<ui64>::max();
            }
        }

        value.insert(value.end(), buffer, buffer + BlockSize);
        code = buffer[BlockSize];
    }

    auto begin = (const char*)value.begin();
    auto end = (const char*)value.end() - BlockSize + code;
    return TStringBuf(begin, end - begin);
}

constexpr size_t UuidSize = 16;

template <bool Desc>
Y_FORCE_INLINE
void EncodeUuid(TVector<ui8>& output, const char* data) {
    output.resize(output.size() + UuidSize);
    auto ptr = output.end() - UuidSize;

    if (Desc) {
        for (size_t i = 0; i < UuidSize; ++i) {
            *ptr++ = ui8(*data++) ^ 0xFF;
        }
    } else {
        std::memcpy(ptr, data, UuidSize);
    }
}

template <bool Desc>
Y_FORCE_INLINE
TStringBuf DecodeUuid(TStringBuf& input, TVector<ui8>& value) {
    EnsureInputSize(input, UuidSize);
    auto data = input.data();
    input.Skip(UuidSize);

    value.resize(UuidSize);
    auto ptr = value.begin();

    if (Desc) {
        for (size_t i = 0; i < UuidSize; ++i) {
            *ptr++ = ui8(*data++) ^ 0xFF;
        }
    } else {
        std::memcpy(ptr, data, UuidSize);
    }

    return TStringBuf((const char*)value.begin(), (const char*)value.end());
}

template <typename TUnsigned, bool Desc>
Y_FORCE_INLINE
void EncodeTzUnsigned(TVector<ui8>& output, TUnsigned value, ui16 tzId) {
    constexpr size_t size = sizeof(TUnsigned);

    if (Desc) {
        value = ~value;
        tzId = ~tzId;
    }

    output.resize(output.size() + size + sizeof(ui16));
    WriteUnaligned<TUnsigned>(output.end() - size - sizeof(ui16), SwapBytes(value));
    WriteUnaligned<ui16>(output.end() - sizeof(ui16), SwapBytes(tzId));
}

template <typename TUnsigned, bool Desc>
Y_FORCE_INLINE
void DecodeTzUnsigned(TStringBuf& input, TUnsigned& value, ui16& tzId) {
    constexpr size_t size = sizeof(TUnsigned);

    EnsureInputSize(input, size + sizeof(ui16));
    auto v = ReadUnaligned<TUnsigned>(input.data());
    auto t = ReadUnaligned<ui16>(input.data() + size);
    input.Skip(size + sizeof(ui16));

    if (Desc) {
        value = ~SwapBytes(v);
        tzId = ~SwapBytes(t);
    } else {
        value = SwapBytes(v);
        tzId = SwapBytes(t);
    }
}

constexpr size_t DecimalSize = sizeof(NYql::NDecimal::TInt128); 

template <bool Desc>
Y_FORCE_INLINE
void EncodeDecimal(TVector<ui8>& output, NYql::NDecimal::TInt128 value) { 
    output.resize(output.size() + DecimalSize);
    auto ptr = reinterpret_cast<char*>(output.end() - DecimalSize);
    output.resize(output.size() + NYql::NDecimal::Serialize(Desc ? -value : value, ptr) - DecimalSize); 
}

template <bool Desc>
Y_FORCE_INLINE
NYql::NDecimal::TInt128 DecodeDecimal(TStringBuf& input) { 
    MKQL_ENSURE(input.size() > 0U && input.size() <= DecimalSize, "premature end of input"); 
    const auto des = NYql::NDecimal::Deserialize(input.data());
    input.Skip(des.second); 
    return Desc ? -des.first : des.first; 
}


template <bool Desc>
Y_FORCE_INLINE
void Encode(TVector<ui8>& output, NUdf::EDataSlot slot, const NUdf::TUnboxedValuePod& value) {
    switch (slot) {

    case NUdf::EDataSlot::Bool:
        EncodeBool<Desc>(output, value.Get<bool>());
        break;
    case NUdf::EDataSlot::Int8:
        EncodeSigned<i8, Desc>(output, value.Get<i8>());
        break;
    case NUdf::EDataSlot::Uint8:
        EncodeUnsigned<ui8, Desc>(output, value.Get<ui8>());
        break;
    case NUdf::EDataSlot::Int16:
        EncodeSigned<i16, Desc>(output, value.Get<i16>());
        break;
    case NUdf::EDataSlot::Uint16:
    case NUdf::EDataSlot::Date:
        EncodeUnsigned<ui16, Desc>(output, value.Get<ui16>());
        break;
    case NUdf::EDataSlot::Int32:
        EncodeSigned<i32, Desc>(output, value.Get<i32>());
        break;
    case NUdf::EDataSlot::Uint32:
    case NUdf::EDataSlot::Datetime:
        EncodeUnsigned<ui32, Desc>(output, value.Get<ui32>());
        break;
    case NUdf::EDataSlot::Int64:
    case NUdf::EDataSlot::Interval:
        EncodeSigned<i64, Desc>(output, value.Get<i64>());
        break;
    case NUdf::EDataSlot::Uint64:
    case NUdf::EDataSlot::Timestamp:
        EncodeUnsigned<ui64, Desc>(output, value.Get<ui64>());
        break;
    case NUdf::EDataSlot::Double:
        EncodeFloating<double, Desc>(output, value.Get<double>());
        break;
    case NUdf::EDataSlot::Float:
        EncodeFloating<float, Desc>(output, value.Get<float>());
        break;
    case NUdf::EDataSlot::DyNumber: 
    case NUdf::EDataSlot::String:
    case NUdf::EDataSlot::Utf8: {
        auto stringRef = value.AsStringRef();
        EncodeString<Desc>(output, TStringBuf(stringRef.Data(), stringRef.Size()));
        break;
    }
    case NUdf::EDataSlot::Uuid:
        EncodeUuid<Desc>(output, value.AsStringRef().Data());
        break;
    case NUdf::EDataSlot::TzDate:
        EncodeTzUnsigned<ui16, Desc>(output, value.Get<ui16>(), value.GetTimezoneId());
        break;
    case NUdf::EDataSlot::TzDatetime:
        EncodeTzUnsigned<ui32, Desc>(output, value.Get<ui32>(), value.GetTimezoneId());
        break;
    case NUdf::EDataSlot::TzTimestamp:
        EncodeTzUnsigned<ui64, Desc>(output, value.Get<ui64>(), value.GetTimezoneId());
        break;
    case NUdf::EDataSlot::Decimal:
        EncodeDecimal<Desc>(output, value.GetInt128());
        break;

    default:
        MKQL_ENSURE(false, TStringBuilder() << "unknown data slot for presort encoding: " << slot);
    }
}

template <bool Desc>
Y_FORCE_INLINE
NUdf::TUnboxedValue Decode(TStringBuf& input, NUdf::EDataSlot slot, TVector<ui8>& buffer) 
{
    switch (slot) {

    case NUdf::EDataSlot::Bool:
        return NUdf::TUnboxedValuePod(DecodeBool<Desc>(input));

    case NUdf::EDataSlot::Int8:
        return NUdf::TUnboxedValuePod(DecodeSigned<i8, Desc>(input));

    case NUdf::EDataSlot::Uint8:
        return NUdf::TUnboxedValuePod(DecodeUnsigned<ui8, Desc>(input));

    case NUdf::EDataSlot::Int16:
        return NUdf::TUnboxedValuePod(DecodeSigned<i16, Desc>(input));

    case NUdf::EDataSlot::Uint16:
    case NUdf::EDataSlot::Date:
        return NUdf::TUnboxedValuePod(DecodeUnsigned<ui16, Desc>(input));

    case NUdf::EDataSlot::Int32:
        return NUdf::TUnboxedValuePod(DecodeSigned<i32, Desc>(input));

    case NUdf::EDataSlot::Uint32:
    case NUdf::EDataSlot::Datetime:
        return NUdf::TUnboxedValuePod(DecodeUnsigned<ui32, Desc>(input));

    case NUdf::EDataSlot::Int64:
    case NUdf::EDataSlot::Interval:
        return NUdf::TUnboxedValuePod(DecodeSigned<i64, Desc>(input));

    case NUdf::EDataSlot::Uint64:
    case NUdf::EDataSlot::Timestamp:
        return NUdf::TUnboxedValuePod(DecodeUnsigned<ui64, Desc>(input));

    case NUdf::EDataSlot::Double:
        return NUdf::TUnboxedValuePod(DecodeFloating<double, Desc>(input));

    case NUdf::EDataSlot::Float:
        return NUdf::TUnboxedValuePod(DecodeFloating<float, Desc>(input));

    case NUdf::EDataSlot::DyNumber: 
    case NUdf::EDataSlot::String:
    case NUdf::EDataSlot::Utf8:
        buffer.clear();
        return MakeString(NUdf::TStringRef(DecodeString<Desc>(input, buffer))); 

    case NUdf::EDataSlot::Uuid:
        buffer.clear();
        return MakeString(NUdf::TStringRef(DecodeUuid<Desc>(input, buffer)));

    case NUdf::EDataSlot::TzDate: {
        ui16 date;
        ui16 tzId;
        DecodeTzUnsigned<ui16, Desc>(input, date, tzId);
        NUdf::TUnboxedValuePod value(date);
        value.SetTimezoneId(tzId);
        return value;
    }
    case NUdf::EDataSlot::TzDatetime: {
        ui32 datetime;
        ui16 tzId;
        DecodeTzUnsigned<ui32, Desc>(input, datetime, tzId);
        NUdf::TUnboxedValuePod value(datetime);
        value.SetTimezoneId(tzId);
        return value;
    }
    case NUdf::EDataSlot::TzTimestamp: {
        ui64 timestamp;
        ui16 tzId;
        DecodeTzUnsigned<ui64, Desc>(input, timestamp, tzId);
        NUdf::TUnboxedValuePod value(timestamp);
        value.SetTimezoneId(tzId);
        return value;
    }
    case NUdf::EDataSlot::Decimal:
        return NUdf::TUnboxedValuePod(DecodeDecimal<Desc>(input));

    default:
        MKQL_ENSURE(false, TStringBuilder() << "unknown data slot for presort decoding: " << slot);
    }
}

struct TDictItem {
    TString KeyBuffer;
    NUdf::TUnboxedValue Payload;

    TDictItem(const TString& keyBuffer, const NUdf::TUnboxedValue& payload)
        : KeyBuffer(keyBuffer)
        , Payload(payload)
    {}

    bool operator<(const TDictItem& other) const {
        return KeyBuffer < other.KeyBuffer;
    }
};

void EncodeValue(TType* type, const NUdf::TUnboxedValue& value, TVector<ui8>& output) {
    switch (type->GetKind()) {
    case TType::EKind::Void:
    case TType::EKind::Null:
    case TType::EKind::EmptyList:
    case TType::EKind::EmptyDict:
        break;
    case TType::EKind::Data: {
        auto slot = *static_cast<TDataType*>(type)->GetDataSlot();
        Encode<false>(output, slot, value);
        break;
    }
    case TType::EKind::Optional: {
        auto itemType = static_cast<TOptionalType*>(type)->GetItemType();
        auto hasValue = (bool)value;
        EncodeBool<false>(output, hasValue);
        if (hasValue) {
            EncodeValue(itemType, value.GetOptionalValue(), output);
        }

        break;
    }

    case TType::EKind::List: {
        auto itemType = static_cast<TListType*>(type)->GetItemType();
        auto iterator = value.GetListIterator();
        NUdf::TUnboxedValue item;
        while (iterator.Next(item)) {
            EncodeBool<false>(output, true);
            EncodeValue(itemType, item, output);
        }

        EncodeBool<false>(output, false);
        break;
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<TTupleType*>(type);
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            EncodeValue(tupleType->GetElementType(i), value.GetElement(i), output);
        }

        break;
    }

    case TType::EKind::Struct: {
        auto structType = static_cast<TStructType*>(type);
        for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
            EncodeValue(structType->GetMemberType(i), value.GetElement(i), output);
        }

        break;
    }

    case TType::EKind::Variant: {
        auto underlyingType = static_cast<TVariantType*>(type)->GetUnderlyingType();
        auto alt = value.GetVariantIndex();
        TType* altType;
        ui32 altCount;
        if (underlyingType->IsStruct()) {
            auto structType = static_cast<TStructType*>(underlyingType);
            altType = structType->GetMemberType(alt);
            altCount = structType->GetMembersCount();
        } else {
            auto tupleType = static_cast<TTupleType*>(underlyingType);
            altType = tupleType->GetElementType(alt);
            altCount = tupleType->GetElementsCount();
        }

        if (altCount < 256) {
            EncodeUnsigned<ui8, false>(output, alt);
        } else if (altCount < 256 * 256) {
            EncodeUnsigned<ui16, false>(output, alt);
        } else {
            EncodeUnsigned<ui32, false>(output, alt);
        }

        EncodeValue(altType, value.GetVariantItem(), output);
        break;
    }

    case TType::EKind::Dict: {
        auto dictType = static_cast<TDictType*>(type);
        auto iter = value.GetDictIterator();
        if (value.IsSortedDict()) {
            NUdf::TUnboxedValue key, payload;
            while (iter.NextPair(key, payload)) {
                EncodeBool<false>(output, true);
                EncodeValue(dictType->GetKeyType(), key, output);
                EncodeValue(dictType->GetPayloadType(), payload, output);
            }
        } else {
            // canonize keys
            TVector<TDictItem> items;
            items.reserve(value.GetDictLength());
            NUdf::TUnboxedValue key, payload;
            TVector<ui8> buffer;
            while (iter.NextPair(key, payload)) {
                buffer.clear();
                EncodeValue(dictType->GetKeyType(), key, buffer);
                TString keyBuffer((const char*)buffer.begin(), buffer.size());
                items.emplace_back(keyBuffer, payload);
            }
            Sort(items.begin(), items.end());
            // output values
            for (const auto& x : items) {
                EncodeBool<false>(output, true);
                output.insert(output.end(), x.KeyBuffer.begin(), x.KeyBuffer.end());
                EncodeValue(dictType->GetPayloadType(), x.Payload, output);
            }
        }

        EncodeBool<false>(output, false);
        break;
    }

    default:
        MKQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
    }
}

NUdf::TUnboxedValue DecodeImpl(TType* type, TStringBuf& input, const THolderFactory& factory, TVector<ui8>& buffer) {
    Y_UNUSED(factory);
    switch (type->GetKind()) {
    case TType::EKind::Void:
        return NUdf::TUnboxedValue::Void();
    case TType::EKind::Null:
        return NUdf::TUnboxedValue();
    case TType::EKind::EmptyList:
        return factory.GetEmptyContainer();
    case TType::EKind::EmptyDict:
        return factory.GetEmptyContainer();
    case TType::EKind::Data: {
        auto slot = *static_cast<TDataType*>(type)->GetDataSlot();
        return Decode<false>(input, slot, buffer);
    }
    case TType::EKind::Optional: {
        auto itemType = static_cast<TOptionalType*>(type)->GetItemType();
        auto hasValue = DecodeBool<false>(input);
        if (!hasValue) {
            return NUdf::TUnboxedValue();
        }

        auto value = DecodeImpl(itemType, input, factory, buffer);
        return value.Release().MakeOptional();
    }
    case TType::EKind::List: {
        auto itemType = static_cast<TListType*>(type)->GetItemType();
        TUnboxedValueVector values;
        while (DecodeBool<false>(input)) {
            auto value = DecodeImpl(itemType, input, factory, buffer);
            values.emplace_back(value);
        }

        return factory.VectorAsArray(values);
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<TTupleType*>(type);
        NUdf::TUnboxedValue* items;
        auto array = factory.CreateDirectArrayHolder(tupleType->GetElementsCount(), items);
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            items[i] = DecodeImpl(tupleType->GetElementType(i), input, factory, buffer);
        }

        return array;
    }

    case TType::EKind::Variant: {
        auto underlyingType = static_cast<TVariantType*>(type)->GetUnderlyingType();
        ui32 altCount;
        MKQL_ENSURE(underlyingType->IsTuple(), "Expcted variant over tuple");
        auto tupleType = static_cast<TTupleType*>(underlyingType);
        altCount = tupleType->GetElementsCount();

        ui32 alt;
        if (altCount < 256) {
            alt = DecodeUnsigned<ui8, false>(input);
        } else if (altCount < 256 * 256) {
            alt = DecodeUnsigned<ui16, false>(input);
        } else {
            alt = DecodeUnsigned<ui32, false>(input);
        }

        TType* altType = tupleType->GetElementType(alt);
        auto value = DecodeImpl(altType, input, factory, buffer);
        return factory.CreateVariantHolder(value.Release(), alt); 
    }

    // Struct and Dict may be encoded into a presort form only to canonize dict keys. No need to decode them.
    case TType::EKind::Struct:
    case TType::EKind::Dict:
    default:
        MKQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
    }
}

} // NDetail

void TPresortCodec::AddType(NUdf::EDataSlot slot, bool isOptional, bool isDesc) {
    Types.push_back({slot, isOptional, isDesc});
}

void TPresortEncoder::Start() {
    Output.clear();
    Current = 0;
}

void TPresortEncoder::Start(TStringBuf prefix) {
    Output.clear();
    auto data = reinterpret_cast<const ui8*>(prefix.data());
    Output.insert(Output.begin(), data, data + prefix.size());
    Current = 0;
}

void TPresortEncoder::Encode(const NUdf::TUnboxedValuePod& value) {
    auto& type = Types[Current++];

    if (type.IsDesc) {
        if (type.IsOptional) {
            auto hasValue = (bool)value;
            NDetail::EncodeBool<true>(Output, hasValue);
            if (!hasValue) {
                return;
            }
        }
        NDetail::Encode<true>(Output, type.Slot, value);
    } else {
        if (type.IsOptional) {
            auto hasValue = (bool)value;
            NDetail::EncodeBool<false>(Output, hasValue);
            if (!hasValue) {
                return;
            }
        }
        NDetail::Encode<false>(Output, type.Slot, value);
    }
}

TStringBuf TPresortEncoder::Finish() {
    MKQL_ENSURE(Current == Types.size(), "not all fields were encoded");
    return TStringBuf((const char*)Output.data(), Output.size());
}


void TPresortDecoder::Start(TStringBuf input) {
    Input = input;
    Current = 0;
}

NUdf::TUnboxedValue TPresortDecoder::Decode() {
    auto& type = Types[Current++];

    if (type.IsDesc) {
        if (type.IsOptional && !NDetail::DecodeBool<true>(Input)) {
            return NUdf::TUnboxedValuePod();
        }
        return NDetail::Decode<true>(Input, type.Slot, Buffer); 
    } else {
        if (type.IsOptional && !NDetail::DecodeBool<false>(Input)) {
            return NUdf::TUnboxedValuePod();
        }
        return NDetail::Decode<false>(Input, type.Slot, Buffer); 
    }
}

void TPresortDecoder::Finish() {
    MKQL_ENSURE(Current == Types.size(), "not all fields were decoded");
    MKQL_ENSURE(Input.empty(), "buffer is not empty");
}

TGenericPresortEncoder::TGenericPresortEncoder(TType* type)
    : Type(type)
{}

TStringBuf TGenericPresortEncoder::Encode(const NUdf::TUnboxedValue& value, bool desc) {
    Output.clear();
    NDetail::EncodeValue(Type, value, Output);
    if (desc) {
        for (auto& x : Output) {
            x = ~x;
        }
    }

    return TStringBuf((const char*)Output.data(), Output.size());
}

NUdf::TUnboxedValue TGenericPresortEncoder::Decode(TStringBuf buf, bool desc, const THolderFactory& factory) {
    if (desc) {
        Output.assign(buf.begin(), buf.end());
        for (auto& x : Output) {
            x = ~x;
        }

        auto newBuf = TStringBuf(reinterpret_cast<const char*>(Output.data()), Output.size());
        auto ret = NDetail::DecodeImpl(Type, newBuf, factory, Buffer);
        Output.clear();
        MKQL_ENSURE(newBuf.empty(), "buffer must be empty");
        return ret;
    } else {
        auto ret = NDetail::DecodeImpl(Type, buf, factory, Buffer);
        MKQL_ENSURE(buf.empty(), "buffer is not empty");
        return ret;
    }
}

} // NMiniKQL
} // NKikimr
