#pragma once

#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/util/tuples.h>
#include <ydb/core/util/templates.h>
#include <ydb/core/base/blobstorage_common.h>

#include <util/system/type_name.h>
#include <util/system/unaligned_mem.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <utility>

// https://wiki.yandex-team.ru/kikimr/techdoc/db/cxxapi/nicedb/

namespace NKikimr {
namespace NIceDb {

using TToughDb = NTable::TDatabase;
using NTable::TUpdateOp;
using NTable::ELookup;

class TTypeValue : public TRawTypeValue {
public:
    TTypeValue() // null
    {}

    TTypeValue(const ui64& value, NScheme::TTypeId type = NScheme::NTypeIds::Uint64)
        : TRawTypeValue(&value, sizeof(value), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const i64& value, NScheme::TTypeId type = NScheme::NTypeIds::Int64)
        : TRawTypeValue(&value, sizeof(value), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const ui32& value, NScheme::TTypeId type = NScheme::NTypeIds::Uint32)
        : TRawTypeValue(&value, sizeof(value), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const i32& value, NScheme::TTypeId type = NScheme::NTypeIds::Int32)
        : TRawTypeValue(&value, sizeof(value), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const ui16& value, NScheme::TTypeId type = NScheme::NTypeIds::Date)
        : TRawTypeValue(&value, sizeof(value), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const ui8& value, NScheme::TTypeId type = NScheme::NTypeIds::Byte)
        : TRawTypeValue(&value, sizeof(value), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const bool& value, NScheme::TTypeId type = NScheme::NTypeIds::Bool)
        : TRawTypeValue(&value, sizeof(value), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const double& value, NScheme::TTypeId type = NScheme::NTypeIds::Double)
        : TRawTypeValue(&value, sizeof(value), NScheme::TTypeInfo(type))
    {}

    template <typename ElementType>
    TTypeValue(const TVector<ElementType> &value, NScheme::TTypeId type = NScheme::NTypeIds::String)
        : TRawTypeValue(value.empty() ? (const ElementType*)0xDEADBEEFDEADBEEF : value.data(), value.size() * sizeof(ElementType), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const TActorId& value, NScheme::TTypeId type = NScheme::NTypeIds::ActorId)
        : TRawTypeValue(&value, sizeof(value), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const std::pair<ui64, ui64>& value, NScheme::TTypeId type = NScheme::NTypeIds::PairUi64Ui64)
        : TRawTypeValue(&value, sizeof(value), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const std::pair<ui64, i64>& value, NScheme::TTypeId type = NScheme::NTypeIds::Decimal)
        : TRawTypeValue(&value, sizeof(value), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const TString& value, NScheme::TTypeId type = NScheme::NTypeIds::Utf8)
        : TRawTypeValue(value.data(), value.size(), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const TBuffer& value, NScheme::TTypeId type = NScheme::NTypeIds::String)
        : TRawTypeValue(value.Empty() ? (const char*)0xDEADBEEFDEADBEEF : value.Data(), value.Size(), NScheme::TTypeInfo(type))
    {}

    TTypeValue(const TStringBuf& value, NScheme::TTypeId type = NScheme::NTypeIds::String)
        : TRawTypeValue(value.empty() ? (const char*)0xDEADBEEFDEADBEEF : value.data(), value.size(), NScheme::TTypeInfo(type))
    {}

    explicit TTypeValue(const TRawTypeValue& rawTypeValue)
        : TRawTypeValue(rawTypeValue)
    {}

    explicit TTypeValue(std::nullptr_t)
        : TRawTypeValue()
    {}

    operator TCell() const {
        return TCell(this);
    }

    bool HaveValue() const {
        return Data() != nullptr;
    }

    operator ui64() const {
        Y_ABORT_UNLESS((Type() == NScheme::NTypeIds::Uint64
                  || Type() == NScheme::NTypeIds::Timestamp)
                 && Size() == sizeof(ui64), "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        return ReadUnaligned<ui64>(reinterpret_cast<const ui64*>(Data()));
    }

    operator i64() const {
        Y_ABORT_UNLESS((Type() == NScheme::NTypeIds::Int64
                  || Type() == NScheme::NTypeIds::Interval
                  || Type() == NScheme::NTypeIds::Datetime64
                  || Type() == NScheme::NTypeIds::Timestamp64
                  || Type() == NScheme::NTypeIds::Interval64)
                 && Size() == sizeof(i64), "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        return ReadUnaligned<i64>(reinterpret_cast<const i64*>(Data()));
    }

    operator ui32() const {
        Y_ABORT_UNLESS((Type() == NScheme::NTypeIds::Uint32
                  || Type() == NScheme::NTypeIds::Datetime)
                 && Size() == sizeof(ui32), "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        ui32 value = ReadUnaligned<ui32>(reinterpret_cast<const ui32*>(Data()));
        return value;
    }

    operator i32() const {
        Y_ABORT_UNLESS((Type() == NScheme::NTypeIds::Int32 
                  || Type() == NScheme::NTypeIds::Date32)
                 && Size() == sizeof(i32), "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        i32 value = ReadUnaligned<i32>(reinterpret_cast<const i32*>(Data()));
        return value;
    }

    operator ui16() const {
        Y_ABORT_UNLESS(Type() == NScheme::NTypeIds::Date && Size() == sizeof(ui16), "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        ui16 value = ReadUnaligned<ui16>(reinterpret_cast<const ui16*>(Data()));
        return value;
    }

    operator ui8() const {
        Y_ABORT_UNLESS(Type() == NScheme::NTypeIds::Byte && Size() == sizeof(ui8), "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        ui8 value = *reinterpret_cast<const ui8*>(Data());
        return value;
    }

    operator bool() const {
        Y_ABORT_UNLESS(Type() == NScheme::NTypeIds::Bool && Size() == sizeof(bool), "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        bool value = *reinterpret_cast<const bool*>(Data());
        return value;
    }

    operator double() const {
        Y_ABORT_UNLESS(Type() == NScheme::NTypeIds::Double && Size() == sizeof(double), "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        double value = ReadUnaligned<double>(reinterpret_cast<const double*>(Data()));
        return value;
    }

    operator TActorId() const {
        Y_ABORT_UNLESS((Type() == NScheme::NTypeIds::ActorId
               || Type() == NScheme::NTypeIds::String
               || Type() == NScheme::NTypeIds::String2m
               || Type() == NScheme::NTypeIds::String4k) && Size() == sizeof(TActorId), "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        return *reinterpret_cast<const TActorId*>(Data());
    }

    operator TString() const {
        Y_ABORT_UNLESS(Type() == NScheme::NTypeIds::Utf8
               || Type() == NScheme::NTypeIds::String
               || Type() == NScheme::NTypeIds::String2m
               || Type() == NScheme::NTypeIds::String4k, "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        return TString(reinterpret_cast<const char*>(Data()), Size());
    }

    operator TBuffer() const {
        Y_ABORT_UNLESS(Type() == NScheme::NTypeIds::String
               || Type() == NScheme::NTypeIds::String2m
               || Type() == NScheme::NTypeIds::String4k, "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        return TBuffer(reinterpret_cast<const char*>(Data()), Size());
    }

    operator std::pair<ui64, ui64>() const {
        Y_ABORT_UNLESS(Type() == NScheme::NTypeIds::PairUi64Ui64 && Size() == sizeof(std::pair<ui64, ui64>), "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        return *reinterpret_cast<const std::pair<ui64, ui64>*>(Data());
    }

    operator std::pair<ui64, i64>() const {
        Y_ABORT_UNLESS(Type() == NScheme::NTypeIds::Decimal && Size() == sizeof(std::pair<ui64, ui64>), "Data=%" PRIxPTR ", Type=%" PRIi64 ", Size=%" PRIi64, (ui64)Data(), (i64)Type(), (i64)Size());
        return *reinterpret_cast<const std::pair<ui64, i64>*>(Data());
    }

    template <typename ElementType>
    operator TVector<ElementType>() const {
        static_assert(std::is_pod<ElementType>::value, "ElementType should be a POD type");
        Y_ABORT_UNLESS(Type() == NScheme::NTypeIds::String || Type() == NScheme::NTypeIds::String4k || Type() == NScheme::NTypeIds::String2m);
        Y_ABORT_UNLESS(Size() % sizeof(ElementType) == 0);
        std::size_t count = Size() / sizeof(ElementType);
        const ElementType *begin = reinterpret_cast<const ElementType*>(Data());
        const ElementType *end = begin + count;
        return TVector<ElementType>(begin, end);
    }

    template <typename ElementType>
    void ExtractArray(THashSet<ElementType> &container) const {
        static_assert(std::is_pod<ElementType>::value, "ElementType should be a POD type");
        Y_ABORT_UNLESS(Type() == NScheme::NTypeIds::String || Type() == NScheme::NTypeIds::String4k || Type() == NScheme::NTypeIds::String2m);
        Y_ABORT_UNLESS(Size() % sizeof(ElementType) == 0);
        const ElementType *begin = reinterpret_cast<const ElementType*>(Data());
        const ElementType *end = begin + Size() / sizeof(ElementType);
        container.resize(Size() / sizeof(ElementType));
        for (auto it = begin; it !=end; ++it) {
            container.insert(*it);
        }
    }
};

template <NScheme::TTypeId T> struct NSchemeTypeMapper;

template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Bool> { typedef bool Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Byte> { typedef ui8 Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Uint32> { typedef ui32 Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Int32> { typedef i32 Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Uint64> { typedef ui64 Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Int64> { typedef i64 Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::String> { typedef TString Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::String4k> : NSchemeTypeMapper<NScheme::NTypeIds::String> {};
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Utf8> { typedef TString Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::ActorId> { typedef TActorId Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::PairUi64Ui64> { typedef std::pair<ui64, ui64> Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Double> { typedef double Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Decimal> { typedef std::pair<ui64, i64> Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Date> { typedef ui16 Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Datetime> { typedef ui32 Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Timestamp> { typedef ui64 Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Interval> { typedef i64 Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Date32> { typedef i32 Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Datetime64> { typedef i64 Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Timestamp64> { typedef i64 Type; };
template <> struct NSchemeTypeMapper<NScheme::NTypeIds::Interval64> { typedef i64 Type; };

/// only for compatibility with old code
template <NScheme::TTypeId ValType>
class TConvertTypeValue : public TRawTypeValue {
public:
    TConvertTypeValue(const TRawTypeValue& value)
        : TRawTypeValue(value.Data(), value.Size(), value.IsEmpty() ? NScheme::TTypeInfo(0) : NScheme::TTypeInfo(ValType))
    {}

    template <typename ValueType> static typename NSchemeTypeMapper<ValType>::Type ConvertFrom(ValueType value) {
        return static_cast<typename NSchemeTypeMapper<ValType>::Type>(value);
    }
};

/// only for compatibility with old code
template <>
class TConvertTypeValue<NScheme::NTypeIds::String> : public TRawTypeValue {
public:
    TConvertTypeValue(const TRawTypeValue& value)
        : TRawTypeValue(value.Data(), value.Size(), value.IsEmpty() ? NScheme::TTypeInfo(0) : NScheme::TTypeInfo(NScheme::NTypeIds::String))
    {}

    static typename NSchemeTypeMapper<NScheme::NTypeIds::String>::Type ConvertFrom(const TString& value) {
        return static_cast<typename NSchemeTypeMapper<NScheme::NTypeIds::String>::Type>(value);
    }

    static typename NSchemeTypeMapper<NScheme::NTypeIds::String>::Type ConvertFrom(const TStringBuf& value) {
        return static_cast<typename NSchemeTypeMapper<NScheme::NTypeIds::String>::Type>(TString(value));
    }

    static typename NSchemeTypeMapper<NScheme::NTypeIds::String>::Type ConvertFrom(const ::google::protobuf::Message& value) {
        return static_cast<typename NSchemeTypeMapper<NScheme::NTypeIds::String>::Type>(value.SerializeAsString());
    }

    template <typename ElementType>
    static typename NSchemeTypeMapper<NScheme::NTypeIds::String>::Type ConvertFrom(const TVector<ElementType>& value) {
        return static_cast<typename NSchemeTypeMapper<NScheme::NTypeIds::String>::Type>(
            TString(
                reinterpret_cast<const char*>(value.data()), value.size() * sizeof(ElementType)
            )
        );
    }
};

////////////////////////////////////////////////////////////////////////////////////////

/// use the following semantic to define custom rules for database types conversion:

//template <typename ColumnType>
//struct TConvertValue<ColumnType, TRawTypeValue, TStringBuf> {
//    TRawTypeValue Value;
//
//    TConvertValue(TStringBuf value)
//        : Value(value.data(), value.size(), ColumnType::ColumnType)
//    {
//        static_assert(ColumnType::ColumnType == NScheme::NTypeIds::String
//                      || ColumnType::ColumnType == NScheme::NTypeIds::Utf8,
//                      "Unsupported ColumnType for TStringBuf");
//    }
//
//    operator const TRawTypeValue&() const {
//        return Value;
//    }
//};

//template <typename ColumnType>
//struct TConvertValue<ColumnType, TStringBuf, TRawTypeValue> {
//    TStringBuf Value;
//
//    TConvertValue(const TRawTypeValue& value)
//        : Value(reinterpret_cast<const char*>(value.Data()), value.Size())
//    {
//        Y_ABORT_UNLESS(value.Type() == NScheme::NTypeIds::String || value.Type() == NScheme::NTypeIds::Utf8);
//    }
//
//    operator TStringBuf() const {
//        return Value;
//    }
//};

////////////////////////////////////////////////////////////////////////////////////////

template <typename ColumnType, typename TargetType, typename SourceType>
struct TConvertValue {
    TargetType Value;

    TConvertValue(const SourceType& value)
        : Value(value)
    {}

    operator const TargetType&() const {
        return Value;
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TInstant conversion

template <typename TColumnType>
struct TConvertValue<TColumnType, TRawTypeValue, TInstant> {
    typename NSchemeTypeMapper<TColumnType::ColumnType>::Type Storage;
    TTypeValue Value;
    TConvertValue(const TInstant& value) : Storage(value.GetValue()), Value(Storage, TColumnType::ColumnType) {}
    operator const TRawTypeValue&() const { return Value; }
};

template <typename TColumnType>
struct TConvertValue<TColumnType, TInstant, TRawTypeValue> {
    TTypeValue Value;
    TConvertValue(const TRawTypeValue& value) : Value(value) {}
    operator TInstant() const { return TInstant::FromValue(static_cast<ui64>(Value)); }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDuration conversion

template <typename TColumnType>
struct TConvertValue<TColumnType, TRawTypeValue, TDuration> {
    typename NSchemeTypeMapper<TColumnType::ColumnType>::Type Storage;
    TTypeValue Value;
    TConvertValue(const TDuration& value) : Storage(value.GetValue()), Value(Storage, TColumnType::ColumnType) {}
    operator const TRawTypeValue&() const { return Value; }
};

template <typename TColumnType>
struct TConvertValue<TColumnType, TDuration, TRawTypeValue> {
    TTypeValue Value;
    TConvertValue(const TRawTypeValue& value) : Value(value) {}
    operator TDuration() const { return TDuration::FromValue(static_cast<ui64>(Value)); }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TIdWrapper conversion

template <typename TColumnType, typename T, typename Tag>
struct TConvertValue<TColumnType, TRawTypeValue, TIdWrapper<T, Tag>> {
    typename NSchemeTypeMapper<TColumnType::ColumnType>::Type Storage;
    TTypeValue Value;
    TConvertValue(const TIdWrapper<T, Tag> & value) : Storage(value.GetRawId()), Value(Storage, TColumnType::ColumnType) {}
    operator const TRawTypeValue&() const { return Value; }
};

template <typename TColumnType, typename T, typename Tag>
struct TConvertValue<TColumnType, TIdWrapper<T, Tag>, TRawTypeValue> {
    TTypeValue Value;
    TConvertValue(const TRawTypeValue & value) : Value(value) {}
    operator TIdWrapper<T, Tag>() const { return TIdWrapper<T, Tag>::FromValue(static_cast<T>(Value)); }
};

template <typename TColumnType, typename SourceType>
struct TConvertValue<TColumnType, TRawTypeValue, SourceType> {
    TTypeValue Value;

    TConvertValue(const SourceType& value)
        : Value(value, TColumnType::ColumnType)
    {}

    operator const TRawTypeValue&() const {
        return Value;
    }
};

template <typename TColumnType, typename SourceType>
struct TConvertValueFromProtoToRawTypeValue {
    TString Storage;
    TTypeValue Value;

    TConvertValueFromProtoToRawTypeValue(const SourceType& value)
        : Storage(value.SerializeAsString())
        , Value(Storage, TColumnType::ColumnType)
    {
        static_assert(TColumnType::ColumnType == NScheme::NTypeIds::String,
                      "Unsupported ColumnType for proto");
    }

    operator const TRawTypeValue&() const {
        return Value;
    }
};

template <typename TColumnType, typename SourceType>
struct TConvertValueFromPodToRawTypeValue {
    typename NSchemeTypeMapper<TColumnType::ColumnType>::Type Storage;
    TTypeValue Value;

    TConvertValueFromPodToRawTypeValue(const SourceType& value)
        : Storage(static_cast<typename NSchemeTypeMapper<TColumnType::ColumnType>::Type>(value))
        , Value(Storage, TColumnType::ColumnType)
    {}

    operator const TRawTypeValue&() const {
        return Value;
    }
};

template <typename TColumnType>
struct TConvertValueFromPodToRawTypeValue<TColumnType, typename NSchemeTypeMapper<TColumnType::ColumnType>::Type> {
    TTypeValue Value;

    TConvertValueFromPodToRawTypeValue(const typename NSchemeTypeMapper<TColumnType::ColumnType>::Type& value)
        : Value(value, TColumnType::ColumnType)
    {}

    operator const TRawTypeValue&() const {
        return Value;
    }
};

template <typename ColumnType, typename TargetType>
struct TConvertValueFromRawTypeValueToProto {
    TTypeValue Value;

    TConvertValueFromRawTypeValueToProto(const TRawTypeValue& value)
        : Value(value)
    {
        Y_ABORT_UNLESS(value.Type() == NScheme::NTypeIds::String);
    }

    operator TargetType() const {
        TargetType msg;
        Y_ABORT_UNLESS(msg.ParseFromArray(Value.Data(), Value.Size()));
        return msg;
    }
};

template <typename ColumnType, typename TargetType>
struct TConvertValue<ColumnType, TargetType, TRawTypeValue> {
    TTypeValue Value;

    TConvertValue(const TRawTypeValue& value)
        : Value(value)
    {}

    operator TargetType() const {
        return static_cast<TargetType>(Value);
    }
};

template <typename TColumnType, typename TargetType>
struct TConvertValueFromRawTypeValueToPod {
    TTypeValue Value;

    TConvertValueFromRawTypeValueToPod(const TRawTypeValue& value)
        : Value(value)
    {}

    operator TargetType() const {
        return static_cast<TargetType>(static_cast<typename NSchemeTypeMapper<TColumnType::ColumnType>::Type>(Value));
    }
};

template <typename ColumnType, typename VectorType>
struct TConvertValue<ColumnType, TVector<VectorType>, TRawTypeValue> {
    TVector<VectorType> Value;

    TConvertValue(const TRawTypeValue& value) {
        Y_ABORT_UNLESS(value.Type() == NScheme::NTypeIds::String);
        Y_ABORT_UNLESS(value.Size() % sizeof(VectorType) == 0);
        const size_t count = value.Size() / sizeof(VectorType);
        Value.reserve(count);
        for (TUnalignedMemoryIterator<VectorType> it(value.Data(), value.Size()); !it.AtEnd(); it.Next()) {
            Value.emplace_back(it.Cur());
        }
        Y_ABORT_UNLESS(Value.size() == count);
    }

    operator const TVector<VectorType>&() const {
        return Value;
    }
};

template <typename TColumnType, typename VectorType>
struct TConvertValue<TColumnType, TRawTypeValue, TVector<VectorType>> {
    TTypeValue Value;

    TConvertValue(const TVector<VectorType>& value)
        : Value(value)
    {
        static_assert(TColumnType::ColumnType == NScheme::NTypeIds::String,
                      "Unsupported ColumnType for vector<>");
    }

    operator const TRawTypeValue&() const {
        return Value;
    }
};

template <typename TColumnType>
struct TConvertValue<TColumnType, TRawTypeValue, TStringBuf> {
    TRawTypeValue Value;

    TConvertValue(TStringBuf value)
        : Value(value.data(), value.size(), NScheme::TTypeInfo(TColumnType::ColumnType))
    {
        static_assert(TColumnType::ColumnType == NScheme::NTypeIds::String
                      || TColumnType::ColumnType == NScheme::NTypeIds::Utf8,
                      "Unsupported ColumnType for TStringBuf");
    }

    operator const TRawTypeValue&() const {
        return Value;
    }
};

template <typename ColumnType>
struct TConvertValue<ColumnType, TStringBuf, TRawTypeValue> {
    TStringBuf Value;

    TConvertValue(const TRawTypeValue& value)
        : Value(reinterpret_cast<const char*>(value.Data()), value.Size())
    {
        Y_ABORT_UNLESS(value.Type() == NScheme::NTypeIds::String || value.Type() == NScheme::NTypeIds::Utf8);
    }

    operator TStringBuf() const {
        return Value;
    }
};

template <typename ColumnType,
          typename TargetType,
          typename SourceType,
          typename std::enable_if<std::is_same<TRawTypeValue, TargetType>::value, bool>::type = true,
          typename std::enable_if<std::is_base_of<::google::protobuf::Message, SourceType>::value, bool>::type = true>
auto ConvertValue(const SourceType& value) {
    return TConvertValueFromProtoToRawTypeValue<ColumnType, SourceType>(value);
}

template <typename ColumnType,
          typename TargetType,
          typename SourceType,
          typename std::enable_if<std::is_same<TRawTypeValue, SourceType>::value, bool>::type = true,
          typename std::enable_if<std::is_base_of<::google::protobuf::Message, TargetType>::value, bool>::type = true>
auto ConvertValue(const SourceType& value) {
    return TConvertValueFromRawTypeValueToProto<ColumnType, TargetType>(value);
}

template <typename ColumnType,
          typename TargetType,
          typename SourceType,
          typename std::enable_if<std::is_same<TRawTypeValue, TargetType>::value, bool>::type = true,
          typename std::enable_if<std::is_pod<SourceType>::value, bool>::type = true>
auto ConvertValue(const SourceType& value) {
    return TConvertValueFromPodToRawTypeValue<ColumnType, SourceType>(value);
}

template <typename ColumnType,
          typename TargetType,
          typename SourceType,
          typename std::enable_if<std::is_same<TRawTypeValue, SourceType>::value, bool>::type = true,
          typename std::enable_if<std::is_pod<TargetType>::value, bool>::type = true>
auto ConvertValue(const SourceType& value) {
    return TConvertValueFromRawTypeValueToPod<ColumnType, TargetType>(value);
}

template <typename ColumnType,
          typename TargetType,
          typename SourceType,
          std::enable_if_t<!std::is_pod<SourceType>::value, bool> = true,
          std::enable_if_t<!std::is_pod<TargetType>::value, bool> = true,
          typename std::enable_if<!std::is_base_of<::google::protobuf::Message, SourceType>::value, bool>::type = true,
          typename std::enable_if<!std::is_base_of<::google::protobuf::Message, TargetType>::value, bool>::type = true>
auto ConvertValue(const SourceType& value) {
    return TConvertValue<ColumnType, TargetType, SourceType>(value);
}

template <typename ColumnType, typename TargetType>
struct TConvert {
    template <typename SourceType>
    static auto Convert(const SourceType& value) {
        return ConvertValue<ColumnType, TargetType, SourceType>(value);
    }
};

template <typename ColumnType, typename TargetType, typename SourceType>
struct TConvertType {
    using Type = decltype(TConvert<ColumnType, TRawTypeValue>::Convert(*(const SourceType*)nullptr));
};

typedef ui32 TTableId;
typedef ui32 TColumnId;

template <typename Column>
struct TUpdate {
    typename TConvertType<Column, TRawTypeValue, typename Column::Type>::Type Value;

    explicit TUpdate(const typename Column::Type& value)
        : Value(TConvert<Column, TRawTypeValue>::Convert(value))
    {}

    operator TUpdateOp() const {
        return TUpdateOp(Column::ColumnId, NTable::ECellOp::Set, Value);
    }
};

template <typename Column>
struct TNull {
    operator TUpdateOp() const {
        return TUpdateOp(Column::ColumnId, NTable::ECellOp::Null, { });
    }
};

enum class EMaterializationMode {
    All,
    Existing,
    NonExisting,
};

struct Schema {
    template <typename T>
    struct Precharger {
        static bool Precharge(
            TToughDb&, ui32,
            NTable::TRawVals, NTable::TRawVals,
            NTable::TTagsRef, NTable::EDirection,
            ui64 = Max<ui64>(), ui64 = Max<ui64>());
    };

    struct NoAutoPrecharge {};
    struct AutoPrecharge {};

    template <TTableId _TableId> struct Table {
        constexpr static TTableId TableId = _TableId;

        using Precharge = AutoPrecharge;

        template <TColumnId _ColumnId, NScheme::TTypeId _ColumnType, bool _IsNotNull = false>
        struct Column {
            constexpr static TColumnId ColumnId = _ColumnId;
            constexpr static NScheme::TTypeId ColumnType = _ColumnType;
            constexpr static bool IsNotNull = _IsNotNull;
            using Type = typename NSchemeTypeMapper<_ColumnType>::Type;

            static TString GetColumnName(const TString& typeName) {
                return typeName.substr(typeName.rfind(':') + 1);
            }
        };

        template <typename...>
        struct TableColumns;

        template <typename T>
        struct TableColumns<T> {
            using Type = typename T::Type;
            using TupleType = std::tuple<typename T::Type>;
            using RealTupleType = std::tuple<typename NSchemeTypeMapper<T::ColumnType>::Type>;

            static TString GetColumnName() {
                return GetColumnName(TypeName<T>());
            }

            static TString GetColumnName(const TString& typeName) {
                return T::GetColumnName(typeName);
            }

            static void Materialize(TToughDb& database) {
                database.Alter().AddColumn(TableId, GetColumnName(), T::ColumnId, T::ColumnType, T::IsNotNull);
            }

            static constexpr bool HaveColumn(ui32 columnId) {
                return T::ColumnId == columnId;
            }

            template <typename OtherT>
            static constexpr bool HaveColumn() {
                return std::is_same<T, OtherT>::value != 0;
            }
        };

        template <typename T, typename... Ts>
        struct TableColumns<T, Ts...> : TableColumns<Ts...> {
            using Type = std::tuple<typename T::Type, typename Ts::Type...>;
            using TupleType = std::tuple<typename T::Type, typename Ts::Type...>;
            using RealTupleType = std::tuple<typename NSchemeTypeMapper<T::ColumnType>::Type, typename NSchemeTypeMapper<Ts::ColumnType>::Type...>;

            static void Materialize(TToughDb& database) {
                TableColumns<T>::Materialize(database);
                TableColumns<Ts...>::Materialize(database);
            }

            static constexpr bool HaveColumn(ui32 columnId) {
                return TableColumns<T>::HaveColumn(columnId) || TableColumns<Ts...>::HaveColumn(columnId);
            }
        };

        template <typename... ColumnsTypes>
        struct Columns : TableColumns<ColumnsTypes...> {
            using ColumnsType = std::tuple<ColumnsTypes...>;

            static const std::array<TColumnId, sizeof...(ColumnsTypes)>& GetColumnIds() {
                static const std::array<TColumnId, sizeof...(ColumnsTypes)> ColumnIds = {{ColumnsTypes::ColumnId...}};
                return ColumnIds;
            }

            template <typename TColumn>
            static constexpr std::size_t GetIndex(TColumn) {
                return index_of<TColumn, ColumnsType>::value;
            }

            template <typename TColumn>
            static constexpr std::size_t GetIndex() {
                return index_of<TColumn, ColumnsType>::value;
            }
        };

        template <typename... ColumnsTypes>
        struct Columns<TableColumns<ColumnsTypes...>> : Columns<ColumnsTypes...> {};

        template <typename IteratorType>
        using ReverseIteratorType = typename IteratorType::TReverseType;

        template <typename, typename> class KeyOperations;
        template <typename, typename> class AnyKeyOperations;
        template <typename, typename, typename> class KeyPrefixOperations;
        template <typename, typename, typename> class GreaterOrEqualKeyOperations;
        template <typename, typename, typename> class LessOrEqualKeyOperations;
        template <typename, typename, typename, typename> class RangeKeyOperations;

        template <typename IteratorType, typename TableType, typename... KeyColumns>
        class TableSelector {
        protected:
            using KeyColumnsType = std::tuple<KeyColumns...>;
            using KeyValuesType = std::tuple<typename KeyColumns::Type...>;
            using ReverseSelector = TableSelector<ReverseIteratorType<IteratorType>, TableType, KeyColumns...>;
            TToughDb* Database;

        public:
            TableSelector(TToughDb& database)
                : Database(&database)
            {}

            TableSelector(const TableSelector&) = default;
            TableSelector(TableSelector&&) = default;

            TableSelector& operator =(TableSelector&& selector) {
                Database = selector.Database;
                return *this;
            }

            TableSelector& operator =(const TableSelector& selector) {
                Database = selector.Database;
                return *this;
            }

            template<typename... Keys>
            auto Key(Keys&&... keyValues) {
                return KeyOperations<TableType, KeyValuesType>(*Database, std::forward<Keys>(keyValues)...);
             }

            template <typename... Keys>
            auto Range(Keys... keyValues) {
                return KeyPrefixOperations<IteratorType, TableType, typename first_n_of<sizeof...(Keys), KeyValuesType>::type>(*Database, keyValues...);
            }

            template <typename... Keys>
            auto Prefix(Keys... keyValues) {
                return KeyPrefixOperations<IteratorType, TableType, typename first_n_of<sizeof...(Keys), KeyValuesType>::type>(*Database, keyValues...);
            }

            auto Range() {
                return AnyKeyOperations<IteratorType, TableType>(*Database);
            }

            auto All() {
                return AnyKeyOperations<IteratorType, TableType>(*Database);
            }

            template <typename... Keys>
            auto GreaterOrEqual(Keys... keyValues) {
                return GreaterOrEqualKeyOperations<IteratorType, TableType, typename first_n_of<sizeof...(Keys), KeyValuesType>::type>(*Database, keyValues...);
            }

            template <typename... Keys>
            auto LessOrEqual(Keys... keyValues) {
                return LessOrEqualKeyOperations<IteratorType, TableType, typename first_n_of<sizeof...(Keys), KeyValuesType>::type>(*Database, keyValues...);
            }

            template <typename... ColumnTypes>
            auto Select() {
                return All().template Select<ColumnTypes...>();
            }

            auto Select() {
                return All().Select();
            }

            template <typename... ColumnTypes>
            auto Precharge() {
                return All().template Precharge<ColumnTypes...>();
            }

            auto Precharge() {
                return All().Precharge();
            }

            auto Reverse() const {
                return ReverseSelector(*Database);
            }
        };

        template <typename... KeyColumnsTypes>
        struct TableKey : TableColumns<KeyColumnsTypes...> {
        private:
            template <typename... Ts>
            struct TableKeyMaterializer;

            template <typename T>
            struct TableKeyMaterializer<T> {
                static void Materialize(TToughDb& database) {
                    database.Alter().AddColumnToKey(TableId, T::ColumnId);
                }
            };

            template <typename T, typename... Ts>
            struct TableKeyMaterializer<T, Ts...> : TableKeyMaterializer<Ts...> {
                static void Materialize(TToughDb& database) {
                    TableKeyMaterializer<T>::Materialize(database);
                    TableKeyMaterializer<Ts...>::Materialize(database);
                }
            };

        public:
            static void Materialize(TToughDb& database) {
                TableKeyMaterializer<KeyColumnsTypes...>::Materialize(database);
            }

            template <typename TableType>
            using Selector = TableSelector<NTable::TTableIter, TableType, KeyColumnsTypes...>;
            using KeyColumnsType = std::tuple<KeyColumnsTypes...>;
            using KeyValuesType = typename TableColumns<KeyColumnsTypes...>::TupleType;
            using RealKeyValuesType = typename TableColumns<KeyColumnsTypes...>::RealTupleType;
        };

        template <typename, typename, std::size_t>
        class TTupleToRawTypeValueFixedSize;

        template <typename... ValuesTypes, typename... ColumnsTypes, std::size_t Size>
        class TTupleToRawTypeValueFixedSize<std::tuple<ValuesTypes...>, std::tuple<ColumnsTypes...>, Size> {
        public:
            using TValuesTuple = std::tuple<ValuesTypes...>;
            using TConvertTuple = std::tuple<typename TConvertType<ColumnsTypes, TRawTypeValue, ValuesTypes>::Type...>;
            using TArray = std::array<const TRawTypeValue, Size>;

            TTupleToRawTypeValueFixedSize(const TValuesTuple& values)
                : ConvertTuple(ValuesTuple2ConvertTuple(values))
                , Array(Tuple2Array(ConvertTuple))
            {}

            operator TArrayRef<const TRawTypeValue>() const {
                return Array;
            }

        protected:
            TConvertTuple ConvertTuple;
            TArray Array;

            template <std::size_t...I>
            static TArray Tuple2Array(const TConvertTuple& tuple, std::index_sequence<I...>) {
                return {{static_cast<const TRawTypeValue&>(std::get<I>(tuple))...}};
            }

            static TArray Tuple2Array(const TConvertTuple& tuple) {
                return Tuple2Array(tuple, std::make_index_sequence<sizeof...(ValuesTypes)>());
            }

            template <std::size_t...I>
            static TConvertTuple ValuesTuple2ConvertTuple(const TValuesTuple& tuple, std::index_sequence<I...>) {
                return TConvertTuple(std::get<I>(tuple)...);
            }

            static TConvertTuple ValuesTuple2ConvertTuple(const TValuesTuple& tuple) {
                return ValuesTuple2ConvertTuple(tuple, std::make_index_sequence<sizeof...(ValuesTypes)>());
            }
        };

        template <typename, typename>
        class TTupleToRawTypeValue;

        template <typename... ValuesTypes, typename... ColumnsTypes>
        class TTupleToRawTypeValue<std::tuple<ValuesTypes...>, std::tuple<ColumnsTypes...>> : public TTupleToRawTypeValueFixedSize<std::tuple<ValuesTypes...>, std::tuple<ColumnsTypes...>, sizeof...(ValuesTypes)> {
        public:
            TTupleToRawTypeValue(const std::tuple<ValuesTypes...>& values)
                : TTupleToRawTypeValueFixedSize<std::tuple<ValuesTypes...>, std::tuple<ColumnsTypes...>, sizeof...(ValuesTypes)>(values)
            {}
        };

        template <typename, typename>
        class TTupleToCell;

        template <typename... ValuesTypes, typename... ColumnsTypes>
        class TTupleToCell<std::tuple<ValuesTypes...>, std::tuple<ColumnsTypes...>> {
        public:
            using TValuesTuple = std::tuple<ValuesTypes...>;
            using TConvertTuple = std::tuple<typename TConvertType<ColumnsTypes, TRawTypeValue, ValuesTypes>::Type...>;
            using TArray = std::array<TCell, sizeof...(ValuesTypes)>;

            TTupleToCell(const TValuesTuple& values)
                : ConvertTuple(ValuesTuple2ConvertTuple(values))
                , Array(Tuple2Array(ConvertTuple))
            {}

            TTupleToCell& operator =(const TValuesTuple& values) {
                ConvertTuple = std::move(ValuesTuple2ConvertTuple(values));
                Array = std::move(Tuple2Array(ConvertTuple));
                return *this;
            }

            const TCell* data() const {
                return Array.data();
            }

            auto size() const {
                return Array.size();
            }

        protected:
            TConvertTuple ConvertTuple;
            TArray Array;

            template <std::size_t...I>
            static TArray Tuple2Array(const TConvertTuple& tuple, std::index_sequence<I...>) {
                return {{TCell(&static_cast<const TRawTypeValue&>(std::get<I>(tuple)))...}};
            }

            static TArray Tuple2Array(const TConvertTuple& tuple) {
                return Tuple2Array(tuple, std::make_index_sequence<sizeof...(ValuesTypes)>());
            }

            template <std::size_t...I>
            static TConvertTuple ValuesTuple2ConvertTuple(const TValuesTuple& tuple, std::index_sequence<I...>) {
                return TConvertTuple(std::get<I>(tuple)...);
            }

            static TConvertTuple ValuesTuple2ConvertTuple(const TValuesTuple& tuple) {
                return ValuesTuple2ConvertTuple(tuple, std::make_index_sequence<sizeof...(ValuesTypes)>());
            }
        };

        class Operations {
        public:
            Operations() = default;
            Operations(Operations&&) = default;
            Operations& operator =(Operations&&) = default;

            template <typename IteratorType, typename DeriveType>
            class KeyIterator {
            public:
                KeyIterator(THolder<IteratorType>&& it)
                    : Iterator(std::move(it))
                {}

                KeyIterator(KeyIterator&&) = default;
                KeyIterator& operator =(KeyIterator&&) = default;

                void Init() {
                    if (Iterator)
                        Next(); /* should invoke for the first key */
                }

                bool IsReady() const {
                    return Iterator && Iterator->Last() != NTable::EReady::Page;
                }

                bool IsValid() const {
                    return Iterator->Last() == NTable::EReady::Data;
                }

                bool IsOk() const {
                    return IsReady() && IsValid();
                }

                bool Next() {
                    while (Iterator->Next(NTable::ENext::Data) == NTable::EReady::Data && IsDeleted()) { }
                    return IsReady();
                }

                TDbTupleRef GetValues() const {
                    return Iterator->GetValues();
                }

                TDbTupleRef GetKey() const {
                    return Iterator->GetKey();
                }

                typename NTable::TIteratorStats* Stats() const {
                    return Iterator ? &Iterator->Stats : nullptr;
                }

            private:
                bool IsDeleted() const {
                    Y_DEBUG_ABORT_UNLESS(
                        Iterator->Row().GetRowState() != NTable::ERowOp::Erase,
                        "Unexpected deleted row returned from iterator");
                    return false;
                }

                const DeriveType* AsDerived() const noexcept {
                    return static_cast<const DeriveType*>(this);
                }

            private:
                THolder<IteratorType> Iterator;
            };

            template <typename IteratorType, typename TableType>
            class AnyKeyIterator
                : public KeyIterator<IteratorType, AnyKeyIterator<IteratorType, TableType>>
            {
            public:
                using KeyColumnsType = typename TableType::TKey::KeyColumnsType;
                using Iterator = KeyIterator<IteratorType, AnyKeyIterator<IteratorType, TableType>>;

                AnyKeyIterator(TToughDb& database, NTable::TTagsRef columns)
                    : Iterator(MakeIterator(database, columns))
                {
                    Iterator::Init();
                }

                AnyKeyIterator(AnyKeyIterator&&) = default;
                AnyKeyIterator& operator =(AnyKeyIterator&&) = default;

                static THolder<IteratorType> MakeIterator(
                        TToughDb& database, NTable::TTagsRef columns)
                {
                    if (!Precharger<typename TableType::Precharge>::Precharge(database, TableId, {}, {}, columns, IteratorType::Direction)) {
                        return nullptr;
                    }
                    return THolder<IteratorType>(database.IterateRangeGeneric<IteratorType>(TableId, NTable::TKeyRange{ }, columns).Release());
                }

                static bool Precharge(
                    TToughDb& database,
                    NTable::TTagsRef columns,
                    ui64 maxRowCount,
                    ui64 maxBytes)
                {
                    return Precharger<AutoPrecharge>::Precharge(
                        database,
                        TableId,
                        {},
                        {},
                        columns,
                        IteratorType::Direction,
                        maxRowCount,
                        maxBytes);
                }
            };

            template <typename, typename, typename>
            class EqualPartialKeyIterator;

            template <typename IteratorType, typename TableType, typename... KeyValuesTypes>
            class EqualPartialKeyIterator<IteratorType, TableType, std::tuple<KeyValuesTypes...>>
                : public KeyIterator<IteratorType, EqualPartialKeyIterator<IteratorType, TableType, std::tuple<KeyValuesTypes...>>>
            {
            public:
                using KeyValuesType = std::tuple<KeyValuesTypes...>;
                using KeyColumnsType = typename first_n_of<sizeof...(KeyValuesTypes), typename TableType::TKey::KeyColumnsType>::type;
                using Iterator = KeyIterator<IteratorType, EqualPartialKeyIterator<IteratorType, TableType, KeyValuesType>>;
                static constexpr auto FullKeySize = std::tuple_size<typename TableType::TKey::KeyColumnsType>::value;

                EqualPartialKeyIterator(TToughDb& database, const KeyValuesType& key, NTable::TTagsRef columns)
                    : Iterator(MakeIterator(database, key, columns))
                {
                    Iterator::Init();
                }

                EqualPartialKeyIterator(const EqualPartialKeyIterator&) = delete;

                EqualPartialKeyIterator(EqualPartialKeyIterator&& iterator) = default;
                EqualPartialKeyIterator& operator =(EqualPartialKeyIterator&& iterator) = default;

                static THolder<IteratorType> MakeIterator(
                        TToughDb& database,
                        const KeyValuesType& keyValues,
                        NTable::TTagsRef columns)
                {
                    TTupleToRawTypeValueFixedSize<KeyValuesType, KeyColumnsType, FullKeySize> minKey(keyValues);
                    TTupleToRawTypeValue<KeyValuesType, KeyColumnsType> maxKey(keyValues);
                    if (!Precharger<typename TableType::Precharge>::Precharge(database, TableId, minKey, maxKey, columns, IteratorType::Direction)) {
                        return nullptr;
                    }
                    NTable::TKeyRange range;
                    range.MinKey = minKey;
                    range.MinInclusive = true;
                    range.MaxKey = maxKey;
                    range.MaxInclusive = true;
                    return THolder<IteratorType>(database.IterateRangeGeneric<IteratorType>(TableId, range, columns).Release());
                }

                static bool Precharge(
                    TToughDb& database,
                    const KeyValuesType& keyValues,
                    NTable::TTagsRef columns,
                    ui64 maxRowCount,
                    ui64 maxBytes)
                {
                    TTupleToRawTypeValueFixedSize<KeyValuesType, KeyColumnsType, FullKeySize> minKey(keyValues);
                    TTupleToRawTypeValue<KeyValuesType, KeyColumnsType> maxKey(keyValues);
                    return Precharger<AutoPrecharge>::Precharge(
                        database,
                        TableId,
                        minKey,
                        maxKey,
                        columns,
                        IteratorType::Direction,
                        maxRowCount,
                        maxBytes);
                }
            };

            template <typename, typename, typename>
            class GreaterOrEqualKeyIterator;

            template <typename IteratorType, typename TableType, typename... KeyValuesTypes>
            class GreaterOrEqualKeyIterator<IteratorType, TableType, std::tuple<KeyValuesTypes...>>
                : public KeyIterator<IteratorType, GreaterOrEqualKeyIterator<IteratorType, TableType, std::tuple<KeyValuesTypes...>>>
            {
            public:
                using KeyValuesType = std::tuple<KeyValuesTypes...>;
                using KeyColumnsType = typename first_n_of<sizeof...(KeyValuesTypes), typename TableType::TKey::KeyColumnsType>::type;
                using Iterator = KeyIterator<IteratorType, GreaterOrEqualKeyIterator<IteratorType, TableType, KeyValuesType>>;

                static constexpr auto FullKeySize = std::tuple_size<typename TableType::TKey::KeyColumnsType>::value;

                GreaterOrEqualKeyIterator(TToughDb& database, const KeyValuesType& key, NTable::TTagsRef columns)
                    : Iterator(MakeIterator(database, key, columns))
                {
                    Iterator::Init();
                }

                GreaterOrEqualKeyIterator(GreaterOrEqualKeyIterator&&) = default;
                GreaterOrEqualKeyIterator& operator =(GreaterOrEqualKeyIterator&&) = default;

                static THolder<IteratorType> MakeIterator(
                        TToughDb& database,
                        const KeyValuesType& keyValues,
                        NTable::TTagsRef columns)
                {
                    TTupleToRawTypeValueFixedSize<KeyValuesType, KeyColumnsType, FullKeySize> minKey(keyValues);
                    if (!Precharger<typename TableType::Precharge>::Precharge(database, TableId, minKey, {}, columns, IteratorType::Direction)) {
                        return nullptr;
                    }
                    NTable::TKeyRange range;
                    range.MinKey = minKey;
                    range.MinInclusive = true;
                    range.MaxKey = { };
                    range.MaxInclusive = true;
                    return THolder<IteratorType>(database.IterateRangeGeneric<IteratorType>(TableId, range, columns).Release());
                }

                static bool Precharge(
                    TToughDb& database,
                    const KeyValuesType& keyValues,
                    NTable::TTagsRef columns,
                    ui64 maxRowCount,
                    ui64 maxBytes)
                {
                    TTupleToRawTypeValueFixedSize<KeyValuesType, KeyColumnsType, FullKeySize> minKey(keyValues);
                    return Precharger<AutoPrecharge>::Precharge(
                        database,
                        TableId,
                        minKey,
                        {},
                        columns,
                        IteratorType::Direction,
                        maxRowCount,
                        maxBytes);
                }
            };

            template <typename, typename, typename>
            class LessOrEqualKeyIterator;

            template <typename IteratorType, typename TableType, typename... KeyValuesTypes>
            class LessOrEqualKeyIterator<IteratorType, TableType, std::tuple<KeyValuesTypes...>>
                : public KeyIterator<IteratorType, LessOrEqualKeyIterator<IteratorType, TableType, std::tuple<KeyValuesTypes...>>>
            {
            public:
                using KeyValuesType = std::tuple<KeyValuesTypes...>;
                using KeyColumnsType = typename first_n_of<sizeof...(KeyValuesTypes), typename TableType::TKey::KeyColumnsType>::type;
                using Iterator = KeyIterator<IteratorType, LessOrEqualKeyIterator<IteratorType, TableType, KeyValuesType>>;

                LessOrEqualKeyIterator(TToughDb& database, const KeyValuesType& key, NTable::TTagsRef columns)
                    : Iterator(MakeIterator(database, key, columns))
                {
                    Iterator::Init();
                }

                LessOrEqualKeyIterator(const LessOrEqualKeyIterator&) = delete;

                LessOrEqualKeyIterator(LessOrEqualKeyIterator&& iterator) = default;
                LessOrEqualKeyIterator& operator =(LessOrEqualKeyIterator&& iterator) = default;

                static THolder<IteratorType> MakeIterator(
                        TToughDb& database,
                        const KeyValuesType& keyValues,
                        NTable::TTagsRef columns)
                {
                    TTupleToRawTypeValue<KeyValuesType, KeyColumnsType> maxKey(keyValues);
                    if (!Precharger<typename TableType::Precharge>::Precharge(database, TableId, {}, maxKey, columns, IteratorType::Direction)) {
                        return nullptr;
                    }
                    NTable::TKeyRange range;
                    range.MinKey = { };
                    range.MinInclusive = true;
                    range.MaxKey = maxKey;
                    range.MaxInclusive = true;
                    return THolder<IteratorType>(database.IterateRangeGeneric<IteratorType>(TableId, range, columns).Release());
                }

                static bool Precharge(
                    TToughDb& database,
                    const KeyValuesType& keyValues,
                    NTable::TTagsRef columns,
                    ui64 maxRowCount,
                    ui64 maxBytes)
                {
                    TTupleToRawTypeValue<KeyValuesType, KeyColumnsType> maxKey(keyValues);
                    return Precharger<AutoPrecharge>::Precharge(
                        database,
                        TableId,
                        {},
                        maxKey,
                        columns,
                        IteratorType::Direction,
                        maxRowCount,
                        maxBytes);
                }
            };

            template <typename, typename, typename, typename>
            class RangeKeyIterator;

            template <typename IteratorType, typename TableType, typename... MinKeyValuesTypes, typename... MaxKeyValuesTypes>
            class RangeKeyIterator<IteratorType, TableType, std::tuple<MinKeyValuesTypes...>, std::tuple<MaxKeyValuesTypes...>>
                : public KeyIterator<IteratorType, RangeKeyIterator<IteratorType, TableType, std::tuple<MinKeyValuesTypes...>, std::tuple<MaxKeyValuesTypes...>>>
            {
            public:
                using MinKeyValuesType = std::tuple<MinKeyValuesTypes...>;
                using MinKeyColumnsType = typename first_n_of<sizeof...(MinKeyValuesTypes), typename TableType::TKey::KeyColumnsType>::type;
                using MaxKeyValuesType = std::tuple<MaxKeyValuesTypes...>;
                using MaxKeyColumnsType = typename first_n_of<sizeof...(MaxKeyValuesTypes), typename TableType::TKey::KeyColumnsType>::type;
                using Iterator = KeyIterator<IteratorType, RangeKeyIterator<IteratorType, TableType, MinKeyValuesType, MaxKeyValuesType>>;

                static constexpr auto FullKeySize = std::tuple_size<typename TableType::TKey::KeyColumnsType>::value;

                RangeKeyIterator(TToughDb& database,
                                 const MinKeyValuesType& minKey,
                                 const MaxKeyValuesType& maxKey,
                                 NTable::TTagsRef columns)
                    : Iterator(MakeIterator(database, minKey, maxKey, columns))
                {
                    Iterator::Init();
                }

                RangeKeyIterator(const RangeKeyIterator&) = delete;

                RangeKeyIterator(RangeKeyIterator&& iterator) = default;
                RangeKeyIterator& operator =(RangeKeyIterator&& iterator) = default;

                static THolder<IteratorType> MakeIterator(
                        TToughDb& database,
                        const MinKeyValuesType& minKeyValues,
                        const MaxKeyValuesType& maxKeyValues,
                        NTable::TTagsRef columns)
                {
                    TTupleToRawTypeValueFixedSize<MinKeyValuesType, MinKeyColumnsType, FullKeySize> minKey(minKeyValues);
                    TTupleToRawTypeValue<MaxKeyValuesType, MaxKeyColumnsType> maxKey(maxKeyValues);
                    if (!Precharger<typename TableType::Precharge>::Precharge(database, TableId, minKey, maxKey, columns, IteratorType::Direction)) {
                        return nullptr;
                    }
                    NTable::TKeyRange range;
                    range.MinKey = minKey;
                    range.MinInclusive = true;
                    range.MaxKey = maxKey;
                    range.MaxInclusive = true;
                    return THolder<IteratorType>(database.IterateRangeGeneric<IteratorType>(TableId, range, columns).Release());
                }

                static bool Precharge(TToughDb& database,
                                      const MinKeyValuesType& minKeyValues,
                                      const MaxKeyValuesType& maxKeyValues,
                                      NTable::TTagsRef columns,
                                      ui64 maxRowCount,
                                      ui64 maxBytes)
                {
                    TTupleToRawTypeValueFixedSize<MinKeyValuesType, MinKeyColumnsType, FullKeySize> minKey(minKeyValues);
                    TTupleToRawTypeValue<MaxKeyValuesType, MaxKeyColumnsType> maxKey(maxKeyValues);
                    return Precharger<AutoPrecharge>::Precharge(
                        database,
                        TableId,
                        minKey,
                        maxKey,
                        columns,
                        IteratorType::Direction,
                        maxRowCount,
                        maxBytes);
                }
            };

            template <typename TableType, typename KeyValuesType>
            class EqualKeyIterator
                : public KeyIterator<NTable::TTableIter, EqualKeyIterator<TableType, KeyValuesType>>
            {
            public:
                using KeyColumnsType = typename TableType::TKey::KeyColumnsType;
                using Iterator = KeyIterator<NTable::TTableIter, EqualKeyIterator<TableType, KeyValuesType>>;

                EqualKeyIterator(TToughDb& database, const KeyValuesType& key, NTable::TTagsRef columns)
                    : Iterator(MakeIterator(database, key, columns))
                {
                    Iterator::Init();
                }

                EqualKeyIterator(const EqualKeyIterator&) = delete;

                EqualKeyIterator(EqualKeyIterator&& iterator) = default;
                EqualKeyIterator& operator =(EqualKeyIterator&& iterator) = default;

                static THolder<NTable::TTableIter> MakeIterator(TToughDb& database, const KeyValuesType& keyValues, NTable::TTagsRef columns) {
                    TTupleToRawTypeValue<KeyValuesType, KeyColumnsType> key(keyValues);
                    return THolder<NTable::TTableIter>(database.IterateExact(TableId, key, columns).Release());
                }

                static bool Precharge(
                    TToughDb& database,
                    const KeyValuesType& keyValues,
                    NTable::TTagsRef columns,
                    ui64 maxRowCount,
                    ui64 maxBytes)
                {
                    TTupleToRawTypeValue<KeyValuesType, KeyColumnsType> key(keyValues);
                    return Precharger<AutoPrecharge>::Precharge(
                        database,
                        TableId,
                        key,
                        key,
                        columns,
                        NTable::TTableIter::Direction,
                        maxRowCount,
                        maxBytes);
                }
            };

            template <typename> class ColumnsValueTuple;
            template <typename... KeyColumnsTypes>
            class ColumnsValueTuple<std::tuple<KeyColumnsTypes...>> {
            public:
                template <typename RowsetType>
                static auto Get(RowsetType& rowset) {
                    return rowset.template GetValue<KeyColumnsTypes...>();
                }
            };

            template <typename TableType, typename KeyIterator, typename... ColumnTypes>
            class Rowset {
            public:
                template <typename... Cs>
                static Columns<Cs...> GetColumns(Columns<Cs...>) { return Columns<Cs...>(); }
                template <typename... Cs>
                static Columns<Cs...> GetColumns(Columns<TableColumns<Cs...>>) { return Columns<Cs...>(); }
                using ColumnsType = decltype(GetColumns(Columns<ColumnTypes...>()));

                template <typename... Args>
                Rowset(TToughDb& database, Args&&... args)
                    : Iterator(database, std::forward<Args>(args)...)
                {}

                bool IsReady() const {
                    return Iterator.IsReady();
                }

                bool IsValid() const {
                    return Iterator.IsValid();
                }

                bool IsOk() const {
                    return Iterator.IsReady() && Iterator.IsValid();
                }

                bool EndOfSet() const {
                    return !IsValid();
                }

                bool Next() {
                    return Iterator.Next();
                }

                template <typename... ColumnType>
                auto GetValue() const {
                    Y_DEBUG_ABORT_UNLESS(IsReady(), "Rowset is not ready");
                    Y_DEBUG_ABORT_UNLESS(IsValid(), "Rowset is not valid");
                    typename Columns<ColumnType...>::Type value(GetColumnValue<ColumnType>()...);
                    return value;
                }

                template <typename ColumnType>
                auto GetValueOrDefault(typename ColumnType::Type defaultValue = GetDefaultValue<ColumnType>(SFINAE::special())) const {
                    Y_DEBUG_ABORT_UNLESS(IsReady(), "Rowset is not ready");
                    Y_DEBUG_ABORT_UNLESS(IsValid(), "Rowset is not valid");
                    typename ColumnType::Type value(HaveValue<ColumnType>() ? GetColumnValue<ColumnType>() : defaultValue);
                    return value;
                }

                auto GetKey() const {
                    return ColumnsValueTuple<typename TableType::TKey::KeyColumnsType>::Get(*this);
                }

                template <typename ColumnType>
                bool HaveValue() const {
                    size_t index = GetIndex<ColumnType>();
                    TDbTupleRef tuple = Iterator.GetValues();
                    auto& cell = tuple.Columns[index];
                    return !cell.IsNull();
                }

                TString DbgPrint(const NScheme::TTypeRegistry& typeRegistry) {
                    Y_DEBUG_ABORT_UNLESS(IsReady(), "Rowset is not ready");
                    Y_DEBUG_ABORT_UNLESS(IsValid(), "Rowset is not valid");
                    return DbgPrintTuple(Iterator.GetKey(), typeRegistry) + " -> " + DbgPrintTuple(Iterator.GetValues(), typeRegistry);
                }

                template <typename ColumnType, typename SFINAE::type_check<decltype(ColumnType::Default)>::type = 0>
                static decltype(ColumnType::Default) GetNullValue(SFINAE::special) {
                    return ColumnType::Default;
                }

                template <typename ColumnType>
                static typename ColumnType::Type GetNullValue(SFINAE::general) {
                    return typename ColumnType::Type();
                }

                template <typename ColumnType, typename SFINAE::type_check<decltype(ColumnType::Default)>::type = 0>
                static decltype(ColumnType::Default) GetDefaultValue(SFINAE::special) {
                    return ColumnType::Default;
                }

                template <typename ColumnType>
                static typename ColumnType::Type GetDefaultValue(SFINAE::general) {
                    return typename ColumnType::Type();
                }

                NTable::TIteratorStats* Stats() const {
                    return Iterator.Stats();
                }

            protected:
                template <typename ColumnType>
                static constexpr size_t GetIndex() {
                    return ColumnsType::template GetIndex<ColumnType>();
                }

                template <typename ColumnType>
                typename ColumnType::Type GetColumnValue() const {
                    size_t index = GetIndex<ColumnType>();
                    TDbTupleRef tuple = Iterator.GetValues();
                    auto& cell = tuple.Columns[index];
                    auto type = tuple.Types[index];
                    if (cell.IsNull())
                        return GetNullValue<ColumnType>(SFINAE::special());
                    return TConvert<ColumnType, typename ColumnType::Type>::Convert(TRawTypeValue(cell.Data(), cell.Size(), type));
                }

                KeyIterator Iterator;
            };
        };

        template <typename IteratorType, typename TableType>
        class AnyKeyOperations: public Operations {
        public:
            using KeyColumnsType = typename TableType::TKey::KeyColumnsType;

            template <typename KeyIterator, typename... Columns>
            using Rowset = typename Operations::template Rowset<KeyIterator, Columns...>;
            using Iterator = typename Operations::template AnyKeyIterator<IteratorType, TableType>;

            using ReverseOperations = AnyKeyOperations<ReverseIteratorType<IteratorType>, TableType>;

        protected:
            TToughDb* Database;

        public:
            AnyKeyOperations(TToughDb& database)
                : Database(&database)
            {}

            AnyKeyOperations(AnyKeyOperations&&) = default;
            AnyKeyOperations& operator =(AnyKeyOperations&&) = default;

            template <typename... ColumnTypes>
            auto Select() {
                return Rowset<TableType, Iterator, ColumnTypes...>(*Database, Columns<ColumnTypes...>::GetColumnIds());
            }

            auto Select() {
                return Rowset<TableType, Iterator, typename TableType::TColumns>(*Database, Columns<typename TableType::TColumns>::GetColumnIds());
            }

            template <typename... ColumnTypes>
            bool Precharge(
                ui64 maxRowCount = Max<ui64>(),
                ui64 maxBytes = Max<ui64>())
            {
                return Iterator::Precharge(
                    *Database,
                    Columns<ColumnTypes...>::GetColumnIds(),
                    maxRowCount,
                    maxBytes);
            }

            bool Precharge(
                ui64 maxRowCount = Max<ui64>(),
                ui64 maxBytes = Max<ui64>())
            {
                return Iterator::Precharge(
                    *Database,
                    Columns<typename TableType::TColumns>::GetColumnIds(),
                    maxRowCount,
                    maxBytes);
            }

            auto Reverse() const {
                return ReverseOperations(*Database);
            }
        };

        template <typename IteratorType, typename TableType, typename... KeyValuesTypes>
        class KeyPrefixOperations<IteratorType, TableType, std::tuple<KeyValuesTypes...>>: public Operations {
        public:
            using KeyColumnsType = typename first_n_of<sizeof...(KeyValuesTypes), typename TableType::TKey::KeyColumnsType>::type;
            using KeyValuesType = typename first_n_of<sizeof...(KeyValuesTypes), typename TableType::TKey::RealKeyValuesType>::type;

            template <typename KeyIterator, typename... Columns>
            using Rowset = typename Operations::template Rowset<KeyIterator, Columns...>;
            using Iterator = typename Operations::template EqualPartialKeyIterator<IteratorType, TableType, KeyValuesType>;

            using ReverseOperations = KeyPrefixOperations<ReverseIteratorType<IteratorType>, TableType, std::tuple<KeyValuesTypes...>>;

        protected:
            TToughDb* Database;
            KeyValuesType KeyValues;

        public:
            KeyPrefixOperations(TToughDb& database, KeyValuesTypes... keyValues)
                : Database(&database)
                , KeyValues(keyValues...)
            {}

            explicit KeyPrefixOperations(const ReverseOperations& rhs)
                : Database(rhs.Database)
                , KeyValues(rhs.KeyValues)
            { }

            explicit KeyPrefixOperations(ReverseOperations&& rhs)
                : Database(rhs.Database)
                , KeyValues(std::move(rhs.KeyValues))
            { }

            KeyPrefixOperations(KeyPrefixOperations&&) = default;
            KeyPrefixOperations& operator =(KeyPrefixOperations&&) = default;

            template <typename... ColumnTypes>
            auto Select() {
                return Rowset<TableType, Iterator, ColumnTypes...>(*Database, KeyValues, Columns<ColumnTypes...>::GetColumnIds());
            }

            auto Select() {
                return Rowset<TableType, Iterator, typename TableType::TColumns>(*Database, KeyValues, Columns<typename TableType::TColumns>::GetColumnIds());
            }

            template <typename... ColumnTypes>
            bool Precharge(
                ui64 maxRowCount = Max<ui64>(),
                ui64 maxBytes = Max<ui64>())
            {
                return Iterator::Precharge(
                    *Database,
                    KeyValues,
                    Columns<ColumnTypes...>::GetColumnIds(),
                    maxRowCount,
                    maxBytes);
            }

            bool Precharge(
                ui64 maxRowCount = Max<ui64>(),
                ui64 maxBytes = Max<ui64>())
            {
                return Iterator::Precharge(
                    *Database,
                    KeyValues,
                    Columns<typename TableType::TColumns>::GetColumnIds(),
                    maxRowCount,
                    maxBytes);
            }

            auto Reverse() const & {
                return ReverseOperations(*this);
            }

            auto Reverse() && {
                return ReverseOperations(std::move(*this));
            }
        };

        template <typename IteratorType, typename TableType, typename... KeyValuesTypes>
        class GreaterOrEqualKeyOperations<IteratorType, TableType, std::tuple<KeyValuesTypes...>>: public Operations {
        public:
            using KeyColumnsType = typename TableType::TKey::KeyColumnsType;
            using KeyValuesType = typename first_n_of<sizeof...(KeyValuesTypes), typename TableType::TKey::RealKeyValuesType>::type;

            template <typename KeyIterator, typename... Columns>
            using Rowset = typename Operations::template Rowset<KeyIterator, Columns...>;
            using Iterator = typename Operations::template GreaterOrEqualKeyIterator<IteratorType, TableType, KeyValuesType>;

            using ReverseOperations = GreaterOrEqualKeyOperations<ReverseIteratorType<IteratorType>, TableType, std::tuple<KeyValuesTypes...>>;

        protected:
            TToughDb* Database;
            KeyValuesType KeyValues;

        public:
            GreaterOrEqualKeyOperations(TToughDb& database, KeyValuesTypes... keyValues)
                : Database(&database)
                , KeyValues(keyValues...)
            {}

            explicit GreaterOrEqualKeyOperations(const ReverseOperations& rhs)
                : Database(rhs.Database)
                , KeyValues(rhs.KeyValues)
            { }

            explicit GreaterOrEqualKeyOperations(ReverseOperations&& rhs)
                : Database(rhs.Database)
                , KeyValues(std::move(rhs.KeyValues))
            { }

            GreaterOrEqualKeyOperations(GreaterOrEqualKeyOperations&&) = default;
            GreaterOrEqualKeyOperations& operator =(GreaterOrEqualKeyOperations&&) = default;

            template <typename... Keys>
            auto LessOrEqual(Keys... keyValues) {
                using MinKeyValuesType = KeyValuesType;
                using MaxKeyValuesType = typename first_n_of<sizeof...(Keys), typename TableType::TKey::KeyValuesType>::type;
                return RangeKeyOperations<IteratorType, TableType, MinKeyValuesType, MaxKeyValuesType>(*Database, KeyValues, keyValues...);
            }

            template <typename... ColumnTypes>
            auto Select() {
                return Rowset<TableType, Iterator, ColumnTypes...>(*Database, KeyValues, Columns<ColumnTypes...>::GetColumnIds());
            }

            auto Select() {
                return Rowset<TableType, Iterator, typename TableType::TColumns>(*Database, KeyValues, Columns<typename TableType::TColumns>::GetColumnIds());
            }

            template <typename... ColumnTypes>
            bool Precharge(
                ui64 maxRowCount = Max<ui64>(),
                ui64 maxBytes = Max<ui64>())
            {
                return Iterator::Precharge(
                    *Database,
                    KeyValues,
                    Columns<ColumnTypes...>::GetColumnIds(),
                    maxRowCount,
                    maxBytes);
            }

            bool Precharge(
                ui64 maxRowCount = Max<ui64>(),
                ui64 maxBytes = Max<ui64>())
            {
                return Iterator::Precharge(
                    *Database,
                    KeyValues,
                    Columns<typename TableType::TColumns>::GetColumnIds(),
                    maxRowCount,
                    maxBytes);
            }

            auto Reverse() const & {
                return ReverseOperations(*this);
            }

            auto Reverse() && {
                return ReverseOperations(std::move(*this));
            }
        };

        template <typename IteratorType, typename TableType, typename... KeyValuesTypes>
        class LessOrEqualKeyOperations<IteratorType, TableType, std::tuple<KeyValuesTypes...>>: public Operations {
        public:
            using KeyColumnsType = typename TableType::TKey::KeyColumnsType;
            using KeyValuesType = typename first_n_of<sizeof...(KeyValuesTypes), typename TableType::TKey::RealKeyValuesType>::type;

            template <typename KeyIterator, typename... Columns>
            using Rowset = typename Operations::template Rowset<KeyIterator, Columns...>;
            using Iterator = typename Operations::template LessOrEqualKeyIterator<IteratorType, TableType, KeyValuesType>;

            using ReverseOperations = LessOrEqualKeyOperations<ReverseIteratorType<IteratorType>, TableType, std::tuple<KeyValuesTypes...>>;

        protected:
            TToughDb* Database;
            KeyValuesType KeyValues;
        public:
            LessOrEqualKeyOperations(TToughDb& database, KeyValuesTypes... keyValues)
                : Database(&database)
                , KeyValues(keyValues...)
            {}

            explicit LessOrEqualKeyOperations(const ReverseOperations& rhs)
                : Database(rhs.Database)
                , KeyValues(rhs.KeyValues)
            { }

            explicit LessOrEqualKeyOperations(ReverseOperations&& rhs)
                : Database(rhs.Database)
                , KeyValues(std::move(rhs.KeyValues))
            { }

            LessOrEqualKeyOperations(LessOrEqualKeyOperations&&) = default;
            LessOrEqualKeyOperations& operator =(LessOrEqualKeyOperations&&) = default;

            template <typename... Keys>
            auto GreaterOrEqual(Keys... keyValues) {
                using MinKeyValuesType = typename first_n_of<sizeof...(Keys), typename TableType::TKey::KeyValuesType>::type;
                using MaxKeyValuesType = KeyValuesType;
                return RangeKeyOperations<IteratorType, TableType, MinKeyValuesType, MaxKeyValuesType>(*Database, keyValues..., KeyValues);
            }

            template <typename... ColumnTypes>
            auto Select() {
                return Rowset<TableType, Iterator, ColumnTypes...>(*Database, KeyValues, Columns<ColumnTypes...>::GetColumnIds());
            }

            auto Select() {
                return Rowset<TableType, Iterator, typename TableType::TColumns>(*Database, KeyValues, Columns<typename TableType::TColumns>::GetColumnIds());
            }

            template <typename... ColumnTypes>
            bool Precharge(
                ui64 maxRowCount = Max<ui64>(),
                ui64 maxBytes = Max<ui64>())
            {
                return Iterator::Precharge(
                    *Database,
                    KeyValues,
                    Columns<ColumnTypes...>::GetColumnIds(),
                    maxRowCount,
                    maxBytes);
            }

            bool Precharge(
                ui64 maxRowCount = Max<ui64>(),
                ui64 maxBytes = Max<ui64>())
            {
                return Iterator::Precharge(
                    *Database,
                    KeyValues,
                    Columns<typename TableType::TColumns>::GetColumnIds(),
                    maxRowCount,
                    maxBytes);
            }

            auto Reverse() const & {
                return ReverseOperations(*this);
            }

            auto Reverse() && {
                return ReverseOperations(std::move(*this));
            }
        };

        template <typename IteratorType, typename TableType, typename... MinKeyValuesTypes, typename... MaxKeyValuesTypes>
        class RangeKeyOperations<IteratorType, TableType, std::tuple<MinKeyValuesTypes...>, std::tuple<MaxKeyValuesTypes...>>: public Operations {
        public:
            using KeyColumnsType = typename TableType::TKey::KeyColumnsType;
            using MinKeyValuesType = typename first_n_of<sizeof...(MinKeyValuesTypes), typename TableType::TKey::RealKeyValuesType>::type;
            using MaxKeyValuesType = typename first_n_of<sizeof...(MaxKeyValuesTypes), typename TableType::TKey::RealKeyValuesType>::type;

            template <typename KeyIterator, typename... Columns>
            using Rowset = typename Operations::template Rowset<KeyIterator, Columns...>;
            using Iterator = typename Operations::template RangeKeyIterator<IteratorType, TableType, MinKeyValuesType, MaxKeyValuesType>;

            using ReverseOperations = RangeKeyOperations<ReverseIteratorType<IteratorType>, TableType, std::tuple<MinKeyValuesTypes...>, std::tuple<MaxKeyValuesTypes...>>;

        protected:
            TToughDb* Database;
            MinKeyValuesType MinKeyValues;
            MaxKeyValuesType MaxKeyValues;

        public:
            RangeKeyOperations(TToughDb& database, const MinKeyValuesType& minKeyValues, MaxKeyValuesTypes... maxKeyValues)
                : Database(&database)
                , MinKeyValues(minKeyValues)
                , MaxKeyValues(maxKeyValues...)
            {}

            RangeKeyOperations(TToughDb& database, MinKeyValuesTypes... minKeyValues, const MaxKeyValuesType& maxKeyValues)
                : Database(&database)
                , MinKeyValues(minKeyValues...)
                , MaxKeyValues(maxKeyValues)
            {}

            explicit RangeKeyOperations(const ReverseOperations& rhs)
                : Database(rhs.Database)
                , MinKeyValues(rhs.MinKeyValues)
                , MaxKeyValues(rhs.MaxKeyValues)
            { }

            explicit RangeKeyOperations(ReverseOperations&& rhs)
                : Database(rhs.Database)
                , MinKeyValues(std::move(rhs.MinKeyValues))
                , MaxKeyValues(std::move(rhs.MaxKeyValues))
            { }

            RangeKeyOperations(RangeKeyOperations&&) = default;
            RangeKeyOperations& operator =(RangeKeyOperations&&) = default;

            template <typename... ColumnTypes>
            auto Select() {
                return Rowset<TableType, Iterator, ColumnTypes...>(*Database, MinKeyValues, MaxKeyValues, Columns<ColumnTypes...>::GetColumnIds());
            }

            auto Select() {
                return Rowset<TableType, Iterator, typename TableType::TColumns>(*Database, MinKeyValues, MaxKeyValues, Columns<typename TableType::TColumns>::GetColumnIds());
            }

            template <typename... ColumnTypes>
            bool Precharge(
                ui64 maxRowCount = Max<ui64>(),
                ui64 maxBytes = Max<ui64>())
            {
                return Iterator::Precharge(
                    *Database,
                    MinKeyValues,
                    MaxKeyValues,
                    Columns<ColumnTypes...>::GetColumnIds(),
                    maxRowCount,
                    maxBytes);
            }

            bool Precharge(
                ui64 maxRowCount = Max<ui64>(),
                ui64 maxBytes = Max<ui64>())
            {
                return Iterator::Precharge(
                    *Database,
                    MinKeyValues,
                    MaxKeyValues,
                    Columns<typename TableType::TColumns>::GetColumnIds(),
                    maxRowCount,
                    maxBytes);
            }

            auto Reverse() const & {
                return ReverseOperations(*this);
            }

            auto Reverse() && {
                return ReverseOperations(std::move(*this));
            }
        };

        template <typename TableType, typename... KeyValuesTypes>
        class KeyOperations<TableType, std::tuple<KeyValuesTypes...>>: public Operations {
        public:
            using KeyColumnsType = typename TableType::TKey::KeyColumnsType;
            using KeyValuesType = typename TableType::TKey::RealKeyValuesType;

            template <typename KeyIterator, typename... Columns>
            using Rowset = typename Operations::template Rowset<KeyIterator, Columns...>;
            using Iterator = typename Operations::template EqualKeyIterator<TableType, KeyValuesType>;

        protected:
            TToughDb* Database;
            KeyValuesType KeyValues;

        public:
            KeyOperations(TToughDb& database, KeyValuesTypes... keyValues)
                : Database(&database)
                , KeyValues(keyValues...)
            {}

            KeyOperations(TToughDb& database, const KeyValuesType& keyValues)
                : Database(&database)
                , KeyValues(keyValues)
            {}

            KeyOperations(KeyOperations&&) = default;
            KeyOperations& operator =(KeyOperations&&) = default;

            template <typename... ColumnTypes>
            auto Select() {
                // TODO
                //static_assert(have columns?, "fail");
                return Rowset<TableType, Iterator, ColumnTypes...>(*Database, KeyValues, Columns<ColumnTypes...>::GetColumnIds());
            }

            auto Select() {
                return Rowset<TableType, Iterator, typename TableType::TColumns>(*Database, KeyValues, Columns<typename TableType::TColumns>::GetColumnIds());
            }

            template <typename... ColumnTypes>
            bool Precharge(
                ui64 maxRowCount = Max<ui64>(),
                ui64 maxBytes = Max<ui64>())
            {
                return Iterator::Precharge(
                    *Database,
                    KeyValues,
                    Columns<ColumnTypes...>::GetColumnIds(),
                    maxRowCount,
                    maxBytes);
            }

            bool Precharge(
                ui64 maxRowCount = Max<ui64>(),
                ui64 maxBytes = Max<ui64>())
            {
                return Iterator::Precharge(
                    *Database,
                    KeyValues,
                    Columns<typename TableType::TColumns>::GetColumnIds(),
                    maxRowCount,
                    maxBytes);
            }

            template <typename... ColumnTypes>
            KeyOperations& Update(const typename ColumnTypes::Type&... value) {
                return Update(TUpdate<ColumnTypes>(value)...);
            }

            template <typename... ColumnTypes>
            KeyOperations& UpdateToNull() {
                return Update(TNull<ColumnTypes>()...);
            }

            template <typename... UpdateTypes>
            KeyOperations& Update(const UpdateTypes&... updates) {
                std::array<TUpdateOp, sizeof...(UpdateTypes)> update_ops = {{updates...}};
                Database->Update(TableId, NTable::ERowOp::Upsert, TTupleToRawTypeValue<KeyValuesType, KeyColumnsType>(KeyValues), update_ops);
                return *this;
            }

            void Update() {
                Database->Update(TableId, NTable::ERowOp::Upsert, TTupleToRawTypeValue<KeyValuesType, KeyColumnsType>(KeyValues), { });
            }

            void Delete() {
                Database->Update(TableId, NTable::ERowOp::Erase, TTupleToRawTypeValue<KeyValuesType, KeyColumnsType>(KeyValues), { });
            }

            template <typename... ColumnTypes>
            KeyOperations& UpdateV(const TRowVersion& rowVersion, const typename ColumnTypes::Type&... value) {
                return UpdateV(rowVersion, TUpdate<ColumnTypes>(value)...);
            }

            template <typename... UpdateTypes>
            KeyOperations& UpdateV(const TRowVersion& rowVersion, const UpdateTypes&... updates) {
                std::array<TUpdateOp, sizeof...(UpdateTypes)> update_ops = {{updates...}};
                Database->Update(TableId, NTable::ERowOp::Upsert, TTupleToRawTypeValue<KeyValuesType, KeyColumnsType>(KeyValues), update_ops, rowVersion);
                return *this;
            }

            void DeleteV(const TRowVersion& rowVersion) {
                Database->Update(TableId, NTable::ERowOp::Erase, TTupleToRawTypeValue<KeyValuesType, KeyColumnsType>(KeyValues), { }, rowVersion);
            }
        };
    };

    template <typename... Settings>
    struct SchemaSettings {
        static void Materialize(TToughDb& database) {
            (Settings::Materialize(database), ...);
        }
    };

    template <bool Allow = false>
    struct ExecutorLogBatching {
        static void Materialize(TToughDb& database) {
            database.Alter().SetExecutorAllowLogBatching(Allow);
        }
    };

    template <TDuration::TValue Period = 0>
    struct ExecutorLogFlushPeriod {
        static void Materialize(TToughDb& database) {
            database.Alter().SetExecutorLogFlushPeriod(TDuration::MicroSeconds(Period));
        }
    };

    template <ui32 LimitTxInFly = 0>
    struct ExecutorLimitInFlyTx {
        static void Materialize(TToughDb& database) {
            database.Alter().SetExecutorLimitInFlyTx(LimitTxInFly);
        }
    };

    template <ui64 CacheSize>
    struct ExecutorCacheSize {
        static void Materialize(TToughDb& database) {
            database.Alter().SetExecutorCacheSize(CacheSize);
        }
    };

    template <typename Type, typename... Types>
    struct SchemaTables: SchemaTables<Types...> {
        static bool Precharge(TToughDb& database) {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wbitwise-instead-of-logical"
            return SchemaTables<Type>::Precharge(database) & SchemaTables<Types...>::Precharge(database);
#pragma clang diagnostic pop
        }

        static void Materialize(TToughDb& database, EMaterializationMode mode = EMaterializationMode::All) {
            SchemaTables<Type>::Materialize(database, mode);
            SchemaTables<Types...>::Materialize(database, mode);
        }

        static void Cleanup(TToughDb& database) {
            SchemaTables<Type>::Cleanup(database);
            SchemaTables<Types...>::Cleanup(database);
        }

        static bool HaveTable(ui32 tableId) {
            return SchemaTables<Type>::HaveTable(tableId) || SchemaTables<Types...>::HaveTable(tableId);
        }
    };

    template <typename Type>
    struct SchemaTables<Type> {
        static TString GetTableName(const TString& typeName) {
            return typeName.substr(typeName.rfind(':') + 1);
        }

        static bool Precharge(TToughDb& database) {
            return typename Type::TKey::template Selector<Type>(database).Precharge();
        }

        static void Materialize(TToughDb& database, EMaterializationMode mode = EMaterializationMode::All) {
            switch (mode) {
            case EMaterializationMode::All:
                break;
            case EMaterializationMode::Existing:
                if (!database.GetScheme().GetTableInfo(Type::TableId)) {
                    return;
                }
                break;
            case EMaterializationMode::NonExisting:
                if (database.GetScheme().GetTableInfo(Type::TableId)) {
                    return;
                }
                break;
            }

            database.Alter().AddTable(GetTableName(TypeName<Type>()), Type::TableId);
            Type::TColumns::Materialize(database);
            Type::TKey::Materialize(database);
        }

        static void Cleanup(TToughDb& database) {
            auto* table = database.GetScheme().GetTableInfo(Type::TableId);
            if (table != nullptr) {
                for (const auto& column : table->Columns) {
                    if (!Type::TColumns::HaveColumn(column.first)) {
                        database.Alter().DropColumn(Type::TableId, column.first);
                    }
                }
            }
        }

        static bool HaveTable(ui32 tableId) {
            return Type::TableId == tableId;
        }
    };

    using TSettings = SchemaSettings<>;
};

template <>
inline bool Schema::Precharger<Schema::AutoPrecharge>::Precharge(
        TToughDb& database, ui32 table,
        NTable::TRawVals minKey, NTable::TRawVals maxKey,
        NTable::TTagsRef columns, NTable::EDirection direction, ui64 maxRowCount, ui64 maxBytes)
{
    return database.Precharge(table, minKey, maxKey, columns, 0, maxRowCount, maxBytes, direction);
}

template <>
inline bool Schema::Precharger<Schema::NoAutoPrecharge>::Precharge(
        TToughDb&, ui32,
        NTable::TRawVals, NTable::TRawVals,
        NTable::TTagsRef, NTable::EDirection, ui64, ui64)
{
    return true;
}

class TNiceDb {
public:
    TNiceDb(TToughDb& database)
        : Database(database)
    {}

    TToughDb& GetDatabase() const {
        return Database;
    }

    template <typename TableType> typename TableType::TKey::template Selector<TableType> Table() { return Database; }

    template <typename TableType>
    bool HaveTable() {
        return Database.GetScheme().GetTableInfo(TableType::TableId);
    }

    template <typename SchemaType>
    bool Precharge() {
        return SchemaType::TTables::Precharge(Database);
    }

    template <typename SchemaType>
    void Materialize() {
        SchemaType::TSettings::Materialize(Database);
        SchemaType::TTables::Materialize(Database, EMaterializationMode::All);
    }

    template <typename SchemaType>
    void MaterializeExisting() {
        SchemaType::TTables::Materialize(Database, EMaterializationMode::Existing);
    }

    template <typename SchemaType>
    void MaterializeNonExisting() {
        SchemaType::TTables::Materialize(Database, EMaterializationMode::NonExisting);
    }

    template <typename SchemaType>
    void Cleanup() {
        SchemaType::TTables::Cleanup(Database);

        for (const auto& table : Database.GetScheme().Tables) {
            if (!SchemaType::TTables::HaveTable(table.first)) {
                Database.Alter().DropTable(table.first);
            }
        }
    }

    void NoMoreReadsForTx() {
        return Database.NoMoreReadsForTx();
    }

protected:
    TToughDb& Database;
};

namespace NHelpers {

// Fills NTable::TScheme::TTableSchema from static NIceDb::Schema
template <class TTable>
struct TStaticSchemaFiller {
    template <typename...>
    struct TFiller;

    template <typename Column>
    struct TFiller<Column> {
        static void Fill(NTable::TScheme::TTableSchema& schema) {
            schema.Columns[Column::ColumnId] = NTable::TColumn(
                TTable::template TableColumns<Column>::GetColumnName(),
                Column::ColumnId,
                NScheme::TTypeInfo(Column::ColumnType), "");
        }
    };

    template <typename Column, typename... Columns>
    struct TFiller<Column, Columns...> {
        static void Fill(NTable::TScheme::TTableSchema& schema) {
            TFiller<Column>::Fill(schema);
            TFiller<Columns...>::Fill(schema);
        }
    };

    template <typename... Columns>
    using TColumnsType = typename TTable::template TableColumns<Columns...>;

    template <typename... Columns>
    static void FillColumns(NTable::TScheme::TTableSchema& schema, TColumnsType<Columns...>) {
        TFiller<Columns...>::Fill(schema);
    }

    template <typename...>
    struct TKeyFiller;

    template <typename Key>
    struct TKeyFiller<Key> {
        static void Fill(NTable::TScheme::TTableSchema& schema, i32 index) {
            schema.KeyColumns.push_back(Key::ColumnId);
            auto& column = schema.Columns[Key::ColumnId];
            column.KeyOrder = index;
        }
    };

    template <typename Key, typename... Keys>
    struct TKeyFiller<Key, Keys...> {
        static void Fill(NTable::TScheme::TTableSchema& schema, i32 index) {
            TKeyFiller<Key>::Fill(schema, index);
            TKeyFiller<Keys...>::Fill(schema, index + 1);
        }
    };

    template <typename... Keys>
    using TKeysType = typename TTable::template TableKey<Keys...>;

    template <typename... Keys>
    static void FillKeys(NTable::TScheme::TTableSchema& schema, TKeysType<Keys...>) {
        TKeyFiller<Keys...>::Fill(schema, 0);
    }

    static void Fill(NTable::TScheme::TTableSchema& schema) {
        FillColumns(schema, typename TTable::TColumns());
        FillKeys(schema, typename TTable::TKey());

        for (const auto& c : schema.Columns) {
            schema.ColumnNames[c.second.Name] = c.second.Id;
        }
    }
};

} // namespace NHelpers

}
}
