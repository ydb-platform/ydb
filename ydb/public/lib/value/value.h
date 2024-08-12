#pragma once

#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <google/protobuf/text_format.h>

namespace NKikimr {
namespace NClient {

template <typename ParameterType> struct SchemeMapper {};
template <> struct SchemeMapper<ui32> { static constexpr NScheme::TTypeId SchemeType = NScheme::NTypeIds::Uint32; };
template <> struct SchemeMapper<i32> { static constexpr NScheme::TTypeId SchemeType = NScheme::NTypeIds::Int32; };
template <> struct SchemeMapper<ui64> { static constexpr NScheme::TTypeId SchemeType = NScheme::NTypeIds::Uint64; };
template <> struct SchemeMapper<i64> { static constexpr NScheme::TTypeId SchemeType = NScheme::NTypeIds::Int64; };
template <> struct SchemeMapper<TString> { static constexpr NScheme::TTypeId SchemeType = NScheme::NTypeIds::Utf8; };
template <> struct SchemeMapper<const char*> { static constexpr NScheme::TTypeId SchemeType = NScheme::NTypeIds::Utf8; };
template <> struct SchemeMapper<bool> { static constexpr NScheme::TTypeId SchemeType = NScheme::NTypeIds::Bool; };
template <> struct SchemeMapper<double> { static constexpr NScheme::TTypeId SchemeType = NScheme::NTypeIds::Double; };
template <> struct SchemeMapper<float> { static constexpr NScheme::TTypeId SchemeType = NScheme::NTypeIds::Float; };

template <NScheme::TTypeId SchemeType> struct ProtoMapper {};
template <> struct ProtoMapper<NScheme::NTypeIds::Int32> { static constexpr void (NKikimrMiniKQL::TValue::* Setter)(i32) = &NKikimrMiniKQL::TValue::SetInt32; };
template <> struct ProtoMapper<NScheme::NTypeIds::Uint32> { static constexpr void (NKikimrMiniKQL::TValue::* Setter)(ui32) = &NKikimrMiniKQL::TValue::SetUint32; };
template <> struct ProtoMapper<NScheme::NTypeIds::Int64> { static constexpr void (NKikimrMiniKQL::TValue::* Setter)(i64) = &NKikimrMiniKQL::TValue::SetInt64; };
template <> struct ProtoMapper<NScheme::NTypeIds::Uint64> { static constexpr void (NKikimrMiniKQL::TValue::* Setter)(ui64) = &NKikimrMiniKQL::TValue::SetUint64; };
template <> struct ProtoMapper<NScheme::NTypeIds::Utf8> { static constexpr void (NKikimrMiniKQL::TValue::* Setter)(const TString&) = &NKikimrMiniKQL::TValue::SetText; };
template <> struct ProtoMapper<NScheme::NTypeIds::String> { static constexpr void (NKikimrMiniKQL::TValue::* Setter)(const TString&) = &NKikimrMiniKQL::TValue::SetBytes; };
template <> struct ProtoMapper<NScheme::NTypeIds::String4k> { static constexpr void (NKikimrMiniKQL::TValue::* Setter)(const TString&) = &NKikimrMiniKQL::TValue::SetBytes; };
template <> struct ProtoMapper<NScheme::NTypeIds::String2m> { static constexpr void (NKikimrMiniKQL::TValue::* Setter)(const TString&) = &NKikimrMiniKQL::TValue::SetBytes; };
template <> struct ProtoMapper<NScheme::NTypeIds::Bool> { static constexpr void (NKikimrMiniKQL::TValue::* Setter)(bool) = &NKikimrMiniKQL::TValue::SetBool; };
template <> struct ProtoMapper<NScheme::NTypeIds::Double> { static constexpr void (NKikimrMiniKQL::TValue::* Setter)(double) = &NKikimrMiniKQL::TValue::SetDouble; };
template <> struct ProtoMapper<NScheme::NTypeIds::Float> { static constexpr void (NKikimrMiniKQL::TValue::* Setter)(float) = &NKikimrMiniKQL::TValue::SetFloat; };

template <typename ValueType, NScheme::TTypeId SchemeType = SchemeMapper<ValueType>::SchemeType>
class TDataValue {
public:
    ValueType Value;

    explicit TDataValue(ValueType value)
        : Value(value)
    {}

    static void StoreType(NKikimrMiniKQL::TType& type) {
        type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
        type.MutableData()->SetScheme(SchemeType);
    }

    void StoreValue(NKikimrMiniKQL::TValue& value) const {
        (value.*ProtoMapper<SchemeType>::Setter)(Value);
    }
};

class TValue {
public:
    // generic universal handler of result values of MiniKQL programs execution
    // it helps access to different values using short and intuitive syntax
    // for examples see cpp_ut.cpp
    static TValue Create(const NKikimrMiniKQL::TValue& value, const NKikimrMiniKQL::TType& type);
    static TValue Create(const NKikimrMiniKQL::TResult& result);

    // check for value existence (in optional)
    bool HaveValue() const;
    bool IsNull() const;
    // named indexer - for struct members and dictionaries
    TValue operator [](const char* name) const;
    TValue operator [](const TStringBuf name) const;
    // numeric indexer - for lists and tuples
    TValue operator [](int index) const;
    // accessors
    operator bool() const;
    operator ui8() const;
    operator i8() const;
    operator ui16() const;
    operator i16() const;
    operator ui64() const;
    operator i64() const;
    operator ui32() const;
    operator i32() const;
    operator double() const;
    operator float() const;
    operator TString() const;
    // returns size of container values (like list or tuple)
    size_t Size() const;
    // gets NScheme::NTypeIds enum value of 'Data' type
    NScheme::TTypeId GetDataType() const;
    // gets text representation of simple 'Data' types
    TString GetDataText() const;

    // gets text representation of simple 'Pg' types
    TString GetPgText() const;
    // gets text representation of simple 'Data' and 'Pg' types
    TString GetSimpleValueText() const;

    // returns text representation of value's type
    template <typename Format> TString GetTypeText(const Format& format = Format()) const;
    // returns text representation of value itself
    template <typename Format> TString GetValueText(const Format& format = Format()) const;
    // returns  member name by index
    TString GetMemberName(int index) const;
    // returns all members
    TVector<TString> GetMembersNames() const;
    // returns member index by name
    int GetMemberIndex(TStringBuf name) const;

    TString DumpToString() const {
        TStringBuilder dump;
        TString res;
        ::google::protobuf::TextFormat::PrintToString(Type, &res);
        dump << "Type:" << Endl << res << Endl;
        ::google::protobuf::TextFormat::PrintToString(Value, &res);
        dump << "Value:" << Endl << res << Endl;
        return std::move(dump);
    }

    void DumpValue() const {
        Cerr << DumpToString();
    }

    const NKikimrMiniKQL::TType& GetType() const { return Type; };
    const NKikimrMiniKQL::TValue& GetValue() const { return Value; };

protected:
    TValue(NKikimrMiniKQL::TValue& value, NKikimrMiniKQL::TType& type);
    TValue EatOptional() const;

    static const NKikimrMiniKQL::TValue Null;
    NKikimrMiniKQL::TType& Type;
    NKikimrMiniKQL::TValue& Value;
};

class TWriteValue : public TValue {
public:
    static TWriteValue Create(NKikimrMiniKQL::TValue& value, NKikimrMiniKQL::TType& type);
    // named indexer - for struct members and dictionaries
    TWriteValue operator [](const char* name);
    TWriteValue operator [](const TStringBuf name);
    // numeric indexer - for lists and tuples
    TWriteValue operator [](int index);
    // accessors
    TWriteValue Optional();
    TWriteValue AddListItem();
    TWriteValue AddTupleItem();
    TWriteValue& Void();
    TWriteValue& Bytes(const char* value);
    TWriteValue& Bytes(const TString& value);
    TWriteValue& Yson(const char* value);
    TWriteValue& Yson(const TString& value);
    TWriteValue& Json(const char* value);
    TWriteValue& Json(const TString& value);
    TWriteValue& Date(ui16 value);
    TWriteValue& Datetime(ui32 value);
    TWriteValue& Timestamp(ui64 value);
    TWriteValue& Interval(i64 value);
    TWriteValue& Date32(i32 value);
    TWriteValue& Datetime64(i64 value);
    TWriteValue& Timestamp64(i64 value);
    TWriteValue& Interval64(i64 value);
    TWriteValue& operator =(bool value);
    TWriteValue& operator =(ui8 value);
    TWriteValue& operator =(i8 value);
    TWriteValue& operator =(ui16 value);
    TWriteValue& operator =(i16 value);
    TWriteValue& operator =(ui64 value);
    TWriteValue& operator =(i64 value);
    TWriteValue& operator =(ui32 value);
    TWriteValue& operator =(i32 value);
    TWriteValue& operator =(double value);
    TWriteValue& operator =(float value);
    TWriteValue& operator =(const char* value);
    TWriteValue& operator =(const TString& value);
    TWriteValue& operator =(const TValue& value);

    template <typename ParameterType, NScheme::TTypeId SchemeType = SchemeMapper<ParameterType>::SchemeType>
    TWriteValue& EmptyData() {
        TDataValue<ParameterType, SchemeType>::StoreType(Type);
        return *this;
    }

    template <typename ItemType, NScheme::TTypeId SchemeType = SchemeMapper<ItemType>::SchemeType>
    TWriteValue& EmptyOptional() {
        Type.SetKind(NKikimrMiniKQL::ETypeKind::Optional);

        auto& optionalType = *Type.MutableOptional()->MutableItem();
        optionalType.SetKind(NKikimrMiniKQL::ETypeKind::Data);
        optionalType.MutableData()->SetScheme(SchemeType);

        return *this;
    }

    template <typename ItemType, NScheme::TTypeId SchemeType = SchemeMapper<ItemType>::SchemeType>
    TWriteValue& EmptyList() {
        Type.SetKind(NKikimrMiniKQL::ETypeKind::List);

        auto& itemType = *Type.MutableList()->MutableItem();
        itemType.SetKind(NKikimrMiniKQL::ETypeKind::Data);
        itemType.MutableData()->SetScheme(SchemeType);

        return *this;
    }

    ui32 GetValueBytesSize() const;
    ui32 GetTypeBytesSize() const;
    ui32 GetBytesSize() const;

protected:
    TWriteValue(NKikimrMiniKQL::TValue& value, NKikimrMiniKQL::TType& type);
};

struct TFormatCxx {};

struct TFormatJSON {
    bool UI64AsString;
    bool BinaryAsBase64;

    TFormatJSON(bool ui64AsString = false, bool binaryAsBase64 = false)
        : UI64AsString(ui64AsString)
        , BinaryAsBase64(binaryAsBase64)
    {}
};

struct TFormatRowset {};
struct TFormatCSV {
    TFormatCSV(TString delim = ",", bool printHeader = false)
        : Delim(delim)
        , PrintHeader(printHeader)
    {}

    static TString EscapeString(const TString& s);

    TString Delim;
    bool PrintHeader;
};

} // namespace NClient
} // namespace NKikimr
