#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

#include <memory>

namespace Ydb {
    class Type;
    class Value;
}

namespace NYdb {

class TResultSetParser;

//! Representation of YDB type.
class TType {
    friend class TProtoAccessor;
public:
    TType(const Ydb::Type& typeProto);
    TType(Ydb::Type&& typeProto);

    TString ToString() const;
    void Out(IOutputStream& o) const;

    const Ydb::Type& GetProto() const;
    Ydb::Type& GetProto();

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

enum class EPrimitiveType {
    Bool         = 0x0006,
    Int8         = 0x0007,
    Uint8        = 0x0005,
    Int16        = 0x0008,
    Uint16       = 0x0009,
    Int32        = 0x0001,
    Uint32       = 0x0002,
    Int64        = 0x0003,
    Uint64       = 0x0004,
    Float        = 0x0021,
    Double       = 0x0020,
    Date         = 0x0030,
    Datetime     = 0x0031,
    Timestamp    = 0x0032,
    Interval     = 0x0033,
    Date32       = 0x0040,
    Datetime64   = 0x0041,
    Timestamp64  = 0x0042,
    Interval64   = 0x0043,
    TzDate       = 0x0034,
    TzDatetime   = 0x0035,
    TzTimestamp  = 0x0036,
    String       = 0x1001,
    Utf8         = 0x1200,
    Yson         = 0x1201,
    Json         = 0x1202,
    Uuid         = 0x1203,
    JsonDocument = 0x1204,
    DyNumber     = 0x1302,
};

struct TDecimalType {
    ui8 Precision;
    ui8 Scale;

    TDecimalType(ui8 precision, ui8 scale)
        : Precision(precision)
        , Scale(scale) {}
};

struct TPgType {
    TString TypeName;
    TString TypeModifier;

    ui32 Oid = 0;
    i16 Typlen = 0;
    i32 Typmod = 0;

    TPgType(const TString& typeName, const TString& typeModifier = {})
        : TypeName(typeName)
        , TypeModifier(typeModifier)
    {}
};

//! Types can be complex, so TTypeParser allows to traverse through this hierarchies.
class TTypeParser : public TMoveOnly {
    friend class TValueParser;
public:
    enum class ETypeKind {
        Primitive,
        Decimal,
        Optional,
        List,
        Tuple,
        Struct,
        Dict,
        Variant,
        Void,
        Null,
        EmptyList,
        EmptyDict,
        Tagged,
        Pg
    };

public:
    TTypeParser(TTypeParser&&);
    TTypeParser(const TType& type);

    ~TTypeParser();

    ETypeKind GetKind() const;

    EPrimitiveType GetPrimitive() const;
    TDecimalType GetDecimal() const;
    TPgType GetPg() const;

    // Optional
    void OpenOptional();
    void CloseOptional();

    // List
    void OpenList();
    void CloseList();

    // Struct
    void OpenStruct();
    bool TryNextMember();
    const TString& GetMemberName();
    void CloseStruct();

    // Tuple
    void OpenTuple();
    bool TryNextElement();
    void CloseTuple();

    // Dict
    void OpenDict();
    void DictKey();
    void DictPayload();
    void CloseDict();

    // Variant
    void OpenVariant(size_t index);
    void OpenVariant();
    void CloseVariant();

    // Tagged
    void OpenTagged();
    const TString& GetTag();
    void CloseTagged();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

bool TypesEqual(const TType& t1, const TType& t2);

TString FormatType(const TType& type);

//! Used to create arbitrary type.
//! To create complex type, corresponding scope should be opened by Begin*/End* calls
//! To create complex repeated type, Add* should be called at least once
class TTypeBuilder : public TMoveOnly {
    friend class TValueBuilderImpl;
public:
    TTypeBuilder(TTypeBuilder&&);
    TTypeBuilder();

    ~TTypeBuilder();

    TTypeBuilder& Primitive(const EPrimitiveType& primitiveType);
    TTypeBuilder& Decimal(const TDecimalType& decimalType);
    TTypeBuilder& Pg(const TPgType& pgType);

    // Optional
    TTypeBuilder& BeginOptional();
    TTypeBuilder& EndOptional();
    TTypeBuilder& Optional(const TType& itemType);

    // List
    TTypeBuilder& BeginList();
    TTypeBuilder& EndList();
    TTypeBuilder& List(const TType& itemType);

    // Struct
    TTypeBuilder& BeginStruct();
    TTypeBuilder& AddMember(const TString& memberName);
    TTypeBuilder& AddMember(const TString& memberName, const TType& memberType);
    TTypeBuilder& EndStruct();

    // Tuple
    TTypeBuilder& BeginTuple();
    TTypeBuilder& AddElement();
    TTypeBuilder& AddElement(const TType& elementType);
    TTypeBuilder& EndTuple();

    // Dict
    TTypeBuilder& BeginDict();
    TTypeBuilder& DictKey();
    TTypeBuilder& DictKey(const TType& keyType);
    TTypeBuilder& DictPayload();
    TTypeBuilder& DictPayload(const TType& payloadType);
    TTypeBuilder& EndDict();

    // Tagged
    TTypeBuilder& BeginTagged(const TString& tag);
    TTypeBuilder& EndTagged();
    TTypeBuilder& Tagged(const TString& tag, const TType& itemType);

    TType Build();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

struct TDecimalValue {
    TString ToString() const;
    TDecimalValue(const Ydb::Value& decimalValueProto, const TDecimalType& decimalType);
    TDecimalValue(const TString& decimalString, ui8 precision = 22, ui8 scale = 9);

    TDecimalType DecimalType_;
    ui64 Low_;
    i64 Hi_;
};

struct TPgValue {
    enum EPgValueKind {
        VK_NULL,
        VK_TEXT,
        VK_BINARY
    };

    TPgValue(const Ydb::Value& pgValueProto, const TPgType& pgType);
    TPgValue(EPgValueKind kind, const TString& content, const TPgType& pgType);
    bool IsNull() const;
    bool IsText() const;

    TPgType PgType_;
    EPgValueKind Kind_ = VK_NULL;
    TString Content_;
};

struct TUuidValue {
    TString ToString() const;
    TUuidValue(ui64 low_128, ui64 high_128);
    TUuidValue(const Ydb::Value& uuidValueProto);
    TUuidValue(const TString& uuidString);

    union {
        char Bytes[16];
        ui64 Halfs[2];
    } Buf_;
};

//! Representation of YDB value.
class TValue {
    friend class TValueParser;
    friend class TProtoAccessor;
public:
    TValue(const TType& type, const Ydb::Value& valueProto);
    TValue(const TType& type, Ydb::Value&& valueProto);

    const TType& GetType() const;
    TType & GetType();

    const Ydb::Value& GetProto() const;
    Ydb::Value& GetProto();

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

class TValueParser : public TMoveOnly {
    friend class TResultSetParser;
public:
    TValueParser(TValueParser&&);
    TValueParser(const TValue& value);

    ~TValueParser();

    TTypeParser::ETypeKind GetKind() const;
    EPrimitiveType GetPrimitiveType() const;

    bool GetBool() const;
    i8 GetInt8() const;
    ui8 GetUint8() const;
    i16 GetInt16() const;
    ui16 GetUint16() const;
    i32 GetInt32() const;
    ui32 GetUint32() const;
    i64 GetInt64() const;
    ui64 GetUint64() const;
    float GetFloat() const;
    double GetDouble() const;
    TInstant GetDate() const;
    TInstant GetDatetime() const;
    TInstant GetTimestamp() const;
    i64 GetInterval() const;
    i32 GetDate32() const;
    i64 GetDatetime64() const;
    i64 GetTimestamp64() const;
    i64 GetInterval64() const;
    const TString& GetTzDate() const;
    const TString& GetTzDatetime() const;
    const TString& GetTzTimestamp() const;
    const TString& GetString() const;
    const TString& GetUtf8() const;
    const TString& GetYson() const;
    const TString& GetJson() const;
    TDecimalValue GetDecimal() const;
    TPgValue GetPg() const;
    TUuidValue GetUuid() const;
    const TString& GetJsonDocument() const;
    const TString& GetDyNumber() const;

    TMaybe<bool> GetOptionalBool() const;
    TMaybe<i8> GetOptionalInt8() const;
    TMaybe<ui8> GetOptionalUint8() const;
    TMaybe<i16> GetOptionalInt16() const;
    TMaybe<ui16> GetOptionalUint16() const;
    TMaybe<i32> GetOptionalInt32() const;
    TMaybe<ui32> GetOptionalUint32() const;
    TMaybe<i64> GetOptionalInt64() const;
    TMaybe<ui64> GetOptionalUint64() const;
    TMaybe<float> GetOptionalFloat() const;
    TMaybe<double> GetOptionalDouble() const;
    TMaybe<TInstant> GetOptionalDate() const;
    TMaybe<TInstant> GetOptionalDatetime() const;
    TMaybe<TInstant> GetOptionalTimestamp() const;
    TMaybe<i64> GetOptionalInterval() const;
    TMaybe<i32> GetOptionalDate32() const;
    TMaybe<i64> GetOptionalDatetime64() const;
    TMaybe<i64> GetOptionalTimestamp64() const;
    TMaybe<i64> GetOptionalInterval64() const;
    TMaybe<TString> GetOptionalTzDate() const;
    TMaybe<TString> GetOptionalTzDatetime() const;
    TMaybe<TString> GetOptionalTzTimestamp() const;
    TMaybe<TString> GetOptionalString() const;
    TMaybe<TString> GetOptionalUtf8() const;
    TMaybe<TString> GetOptionalYson() const;
    TMaybe<TString> GetOptionalJson() const;
    TMaybe<TDecimalValue> GetOptionalDecimal() const;
    TMaybe<TUuidValue> GetOptionalUuid() const;
    TMaybe<TString> GetOptionalJsonDocument() const;
    TMaybe<TString> GetOptionalDyNumber() const;

    // Optional
    void OpenOptional();
    bool IsNull() const;
    void CloseOptional();

    // List
    void OpenList();
    void CloseList();
    bool TryNextListItem();

    // Struct
    void OpenStruct();
    bool TryNextMember();
    const TString& GetMemberName() const;
    void CloseStruct();

    // Tuple
    void OpenTuple();
    bool TryNextElement();
    void CloseTuple();

    // Dict
    void OpenDict();
    bool TryNextDictItem();
    void DictKey();
    void DictPayload();
    void CloseDict();

    // Variant
    void OpenVariant();
    void CloseVariant();

    // Tagged
    void OpenTagged();
    const TString& GetTag() const;
    void CloseTagged();

private:
    TValueParser(const TType& type);
    void Reset(const Ydb::Value& value);

    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

class TValueBuilderImpl;

template<typename TDerived>
class TValueBuilderBase : public TMoveOnly {
    friend TDerived;
public:
    TDerived& Bool(bool value);
    TDerived& Int8(i8 value);
    TDerived& Uint8(ui8 value);
    TDerived& Int16(i16 value);
    TDerived& Uint16(ui16 value);
    TDerived& Int32(i32 value);
    TDerived& Uint32(ui32 value);
    TDerived& Int64(i64 value);
    TDerived& Uint64(ui64 value);
    TDerived& Float(float value);
    TDerived& Double(double value);
    TDerived& Date(const TInstant& value);
    TDerived& Datetime(const TInstant& value);
    TDerived& Timestamp(const TInstant& value);
    TDerived& Interval(i64 value);
    TDerived& TzDate(const TString& value);
    TDerived& TzDatetime(const TString& value);
    TDerived& TzTimestamp(const TString& value);
    TDerived& String(const TString& value);
    TDerived& Utf8(const TString& value);
    TDerived& Yson(const TString& value);
    TDerived& Json(const TString& value);
    TDerived& Decimal(const TDecimalValue& value);
    TDerived& Pg(const TPgValue& value);
    TDerived& Uuid(const TUuidValue& value);
    TDerived& JsonDocument(const TString& value);
    TDerived& DyNumber(const TString& value);
    TDerived& Date32(const i32 value);
    TDerived& Datetime64(const i64 value);
    TDerived& Timestamp64(const i64 value);
    TDerived& Interval64(const i64 value);

    TDerived& OptionalBool(const TMaybe<bool>& value);
    TDerived& OptionalInt8(const TMaybe<i8>& value);
    TDerived& OptionalUint8(const TMaybe<ui8>& value);
    TDerived& OptionalInt16(const TMaybe<i16>& value);
    TDerived& OptionalUint16(const TMaybe<ui16>& value);
    TDerived& OptionalInt32(const TMaybe<i32>& value);
    TDerived& OptionalUint32(const TMaybe<ui32>& value);
    TDerived& OptionalInt64(const TMaybe<i64>& value);
    TDerived& OptionalUint64(const TMaybe<ui64>& value);
    TDerived& OptionalFloat(const TMaybe<float>& value);
    TDerived& OptionalDouble(const TMaybe<double>& value);
    TDerived& OptionalDate(const TMaybe<TInstant>& value);
    TDerived& OptionalDatetime(const TMaybe<TInstant>& value);
    TDerived& OptionalTimestamp(const TMaybe<TInstant>& value);
    TDerived& OptionalInterval(const TMaybe<i64>& value);
    TDerived& OptionalTzDate(const TMaybe<TString>& value);
    TDerived& OptionalTzDatetime(const TMaybe<TString>& value);
    TDerived& OptionalTzTimestamp(const TMaybe<TString>& value);
    TDerived& OptionalString(const TMaybe<TString>& value);
    TDerived& OptionalUtf8(const TMaybe<TString>& value);
    TDerived& OptionalYson(const TMaybe<TString>& value);
    TDerived& OptionalJson(const TMaybe<TString>& value);
    TDerived& OptionalUuid(const TMaybe<TUuidValue>& value);
    TDerived& OptionalJsonDocument(const TMaybe<TString>& value);
    TDerived& OptionalDyNumber(const TMaybe<TString>& value);
    TDerived& OptionalDate32(const TMaybe<i32>& value);
    TDerived& OptionalDatetime64(const TMaybe<i64>& value);
    TDerived& OptionalTimestamp64(const TMaybe<i64>& value);
    TDerived& OptionalInterval64(const TMaybe<i64>& value);

    // Optional
    TDerived& BeginOptional();
    TDerived& EndOptional();
    TDerived& EmptyOptional(const TType& itemType);
    TDerived& EmptyOptional(EPrimitiveType itemType);
    TDerived& EmptyOptional();

    // List
    TDerived& BeginList();
    TDerived& AddListItem();
    TDerived& AddListItem(const TValue& itemValue);
    TDerived& AddListItem(TValue&& itemValue);
    TDerived& EndList();
    TDerived& EmptyList(const TType& itemType);
    TDerived& EmptyList();

    // Struct
    TDerived& BeginStruct();
    TDerived& AddMember(const TString& memberName);
    TDerived& AddMember(const TString& memberName, const TValue& memberValue);
    TDerived& AddMember(const TString& memberName, TValue&& memberValue);
    TDerived& EndStruct();

    // Tuple
    TDerived& BeginTuple();
    TDerived& AddElement();
    TDerived& AddElement(const TValue& elementValue);
    TDerived& EndTuple();

    // Dict
    TDerived& BeginDict();
    TDerived& AddDictItem();
    TDerived& DictKey();
    TDerived& DictKey(const TValue& keyValue);
    TDerived& DictPayload();
    TDerived& DictPayload(const TValue& payloadValue);
    TDerived& EndDict();
    TDerived& EmptyDict(const TType& keyType, const TType& payloadType);
    TDerived& EmptyDict();

    // Tagged
    TDerived& BeginTagged(const TString& tag);
    TDerived& EndTagged();

protected:
    TValueBuilderBase(TValueBuilderBase&&);

    TValueBuilderBase();

    TValueBuilderBase(const TType& type);

    TValueBuilderBase(Ydb::Type& type, Ydb::Value& value);

    ~TValueBuilderBase();

    void CheckValue();

private:
    std::unique_ptr<TValueBuilderImpl> Impl_;
};

class TValueBuilder : public TValueBuilderBase<TValueBuilder> {
public:
    TValueBuilder();

    TValueBuilder(const TType& type);

    TValue Build();
};

} // namespace NYdb
