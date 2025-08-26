#pragma once

#include "fwd.h"

#include <util/datetime/base.h>

#include <google/protobuf/arena.h>

#include <optional>
#include <memory>

namespace Ydb {
    class Type;
    class Value;
}

namespace NYdb::inline Dev {

class TResultSetParser;

//! Representation of YDB type.
class TType {
    friend class TProtoAccessor;
public:
    TType(const Ydb::Type& typeProto);
    TType(Ydb::Type&& typeProto);

    std::string ToString() const;
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
    uint8_t Precision;
    uint8_t Scale;

    TDecimalType(uint8_t precision, uint8_t scale)
        : Precision(precision)
        , Scale(scale) {}
};

struct TPgType {
    std::string TypeName;
    std::string TypeModifier;

    uint32_t Oid = 0;
    int16_t Typlen = 0;
    int32_t Typmod = 0;

    TPgType(const std::string& typeName, const std::string& typeModifier = {})
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
    const std::string& GetMemberName();
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
    const std::string& GetTag();
    void CloseTagged();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

bool TypesEqual(const TType& t1, const TType& t2);

std::string FormatType(const TType& type);

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
    TTypeBuilder& AddMember(const std::string& memberName);
    TTypeBuilder& AddMember(const std::string& memberName, const TType& memberType);
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
    TTypeBuilder& BeginTagged(const std::string& tag);
    TTypeBuilder& EndTagged();
    TTypeBuilder& Tagged(const std::string& tag, const TType& itemType);

    TType Build();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

struct TDecimalValue {
    std::string ToString() const;
    TDecimalValue(const Ydb::Value& decimalValueProto, const TDecimalType& decimalType);
    TDecimalValue(const std::string& decimalString, uint8_t precision, uint8_t scale);

    TDecimalType DecimalType_;
    uint64_t Low_;
    int64_t Hi_;
};

struct TPgValue {
    enum EPgValueKind {
        VK_NULL,
        VK_TEXT,
        VK_BINARY
    };

    TPgValue(const Ydb::Value& pgValueProto, const TPgType& pgType);
    TPgValue(EPgValueKind kind, const std::string& content, const TPgType& pgType);
    bool IsNull() const;
    bool IsText() const;

    TPgType PgType_;
    EPgValueKind Kind_ = VK_NULL;
    std::string Content_;
};

struct TUuidValue {
    std::string ToString() const;
    TUuidValue(uint64_t low_128, uint64_t high_128);
    TUuidValue(const Ydb::Value& uuidValueProto);
    TUuidValue(const std::string& uuidString);

    union {
        char Bytes[16];
        uint64_t Halfs[2];
    } Buf_;
};

namespace NTable {

class TTableClient;

} // namespace NTable

//! Representation of YDB value.
class TValue {
    friend class TValueParser;
    friend class TProtoAccessor;
    friend class NTable::TTableClient;
public:
    TValue(const TType& type, const Ydb::Value& valueProto);
    TValue(const TType& type, Ydb::Value&& valueProto);
    /**
    * Lifetime of the arena, and hence the `Ydb::Value`, is expected to be managed by the caller.
    * The `Ydb::Value` is expected to be arena-allocated.
    *
    * See: https://protobuf.dev/reference/cpp/arenas
    */
    TValue(const TType& type, Ydb::Value* arenaAllocatedValueProto);

    const TType& GetType() const;
    TType& GetType();

    const Ydb::Value& GetProto() const;
    Ydb::Value& GetProto();
private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

//! Wide types are used to represent YDB date types (Date32, Datetime64, Timestamp64, Interval64).
//! They are used to avoid overflows when converting from YDB types to C++ types.
using TWideDays = std::chrono::duration<int32_t, std::ratio<86400>>;
using TWideSeconds = std::chrono::duration<int64_t, std::ratio<1>>;
using TWideMicroseconds = std::chrono::duration<int64_t, std::micro>;

class TValueParser : public TMoveOnly {
    friend class TResultSetParser;
public:
    TValueParser(TValueParser&&);
    TValueParser(const TValue& value);

    ~TValueParser();

    TTypeParser::ETypeKind GetKind() const;
    EPrimitiveType GetPrimitiveType() const;

    bool GetBool() const;
    int8_t GetInt8() const;
    uint8_t GetUint8() const;
    int16_t GetInt16() const;
    uint16_t GetUint16() const;
    int32_t GetInt32() const;
    uint32_t GetUint32() const;
    int64_t GetInt64() const;
    uint64_t GetUint64() const;
    float GetFloat() const;
    double GetDouble() const;
    TInstant GetDate() const;
    TInstant GetDatetime() const;
    TInstant GetTimestamp() const;
    int64_t GetInterval() const;
    std::chrono::sys_time<TWideDays> GetDate32() const;
    std::chrono::sys_time<TWideSeconds> GetDatetime64() const;
    std::chrono::sys_time<TWideMicroseconds> GetTimestamp64() const;
    TWideMicroseconds GetInterval64() const;
    const std::string& GetTzDate() const;
    const std::string& GetTzDatetime() const;
    const std::string& GetTzTimestamp() const;
    const std::string& GetString() const;
    const std::string& GetUtf8() const;
    const std::string& GetYson() const;
    const std::string& GetJson() const;
    TDecimalValue GetDecimal() const;
    TPgValue GetPg() const;
    TUuidValue GetUuid() const;
    const std::string& GetJsonDocument() const;
    const std::string& GetDyNumber() const;

    std::optional<bool> GetOptionalBool() const;
    std::optional<int8_t> GetOptionalInt8() const;
    std::optional<uint8_t> GetOptionalUint8() const;
    std::optional<int16_t> GetOptionalInt16() const;
    std::optional<uint16_t> GetOptionalUint16() const;
    std::optional<int32_t> GetOptionalInt32() const;
    std::optional<uint32_t> GetOptionalUint32() const;
    std::optional<int64_t> GetOptionalInt64() const;
    std::optional<uint64_t> GetOptionalUint64() const;
    std::optional<float> GetOptionalFloat() const;
    std::optional<double> GetOptionalDouble() const;
    std::optional<TInstant> GetOptionalDate() const;
    std::optional<TInstant> GetOptionalDatetime() const;
    std::optional<TInstant> GetOptionalTimestamp() const;
    std::optional<int64_t> GetOptionalInterval() const;
    std::optional<std::chrono::sys_time<TWideDays>> GetOptionalDate32() const;
    std::optional<std::chrono::sys_time<TWideSeconds>> GetOptionalDatetime64() const;
    std::optional<std::chrono::sys_time<TWideMicroseconds>> GetOptionalTimestamp64() const;
    std::optional<TWideMicroseconds> GetOptionalInterval64() const;
    std::optional<std::string> GetOptionalTzDate() const;
    std::optional<std::string> GetOptionalTzDatetime() const;
    std::optional<std::string> GetOptionalTzTimestamp() const;
    std::optional<std::string> GetOptionalString() const;
    std::optional<std::string> GetOptionalUtf8() const;
    std::optional<std::string> GetOptionalYson() const;
    std::optional<std::string> GetOptionalJson() const;
    std::optional<TDecimalValue> GetOptionalDecimal() const;
    std::optional<TUuidValue> GetOptionalUuid() const;
    std::optional<std::string> GetOptionalJsonDocument() const;
    std::optional<std::string> GetOptionalDyNumber() const;

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
    const std::string& GetMemberName() const;
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
    const std::string& GetTag() const;
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
    TDerived& Int8(int8_t value);
    TDerived& Uint8(uint8_t value);
    TDerived& Int16(int16_t value);
    TDerived& Uint16(uint16_t value);
    TDerived& Int32(int32_t value);
    TDerived& Uint32(uint32_t value);
    TDerived& Int64(int64_t value);
    TDerived& Uint64(uint64_t value);
    TDerived& Float(float value);
    TDerived& Double(double value);
    TDerived& Date(const TInstant& value);
    TDerived& Datetime(const TInstant& value);
    TDerived& Timestamp(const TInstant& value);
    TDerived& Interval(int64_t value);
    TDerived& TzDate(const std::string& value);
    TDerived& TzDatetime(const std::string& value);
    TDerived& TzTimestamp(const std::string& value);
    TDerived& String(const std::string& value);
    TDerived& Utf8(const std::string& value);
    TDerived& Yson(const std::string& value);
    TDerived& Json(const std::string& value);
    TDerived& Decimal(const TDecimalValue& value);
    TDerived& Pg(const TPgValue& value);
    TDerived& Uuid(const TUuidValue& value);
    TDerived& JsonDocument(const std::string& value);
    TDerived& DyNumber(const std::string& value);
    TDerived& Date32(const std::chrono::sys_time<TWideDays>& value);
    TDerived& Datetime64(const std::chrono::sys_time<TWideSeconds>& value);
    TDerived& Timestamp64(const std::chrono::sys_time<TWideMicroseconds>& value);
    TDerived& Interval64(const TWideMicroseconds& value);

    TDerived& OptionalBool(const std::optional<bool>& value);
    TDerived& OptionalInt8(const std::optional<int8_t>& value);
    TDerived& OptionalUint8(const std::optional<uint8_t>& value);
    TDerived& OptionalInt16(const std::optional<int16_t>& value);
    TDerived& OptionalUint16(const std::optional<uint16_t>& value);
    TDerived& OptionalInt32(const std::optional<int32_t>& value);
    TDerived& OptionalUint32(const std::optional<uint32_t>& value);
    TDerived& OptionalInt64(const std::optional<int64_t>& value);
    TDerived& OptionalUint64(const std::optional<uint64_t>& value);
    TDerived& OptionalFloat(const std::optional<float>& value);
    TDerived& OptionalDouble(const std::optional<double>& value);
    TDerived& OptionalDate(const std::optional<TInstant>& value);
    TDerived& OptionalDatetime(const std::optional<TInstant>& value);
    TDerived& OptionalTimestamp(const std::optional<TInstant>& value);
    TDerived& OptionalInterval(const std::optional<int64_t>& value);
    TDerived& OptionalTzDate(const std::optional<std::string>& value);
    TDerived& OptionalTzDatetime(const std::optional<std::string>& value);
    TDerived& OptionalTzTimestamp(const std::optional<std::string>& value);
    TDerived& OptionalString(const std::optional<std::string>& value);
    TDerived& OptionalUtf8(const std::optional<std::string>& value);
    TDerived& OptionalYson(const std::optional<std::string>& value);
    TDerived& OptionalJson(const std::optional<std::string>& value);
    TDerived& OptionalUuid(const std::optional<TUuidValue>& value);
    TDerived& OptionalJsonDocument(const std::optional<std::string>& value);
    TDerived& OptionalDyNumber(const std::optional<std::string>& value);
    TDerived& OptionalDate32(const std::optional<std::chrono::sys_time<TWideDays>>& value);
    TDerived& OptionalDatetime64(const std::optional<std::chrono::sys_time<TWideSeconds>>& value);
    TDerived& OptionalTimestamp64(const std::optional<std::chrono::sys_time<TWideMicroseconds>>& value);
    TDerived& OptionalInterval64(const std::optional<TWideMicroseconds>& value);

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
    TDerived& AddMember(const std::string& memberName);
    TDerived& AddMember(const std::string& memberName, const TValue& memberValue);
    TDerived& AddMember(const std::string& memberName, TValue&& memberValue);
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
    TDerived& BeginTagged(const std::string& tag);
    TDerived& EndTagged();

protected:
    TValueBuilderBase(TValueBuilderBase&&);

    TValueBuilderBase();
    explicit TValueBuilderBase(google::protobuf::Arena* arena);

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
    explicit TValueBuilder(google::protobuf::Arena* arena);

    TValueBuilder(const TType& type);

    TValue Build();
};

} // namespace NYdb

template<>
void Out<NYdb::TUuidValue>(IOutputStream& o, const NYdb::TUuidValue& value);
