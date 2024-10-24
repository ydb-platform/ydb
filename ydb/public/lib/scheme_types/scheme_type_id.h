#pragma once

#include <util/system/types.h>
#include <util/generic/array_size.h>
#include <util/generic/strbuf.h>
#include <ydb/library/yql/public/types/yql_types.pb.h>

namespace NKikimr {
namespace NScheme {

constexpr ui32 DECIMAL_MAX_PRECISION = 35;
constexpr ui32 DECIMAL_PRECISION = 22;
constexpr ui32 DECIMAL_SCALE = 9;

using TTypeId = ui16;

namespace NTypeIds {

static constexpr TTypeId Int32 = NYql::NProto::Int32;
static constexpr TTypeId Uint32 = NYql::NProto::Uint32;
static constexpr TTypeId Int64 = NYql::NProto::Int64;
static constexpr TTypeId Uint64 = NYql::NProto::Uint64;
static constexpr TTypeId Byte = NYql::NProto::Uint8;
static constexpr TTypeId Bool = NYql::NProto::Bool;
static constexpr TTypeId Int8 = NYql::NProto::Int8;
static constexpr TTypeId Uint8 = NYql::NProto::Uint8;
static constexpr TTypeId Int16 = NYql::NProto::Int16;
static constexpr TTypeId Uint16 = NYql::NProto::Uint16;

static constexpr TTypeId Double = NYql::NProto::Double;
static constexpr TTypeId Float = NYql::NProto::Float;

static constexpr TTypeId Date = NYql::NProto::Date; // days since 1970
static constexpr TTypeId Datetime = NYql::NProto::Datetime; // seconds since 1970
static constexpr TTypeId Timestamp = NYql::NProto::Timestamp; // microseconds since 1970 aka TInstant
static constexpr TTypeId Interval = NYql::NProto::Interval; // microseconds aka TDuration, signed

static constexpr TTypeId Date32 = NYql::NProto::Date32; // days since 1970, i32
static constexpr TTypeId Datetime64 = NYql::NProto::Datetime64; // seconds since 1970
static constexpr TTypeId Timestamp64 = NYql::NProto::Timestamp64; // microseconds since 1970 aka TInstant
static constexpr TTypeId Interval64 = NYql::NProto::Interval64; // microseconds aka TDuration, signed

static constexpr TTypeId PairUi64Ui64 = 0x101; // DEPRECATED, don't use

static constexpr TTypeId String = NYql::NProto::String;
static constexpr TTypeId String4k = 0x1011;
static constexpr TTypeId String2m = 0x1012;
static constexpr TTypeId Bytes = String;

static constexpr TTypeId Utf8 = NYql::NProto::Utf8;
static constexpr TTypeId Text = Utf8;

static constexpr TTypeId Yson = NYql::NProto::Yson;
static constexpr TTypeId Json = NYql::NProto::Json;

static constexpr TTypeId Uuid = NYql::NProto::Uuid;
static constexpr TTypeId JsonDocument = NYql::NProto::JsonDocument;

static constexpr TTypeId DyNumber = NYql::NProto::DyNumber;

static constexpr TTypeId Decimal = NYql::NProto::Decimal;

static constexpr TTypeId Pg = 0x3000;

static constexpr TTypeId YqlIds[] = {
    Int8,
    Uint8,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int64,
    Uint64,
    Bool,
    Double,
    Float,
    String,
    Utf8,
    Yson,
    Json,
    Decimal,
    Date,
    Datetime,
    Timestamp,
    Interval,
    JsonDocument,
    DyNumber,
    Uuid,
    Date32,
    Datetime64,
    Timestamp64,
    Interval64,
};

// types must be defined in GetValueHash and CompareTypedCells
constexpr bool IsYqlTypeImpl(TTypeId typeId, ui32 i) {
    return i == Y_ARRAY_SIZE(YqlIds) ? false :
        YqlIds[i] == typeId ? true : IsYqlTypeImpl(typeId, i + 1);
}

constexpr bool IsYqlType(TTypeId typeId) {
    return IsYqlTypeImpl(typeId, 0);
}

constexpr bool IsParametrizedType(TTypeId typeId) {
    return typeId == Pg
        || typeId == Decimal
    ;
}

} // namespace NTypeIds

#ifdef _MSC_VER
inline
#else
constexpr
#endif
const char *TypeName(TTypeId typeId) {
    switch (typeId) {
        case 0:                         return "Null";
        case NTypeIds::Int32:           return "Int32";
        case NTypeIds::Uint32:          return "Uint32";
        case NTypeIds::Int64:           return "Int64";
        case NTypeIds::Uint64:          return "Uint64";
        case NTypeIds::Int8:            return "Int8";
        case NTypeIds::Uint8:           return "Uint8";
        case NTypeIds::Int16:           return "Int16";
        case NTypeIds::Uint16:          return "Uint16";
        case NTypeIds::Bool:            return "Bool";
        case NTypeIds::Double:          return "Double";
        case NTypeIds::Float:           return "Float";
        case NTypeIds::Date:            return "Date";
        case NTypeIds::Datetime:        return "Datetime";
        case NTypeIds::Timestamp:       return "Timestamp";
        case NTypeIds::Interval:        return "Interval";
        case NTypeIds::Date32:          return "Date32";
        case NTypeIds::Datetime64:      return "Datetime64";
        case NTypeIds::Timestamp64:     return "Timestamp64";
        case NTypeIds::Interval64:      return "Interval64";
        case NTypeIds::PairUi64Ui64:    return "PairUi64Ui64";
        case NTypeIds::String:          return "String";
        case NTypeIds::String4k:        return "SmallBoundedString"; // string name differs from var
        case NTypeIds::String2m:        return "LargeBoundedString"; // string name differs from var
        case NTypeIds::Utf8:            return "Utf8";
        case NTypeIds::Yson:            return "Yson";
        case NTypeIds::Json:            return "Json";
        case NTypeIds::JsonDocument:    return "JsonDocument";
        case NTypeIds::DyNumber:        return "DyNumber";
        case NTypeIds::Uuid:            return "Uuid";
        default:                        return "Unknown";
    }
}

} // namspace NScheme
} // namspace NKikimr
