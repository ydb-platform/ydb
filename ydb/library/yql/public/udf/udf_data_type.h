#pragma once

#include <ydb/library/yql/public/types/yql_types.pb.h>

#include <util/system/types.h>
#include <util/stream/output.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

namespace NYql {
namespace NUdf {

using TDataTypeId = ui16;

using TTimezoneId = ui16;

enum EDataTypeFeatures : ui32 {
    CanCompare = 1u << 0,
    HasDeterministicCompare = 1u << 1,
    CanEquate = 1u << 2,
    HasDeterministicEquals = 1u << 3,
    CanHash = 1u << 4,
    HasDeterministicHash = 1u << 5,
    AllowToBytes = 1u << 6,
    HasDeterministicToBytes = 1u << 7,
    AllowFromBytes = 1u << 8,
    AllowToString = 1u << 9,
    HasDeterministicToString = 1u << 10,
    AllowFromString = 1u << 11,

    PayloadType = AllowToBytes | AllowFromBytes | AllowToString | AllowFromString |
    HasDeterministicToString | HasDeterministicToBytes,
    CommonType = PayloadType | CanCompare | HasDeterministicCompare | CanEquate | HasDeterministicEquals |
    CanHash | HasDeterministicHash,
    UnknownType = 0,

    StringType = 1u << 20,
    NumericType = 1u << 21,
    FloatType = 1u << 22,
    IntegralType = 1u << 23,
    SignedIntegralType = 1u << 24,
    UnsignedIntegralType = 1u << 25,
    DateType = 1u << 26,
    TzDateType = 1u << 27,
    DecimalType = 1u << 28,
    TimeIntervalType = 1u << 29,
    BigDateType = 1u << 30,
};

template <typename T>
struct TDataType;

template <typename T>
struct TKnownDataType
{
    static constexpr bool Result = (TDataType<T>::Id != 0);
};

template <typename T>
struct TPlainDataType
{
    static constexpr bool Result = (TDataType<T>::Features & (NumericType | DateType | TimeIntervalType)) != 0 || std::is_same<T, bool>::value;
};

template <typename T>
struct TTzDataType
{
    static constexpr bool Result = (TDataType<T>::Features & (TzDateType)) != 0;
};

#define INTEGRAL_VALUE_TYPES(xx) \
    xx(i8)     \
    xx(ui8)    \
    xx(i16)    \
    xx(ui16)   \
    xx(i32)    \
    xx(ui32)   \
    xx(i64)    \
    xx(ui64)

#define PRIMITIVE_VALUE_TYPES(xx) \
    INTEGRAL_VALUE_TYPES(xx)     \
    xx(float)  \
    xx(double)

#define KNOWN_PRIMITIVE_VALUE_TYPES(xx) \
    xx(bool)    \
    xx(i8)      \
    xx(ui8)     \
    xx(i16)     \
    xx(ui16)    \
    xx(i32)     \
    xx(ui32)    \
    xx(i64)     \
    xx(ui64)    \
    xx(float)   \
    xx(double)

#define KNOWN_FIXED_VALUE_TYPES(xx)         \
    xx(bool, bool)               \
    xx(i8, i8)                   \
    xx(ui8, ui8)                 \
    xx(i16, i16)                 \
    xx(ui16, ui16)               \
    xx(i32, i32)                 \
    xx(ui32, ui32)               \
    xx(i64, i64)                 \
    xx(ui64, ui64)               \
    xx(float, float)             \
    xx(double, double)           \
    xx(NUdf::TDate, ui16)        \
    xx(NUdf::TDatetime, ui32)    \
    xx(NUdf::TTimestamp, ui64)   \
    xx(NUdf::TInterval, i64)     \
    xx(NUdf::TDate32, i32)       \
    xx(NUdf::TDatetime64, i64)   \
    xx(NUdf::TTimestamp64, i64)  \
    xx(NUdf::TInterval64, i64)

template <typename T>
struct TPrimitiveDataType
{
    static constexpr bool Result = false;
};

#define UDF_PRIMITIVE_TYPE_IMPL(type) \
    template <> \
    struct TPrimitiveDataType<type> \
    { \
        static constexpr bool Result = true; \
    };

PRIMITIVE_VALUE_TYPES(UDF_PRIMITIVE_TYPE_IMPL)
UDF_PRIMITIVE_TYPE_IMPL(bool)

class TUtf8 {};
class TYson {};
class TJson {};
class TUuid {};
class TJsonDocument {};

class TDate {};
class TDatetime {};
class TTimestamp {};
class TInterval {};
class TTzDate {};
class TTzDatetime {};
class TTzTimestamp {};
class TDate32 {};
class TDatetime64 {};
class TTimestamp64 {};
class TInterval64 {};
class TTzDate32 {};
class TTzDatetime64 {};
class TTzTimestamp64 {};
class TDecimal {};
class TDyNumber {};

constexpr ui16 MAX_DATE = 49673u; // non-inclusive
constexpr ui32 MAX_DATETIME = 86400u * 49673u;
constexpr ui64 MAX_TIMESTAMP = 86400000000ull * 49673u;
constexpr ui32 MIN_YEAR = 1970u;
constexpr ui32 MAX_YEAR = 2106u; // non-inclusive

constexpr i32 MIN_DATE32 = -53375809; // inclusive
constexpr i64 MIN_DATETIME64 = -4611669897600ll;
constexpr i64 MIN_TIMESTAMP64 = -4611669897600000000ll;
constexpr i32 MAX_DATE32 = 53375807; // inclusive
constexpr i64 MAX_DATETIME64 = 4611669811199ll;
constexpr i64 MAX_TIMESTAMP64 = 4611669811199999999ll;
constexpr i64 MAX_INTERVAL64 = MAX_TIMESTAMP64 - MIN_TIMESTAMP64;
constexpr i32 MIN_YEAR32 = -144169; // inclusive
constexpr i32 MAX_YEAR32 = 148108; // non-inclusive

#define UDF_TYPE_ID_MAP(XX)  \
    XX(Bool, NYql::NProto::Bool, bool, CommonType, bool, 0) \
    XX(Int8, NYql::NProto::Int8, i8, CommonType | NumericType | IntegralType | SignedIntegralType, i8, 0) \
    XX(Uint8, NYql::NProto::Uint8, ui8, CommonType | NumericType | IntegralType | UnsignedIntegralType, ui8, 0) \
    XX(Int16, NYql::NProto::Int16, i16, CommonType | NumericType | IntegralType | SignedIntegralType, i16, 0) \
    XX(Uint16, NYql::NProto::Uint16, ui16, CommonType | NumericType | IntegralType | UnsignedIntegralType, ui16, 0) \
    XX(Int32, NYql::NProto::Int32, i32, CommonType | NumericType | IntegralType | SignedIntegralType, i32, 0) \
    XX(Uint32, NYql::NProto::Uint32, ui32, CommonType | NumericType | IntegralType | UnsignedIntegralType, ui32, 0) \
    XX(Int64, NYql::NProto::Int64, i64, CommonType | NumericType | IntegralType | SignedIntegralType, i64, 0) \
    XX(Uint64, NYql::NProto::Uint64, ui64, CommonType | NumericType | IntegralType | UnsignedIntegralType, ui64, 0) \
    XX(Double, NYql::NProto::Double, double, CommonType | NumericType | FloatType, double, 0) \
    XX(Float, NYql::NProto::Float, float, CommonType | NumericType | FloatType, float, 0) \
    XX(String, NYql::NProto::String, char*, CommonType | StringType, char*, 0) \
    XX(Utf8, NYql::NProto::Utf8, TUtf8, CommonType | StringType, TUtf8, 0) \
    XX(Yson, NYql::NProto::Yson, TYson, PayloadType | StringType, TYson, 0) \
    XX(Json, NYql::NProto::Json, TJson, PayloadType | StringType, TJson, 0) \
    XX(Uuid, NYql::NProto::Uuid, TUuid, CommonType, TUuid, 0) \
    XX(Date, NYql::NProto::Date, TDate, CommonType | DateType, ui16, 0) \
    XX(Datetime, NYql::NProto::Datetime, TDatetime, CommonType | DateType, ui32, 0) \
    XX(Timestamp, NYql::NProto::Timestamp, TTimestamp, CommonType | DateType, ui64, 0) \
    XX(Interval, NYql::NProto::Interval, TInterval, CommonType | TimeIntervalType, i64, 0) \
    XX(TzDate, NYql::NProto::TzDate, TTzDate, CommonType | TzDateType, ui16, 0) \
    XX(TzDatetime, NYql::NProto::TzDatetime, TTzDatetime, CommonType | TzDateType, ui32, 0) \
    XX(TzTimestamp, NYql::NProto::TzTimestamp, TTzTimestamp, CommonType | TzDateType, ui64, 0) \
    XX(Decimal, NYql::NProto::Decimal, TDecimal, CommonType | DecimalType, TDecimal, 2) \
    XX(DyNumber, NYql::NProto::DyNumber, TDyNumber, CommonType, TDyNumber, 0) \
    XX(JsonDocument, NYql::NProto::JsonDocument, TJsonDocument, PayloadType, TJsonDocument, 0) \
    XX(Date32, NYql::NProto::Date32, TDate32, CommonType | DateType | BigDateType, i32, 0) \
    XX(Datetime64, NYql::NProto::Datetime64, TDatetime64, CommonType | DateType | BigDateType, i64, 0) \
    XX(Timestamp64, NYql::NProto::Timestamp64, TTimestamp64, CommonType | DateType | BigDateType, i64, 0) \
    XX(Interval64, NYql::NProto::Interval64, TInterval64, CommonType | TimeIntervalType | BigDateType, i64, 0) \
    XX(TzDate32, NYql::NProto::TzDate32, TTzDate32, CommonType | TzDateType | BigDateType, i32, 0) \
    XX(TzDatetime64, NYql::NProto::TzDatetime64, TTzDatetime64, CommonType | TzDateType | BigDateType, i64, 0) \
    XX(TzTimestamp64, NYql::NProto::TzTimestamp64, TTzTimestamp64, CommonType | TzDateType | BigDateType, i64, 0) \

#define UDF_TYPE_ID(xName, xTypeId, xType, xFeatures, xLayoutType, xParamsCount)                 \
    template <>                                                                                  \
    struct TDataType<xType> {                                                                    \
        static constexpr TDataTypeId Id = xTypeId;                                               \
        static constexpr EDataSlot Slot = EDataSlot::xName;                                      \
        static constexpr EDataTypeFeatures Features = static_cast<EDataTypeFeatures>(xFeatures); \
        static constexpr ui8 ParamsCount = xParamsCount;                                         \
        using TValue = xType;                                                                    \
        using TLayout = xLayoutType;                                                             \
    };

enum class EDataSlot {
    UDF_TYPE_ID_MAP(ENUM_VALUE_GEN_NO_VALUE)
};

enum class ECountDataSlot {
    UDF_TYPE_ID_MAP(ENUM_VALUE_GEN_NO_VALUE)
    Count
};

constexpr ui32 DataSlotCount = static_cast<ui32>(ECountDataSlot::Count);

UDF_TYPE_ID_MAP(UDF_TYPE_ID)
UDF_TYPE_ID(String, 0x1001, const char*, CommonType | StringType, const char*, 0)
UDF_TYPE_ID(String, 0x1001, char* const, CommonType | StringType, char* const, 0)

template <ui8 Precision, ui8 Scale>
struct TDecimalDataType : public TDataType<TDecimal> {};

TMaybe<EDataSlot> FindDataSlot(TDataTypeId id);
EDataSlot GetDataSlot(TDataTypeId id); // throws if data type is unknown
TMaybe<EDataSlot> FindDataSlot(TStringBuf str);
EDataSlot GetDataSlot(TStringBuf str); // throws if data type is unknown

struct TDataTypeInfo {
    TStringBuf Name;
    TDataTypeId TypeId;
    EDataTypeFeatures Features;
    ui32 FixedSize;
    ui8 ParamsCount;
    ui8 DecimalDigits;
};

extern const TDataTypeInfo DataTypeInfos[DataSlotCount];

inline const TDataTypeInfo& GetDataTypeInfo(EDataSlot slot) {
    Y_DEBUG_ABORT_UNLESS(static_cast<ui32>(slot) < DataSlotCount);
    return DataTypeInfos[static_cast<ui32>(slot)];
}

using TCastResultOptions = ui8;

enum ECastOptions : TCastResultOptions {
    Complete = 0U,

    MayFail = 1U << 0U,
    MayLoseData = 1U << 1U,
    AnywayLoseData = 1U << 2U,

    Impossible = 1U << 3U,
    Undefined = 1U << 4U
};

TMaybe<TCastResultOptions> GetCastResult(EDataSlot source, EDataSlot target);

bool IsComparable(EDataSlot left, EDataSlot right);

inline IOutputStream& operator<<(IOutputStream& os, EDataSlot slot) {
    os << GetDataTypeInfo(slot).Name;
    return os;
}

} // namspace NUdf
} // namspace NYql
