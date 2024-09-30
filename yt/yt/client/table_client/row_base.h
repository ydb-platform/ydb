#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EValueType, ui8,
    ((Min)         (0x00))

    ((TheBottom)   (0x01))
    ((Null)        (0x02))

    ((Int64)       (0x03))
    ((Uint64)      (0x04))
    ((Double)      (0x05))
    ((Boolean)     (0x06))

    ((String)      (0x10))
    ((Any)         (0x11))

    ((Composite)   (0x12))

    ((Max)         (0xef))
);

static_assert(
    EValueType::Int64 < EValueType::Uint64 &&
    EValueType::Uint64 < EValueType::Double,
    "Incorrect type order.");

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EValueFlags, ui8,
    ((None)        (0x00))
    ((Aggregate)   (0x01))
    ((Hunk)        (0x02))
);

DEFINE_ENUM_WITH_UNDERLYING_TYPE(ESimpleLogicalValueType, ui32,
    ((Null)        (0x02))

    ((Int64)       (0x03))
    ((Uint64)      (0x04))
    ((Double)      (0x05))
    ((Boolean)     (0x06))

    ((String)      (0x10))
    ((Any)         (0x11))

    ((Int8)        (0x1000))
    ((Uint8)       (0x1001))

    ((Int16)       (0x1003))
    ((Uint16)      (0x1004))

    ((Int32)       (0x1005))
    ((Uint32)      (0x1006))

    ((Utf8)        (0x1007))

    ((Date)        (0x1008))
    ((Datetime)    (0x1009))
    ((Timestamp)   (0x100a))
    ((Interval)    (0x100b))

    ((Void)        (0x100c))

    ((Float)       (0x100d))
    ((Json)        (0x100e))

    ((Uuid)        (0x100f))

    ((Date32)      (0x1010))
    ((Datetime64)  (0x1011))
    ((Timestamp64) (0x1012))
    ((Interval64)  (0x1013))
);

//! Debug printers for Gtest unittests.
void PrintTo(EValueType type, std::ostream* os);
void PrintTo(ESimpleLogicalValueType type, std::ostream* os);

inline bool IsIntegralTypeSigned(ESimpleLogicalValueType type)
{
    switch (type) {
        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Int32:
        case ESimpleLogicalValueType::Int64:
            return true;
        case ESimpleLogicalValueType::Uint8:
        case ESimpleLogicalValueType::Uint16:
        case ESimpleLogicalValueType::Uint32:
        case ESimpleLogicalValueType::Uint64:
            return false;
        default:
            YT_ABORT();
    }
}

inline bool IsIntegralType(ESimpleLogicalValueType type)
{
    switch (type) {
        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Int32:
        case ESimpleLogicalValueType::Int64:
        case ESimpleLogicalValueType::Uint8:
        case ESimpleLogicalValueType::Uint16:
        case ESimpleLogicalValueType::Uint32:
        case ESimpleLogicalValueType::Uint64:
            return true;
        default:
            return false;
    }
}

inline bool IsFloatingPointType(ESimpleLogicalValueType type)
{
    switch (type) {
        case ESimpleLogicalValueType::Double:
        case ESimpleLogicalValueType::Float:
            return true;
        default:
            return false;
    }
}

inline bool IsStringLikeType(ESimpleLogicalValueType type)
{
    switch (type) {
        case ESimpleLogicalValueType::String:
        case ESimpleLogicalValueType::Any:
        case ESimpleLogicalValueType::Utf8:
        case ESimpleLogicalValueType::Json:
        case ESimpleLogicalValueType::Uuid:
            return true;
        default:
            return false;
    }
}

inline int GetIntegralTypeBitWidth(ESimpleLogicalValueType type)
{
    switch (type) {
        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Uint8:
            return 8;
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Uint16:
            return 16;
        case ESimpleLogicalValueType::Int32:
        case ESimpleLogicalValueType::Uint32:
            return 32;
        case ESimpleLogicalValueType::Int64:
        case ESimpleLogicalValueType::Uint64:
            return 64;
        default:
            YT_ABORT();
    }
}

inline int GetIntegralTypeByteSize(ESimpleLogicalValueType type)
{
    switch (type) {
        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Uint8:
            return 1;
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Uint16:
            return 2;
        case ESimpleLogicalValueType::Int32:
        case ESimpleLogicalValueType::Uint32:
            return 4;
        case ESimpleLogicalValueType::Int64:
        case ESimpleLogicalValueType::Uint64:
            return 8;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

inline constexpr EValueType GetPhysicalType(ESimpleLogicalValueType type)
{
    switch (type) {
        case ESimpleLogicalValueType::Null:
        case ESimpleLogicalValueType::Int64:
        case ESimpleLogicalValueType::Uint64:
        case ESimpleLogicalValueType::Double:
        case ESimpleLogicalValueType::Boolean:
        case ESimpleLogicalValueType::String:
        case ESimpleLogicalValueType::Any:
            return static_cast<EValueType>(type);

        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Int32:
            return EValueType::Int64;

        case ESimpleLogicalValueType::Uint8:
        case ESimpleLogicalValueType::Uint16:
        case ESimpleLogicalValueType::Uint32:
            return EValueType::Uint64;

        case ESimpleLogicalValueType::Utf8:
        case ESimpleLogicalValueType::Json:
        case ESimpleLogicalValueType::Uuid:
            return EValueType::String;
        case ESimpleLogicalValueType::Date:
        case ESimpleLogicalValueType::Datetime:
        case ESimpleLogicalValueType::Timestamp:
            return EValueType::Uint64;
        case ESimpleLogicalValueType::Interval:
            return EValueType::Int64;

        case ESimpleLogicalValueType::Void:
            return EValueType::Null;

        case ESimpleLogicalValueType::Float:
            return EValueType::Double;

        case ESimpleLogicalValueType::Date32:
        case ESimpleLogicalValueType::Datetime64:
        case ESimpleLogicalValueType::Timestamp64:
        case ESimpleLogicalValueType::Interval64:
            return EValueType::Int64;

        default:
            YT_ABORT();
    }
}

inline ESimpleLogicalValueType GetLogicalType(EValueType type)
{
    switch (type) {
        case EValueType::Null:
        case EValueType::Int64:
        case EValueType::Uint64:
        case EValueType::Double:
        case EValueType::Boolean:
        case EValueType::String:
        case EValueType::Any:
            return static_cast<ESimpleLogicalValueType>(type);

        case EValueType::Composite:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            THROW_ERROR_EXCEPTION("Value type %Qlv has no corresponding logical type",
                type);

        default:
            YT_ABORT();
    }
}

inline constexpr bool IsIntegralType(EValueType type)
{
    return type == EValueType::Int64 || type == EValueType::Uint64;
}

inline constexpr bool IsArithmeticType(EValueType type)
{
    return IsIntegralType(type) || type == EValueType::Double;
}

inline constexpr bool IsAnyOrComposite(EValueType type)
{
    return type == EValueType::Any || type == EValueType::Composite;
}

inline constexpr bool IsStringLikeType(EValueType type)
{
    return type == EValueType::String || IsAnyOrComposite(type);
}

inline constexpr bool IsValueType(EValueType type)
{
    return
        type == EValueType::Int64 ||
        type == EValueType::Uint64 ||
        type == EValueType::Double ||
        type == EValueType::Boolean ||
        type == EValueType::String ||
        type == EValueType::Any;
}

inline constexpr bool IsAnyColumnCompatibleType(EValueType type)
{
    return
        type == EValueType::Null ||
        type == EValueType::Int64 ||
        type == EValueType::Uint64 ||
        type == EValueType::Double ||
        type == EValueType::Boolean ||
        type == EValueType::String ||
        type == EValueType::Any ||
        type == EValueType::Composite;
}

inline constexpr bool IsSentinelType(EValueType type)
{
    return type == EValueType::Min || type == EValueType::Max;
}

inline constexpr bool IsPrimitiveType(EValueType type)
{
    return
        type == EValueType::Int64 ||
        type == EValueType::Uint64 ||
        type == EValueType::Double ||
        type == EValueType::Boolean ||
        type == EValueType::String;
}

////////////////////////////////////////////////////////////////////////////////

// This class contains ordered collection of indexes of columns of some table.
// Position in context of the class means position of some column index in ordered collection.
class TColumnFilter
{
public:
    using TIndexes = TCompactVector<int, TypicalColumnCount>;

    TColumnFilter();
    TColumnFilter(const std::initializer_list<int>& indexes);
    explicit TColumnFilter(TIndexes&& indexes);
    explicit TColumnFilter(const std::vector<int>& indexes);
    explicit TColumnFilter(int schemaColumnCount);

    bool ContainsIndex(int columnIndex) const;
    int GetPosition(int columnIndex) const;
    std::optional<int> FindPosition(int columnIndex) const;
    const TIndexes& GetIndexes() const;

    static const TColumnFilter& MakeUniversal();
    bool IsUniversal() const;

private:
    bool Universal_;
    TIndexes Indexes_;
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TColumnFilter& columnFilter, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TTypeErasedRow
{
    const void* OpaqueHeader;

    explicit operator bool() const
    {
        return OpaqueHeader != nullptr;
    }
};

static_assert((std::is_standard_layout_v<TTypeErasedRow> && std::is_trivial_v<TTypeErasedRow>), "TTypeErasedRow must be POD.");

////////////////////////////////////////////////////////////////////////////////

//! Checks that #type is allowed to appear in data. Throws on failure.
void ValidateDataValueType(EValueType type);

//! Checks that #type is allowed to appear in keys. Throws on failure.
void ValidateKeyValueType(EValueType type);

//! Checks that #type is allowed to appear in schema. Throws on failure.
void ValidateSchemaValueType(EValueType type);

//! Checks that column filter contains indexes in range |[0, schemaColumnCount - 1]|.
void ValidateColumnFilter(const TColumnFilter& columnFilter, int schemaColumnCount);

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
constexpr TValue MakeSentinelValue(EValueType type, int id = 0, EValueFlags flags = EValueFlags::None)
{
    TValue result{};
    result.Id = id;
    result.Type = type;
    result.Flags = flags;
    return result;
}

template <class TValue>
constexpr TValue MakeNullValue(int id = 0, EValueFlags flags = EValueFlags::None)
{
    TValue result{};
    result.Id = id;
    result.Type = EValueType::Null;
    result.Flags = flags;
    return result;
}

template <class TValue>
constexpr TValue MakeInt64Value(i64 value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    TValue result{};
    result.Id = id;
    result.Type = EValueType::Int64;
    result.Flags = flags;
    result.Data.Int64 = value;
    return result;
}

template <class TValue>
constexpr TValue MakeUint64Value(ui64 value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    TValue result{};
    result.Id = id;
    result.Type = EValueType::Uint64;
    result.Flags = flags;
    result.Data.Uint64 = value;
    return result;
}

template <class TValue>
constexpr TValue MakeDoubleValue(double value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    TValue result{};
    result.Id = id;
    result.Type = EValueType::Double;
    result.Flags = flags;
    result.Data.Double = value;
    return result;
}

template <class TValue>
constexpr TValue MakeBooleanValue(bool value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    TValue result{};
    result.Id = id;
    result.Type = EValueType::Boolean;
    result.Flags = flags;
    result.Data.Boolean = value;
    return result;
}

template <class TValue>
constexpr TValue MakeStringLikeValue(EValueType valueType, TStringBuf value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    TValue result{};
    result.Id = id;
    result.Type = valueType;
    result.Flags = flags;
    result.Length = value.length();
    result.Data.String = value.begin();
    return result;
}

template <class TValue>
constexpr TValue MakeStringValue(TStringBuf value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeStringLikeValue<TValue>(EValueType::String, value, id, flags);
}

template <class TValue>
constexpr TValue MakeAnyValue(TStringBuf value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeStringLikeValue<TValue>(EValueType::Any, value, id, flags);
}

template <class TValue>
constexpr TValue MakeCompositeValue(TStringBuf value, int id = 0, EValueFlags flags = EValueFlags::None)
{
    return MakeStringLikeValue<TValue>(EValueType::Composite, value, id, flags);
}

[[noreturn]] void ThrowUnexpectedValueType(EValueType valueType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
