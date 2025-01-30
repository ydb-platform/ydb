#pragma once

#include "udf_types.h"
#include "udf_value.h"

#include <util/generic/hash.h>
#include <util/digest/numeric.h>

namespace NYql {
namespace NUdf {

using THashType = ui64;

template <EDataSlot Type>
inline THashType GetValueHash(const TUnboxedValuePod& value);
inline THashType GetValueHash(EDataSlot type, const TUnboxedValuePod& value);

template <EDataSlot Type>
inline int CompareValues(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs);
inline int CompareValues(EDataSlot type, const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs);

template <EDataSlot Type>
inline bool EquateValues(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs);
inline bool EquateValues(EDataSlot type, const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs);

template <EDataSlot Type>
struct TUnboxedValueHash {
    std::size_t operator()(const TUnboxedValuePod& value) const {
        return static_cast<std::size_t>(GetValueHash<Type>(value));
    }
};

template <EDataSlot Type>
struct TUnboxedValueEquals {
    bool operator()(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) const {
        return EquateValues<Type>(lhs, rhs);
    }
};

// hash

template <typename T, std::enable_if_t<std::is_integral<T>::value>* = nullptr>
inline THashType GetIntegerHash(const TUnboxedValuePod& value) {
    return std::hash<T>()(value.Get<T>());
}

template <typename T, std::enable_if_t<std::is_floating_point<T>::value>* = nullptr>
inline THashType GetFloatHash(const TUnboxedValuePod& value) {
    const auto x = value.Get<T>();
    return std::isunordered(x, x) ? ~0ULL : std::hash<T>()(x);
}

inline THashType GetStringHash(TStringBuf value) {
    return THash<TStringBuf>{}(value);
}

inline THashType GetStringHash(const TUnboxedValuePod& value) {
    return GetStringHash(value.AsStringRef());
}

template <typename T, std::enable_if_t<std::is_integral<T>::value>* = nullptr>
inline THashType GetTzIntegerHash(const TUnboxedValuePod& value) {
    return CombineHashes(std::hash<T>()(value.Get<T>()), std::hash<ui16>()(value.GetTimezoneId()));
}

template <>
inline THashType GetValueHash<EDataSlot::Bool>(const TUnboxedValuePod& value) {
    return std::hash<bool>()(value.Get<bool>());
}

template <>
inline THashType GetValueHash<EDataSlot::Int8>(const TUnboxedValuePod& value) {
    return GetIntegerHash<i8>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Uint8>(const TUnboxedValuePod& value) {
    return GetIntegerHash<ui8>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Int16>(const TUnboxedValuePod& value) {
    return GetIntegerHash<i16>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Uint16>(const TUnboxedValuePod& value) {
    return GetIntegerHash<ui16>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Int32>(const TUnboxedValuePod& value) {
    return GetIntegerHash<i32>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Uint32>(const TUnboxedValuePod& value) {
    return GetIntegerHash<ui32>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Int64>(const TUnboxedValuePod& value) {
    return GetIntegerHash<i64>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Uint64>(const TUnboxedValuePod& value) {
    return GetIntegerHash<ui64>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Float>(const TUnboxedValuePod& value) {
    return GetFloatHash<float>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Double>(const TUnboxedValuePod& value) {
    return GetFloatHash<double>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::String>(const TUnboxedValuePod& value) {
    return GetStringHash(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Utf8>(const TUnboxedValuePod& value) {
    return GetStringHash(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Uuid>(const TUnboxedValuePod& value) {
    return GetStringHash(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Yson>(const TUnboxedValuePod&) {
    Y_ABORT("Yson isn't hashable.");
}

template <>
inline THashType GetValueHash<EDataSlot::Json>(const TUnboxedValuePod&) {
    Y_ABORT("Json isn't hashable.");
}

template <>
inline THashType GetValueHash<EDataSlot::JsonDocument>(const TUnboxedValuePod&) {
    Y_ABORT("JsonDocument isn't hashable.");
}

template <>
inline THashType GetValueHash<EDataSlot::Date>(const TUnboxedValuePod& value) {
    return GetIntegerHash<ui16>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Datetime>(const TUnboxedValuePod& value) {
    return GetIntegerHash<ui32>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Timestamp>(const TUnboxedValuePod& value) {
    return GetIntegerHash<ui64>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Interval>(const TUnboxedValuePod& value) {
    return GetIntegerHash<i64>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::TzDate>(const TUnboxedValuePod& value) {
    return GetTzIntegerHash<ui16>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::TzDatetime>(const TUnboxedValuePod& value) {
    return GetTzIntegerHash<ui32>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::TzTimestamp>(const TUnboxedValuePod& value) {
    return GetTzIntegerHash<ui64>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Date32>(const TUnboxedValuePod& value) {
    return GetIntegerHash<i32>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Datetime64>(const TUnboxedValuePod& value) {
    return GetIntegerHash<i64>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Timestamp64>(const TUnboxedValuePod& value) {
    return GetIntegerHash<i64>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Interval64>(const TUnboxedValuePod& value) {
    return GetIntegerHash<i64>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::TzDate32>(const TUnboxedValuePod& value) {
    return GetTzIntegerHash<i32>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::TzDatetime64>(const TUnboxedValuePod& value) {
    return GetTzIntegerHash<i64>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::TzTimestamp64>(const TUnboxedValuePod& value) {
    return GetTzIntegerHash<i64>(value);
}

template <>
inline THashType GetValueHash<EDataSlot::Decimal>(const TUnboxedValuePod& value) {
    const auto pair = NYql::NDecimal::MakePair(value.GetInt128());
    return CombineHashes(pair.first, pair.second);
}

template <>
inline THashType GetValueHash<EDataSlot::DyNumber>(const TUnboxedValuePod& value) {
    return GetStringHash(value);
}

inline THashType GetValueHash(EDataSlot type, const TUnboxedValuePod& value) {
#define HASH_TYPE(slot, ...) \
    case EDataSlot::slot:    \
        return GetValueHash<EDataSlot::slot>(value);

    switch (type) {
    UDF_TYPE_ID_MAP(HASH_TYPE)
    }

#undef HASH_TYPE

    Y_ENSURE(false, "Incorrect data slot: " << (ui32)type);
}

// compare

template <typename T, std::enable_if_t<std::is_integral<T>::value>* = nullptr>
inline int CompareIntegers(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    const auto x = lhs.Get<T>();
    const auto y = rhs.Get<T>();
    return (x == y) ? 0 : (x < y ? -1 : 1);
}

template <typename T, std::enable_if_t<std::is_floating_point<T>::value>* = nullptr>
inline int CompareFloats(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    const auto x = lhs.Get<T>();
    const auto y = rhs.Get<T>();
    if (std::isunordered(x, y)) {
        const auto xn = std::isnan(x);
        const auto yn = std::isnan(y);
        return (xn == yn) ? 0 : xn ? 1 : -1;
    }
    return (x == y) ? 0 : (x < y ? -1 : 1);
}

inline int CompareStrings(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    const TStringBuf lhsBuf = lhs.AsStringRef();
    const TStringBuf rhsBuf = rhs.AsStringRef();
    return lhsBuf.compare(rhsBuf);
}

template <typename T, std::enable_if_t<std::is_integral<T>::value>* = nullptr>
inline int CompareTzIntegers(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    const auto x = lhs.Get<T>();
    const auto y = rhs.Get<T>();
    if (x < y) {
        return -1;
    }

    if (x > y) {
        return 1;
    }

    const auto tx = lhs.GetTimezoneId();
    const auto ty = rhs.GetTimezoneId();
    return (tx == ty) ? 0 : (tx < ty ? -1 : 1);
}

template <>
inline int CompareValues<EDataSlot::Bool>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    const auto x = lhs.Get<bool>();
    const auto y = rhs.Get<bool>();
    return x == y ? 0 : (!x ? -1 : 1);
}

template <>
inline int CompareValues<EDataSlot::Int8>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<i8>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Uint8>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<ui8>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Int16>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<i16>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Uint16>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<ui16>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Int32>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<i32>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Uint32>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<ui32>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Int64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<i64>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Uint64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<ui64>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Float>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareFloats<float>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Double>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareFloats<double>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::String>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareStrings(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Utf8>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareStrings(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Uuid>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareStrings(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Yson>(const TUnboxedValuePod&, const TUnboxedValuePod&) {
    Y_ABORT("Yson isn't comparable.");
}

template <>
inline int CompareValues<EDataSlot::Json>(const TUnboxedValuePod&, const TUnboxedValuePod&) {
    Y_ABORT("Json isn't comparable.");
}

template <>
inline int CompareValues<EDataSlot::JsonDocument>(const TUnboxedValuePod&, const TUnboxedValuePod&) {
    Y_ABORT("JsonDocument isn't comparable.");
}

template <>
inline int CompareValues<EDataSlot::Date>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<ui16>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Datetime>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<ui32>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Timestamp>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<ui64>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Interval>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<i64>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::TzDate>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareTzIntegers<ui16>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::TzDatetime>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareTzIntegers<ui32>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::TzTimestamp>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareTzIntegers<ui64>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Date32>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<i32>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Datetime64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<i64>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Timestamp64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<i64>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Interval64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareIntegers<i64>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::TzDate32>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareTzIntegers<i32>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::TzDatetime64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareTzIntegers<i64>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::TzTimestamp64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareTzIntegers<i64>(lhs, rhs);
}

template <>
inline int CompareValues<EDataSlot::Decimal>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    const auto x = lhs.GetInt128();
    const auto y = rhs.GetInt128();
    return x == y ? 0 : (x < y ? -1 : 1);
}

template <>
inline int CompareValues<EDataSlot::DyNumber>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return CompareStrings(lhs, rhs);
}

inline int CompareValues(EDataSlot type, const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
#define COMPARE_TYPE(slot, ...) \
    case EDataSlot::slot:       \
        return CompareValues<EDataSlot::slot>(lhs, rhs);

    switch (type) {
    UDF_TYPE_ID_MAP(COMPARE_TYPE)
    }

#undef COMPARE_TYPE
    Y_ENSURE(false, "Incorrect data slot: " << (ui32)type);
}

// equate

template <typename T, std::enable_if_t<std::is_integral<T>::value>* = nullptr>
inline bool EquateIntegers(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return lhs.Get<T>() == rhs.Get<T>();
}

template <typename T, std::enable_if_t<std::is_floating_point<T>::value>* = nullptr>
inline bool EquateFloats(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    const auto x = lhs.Get<T>();
    const auto y = rhs.Get<T>();
    return std::isunordered(x, y) ? std::isnan(x) == std::isnan(y) : x == y;
}

inline bool EquateStrings(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    const auto& lhsBuf = lhs.AsStringRef();
    const auto& rhsBuf = rhs.AsStringRef();
    return lhsBuf == rhsBuf;
}

template <typename T, std::enable_if_t<std::is_integral<T>::value>* = nullptr>
inline bool EquateTzIntegers(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return lhs.Get<T>() == rhs.Get<T>() && lhs.GetTimezoneId() == rhs.GetTimezoneId();
}

template <>
inline bool EquateValues<EDataSlot::Bool>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<bool>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Int8>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<i8>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Uint8>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<ui8>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Int16>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<i16>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Uint16>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<ui16>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Int32>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<i32>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Uint32>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<ui32>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Int64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<i64>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Uint64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<ui64>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Float>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateFloats<float>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Double>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateFloats<double>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::String>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateStrings(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Utf8>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateStrings(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Uuid>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateStrings(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Yson>(const TUnboxedValuePod&, const TUnboxedValuePod&) {
    Y_ABORT("Yson isn't comparable.");
}

template <>
inline bool EquateValues<EDataSlot::Json>(const TUnboxedValuePod&, const TUnboxedValuePod&) {
    Y_ABORT("Json isn't comparable.");
}

template <>
inline bool EquateValues<EDataSlot::JsonDocument>(const TUnboxedValuePod&, const TUnboxedValuePod&) {
    Y_ABORT("JsonDocument isn't comparable.");
}

template <>
inline bool EquateValues<EDataSlot::Date>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<ui16>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Datetime>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<ui32>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Timestamp>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<ui64>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Interval>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<i64>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::TzDate>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateTzIntegers<ui16>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::TzDatetime>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateTzIntegers<ui32>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::TzTimestamp>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateTzIntegers<ui64>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Date32>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<i32>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Datetime64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<i64>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Timestamp64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<i64>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Interval64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateIntegers<i64>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::TzDate32>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateTzIntegers<i32>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::TzDatetime64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateTzIntegers<i64>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::TzTimestamp64>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateTzIntegers<i64>(lhs, rhs);
}

template <>
inline bool EquateValues<EDataSlot::Decimal>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return lhs.GetInt128() == rhs.GetInt128();
}

template <>
inline bool EquateValues<EDataSlot::DyNumber>(const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
    return EquateStrings(lhs, rhs);
}

inline bool EquateValues(EDataSlot type, const TUnboxedValuePod& lhs, const TUnboxedValuePod& rhs) {
#define EQUATE_TYPE(slot, ...) \
    case EDataSlot::slot:      \
        return EquateValues<EDataSlot::slot>(lhs, rhs);

    switch (type) {
    UDF_TYPE_ID_MAP(EQUATE_TYPE)
    }

#undef EQUATE_TYPE
    Y_ENSURE(false, "Incorrect data slot: " << (ui32)type);
}

} // namespace NUdf
} // namespace NYql
