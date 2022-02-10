#pragma once

#include <ydb/public/lib/scheme_types/scheme_type_id.h> 

namespace NKikimr {
namespace NScheme {

constexpr bool IsNumericType(TTypeId id) noexcept {
    return NTypeIds::Int8 == id
        || NTypeIds::Uint8 == id
        || NTypeIds::Int16 == id
        || NTypeIds::Uint16 == id
        || NTypeIds::Int32 == id
        || NTypeIds::Uint32 == id
        || NTypeIds::Int64 == id
        || NTypeIds::Uint64 == id
        || NTypeIds::Double == id
        || NTypeIds::Float == id;
}

constexpr bool IsIntegralType(TTypeId id) noexcept {
    return NTypeIds::Int8 == id
        || NTypeIds::Uint8 == id
        || NTypeIds::Int16 == id
        || NTypeIds::Uint16 == id
        || NTypeIds::Int32 == id
        || NTypeIds::Uint32 == id
        || NTypeIds::Int64 == id
        || NTypeIds::Uint64 == id;
}

constexpr bool IsSignedIntegerType(TTypeId id) noexcept {
    return NTypeIds::Int8 == id || NTypeIds::Int16 == id || NTypeIds::Int32 == id || NTypeIds::Int64 == id;
}

constexpr bool IsUnsignedIntegerType(TTypeId id) noexcept {
    return NTypeIds::Uint8 == id || NTypeIds::Uint16 == id || NTypeIds::Uint32 == id || NTypeIds::Uint64 == id;
}

constexpr bool IsFloatingType(TTypeId id) noexcept {
    return NTypeIds::Double == id || NTypeIds::Float == id;
}

constexpr bool IsStringType(TTypeId id) noexcept {
    return NTypeIds::String == id
        || NTypeIds::String4k == id
        || NTypeIds::String2m == id
        || NTypeIds::Utf8 == id
        || NTypeIds::Yson == id
        || NTypeIds::Json == id
        || NTypeIds::JsonDocument == id
        || NTypeIds::DyNumber
        ;
}

} // namspace NScheme
} // namspace NKikimr
