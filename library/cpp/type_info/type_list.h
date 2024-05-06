#pragma once

//! @file type_list.h
//!
//! Enum with all type names that are included in the Common Type System.
//!
//!
//! # Primitive and non-primitive types
//!
//! Some systems only work with primitive types, so we've split the enum in two: the first contains primitive types,
//! and the second contains all types, including the primitive ones. This way systems that are only interested
//! in primitives can stop handling containers and make all their switch-cases exhaustive.
//!
//! Consequently, the class hierarchy follows the same division: there is `NTi::TPrimitiveType`,
//! from which all primitives are derived.
//!
//!
//! # Enumerator values
//!
//! Enumerator values are implementation detail and should not be relied upon. In particular, use `NTi::ToTypeName`
//! and `NTi::ToPrimitiveTypeName` to safely cast between `NTi::EPrimitiveTypeName` and `NTi::ETypeName`. Also, don't
//! use enumerator values for serialization and deserialization — convert enumerator values to strings
//! it you need persistence.

#include <util/system/types.h>
#include <util/generic/variant.h>

namespace NTi {
    /// Enum with names of all primitive types.
    ///
    /// See the file-level documentation.
    enum class EPrimitiveTypeName : i32 {
        Bool,

        Int8,
        Int16,
        Int32,
        Int64,
        Uint8,
        Uint16,
        Uint32,
        Uint64,

        Float,
        Double,

        String,
        Utf8,

        Date,
        Datetime,
        Timestamp,
        TzDate,
        TzDatetime,
        TzTimestamp,
        Interval,

        Decimal,
        Json,
        Yson,
        Uuid,

        Date32,
        Datetime64,
        Timestamp64,
        Interval64,
    };

    /// Enum with names of all types, including primitives.
    ///
    /// See the file-level documentation.
    enum class ETypeName : i32 {
        //
        // # Primitive types

        Bool,

        Int8,
        Int16,
        Int32,
        Int64,
        Uint8,
        Uint16,
        Uint32,
        Uint64,

        Float,
        Double,

        String,
        Utf8,

        Date,
        Datetime,
        Timestamp,
        TzDate,
        TzDatetime,
        TzTimestamp,
        Interval,

        Decimal,
        Json,
        Yson,
        Uuid,

        Date32,
        Datetime64,
        Timestamp64,
        Interval64,

        FIRST_PRIMITIVE = Bool,
        LAST_PRIMITIVE = Interval64,

        //
        // # Singular types

        Void,
        Null,

        FIRST_SINGULAR = Void,
        LAST_SINGULAR = Null,

        //
        // # Containers

        Optional,
        List,
        Dict,
        Struct,
        Tuple,
        Variant,
        Tagged,

        FIRST_CONTAINER = Optional,
        LAST_CONTAINER = Tagged,
    };

    /// Return true if the given type is a primitive one.
    ///
    /// Primitive type is a type that have no type parameters and is not a singular one.
    inline constexpr bool IsPrimitive(ETypeName typeName) {
        return ETypeName::FIRST_PRIMITIVE <= typeName && typeName <= ETypeName::LAST_PRIMITIVE;
    }

    /// Return true if the given type is one of singular types.
    ///
    /// Singular type is a type that has only one instance and therefore carries no information,
    /// i.e. occupy zero-length memory buffer.
    inline constexpr bool IsSingular(ETypeName typeName) {
        return ETypeName::FIRST_SINGULAR <= typeName && typeName <= ETypeName::LAST_SINGULAR;
    }

    /// Return true if the given type is one of containers.
    ///
    /// Container type is a type that has type parameters.
    inline constexpr bool IsContainer(ETypeName typeName) {
        return ETypeName::FIRST_CONTAINER <= typeName && typeName <= ETypeName::LAST_CONTAINER;
    }

    /// Return true if the given type has any type parameters.
    inline constexpr bool HasTypeParameters(ETypeName typeName) {
        return IsContainer(typeName);
    }

    /// Return true if the given type has any non-type parameters.
    inline constexpr bool HasNonTypeParameters(ETypeName typeName) {
        return typeName == ETypeName::Decimal;
    }

    /// Return true if the given type has any type or non-type parameters.
    inline constexpr bool HasParameters(ETypeName typeName) {
        return HasTypeParameters(typeName) || HasNonTypeParameters(typeName);
    }

    /// Safely cast `NTi::EPrimitiveTypeName` to `NTi::ETypeName`.
    ///
    /// Enumerator values should not relied upon, therefore users should not cast `NTi::EPrimitiveTypeName`
    /// to `NTi::ETypeName` using `static_cast`.
    inline constexpr ETypeName ToTypeName(EPrimitiveTypeName primitiveTypeName) {
        // Note: there's a test in ut/type_list.cpp that checks this is a safe conversion
        return static_cast<ETypeName>(primitiveTypeName);
    }

    /// Cast `NTi::ETypeName` to `NTi::EPrimitiveTypeName`, panic if the given type is not a primitive one.
    ///
    /// Enumerator values should not relied upon, therefore users should not cast `NTi::ETypeName`
    /// to `NTi::EPrimitiveTypeName` using `static_cast`.
    inline constexpr EPrimitiveTypeName ToPrimitiveTypeName(ETypeName typeName) {
        Y_ABORT_UNLESS(IsPrimitive(typeName));
        // Note: there's a test in ut/type_list.cpp that checks this is a safe conversion
        return static_cast<EPrimitiveTypeName>(typeName);
    }
}
