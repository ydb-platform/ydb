#pragma once

//! @file type_constructors.h

#include "type.h"

namespace NTi {
    /// Create new `Null` type using the default heap factory.
    TNullTypePtr Null();

    /// Create new `Bool` type using the default heap factory.
    TBoolTypePtr Bool();

    /// Create new `Int8` type using the default heap factory.
    TInt8TypePtr Int8();

    /// Create new `Int16` type using the default heap factory.
    TInt16TypePtr Int16();

    /// Create new `Int32` type using the default heap factory.
    TInt32TypePtr Int32();

    /// Create new `Int64` type using the default heap factory.
    TInt64TypePtr Int64();

    /// Create new `Uint8` type using the default heap factory.
    TUint8TypePtr Uint8();

    /// Create new `Uint16` type using the default heap factory.
    TUint16TypePtr Uint16();

    /// Create new `Uint32` type using the default heap factory.
    TUint32TypePtr Uint32();

    /// Create new `Uint64` type using the default heap factory.
    TUint64TypePtr Uint64();

    /// Create new `Float` type using the default heap factory.
    TFloatTypePtr Float();

    /// Create new `Double` type using the default heap factory.
    TDoubleTypePtr Double();

    /// Create new `String` type using the default heap factory.
    TStringTypePtr String();

    /// Create new `Utf8` type using the default heap factory.
    TUtf8TypePtr Utf8();

    /// Create new `Date` type using the default heap factory.
    TDateTypePtr Date();

    /// Create new `Datetime` type using the default heap factory.
    TDatetimeTypePtr Datetime();

    /// Create new `Timestamp` type using the default heap factory.
    TTimestampTypePtr Timestamp();

    /// Create new `TzDate` type using the default heap factory.
    TTzDateTypePtr TzDate();

    /// Create new `TzDatetime` type using the default heap factory.
    TTzDatetimeTypePtr TzDatetime();

    /// Create new `TzTimestamp` type using the default heap factory.
    TTzTimestampTypePtr TzTimestamp();

    /// Create new `Interval` type using the default heap factory.
    TIntervalTypePtr Interval();

    /// Create new `Decimal` type using the default heap factory.
    TDecimalTypePtr Decimal(ui8 precision, ui8 scale);

    /// Create new `Json` type using the default heap factory.
    TJsonTypePtr Json();

    /// Create new `Yson` type using the default heap factory.
    TYsonTypePtr Yson();

    /// Create new `Uuid` type using the default heap factory.
    TUuidTypePtr Uuid();

    /// Create new `Date32` type using the default heap factory.
    TDate32TypePtr Date32();

    /// Create new `Datetime64` type using the default heap factory.
    TDatetime64TypePtr Datetime64();

    /// Create new `Timestamp64` type using the default heap factory.
    TTimestamp64TypePtr Timestamp64();

    /// Create new `Interval64` type using the default heap factory.
    TInterval64TypePtr Interval64();

    /// Create new `Optional` type using the default heap factory.
    TOptionalTypePtr Optional(TTypePtr item);

    /// Create new `List` type using the default heap factory.
    TListTypePtr List(TTypePtr item);

    /// Create new `Dict` type using the default heap factory.
    TDictTypePtr Dict(TTypePtr key, TTypePtr value);

    /// Create new `Struct` type using the default heap factory.
    TStructTypePtr Struct(TStructType::TOwnedMembers items);
    /// Create new `Struct` type using the default heap factory.
    TStructTypePtr Struct(TMaybe<TStringBuf> name, TStructType::TOwnedMembers items);

    /// Create new `Tuple` type using the default heap factory.
    TTupleTypePtr Tuple(TTupleType::TOwnedElements items);
    /// Create new `Tuple` type using the default heap factory.
    TTupleTypePtr Tuple(TMaybe<TStringBuf> name, TTupleType::TOwnedElements items);

    /// Create new `Variant` type using the default heap factory.
    TVariantTypePtr Variant(TTypePtr underlying);
    /// Create new `Variant` type using the default heap factory.
    TVariantTypePtr Variant(TMaybe<TStringBuf> name, TTypePtr underlying);

    /// Create new `Tagged` type using the default heap factory.
    TTaggedTypePtr Tagged(TTypePtr type, TStringBuf tag);

} // namespace NTi
