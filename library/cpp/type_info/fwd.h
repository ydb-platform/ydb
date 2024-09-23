#pragma once

#include <util/generic/fwd.h>

namespace NTi {
    class ITypeFactoryInternal;

    class TNamedTypeBuilderRaw;
    class TStructBuilderRaw;
    class TTupleBuilderRaw;
    class TTaggedBuilderRaw;

    class ITypeFactory;
    using ITypeFactoryPtr = TIntrusivePtr<ITypeFactory>;

    class IPoolTypeFactory;
    using IPoolTypeFactoryPtr = TIntrusivePtr<IPoolTypeFactory>;

    class TType;
    using TTypePtr = TIntrusiveConstPtr<TType>;

    class TVoidType;
    using TVoidTypePtr = TIntrusiveConstPtr<TVoidType>;

    class TNullType;
    using TNullTypePtr = TIntrusiveConstPtr<TNullType>;

    class TPrimitiveType;
    using TPrimitiveTypePtr = TIntrusiveConstPtr<TPrimitiveType>;

    class TBoolType;
    using TBoolTypePtr = TIntrusiveConstPtr<TBoolType>;

    class TInt8Type;
    using TInt8TypePtr = TIntrusiveConstPtr<TInt8Type>;

    class TInt16Type;
    using TInt16TypePtr = TIntrusiveConstPtr<TInt16Type>;

    class TInt32Type;
    using TInt32TypePtr = TIntrusiveConstPtr<TInt32Type>;

    class TInt64Type;
    using TInt64TypePtr = TIntrusiveConstPtr<TInt64Type>;

    class TUint8Type;
    using TUint8TypePtr = TIntrusiveConstPtr<TUint8Type>;

    class TUint16Type;
    using TUint16TypePtr = TIntrusiveConstPtr<TUint16Type>;

    class TUint32Type;
    using TUint32TypePtr = TIntrusiveConstPtr<TUint32Type>;

    class TUint64Type;
    using TUint64TypePtr = TIntrusiveConstPtr<TUint64Type>;

    class TFloatType;
    using TFloatTypePtr = TIntrusiveConstPtr<TFloatType>;

    class TDoubleType;
    using TDoubleTypePtr = TIntrusiveConstPtr<TDoubleType>;

    class TStringType;
    using TStringTypePtr = TIntrusiveConstPtr<TStringType>;

    class TUtf8Type;
    using TUtf8TypePtr = TIntrusiveConstPtr<TUtf8Type>;

    class TDateType;
    using TDateTypePtr = TIntrusiveConstPtr<TDateType>;

    class TDatetimeType;
    using TDatetimeTypePtr = TIntrusiveConstPtr<TDatetimeType>;

    class TTimestampType;
    using TTimestampTypePtr = TIntrusiveConstPtr<TTimestampType>;

    class TTzDateType;
    using TTzDateTypePtr = TIntrusiveConstPtr<TTzDateType>;

    class TTzDatetimeType;
    using TTzDatetimeTypePtr = TIntrusiveConstPtr<TTzDatetimeType>;

    class TTzTimestampType;
    using TTzTimestampTypePtr = TIntrusiveConstPtr<TTzTimestampType>;

    class TIntervalType;
    using TIntervalTypePtr = TIntrusiveConstPtr<TIntervalType>;

    class TDecimalType;
    using TDecimalTypePtr = TIntrusiveConstPtr<TDecimalType>;

    class TJsonType;
    using TJsonTypePtr = TIntrusiveConstPtr<TJsonType>;

    class TYsonType;
    using TYsonTypePtr = TIntrusiveConstPtr<TYsonType>;

    class TUuidType;
    using TUuidTypePtr = TIntrusiveConstPtr<TUuidType>;

    class TDate32Type;
    using TDate32TypePtr = TIntrusiveConstPtr<TDate32Type>;

    class TDatetime64Type;
    using TDatetime64TypePtr = TIntrusiveConstPtr<TDatetime64Type>;

    class TTimestamp64Type;
    using TTimestamp64TypePtr = TIntrusiveConstPtr<TTimestamp64Type>;

    class TInterval64Type;
    using TInterval64TypePtr = TIntrusiveConstPtr<TInterval64Type>;

    class TOptionalType;
    using TOptionalTypePtr = TIntrusiveConstPtr<TOptionalType>;

    class TListType;
    using TListTypePtr = TIntrusiveConstPtr<TListType>;

    class TDictType;
    using TDictTypePtr = TIntrusiveConstPtr<TDictType>;

    class TStructType;
    using TStructTypePtr = TIntrusiveConstPtr<TStructType>;

    class TTupleType;
    using TTupleTypePtr = TIntrusiveConstPtr<TTupleType>;

    class TVariantType;
    using TVariantTypePtr = TIntrusiveConstPtr<TVariantType>;

    class TTaggedType;
    using TTaggedTypePtr = TIntrusiveConstPtr<TTaggedType>;
}
