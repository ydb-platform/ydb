#pragma once

#include <yql/essentials/public/udf/arrow/dispatch_traits.h>

namespace NKikimr::NMiniKQL::NLayoutConverter {

using namespace NYql::NUdf;

template <typename TTraits, typename... TArgs>
std::unique_ptr<typename TTraits::TResult> DispatchLayoutTraits(const ITypeInfoHelper& typeInfoHelper, const NUdf::TType* type, TArgs&&... args) {
    type = SkipTaggedType(typeInfoHelper, type);
    const NUdf::TType* unpacked = type;
    TOptionalTypeInspector typeOpt(typeInfoHelper, type);
    bool isOptional = false;
    if (typeOpt) {
        unpacked = typeOpt.GetItemType();
        isOptional = true;
    }

    unpacked = SkipTaggedType(typeInfoHelper, unpacked);

    if (NeedWrapWithExternalOptional(typeInfoHelper, type)) {
        ui32 nestLevel = 0;
        auto currentType = type;
        auto previousType = type;
        TVector<const NUdf::TType*> types;
        for (;;) {
            ++nestLevel;
            previousType = currentType;
            types.push_back(currentType);
            TOptionalTypeInspector currentOpt(typeInfoHelper, currentType);
            currentType = currentOpt.GetItemType();

            currentType = SkipTaggedType(typeInfoHelper, currentType);

            TOptionalTypeInspector nexOpt(typeInfoHelper, currentType);
            if (!nexOpt) {
                break;
            }
        }

        if (NeedWrapWithExternalOptional(typeInfoHelper, previousType)) {
            previousType = currentType;
            ++nestLevel;
        }

        auto reader = DispatchLayoutTraits<TTraits>(typeInfoHelper, previousType, args...);
        for (ui32 i = 1; i < nestLevel; ++i) {
            if constexpr (TTraits::PassType) {
                reader = std::make_unique<typename TTraits::TExtOptional>(std::move(reader), types[nestLevel - 1 - i], args...);
            } else {
                reader = std::make_unique<typename TTraits::TExtOptional>(std::move(reader), args...);
            }
        }

        return reader;
    }

    type = unpacked;

    TStructTypeInspector typeStruct(typeInfoHelper, type);
    if (typeStruct) {
        TVector<std::unique_ptr<typename TTraits::TResult>> members;
        for (ui32 i = 0; i < typeStruct.GetMembersCount(); i++) {
            members.emplace_back(DispatchLayoutTraits<TTraits>(typeInfoHelper, SkipTaggedType(typeInfoHelper, typeStruct.GetMemberType(i)), args...));
        }
        // XXX: Use Tuple block reader for Struct.
        return MakeTupleArrowTraitsImpl<TTraits>(isOptional, std::move(members), type, std::forward<TArgs>(args)...);
    }

    TTupleTypeInspector typeTuple(typeInfoHelper, type);
    if (typeTuple) {
        TVector<std::unique_ptr<typename TTraits::TResult>> children;
        for (ui32 i = 0; i < typeTuple.GetElementsCount(); ++i) {
            children.emplace_back(DispatchLayoutTraits<TTraits>(typeInfoHelper, SkipTaggedType(typeInfoHelper, typeTuple.GetElementType(i)), args...));
        }

        return MakeTupleArrowTraitsImpl<TTraits>(isOptional, std::move(children), type, std::forward<TArgs>(args)...);
    }

    TVariantTypeInspector typeVariant(typeInfoHelper, type);
    if (typeVariant) {
        const NUdf::TType* underlying = SkipTaggedType(typeInfoHelper, typeVariant.GetUnderlyingType());
        TStructTypeInspector typeStructUnderlying(typeInfoHelper, underlying);
        TTupleTypeInspector typeTupleUnderlying(typeInfoHelper, underlying);
        const ui32 altCount = typeStructUnderlying ? typeStructUnderlying.GetMembersCount() : typeTupleUnderlying.GetElementsCount();
        TVector<std::unique_ptr<typename TTraits::TResult>> children;
        children.reserve(altCount);
        for (ui32 i = 0; i < altCount; ++i) {
            const NUdf::TType* altType = typeStructUnderlying
                                       ? typeStructUnderlying.GetMemberType(i)
                                       : typeTupleUnderlying.GetElementType(i);
            children.emplace_back(DispatchLayoutTraits<TTraits>(
                typeInfoHelper,
                SkipTaggedType(typeInfoHelper, altType),
                args...));
        }
        return MakeVariantArrowTraitsImpl<TTraits>(isOptional, std::move(children), type, std::forward<TArgs>(args)...);
    }

    TDataTypeInspector typeData(typeInfoHelper, type);
    if (typeData) {
        auto typeId = typeData.GetTypeId();
        switch (GetDataSlot(typeId)) {
            case NUdf::EDataSlot::Int8:
                return MakeFixedSizeArrowTraitsImpl<TTraits, i8>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Bool:
            case NUdf::EDataSlot::Uint8:
                return MakeFixedSizeArrowTraitsImpl<TTraits, ui8>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Int16:
                return MakeFixedSizeArrowTraitsImpl<TTraits, i16>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Uint16:
            case NUdf::EDataSlot::Date:
                return MakeFixedSizeArrowTraitsImpl<TTraits, ui16>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Int32:
            case NUdf::EDataSlot::Date32:
                return MakeFixedSizeArrowTraitsImpl<TTraits, i32>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Uint32:
            case NUdf::EDataSlot::Datetime:
                return MakeFixedSizeArrowTraitsImpl<TTraits, ui32>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Int64:
            case NUdf::EDataSlot::Interval:
            case NUdf::EDataSlot::Interval64:
            case NUdf::EDataSlot::Datetime64:
            case NUdf::EDataSlot::Timestamp64:
                return MakeFixedSizeArrowTraitsImpl<TTraits, i64>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Uint64:
            case NUdf::EDataSlot::Timestamp:
                return MakeFixedSizeArrowTraitsImpl<TTraits, ui64>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Float:
                return MakeFixedSizeArrowTraitsImpl<TTraits, float>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Double:
                return MakeFixedSizeArrowTraitsImpl<TTraits, double>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::String:
                return MakeStringArrowTraitsImpl<TTraits, arrow::BinaryType, NUdf::EDataSlot::String>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Yson:
                return MakeStringArrowTraitsImpl<TTraits, arrow::BinaryType, NUdf::EDataSlot::Yson>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::JsonDocument:
                return MakeStringArrowTraitsImpl<TTraits, arrow::BinaryType, NUdf::EDataSlot::JsonDocument>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Utf8:
                return MakeStringArrowTraitsImpl<TTraits, arrow::StringType, NUdf::EDataSlot::Utf8>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Json:
                return MakeStringArrowTraitsImpl<TTraits, arrow::StringType, NUdf::EDataSlot::Json>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::TzDate:
                return MakeTzDateArrowTraitsImpl<TTraits, TTzDate>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::TzDatetime:
                return MakeTzDateArrowTraitsImpl<TTraits, TTzDatetime>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::TzTimestamp:
                return MakeTzDateArrowTraitsImpl<TTraits, TTzTimestamp>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::TzDate32:
                return MakeTzDateArrowTraitsImpl<TTraits, TTzDate32>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::TzDatetime64:
                return MakeTzDateArrowTraitsImpl<TTraits, TTzDatetime64>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::TzTimestamp64:
                return MakeTzDateArrowTraitsImpl<TTraits, TTzTimestamp64>(isOptional, type, std::forward<TArgs>(args)...);
            case NUdf::EDataSlot::Decimal: {
                if constexpr (CanInstantiateArrowTraitsForDecimal<TTraits>) {
                    return MakeFixedSizeArrowTraitsImpl<TTraits, NYql::NDecimal::TInt128>(isOptional, type, std::forward<TArgs>(args)...);
                } else {
                    Y_ENSURE(false, "Unsupported data slot");
                }
            }
            case NUdf::EDataSlot::Uuid: {
                if constexpr (requires { typename TTraits::template TFixedSize<TGUID, true>; }) {
                    return MakeFixedSizeArrowTraitsImpl<TTraits, TGUID>(isOptional, type, std::forward<TArgs>(args)...);
                } else {
                    Y_ENSURE(false, "Unsupported data slot");
                }
            }
            case NUdf::EDataSlot::DyNumber:
                return MakeStringArrowTraitsImpl<TTraits, arrow::BinaryType, NUdf::EDataSlot::DyNumber>(isOptional, type, std::forward<TArgs>(args)...);
        }
    }

    TResourceTypeInspector resource(typeInfoHelper, type);
    if (resource) {
        if constexpr (TTraits::PassType) {
            return TTraits::MakeResource(isOptional, type, std::forward<TArgs>(args)...);
        } else {
            return TTraits::MakeResource(isOptional, std::forward<TArgs>(args)...);
        }
    }

    if (IsSingularType(typeInfoHelper, type)) {
        Y_ENSURE(!isOptional, "Optional data types are not supported directly for singular type. Please use TExternalOptional wrapper.");
        bool isNull = typeInfoHelper.GetTypeKind(type) == ETypeKind::Null;
        if constexpr (TTraits::PassType) {
            if (isNull) {
                return TTraits::template MakeSingular<true>(type, std::forward<TArgs>(args)...);
            } else {
                return TTraits::template MakeSingular<false>(type, std::forward<TArgs>(args)...);
            }
        } else {
            if (isNull) {
                return TTraits::template MakeSingular<true>(std::forward<TArgs>(args)...);
            } else {
                return TTraits::template MakeSingular<false>(std::forward<TArgs>(args)...);
            }
        }
    }

    Y_ENSURE(false, "Unsupported type");
}

} // namespace NKikimr::NMiniKQL::NLayoutConverter
