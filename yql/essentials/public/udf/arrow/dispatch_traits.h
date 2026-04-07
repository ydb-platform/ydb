#pragma once

#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/public/udf/udf_type_inspection.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include <arrow/type.h>

namespace NYql::NUdf {

template <typename TTraits, typename... TArgs>
std::unique_ptr<typename TTraits::TResult> MakeTupleArrowTraitsImpl(bool isOptional, TVector<std::unique_ptr<typename TTraits::TResult>>&& children, const TType* type, TArgs&&... args) {
    if (isOptional) {
        if constexpr (TTraits::PassType) {
            return std::make_unique<typename TTraits::template TTuple<true>>(std::move(children), type, std::forward<TArgs>(args)...);
        } else {
            return std::make_unique<typename TTraits::template TTuple<true>>(std::move(children), std::forward<TArgs>(args)...);
        }
    } else {
        if constexpr (TTraits::PassType) {
            return std::make_unique<typename TTraits::template TTuple<false>>(std::move(children), type, std::forward<TArgs>(args)...);
        } else {
            return std::make_unique<typename TTraits::template TTuple<false>>(std::move(children), std::forward<TArgs>(args)...);
        }
    }
}

template <typename TTraits, typename T, typename... TArgs>
std::unique_ptr<typename TTraits::TResult> MakeFixedSizeArrowTraitsImpl(bool isOptional, const TType* type, TArgs&&... args) {
    if (isOptional) {
        if constexpr (TTraits::PassType) {
            return std::make_unique<typename TTraits::template TFixedSize<T, true>>(type, std::forward<TArgs>(args)...);
        } else {
            return std::make_unique<typename TTraits::template TFixedSize<T, true>>(std::forward<TArgs>(args)...);
        }
    } else {
        if constexpr (TTraits::PassType) {
            return std::make_unique<typename TTraits::template TFixedSize<T, false>>(type, std::forward<TArgs>(args)...);
        } else {
            return std::make_unique<typename TTraits::template TFixedSize<T, false>>(std::forward<TArgs>(args)...);
        }
    }
}

template <typename TTraits, typename T, NKikimr::NUdf::EDataSlot TOriginal, typename... TArgs>
std::unique_ptr<typename TTraits::TResult> MakeStringArrowTraitsImpl(bool isOptional, const TType* type, TArgs&&... args) {
    if (isOptional) {
        if constexpr (TTraits::PassType) {
            return std::make_unique<typename TTraits::template TStrings<T, true, TOriginal>>(type, std::forward<TArgs>(args)...);
        } else {
            return std::make_unique<typename TTraits::template TStrings<T, true, TOriginal>>(std::forward<TArgs>(args)...);
        }
    } else {
        if constexpr (TTraits::PassType) {
            return std::make_unique<typename TTraits::template TStrings<T, false, TOriginal>>(type, std::forward<TArgs>(args)...);
        } else {
            return std::make_unique<typename TTraits::template TStrings<T, false, TOriginal>>(std::forward<TArgs>(args)...);
        }
    }
}

template <typename TTraits, typename TTzDate, typename... TArgs>
std::unique_ptr<typename TTraits::TResult> MakeTzDateArrowTraitsImpl(bool isOptional, const TType* type, TArgs&&... args) {
    if constexpr (TTraits::PassType) {
        return TTraits::template MakeTzDate<TTzDate>(isOptional, type, std::forward<TArgs>(args)...);
    } else {
        return TTraits::template MakeTzDate<TTzDate>(isOptional, std::forward<TArgs>(args)...);
    }
}

template <typename TTraits>
concept CanInstantiateArrowTraitsForDecimal = requires {
    typename TTraits::template TFixedSize<NYql::NDecimal::TInt128, true>;
};

template <typename TTraits, typename... TArgs>
std::unique_ptr<typename TTraits::TResult> DispatchByArrowTraits(const ITypeInfoHelper& typeInfoHelper, const TType* type, const IPgBuilder* pgBuilder, TArgs&&... args) {
    type = SkipTaggedType(typeInfoHelper, type);
    const TType* unpacked = type;
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
        TVector<const TType*> types;
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

        auto reader = DispatchByArrowTraits<TTraits>(typeInfoHelper, previousType, pgBuilder, args...);
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
            members.emplace_back(DispatchByArrowTraits<TTraits>(typeInfoHelper, SkipTaggedType(typeInfoHelper, typeStruct.GetMemberType(i)), pgBuilder, args...));
        }
        // XXX: Use Tuple block reader for Struct.
        return MakeTupleArrowTraitsImpl<TTraits>(isOptional, std::move(members), type, std::forward<TArgs>(args)...);
    }

    TTupleTypeInspector typeTuple(typeInfoHelper, type);
    if (typeTuple) {
        TVector<std::unique_ptr<typename TTraits::TResult>> children;
        for (ui32 i = 0; i < typeTuple.GetElementsCount(); ++i) {
            children.emplace_back(DispatchByArrowTraits<TTraits>(typeInfoHelper, SkipTaggedType(typeInfoHelper, typeTuple.GetElementType(i)), pgBuilder, args...));
        }

        return MakeTupleArrowTraitsImpl<TTraits>(isOptional, std::move(children), type, std::forward<TArgs>(args)...);
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
            case NUdf::EDataSlot::Uuid:
            case NUdf::EDataSlot::DyNumber:
                Y_ENSURE(false, "Unsupported data slot");
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

    TPgTypeInspector typePg(typeInfoHelper, type);
    if (typePg) {
        auto desc = typeInfoHelper.FindPgTypeDescription(typePg.GetTypeId());
        if constexpr (TTraits::PassType) {
            return TTraits::MakePg(*desc, pgBuilder, type, std::forward<TArgs>(args)...);
        } else {
            return TTraits::MakePg(*desc, pgBuilder, std::forward<TArgs>(args)...);
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

} // namespace NYql::NUdf
