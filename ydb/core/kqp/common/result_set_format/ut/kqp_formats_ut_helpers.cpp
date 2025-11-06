#include "kqp_formats_ut_helpers.h"

#include <ydb/core/kqp/common/result_set_format/kqp_formats_arrow.h>

#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_type_helper.h>
#include <yql/essentials/types/dynumber/dynumber.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NKikimr::NKqp::NFormats {

namespace {

template <typename TArrowType>
NUdf::TUnboxedValue GetUnboxedValue(std::shared_ptr<arrow::Array> column, ui32 row, NUdf::EDataSlot dataSlot) {
    Y_UNUSED(dataSlot);
    using TArrayType = typename arrow::TypeTraits<TArrowType>::ArrayType;
    auto array = std::static_pointer_cast<TArrayType>(column);
    return NUdf::TUnboxedValuePod(static_cast<typename TArrowType::c_type>(array->Value(row)));
}

template <> // For darwin build
NUdf::TUnboxedValue GetUnboxedValue<arrow::UInt64Type>(std::shared_ptr<arrow::Array> column, ui32 row, NUdf::EDataSlot dataSlot) {
    Y_UNUSED(dataSlot);
    auto array = std::static_pointer_cast<arrow::UInt64Array>(column);
    return NUdf::TUnboxedValuePod(static_cast<ui64>(array->Value(row)));
}

template <> // For darwin build
NUdf::TUnboxedValue GetUnboxedValue<arrow::Int64Type>(std::shared_ptr<arrow::Array> column, ui32 row, NUdf::EDataSlot dataSlot) {
    Y_UNUSED(dataSlot);
    auto array = std::static_pointer_cast<arrow::Int64Array>(column);
    return NUdf::TUnboxedValuePod(static_cast<i64>(array->Value(row)));
}

template <>
NUdf::TUnboxedValue GetUnboxedValue<arrow::StructType>(std::shared_ptr<arrow::Array> column, ui32 row, NUdf::EDataSlot dataSlot) {
    auto array = std::static_pointer_cast<arrow::StructArray>(column);
    YQL_ENSURE(array->num_fields() == 2, "StructArray of some TzDate type should have 2 fields");

    auto datetimeArray = array->field(0);
    auto timezoneArray = std::static_pointer_cast<arrow::StringArray>(array->field(1));

    NUdf::TUnboxedValuePod value;
    auto typeId = datetimeArray->type_id();

    switch (dataSlot) {
        case NUdf::EDataSlot::TzDate: {
            YQL_ENSURE(typeId == arrow::Type::UINT16);
            value = NUdf::TUnboxedValuePod(static_cast<ui16>(
                std::static_pointer_cast<arrow::UInt16Array>(datetimeArray)->Value(row)));
            break;
        }

        case NUdf::EDataSlot::TzDatetime: {
            YQL_ENSURE(typeId == arrow::Type::UINT32);
            value = NUdf::TUnboxedValuePod(static_cast<ui32>(
                std::static_pointer_cast<arrow::UInt32Array>(datetimeArray)->Value(row)));
            break;
        }

        case NUdf::EDataSlot::TzTimestamp: {
            YQL_ENSURE(typeId == arrow::Type::UINT64);
            value = NUdf::TUnboxedValuePod(static_cast<ui64>(
                std::static_pointer_cast<arrow::UInt64Array>(datetimeArray)->Value(row)));
            break;
        }

        case NUdf::EDataSlot::TzDate32: {
            YQL_ENSURE(typeId == arrow::Type::INT32);
            value = NUdf::TUnboxedValuePod(static_cast<i32>(
                std::static_pointer_cast<arrow::Int32Array>(datetimeArray)->Value(row)));
            break;
        }

        case NUdf::EDataSlot::TzDatetime64:
        case NUdf::EDataSlot::TzTimestamp64: {
            YQL_ENSURE(typeId == arrow::Type::INT64);
            value = NUdf::TUnboxedValuePod(static_cast<i64>(
                std::static_pointer_cast<arrow::Int64Array>(datetimeArray)->Value(row)));
            break;
        }

        default: {
            YQL_ENSURE(false, "Unexpected timezone datetime data type");
            return NUdf::TUnboxedValuePod();
        }
    }

    auto view = timezoneArray->Value(row);
    value.SetTimezoneId(NMiniKQL::GetTimezoneId(NUdf::TStringRef(view.data(), view.size())));
    return value;
}

template <>
NUdf::TUnboxedValue GetUnboxedValue<arrow::BinaryType>(std::shared_ptr<arrow::Array> column, ui32 row, NUdf::EDataSlot dataSlot) {
    Y_UNUSED(dataSlot);
    auto array = std::static_pointer_cast<arrow::BinaryArray>(column);
    auto data = array->GetView(row);
    return NMiniKQL::MakeString(NUdf::TStringRef(data.data(), data.size()));
}

template <>
NUdf::TUnboxedValue GetUnboxedValue<arrow::StringType>(std::shared_ptr<arrow::Array> column, ui32 row, NUdf::EDataSlot dataSlot) {
    auto array = std::static_pointer_cast<arrow::StringArray>(column);
    auto data = array->GetView(row);

    switch (dataSlot) {
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Json: {
            return NMiniKQL::MakeString(NUdf::TStringRef(data.data(), data.size()));
        }

        case NUdf::EDataSlot::JsonDocument: {
            auto variant = NBinaryJson::SerializeToBinaryJson(TStringBuf(data.data(), data.size()));
            if (std::holds_alternative<NBinaryJson::TBinaryJson>(variant)) {
                const auto& json = std::get<NBinaryJson::TBinaryJson>(variant);
                return NMiniKQL::MakeString(NUdf::TStringRef(json.Data(), json.Size()));
            }

            YQL_ENSURE(false, "Cannot serialize to binary json");
            break;
        }

        case NUdf::EDataSlot::DyNumber: {
            auto number = NDyNumber::ParseDyNumberString(TStringBuf(data.data(), data.size()));
            if (number.Defined()) {
                return NMiniKQL::MakeString(*number);
            }

            YQL_ENSURE(false, "Failed to convert string to DyNumber");
            break;
        }

        default: {
            YQL_ENSURE(false, "Unexpected data slot");
        }
    }
    return NUdf::TUnboxedValuePod();
}

template <>
NUdf::TUnboxedValue GetUnboxedValue<arrow::FixedSizeBinaryType>(std::shared_ptr<arrow::Array> column, ui32 row, NUdf::EDataSlot dataSlot) {
    auto array = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(column);
    auto data = array->GetView(row);

    switch (dataSlot) {
        case NUdf::EDataSlot::Uuid: {
            return NMiniKQL::MakeString(NUdf::TStringRef(data.data(), data.size()));
        }

        case NUdf::EDataSlot::Decimal: {
            NYql::NDecimal::TInt128 value;
            std::memcpy(&value, data.data(), data.size());
            return NUdf::TUnboxedValuePod(value);
        }

        default: {
            YQL_ENSURE(false, "Unexpected data slot");
        }
    }
    return NUdf::TUnboxedValuePod();
}

} // namespace

std::unique_ptr<arrow::ArrayBuilder> MakeArrowBuilder(const NMiniKQL::TType* type) {
    auto arrayType = GetArrowType(type);
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto status = arrow::MakeBuilder(arrow::default_memory_pool(), arrayType, &builder);
    YQL_ENSURE(status.ok(), "Failed to make arrow builder: " << status.ToString());
    return builder;
}

std::shared_ptr<arrow::Array> MakeArrowArray(NMiniKQL::TUnboxedValueVector& values, const NMiniKQL::TType* itemType) {
    auto builder = MakeArrowBuilder(itemType);
    auto status = builder->Reserve(values.size());
    YQL_ENSURE(status.ok(), "Failed to reserve space for array: " << status.ToString());
    for (auto& value : values) {
        AppendElement(value, builder.get(), itemType);
    }
    std::shared_ptr<arrow::Array> result;
    status = builder->Finish(&result);
    YQL_ENSURE(status.ok(), "Failed to finish array: " << status.ToString());
    return result;
}

NUdf::TUnboxedValue ExtractUnboxedValue(const std::shared_ptr<arrow::Array>& array, ui64 row, const NMiniKQL::TType* itemType,
    const NMiniKQL::THolderFactory& holderFactory)
{
    if (array->IsNull(row)) {
        return NUdf::TUnboxedValuePod();
    }

    switch (itemType->GetKind()) {
        case NMiniKQL::TType::EKind::Void:
        case NMiniKQL::TType::EKind::Null:
        case NMiniKQL::TType::EKind::EmptyList:
        case NMiniKQL::TType::EKind::EmptyDict: {
            break;
        }

        case NMiniKQL::TType::EKind::Data: {
            auto dataType = static_cast<const NMiniKQL::TDataType*>(itemType);
            NUdf::TUnboxedValue result;
            auto dataSlot = *dataType->GetDataSlot().Get();
            bool success = SwitchMiniKQLDataTypeToArrowType(dataSlot,
                [&]<typename TType>() {
                    result = GetUnboxedValue<TType>(array, row, dataSlot);
                    return true;
                });
            YQL_ENSURE(success, "Failed to extract unboxed value from arrow array");
            return result;
        }

        case NMiniKQL::TType::EKind::Struct: {
            auto structType = static_cast<const NMiniKQL::TStructType*>(itemType);

            YQL_ENSURE(array->type_id() == arrow::Type::STRUCT, "Unexpected array type");
            auto typedArray = static_pointer_cast<arrow::StructArray>(array);
            YQL_ENSURE(static_cast<ui32>(typedArray->num_fields()) == structType->GetMembersCount(), "Unexpected count of fields");

            NUdf::TUnboxedValue* itemsPtr = nullptr;
            auto result = holderFactory.CreateDirectArrayHolder(structType->GetMembersCount(), itemsPtr);

            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto memberType = structType->GetMemberType(index);
                itemsPtr[index] = ExtractUnboxedValue(typedArray->field(index), row, memberType, holderFactory);
            }
            return result;
        }

        case NMiniKQL::TType::EKind::Tuple: {
            auto tupleType = static_cast<const NMiniKQL::TTupleType*>(itemType);

            YQL_ENSURE(array->type_id() == arrow::Type::STRUCT, "Unexpected array type");
            auto typedArray = static_pointer_cast<arrow::StructArray>(array);
            YQL_ENSURE(static_cast<ui32>(typedArray->num_fields()) == tupleType->GetElementsCount(), "Unexpected count of fields");

            NUdf::TUnboxedValue* itemsPtr = nullptr;
            auto result = holderFactory.CreateDirectArrayHolder(tupleType->GetElementsCount(), itemsPtr);

            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                auto elementType = tupleType->GetElementType(index);
                itemsPtr[index] = ExtractUnboxedValue(typedArray->field(index), row, elementType, holderFactory);
            }
            return result;
        }

        case NMiniKQL::TType::EKind::Optional: {
            auto optionalType = static_cast<const NMiniKQL::TOptionalType*>(itemType);
            auto innerOptionalType = SkipTaggedType(optionalType->GetItemType());

            if (NeedWrapByExternalOptional(innerOptionalType)) {
                YQL_ENSURE(array->type_id() == arrow::Type::STRUCT, "Unexpected array type");

                auto innerArray = array;
                auto innerType = itemType;
                int depth = 0;

                while (innerArray->type_id() == arrow::Type::STRUCT) {
                    auto structArray = static_pointer_cast<arrow::StructArray>(innerArray);
                    YQL_ENSURE(structArray->num_fields() == 1, "Unexpected count of fields");

                    if (structArray->IsNull(row)) {
                        NUdf::TUnboxedValue value;
                        for (int i = 0; i < depth; ++i) {
                            value = value.MakeOptional();
                        }
                        return value;
                    }

                    innerType = SkipTaggedType(static_cast<const NMiniKQL::TOptionalType*>(innerType)->GetItemType());
                    innerArray = structArray->field(0);
                    ++depth;
                }

                NUdf::TUnboxedValue value;
                if (NeedWrapByExternalOptional(innerType) || !innerArray->IsNull(row)) {
                    value = ExtractUnboxedValue(innerArray, row, innerType, holderFactory);
                    if (NeedWrapByExternalOptional(innerType)) {
                        --depth;
                    }
                }

                for (int i = 0; i < depth; ++i) {
                    value = value.MakeOptional();
                }
                return value;
            }

            return ExtractUnboxedValue(array, row, innerOptionalType, holderFactory).Release().MakeOptional();
        }

        case NMiniKQL::TType::EKind::List: {
            auto listType = static_cast<const NMiniKQL::TListType*>(itemType);

            YQL_ENSURE(array->type_id() == arrow::Type::LIST, "Unexpected array type");
            auto typedArray = static_pointer_cast<arrow::ListArray>(array);

            auto arraySlice = typedArray->value_slice(row);
            auto itemType = listType->GetItemType();
            const auto len = arraySlice->length();

            NUdf::TUnboxedValue* items = nullptr;
            auto list = holderFactory.CreateDirectArrayHolder(len, items);
            for (ui64 i = 0; i < static_cast<ui64>(len); ++i) {
                *items++ = ExtractUnboxedValue(arraySlice, i, itemType, holderFactory);
            }
            return list;
        }

        case NMiniKQL::TType::EKind::Dict: {
            auto dictType = static_cast<const NMiniKQL::TDictType*>(itemType);

            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();
            auto dictBuilder = holderFactory.NewDict(dictType, 0);

            YQL_ENSURE(array->type_id() == arrow::Type::LIST, "Unexpected array type");
            auto listArray = static_pointer_cast<arrow::ListArray>(array);
            YQL_ENSURE(listArray->value_type()->id() == arrow::Type::STRUCT, "Unexpected array type");

            auto structArray = static_pointer_cast<arrow::StructArray>(listArray->value_slice(row));
            YQL_ENSURE(static_cast<ui32>(structArray->num_fields()) == 2, "Unexpected count of fields");

            std::shared_ptr<arrow::Array> keyArray = structArray->field(0);
            std::shared_ptr<arrow::Array> payloadArray = structArray->field(1);

            for (ui64 i = 0; i < static_cast<ui64>(structArray->length()); ++i) {
                auto key = ExtractUnboxedValue(keyArray, i, keyType, holderFactory);
                auto payload = ExtractUnboxedValue(payloadArray, i, payloadType, holderFactory);
                dictBuilder->Add(std::move(key), std::move(payload));
            }
            return dictBuilder->Build();
        }

        case NMiniKQL::TType::EKind::Variant: {
            // TODO Need to properly convert variants containing more than 127*127
            // types?
            auto variantType = static_cast<const NMiniKQL::TVariantType*>(itemType);

            YQL_ENSURE(array->type_id() == arrow::Type::DENSE_UNION, "Unexpected array type");
            auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(array);

            auto variantIndex = unionArray->child_id(row);
            auto rowInChild = unionArray->value_offset(row);
            std::shared_ptr<arrow::Array> valuesArray = unionArray->field(variantIndex);

            if (variantType->GetAlternativesCount() > arrow::UnionType::kMaxTypeCode) {
                // Go one step deeper
                YQL_ENSURE(valuesArray->type_id() == arrow::Type::DENSE_UNION, "Unexpected array type");
                auto innerUnionArray = static_pointer_cast<arrow::DenseUnionArray>(valuesArray);
                auto innerVariantIndex = innerUnionArray->child_id(rowInChild);

                rowInChild = innerUnionArray->value_offset(rowInChild);
                valuesArray = innerUnionArray->field(innerVariantIndex);
                variantIndex =variantIndex * arrow::UnionType::kMaxTypeCode + innerVariantIndex;
            }

            NMiniKQL::TType* innerType = variantType->GetUnderlyingType();
            if (innerType->IsStruct()) {
                innerType =static_cast<NMiniKQL::TStructType*>(innerType)->GetMemberType(variantIndex);
            } else {
                YQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
                innerType = static_cast<NMiniKQL::TTupleType*>(innerType)->GetElementType(variantIndex);
            }

            NUdf::TUnboxedValue value = ExtractUnboxedValue(valuesArray, rowInChild, innerType, holderFactory);
            return holderFactory.CreateVariantHolder(value.Release(), variantIndex);
        }

        case NMiniKQL::TType::EKind::Tagged: {
            auto taggedType = static_cast<const NMiniKQL::TTaggedType*>(itemType);
            return ExtractUnboxedValue(array, row, taggedType->GetBaseType(), holderFactory);
        }

        case NMiniKQL::TType::EKind::Type:
        case NMiniKQL::TType::EKind::Stream:
        case NMiniKQL::TType::EKind::Callable:
        case NMiniKQL::TType::EKind::Any:
        case NMiniKQL::TType::EKind::Resource:
        case NMiniKQL::TType::EKind::Flow:
        case NMiniKQL::TType::EKind::ReservedKind:
        case NMiniKQL::TType::EKind::Block:
        case NMiniKQL::TType::EKind::Pg:
        case NMiniKQL::TType::EKind::Multi:
        case NMiniKQL::TType::EKind::Linear: {
            YQL_ENSURE(false, "Unsupported type: " << itemType->GetKindAsStr());
        }
    }
    return NUdf::TUnboxedValuePod();
}

NMiniKQL::TUnboxedValueVector ExtractUnboxedVector(const std::shared_ptr<arrow::Array>& array, const NMiniKQL::TType* itemType,
    const NMiniKQL::THolderFactory& holderFactory)
{
    NMiniKQL::TUnboxedValueVector values;
    values.reserve(array->length());
    for (auto i = 0; i < array->length(); ++i) {
        values.push_back(ExtractUnboxedValue(array, i, itemType, holderFactory));
    }
    return values;
}

} // namespace NKikimr::NKqp::NFormats
