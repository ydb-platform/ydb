#include "kqp_formats_arrow.h"

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <yql/essentials/minikql/mkql_type_helper.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/public/udf/arrow/block_type_helper.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/dynumber/dynumber.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NKikimr::NKqp::NFormats {

namespace {

template <typename TType>
std::shared_ptr<arrow::DataType> BuildArrowType(NUdf::EDataSlot slot) {
    Y_UNUSED(slot);
    return std::make_shared<TType>();
}

template <>
std::shared_ptr<arrow::DataType> BuildArrowType<arrow::FixedSizeBinaryType>(NUdf::EDataSlot slot) {
    Y_UNUSED(slot);
    return arrow::fixed_size_binary(NScheme::FSB_SIZE);
}

template <>
std::shared_ptr<arrow::DataType> BuildArrowType<arrow::StructType>(NUdf::EDataSlot slot) {
    std::shared_ptr<arrow::DataType> type;
    switch (slot) {
        case NUdf::EDataSlot::TzDate:
            type = NYql::NUdf::MakeTzLayoutArrowType<NUdf::EDataSlot::TzDate>();
            break;
        case NUdf::EDataSlot::TzDatetime:
            type = NYql::NUdf::MakeTzLayoutArrowType<NUdf::EDataSlot::TzDatetime>();
            break;
        case NUdf::EDataSlot::TzTimestamp:
            type = NYql::NUdf::MakeTzLayoutArrowType<NUdf::EDataSlot::TzTimestamp>();
            break;
        case NUdf::EDataSlot::TzDate32:
            type = NYql::NUdf::MakeTzLayoutArrowType<NUdf::EDataSlot::TzDate32>();
            break;
        case NUdf::EDataSlot::TzDatetime64:
            type = NYql::NUdf::MakeTzLayoutArrowType<NUdf::EDataSlot::TzDatetime64>();
            break;
        case NUdf::EDataSlot::TzTimestamp64:
            type = NYql::NUdf::MakeTzLayoutArrowType<NUdf::EDataSlot::TzTimestamp64>();
            break;
        default:
            YQL_ENSURE(false, "Unexpected timezone datetime slot");
            return std::make_shared<arrow::NullType>();
    }

    arrow::FieldVector fields{
        std::make_shared<arrow::Field>("datetime", type, false),
        std::make_shared<arrow::Field>("timezone", arrow::utf8(), false),
    };
    return arrow::struct_(fields);
}

std::shared_ptr<arrow::DataType> GetArrowType(const NMiniKQL::TDataType* dataType) {
    std::shared_ptr<arrow::DataType> result;
    bool success = SwitchMiniKQLDataTypeToArrowType(*dataType->GetDataSlot().Get(),
        [&]<typename TType>() {
            result = BuildArrowType<TType>(*dataType->GetDataSlot().Get());
            return true;
        });
    if (success) {
        return result;
    }
    return std::make_shared<arrow::NullType>();
}

std::shared_ptr<arrow::DataType> GetArrowType(const NMiniKQL::TStructType* structType) {
    arrow::FieldVector fields;
    fields.reserve(structType->GetMembersCount());
    for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
        auto memberType = structType->GetMemberType(index);
        auto memberName = std::string(structType->GetMemberName(index));
        auto memberArrowType = NFormats::GetArrowType(memberType);

        fields.emplace_back(std::make_shared<arrow::Field>(memberName, memberArrowType, memberType->IsOptional()));
    }
    return arrow::struct_(fields);
}

std::shared_ptr<arrow::DataType> GetArrowType(const NMiniKQL::TTupleType* tupleType) {
    arrow::FieldVector fields;
    fields.reserve(tupleType->GetElementsCount());
    for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
        auto elementName = "field" + std::to_string(index);
        auto elementType = tupleType->GetElementType(index);
        auto elementArrowType = NFormats::GetArrowType(elementType);

        fields.emplace_back(std::make_shared<arrow::Field>(elementName, elementArrowType, elementType->IsOptional()));
    }
    return arrow::struct_(fields);
}

std::shared_ptr<arrow::DataType> GetArrowType(const NMiniKQL::TListType* listType) {
    auto itemType = listType->GetItemType();
    auto itemArrowType = NFormats::GetArrowType(itemType);
    auto field = std::make_shared<arrow::Field>("item", itemArrowType, itemType->IsOptional());
    return arrow::list(field);
}

std::shared_ptr<arrow::DataType> GetArrowType(const NMiniKQL::TDictType* dictType) {
    auto keyType = dictType->GetKeyType();
    auto payloadType = dictType->GetPayloadType();

    auto structType = arrow::struct_({
        std::make_shared<arrow::Field>("key", NFormats::GetArrowType(keyType), keyType->IsOptional()),
        std::make_shared<arrow::Field>("payload", NFormats::GetArrowType(payloadType), payloadType->IsOptional())
    });
    return arrow::list(structType);
}

std::shared_ptr<arrow::DataType> GetArrowType(const NMiniKQL::TVariantType* variantType) {
    NMiniKQL::TType* innerType = variantType->GetUnderlyingType();
    NMiniKQL::TStructType* structType = nullptr;
    NMiniKQL::TTupleType* tupleType = nullptr;

    if (innerType->IsStruct()) {
        structType = static_cast<NMiniKQL::TStructType*>(innerType);
    } else {
        YQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
        tupleType = static_cast<NMiniKQL::TTupleType*>(innerType);
    }

    YQL_ENSURE(variantType->GetAlternativesCount() <= MAX_VARIANT_NESTED_SIZE, "Variant type has more than " << MAX_VARIANT_NESTED_SIZE << " alternatives");

    arrow::FieldVector fields;
    if (variantType->GetAlternativesCount() > MAX_VARIANT_FLATTEN_SIZE) {
        ui32 numberOfGroups = ((variantType->GetAlternativesCount() - 1) / MAX_VARIANT_FLATTEN_SIZE) + 1;
        fields.reserve(numberOfGroups);

        for (ui32 group = 0; group < numberOfGroups; ++group) {
            ui32 beginIndex = group * MAX_VARIANT_FLATTEN_SIZE;
            ui32 endIndex = std::min<ui32>((group + 1) * MAX_VARIANT_FLATTEN_SIZE, variantType->GetAlternativesCount());

            arrow::FieldVector groupFields;
            groupFields.reserve(endIndex - beginIndex);

            for (ui32 i = beginIndex; i < endIndex; ++i) {
                auto itemName = (structType == nullptr) ? std::string("field" + ToString(i)) : std::string(structType->GetMemberName(i));
                auto itemType = (structType == nullptr) ? tupleType->GetElementType(i) : structType->GetMemberType(i);
                auto itemArrowType = NFormats::GetArrowType(itemType);

                groupFields.emplace_back(std::make_shared<arrow::Field>( itemName, itemArrowType, itemType->IsOptional()));
            }

            std::vector<int8_t> typeCodes(groupFields.size());
            std::iota(typeCodes.begin(), typeCodes.end(), 0);

            auto fieldName = std::string("field" + ToString(group));
            fields.emplace_back(std::make_shared<arrow::Field>(fieldName, arrow::dense_union(groupFields, typeCodes), false));
        }

        return arrow::dense_union(fields);
    }

    fields.reserve(variantType->GetAlternativesCount());
    for (ui32 index = 0; index < variantType->GetAlternativesCount(); ++index) {
        auto itemName = (structType == nullptr) ? std::string("field" + ToString(index)) : std::string(structType->GetMemberName(index));
        auto itemType = (structType == nullptr) ? tupleType->GetElementType(index) : structType->GetMemberType(index);
        auto itemArrowType = NFormats::GetArrowType(itemType);

        fields.emplace_back(std::make_shared<arrow::Field>(itemName, itemArrowType, itemType->IsOptional()));
    }

    std::vector<int8_t> typeCodes(fields.size());
    std::iota(typeCodes.begin(), typeCodes.end(), 0);
    return arrow::dense_union(fields, typeCodes);
}

std::shared_ptr<arrow::DataType> GetArrowType(const NMiniKQL::TOptionalType* optionalType) {
    auto currentType = SkipTaggedType(optionalType->GetItemType());
    ui32 depth = 1;

    while (currentType->IsOptional()) {
        currentType = SkipTaggedType(static_cast<const NMiniKQL::TOptionalType*>(currentType)->GetItemType());
        ++depth;
    }

    // For types without native validity bitmap (e.g., Variant, Null) we need to wrap them in an additional struct layer
    // Furthermore, other singular types (e.g., Void, EmptyList, EmptyDict) also need to wrap (from YQL-15332)
    // Thus, the depth == 2 for Optional<Variant<T, F, ...>> type
    if (NeedWrapByExternalOptional(currentType)) {
        ++depth;
    }

    std::shared_ptr<arrow::DataType> innerArrowType = NFormats::GetArrowType(currentType);
    while (depth > 1) {
        innerArrowType = arrow::struct_({std::make_shared<arrow::Field>("opt", innerArrowType, true)});
        --depth;
    }
    return innerArrowType;
}

template <typename TArrowType>
void AppendDataValue(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value, NUdf::EDataSlot dataSlot) {
    Y_UNUSED(dataSlot);
    auto typedBuilder = reinterpret_cast<typename arrow::TypeTraits<TArrowType>::BuilderType*>(builder);
    arrow::Status status;
    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        status = typedBuilder->Append(value.Get<typename TArrowType::c_type>());
    }
    YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());
}

template <>
void AppendDataValue<arrow::UInt64Type>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value, NUdf::EDataSlot dataSlot) {
    Y_UNUSED(dataSlot);
    YQL_ENSURE(builder->type()->id() == arrow::Type::UINT64, "Unexpected builder type");
    auto typedBuilder = reinterpret_cast<arrow::UInt64Builder*>(builder);
    arrow::Status status;
    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        status = typedBuilder->Append(value.Get<ui64>());
    }
    YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());
}

template <>
void AppendDataValue<arrow::Int64Type>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value, NUdf::EDataSlot dataSlot) {
    Y_UNUSED(dataSlot);
    YQL_ENSURE(builder->type()->id() == arrow::Type::INT64, "Unexpected builder type");
    auto typedBuilder = reinterpret_cast<arrow::Int64Builder*>(builder);
    arrow::Status status;
    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        status = typedBuilder->Append(value.Get<i64>());
    }
    YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());
}

template <>
void AppendDataValue<arrow::StringType>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value, NUdf::EDataSlot dataSlot) {
    YQL_ENSURE(builder->type()->id() == arrow::Type::STRING, "Unexpected builder type");
    auto typedBuilder = reinterpret_cast<arrow::StringBuilder*>(builder);
    arrow::Status status;
    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        switch (dataSlot) {
            case NUdf::EDataSlot::Utf8:
            case NUdf::EDataSlot::Json: {
                auto data = value.AsStringRef();
                status = typedBuilder->Append(data.Data(), data.Size());
                break;
            }

            case NUdf::EDataSlot::JsonDocument: {
                YQL_ENSURE(NBinaryJson::IsValidBinaryJson(value.AsStringRef()));
                auto textJson = NBinaryJson::SerializeToJson(value.AsStringRef());
                status = typedBuilder->Append(textJson.data(), textJson.size());
                break;
            }

            case NUdf::EDataSlot::DyNumber: {
                auto number = NDyNumber::DyNumberToString(value.AsStringRef());
                YQL_ENSURE(number.Defined(), "Failed to convert DyNumber to string");
                status = typedBuilder->Append(number->data(), number->size());
                break;
            }

            default: {
                YQL_ENSURE(false, "Unexpected data slot");
            }
        }
    }
    YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());
}

template <>
void AppendDataValue<arrow::BinaryType>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value, NUdf::EDataSlot dataSlot) {
    Y_UNUSED(dataSlot);
    YQL_ENSURE(builder->type()->id() == arrow::Type::BINARY, "Unexpected builder type");
    auto typedBuilder = reinterpret_cast<arrow::BinaryBuilder*>(builder);
    arrow::Status status;
    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        auto data = value.AsStringRef();
        status = typedBuilder->Append(data.Data(), data.Size());
    }
    YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());
}

template <>
void AppendDataValue<arrow::StructType>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value, NUdf::EDataSlot dataSlot) {
    Y_UNUSED(dataSlot);
    YQL_ENSURE(builder->type()->id() == arrow::Type::STRUCT, "Unexpected builder type");
    auto typedBuilder = reinterpret_cast<arrow::StructBuilder*>(builder);
    YQL_ENSURE(typedBuilder->num_fields() == 2, "StructBuilder of timezone datetime types should have 2 fields");

    if (!value.HasValue()) {
        auto status = typedBuilder->AppendNull();
        YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());
        return;
    }

    auto status = typedBuilder->Append();
    YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());

    auto datetimeArray = typedBuilder->field_builder(0);
    auto timezoneArray = reinterpret_cast<arrow::StringBuilder*>(typedBuilder->field_builder(1));

    switch (dataSlot) {
        case NUdf::EDataSlot::TzDate: {
            YQL_ENSURE(datetimeArray->type()->id() == arrow::Type::UINT16);
            status = reinterpret_cast<arrow::UInt16Builder*>(datetimeArray)->Append(value.Get<ui16>());
            break;
        }

        case NUdf::EDataSlot::TzDatetime: {
            YQL_ENSURE(datetimeArray->type()->id() == arrow::Type::UINT32);
            status = reinterpret_cast<arrow::UInt32Builder*>(datetimeArray)->Append(value.Get<ui32>());
            break;
        }

        case NUdf::EDataSlot::TzTimestamp: {
            YQL_ENSURE(datetimeArray->type()->id() == arrow::Type::UINT64);
            status = reinterpret_cast<arrow::UInt64Builder*>(datetimeArray)->Append(value.Get<ui64>());
            break;
        }

        case NUdf::EDataSlot::TzDate32: {
            YQL_ENSURE(datetimeArray->type()->id() == arrow::Type::INT32);
            status = reinterpret_cast<arrow::Int32Builder*>(datetimeArray)->Append(value.Get<i32>());
            break;
        }

        case NUdf::EDataSlot::TzDatetime64:
        case NUdf::EDataSlot::TzTimestamp64: {
            YQL_ENSURE(datetimeArray->type()->id() == arrow::Type::INT64);
            status = reinterpret_cast<arrow::Int64Builder*>(datetimeArray)->Append(value.Get<i64>());
            break;
        }

        default: {
            YQL_ENSURE(false, "Unexpected timezone datetime slot");
            return;
        }
    }
    YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());

    auto tzName = NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
    status = timezoneArray->Append(tzName.Data(), tzName.size());
    YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());
}

template <>
void AppendDataValue<arrow::FixedSizeBinaryType>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value, NUdf::EDataSlot dataSlot) {
    YQL_ENSURE(builder->type()->id() == arrow::Type::FIXED_SIZE_BINARY, "Unexpected builder type");
    auto typedBuilder = reinterpret_cast<arrow::FixedSizeBinaryBuilder*>(builder);
    arrow::Status status;

    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
        YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());
        return;
    }

    switch (dataSlot) {
        case NUdf::EDataSlot::Uuid: {
            auto data = value.AsStringRef();
            status = typedBuilder->Append(data.Data());
            break;
        }

        case NUdf::EDataSlot::Decimal: {
            auto intVal = value.GetInt128();
            status = typedBuilder->Append(reinterpret_cast<const char*>(&intVal));
            break;
        }

        default: {
            YQL_ENSURE(false, "Unexpected data slot");
        }
    }

    YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());
}

void AppendElement(NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const NMiniKQL::TDataType* dataType) {
    auto slot = *dataType->GetDataSlot().Get();
    bool success = SwitchMiniKQLDataTypeToArrowType(slot, [&]<typename TType>() {
            AppendDataValue<TType>(builder, value, slot);
            return true;
        });
    YQL_ENSURE(success, "Failed to append data value to arrow builder");
}

void AppendElement(NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const NMiniKQL::TOptionalType* optionalType) {
    auto innerType = SkipTaggedType(optionalType->GetItemType());
    ui32 depth = 1;

    while (innerType->IsOptional()) {
        innerType = SkipTaggedType(static_cast<const NMiniKQL::TOptionalType*>(innerType) ->GetItemType());
        ++depth;
    }

    // For types without native validity bitmap (e.g., Variant, Null) we need to wrap them in an additional struct layer
    // Furthermore, other singular types (e.g., Void, EmptyList, EmptyDict) also need to wrap (from YQL-15332)
    // Thus, the depth == 2 for Optional<Variant<T, F, ...>> type
    if (NeedWrapByExternalOptional(innerType)) {
        ++depth;
    }

    auto innerBuilder = builder;
    auto innerValue = value;

    for (ui32 i = 1; i < depth; ++i) {
        YQL_ENSURE(innerBuilder->type()->id() == arrow::Type::STRUCT, "Unexpected builder type");
        auto structBuilder = reinterpret_cast<arrow::StructBuilder*>(innerBuilder);
        YQL_ENSURE(structBuilder->num_fields() == 1, "Unexpected number of fields");

        if (!innerValue) {
            auto status = innerBuilder->AppendNull();
            YQL_ENSURE(status.ok(), "Failed to append null optional value: " << status.ToString());
            return;
        }

        auto status = structBuilder->Append();
        YQL_ENSURE(status.ok(), "Failed to append optional value: " << status.ToString());

        innerValue = innerValue.GetOptionalValue();
        innerBuilder = structBuilder->field_builder(0);
    }

    if (innerValue) {
        NFormats::AppendElement(innerValue.GetOptionalValue(), innerBuilder, innerType);
    } else {
        auto status = innerBuilder->AppendNull();
        YQL_ENSURE(status.ok(), "Failed to append null optional value: " << status.ToString());
    }
}

void AppendElement(NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const NMiniKQL::TListType* listType) {
    auto itemType = listType->GetItemType();

    YQL_ENSURE(builder->type()->id() == arrow::Type::LIST, "Unexpected builder type");
    auto listBuilder = reinterpret_cast<arrow::ListBuilder*>(builder);

    auto status = listBuilder->Append();
    YQL_ENSURE(status.ok(), "Failed to append list value: " << status.ToString());

    auto innerBuilder = listBuilder->value_builder();
    if (auto item = value.GetElements()) {
        auto length = value.GetListLength();
        while (length > 0) {
            NFormats::AppendElement(*item++, innerBuilder, itemType);
            --length;
        }
    } else {
        const auto iter = value.GetListIterator();
        for (NUdf::TUnboxedValue item; iter.Next(item);) {
            NFormats::AppendElement(item, innerBuilder, itemType);
        }
    }
}

void AppendElement(NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const NMiniKQL::TStructType* structType) {
    YQL_ENSURE(builder->type()->id() == arrow::Type::STRUCT, "Unexpected builder type");
    auto structBuilder = reinterpret_cast<arrow::StructBuilder*>(builder);

    auto status = structBuilder->Append();
    YQL_ENSURE(status.ok(), "Failed to append struct value: " << status.ToString());

    YQL_ENSURE(static_cast<ui32>(structBuilder->num_fields()) == structType->GetMembersCount(), "Unexpected number of fields");
    for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
        auto innerBuilder = structBuilder->field_builder(index);
        auto memberType = structType->GetMemberType(index);
        NFormats::AppendElement(value.GetElement(index), innerBuilder, memberType);
    }
}

void AppendElement(NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const NMiniKQL::TTupleType* tupleType) {
    YQL_ENSURE(builder->type()->id() == arrow::Type::STRUCT, "Unexpected builder type");
    auto structBuilder = reinterpret_cast<arrow::StructBuilder*>(builder);

    auto status = structBuilder->Append();
    YQL_ENSURE(status.ok(), "Failed to append tuple value: " << status.ToString());

    YQL_ENSURE(static_cast<ui32>(structBuilder->num_fields()) == tupleType->GetElementsCount(), "Unexpected number of fields");
    for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
        auto innerBuilder = structBuilder->field_builder(index);
        auto elementType = tupleType->GetElementType(index);
        NFormats::AppendElement(value.GetElement(index), innerBuilder, elementType);
    }
}

void AppendElement(NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const NMiniKQL::TDictType* dictType) {
    auto keyType = dictType->GetKeyType();
    auto payloadType = dictType->GetPayloadType();

    YQL_ENSURE(builder->type()->id() == arrow::Type::LIST, "Unexpected builder type");
    auto listBuilder = reinterpret_cast<arrow::ListBuilder*>(builder);

    auto status = listBuilder->Append();
    YQL_ENSURE(status.ok(), "Failed to append dict value: " << status.ToString());

    YQL_ENSURE(listBuilder->value_builder()->type()->id() == arrow::Type::STRUCT, "Unexpected builder type");
    auto structBuilder = reinterpret_cast<arrow::StructBuilder*>(listBuilder->value_builder());
    YQL_ENSURE(structBuilder->num_fields() == 2, "Unexpected number of fields");

    auto keyBuilder = structBuilder->field_builder(0);
    auto itemBuilder = structBuilder->field_builder(1);

    const auto iter = value.GetDictIterator();
    for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
        auto status = structBuilder->Append();
        YQL_ENSURE(status.ok(), "Failed to append dict value: " << status.ToString());

        NFormats::AppendElement(key, keyBuilder, keyType);
        NFormats::AppendElement(payload, itemBuilder, payloadType);
    }
}

void AppendElement(NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const NMiniKQL::TVariantType* variantType) {
    YQL_ENSURE(builder->type()->id() == arrow::Type::DENSE_UNION, "Unexpected builder type");
    auto unionBuilder = reinterpret_cast<arrow::DenseUnionBuilder*>(builder);

    ui32 variantIndex = value.GetVariantIndex();
    NMiniKQL::TType* innerType = variantType->GetUnderlyingType();

    if (innerType->IsStruct()) {
        innerType = static_cast<NMiniKQL::TStructType*>(innerType)->GetMemberType(variantIndex);
    } else {
        YQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
        innerType = static_cast<NMiniKQL::TTupleType*>(innerType)->GetElementType(variantIndex);
    }

    YQL_ENSURE(variantType->GetAlternativesCount() <= MAX_VARIANT_NESTED_SIZE, "Variant type has more than " << MAX_VARIANT_NESTED_SIZE << " alternatives");

    if (variantType->GetAlternativesCount() > MAX_VARIANT_FLATTEN_SIZE) {
        ui32 numberOfGroups = ((variantType->GetAlternativesCount() - 1) / MAX_VARIANT_FLATTEN_SIZE) + 1;
        YQL_ENSURE(static_cast<ui32>(unionBuilder->num_children()) == numberOfGroups, "Unexpected variant number of groups");

        ui32 groupIndex = variantIndex / MAX_VARIANT_FLATTEN_SIZE;
        auto status = unionBuilder->Append(groupIndex);
        YQL_ENSURE(status.ok(), "Failed to append variant value: " << status.ToString());

        auto innerBuilder = unionBuilder->child_builder(groupIndex);
        YQL_ENSURE(innerBuilder->type()->id() == arrow::Type::DENSE_UNION, "Unexpected builder type");
        auto innerUnionBuilder = reinterpret_cast<arrow::DenseUnionBuilder*>(innerBuilder.get());

        ui32 innerVariantIndex = variantIndex % MAX_VARIANT_FLATTEN_SIZE;
        status = innerUnionBuilder->Append(innerVariantIndex);
        YQL_ENSURE(status.ok(), "Failed to append variant value: " << status.ToString());

        auto doubleInnerBuilder = innerUnionBuilder->child_builder(innerVariantIndex);
        NFormats::AppendElement(value.GetVariantItem(), doubleInnerBuilder.get(), innerType);
    } else {
        auto status = unionBuilder->Append(variantIndex);
        YQL_ENSURE(status.ok(), "Failed to append variant value: " << status.ToString());

        auto innerBuilder = unionBuilder->child_builder(variantIndex);
        NFormats::AppendElement(value.GetVariantItem(), innerBuilder.get(), innerType);
    }
}

} // namespace

bool NeedWrapByExternalOptional(const NMiniKQL::TType* type) {
    switch (type->GetKind()) {
        case NMiniKQL::TType::EKind::Null:
        case NMiniKQL::TType::EKind::Void:
        case NMiniKQL::TType::EKind::EmptyList:
        case NMiniKQL::TType::EKind::EmptyDict:
        case NMiniKQL::TType::EKind::Optional:
        case NMiniKQL::TType::EKind::Variant: {
            return true;
        }

        case NMiniKQL::TType::EKind::Data:
        case NMiniKQL::TType::EKind::Struct:
        case NMiniKQL::TType::EKind::Tuple:
        case NMiniKQL::TType::EKind::List:
        case NMiniKQL::TType::EKind::Dict:
        case NMiniKQL::TType::EKind::Tagged: {
            return false;
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
            YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
        }
    }
    return false;
}

std::shared_ptr<arrow::DataType> GetArrowType(const NMiniKQL::TType* type) {
    YQL_ENSURE(IsArrowCompatible(type));
    switch (type->GetKind()) {
        case NMiniKQL::TType::EKind::Null: {
            return arrow::null();
        }

        case NMiniKQL::TType::EKind::Void:
        case NMiniKQL::TType::EKind::EmptyList:
        case NMiniKQL::TType::EKind::EmptyDict: {
            return arrow::struct_({});
        }

        case NMiniKQL::TType::EKind::Data: {
            return GetArrowType(static_cast<const NMiniKQL::TDataType*>(type));
        }

        case NMiniKQL::TType::EKind::Optional: {
            return GetArrowType(static_cast<const NMiniKQL::TOptionalType*>(type));
        }

        case NMiniKQL::TType::EKind::Struct: {
            return GetArrowType(static_cast<const NMiniKQL::TStructType*>(type));
        }

        case NMiniKQL::TType::EKind::Tuple: {
            return GetArrowType(static_cast<const NMiniKQL::TTupleType*>(type));
        }

        case NMiniKQL::TType::EKind::List: {
            return GetArrowType(static_cast<const NMiniKQL::TListType*>(type));
        }

        case NMiniKQL::TType::EKind::Dict: {
            return GetArrowType(static_cast<const NMiniKQL::TDictType*>(type));
        }

        case NMiniKQL::TType::EKind::Variant: {
            return GetArrowType(static_cast<const NMiniKQL::TVariantType*>(type));
        }

        case NMiniKQL::TType::EKind::Tagged: {
            return GetArrowType(static_cast<const NMiniKQL::TTaggedType*>(type)->GetBaseType());
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
            YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
        }
    }
    return arrow::null();
}

bool IsArrowCompatible(const NKikimr::NMiniKQL::TType* type) {
    switch (type->GetKind()) {
        case NMiniKQL::TType::EKind::Null:
        case NMiniKQL::TType::EKind::Void:
        case NMiniKQL::TType::EKind::EmptyList:
        case NMiniKQL::TType::EKind::EmptyDict:
        case NMiniKQL::TType::EKind::Data: {
            return true;
        }

        case NMiniKQL::TType::EKind::Optional: {
            auto optionalType = static_cast<const NMiniKQL::TOptionalType*>(type);
            return IsArrowCompatible(optionalType->GetItemType());
        }

        case NMiniKQL::TType::EKind::Struct: {
            auto structType = static_cast<const NMiniKQL::TStructType*>(type);
            bool isCompatible = true;
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto memberType = structType->GetMemberType(index);
                isCompatible = isCompatible && IsArrowCompatible(memberType);
            }
            return isCompatible;
        }

        case NMiniKQL::TType::EKind::Tuple: {
            auto tupleType = static_cast<const NMiniKQL::TTupleType*>(type);
            bool isCompatible = true;
            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                auto elementType = tupleType->GetElementType(index);
                isCompatible = isCompatible && IsArrowCompatible(elementType);
            }
            return isCompatible;
        }

        case NMiniKQL::TType::EKind::List: {
            auto listType = static_cast<const NMiniKQL::TListType*>(type);
            auto itemType = listType->GetItemType();
            return IsArrowCompatible(itemType);
        }

        case NMiniKQL::TType::EKind::Dict: {
            auto dictType = static_cast<const NMiniKQL::TDictType*>(type);
            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();
            return IsArrowCompatible(keyType) && IsArrowCompatible(payloadType);
        }

        case NMiniKQL::TType::EKind::Variant: {
            auto variantType = static_cast<const NMiniKQL::TVariantType*>(type);
            if (variantType->GetAlternativesCount() > MAX_VARIANT_NESTED_SIZE) {
                return false;
            }

            NMiniKQL::TType* innerType = variantType->GetUnderlyingType();
            return (innerType->IsStruct() || innerType->IsTuple()) && IsArrowCompatible(innerType);
        }

        case NMiniKQL::TType::EKind::Tagged: {
            auto taggedType = static_cast<const NMiniKQL::TTaggedType*>(type);
            return IsArrowCompatible(taggedType->GetBaseType());
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
            return false;
        }
    }
    return true;
}

void AppendElement(NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const NMiniKQL::TType* type) {
    switch (type->GetKind()) {
        case NMiniKQL::TType::EKind::Null: {
            YQL_ENSURE(builder->type()->id() == arrow::Type::NA, "Unexpected builder type");
            auto status = builder->AppendNull();
            YQL_ENSURE(status.ok(), "Failed to append null value: " << status.ToString());
            break;
        }

        case NMiniKQL::TType::EKind::Void:
        case NMiniKQL::TType::EKind::EmptyList:
        case NMiniKQL::TType::EKind::EmptyDict: {
            YQL_ENSURE(builder->type()->id() == arrow::Type::STRUCT, "Unexpected builder type");
            auto structBuilder = reinterpret_cast<arrow::StructBuilder*>(builder);
            auto status = structBuilder->Append();
            YQL_ENSURE(status.ok(), "Failed to append struct value of a singular type: " << status.ToString());
            break;
        }

        case NMiniKQL::TType::EKind::Data: {
            AppendElement(value, builder, static_cast<const NMiniKQL::TDataType*>(type));
            break;
        }

        case NMiniKQL::TType::EKind::Optional: {
            AppendElement(value, builder, static_cast<const NMiniKQL::TOptionalType*>(type));
            break;
        }

        case NMiniKQL::TType::EKind::Struct: {
            AppendElement(value, builder, static_cast<const NMiniKQL::TStructType*>(type));
            break;
        }

        case NMiniKQL::TType::EKind::Tuple: {
            AppendElement(value, builder, static_cast<const NMiniKQL::TTupleType*>(type));
            break;
        }

        case NMiniKQL::TType::EKind::List: {
            AppendElement(value, builder, static_cast<const NMiniKQL::TListType*>(type));
            break;
        }

        case NMiniKQL::TType::EKind::Dict: {
            AppendElement(value, builder, static_cast<const NMiniKQL::TDictType*>(type));
            break;
        }

        case NMiniKQL::TType::EKind::Variant: {
            AppendElement(value, builder, static_cast<const NMiniKQL::TVariantType*>(type));
            break;
        }

        case NMiniKQL::TType::EKind::Tagged: {
            AppendElement(value, builder, static_cast<const NMiniKQL::TTaggedType*>(type)->GetBaseType());
            break;
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
            YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
        }
    }
}

} // namespace NKikimr::NKqp::NFormats
