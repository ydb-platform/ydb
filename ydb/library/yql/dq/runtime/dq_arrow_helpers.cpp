#include "dq_arrow_helpers.h"

#include <cstddef>

#include <yql/essentials/public/udf/arrow/block_type_helper.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/utils/yql_panic.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/type_fwd.h>

#include <util/system/compiler.h>
#include <util/system/yassert.h>

namespace NYql {
namespace NArrow {

using namespace NKikimr;
using namespace NMiniKQL;

namespace {

template <typename TArrowType>
struct TTypeWrapper
{
    using T = TArrowType;
};

/**
 * @brief Function to switch MiniKQL DataType correctly and uniformly converting it to arrow type using callback
 *
 * @tparam TFunc Callback type
 * @param typeId Type callback work with.
 * @param callback Template function of signature (TTypeWrapper) -> bool
 * @return Result of execution of callback or false if the type typeId is not supported.
 */
template <typename TFunc>
bool SwitchMiniKQLDataTypeToArrowType(NUdf::EDataSlot type, TFunc&& callback) {
    switch (type) {
        case NUdf::EDataSlot::Int8:
            return callback(TTypeWrapper<arrow::Int8Type>());
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Bool:
            return callback(TTypeWrapper<arrow::UInt8Type>());
        case NUdf::EDataSlot::Int16:
            return callback(TTypeWrapper<arrow::Int16Type>());
        case NUdf::EDataSlot::Date:
        case NUdf::EDataSlot::Uint16:
            return callback(TTypeWrapper<arrow::UInt16Type>());
        case NUdf::EDataSlot::Int32:
        case NUdf::EDataSlot::Date32:
            return callback(TTypeWrapper<arrow::Int32Type>());
        case NUdf::EDataSlot::Datetime:
        case NUdf::EDataSlot::Uint32:
            return callback(TTypeWrapper<arrow::UInt32Type>());
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Datetime64:
        case NUdf::EDataSlot::Timestamp64:
        case NUdf::EDataSlot::Interval64:
            return callback(TTypeWrapper<arrow::Int64Type>());
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return callback(TTypeWrapper<arrow::UInt64Type>());
        case NUdf::EDataSlot::Float:
            return callback(TTypeWrapper<arrow::FloatType>());
        case NUdf::EDataSlot::Double:
            return callback(TTypeWrapper<arrow::DoubleType>());
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Json:
            return callback(TTypeWrapper<arrow::StringType>());
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::DyNumber:
        case NUdf::EDataSlot::Yson:
        case NUdf::EDataSlot::JsonDocument:
            return callback(TTypeWrapper<arrow::BinaryType>());
        case NUdf::EDataSlot::Decimal:
        case NUdf::EDataSlot::Uuid:
            return callback(TTypeWrapper<arrow::FixedSizeBinaryType>());
        case NUdf::EDataSlot::TzDate:
        case NUdf::EDataSlot::TzDatetime:
        case NUdf::EDataSlot::TzTimestamp:
        case NUdf::EDataSlot::TzDate32:
        case NUdf::EDataSlot::TzDatetime64:
        case NUdf::EDataSlot::TzTimestamp64:
            return callback(TTypeWrapper<arrow::StructType>());
    }
}

bool NeedWrapByExternalOptional(const TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::Null:
        case TType::EKind::Variant:
        case TType::EKind::Optional:
            return true;
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict:
        case TType::EKind::Data:
        case TType::EKind::Struct:
        case TType::EKind::Tuple:
        case TType::EKind::List:
        case TType::EKind::Dict:
            return false;
        default:
            YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
    }

    return true;
}

template <typename TArrowType>
NUdf::TUnboxedValue GetUnboxedValue(std::shared_ptr<arrow::Array> column, ui32 row) {
    using TArrayType = typename arrow::TypeTraits<TArrowType>::ArrayType;
    auto array = std::static_pointer_cast<TArrayType>(column);
    return NUdf::TUnboxedValuePod(static_cast<typename TArrowType::c_type>(array->Value(row)));
}

template <>
NUdf::TUnboxedValue GetUnboxedValue<arrow::StructType>(std::shared_ptr<arrow::Array> column, ui32 row) {
    auto array = std::static_pointer_cast<arrow::StructArray>(column);
    YQL_ENSURE(array->num_fields() == 2, "StructArray of some TzDate type should have 2 fields");

    auto datetimeArray = array->field(0);
    auto timezoneArray = std::static_pointer_cast<arrow::UInt16Array>(array->field(1));

    NUdf::TUnboxedValuePod value;

    switch (datetimeArray->type()->id()) {
        // NUdf::EDataSlot::TzDate
        case arrow::Type::UINT16: {
            value = NUdf::TUnboxedValuePod(static_cast<ui16>(std::static_pointer_cast<arrow::UInt16Array>(datetimeArray)->Value(row)));
            break;
        }
        // NUdf::EDataSlot::TzDatetime
        case arrow::Type::UINT32: {
            value = NUdf::TUnboxedValuePod(static_cast<ui32>(std::static_pointer_cast<arrow::UInt32Array>(datetimeArray)->Value(row)));
            break;
        }
        // NUdf::EDataSlot::TzTimestamp
        case arrow::Type::UINT64: {
            value = NUdf::TUnboxedValuePod(static_cast<ui64>(std::static_pointer_cast<arrow::UInt64Array>(datetimeArray)->Value(row)));
            break;
        }
        // NUdf::EDataSlot::TzDate32
        case arrow::Type::INT32: {
            value = NUdf::TUnboxedValuePod(static_cast<i32>(std::static_pointer_cast<arrow::Int32Array>(datetimeArray)->Value(row)));
            break;
        }
        // NUdf::EDataSlot::TzDatetime64, NUdf::EDataSlot::TzTimestamp64
        case arrow::Type::INT64: {
            value = NUdf::TUnboxedValuePod(static_cast<i64>(std::static_pointer_cast<arrow::Int64Array>(datetimeArray)->Value(row)));
            break;
        }
        default:
            YQL_ENSURE(false, "Unexpected timezone datetime slot");
            return NUdf::TUnboxedValuePod();
    }

    value.SetTimezoneId(timezoneArray->Value(row));
    return value;
}

// The following specializations are for darwin build (because of difference in long long)

template <> // For darwin build
NUdf::TUnboxedValue GetUnboxedValue<arrow::UInt64Type>(std::shared_ptr<arrow::Array> column, ui32 row) {
    auto array = std::static_pointer_cast<arrow::UInt64Array>(column);
    return NUdf::TUnboxedValuePod(static_cast<ui64>(array->Value(row)));
}

template <> // For darwin build
NUdf::TUnboxedValue GetUnboxedValue<arrow::Int64Type>(std::shared_ptr<arrow::Array> column, ui32 row) {
    auto array = std::static_pointer_cast<arrow::Int64Array>(column);
    return NUdf::TUnboxedValuePod(static_cast<i64>(array->Value(row)));
}

template <>
NUdf::TUnboxedValue GetUnboxedValue<arrow::BinaryType>(std::shared_ptr<arrow::Array> column, ui32 row) {
    auto array = std::static_pointer_cast<arrow::BinaryArray>(column);
    auto data = array->GetView(row);
    return NMiniKQL::MakeString(NUdf::TStringRef(data.data(), data.size()));
}

template <>
NUdf::TUnboxedValue GetUnboxedValue<arrow::StringType>(std::shared_ptr<arrow::Array> column, ui32 row) {
    auto array = std::static_pointer_cast<arrow::StringArray>(column);
    auto data = array->GetView(row);
    return NMiniKQL::MakeString(NUdf::TStringRef(data.data(), data.size()));
}

template <>
NUdf::TUnboxedValue GetUnboxedValue<arrow::FixedSizeBinaryType>(std::shared_ptr<arrow::Array> column, ui32 row) {
    auto array = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(column);
    auto data = array->GetView(row);
    return NMiniKQL::MakeString(NUdf::TStringRef(data.data(), data.size()));
}

template <typename TType>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl(NUdf::EDataSlot slot) {
    Y_UNUSED(slot);
    return std::make_shared<TType>();
}

template <>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl<arrow::FixedSizeBinaryType>(NUdf::EDataSlot slot) {
    Y_UNUSED(slot);
    return arrow::fixed_size_binary(NScheme::FSB_SIZE);
}

template <>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl<arrow::StructType>(NUdf::EDataSlot slot) {
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

    std::vector<std::shared_ptr<arrow::Field>> fields {
        std::make_shared<arrow::Field>("datetime", type, false),
        std::make_shared<arrow::Field>("timezoneId", arrow::uint16(), false),
    };
    return arrow::struct_(fields);
}

std::shared_ptr<arrow::DataType> GetArrowType(const TDataType* dataType) {
    std::shared_ptr<arrow::DataType> result;
    bool success = SwitchMiniKQLDataTypeToArrowType(*dataType->GetDataSlot().Get(), [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
        Y_UNUSED(typeHolder);
        result = CreateEmptyArrowImpl<TType>(*dataType->GetDataSlot().Get());
        return true;
    });
    if (success) {
        return result;
    }
    return std::make_shared<arrow::NullType>();
}

std::shared_ptr<arrow::DataType> GetArrowType(const TStructType* structType) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(structType->GetMembersCount());
    for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
        auto memberType = structType->GetMemberType(index);
        auto memberName = std::string(structType->GetMemberName(index));
        auto memberArrowType = NArrow::GetArrowType(memberType);

        fields.emplace_back(std::make_shared<arrow::Field>(memberName, memberArrowType, memberType->IsOptional()));
    }
    return arrow::struct_(fields);
}

std::shared_ptr<arrow::DataType> GetArrowType(const TTupleType* tupleType) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(tupleType->GetElementsCount());
    for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
        auto elementName = std::string("field" + ToString(index));
        auto elementType = tupleType->GetElementType(index);
        auto elementArrowType = NArrow::GetArrowType(elementType);

        fields.push_back(std::make_shared<arrow::Field>(elementName, elementArrowType, elementType->IsOptional()));
    }
    return arrow::struct_(fields);
}

std::shared_ptr<arrow::DataType> GetArrowType(const TListType* listType) {
    auto itemType = listType->GetItemType();
    auto itemArrowType = NArrow::GetArrowType(itemType);
    auto field = std::make_shared<arrow::Field>("item", itemArrowType, itemType->IsOptional());
    return arrow::list(field);
}

std::shared_ptr<arrow::DataType> GetArrowType(const TDictType* dictType) {
    auto keyType = dictType->GetKeyType();
    auto payloadType = dictType->GetPayloadType();

    auto keyArrowType = NArrow::GetArrowType(keyType);
    auto payloadArrowType = NArrow::GetArrowType(payloadType);

    auto custom = std::make_shared<arrow::Field>("custom", arrow::uint64(), false);

    if (keyType->GetKind() == TType::EKind::Optional) {
        std::vector<std::shared_ptr<arrow::Field>> items;
        items.emplace_back(std::make_shared<arrow::Field>("key", keyArrowType, true));
        items.emplace_back(std::make_shared<arrow::Field>("payload", payloadArrowType, payloadType->IsOptional()));

        auto fieldMap = std::make_shared<arrow::Field>("map", arrow::list(arrow::struct_(items)), false);
        return arrow::struct_({fieldMap, custom});
    }

    auto fieldMap = std::make_shared<arrow::Field>("map", arrow::map(keyArrowType, payloadArrowType), false);
    return arrow::struct_({fieldMap, custom});
}

std::shared_ptr<arrow::DataType> GetArrowType(const TVariantType* variantType) {
    TType* innerType = variantType->GetUnderlyingType();
    arrow::FieldVector types;
    TStructType* structType = nullptr;
    TTupleType* tupleType = nullptr;

    if (innerType->IsStruct()) {
        structType = static_cast<TStructType*>(innerType);
    } else {
        YQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
        tupleType = static_cast<TTupleType*>(innerType);
    }

    // Create Union of unions if there are more types then arrow::dense_union supports.
    if (variantType->GetAlternativesCount() > arrow::UnionType::kMaxTypeCode) {
        ui32 numberOfGroups = (variantType->GetAlternativesCount() - 1) / arrow::UnionType::kMaxTypeCode + 1;
        types.reserve(numberOfGroups);

        for (ui32 groupIndex = 0; groupIndex < numberOfGroups; ++groupIndex) {
            ui32 beginIndex = groupIndex * arrow::UnionType::kMaxTypeCode;
            ui32 endIndex = std::min((groupIndex + 1) * arrow::UnionType::kMaxTypeCode, variantType->GetAlternativesCount());

            arrow::FieldVector groupTypes;
            groupTypes.reserve(endIndex - beginIndex);

            for (ui32 index = beginIndex; index < endIndex; ++index) {
                auto itemName = (structType == nullptr) ? std::string("field" + ToString(index)) : std::string(structType->GetMemberName(index));
                auto itemType = (structType == nullptr) ? tupleType->GetElementType(index) : structType->GetMemberType(index);
                auto itemArrowType = NArrow::GetArrowType(itemType);

                groupTypes.emplace_back(std::make_shared<arrow::Field>(itemName, itemArrowType, itemType->IsOptional()));
            }

            auto fieldName = std::string("field" + ToString(groupIndex));
            types.emplace_back(std::make_shared<arrow::Field>(fieldName, arrow::dense_union(groupTypes), false));
        }

        return arrow::dense_union(types);
    }

    // Else put all types in one arrow::dense_union
    types.reserve(variantType->GetAlternativesCount());
    for (ui32 index = 0; index < variantType->GetAlternativesCount(); ++index) {
        auto itemName = (structType == nullptr) ? std::string("field" + ToString(index)) : std::string(structType->GetMemberName(index));
        auto itemType = (structType == nullptr) ? tupleType->GetElementType(index) : structType->GetMemberType(index);
        auto itemArrowType = NArrow::GetArrowType(itemType);

        types.emplace_back(std::make_shared<arrow::Field>(itemName, itemArrowType, itemType->IsOptional()));
    }

    return arrow::dense_union(types);
}

std::shared_ptr<arrow::DataType> GetArrowType(const TOptionalType* optionalType) {
    auto currentType = optionalType->GetItemType();
    ui32 depth = 1;

    while (currentType->IsOptional()) {
        currentType = static_cast<const TOptionalType*>(currentType)->GetItemType();
        ++depth;
    }

    if (NeedWrapByExternalOptional(currentType)) {
        ++depth;
    }

    std::shared_ptr<arrow::DataType> innerArrowType = NArrow::GetArrowType(currentType);

    for (ui32 i = 1; i < depth; ++i) {
        auto field = std::make_shared<arrow::Field>("opt", innerArrowType, false);
        innerArrowType = std::make_shared<arrow::StructType>(std::vector<std::shared_ptr<arrow::Field>>{ field });
    }

    return innerArrowType;
}

template <typename TArrowType>
void AppendDataValue(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
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
void AppendDataValue<arrow::UInt64Type>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
    YQL_ENSURE(builder->type()->id() == arrow::Type::UINT64);
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
void AppendDataValue<arrow::Int64Type>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
    YQL_ENSURE(builder->type()->id() == arrow::Type::INT64);
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
void AppendDataValue<arrow::StringType>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
    YQL_ENSURE(builder->type()->id() == arrow::Type::STRING);
    auto typedBuilder = reinterpret_cast<arrow::StringBuilder*>(builder);
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
void AppendDataValue<arrow::BinaryType>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
    YQL_ENSURE(builder->type()->id() == arrow::Type::BINARY);
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

// Only for timezone datetime types
template <>
void AppendDataValue<arrow::StructType>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
    YQL_ENSURE(builder->type()->id() == arrow::Type::STRUCT);
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
    auto timezoneArray = reinterpret_cast<arrow::UInt16Builder*>(typedBuilder->field_builder(1));

    switch (datetimeArray->type()->id()) {
        // NUdf::EDataSlot::TzDate
        case arrow::Type::UINT16: {
            status = reinterpret_cast<arrow::UInt16Builder*>(datetimeArray)->Append(value.Get<ui16>());
            break;
        }
        // NUdf::EDataSlot::TzDatetime
        case arrow::Type::UINT32: {
            status = reinterpret_cast<arrow::UInt32Builder*>(datetimeArray)->Append(value.Get<ui32>());
            break;
        }
        // NUdf::EDataSlot::TzTimestamp
        case arrow::Type::UINT64: {
            status = reinterpret_cast<arrow::UInt64Builder*>(datetimeArray)->Append(value.Get<ui64>());
            break;
        }
        // NUdf::EDataSlot::TzDate32
        case arrow::Type::INT32: {
            status = reinterpret_cast<arrow::Int32Builder*>(datetimeArray)->Append(value.Get<i32>());
            break;
        }
        // NUdf::EDataSlot::TzDatetime64, NUdf::EDataSlot::TzTimestamp64
        case arrow::Type::INT64: {
            status = reinterpret_cast<arrow::Int64Builder*>(datetimeArray)->Append(value.Get<i64>());
            break;
        }
        default:
            YQL_ENSURE(false, "Unexpected timezone datetime slot");
            return;
    }
    YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());

    status = timezoneArray->Append(value.GetTimezoneId());
    YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());
}

template <typename TArrowType>
void AppendFixedSizeDataValue(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value, NUdf::EDataSlot dataSlot) {
    static_assert(std::is_same_v<TArrowType, arrow::FixedSizeBinaryType>, "This function is only for FixedSizeBinaryType");

    YQL_ENSURE(builder->type()->id() == arrow::Type::FIXED_SIZE_BINARY);
    auto typedBuilder = reinterpret_cast<arrow::FixedSizeBinaryBuilder*>(builder);
    arrow::Status status;

    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        if (dataSlot == NUdf::EDataSlot::Uuid) {
            auto data = value.AsStringRef();
            status = typedBuilder->Append(data.Data());
        } else if (dataSlot == NUdf::EDataSlot::Decimal) {
            auto intVal = value.GetInt128();
            status = typedBuilder->Append(reinterpret_cast<const char*>(&intVal));
        } else {
            YQL_ENSURE(false, "Unexpected data slot");
        }
    }
    YQL_ENSURE(status.ok(), "Failed to append data value: " << status.ToString());
}

} // namespace

std::shared_ptr<arrow::DataType> GetArrowType(const TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::Null:
            return arrow::null();
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict:
            return arrow::struct_({});
        case TType::EKind::Data: {
            auto dataType = static_cast<const TDataType*>(type);
            return GetArrowType(dataType);
        }
        case TType::EKind::Struct: {
            auto structType = static_cast<const TStructType*>(type);
            return GetArrowType(structType);
        }
        case TType::EKind::Tuple: {
            auto tupleType = static_cast<const TTupleType*>(type);
            return GetArrowType(tupleType);
        }
        case TType::EKind::Optional: {
            auto optionalType = static_cast<const TOptionalType*>(type);
            return GetArrowType(optionalType);
        }
        case TType::EKind::List: {
            auto listType = static_cast<const TListType*>(type);
            return GetArrowType(listType);
        }
        case TType::EKind::Dict: {
            auto dictType = static_cast<const TDictType*>(type);
            return GetArrowType(dictType);
        }
        case TType::EKind::Variant: {
            auto variantType = static_cast<const TVariantType*>(type);
            return GetArrowType(variantType);
        }
    default:
        YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
    }
    return arrow::null();
}

bool IsArrowCompatible(const NKikimr::NMiniKQL::TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::Null:
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict:
        case TType::EKind::Data:
            return true;

        case TType::EKind::Struct: {
            auto structType = static_cast<const TStructType*>(type);
            bool isCompatible = true;
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto memberType = structType->GetMemberType(index);
                isCompatible = isCompatible && IsArrowCompatible(memberType);
            }
            return isCompatible;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<const TTupleType*>(type);
            bool isCompatible = true;
            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                auto elementType = tupleType->GetElementType(index);
                isCompatible = isCompatible && IsArrowCompatible(elementType);
            }
            return isCompatible;
        }

        case TType::EKind::Optional: {
            auto optionalType = static_cast<const TOptionalType*>(type);
            auto innerOptionalType = optionalType->GetItemType();
            if (NeedWrapByExternalOptional(innerOptionalType)) {
                return false;
            }
            return IsArrowCompatible(innerOptionalType);
        }

        case TType::EKind::List: {
            auto listType = static_cast<const TListType*>(type);
            auto itemType = listType->GetItemType();
            return IsArrowCompatible(itemType);
        }

        case TType::EKind::Variant: {
            auto variantType = static_cast<const TVariantType*>(type);
            if (variantType->GetAlternativesCount() > arrow::UnionType::kMaxTypeCode) {
                return false;
            }
            TType* innerType = variantType->GetUnderlyingType();
            YQL_ENSURE(innerType->IsTuple() || innerType->IsStruct(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
            return IsArrowCompatible(innerType);
        }

        case TType::EKind::Dict:
        case TType::EKind::Block:
        case TType::EKind::Type:
        case TType::EKind::Stream:
        case TType::EKind::Callable:
        case TType::EKind::Any:
        case TType::EKind::Resource:
        case TType::EKind::ReservedKind:
        case TType::EKind::Flow:
        case TType::EKind::Tagged:
        case TType::EKind::Pg:
        case TType::EKind::Multi:
        case TType::EKind::Linear:
            return false;
    }
    return false;
}

std::unique_ptr<arrow::ArrayBuilder> MakeArrowBuilder(const TType* type) {
    auto arrayType = GetArrowType(type);
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto status = arrow::MakeBuilder(arrow::default_memory_pool(), arrayType, &builder);
    YQL_ENSURE(status.ok(), "Failed to make arrow builder: " << status.ToString());
    return builder;
}

void AppendElement(NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::Null: {
            YQL_ENSURE(builder->type()->id() == arrow::Type::NA, "Unexpected builder type");
            auto status = builder->AppendNull();
            YQL_ENSURE(status.ok(), "Failed to append null value: " << status.ToString());
            break;
        }

        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict: {
            YQL_ENSURE(builder->type()->id() == arrow::Type::STRUCT, "Unexpected builder type");
            auto structBuilder = reinterpret_cast<arrow::StructBuilder*>(builder);
            auto status = structBuilder->Append();
            YQL_ENSURE(status.ok(), "Failed to append empty dict/list value: " << status.ToString());
            break;
        }

        case TType::EKind::Data: {
            auto dataType = static_cast<const TDataType*>(type);
            auto slot = *dataType->GetDataSlot().Get();
            bool success = SwitchMiniKQLDataTypeToArrowType(slot, [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
                Y_UNUSED(typeHolder);
                if constexpr (std::is_same_v<TType, arrow::FixedSizeBinaryType>) {
                    AppendFixedSizeDataValue<TType>(builder, value, slot);
                } else {
                    AppendDataValue<TType>(builder, value);
                }
                return true;
            });
            YQL_ENSURE(success, "Failed to append data value to arrow builder");
            break;
        }

        case TType::EKind::Optional: {
            auto innerType = static_cast<const TOptionalType*>(type)->GetItemType();
            ui32 depth = 1;

            while (innerType->IsOptional()) {
                innerType = static_cast<const TOptionalType*>(innerType)->GetItemType();
                ++depth;
            }

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
                AppendElement(innerValue.GetOptionalValue(), innerBuilder, innerType);
            } else {
                auto status = innerBuilder->AppendNull();
                YQL_ENSURE(status.ok(), "Failed to append null optional value: " << status.ToString());
            }
            break;
        }

        case TType::EKind::List: {
            auto listType = static_cast<const TListType*>(type);
            auto itemType = listType->GetItemType();

            YQL_ENSURE(builder->type()->id() == arrow::Type::LIST, "Unexpected builder type");
            auto listBuilder = reinterpret_cast<arrow::ListBuilder*>(builder);

            auto status = listBuilder->Append();
            YQL_ENSURE(status.ok(), "Failed to append list value: " << status.ToString());

            auto innerBuilder = listBuilder->value_builder();
            if (auto item = value.GetElements()) {
                auto length = value.GetListLength();
                while (length > 0) {
                    AppendElement(*item++, innerBuilder, itemType);
                    --length;
                }
            } else {
                const auto iter = value.GetListIterator();
                for (NUdf::TUnboxedValue item; iter.Next(item);) {
                    AppendElement(item, innerBuilder, itemType);
                }
            }
            break;
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<const TStructType*>(type);

            YQL_ENSURE(builder->type()->id() == arrow::Type::STRUCT, "Unexpected builder type");
            auto structBuilder = reinterpret_cast<arrow::StructBuilder*>(builder);

            auto status = structBuilder->Append();
            YQL_ENSURE(status.ok(), "Failed to append struct value: " << status.ToString());

            YQL_ENSURE(static_cast<ui32>(structBuilder->num_fields()) == structType->GetMembersCount(), "Unexpected number of fields");
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto innerBuilder = structBuilder->field_builder(index);
                auto memberType = structType->GetMemberType(index);
                AppendElement(value.GetElement(index), innerBuilder, memberType);
            }
            break;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<const TTupleType*>(type);

            YQL_ENSURE(builder->type()->id() == arrow::Type::STRUCT, "Unexpected builder type");
            auto structBuilder = reinterpret_cast<arrow::StructBuilder*>(builder);

            auto status = structBuilder->Append();
            YQL_ENSURE(status.ok(), "Failed to append tuple value: " << status.ToString());

            YQL_ENSURE(static_cast<ui32>(structBuilder->num_fields()) == tupleType->GetElementsCount(), "Unexpected number of fields");
            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                auto innerBuilder = structBuilder->field_builder(index);
                auto elementType = tupleType->GetElementType(index);
                AppendElement(value.GetElement(index), innerBuilder, elementType);
            }
            break;
        }

        case TType::EKind::Dict: {
            auto dictType = static_cast<const TDictType*>(type);
            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();

            arrow::ArrayBuilder* keyBuilder = nullptr;
            arrow::ArrayBuilder* itemBuilder = nullptr;
            arrow::StructBuilder* structBuilder = nullptr;

            YQL_ENSURE(builder->type()->id() == arrow::Type::STRUCT, "Unexpected builder type");
            arrow::StructBuilder* wrapBuilder = reinterpret_cast<arrow::StructBuilder*>(builder);
            YQL_ENSURE(wrapBuilder->num_fields() == 2, "Unexpected number of fields");

            auto status = wrapBuilder->Append();
            YQL_ENSURE(status.ok(), "Failed to append dict value: " << status.ToString());

            if (keyType->GetKind() == TType::EKind::Optional) {
                YQL_ENSURE(wrapBuilder->field_builder(0)->type()->id() == arrow::Type::LIST, "Unexpected builder type");
                auto listBuilder = reinterpret_cast<arrow::ListBuilder*>(wrapBuilder->field_builder(0));

                auto status = listBuilder->Append();
                YQL_ENSURE(status.ok(), "Failed to append dict value: " << status.ToString());

                YQL_ENSURE(listBuilder->value_builder()->type()->id() == arrow::Type::STRUCT, "Unexpected builder type");
                structBuilder = reinterpret_cast<arrow::StructBuilder*>(listBuilder->value_builder());
                YQL_ENSURE(structBuilder->num_fields() == 2, "Unexpected number of fields");

                keyBuilder = structBuilder->field_builder(0);
                itemBuilder = structBuilder->field_builder(1);
            } else {
                YQL_ENSURE(wrapBuilder->field_builder(0)->type()->id() == arrow::Type::MAP, "Unexpected builder type");
                auto mapBuilder = reinterpret_cast<arrow::MapBuilder*>(wrapBuilder->field_builder(0));

                auto status = mapBuilder->Append();
                YQL_ENSURE(status.ok(), "Failed to append dict value: " << status.ToString());

                keyBuilder = mapBuilder->key_builder();
                itemBuilder = mapBuilder->item_builder();
            }

            arrow::UInt64Builder* customBuilder = reinterpret_cast<arrow::UInt64Builder*>(wrapBuilder->field_builder(1));
            status = customBuilder->Append(0);
            YQL_ENSURE(status.ok(), "Failed to append dict value: " << status.ToString());

            // We do not sort dictionary before appending it to builder.
            const auto iter = value.GetDictIterator();
            for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                if (structBuilder != nullptr) {
                    status = structBuilder->Append();
                    YQL_ENSURE(status.ok(), "Failed to append dict value: " << status.ToString());
                }

                AppendElement(key, keyBuilder, keyType);
                AppendElement(payload, itemBuilder, payloadType);
            }
            break;
        }

        case TType::EKind::Variant: {
            // TODO Need to properly convert variants containing more than 127*127 types?
            auto variantType = static_cast<const TVariantType*>(type);

            YQL_ENSURE(builder->type()->id() == arrow::Type::DENSE_UNION, "Unexpected builder type");
            auto unionBuilder = reinterpret_cast<arrow::DenseUnionBuilder*>(builder);

            ui32 variantIndex = value.GetVariantIndex();
            TType* innerType = variantType->GetUnderlyingType();

            if (innerType->IsStruct()) {
                innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
            } else {
                YQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
                innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
            }

            if (variantType->GetAlternativesCount() > arrow::UnionType::kMaxTypeCode) {
                ui32 numberOfGroups = (variantType->GetAlternativesCount() - 1) / arrow::UnionType::kMaxTypeCode + 1;
                YQL_ENSURE(static_cast<ui32>(unionBuilder->num_children()) == numberOfGroups);

                ui32 groupIndex = variantIndex / arrow::UnionType::kMaxTypeCode;
                auto status = unionBuilder->Append(groupIndex);
                YQL_ENSURE(status.ok(), "Failed to append variant value: " << status.ToString());

                auto innerBuilder = unionBuilder->child_builder(groupIndex);
                YQL_ENSURE(innerBuilder->type()->id() == arrow::Type::DENSE_UNION);
                auto innerUnionBuilder = reinterpret_cast<arrow::DenseUnionBuilder*>(innerBuilder.get());

                ui32 innerVariantIndex = variantIndex % arrow::UnionType::kMaxTypeCode;
                status = innerUnionBuilder->Append(innerVariantIndex);
                YQL_ENSURE(status.ok(), "Failed to append variant value: " << status.ToString());

                auto doubleInnerBuilder = innerUnionBuilder->child_builder(innerVariantIndex);
                AppendElement(value.GetVariantItem(), doubleInnerBuilder.get(), innerType);
            } else {
                auto status = unionBuilder->Append(variantIndex);
                YQL_ENSURE(status.ok(), "Failed to append variant value: " << status.ToString());

                auto innerBuilder = unionBuilder->child_builder(variantIndex);
                AppendElement(value.GetVariantItem(), innerBuilder.get(), innerType);
            }
            break;
        }

    default:
        YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
    }
}

std::shared_ptr<arrow::Array> MakeArray(NMiniKQL::TUnboxedValueVector& values, const TType* itemType) {
    auto builder = MakeArrowBuilder(itemType);
    auto status = builder->Reserve(values.size());
    YQL_ENSURE(status.ok(), "Failed to reserve space for array: " << status.ToString());
    for (auto& value: values) {
        AppendElement(value, builder.get(), itemType);
    }
    std::shared_ptr<arrow::Array> result;
    status = builder->Finish(&result);
    YQL_ENSURE(status.ok(), "Failed to finish array: " << status.ToString());
    return result;
}

NUdf::TUnboxedValue ExtractUnboxedValue(const std::shared_ptr<arrow::Array>& array, ui64 row, const TType* itemType, const NMiniKQL::THolderFactory& holderFactory) {
    if (array->IsNull(row)) {
        return NUdf::TUnboxedValuePod();
    }

    switch(itemType->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::Null:
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict:
            break;

        case TType::EKind::Data: {
            auto dataType = static_cast<const TDataType*>(itemType);
            NUdf::TUnboxedValue result;
            bool success = SwitchMiniKQLDataTypeToArrowType(*dataType->GetDataSlot().Get(), [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
                Y_UNUSED(typeHolder);
                result = GetUnboxedValue<TType>(array, row);
                return true;
            });
            Y_ENSURE(success, "Failed to extract unboxed value from arrow array");
            return result;
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<const TStructType*>(itemType);

            YQL_ENSURE(array->type_id() == arrow::Type::STRUCT);
            auto typedArray = static_pointer_cast<arrow::StructArray>(array);
            YQL_ENSURE(static_cast<ui32>(typedArray->num_fields()) == structType->GetMembersCount());

            NUdf::TUnboxedValue* itemsPtr = nullptr;
            auto result = holderFactory.CreateDirectArrayHolder(structType->GetMembersCount(), itemsPtr);

            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto memberType = structType->GetMemberType(index);
                itemsPtr[index] = ExtractUnboxedValue(typedArray->field(index), row, memberType, holderFactory);
            }
            return result;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<const TTupleType*>(itemType);

            YQL_ENSURE(array->type_id() == arrow::Type::STRUCT);
            auto typedArray = static_pointer_cast<arrow::StructArray>(array);
            YQL_ENSURE(static_cast<ui32>(typedArray->num_fields()) == tupleType->GetElementsCount());

            NUdf::TUnboxedValue* itemsPtr = nullptr;
            auto result = holderFactory.CreateDirectArrayHolder(tupleType->GetElementsCount(), itemsPtr);

            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                auto elementType = tupleType->GetElementType(index);
                itemsPtr[index] = ExtractUnboxedValue(typedArray->field(index), row, elementType, holderFactory);
            }
            return result;
        }

        case TType::EKind::Optional: {
            auto optionalType = static_cast<const TOptionalType*>(itemType);
            auto innerOptionalType = optionalType->GetItemType();

            if (NeedWrapByExternalOptional(innerOptionalType)) {
                YQL_ENSURE(array->type_id() == arrow::Type::STRUCT);

                auto innerArray = array;
                auto innerType = itemType;

                NUdf::TUnboxedValue value;
                int depth = 0;

                while (innerArray->type_id() == arrow::Type::STRUCT) {
                    auto structArray = static_pointer_cast<arrow::StructArray>(innerArray);
                    YQL_ENSURE(structArray->num_fields() == 1);

                    if (structArray->IsNull(row)) {
                        value = NUdf::TUnboxedValuePod();
                        break;
                    }

                    innerType = static_cast<const TOptionalType*>(innerType)->GetItemType();
                    innerArray = structArray->field(0);
                    ++depth;
                }

                auto wrap = NeedWrapByExternalOptional(innerType);
                if (wrap || !innerArray->IsNull(row)) {
                    value = ExtractUnboxedValue(innerArray, row, innerType, holderFactory);
                    if (wrap) {
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

        case TType::EKind::List: {
            auto listType = static_cast<const TListType*>(itemType);

            YQL_ENSURE(array->type_id() == arrow::Type::LIST);
            auto typedArray = static_pointer_cast<arrow::ListArray>(array);

            auto arraySlice = typedArray->value_slice(row);
            auto itemType = listType->GetItemType();
            const auto len = arraySlice->length();

            NUdf::TUnboxedValue *items = nullptr;
            auto list = holderFactory.CreateDirectArrayHolder(len, items);
            for (ui64 i = 0; i < static_cast<ui64>(len); ++i) {
                *items++ = ExtractUnboxedValue(arraySlice, i, itemType, holderFactory);
            }
            return list;
        }

        case TType::EKind::Dict: {
            auto dictType = static_cast<const TDictType*>(itemType);

            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();
            auto dictBuilder = holderFactory.NewDict(dictType, NUdf::TDictFlags::EDictKind::Hashed);

            std::shared_ptr<arrow::Array> keyArray = nullptr;
            std::shared_ptr<arrow::Array> payloadArray = nullptr;
            ui64 dictLength = 0;
            ui64 offset = 0;

            YQL_ENSURE(array->type_id() == arrow::Type::STRUCT);
            auto wrapArray = static_pointer_cast<arrow::StructArray>(array);
            YQL_ENSURE(wrapArray->num_fields() == 2);

            auto dictSlice = wrapArray->field(0);

            if (keyType->GetKind() == TType::EKind::Optional) {
                YQL_ENSURE(dictSlice->type_id() == arrow::Type::LIST);
                auto listArray = static_pointer_cast<arrow::ListArray>(dictSlice);

                auto arraySlice = listArray->value_slice(row);
                YQL_ENSURE(arraySlice->type_id() == arrow::Type::STRUCT);
                auto structArray = static_pointer_cast<arrow::StructArray>(arraySlice);
                YQL_ENSURE(structArray->num_fields() == 2);

                dictLength = arraySlice->length();
                keyArray = structArray->field(0);
                payloadArray = structArray->field(1);
            } else {
                YQL_ENSURE(dictSlice->type_id() == arrow::Type::MAP);
                auto mapArray = static_pointer_cast<arrow::MapArray>(dictSlice);

                dictLength = mapArray->value_length(row);
                offset = mapArray->value_offset(row);
                keyArray = mapArray->keys();
                payloadArray = mapArray->items();
            }

            for (ui64 i = offset; i < offset + static_cast<ui64>(dictLength); ++i) {
                auto key = ExtractUnboxedValue(keyArray, i, keyType, holderFactory);
                auto payload = ExtractUnboxedValue(payloadArray, i, payloadType, holderFactory);
                dictBuilder->Add(std::move(key), std::move(payload));
            }
            return dictBuilder->Build();
        }

        case TType::EKind::Variant: {
            // TODO Need to properly convert variants containing more than 127*127 types?
            auto variantType = static_cast<const TVariantType*>(itemType);

            YQL_ENSURE(array->type_id() == arrow::Type::DENSE_UNION);
            auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(array);

            auto variantIndex = unionArray->child_id(row);
            auto rowInChild = unionArray->value_offset(row);
            std::shared_ptr<arrow::Array> valuesArray = unionArray->field(variantIndex);

            if (variantType->GetAlternativesCount() > arrow::UnionType::kMaxTypeCode) {
                // Go one step deeper
                YQL_ENSURE(valuesArray->type_id() == arrow::Type::DENSE_UNION);
                auto innerUnionArray = static_pointer_cast<arrow::DenseUnionArray>(valuesArray);
                auto innerVariantIndex = innerUnionArray->child_id(rowInChild);

                rowInChild = innerUnionArray->value_offset(rowInChild);
                valuesArray = innerUnionArray->field(innerVariantIndex);
                variantIndex = variantIndex * arrow::UnionType::kMaxTypeCode + innerVariantIndex;
            }

            TType* innerType = variantType->GetUnderlyingType();
            if (innerType->IsStruct()) {
                innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
            } else {
                YQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
                innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
            }

            NUdf::TUnboxedValue value = ExtractUnboxedValue(valuesArray, rowInChild, innerType, holderFactory);
            return holderFactory.CreateVariantHolder(value.Release(), variantIndex);
        }
    default:
        YQL_ENSURE(false, "Unsupported type: " << itemType->GetKindAsStr());
    }
    return NUdf::TUnboxedValuePod();
}

NMiniKQL::TUnboxedValueVector ExtractUnboxedValues(const std::shared_ptr<arrow::Array>& array, const TType* itemType, const NMiniKQL::THolderFactory& holderFactory) {
    NMiniKQL::TUnboxedValueVector values;
    values.reserve(array->length());
    for (auto i = 0 ; i < array->length(); ++i) {
        values.push_back(ExtractUnboxedValue(array, i, itemType, holderFactory));
    }
    return values;
}

std::string SerializeArray(const std::shared_ptr<arrow::Array>& array) {
    auto schema = std::make_shared<arrow::Schema>(std::vector<std::shared_ptr<arrow::Field>>{arrow::field("", array->type())});
    auto batch = arrow::RecordBatch::Make(schema, array->length(), {array});
    auto writeOptions = arrow::ipc::IpcWriteOptions::Defaults(); // no compression set
    writeOptions.use_threads = false;
    // TODO Decide which compression level will be default. Will it depend on the length of array?
    auto codecResult = arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME);
    YQL_ENSURE(codecResult.ok());
    writeOptions.codec = std::move(codecResult.ValueOrDie());
    int64_t size;
    auto status = GetRecordBatchSize(*batch, writeOptions, &size);
    YQL_ENSURE(status.ok());

    std::string str;
    str.resize(size);

    auto writer = arrow::Buffer::GetWriter(arrow::MutableBuffer::Wrap(&str[0], size));
    YQL_ENSURE(writer.status().ok());

    status = SerializeRecordBatch(*batch, writeOptions, (*writer).get());
    YQL_ENSURE(status.ok());
    return str;
}

std::shared_ptr<arrow::Array> DeserializeArray(const std::string& blob, std::shared_ptr<arrow::DataType> type) {
    arrow::ipc::DictionaryMemo dictMemo;
    auto options = arrow::ipc::IpcReadOptions::Defaults();
    options.use_threads = false;

    std::shared_ptr<arrow::Buffer> buffer = arrow::Buffer::FromString(blob);
    arrow::io::BufferReader reader(buffer);
    auto schema = std::make_shared<arrow::Schema>(std::vector<std::shared_ptr<arrow::Field>>{arrow::field("", type)});
    auto batch = ReadRecordBatch(schema, &dictMemo, options, &reader);
    YQL_ENSURE(batch.ok() && (*batch)->ValidateFull().ok(), "Failed to deserialize batch");
    return (*batch)->column(0);
}

// Block splitter

namespace {

class TBlockSplitter : public IBlockSplitter {
    class TItem {
    public:
        TItem(TBlockSplitter& self, const std::vector<arrow::Datum>& data)
            : Self(self)
            , Data(data)
        {
            ArraysIdx.reserve(Self.Width);
            for (ui64 i = 0; i < Self.Width; ++i) {
                if (Data[i].is_scalar()) {
                    ScalarsSize += Self.GetDatumMemorySize(i, Data[i]);
                } else {
                    ArraysIdx.emplace_back(i);
                }
            }

            Length = Data.back().scalar_as<arrow::UInt64Scalar>().value;
            UpdateArraysSize();
        }

        ui64 GetLength() const {
            return Length;
        }

        ui64 GetSize() const {
            return ScalarsSize + ArraysSize;
        }

        std::vector<arrow::Datum> GetData() const {
            std::vector<arrow::Datum> result(Data);
            for (ui64 i : ArraysIdx) {
                result[i] = DeepSlice(result[i].array(), Offset, Length);
            }
            result.back() = arrow::Datum(std::make_shared<arrow::UInt64Scalar>(Length));
            return result;
        }

        TItem PopBack(ui64 numberRows) {
            YQL_ENSURE(numberRows <= Length, "Can not pop more than number of rows");
            Length -= numberRows;
            UpdateArraysSize();

            return TItem(*this, Offset + Length, numberRows);
        }

    private:
        TItem(const TItem& other, ui64 offset, ui64 length)
            : Self(other.Self)
            , Data(other.Data)
            , ArraysIdx(other.ArraysIdx)
            , Offset(offset)
            , Length(length)
            , ScalarsSize(other.ScalarsSize)
        {
            UpdateArraysSize();
        }

        void UpdateArraysSize() {
            ArraysSize = 0;
            for (ui64 i : ArraysIdx) {
                const auto& array = *Data[i].array();
                ArraysSize += Self.GetColumnReader(i).GetSliceDataWeight(array, array.offset + Offset, Length);
            }
        }

    private:
        TBlockSplitter& Self;
        const std::vector<arrow::Datum>& Data;
        std::vector<ui64> ArraysIdx;
        ui64 Offset = 0;
        ui64 Length = 0;
        ui64 ScalarsSize = 0;
        ui64 ArraysSize = 0;
    };

public:
    TBlockSplitter(const TVector<const TBlockType*>& items, ui64 chunkSizeLimit)
        : Items(items)
        , Width(items.size())
        , ChunkSizeLimit(chunkSizeLimit)
        , ScalarSizes(Width)
        , BlockReaders(Width)
    {}

    bool ShouldSplitItem(const NUdf::TUnboxedValuePod* values, ui32 count) override {
        const auto& datums = ExtractDatums(values, count);

        ui64 itemSize = 0;
        for (size_t i = 0; i < Width; ++i) {
            itemSize += GetDatumMemorySize(i, datums[i]);
        }
        return itemSize > ChunkSizeLimit;
    }

    std::vector<std::vector<arrow::Datum>> SplitItem(const NUdf::TUnboxedValuePod* values, ui32 count) override {
        const auto& datums = ExtractDatums(values, count);

        SplitStack.clear();
        SplitStack.emplace_back(*this, datums);
        std::vector<std::vector<arrow::Datum>> result;

        const auto estimatedSize = SplitStack.back().GetSize() / std::max(ChunkSizeLimit, ui64(1));
        result.reserve(estimatedSize);
        while (!SplitStack.empty()) {
            auto item = std::move(SplitStack.back());
            SplitStack.pop_back();

            while (item.GetSize() > ChunkSizeLimit) {
                if (item.GetLength() <= 1) {
                    throw yexception() << "Row size in block is " << item.GetSize() << ", that is larger than allowed limit " << ChunkSizeLimit;
                }
                SplitStack.emplace_back(item.PopBack(item.GetLength() / 2));
            }
            result.emplace_back(item.GetData());
        }
        return result;
    }

private:
    std::vector<arrow::Datum> ExtractDatums(const NUdf::TUnboxedValuePod* values, ui32 count) const {
        YQL_ENSURE(count == Width, "Invalid width");

        std::vector<arrow::Datum> result;
        result.reserve(Width);
        for (size_t i = 0; i < Width; ++i) {
            const auto& datum = result.emplace_back(TArrowBlock::From(values[i]).GetDatum());
            YQL_ENSURE(datum.is_array() || datum.is_scalar(), "Expecting array or scalar");
        }
        return result;
    }

    ui64 GetDatumMemorySize(ui64 index, const arrow::Datum& datum) {
        YQL_ENSURE(index < Width, "Invalid index");
        if (datum.is_array()) {
            return GetColumnReader(index).GetDataWeight(*datum.array());
        }

        if (!ScalarSizes[index]) {
            const auto& array = ARROW_RESULT(arrow::MakeArrayFromScalar(*datum.scalar(), 1));
            ScalarSizes[index] = NUdf::GetSizeOfArrayDataInBytes(*array->data());
        }
        return *ScalarSizes[index];
    }

    IBlockReader& GetColumnReader(ui64 index) {
        YQL_ENSURE(index < Width, "Invalid index");
        if (!BlockReaders[index]) {
            BlockReaders[index] = MakeBlockReader(TTypeInfoHelper(), Items[index]->GetItemType());
        }
        return *BlockReaders[index];
    }

private:
    const TVector<const TBlockType*> Items;
    const ui64 Width;
    const ui64 ChunkSizeLimit;

    std::vector<std::optional<ui64>> ScalarSizes;
    std::vector<std::unique_ptr<IBlockReader>> BlockReaders;
    std::vector<TItem> SplitStack;
};

} // namespace

IBlockSplitter::TPtr CreateBlockSplitter(const TType* type, ui64 chunkSizeLimit) {
    if (!type->IsMulti()) {
        // Not supported for legacy blocks
        return nullptr;
    }

    const TMultiType* multiType = static_cast<const TMultiType*>(type);
    const ui32 width = multiType->GetElementsCount();
    YQL_ENSURE(width > 0);

    TVector<const TBlockType*> items;
    items.reserve(width);
    for (ui32 i = 0; i < width - 1; i++) {
        const auto elementType = multiType->GetElementType(i);
        YQL_ENSURE(elementType->IsBlock());
        items.push_back(static_cast<const TBlockType*>(elementType));
    }

    const auto lengthType = multiType->GetElementType(width - 1);
    YQL_ENSURE(lengthType->IsBlock());

    const TBlockType* lengthBlockType = static_cast<const TBlockType*>(lengthType);
    YQL_ENSURE(lengthBlockType->GetShape() == TBlockType::EShape::Scalar);
    YQL_ENSURE(lengthBlockType->GetItemType()->IsData());
    YQL_ENSURE(static_cast<const TDataType*>(lengthBlockType->GetItemType())->GetDataSlot() == NUdf::EDataSlot::Uint64);
    items.push_back(lengthBlockType);

    return MakeIntrusive<TBlockSplitter>(items, chunkSizeLimit);
}

} // namespace NArrow
} // namespace NYql
