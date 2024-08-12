#include "dq_arrow_helpers.h"

#include <cstddef>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

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
        case NUdf::EDataSlot::Bool:
            return callback(TTypeWrapper<arrow::BooleanType>());
        case NUdf::EDataSlot::Int8:
            return callback(TTypeWrapper<arrow::Int8Type>());
        case NUdf::EDataSlot::Uint8:
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
        case NUdf::EDataSlot::Datetime64:
        case NUdf::EDataSlot::Timestamp64:
        case NUdf::EDataSlot::Interval64:
            return callback(TTypeWrapper<arrow::Int64Type>());
        case NUdf::EDataSlot::Uint64:
            return callback(TTypeWrapper<arrow::UInt64Type>());
        case NUdf::EDataSlot::Float:
            return callback(TTypeWrapper<arrow::FloatType>());
        case NUdf::EDataSlot::Double:
            return callback(TTypeWrapper<arrow::DoubleType>());
        case NUdf::EDataSlot::Timestamp:
            return callback(TTypeWrapper<arrow::TimestampType>());
        case NUdf::EDataSlot::Interval:
            return callback(TTypeWrapper<arrow::DurationType>());
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Json:
        case NUdf::EDataSlot::Yson:
        case NUdf::EDataSlot::JsonDocument:
            return callback(TTypeWrapper<arrow::StringType>());
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Uuid:
        case NUdf::EDataSlot::DyNumber:
            return callback(TTypeWrapper<arrow::BinaryType>());
        case NUdf::EDataSlot::Decimal:
            return callback(TTypeWrapper<arrow::Decimal128Type>());
        // TODO convert Tz-types to native arrow date and time types.
        case NUdf::EDataSlot::TzDate:
        case NUdf::EDataSlot::TzDatetime:
        case NUdf::EDataSlot::TzTimestamp:
        case NUdf::EDataSlot::TzDate32:
        case NUdf::EDataSlot::TzDatetime64:
        case NUdf::EDataSlot::TzTimestamp64:
            return false;
    }
}

template <typename TArrowType>
NUdf::TUnboxedValue GetUnboxedValue(std::shared_ptr<arrow::Array> column, ui32 row) {
    using TArrayType = typename arrow::TypeTraits<TArrowType>::ArrayType;
    auto array = std::static_pointer_cast<TArrayType>(column);
    return NUdf::TUnboxedValuePod(static_cast<typename TArrowType::c_type>(array->Value(row)));
}

// The following 4 specialization are for darwin build (because of difference in long long)

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

template <> // For darwin build
NUdf::TUnboxedValue GetUnboxedValue<arrow::TimestampType>(std::shared_ptr<arrow::Array> column, ui32 row) {
    using TArrayType = typename arrow::TypeTraits<arrow::TimestampType>::ArrayType;
    auto array = std::static_pointer_cast<TArrayType>(column);
    return NUdf::TUnboxedValuePod(static_cast<ui64>(array->Value(row)));
}

template <> // For darwin build
NUdf::TUnboxedValue GetUnboxedValue<arrow::DurationType>(std::shared_ptr<arrow::Array> column, ui32 row) {
    using TArrayType = typename arrow::TypeTraits<arrow::DurationType>::ArrayType;
    auto array = std::static_pointer_cast<TArrayType>(column);
    return NUdf::TUnboxedValuePod(static_cast<ui64>(array->Value(row)));
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
NUdf::TUnboxedValue GetUnboxedValue<arrow::Decimal128Type>(std::shared_ptr<arrow::Array> column, ui32 row) {
    auto array = std::static_pointer_cast<arrow::Decimal128Array>(column);
    auto data = array->GetView(row);
    // We check that Decimal(22,9) but it may not be true
    // TODO Support other decimal precisions.
    const auto& type = arrow::internal::checked_cast<const arrow::Decimal128Type&>(*array->type());
    Y_ABORT_UNLESS(type.precision() == NScheme::DECIMAL_PRECISION, "Unsupported Decimal precision.");
    Y_ABORT_UNLESS(type.scale() == NScheme::DECIMAL_SCALE, "Unsupported Decimal scale.");
    Y_ABORT_UNLESS(data.size() == sizeof(NYql::NDecimal::TInt128), "Wrong data size");
    NYql::NDecimal::TInt128 val;
    std::memcpy(reinterpret_cast<char*>(&val), data.data(), data.size());
    return NUdf::TUnboxedValuePod(val);
}

template <typename TType>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl() {
    return std::make_shared<TType>();
}

template <>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl<arrow::Decimal128Type>() {
    // TODO use non-fixed precision, derive it from data.
    return arrow::decimal(NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE);
}

template <>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl<arrow::TimestampType>() {
    return arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO);
}

template <>
std::shared_ptr<arrow::DataType> CreateEmptyArrowImpl<arrow::DurationType>() {
    return arrow::duration(arrow::TimeUnit::TimeUnit::MICRO);
}

std::shared_ptr<arrow::DataType> GetArrowType(const TDataType* dataType) {
    std::shared_ptr<arrow::DataType> result;
    bool success = SwitchMiniKQLDataTypeToArrowType(*dataType->GetDataSlot().Get(), [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
        Y_UNUSED(typeHolder);
        result = CreateEmptyArrowImpl<TType>();
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
        fields.emplace_back(std::make_shared<arrow::Field>(std::string(structType->GetMemberName(index)),
            NArrow::GetArrowType(memberType)));
    }
    return arrow::struct_(fields);
}

std::shared_ptr<arrow::DataType> GetArrowType(const TTupleType* tupleType) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(tupleType->GetElementsCount());
    for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
        auto elementType = tupleType->GetElementType(index);
        fields.push_back(std::make_shared<arrow::Field>("", NArrow::GetArrowType(elementType)));
    }
    return arrow::struct_(fields);
}

std::shared_ptr<arrow::DataType> GetArrowType(const TListType* listType) {
    auto itemType = listType->GetItemType();
    return arrow::list(NArrow::GetArrowType(itemType));
}

std::shared_ptr<arrow::DataType> GetArrowType(const TDictType* dictType) {
    auto keyType = dictType->GetKeyType();
    auto payloadType = dictType->GetPayloadType();
    if (keyType->GetKind() == TType::EKind::Optional) {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.emplace_back(std::make_shared<arrow::Field>("", NArrow::GetArrowType(keyType)));
        fields.emplace_back(std::make_shared<arrow::Field>("", NArrow::GetArrowType(payloadType)));
        return arrow::list(arrow::struct_(fields));
    }
    return arrow::map(NArrow::GetArrowType(keyType), NArrow::GetArrowType(payloadType));
}

std::shared_ptr<arrow::DataType> GetArrowType(const TVariantType* variantType) {
    TType* innerType = variantType->GetUnderlyingType();
    arrow::FieldVector types;
    TStructType* structType = nullptr;
    TTupleType* tupleType = nullptr;
    if (innerType->IsStruct()) {
        structType = static_cast<TStructType*>(innerType);
    } else {
        Y_VERIFY_S(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
        tupleType = static_cast<TTupleType*>(innerType);
    }

    if (variantType->GetAlternativesCount() > arrow::UnionType::kMaxTypeCode) {
        // Create Union of unions if there are more types then arrow::dense_union supports.
        ui32 numberOfGroups = (variantType->GetAlternativesCount() - 1) / arrow::UnionType::kMaxTypeCode + 1;
        types.reserve(numberOfGroups);
        for (ui32 groupIndex = 0; groupIndex < numberOfGroups; ++groupIndex) {
            ui32 beginIndex = groupIndex * arrow::UnionType::kMaxTypeCode;
            ui32 endIndex = std::min((groupIndex + 1) * arrow::UnionType::kMaxTypeCode, variantType->GetAlternativesCount());
            arrow::FieldVector groupTypes;
            groupTypes.reserve(endIndex - beginIndex);
            if (structType == nullptr) {
                for (ui32 index = beginIndex; index < endIndex; ++ index) {
                    groupTypes.emplace_back(std::make_shared<arrow::Field>("",
                        NArrow::GetArrowType(tupleType->GetElementType(index))));
                }
            } else {
                for (ui32 index = beginIndex; index < endIndex; ++ index) {
                    groupTypes.emplace_back(std::make_shared<arrow::Field>(std::string(structType->GetMemberName(index)),
                        NArrow::GetArrowType(structType->GetMemberType(index))));
                }
            }
            types.emplace_back(std::make_shared<arrow::Field>("", arrow::dense_union(groupTypes)));
        }
    } else {
        // Simply put all types in one arrow::dense_union
        types.reserve(variantType->GetAlternativesCount());
        if (structType == nullptr) {
            for (ui32 index = 0; index < variantType->GetAlternativesCount(); ++index) {
                types.push_back(std::make_shared<arrow::Field>("", NArrow::GetArrowType(tupleType->GetElementType(index))));
            }
        } else {
            for (ui32 index = 0; index < variantType->GetAlternativesCount(); ++index) {
                types.emplace_back(std::make_shared<arrow::Field>(std::string(structType->GetMemberName(index)),
                   NArrow::GetArrowType(structType->GetMemberType(index))));
            }
        }
    }
    return arrow::dense_union(types);
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
    Y_VERIFY_S(status.ok(), status.ToString());
}

template <>
void AppendDataValue<arrow::UInt64Type>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
    Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::UINT64);
    auto typedBuilder = reinterpret_cast<arrow::UInt64Builder*>(builder);
    arrow::Status status;
    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        status = typedBuilder->Append(value.Get<ui64>());
    }
    Y_VERIFY_S(status.ok(), status.ToString());
}

template <>
void AppendDataValue<arrow::Int64Type>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
    Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::INT64);
    auto typedBuilder = reinterpret_cast<arrow::Int64Builder*>(builder);
    arrow::Status status;
    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        status = typedBuilder->Append(value.Get<i64>());
    }
    Y_VERIFY_S(status.ok(), status.ToString());
}

template <>
void AppendDataValue<arrow::TimestampType>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
    Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::TIMESTAMP);
    auto typedBuilder = reinterpret_cast<arrow::TimestampBuilder*>(builder);
    arrow::Status status;
    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        status = typedBuilder->Append(value.Get<ui64>());
    }
    Y_VERIFY_S(status.ok(), status.ToString());
}

template <>
void AppendDataValue<arrow::DurationType>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
    Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::DURATION);
    auto typedBuilder = reinterpret_cast<arrow::DurationBuilder*>(builder);
    arrow::Status status;
    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        status = typedBuilder->Append(value.Get<ui64>());
    }
    Y_VERIFY_S(status.ok(), status.ToString());
}

template <>
void AppendDataValue<arrow::StringType>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
    Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::STRING);
    auto typedBuilder = reinterpret_cast<arrow::StringBuilder*>(builder);
    arrow::Status status;
    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        auto data = value.AsStringRef();
        status = typedBuilder->Append(data.Data(), data.Size());
    }
    Y_VERIFY_S(status.ok(), status.ToString());
}

template <>
void AppendDataValue<arrow::BinaryType>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
    Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::BINARY);
    auto typedBuilder = reinterpret_cast<arrow::BinaryBuilder*>(builder);
    arrow::Status status;
    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        auto data = value.AsStringRef();
        status = typedBuilder->Append(data.Data(), data.Size());
    }
    Y_VERIFY_S(status.ok(), status.ToString());
}

template <>
void AppendDataValue<arrow::Decimal128Type>(arrow::ArrayBuilder* builder, NUdf::TUnboxedValue value) {
    Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::DECIMAL128);
    auto typedBuilder = reinterpret_cast<arrow::Decimal128Builder*>(builder);
    arrow::Status status;
    if (!value.HasValue()) {
        status = typedBuilder->AppendNull();
    } else {
        // Parse value from string
        status = typedBuilder->Append(value.AsStringRef().Data());
    }
    Y_VERIFY_S(status.ok(), status.ToString());
}

} // namespace

std::shared_ptr<arrow::DataType> GetArrowType(const TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::Null:
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict:
            break;
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
            auto innerOptionalType = optionalType->GetItemType();
            if (innerOptionalType->GetKind() == TType::EKind::Optional) {
                std::vector<std::shared_ptr<arrow::Field>> fields;
                fields.emplace_back(std::make_shared<arrow::Field>("", std::make_shared<arrow::UInt64Type>()));
                while (innerOptionalType->GetKind() == TType::EKind::Optional) {
                    innerOptionalType = static_cast<const TOptionalType*>(innerOptionalType)->GetItemType();
                }
                fields.emplace_back(std::make_shared<arrow::Field>("", GetArrowType(innerOptionalType)));
                return arrow::struct_(fields);
            }
            return GetArrowType(innerOptionalType);
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
        THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
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
            if (innerOptionalType->GetKind() == TType::EKind::Optional) {
                return false;
            }
            return IsArrowCompatible(innerOptionalType);
        }
        case TType::EKind::List: {
            auto listType = static_cast<const TListType*>(type);
            auto itemType = listType->GetItemType();
            return IsArrowCompatible(itemType);
        }
        case TType::EKind::Dict: {
            auto dictType = static_cast<const TDictType*>(type);
            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();
            if (keyType->GetKind() == TType::EKind::Optional) {
                return false;
            }
            return IsArrowCompatible(keyType) && IsArrowCompatible(payloadType);
        }
        case TType::EKind::Variant: {
            auto variantType = static_cast<const TVariantType*>(type);
            if (variantType->GetAlternativesCount() > arrow::UnionType::kMaxTypeCode) {
                return false;
            }
            TType* innerType = variantType->GetUnderlyingType();
            Y_VERIFY_S(innerType->IsTuple() || innerType->IsStruct(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
            return IsArrowCompatible(innerType);
        }
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
            return false;
    }
    return false;
}

std::unique_ptr<arrow::ArrayBuilder> MakeArrowBuilder(const TType* type) {
    auto arrayType = GetArrowType(type);
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto status = arrow::MakeBuilder(arrow::default_memory_pool(), arrayType, &builder);
    Y_VERIFY_S(status.ok(), status.ToString());
    return builder;
}

void AppendElement(NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::Null:
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict: {
            auto status = builder->AppendNull();
            Y_VERIFY_S(status.ok(), status.ToString());
            break;
        }

        case TType::EKind::Data: {
            // TODO for TzDate, TzDatetime, TzTimestamp pass timezone to arrow builder?
            auto dataType = static_cast<const TDataType*>(type);
            bool success = SwitchMiniKQLDataTypeToArrowType(*dataType->GetDataSlot().Get(), [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
                Y_UNUSED(typeHolder);
                AppendDataValue<TType>(builder, value);
                return true;
            });
            Y_ABORT_UNLESS(success);
            break;
        }

        case TType::EKind::Optional: {
            auto optionalType = static_cast<const TOptionalType*>(type);
            if (optionalType->GetItemType()->GetKind() != TType::EKind::Optional) {
                if (value.HasValue()) {
                    AppendElement(value.GetOptionalValue(), builder, optionalType->GetItemType());
                } else {
                    auto status = builder->AppendNull();
                    Y_VERIFY_S(status.ok(), status.ToString());
                }
            } else {
                Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::STRUCT);
                auto structBuilder = reinterpret_cast<arrow::StructBuilder*>(builder);
                Y_DEBUG_ABORT_UNLESS(structBuilder->num_fields() == 2);
                Y_DEBUG_ABORT_UNLESS(structBuilder->field_builder(0)->type()->id() == arrow::Type::UINT64);
                auto status = structBuilder->Append();
                Y_VERIFY_S(status.ok(), status.ToString());
                auto depthBuilder = reinterpret_cast<arrow::UInt64Builder*>(structBuilder->field_builder(0));
                auto valueBuilder = structBuilder->field_builder(1);
                ui64 depth = 0;
                TType* innerType = optionalType->GetItemType();
                while (innerType->GetKind() == TType::EKind::Optional && value.HasValue()) {
                    innerType = static_cast<const TOptionalType*>(innerType)->GetItemType();
                    value = value.GetOptionalValue();
                    ++depth;
                }
                status = depthBuilder->Append(depth);
                Y_VERIFY_S(status.ok(), status.ToString());
                if (value.HasValue()) {
                    AppendElement(value, valueBuilder, innerType);
                } else {
                    status = valueBuilder->AppendNull();
                    Y_VERIFY_S(status.ok(), status.ToString());
                }
            }
            break;
        }

        case TType::EKind::List: {
            auto listType = static_cast<const TListType*>(type);
            auto itemType = listType->GetItemType();
            Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::LIST);
            auto listBuilder = reinterpret_cast<arrow::ListBuilder*>(builder);
            auto status = listBuilder->Append();
            Y_VERIFY_S(status.ok(), status.ToString());
            auto innerBuilder = listBuilder->value_builder();
            if (auto p = value.GetElements()) {
                auto len = value.GetListLength();
                while (len > 0) {
                    AppendElement(*p++, innerBuilder, itemType);
                    --len;
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
            Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::STRUCT);
            auto structBuilder = reinterpret_cast<arrow::StructBuilder*>(builder);
            auto status = structBuilder->Append();
            Y_VERIFY_S(status.ok(), status.ToString());
            Y_DEBUG_ABORT_UNLESS(static_cast<ui32>(structBuilder->num_fields()) == structType->GetMembersCount());
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto innerBuilder = structBuilder->field_builder(index);
                auto memberType = structType->GetMemberType(index);
                AppendElement(value.GetElement(index), innerBuilder, memberType);
            }
            break;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<const TTupleType*>(type);
            Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::STRUCT);
            auto structBuilder = reinterpret_cast<arrow::StructBuilder*>(builder);
            auto status = structBuilder->Append();
            Y_VERIFY_S(status.ok(), status.ToString());
            Y_DEBUG_ABORT_UNLESS(static_cast<ui32>(structBuilder->num_fields()) == tupleType->GetElementsCount());
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

            arrow::ArrayBuilder* keyBuilder;
            arrow::ArrayBuilder* itemBuilder;
            arrow::StructBuilder* structBuilder = nullptr;
            if (keyType->GetKind() == TType::EKind::Optional) {
                Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::LIST);
                auto listBuilder = reinterpret_cast<arrow::ListBuilder*>(builder);
                Y_DEBUG_ABORT_UNLESS(listBuilder->value_builder()->type()->id() == arrow::Type::STRUCT);
                // Start a new list in ListArray of structs
                auto status = listBuilder->Append();
                Y_VERIFY_S(status.ok(), status.ToString());
                structBuilder = reinterpret_cast<arrow::StructBuilder*>(listBuilder->value_builder());
                Y_DEBUG_ABORT_UNLESS(structBuilder->num_fields() == 2);
                keyBuilder = structBuilder->field_builder(0);
                itemBuilder = structBuilder->field_builder(1);
            } else {
                Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::MAP);
                auto mapBuilder = reinterpret_cast<arrow::MapBuilder*>(builder);
                // Start a new map in MapArray
                auto status = mapBuilder->Append();
                Y_VERIFY_S(status.ok(), status.ToString());
                keyBuilder = mapBuilder->key_builder();
                itemBuilder = mapBuilder->item_builder();
            }

            const auto iter = value.GetDictIterator();
            // We do not sort dictionary before appending it to builder.
            for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                if (structBuilder != nullptr) {
                    auto status = structBuilder->Append();
                    Y_VERIFY_S(status.ok(), status.ToString());
                }
                AppendElement(key, keyBuilder, keyType);
                AppendElement(payload, itemBuilder, payloadType);
            }
            break;
        }

        case TType::EKind::Variant: {
            // TODO Need to properly convert variants containing more than 127*127 types?
            auto variantType = static_cast<const TVariantType*>(type);
            Y_DEBUG_ABORT_UNLESS(builder->type()->id() == arrow::Type::DENSE_UNION);
            auto unionBuilder = reinterpret_cast<arrow::DenseUnionBuilder*>(builder);
            ui32 variantIndex = value.GetVariantIndex();
            TType* innerType = variantType->GetUnderlyingType();
            if (innerType->IsStruct()) {
                innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
            } else {
                Y_VERIFY_S(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
                innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
            }
            if (variantType->GetAlternativesCount() > arrow::UnionType::kMaxTypeCode) {
                ui32 numberOfGroups = (variantType->GetAlternativesCount() - 1) / arrow::UnionType::kMaxTypeCode + 1;
                Y_DEBUG_ABORT_UNLESS(static_cast<ui32>(unionBuilder->num_children()) == numberOfGroups);
                ui32 groupIndex = variantIndex / arrow::UnionType::kMaxTypeCode;
                auto status = unionBuilder->Append(groupIndex);
                Y_VERIFY_S(status.ok(), status.ToString());
                auto innerBuilder = unionBuilder->child_builder(groupIndex);
                Y_DEBUG_ABORT_UNLESS(innerBuilder->type()->id() == arrow::Type::DENSE_UNION);
                auto innerUnionBuilder = reinterpret_cast<arrow::DenseUnionBuilder*>(innerBuilder.get());
                ui32 innerVariantIndex = variantIndex % arrow::UnionType::kMaxTypeCode;
                status = innerUnionBuilder->Append(innerVariantIndex);
                Y_VERIFY_S(status.ok(), status.ToString());
                auto doubleInnerBuilder = innerUnionBuilder->child_builder(innerVariantIndex);
                AppendElement(value.GetVariantItem(), doubleInnerBuilder.get(), innerType);
            } else {
                auto status = unionBuilder->Append(variantIndex);
                Y_VERIFY_S(status.ok(), status.ToString());
                auto innerBuilder = unionBuilder->child_builder(variantIndex);
                AppendElement(value.GetVariantItem(), innerBuilder.get(), innerType);
            }
            break;
        }

    default:
        THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
    }
}

std::shared_ptr<arrow::Array> MakeArray(NMiniKQL::TUnboxedValueVector& values, const TType* itemType) {
    auto builder = MakeArrowBuilder(itemType);
    auto status = builder->Reserve(values.size());
    Y_VERIFY_S(status.ok(), status.ToString());
    for (auto& value: values) {
        AppendElement(value, builder.get(), itemType);
    }
    std::shared_ptr<arrow::Array> result;
    status = builder->Finish(&result);
    Y_VERIFY_S(status.ok(), status.ToString());
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
        case TType::EKind::Data: { // TODO TzDate need special care
            auto dataType = static_cast<const TDataType*>(itemType);
            NUdf::TUnboxedValue result;
            bool success = SwitchMiniKQLDataTypeToArrowType(*dataType->GetDataSlot().Get(), [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
                Y_UNUSED(typeHolder);
                result = GetUnboxedValue<TType>(array, row);
                return true;
            });
            Y_DEBUG_ABORT_UNLESS(success);
            return result;
        }
        case TType::EKind::Struct: {
            auto structType = static_cast<const TStructType*>(itemType);
            Y_DEBUG_ABORT_UNLESS(array->type_id() == arrow::Type::STRUCT);
            auto typedArray = static_pointer_cast<arrow::StructArray>(array);
            Y_DEBUG_ABORT_UNLESS(static_cast<ui32>(typedArray->num_fields()) == structType->GetMembersCount());
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
            Y_DEBUG_ABORT_UNLESS(array->type_id() == arrow::Type::STRUCT);
            auto typedArray = static_pointer_cast<arrow::StructArray>(array);
            Y_DEBUG_ABORT_UNLESS(static_cast<ui32>(typedArray->num_fields()) == tupleType->GetElementsCount());
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
            if (innerOptionalType->GetKind() == TType::EKind::Optional) {
                Y_DEBUG_ABORT_UNLESS(array->type_id() == arrow::Type::STRUCT);
                auto structArray = static_pointer_cast<arrow::StructArray>(array);
                Y_DEBUG_ABORT_UNLESS(structArray->num_fields() == 2);
                Y_DEBUG_ABORT_UNLESS(structArray->field(0)->type_id() == arrow::Type::UINT64);
                auto depthArray = static_pointer_cast<arrow::UInt64Array>(structArray->field(0));
                auto valuesArray = structArray->field(1);
                auto depth = depthArray->Value(row);
                NUdf::TUnboxedValue value;
                if (valuesArray->IsNull(row)) {
                    value = NUdf::TUnboxedValuePod();
                } else {
                    while (innerOptionalType->GetKind() == TType::EKind::Optional) {
                        innerOptionalType = static_cast<const TOptionalType*>(innerOptionalType)->GetItemType();
                    }
                    value = ExtractUnboxedValue(valuesArray, row, innerOptionalType, holderFactory);
                }
                for (ui64 i = 0; i < depth; ++i) {
                    value = value.MakeOptional();
                }
                return value;
            } else {
                return ExtractUnboxedValue(array, row, innerOptionalType, holderFactory).Release().MakeOptional();
            }
        }
        case TType::EKind::List: {
            auto listType = static_cast<const TListType*>(itemType);
            Y_DEBUG_ABORT_UNLESS(array->type_id() == arrow::Type::LIST);
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
            if (keyType->GetKind() == TType::EKind::Optional) {
                Y_DEBUG_ABORT_UNLESS(array->type_id() == arrow::Type::LIST);
                auto listArray = static_pointer_cast<arrow::ListArray>(array);
                auto arraySlice = listArray->value_slice(row);
                Y_DEBUG_ABORT_UNLESS(arraySlice->type_id() == arrow::Type::STRUCT);
                auto structArray = static_pointer_cast<arrow::StructArray>(arraySlice);
                Y_DEBUG_ABORT_UNLESS(structArray->num_fields() == 2);
                dictLength = arraySlice->length();
                keyArray = structArray->field(0);
                payloadArray = structArray->field(1);
            } else {
                Y_DEBUG_ABORT_UNLESS(array->type_id() == arrow::Type::MAP);
                auto mapArray = static_pointer_cast<arrow::MapArray>(array);
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
            Y_DEBUG_ABORT_UNLESS(array->type_id() == arrow::Type::DENSE_UNION);
            auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(array);
            auto variantIndex = unionArray->child_id(row);
            auto rowInChild = unionArray->value_offset(row);
            std::shared_ptr<arrow::Array> valuesArray = unionArray->field(variantIndex);
            if (variantType->GetAlternativesCount() > arrow::UnionType::kMaxTypeCode) {
                // Go one step deeper
                Y_DEBUG_ABORT_UNLESS(valuesArray->type_id() == arrow::Type::DENSE_UNION);
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
                Y_VERIFY_S(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
                innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
            }
            NUdf::TUnboxedValue value = ExtractUnboxedValue(valuesArray, rowInChild, innerType, holderFactory);
            return holderFactory.CreateVariantHolder(value.Release(), variantIndex);
        }
    default:
        THROW yexception() << "Unsupported type: " << itemType->GetKindAsStr();
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
    Y_ABORT_UNLESS(codecResult.ok());
    writeOptions.codec = std::move(codecResult.ValueOrDie());
    int64_t size;
    auto status = GetRecordBatchSize(*batch, writeOptions, &size);
    Y_ABORT_UNLESS(status.ok());

    std::string str;
    str.resize(size);

    auto writer = arrow::Buffer::GetWriter(arrow::MutableBuffer::Wrap(&str[0], size));
    Y_ABORT_UNLESS(writer.status().ok());

    status = SerializeRecordBatch(*batch, writeOptions, (*writer).get());
    Y_ABORT_UNLESS(status.ok());
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
    Y_DEBUG_ABORT_UNLESS(batch.ok() && (*batch)->ValidateFull().ok(), "Failed to deserialize batch");
    return (*batch)->column(0);
}

} // namespace NArrow
} // namespace NYql

