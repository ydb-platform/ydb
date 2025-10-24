#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr::NKqp::NFormats {

namespace {

template <typename TArrowType>
struct TTypeWrapper {
    using T = TArrowType;
};

} // namespace

/**
 * @brief Function to switch MiniKQL DataType correctly and uniformly converting
 * it to arrow type using callback
 *
 * @tparam TFunc Callback type
 * @param typeId Type callback work with.
 * @param callback Template function of signature (TTypeWrapper) -> bool
 * @return Result of execution of callback or false if the type typeId is not
 * supported.
 */
template <typename TFunc>
bool SwitchMiniKQLDataTypeToArrowType(NUdf::EDataSlot typeId, TFunc&& callback) {
    switch (typeId) {
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
        case NUdf::EDataSlot::DyNumber:
        case NUdf::EDataSlot::JsonDocument:
            return callback(TTypeWrapper<arrow::StringType>());
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Yson:
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

/**
 * @brief Check if the type needs to be wrapped by external optional.
 * For example, some types does not have validity bitmap.
 *
 * @param type Yql type to check
 * @return true if the type needs to be wrapped by external optional, false otherwise
 */
bool NeedWrapByExternalOptional(const NMiniKQL::TType* type);

/**
 * @brief Convert TType to the arrow::DataType object
 *
 * @param type Yql type to parse
 * @return std::shared_ptr<arrow::DataType> arrow type of the same structure as
 * type
 */
std::shared_ptr<arrow::DataType> GetArrowType(const NMiniKQL::TType* type);

/**
 * @brief Check if the type can be converted to arrow type.
 *
 * @param type Yql type to check
 * @return true if the type is compatible with arrow, false otherwise
 */
bool IsArrowCompatible(const NMiniKQL::TType* type);

/**
 * @brief Append UnboxedValue to arrow Array via arrow Builder.
 * This function is used in TArrowBatchBuilder.
 *
 * @param value value to append
 * @param builder arrow Builder with proper type used to append converted value array
 * @param type Yql type of the element
 */
void AppendElement(NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const NMiniKQL::TType* type);

} // namespace NKikimr::NKqp::NFormats
