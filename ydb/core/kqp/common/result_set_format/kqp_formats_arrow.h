#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <yql/essentials/minikql/mkql_node.h>

/**
 * @file kqp_formats_arrow.h
 * @brief Utilities for converting MiniKQL types to Apache Arrow types and vice versa.
 *
 * This module provides a comprehensive mapping between YQL internal type system (MiniKQL)
 * and Apache Arrow format. It handles conversion of both simple data types
 * (integers, strings, etc.) and complex types (structs, lists, optionals, etc.).
 */

namespace NKikimr::NKqp::NFormats {

/**
 * @brief Dispatches MiniKQL data type to corresponding Arrow type via compile-time callback.
 *
 * This template function provides a type-safe way to map MiniKQL primitive data types
 * to their Arrow counterparts. The callback receives the Arrow type as a template parameter,
 * allowing for compile-time type dispatch without runtime overhead.
 *
 * Type mapping overview:
 * - Integer types: Int8/16/32/64, UInt8/16/32/64
 * - Floating point: Float, Double
 * - Temporal types: Date, Datetime, Timestamp, Interval (and their extended variants)
 * - String types: Utf8, Json, JsonDocument (serialized to string), DyNumber (serialized to string) -> arrow::StringType
 * - Binary types: String, Yson -> arrow::BinaryType
 * - Fixed-size binary: Decimal, Uuid -> arrow::FixedSizeBinaryType
 * - Timezone-aware: TzDate, TzDatetime, TzTimestamp -> arrow::StructType<datetimeType, arrow::StringType (serialized name of timezone)>
 *
 * @tparam TFunc Callable type accepting a single template parameter (Arrow type)
 * @param typeId The MiniKQL data slot to convert
 * @param callback A callable object with signature: template<typename TArrowType> bool operator()()
 * @return true if the type is supported and callback executed successfully, false otherwise
 */
template <typename TFunc>
bool SwitchMiniKQLDataTypeToArrowType(NUdf::EDataSlot typeId, TFunc&& callback) {
    switch (typeId) {
        case NUdf::EDataSlot::Int8:
            return callback.template operator()<arrow::Int8Type>();

        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Bool:
            return callback.template operator()<arrow::UInt8Type>();

        case NUdf::EDataSlot::Int16:
            return callback.template operator()<arrow::Int16Type>();

        case NUdf::EDataSlot::Date:
        case NUdf::EDataSlot::Uint16:
            return callback.template operator()<arrow::UInt16Type>();

        case NUdf::EDataSlot::Int32:
        case NUdf::EDataSlot::Date32:
            return callback.template operator()<arrow::Int32Type>();

        case NUdf::EDataSlot::Datetime:
        case NUdf::EDataSlot::Uint32:
            return callback.template operator()<arrow::UInt32Type>();

        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Datetime64:
        case NUdf::EDataSlot::Timestamp64:
        case NUdf::EDataSlot::Interval64:
            return callback.template operator()<arrow::Int64Type>();

        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return callback.template operator()<arrow::UInt64Type>();

        case NUdf::EDataSlot::Float:
            return callback.template operator()<arrow::FloatType>();

        case NUdf::EDataSlot::Double:
            return callback.template operator()<arrow::DoubleType>();

        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Json:
        case NUdf::EDataSlot::DyNumber:
        case NUdf::EDataSlot::JsonDocument:
            return callback.template operator()<arrow::StringType>();

        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Yson:
            return callback.template operator()<arrow::BinaryType>();

        case NUdf::EDataSlot::Decimal:
        case NUdf::EDataSlot::Uuid:
            return callback.template operator()<arrow::FixedSizeBinaryType>();

        case NUdf::EDataSlot::TzDate:
        case NUdf::EDataSlot::TzDatetime:
        case NUdf::EDataSlot::TzTimestamp:
        case NUdf::EDataSlot::TzDate32:
        case NUdf::EDataSlot::TzDatetime64:
        case NUdf::EDataSlot::TzTimestamp64:
            return callback.template operator()<arrow::StructType>();
    }
    return false;
}

/**
 * @brief Determines if a type requires wrapping in an external Optional layer.
 *
 * Some MiniKQL types don't have a native validity bitmap in Arrow representation
 * (e.g., Variant, Null, Void). These types need to be wrapped in an additional
 * struct layer when used as optional values to properly represent NULL states.
 *
 * @param type The MiniKQL type to check
 * @return true if the type needs external Optional wrapping, false otherwise
 *
 * @note Types that need wrapping: Void, Null, Variant, Optional, EmptyList, EmptyDict
 */
bool NeedWrapByExternalOptional(const NMiniKQL::TType* type);

/**
 * @brief Converts a MiniKQL type to its corresponding Arrow DataType.
 *
 * This function recursively converts complex MiniKQL types (Struct, Tuple, List, Dict,
 * Variant, Optional) to their Arrow equivalents. The conversion preserves the structure
 * and nullability information.
 *
 * Conversion rules:
 * - Data types: mapped according to SwitchMiniKQLDataTypeToArrowType
 * - Struct/Tuple: converted to arrow::StructType
 * - List: converted to arrow::ListType
 * - Dict: converted to arrow::ListType of arrow::StructType<Key, Value>
 * - Variant: converted to arrow::DenseUnionType
 * - Optional: nested optionals are flattened and represented via struct wrapping
 *
 * @param type The MiniKQL type to convert
 * @return Shared pointer to corresponding Arrow DataType, or arrow::NullType if unsupported
 */
std::shared_ptr<arrow::DataType> GetArrowType(const NMiniKQL::TType* type);

/**
 * @brief Checks if a MiniKQL type can be represented in Arrow format.
 *
 * Not all MiniKQL types are compatible with Arrow. For example, Callable, Stream,
 * and Flow types cannot be represented. This function recursively checks complex
 * types (Struct, List, etc.) to ensure all nested types are compatible.
 *
 * @param type The MiniKQL type to validate
 * @return true if the type can be converted to Arrow format, false otherwise
 *
 * @note Compatible types: Data, Struct, Tuple, List, Dict, Variant, Optional, Tagged
 * @note Incompatible types: Type, Stream, Callable, Any, Resource, Flow, Block, Pg, Multi, Linear
 */
bool IsArrowCompatible(const NMiniKQL::TType* type);

/**
 * @brief Appends a MiniKQL UnboxedValue to an Arrow ArrayBuilder.
 *
 * This function is the core serialization routine for converting MiniKQL values
 * to Arrow format. It handles all supported MiniKQL types, including
 * complex nested structures, and properly manages NULL values.
 *
 * The builder must be pre-configured with the correct Arrow type matching the
 * provided MiniKQL type. Type mismatches will result in assertion failures.
 *
 * @param value The MiniKQL value to append (may be NULL/empty)
 * @param builder The Arrow builder to append to (must match the type)
 * @param type The MiniKQL type descriptor for the value
 */
void AppendElement(NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const NMiniKQL::TType* type);

} // namespace NKikimr::NKqp::NFormats
