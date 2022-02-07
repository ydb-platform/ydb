#pragma once

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>

namespace NYql {
namespace NArrow {

/**
 * @brief Convert TType to the arrow::DataType object
 *
 * The logic of this conversion is the following:
 *
 * Struct, tuple => StructArray
 * Names of fields constructed from tuple are just empty strings.
 *
 * List => ListArray
 *
 * Variant => DenseUnionArray
 * If variant contains more than 127 items then we map
 * Variant => DenseUnionArray<DenseUnionArray>
 * TODO Implement convertion of data to DenseUnionArray<DenseUnionArray> and back
 *
 * Optional(Optional ..(type)..) => StructArray<ui64, type>
 * Here the integer value equals the number of calls of method GetOptionalValue().
 * If value is null at some depth, then the value in second field of Array is Null
 * (and the integer equals this depth). If value is present, then it is contained in the
 * second field (and the integer equals the number of Optional(...) levels).
 * This information is sufficient to restore an UnboxedValue knowing its type.
 *
 * Dict<KeyType, ValueType> => MapArray<KeyArray, ValueArray>
 * We do not use arrow::DictArray because it must be used for encoding not for mapping keys to values.
 * (https://arrow.apache.org/docs/cpp/api/array.html#classarrow_1_1_dictionary_array)
 * If the type of dict key is optional then we map
 * Dict<Optional(KeyType), ValueType> => ListArray<StructArray<KeyArray, ValueArray>>
 * because keys of MapArray can not be nullable
 *
 * @param type Yql type to parse
 * @return std::shared_ptr<arrow::DataType> arrow type of the same structure as type
 */
std::shared_ptr<arrow::DataType> GetArrowType(const NKikimr::NMiniKQL::TType* type);

/**
 * @brief Check if type can be converted to arrow format using only native arrow classes.
 *
 * @param type Type of UnboxedValue to check.
 * @return true if type does not contain neither nested Optional, nor Dicts with Optional keys, nor Variants
 * between more than 255 types.
 * @return false otherwise
 */
bool IsArrowCompatible(const NKikimr::NMiniKQL::TType* type);

std::unique_ptr<arrow::ArrayBuilder> MakeArrowBuilder(const NKikimr::NMiniKQL::TType* type);

/**
 * @brief Convert UnboxedValue-s to arrow Array
 *
 * @param values elements of future array
 * @param itemType type of each element to parse it and to construct corresponding arrow type
 * @return std::shared_ptr<arrow::Array> data in arrow format
 */
std::shared_ptr<arrow::Array> MakeArray(NKikimr::NMiniKQL::TUnboxedValueVector& values,
    const NKikimr::NMiniKQL::TType* itemType);

NKikimr::NMiniKQL::TUnboxedValueVector ExtractUnboxedValues(const std::shared_ptr<arrow::Array>& array,
    const NKikimr::NMiniKQL::TType* itemType, const NKikimr::NMiniKQL::THolderFactory& holderFactory);

std::string SerializeArray(const std::shared_ptr<arrow::Array>& array);

std::shared_ptr<arrow::Array> DeserializeArray(const std::string& blob, std::shared_ptr<arrow::DataType> type);

/**
 * @brief Append UnboxedValue to arrow Array via arrow Builder
 *
 * @param value unboxed value to append
 * @param builder arrow Builder with proper type used to append converted value array
 * @param type type of element to parse it and to construct corresponding arrow type
 * @return std::shared_ptr<arrow::Array> data in arrow format
 */
void AppendElement(NYql::NUdf::TUnboxedValue value, arrow::ArrayBuilder* builder, const NKikimr::NMiniKQL::TType* type);


} // NArrow
} // NYql
