#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>

namespace NYql {
namespace NArrow {

/**
 * @brief Convert TType to the arrow::DataType object
 *
 * The logic of this conversion is from YQL-15332:
 *
 * Void, Null => NullType
 * Bool => Uint8
 * Integral => Uint8..Uint64, Int8..Int64
 * Floats => Float, Double
 * Date => Uint16
 * Datetime => Uint32
 * Timestamp => Uint64
 * Interval => Int64
 * Date32 => Int32
 * Interval64, Timestamp64, Datetime64 => Int64
 * Utf8, Json => String
 * String, Yson, JsonDocument => Binary
 * Decimal, UUID => FixedSizeBinary(16)
 * Timezone datetime type => StructArray<type, Uint16>
 * DyNumber => BinaryArray (it is not added to YQL-15332)
 *
 * Struct, Tuple, EmptyList, EmptyDict => StructArray
 * Names of fields constructed from tuple are just empty strings.
 *
 * List => ListArray
 *
 * Variant => DenseUnionArray
 * If variant contains more than 127 items then we map
 * Variant => DenseUnionArray<DenseUnionArray>
 * TODO Implement convertion of data to DenseUnionArray<DenseUnionArray> and back
 *
 * Optional<T> => StructArray<T> if T is Variant
 * Because DenseUnionArray does not have validity bitmap
 * Optional<T> => T for other types
 * By default, other types have a validity bitmap
 *
 * Optional<Optional<...<T>...>> => StructArray<StructArray<...StructArray<T>...>>
 * For example:
 * - Optional<Optional<Int32>> => StructArray<Int32>
 *   Int32 has validity bitmap, so we wrap it in StructArray N - 1 times, where N is the number of Optional levels
 * - Optional<Optional<Variant<Int32, Int64>>> => StructArray<StructArray<DenseUnionArray<Int32, Int64>>>
 *   DenseUnionArray does not have validity bitmap, so we wrap it in StructArray N times, where N is the number of Optional levels
 *
 * Dict<KeyType, ValueType> => StructArray<MapArray<KeyArray, ValueArray>, Uint64Array (on demand, default: 0)>
 * We do not use arrow::DictArray because it must be used for encoding not for mapping keys to values.
 * (https://arrow.apache.org/docs/cpp/api/array.html#classarrow_1_1_dictionary_array)
 * If the type of dict key is optional then we map
 * Dict<Optional<KeyType>, ValueType> => StructArray<ListArray<StructArray<KeyArray, ValueArray>, Uint64Array (on demand, default: 0)>
 * because keys of MapArray can not be nullable
 *
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

NUdf::TUnboxedValue ExtractUnboxedValue(const std::shared_ptr<arrow::Array>& array,
    ui64 row, const NKikimr::NMiniKQL::TType* itemType, const NKikimr::NMiniKQL::THolderFactory& holderFactory);

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

class IBlockSplitter : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IBlockSplitter>;

    virtual bool ShouldSplitItem(const NUdf::TUnboxedValuePod* values, ui32 count) = 0;

    virtual std::vector<std::vector<arrow::Datum>> SplitItem(const NUdf::TUnboxedValuePod* values, ui32 count) = 0;
};

IBlockSplitter::TPtr CreateBlockSplitter(const NKikimr::NMiniKQL::TType* type, ui64 chunkSizeLimit);

} // NArrow
} // NYql
