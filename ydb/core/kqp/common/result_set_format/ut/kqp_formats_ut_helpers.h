#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr::NKqp::NFormats {

/**
 * @brief Make arrow array builder for given type.
 * The type is converted to arrow type by NKqp::NFormats::GetArrowType function.
 *
 * @param type type to make builder for
 * @return unique pointer to arrow array builder
 */
std::unique_ptr<arrow::ArrayBuilder> MakeArrowBuilder(const NMiniKQL::TType* type);

/**
 * @brief Make arrow array for given values and type.
 * The type is converted to arrow type by NKqp::NFormats::GetArrowType function.
 *
 * @param values values to make array for
 * @param itemType type of each element to parse it and to construct corresponding arrow type
 * @return shared pointer to arrow array
 */
std::shared_ptr<arrow::Array> MakeArrowArray(NMiniKQL::TUnboxedValueVector& values, const NMiniKQL::TType* itemType);

/**
 * @brief Extract unboxed value from arrow array for given row and type.
 * The type of the item and the arrow array type must be the same by NKqp::NFormats::GetArrowType function.
 *
 * @param array arrow array to extract value from
 * @param row row to extract value from
 * @param itemType type of each element to parse it and to construct corresponding arrow type
 * @param holderFactory holder factory to use
 * @return unboxed value
 */
NUdf::TUnboxedValue ExtractUnboxedValue(const std::shared_ptr<arrow::Array>& array, ui64 row,
    const NMiniKQL::TType* itemType, const NMiniKQL::THolderFactory& holderFactory);

/**
 * @brief Extract unboxed values from arrow array for given type.
 * The type of items and the arrow array type must be the same by NKqp::NFormats::GetArrowType function.
 *
 * @param array arrow array to extract values from
 * @param itemType type of each element to parse it and to construct corresponding arrow type
 * @param holderFactory holder factory to use
 * @return vector of unboxed values
 */
NMiniKQL::TUnboxedValueVector ExtractUnboxedVector(const std::shared_ptr<arrow::Array>& array,
    const NMiniKQL::TType* itemType, const NMiniKQL::THolderFactory& holderFactory);

} // namespace NKikimr::NKqp::NFormats
