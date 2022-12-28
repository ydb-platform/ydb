#pragma once

#include <arrow/array/data.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NKikimr::NMiniKQL {

/// \brief Recursive version of ArrayData::Slice() method
std::shared_ptr<arrow::ArrayData> DeepSlice(const std::shared_ptr<arrow::ArrayData>& data, size_t offset, size_t len);

/// \brief Chops first len items of `data` as new ArrayData object
std::shared_ptr<arrow::ArrayData> Chop(std::shared_ptr<arrow::ArrayData>& data, size_t len);

/// \brief Remove optional from `data` as new ArrayData object
std::shared_ptr<arrow::ArrayData> Unwrap(const arrow::ArrayData& data, TType* itemType);

}
