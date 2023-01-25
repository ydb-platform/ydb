#pragma once

#include <util/generic/vector.h>

#include <arrow/datum.h>

#include <functional>

namespace NYql {
namespace NUdf {

/// \brief Recursive version of ArrayData::Slice() method
std::shared_ptr<arrow::ArrayData> DeepSlice(const std::shared_ptr<arrow::ArrayData>& data, size_t offset, size_t len);

/// \brief Chops first len items of `data` as new ArrayData object
std::shared_ptr<arrow::ArrayData> Chop(std::shared_ptr<arrow::ArrayData>& data, size_t len);

void ForEachArrayData(const arrow::Datum& datum, const std::function<void(const std::shared_ptr<arrow::ArrayData>&)>& func);
arrow::Datum MakeArray(const TVector<std::shared_ptr<arrow::ArrayData>>& chunks);

}
}
