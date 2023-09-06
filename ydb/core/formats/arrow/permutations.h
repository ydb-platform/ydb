#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/system/types.h>

namespace NKikimr::NArrow {

std::shared_ptr<arrow::UInt64Array> MakePermutation(const int size, const bool reverse = false);
std::shared_ptr<arrow::UInt64Array> MakeFilterPermutation(const std::vector<ui64>& indexes);
std::shared_ptr<arrow::UInt64Array> MakeSortPermutation(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                        const std::shared_ptr<arrow::Schema>& sortingKey, const bool andUnique);

std::shared_ptr<arrow::Array> CopyRecords(const std::shared_ptr<arrow::Array>& source, const std::vector<ui64>& indexes);

}
