#pragma once
#include "arrow_helpers.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/permutations.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/system/types.h>

namespace NKikimr::NArrow {

class THashConstructor {
public:
    static bool BuildHashUI64(std::shared_ptr<arrow::Table>& batch, const std::vector<std::string>& fieldNames, const std::string& hashFieldName);
    static bool BuildHashUI64(std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& fieldNames, const std::string& hashFieldName);

};

std::shared_ptr<arrow::UInt64Array> MakeSortPermutation(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& sortingKey, const bool andUnique);

std::shared_ptr<arrow::UInt64Array> MakeSortPermutation(const std::vector<std::shared_ptr<arrow::Array>>& columns, const bool andUnique);

}   // namespace NKikimr::NArrow
