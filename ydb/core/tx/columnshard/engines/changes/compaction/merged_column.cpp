#include "merged_column.h"

namespace NKikimr::NOlap::NCompaction {

void TMergedColumn::AppendBlob(const TString& data, const TColumnRecord& columnChunk) {
    RecordsCount += columnChunk.GetMeta().GetNumRowsVerified();
    auto remained = Portions.back().AppendBlob(data, columnChunk);
    while (remained) {
        Y_ABORT_UNLESS(Portions.back().IsFullPortion());
        NewPortion();
        remained = Portions.back().AppendSlice(remained);
    }
    if (Portions.back().IsFullPortion()) {
        NewPortion();
    }
}

void TMergedColumn::AppendSlice(const std::shared_ptr<arrow::RecordBatch>& data) {
    RecordsCount += data->num_rows();
    Y_ABORT_UNLESS(data);
    Y_ABORT_UNLESS(data->num_columns() == 1);
    auto remained = data->column(0);
    while (remained = Portions.back().AppendSlice(remained)) {
        Y_ABORT_UNLESS(Portions.back().IsFullPortion());
        NewPortion();
    }
    if (Portions.back().IsFullPortion()) {
        NewPortion();
    }
}

void TMergedColumn::AppendSlice(const std::shared_ptr<arrow::Array>& data) {
    RecordsCount += data->length();
    Y_ABORT_UNLESS(data);
    auto remained = data;
    while (remained = Portions.back().AppendSlice(remained)) {
        Y_ABORT_UNLESS(Portions.back().IsFullPortion());
        NewPortion();
    }
    if (Portions.back().IsFullPortion()) {
        NewPortion();
    }
}

std::vector<NKikimr::NOlap::NCompaction::TColumnPortionResult> TMergedColumn::BuildResult() {
    std::vector<TColumnPortionResult> result;
    if (Portions.size()) {
        Portions.back().FlushBuffer();
    }
    for (auto&& i : Portions) {
        result.emplace_back(i);
    }
    return result;
}

}
