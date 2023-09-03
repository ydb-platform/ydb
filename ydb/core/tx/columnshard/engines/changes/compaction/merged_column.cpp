#include "merged_column.h"

namespace NKikimr::NOlap::NCompaction {

void TMergedColumn::AppendBlob(const TString& data, const TColumnRecord& columnChunk) {
    auto remained = Portions.back().AppendBlob(data, columnChunk);
    while (remained) {
        Y_VERIFY(Portions.back().IsFullPortion());
        NewPortion();
        remained = Portions.back().AppendSlice(remained);
    }
    if (Portions.back().IsFullPortion()) {
        NewPortion();
    }
}

void TMergedColumn::AppendSlice(const std::shared_ptr<arrow::RecordBatch>& data) {
    Y_VERIFY(data);
    Y_VERIFY(data->num_columns() == 1);
    auto remained = data->column(0);
    while (remained = Portions.back().AppendSlice(remained)) {
        Y_VERIFY(Portions.back().IsFullPortion());
        NewPortion();
    }
    if (Portions.back().IsFullPortion()) {
        NewPortion();
    }
}

void TMergedColumn::AppendSlice(const std::shared_ptr<arrow::Array>& data) {
    Y_VERIFY(data);
    auto remained = data;
    while (remained = Portions.back().AppendSlice(remained)) {
        Y_VERIFY(Portions.back().IsFullPortion());
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
