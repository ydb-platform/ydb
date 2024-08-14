#include "merged_column.h"

namespace NKikimr::NOlap::NCompaction {

void TMergedColumn::AppendBlob(const TString& data, const TColumnRecord& columnChunk) {
    RecordsCount += columnChunk.GetMeta().GetNumRows();
    ui32 remained;
    std::shared_ptr<arrow::Array> dataArray = Portions.back().AppendBlob(data, columnChunk, remained);
    while (remained) {
        Y_ABORT_UNLESS(Portions.back().IsFullPortion());
        NewPortion();
        remained = Portions.back().AppendSlice(dataArray, dataArray->length() - remained, remained);
    }
    if (Portions.back().IsFullPortion()) {
        NewPortion();
    }
}

void TMergedColumn::AppendSlice(const std::shared_ptr<arrow::Array>& data, const ui32 startIndex, const ui32 length) {
    RecordsCount += length;
    Y_ABORT_UNLESS(data);
    ui32 remained = length;
    while (remained = Portions.back().AppendSlice(data, startIndex + length - remained, remained)) {
        Y_ABORT_UNLESS(Portions.back().IsFullPortion());
        NewPortion();
    }
    if (Portions.back().IsFullPortion()) {
        NewPortion();
    }
}

std::vector<TColumnPortionResult> TMergedColumn::BuildResult() {
    std::vector<TColumnPortionResult> result;
    if (Portions.size()) {
        Portions.back().FlushBuffer();
    }
    for (auto&& i : Portions) {
        result.emplace_back(i);
    }
    return result;
}

void TMergedColumn::NewPortion() {
    if (Portions.size()) {
        Portions.back().FlushBuffer();
    }
    Portions.emplace_back(TColumnPortion(Context, ChunkContext));
}

}
