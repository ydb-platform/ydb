#pragma once
#include "column_portion_chunk.h"
#include "merge_context.h"
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>

namespace NKikimr::NOlap::NCompaction {

class TMergedColumn {
private:
    TColumnMergeContext Context;
    YDB_READONLY_DEF(std::vector<TColumnPortion>, Portions);
    YDB_READONLY(ui32, RecordsCount, 0);

    void NewPortion() {
        Portions.emplace_back(TColumnPortion(Context));
    }

public:
    TMergedColumn(const TColumnMergeContext& context)
        : Context(context) {
        NewPortion();
    }

    void AppendBlob(const TString& data, const TColumnRecord& columnChunk);
    void AppendSlice(const std::shared_ptr<arrow::Array>& data, const ui32 startIndex, const ui32 length);

    std::vector<TColumnPortionResult> BuildResult();
};

}
