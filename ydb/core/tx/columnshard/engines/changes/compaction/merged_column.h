#pragma once
#include "column_portion_chunk.h"
#include "merge_context.h"
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>

namespace NKikimr::NOlap::NCompaction {

class TMergedColumn {
private:
    TColumnMergeContext Context;
    YDB_READONLY_DEF(std::vector<TColumnPortion>, Portions);

    void NewPortion() {
        Portions.emplace_back(TColumnPortion(Context));
    }

public:
    TMergedColumn(const TColumnMergeContext& context)
        : Context(context) {
        NewPortion();
    }

    void AppendBlob(const TString& data, const TColumnRecord& columnChunk);
    void AppendSlice(const std::shared_ptr<arrow::RecordBatch>& data);
    void AppendSlice(const std::shared_ptr<arrow::Array>& data);

    std::vector<TColumnPortionResult> BuildResult();
};

}
