#pragma once
#include "column_portion_chunk.h"

#include <ydb/core/tx/columnshard/engines/changes/compaction/common/context.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>

namespace NKikimr::NOlap::NCompaction {

class TMergedColumn {
private:
    TColumnMergeContext Context;
    TColumnPortion Portion;
    YDB_READONLY(ui32, RecordsCount, 0);

public:
    TMergedColumn(const TColumnMergeContext& context)
        : Context(context)
        , Portion(Context) {
    }

    const TColumnPortion& GetPortion() const {
        return Portion;
    }

    void AppendSlice(const std::shared_ptr<arrow::Array>& data, const ui32 startIndex, const ui32 length);

    TColumnPortionResult BuildResult();
};

}   // namespace NKikimr::NOlap::NCompaction
