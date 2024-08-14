#pragma once
#include "column_portion_chunk.h"

#include <ydb/core/tx/columnshard/engines/changes/compaction/common/context.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>

namespace NKikimr::NOlap::NCompaction {

class TMergedColumn {
private:
    TColumnMergeContext Context;
    TChunkMergeContext ChunkContext;
    YDB_READONLY_DEF(std::vector<TColumnPortion>, Portions);
    YDB_READONLY(ui32, RecordsCount, 0);

    void NewPortion();

public:
    TMergedColumn(const TColumnMergeContext& context, const TChunkMergeContext& chunkContext)
        : Context(context)
        , ChunkContext(chunkContext)
    {
        NewPortion();
    }

    void AppendBlob(const TString& data, const TColumnRecord& columnChunk);
    void AppendSlice(const std::shared_ptr<arrow::Array>& data, const ui32 startIndex, const ui32 length);

    std::vector<TColumnPortionResult> BuildResult();
};

}   // namespace NKikimr::NOlap::NCompaction
