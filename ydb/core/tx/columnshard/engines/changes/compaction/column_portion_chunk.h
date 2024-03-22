#pragma once
#include "merge_context.h"
#include <ydb/core/formats/arrow/simple_arrays_cache.h>
#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/counters/splitter.h>
#include <ydb/core/tx/columnshard/splitter/chunk_meta.h>

namespace NKikimr::NOlap::NCompaction {

class TColumnPortionResult {
protected:
    std::vector<std::shared_ptr<IPortionDataChunk>> Chunks;
    ui64 CurrentPortionRecords = 0;
    const ui32 ColumnId;
    ui64 PackedSize = 0;
public:
    ui64 GetPackedSize() const {
        return PackedSize;
    }

    TColumnPortionResult(const ui32 columnId)
        : ColumnId(columnId) {

    }

    const std::vector<std::shared_ptr<IPortionDataChunk>>& GetChunks() const {
        return Chunks;
    }

    ui64 GetCurrentPortionRecords() const {
        return CurrentPortionRecords;
    }

    TString DebugString() const {
        return TStringBuilder() << "chunks=" << Chunks.size() << ";records=" << CurrentPortionRecords << ";";
    }

};

class TColumnPortion: public TColumnPortionResult {
private:
    using TBase = TColumnPortionResult;
    std::unique_ptr<arrow::ArrayBuilder> Builder;
    const TColumnMergeContext& Context;
    YDB_READONLY(ui64, CurrentChunkRawSize, 0);
    double PredictedPackedBytes = 0;
    const TSimpleColumnInfo ColumnInfo;
public:
    TColumnPortion(const TColumnMergeContext& context)
        : TBase(context.GetColumnId())
        , Context(context)
        , ColumnInfo(Context.GetIndexInfo().GetColumnFeaturesVerified(context.GetColumnId()))
    {
        Builder = Context.MakeBuilder();
    }

    bool IsFullPortion() const {
        Y_ABORT_UNLESS(CurrentPortionRecords <= Context.GetPortionRowsCountLimit());
        return CurrentPortionRecords == Context.GetPortionRowsCountLimit();
    }

    bool FlushBuffer();

    std::shared_ptr<arrow::Array> AppendBlob(const TString& data, const TColumnRecord& columnChunk, ui32& remained);
    ui32 AppendSlice(const std::shared_ptr<arrow::Array>& a, const ui32 startIndex, const ui32 length);
};

}
