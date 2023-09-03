#pragma once
#include "merge_context.h"
#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/counters/splitter.h>
#include <ydb/core/tx/columnshard/splitter/chunk_meta.h>

namespace NKikimr::NOlap::NCompaction {

class TChunkPreparation: public IPortionColumnChunk {
private:
    using TBase = IPortionColumnChunk;
    TString Data;
    TColumnRecord Record;
    ISnapshotSchema::TPtr SchemaInfo;
protected:
    virtual std::vector<IPortionColumnChunk::TPtr> DoInternalSplit(const TColumnSaver& saver, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const std::vector<ui64>& splitSizes) const override;
    virtual const TString& DoGetData() const override {
        return Data;
    }
    virtual ui32 DoGetRecordsCount() const override {
        return Record.GetMeta().GetNumRowsVerified();
    }
    virtual TString DoDebugString() const override {
        return "";
    }
    virtual TSimpleChunkMeta DoBuildSimpleChunkMeta() const override {
        return Record.GetMeta();
    }
public:
    const TColumnRecord& GetRecord() const {
        return Record;
    }

    TChunkPreparation(const TString& data, const TColumnRecord& columnChunk, ISnapshotSchema::TPtr schema)
        : TBase(columnChunk.ColumnId)
        , Data(data)
        , Record(columnChunk)
        , SchemaInfo(schema) {
        Y_VERIFY(Data.size() == Record.BlobRange.Size || columnChunk.BlobRange.Size == 0);
    }

    TChunkPreparation(const TString& data, const std::shared_ptr<arrow::Array>& column, const ui32 columnId, ISnapshotSchema::TPtr schema)
        : TBase(columnId)
        , Data(data)
        , Record(TChunkAddress(columnId, 0), column, schema->GetIndexInfo())
        , SchemaInfo(schema) {
        Record.BlobRange.Size = data.size();
    }
};

class TColumnPortionResult {
protected:
    std::vector<std::shared_ptr<IPortionColumnChunk>> Chunks;
    ui64 CurrentPortionRecords = 0;
    const ui32 ColumnId;
    std::string ColumnName;
    ui64 PackedSize = 0;
public:
    ui64 GetPackedSize() const {
        return PackedSize;
    }

    const std::string& GetColumnName() const {
        return ColumnName;
    }

    TColumnPortionResult(const std::string& columnName, const ui32 columnId)
        : ColumnId(columnId)
        , ColumnName(columnName) {

    }

    const std::vector<std::shared_ptr<IPortionColumnChunk>>& GetChunks() const {
        return Chunks;
    }

    ui64 GetCurrentPortionRecords() const {
        return CurrentPortionRecords;
    }

};

class TColumnPortion: public TColumnPortionResult {
private:
    using TBase = TColumnPortionResult;
    std::unique_ptr<arrow::ArrayBuilder> Builder;
    const TColumnMergeContext& Context;
    YDB_READONLY(ui64, CurrentChunkRawSize, 0);
    double PredictedPackedBytes = 0;
public:
    TColumnPortion(const TColumnMergeContext& context)
        : TBase(context.GetField()->name(), context.GetColumnId())
        , Context(context) {
        Builder = Context.MakeBuilder();
    }

    bool IsFullPortion() const {
        Y_VERIFY(CurrentPortionRecords <= Context.GetPortionRowsCountLimit());
        return CurrentPortionRecords == Context.GetPortionRowsCountLimit();
    }

    void FlushBuffer();

    std::shared_ptr<arrow::Array> AppendBlob(const TString& data, const TColumnRecord& columnChunk);
    std::shared_ptr<arrow::Array> AppendSlice(const std::shared_ptr<arrow::RecordBatch>& data) {
        Y_VERIFY(data->num_columns() == 1);
        return AppendSlice(data->column(0));
    }
    std::shared_ptr<arrow::Array> AppendSlice(const std::shared_ptr<arrow::Array>& a);
};

}
