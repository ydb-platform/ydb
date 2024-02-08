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

class TChunkPreparation: public IPortionColumnChunk {
private:
    using TBase = IPortionColumnChunk;
    TString Data;
    TColumnRecord Record;
    ISnapshotSchema::TPtr SchemaInfo;
    std::shared_ptr<arrow::Scalar> First;
    std::shared_ptr<arrow::Scalar> Last;
protected:
    virtual std::vector<std::shared_ptr<IPortionDataChunk>> DoInternalSplitImpl(const TColumnSaver& saver, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters, const std::vector<ui64>& splitSizes) const override;
    virtual const TString& DoGetData() const override {
        return Data;
    }
    virtual ui32 DoGetRecordsCountImpl() const override {
        return Record.GetMeta().GetNumRowsVerified();
    }
    virtual TString DoDebugString() const override {
        return "";
    }
    virtual TSimpleChunkMeta DoBuildSimpleChunkMeta() const override {
        return Record.GetMeta();
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetFirstScalar() const override {
        return First;
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetLastScalar() const override {
        return Last;
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
        Y_ABORT_UNLESS(Data.size() == Record.BlobRange.Size || columnChunk.BlobRange.Size == 0);
    }

    TChunkPreparation(const TString& data, const std::shared_ptr<arrow::Array>& column, const ui32 columnId, ISnapshotSchema::TPtr schema)
        : TBase(columnId)
        , Data(data)
        , Record(TChunkAddress(columnId, 0), column, schema->GetIndexInfo())
        , SchemaInfo(schema) {
        Y_ABORT_UNLESS(column->length());
        First = NArrow::TStatusValidator::GetValid(column->GetScalar(0));
        Last = NArrow::TStatusValidator::GetValid(column->GetScalar(column->length() - 1));
        Record.BlobRange.Size = data.size();
    }
};

class TNullChunkPreparation: public IPortionColumnChunk {
private:
    using TBase = IPortionColumnChunk;
    const ui32 RecordsCount;
    TString Data;
protected:
    virtual std::vector<std::shared_ptr<IPortionDataChunk>> DoInternalSplitImpl(const TColumnSaver& /*saver*/, const std::shared_ptr<NColumnShard::TSplitterCounters>& /*counters*/,
                                                                                const std::vector<ui64>& /*splitSizes*/) const override {
        AFL_VERIFY(false);
        return {};
    }
    virtual const TString& DoGetData() const override {
        return Data;
    }
    virtual ui32 DoGetRecordsCountImpl() const override {
        return RecordsCount;
    }
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "rc=" << RecordsCount << ";data_size=" << Data.size() << ";";
    }
    virtual TSimpleChunkMeta DoBuildSimpleChunkMeta() const override {
        AFL_VERIFY(false);
        return TSimpleChunkMeta(nullptr, false, false);
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetFirstScalar() const override {
        return nullptr;
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetLastScalar() const override {
        return nullptr;
    }

public:
    TNullChunkPreparation(const ui32 columnId, const ui32 recordsCount, const std::shared_ptr<arrow::Field>& f, const TColumnSaver& saver)
        : TBase(columnId)
        , RecordsCount(recordsCount)
        , Data(saver.Apply(NArrow::TThreadSimpleArraysCache::GetNull(f->type(), recordsCount), f))
    {
        Y_ABORT_UNLESS(RecordsCount);
        SetChunkIdx(0);
    }
};

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
public:
    TColumnPortion(const TColumnMergeContext& context)
        : TBase(context.GetColumnId())
        , Context(context) {
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
