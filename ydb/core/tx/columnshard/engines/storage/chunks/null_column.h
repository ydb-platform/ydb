#pragma once
#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/tx/columnshard/counters/splitter.h>
#include <ydb/core/tx/columnshard/splitter/abstract/chunk_meta.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NChunks {

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

}
