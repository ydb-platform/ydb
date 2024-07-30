#pragma once
#include "abstract/chunk_meta.h"
#include "abstract/chunks.h"

#include <ydb/core/tx/columnshard/counters/splitter.h>
#include <ydb/core/tx/columnshard/engines/portions/common.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>

namespace NKikimr::NOlap {

class IPortionColumnChunk : public IPortionDataChunk {
private:
    using TBase = IPortionDataChunk;

protected:
    virtual TSimpleChunkMeta DoBuildSimpleChunkMeta() const = 0;
    virtual ui32 DoGetRecordsCountImpl() const = 0;
    virtual ui64 DoGetRawBytesImpl() const = 0;

    virtual std::optional<ui64> DoGetRawBytes() const final {
        return DoGetRawBytesImpl();
    }

    virtual std::optional<ui32> DoGetRecordsCount() const override final {
        return DoGetRecordsCountImpl();
    }

    virtual void DoAddIntoPortionBeforeBlob(const TBlobRangeLink16& bRange, TPortionInfoConstructor& portionInfo) const override;

    virtual std::vector<std::shared_ptr<IPortionDataChunk>> DoInternalSplitImpl(const TColumnSaver& saver,
        const std::shared_ptr<NColumnShard::TSplitterCounters>& counters, const std::vector<ui64>& splitSizes) const = 0;
    virtual std::vector<std::shared_ptr<IPortionDataChunk>> DoInternalSplit(const TColumnSaver& saver,
        const std::shared_ptr<NColumnShard::TSplitterCounters>& counters, const std::vector<ui64>& splitSizes) const override;
    virtual bool DoIsSplittable() const override {
        return GetRecordsCount() > 1;
    }

public:
    IPortionColumnChunk(const ui32 entityId, const std::optional<ui16>& chunkIdx = {})
        : TBase(entityId, chunkIdx) {
    }
    virtual ~IPortionColumnChunk() = default;

    TSimpleChunkMeta BuildSimpleChunkMeta() const {
        return DoBuildSimpleChunkMeta();
    }

    ui32 GetColumnId() const {
        return GetEntityId();
    }
};

class TChunkedColumnReader {
private:
    std::vector<std::shared_ptr<IPortionDataChunk>> Chunks;
    std::shared_ptr<TColumnLoader> Loader;

    std::shared_ptr<NArrow::NAccessor::IChunkedArray> CurrentChunk;
    std::optional<NArrow::NAccessor::IChunkedArray::TFullDataAddress> CurrentChunkArray;
    ui32 CurrentChunkIndex = 0;
    ui32 CurrentRecordIndex = 0;
public:
    TChunkedColumnReader(const std::vector<std::shared_ptr<IPortionDataChunk>>& chunks, const std::shared_ptr<TColumnLoader>& loader)
        : Chunks(chunks) 
        , Loader(loader)
    {
        Start();
    }

    void Start() {
        CurrentChunkIndex = 0;
        CurrentRecordIndex = 0;
        if (Chunks.size()) {
            CurrentChunk = Loader->ApplyVerified(Chunks.front()->GetData(), Chunks.front()->GetRecordsCountVerified());
            CurrentChunkArray.reset();
        }
    }

    const std::shared_ptr<arrow::Array>& GetCurrentChunk() {
        if (!CurrentChunkArray || !CurrentChunkArray->GetAddress().Contains(CurrentRecordIndex)) {
            CurrentChunkArray = CurrentChunk->GetChunk(CurrentChunkArray, CurrentRecordIndex);
        }
        AFL_VERIFY(CurrentChunkArray);
        return CurrentChunkArray->GetArray();
    }

    const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& GetCurrentAccessor() const {
        AFL_VERIFY(CurrentChunk);
        return CurrentChunk;
    }

    ui32 GetCurrentRecordIndex() {
        if (!CurrentChunkArray || !CurrentChunkArray->GetAddress().Contains(CurrentRecordIndex)) {
            CurrentChunkArray = CurrentChunk->GetChunk(CurrentChunkArray->GetAddress(), CurrentRecordIndex);
        }
        return CurrentChunkArray->GetAddress().GetLocalIndex(CurrentRecordIndex);
    }

    bool IsCorrect() const {
        return !!CurrentChunk;
    }

    bool ReadNextChunk() {
        while (++CurrentChunkIndex < Chunks.size()) {
            CurrentChunk = Loader->ApplyVerified(Chunks[CurrentChunkIndex]->GetData(), Chunks[CurrentChunkIndex]->GetRecordsCountVerified());
            CurrentChunkArray.reset();
            CurrentRecordIndex = 0;
            if (CurrentRecordIndex < CurrentChunk->GetRecordsCount()) {
                return true;
            }
        }
        CurrentChunkArray.reset();
        CurrentChunk = nullptr;
        return false;
    }

    bool ReadNext() {
        AFL_VERIFY(!!CurrentChunk);
        if (++CurrentRecordIndex < CurrentChunk->GetRecordsCount()) {
            return true;
        }
        return ReadNextChunk();
    }
};

class TChunkedBatchReader {
private:
    std::vector<TChunkedColumnReader> Columns;
    bool IsCorrectFlag = true;
public:
    TChunkedBatchReader(const std::vector<TChunkedColumnReader>& columnReaders)
        : Columns(columnReaders)
    {
        AFL_VERIFY(Columns.size());
        for (auto&& i : Columns) {
            AFL_VERIFY(i.IsCorrect());
        }
    }

    bool IsCorrect() const {
        return IsCorrectFlag;
    }

    void Start() {
        IsCorrectFlag = true;
        for (auto&& i : Columns) {
            i.Start();
        }
    }

    bool ReadNext() {
        std::optional<bool> result;
        for (auto&& i : Columns) {
            if (!result) {
                result = i.ReadNext();
            } else {
                AFL_VERIFY(*result == i.ReadNext());
            }
        }
        if (!*result) {
            IsCorrectFlag = false;
        }
        return *result;
    }

    ui32 GetColumnsCount() const {
        return Columns.size();
    }

    std::vector<TChunkedColumnReader>::const_iterator begin() const {
        return Columns.begin();
    }

    std::vector<TChunkedColumnReader>::const_iterator end() const {
        return Columns.end();
    }

    std::vector<TChunkedColumnReader>::iterator begin() {
        return Columns.begin();
    }

    std::vector<TChunkedColumnReader>::iterator end() {
        return Columns.end();
    }
};

}
