#pragma once
#include "abstract/chunk_meta.h"
#include "abstract/chunks.h"

#include <ydb/core/tx/columnshard/counters/splitter.h>
#include <ydb/core/tx/columnshard/engines/portions/common.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>

namespace NKikimr::NOlap {

class IPortionColumnChunk: public IPortionDataChunk {
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

    virtual void DoAddIntoPortionBeforeBlob(const TBlobRangeLink16& bRange, TPortionAccessorConstructor& portionInfo) const override;

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

    std::shared_ptr<NArrow::NAccessor::IChunkedArray> CurrentArray;
    std::optional<NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress> CurrentChunkArray;
    ui32 CurrentArrayIndex = 0;
    ui32 CurrentRecordIndex = 0;

public:

    TChunkedColumnReader(const std::vector<std::shared_ptr<IPortionDataChunk>>& chunks, const std::shared_ptr<TColumnLoader>& loader)
        : Chunks(chunks)
        , Loader(loader) {
        Start();
    }

    void Start() {
        CurrentArrayIndex = 0;
        CurrentRecordIndex = 0;
        if (Chunks.size()) {
            CurrentArray = Loader->ApplyVerified(Chunks.front()->GetData(), Chunks.front()->GetRecordsCountVerified());
            CurrentChunkArray.reset();
        }
    }

    const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& GetCurrentChunk() {
        if (!CurrentChunkArray || !CurrentChunkArray->GetAddress().Contains(CurrentRecordIndex)) {
            CurrentChunkArray = CurrentArray->GetArray(CurrentChunkArray, CurrentRecordIndex, CurrentArray);
        }
        AFL_VERIFY(CurrentChunkArray);
        return CurrentChunkArray->GetArray();
    }

    bool IsCorrect() const {
        return !!CurrentArray;
    }

    bool ReadNextChunk() {
        while (++CurrentArrayIndex < Chunks.size()) {
            CurrentArray = Loader->ApplyVerified(Chunks[CurrentArrayIndex]->GetData(), Chunks[CurrentArrayIndex]->GetRecordsCountVerified());
            CurrentChunkArray.reset();
            CurrentRecordIndex = 0;
            if (CurrentRecordIndex < CurrentArray->GetRecordsCount()) {
                return true;
            }
        }
        CurrentChunkArray.reset();
        CurrentArray = nullptr;
        return false;
    }

    bool ReadNext() {
        AFL_VERIFY(!!CurrentArray);
        if (++CurrentRecordIndex < CurrentArray->GetRecordsCount()) {
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
        : Columns(columnReaders) {
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

    bool ReadNext(const ui32 count) {
        for (ui32 i = 0; i < count; ++i) {
            if (!ReadNext()) {
                AFL_VERIFY(i + 1 == count);
                return false;
            }
        }
        return true;
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

}   // namespace NKikimr::NOlap
