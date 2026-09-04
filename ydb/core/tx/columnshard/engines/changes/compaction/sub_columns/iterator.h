#pragma once
#include "remap.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/settings.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction::NSubColumns {

class TChunksIterator {
private:
    using TReadIteratorOrderedKeys = NArrow::NAccessor::NSubColumns::TReadIteratorOrderedKeys;
    using IChunkedArray = NArrow::NAccessor::IChunkedArray;
    using TSubColumnsArray = NArrow::NAccessor::TSubColumnsArray;
    const std::shared_ptr<IChunkedArray> OriginalArray;
    std::optional<IChunkedArray::TFullChunkedArrayAddress> CurrentChunk;
    ui32 CurrentChunkStartPosition = 0;
    YDB_READONLY_DEF(std::shared_ptr<TSubColumnsArray>, CurrentSubColumnsArray);
    std::shared_ptr<TReadIteratorOrderedKeys> DataIterator;
    std::shared_ptr<TColumnLoader> Loader;
    TRemapColumns& Remapper;
    const ui32 SourceIdx;

    std::shared_ptr<TSubColumnsArray> MaterializeSubColumnsArray(const std::shared_ptr<IChunkedArray>& array) const {
        if (array->GetType() == IChunkedArray::EType::SubColumnsArray) {
            return std::static_pointer_cast<TSubColumnsArray>(array);
        }
        return std::static_pointer_cast<TSubColumnsArray>(
            Loader->GetAccessorConstructor()->Construct(array, Loader->BuildAccessorContext(array->GetRecordsCount())).DetachResult());
    }

    void InitArraysImpl(const ui32 position) {
        if (OriginalArray) {
            CurrentChunk = OriginalArray->GetArray(CurrentChunk, position, OriginalArray);
            CurrentChunkStartPosition = CurrentChunk->GetAddress().GetGlobalStartPosition();
            CurrentSubColumnsArray = MaterializeSubColumnsArray(CurrentChunk->GetArray());
        }
    }

    void InitArray(const ui32 position) {
        if (OriginalArray) {
            if (position) {
                InitArraysImpl(position);
            }
            Remapper.StartSourceChunk(
                SourceIdx, CurrentSubColumnsArray->GetColumnsData().GetStats(), CurrentSubColumnsArray->GetOthersData().GetStats());
            DataIterator = CurrentSubColumnsArray->BuildOrderedIterator();
        }
    }

public:
    TChunksIterator(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& originalArray, const std::shared_ptr<TColumnLoader>& loader,
        TRemapColumns& remapper, const ui32 sourceIdx)
        : OriginalArray(originalArray)
        , Loader(loader)
        , Remapper(remapper)
        , SourceIdx(sourceIdx)
    {
        InitArraysImpl(0);
    }

    void Start() {
        InitArray(0);
    }

    std::vector<std::shared_ptr<TSubColumnsArray>> MaterializePerChunkArrays() const {
        std::vector<std::shared_ptr<TSubColumnsArray>> result;
        if (!OriginalArray) {
            return result;
        }
        std::optional<IChunkedArray::TFullChunkedArrayAddress> chunk;
        ui32 position = 0;
        while (position < OriginalArray->GetRecordsCount()) {
            chunk = OriginalArray->GetArray(chunk, position, OriginalArray);
            result.emplace_back(MaterializeSubColumnsArray(chunk->GetArray()));
            position = chunk->GetAddress().GetGlobalStartPosition() + chunk->GetArray()->GetRecordsCount();
        }
        return result;
    }

    template <class TStartRecordActor, class TKVActor, class TFinishRecordActor>
    void ReadRecord(const ui32 recordIndex, const TStartRecordActor& startRecordActor, const TKVActor& kvActor,
        const TFinishRecordActor& finishRecordActor) {
        if (!OriginalArray) {
            startRecordActor(recordIndex);
            finishRecordActor();
            return;
        }
        AFL_VERIFY(CurrentChunkStartPosition <= recordIndex)("pred", CurrentChunkStartPosition)("record", recordIndex);
        if (recordIndex - CurrentChunkStartPosition >= CurrentChunk->GetArray()->GetRecordsCount()) {
            InitArray(recordIndex);
        }
        AFL_VERIFY(CurrentChunk->GetAddress().Contains(recordIndex));
        DataIterator->ReadRecord(recordIndex - CurrentChunkStartPosition, startRecordActor, kvActor, finishRecordActor);
    }
};

}   // namespace NKikimr::NOlap::NCompaction::NSubColumns
