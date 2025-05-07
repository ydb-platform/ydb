#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/common/const.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction::NDictionary {

template <class TArrayImpl, class TConstructorImpl>
class TPortionColumn: public TColumnPortionResult {
private:
    using TBase = TColumnPortionResult;
    const TConstructorImpl Constructor;

public:
    TPortionColumn(const TConstructorImpl& constructor, const ui32 columnId)
        : TBase(columnId)
        , Constructor(constructor) {
    }

    void AddChunk(const std::shared_ptr<TArrayImpl>& cArray, const TColumnMergeContext& cmContext) {
        AFL_VERIFY(cArray);
        AFL_VERIFY(cArray->GetRecordsCount());
        auto accContext = cmContext.GetLoader()->BuildAccessorContext(cArray->GetRecordsCount());
        Chunks.emplace_back(std::make_shared<NChunks::TChunkPreparation>(Constructor.SerializeToString(cArray, accContext), cArray,
            TChunkAddress(cmContext.GetColumnId(), Chunks.size()), cmContext.GetIndexInfo().GetColumnFeaturesVerified(cmContext.GetColumnId())));
    }
};

template <class TArrayImpl>
class TBaseIterator {
private:
    std::shared_ptr<NArrow::NAccessor::IChunkedArray> Input;
    std::shared_ptr<TArrayImpl> CurrentArray;
    std::optional<NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress> CurrentChunk;
    std::shared_ptr<TColumnLoader> Loader;
    ui32 ChunkPosition = 0;
    ui32 GlobalPosition = 0;

    void InitArray(const ui32 globalPosition) {
        if (globalPosition == Input->GetRecordsCount()) {
            GlobalPosition = globalPosition;
            return;
        }
        AFL_VERIFY(globalPosition < Input->GetRecordsCount());
        if (Input) {
            CurrentChunk = Input->GetArray(CurrentChunk, globalPosition, Input);
            if (CurrentChunk->GetArray()->GetType() == TArrayImpl::GetTypeStatic()) {
                CurrentArray = std::static_pointer_cast<TArrayImpl>(CurrentChunk->GetArray());
            } else {
                CurrentArray = std::static_pointer_cast<TArrayImpl>(Loader->GetAccessorConstructor()
                        ->Construct(CurrentChunk->GetArray(), Loader->BuildAccessorContext(CurrentChunk->GetArray()->GetRecordsCount()))
                        .DetachResult());
            }
            ChunkPosition = CurrentChunk->GetAddress().GetLocalIndex(globalPosition);
            GlobalPosition = globalPosition;
        }
    }

public:
    void MoveFurther(const ui32 delta) {
        MoveToPosition(GlobalPosition + delta);
    }

    bool IsValid() const {
        return Input && GlobalPosition < Input->GetRecordsCount();
    }

    void Reset() {
        InitArray(0);
    }

    TBaseIterator(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& input)
        : Input(input) {
        InitArray(0);
    }

    ui32 GetChunkPosition() const {
        return ChunkPosition;
    }

    const TArrayImpl& GetCurrentDataChunk() const {
        AFL_VERIFY(CurrentArray);
        return *CurrentArray;
    }

    void MoveToPosition(const ui32 globalPosition) {
        if (GlobalPosition == globalPosition) {
            AFL_VERIFY(GlobalPosition == 0)("old", GlobalPosition);
            return;
        }
        AFL_VERIFY(Input);
        AFL_VERIFY(GlobalPosition < globalPosition)("old", GlobalPosition)("new", globalPosition)("count", Input->GetRecordsCount());
        if (CurrentChunk->GetAddress().Contains(globalPosition)) {
            ChunkPosition = CurrentChunk->GetAddress().GetLocalIndex(globalPosition);
            GlobalPosition = globalPosition;
        } else {
            InitArray(globalPosition);
        }
    }
};

class TIterator: public TBaseIterator<NArrow::NAccessor::TDictionaryArray> {
private:
    using TBase = TBaseIterator<NArrow::NAccessor::TDictionaryArray>;

public:
    using TBase::TBase;
};

class TMerger: public IColumnMerger {
private:
    using TBase = IColumnMerger;

    static inline auto Registrator = TFactory::TRegistrator<TMerger>(NArrow::NAccessor::TGlobalConst::DictionaryAccessorName);
    std::vector<TIterator> Iterators;
    std::vector<std::vector<ui32>> RemapIndexes;
    std::shared_ptr<arrow::Array> ArrayVariantsFull;

    virtual void DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext) override;
    virtual std::vector<TColumnPortionResult> DoExecute(const TChunkMergeContext& context, TMergingContext& mergeContext) override;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NCompaction::NDictionary
