#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/common/const.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>

namespace NKikimr::NOlap::NCompaction {

class TSparsedMerger: public IColumnMerger {
private:
    static inline auto Registrator = TFactory::TRegistrator<TSparsedMerger>(NArrow::NAccessor::TGlobalConst::SparsedDataAccessorName);

    using TBase = IColumnMerger;
    class TWriter: public TColumnPortionResult {
    private:
        using TBase = TColumnPortionResult;
        const std::shared_ptr<arrow::DataType> DataType;
        const TColumnMergeContext& Context;
        std::unique_ptr<arrow::ArrayBuilder> IndexBuilder;
        std::unique_ptr<arrow::ArrayBuilder> ValueBuilder;
        arrow::UInt32Builder* IndexBuilderImpl = nullptr;
        ui32 CurrentRecordIdx = 0;
        ui32 UsefulRecordsCount = 0;

    public:
        TWriter(const TColumnMergeContext& context);

        bool HasData() const {
            return CurrentRecordIdx;
        }

        ui32 GetCurrentSize() const {
            return CurrentRecordIdx;
        }

        bool HasUsefulData() const {
            return UsefulRecordsCount;
        }

        ui32 AddPositions(const i32 delta) {
            AFL_VERIFY(delta > 0);
            CurrentRecordIdx += delta;
            return CurrentRecordIdx;
        }

        void AddRealData(const std::shared_ptr<arrow::Array>& arr, const ui32 index);

        TColumnPortionResult Flush();
    };

    class TPlainChunkCursor {
    private:
        std::shared_ptr<NArrow::NAccessor::IChunkedArray> CurrentChunkedArray;
        std::optional<NArrow::NAccessor::IChunkedArray::TFullDataAddress> ChunkAddress;
        const NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress* CurrentOwnedArray;
        ui32 ChunkStartPosition = 0;
        ui32 ChunkFinishPosition = 0;

        void InitArrays(const ui32 position) {
            AFL_VERIFY(!ChunkAddress || ChunkFinishPosition <= position);
            ChunkAddress = CurrentChunkedArray->GetChunk(ChunkAddress, position);
            AFL_VERIFY(ChunkAddress);
            ChunkStartPosition = CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + ChunkAddress->GetAddress().GetGlobalStartPosition();
            ChunkFinishPosition =
                CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + ChunkAddress->GetAddress().GetGlobalFinishPosition();
        }

    public:
        TPlainChunkCursor(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& chunked,
            const NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress* currentOwnedArray)
            : CurrentChunkedArray(chunked)
            , CurrentOwnedArray(currentOwnedArray) {
            AFL_VERIFY(CurrentChunkedArray);
            AFL_VERIFY(CurrentOwnedArray);
            InitArrays(CurrentOwnedArray->GetAddress().GetGlobalStartPosition());
        }
        bool AddIndexTo(const ui32 index, TWriter& writer);
        std::optional<ui32> MoveToSignificant(const ui32 currentGlobalPosition, const TColumnMergeContext& context) {
            AFL_VERIFY(ChunkStartPosition <= currentGlobalPosition);
            ui32 currentIndex = currentGlobalPosition;
            while (true) {
                if (CurrentOwnedArray->GetAddress().GetGlobalFinishPosition() <= currentIndex) {
                    return {};
                }
                if (ChunkFinishPosition <= currentIndex) {
                    InitArrays(currentGlobalPosition);
                    continue;
                }
                for (; currentIndex < ChunkFinishPosition; ++currentIndex) {
                    if (!NArrow::ColumnEqualsScalar(
                            ChunkAddress->GetArray(), currentIndex - ChunkStartPosition, context.GetLoader()->GetDefaultValue())) {
                        return currentIndex;
                    }
                }
            }
        }
    };

    class TSparsedChunkCursor {
    private:
        std::shared_ptr<NArrow::NAccessor::TSparsedArray> CurrentSparsedArray;
        const NArrow::NAccessor::TSparsedArrayChunk* Chunk = nullptr;
        const NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress* CurrentOwnedArray;
        ui32 ChunkStartGlobalPosition = 0;
        ui32 NextGlobalPosition = 0;
        ui32 NextLocalPosition = 0;
        ui32 FinishGlobalPosition = 0;
        void InitArrays(const ui32 position) {
            AFL_VERIFY(!Chunk || CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + Chunk->GetFinishPosition() <= position);
            Chunk = &CurrentSparsedArray->GetSparsedChunk(CurrentOwnedArray->GetAddress().GetLocalIndex(position));
            AFL_VERIFY(Chunk->GetRecordsCount());
            AFL_VERIFY(CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + Chunk->GetStartPosition() <= position && 
                    position < CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + Chunk->GetFinishPosition())
            ("pos", position)("start", Chunk->GetStartPosition())("finish", Chunk->GetFinishPosition())(
                "shift", CurrentOwnedArray->GetAddress().GetGlobalStartPosition());
            ChunkStartGlobalPosition = CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + Chunk->GetStartPosition();
            NextGlobalPosition = CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + Chunk->GetFirstIndexNotDefault();
            NextLocalPosition = 0;
            FinishGlobalPosition = CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + Chunk->GetFinishPosition();
        }

    public:
        std::optional<ui32> MoveToSignificant(const ui32 currentGlobalPosition, const TColumnMergeContext& /*context*/) {
            while (true) {
                if (NextGlobalPosition == CurrentOwnedArray->GetAddress().GetGlobalFinishPosition()) {
                    return {};
                }
                if (NextGlobalPosition == FinishGlobalPosition) {
                    InitArrays(NextGlobalPosition);
                    continue;
                }
                if (currentGlobalPosition == NextGlobalPosition) {
                    return NextGlobalPosition;
                }
                for (; NextLocalPosition < Chunk->GetNotDefaultRecordsCount(); ++NextLocalPosition) {
                    NextGlobalPosition = ChunkStartGlobalPosition + Chunk->GetIndexUnsafeFast(NextLocalPosition);
                    if (currentGlobalPosition <= NextGlobalPosition) {
                        return NextGlobalPosition;
                    }
                }
                NextGlobalPosition = FinishGlobalPosition;
            }
        }
        bool AddIndexTo(const ui32 index, TWriter& writer);
        TSparsedChunkCursor(const std::shared_ptr<NArrow::NAccessor::TSparsedArray>& sparsed,
            const NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress* currentOwnedArray)
            : CurrentSparsedArray(sparsed)
            , CurrentOwnedArray(currentOwnedArray) {
            AFL_VERIFY(sparsed);
            AFL_VERIFY(currentOwnedArray);
            InitArrays(CurrentOwnedArray->GetAddress().GetGlobalStartPosition());
        }
    };

    class TCursor {
    private:
        std::shared_ptr<NArrow::NAccessor::IChunkedArray> Array;
        std::optional<NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress> CurrentOwnedArray;
        std::shared_ptr<TSparsedChunkCursor> SparsedCursor;
        std::shared_ptr<TPlainChunkCursor> PlainCursor;
        ui32 FinishGlobalPosition = 0;
        const TColumnMergeContext& Context;
        void InitArrays(const ui32 position);

    public:
        TCursor(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& array, const TColumnMergeContext& context)
            : Array(array)
            , Context(context) {
            AFL_VERIFY(Array);
            AFL_VERIFY(Array->GetRecordsCount());
            InitArrays(0);
        }

        ui32 GetRecordsCount() const {
            return Array->GetRecordsCount();
        }

        ui32 MoveToSignificant(const ui32 start) {
            ui32 currentPosition = start;
            while (true) {
                std::optional<ui32> significantIndex;
                if (SparsedCursor) {
                    significantIndex = SparsedCursor->MoveToSignificant(currentPosition, Context);
                } else if (PlainCursor) {
                    significantIndex = PlainCursor->MoveToSignificant(currentPosition, Context);
                }
                if (significantIndex) {
                    return *significantIndex;
                }
                if (FinishGlobalPosition == Array->GetRecordsCount()) {
                    return FinishGlobalPosition;
                } else {
                    InitArrays(FinishGlobalPosition);
                }
            }
        }

        bool AddIndexTo(const ui32 index, TWriter& writer);
    };

    class TCursorPosition: TMoveOnly {
    private:
        TCursor* Cursor;
        ui32 CurrentIndex = 0;
        const std::vector<TMergingContext::TAddress>* GlobalSequence = nullptr;

        bool InitPosition(const ui32 start) {
            CurrentIndex = start;
            while (true) {
                CurrentIndex = Cursor->MoveToSignificant(CurrentIndex);
                if (CurrentIndex == GlobalSequence->size()) {
                    return false;
                }
                if ((*GlobalSequence)[CurrentIndex].GetGlobalPosition() != -1) {
                    return true;
                }
                if (++CurrentIndex == GlobalSequence->size()) {
                    return false;
                }
            }
        }

    public:
        TCursor* operator->() {
            return Cursor;
        }

        void AddIndexTo(TWriter& writer) const {
            AFL_VERIFY(Cursor->AddIndexTo(CurrentIndex, writer));
        }

        TCursorPosition(TCursor* cursor, const std::vector<TMergingContext::TAddress>& globalSequence)
            : Cursor(cursor)
            , GlobalSequence(&globalSequence) {
            AFL_VERIFY(GlobalSequence->size() == cursor->GetRecordsCount());
            InitPosition(0);
        }

        bool IsFinished() const {
            AFL_VERIFY(CurrentIndex <= GlobalSequence->size());
            return CurrentIndex == GlobalSequence->size();
        }

        ui32 GetCurrentGlobalPosition() const {
            AFL_VERIFY(!IsFinished());
            const TMergingContext::TAddress& result = (*GlobalSequence)[CurrentIndex];
            AFL_VERIFY(result.IsValid());
            return (ui32)result.GetGlobalPosition();
        }

        i32 GetCurrentGlobalChunkIdx() const {
            AFL_VERIFY(!IsFinished());
            const TMergingContext::TAddress& result = (*GlobalSequence)[CurrentIndex];
            AFL_VERIFY(result.IsValid());
            return result.GetChunkIdx();
        }

        const TMergingContext::TAddress& GetCurrentAddress() const {
            AFL_VERIFY(!IsFinished());
            const TMergingContext::TAddress& result = (*GlobalSequence)[CurrentIndex];
            AFL_VERIFY(result.IsValid());
            return result;
        }

        bool operator<(const TCursorPosition& item) const {
            return item.GetCurrentAddress() < GetCurrentAddress();
        }

        [[nodiscard]] bool Next() {
            return InitPosition(++CurrentIndex);
        }
    };

    std::deque<TCursor> Cursors;
    std::list<TCursorPosition> CursorPositions;

    virtual void DoStart(
        const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext) override;

    virtual std::vector<TColumnPortionResult> DoExecute(const TChunkMergeContext& context, TMergingContext& mergeContext) override;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NCompaction
