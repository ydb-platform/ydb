#pragma once
#include <ydb/core/tx/columnshard/engines/changes/compaction/common/context.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/common/result.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction {

class TMergingChunkContext {
private:
    std::shared_ptr<arrow::UInt16Array> IdxArray;
    std::shared_ptr<arrow::UInt32Array> RecordIdxArray;

public:
    ui32 GetRecordsCount() const {
        return IdxArray->length();
    }

    const arrow::UInt16Array& GetIdxArray() const {
        return *IdxArray;
    }
    const arrow::UInt32Array& GetRecordIdxArray() const {
        return *RecordIdxArray;
    }

    TMergingChunkContext(const std::shared_ptr<arrow::RecordBatch>& pkAndAddresses);
};

class TMergingContext {
public:
    class TAddress {
    private:
        YDB_ACCESSOR(i32, ChunkIdx, -1);
        YDB_ACCESSOR(i32, GlobalPosition, -1);

    public:
        TAddress() = default;
        bool operator<(const TAddress& item) const {
            if (ChunkIdx < item.ChunkIdx) {
                return true;
            } else if (item.ChunkIdx < ChunkIdx) {
                return false;
            } else {
                return GlobalPosition < item.GlobalPosition;
            }
        }

        bool IsValid() const {
            return ChunkIdx >= 0 && GlobalPosition >= 0;
        }
    };

private:
    YDB_READONLY_DEF(std::vector<TMergingChunkContext>, Chunks);
    std::vector<std::shared_ptr<NArrow::TGeneralContainer>> InputContainers;

    std::optional<std::vector<std::vector<TAddress>>> RemapPortionIndexToResultIndex;

public:
    const TMergingChunkContext& GetChunk(const ui32 idx) const {
        AFL_VERIFY(idx < Chunks.size());
        return Chunks[idx];
    }

    bool HasRemapInfo(const ui32 idx) {
        return GetRemapPortionIndexToResultIndex(idx).size();
    }

    const std::vector<std::vector<TAddress>>& GetRemapPortionIndexToResultIndex() {
        if (!RemapPortionIndexToResultIndex) {
            std::vector<std::vector<TAddress>> result;
            result.resize(InputContainers.size());
            {
                ui32 idx = 0;
                for (auto&& p : InputContainers) {
                    if (p) {
                        result[idx].resize(p->GetRecordsCount());
                    }
                    ++idx;
                }
            }
            ui32 chunkIdx = 0;
            for (auto&& i : Chunks) {
                auto& pIdxArray = i.GetIdxArray();
                auto& pRecordIdxArray = i.GetRecordIdxArray();
                for (ui32 recordIdx = 0; recordIdx < i.GetIdxArray().length(); ++recordIdx) {
                    auto& sourceRemap = result[pIdxArray.Value(recordIdx)];
                    if (sourceRemap.size()) {
                        sourceRemap[pRecordIdxArray.Value(recordIdx)].SetChunkIdx(chunkIdx);
                        sourceRemap[pRecordIdxArray.Value(recordIdx)].SetGlobalPosition(recordIdx);
                    }
                }
                ++chunkIdx;
            }
            RemapPortionIndexToResultIndex = std::move(result);
        }
        return *RemapPortionIndexToResultIndex;
    }

    const std::vector<TAddress>& GetRemapPortionIndexToResultIndex(const ui32 idx) {
        auto& result = GetRemapPortionIndexToResultIndex();
        AFL_VERIFY(idx < result.size());
        return result[idx];
    }

    TMergingContext(const std::vector<std::shared_ptr<arrow::RecordBatch>>& pkAndAddresses,
        const std::vector<std::shared_ptr<NArrow::TGeneralContainer>>& inputContainers)
        : InputContainers(inputContainers) {
        for (auto&& i : pkAndAddresses) {
            Chunks.emplace_back(i);
        }
    }
};

class IColumnMerger {
public:
    using TFactory = NObjectFactory::TParametrizedObjectFactory<IColumnMerger, TString, const TColumnMergeContext&>;

private:
    bool Started = false;

    virtual std::vector<TColumnPortionResult> DoExecute(const TChunkMergeContext& context, TMergingContext& mergeContext) = 0;
    virtual void DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext) = 0;

protected:
    const TColumnMergeContext& Context;

public:
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
            if (Input) {
                if (globalPosition == Input->GetRecordsCount()) {
                    GlobalPosition = globalPosition;
                    return;
                }
                AFL_VERIFY(globalPosition < Input->GetRecordsCount())("pos", globalPosition)("size", Input->GetRecordsCount());
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
            } else {
                AFL_VERIFY(globalPosition == 0);
                GlobalPosition = globalPosition;
            }
        }

    public:
        bool IsEmpty() const {
            return !Input;
        }

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

    template <class TArrayImpl, class TConstructorImpl>
    class TPortionColumnChunkWriter: public TColumnPortionResult {
    private:
        using TBase = TColumnPortionResult;
        const TConstructorImpl Constructor;

    public:
        TPortionColumnChunkWriter(const TConstructorImpl& constructor, const ui32 columnId)
            : TBase(columnId)
            , Constructor(constructor) {
        }

        void AddChunk(const std::shared_ptr<TArrayImpl>& cArray, const TColumnMergeContext& cmContext) {
            AFL_VERIFY(cArray);
            AFL_VERIFY(cArray->GetRecordsCount());
            auto accContext = cmContext.GetLoader()->BuildAccessorContext(cArray->GetRecordsCount());
            Chunks.emplace_back(std::make_shared<NChunks::TChunkPreparation>(Constructor.SerializeToString(cArray, accContext), cArray,
                TChunkAddress(cmContext.GetColumnId(), Chunks.size()),
                cmContext.GetIndexInfo().GetColumnFeaturesVerified(cmContext.GetColumnId())));
        }
    };

    static inline const TString PortionIdFieldName = "$$__portion_id";
    static inline const TString PortionRecordIndexFieldName = "$$__portion_record_idx";
    static inline const std::shared_ptr<arrow::Field> PortionIdField =
        std::make_shared<arrow::Field>(PortionIdFieldName, std::make_shared<arrow::UInt16Type>());
    static inline const std::shared_ptr<arrow::Field> PortionRecordIndexField =
        std::make_shared<arrow::Field>(PortionRecordIndexFieldName, std::make_shared<arrow::UInt32Type>());

    IColumnMerger(const TColumnMergeContext& context)
        : Context(context) {
    }
    virtual ~IColumnMerger() = default;

    void Start(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext);

    std::vector<TColumnPortionResult> Execute(const TChunkMergeContext& context, TMergingContext& mergeContext) {
        const auto& chunk = mergeContext.GetChunk(context.GetBatchIdx());
        AFL_VERIFY(context.GetRecordsCount() == chunk.GetIdxArray().length());
        return DoExecute(context, mergeContext);
    }
};

}   // namespace NKikimr::NOlap::NCompaction
