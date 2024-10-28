#pragma once
#include <ydb/core/tx/columnshard/engines/changes/compaction/common/context.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/common/result.h>

namespace NKikimr::NOlap::NCompaction {

class TMergingChunkContext {
private:
    std::shared_ptr<arrow::UInt16Array> IdxArray;
    std::shared_ptr<arrow::UInt32Array> RecordIdxArray;

public:
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
        : InputContainers(inputContainers)
    {
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
        return DoExecute(context, mergeContext);
    }
};

}   // namespace NKikimr::NOlap::NCompaction
