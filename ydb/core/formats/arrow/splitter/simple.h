#pragma once
#include <ydb/library/accessor/accessor.h>
#include "stats.h"
#include "scheme_info.h"

namespace NKikimr::NArrow::NSplitter {

class TSaverSplittedChunk {
private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, SlicedBatch);
    YDB_READONLY_DEF(TString, SerializedChunk);
public:
    ui32 GetRecordsCount() const {
        return SlicedBatch->num_rows();
    }

    TSaverSplittedChunk(const std::shared_ptr<arrow::RecordBatch>& batch, TString&& serializedChunk)
        : SlicedBatch(batch)
        , SerializedChunk(std::move(serializedChunk)) {
        Y_ABORT_UNLESS(SlicedBatch);
        Y_ABORT_UNLESS(SlicedBatch->num_rows());
    }
};

class TLinearSplitInfo {
private:
    YDB_READONLY(ui64, PacksCount, 0);
    YDB_READONLY(ui64, PackSize, 0);
    YDB_READONLY(ui64, ObjectsCount, 0);
public:
    bool IsMinimalGranularity() const {
        return PackSize == 1;
    }

    TLinearSplitInfo(const ui64 packsCount, const ui64 packSize, const ui64 objectsCount)
        : PacksCount(packsCount)
        , PackSize(packSize)
        , ObjectsCount(objectsCount)
    {
        AFL_VERIFY(objectsCount >= packsCount)("objects_count", objectsCount)("packs_count", packsCount);
        AFL_VERIFY(PackSize);
        AFL_VERIFY(PacksCount);
    }

    class TIterator {
    private:
        const TLinearSplitInfo& Owner;
        YDB_READONLY(ui64, Position, 0);
        YDB_READONLY(ui64, CurrentPackSize, 0);
        ui64 PackIdx = 0;
        void InitPack() {
            CurrentPackSize = (PackIdx + 1 == Owner.GetPacksCount()) ? Owner.ObjectsCount - Position : Owner.GetPackSize();
        }
    public:
        explicit TIterator(const TLinearSplitInfo& owner)
            : Owner(owner)
        {
            InitPack();
        }

        bool IsValid() const {
            if (Position < Owner.GetObjectsCount() && PackIdx < Owner.GetPacksCount()) {
                return true;
            } else {
                Y_ABORT_UNLESS(Position == Owner.GetObjectsCount() && PackIdx == Owner.GetPacksCount());
                return false;
            }
        }

        bool Next() {
            Y_ABORT_UNLESS(IsValid());
            Position += CurrentPackSize;
            ++PackIdx;
            InitPack();
            return IsValid();
        }
    };

    TIterator StartIterator() const {
        return TIterator(*this);
    }
};

class TSimpleSplitter {
private:
    NAccessor::TColumnSaver ColumnSaver;
    YDB_ACCESSOR_DEF(std::optional<TBatchSerializationStat>, Stats);
public:
    explicit TSimpleSplitter(const NAccessor::TColumnSaver& columnSaver)
        : ColumnSaver(columnSaver)
    {

    }

    static TLinearSplitInfo GetOptimalLinearSplitting(const ui64 objectsCount, const i64 optimalPackSizeExt) {
        const i64 optimalPackSize = optimalPackSizeExt ? optimalPackSizeExt : 1;
        const ui32 countPacksMax = std::max<ui32>(1, (ui32)floor(1.0 * objectsCount / optimalPackSize));
        const ui32 countPacksMin = std::max<ui32>(1, (ui32)ceil(1.0 * objectsCount / optimalPackSize));
        const ui32 stepPackMax = objectsCount / countPacksMin;
        const ui32 stepPackMin = objectsCount / countPacksMax;
        if (std::abs(optimalPackSize - stepPackMax) > std::abs(optimalPackSize - stepPackMin)) {
            return TLinearSplitInfo(countPacksMax, stepPackMin, objectsCount);
        } else {
            return TLinearSplitInfo(countPacksMin, stepPackMax, objectsCount);
        }
    }

    static TLinearSplitInfo GetLinearSplittingByMax(const ui64 objectsCount, const ui64 maxPackSizeExt) {
        const ui64 maxPackSize = maxPackSizeExt ? maxPackSizeExt : 1;
        const ui32 countPacksMax = std::max<ui32>(1, (ui32)floor(1.0 * objectsCount / maxPackSize));
        const ui32 stepPackMin = objectsCount / countPacksMax;
        return TLinearSplitInfo(countPacksMax, stepPackMin, objectsCount);
    }

    std::vector<TSaverSplittedChunk> Split(const std::shared_ptr<arrow::Array>& data, const std::shared_ptr<arrow::Field>& field, const ui32 maxBlobSize) const;
    std::vector<TSaverSplittedChunk> Split(const std::shared_ptr<arrow::RecordBatch>& data, const ui32 maxBlobSize) const;
    std::vector<TSaverSplittedChunk> SplitByRecordsCount(std::shared_ptr<arrow::RecordBatch> data, const std::vector<ui64>& recordsCount) const;
    std::vector<TSaverSplittedChunk> SplitBySizes(std::shared_ptr<arrow::RecordBatch> data, const TString& dataSerialization, const std::vector<ui64>& splitPartSizesExt) const;
};

}
