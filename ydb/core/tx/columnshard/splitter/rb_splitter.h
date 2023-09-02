#pragma once
#include "batch_slice.h"
#include "stats.h"
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {

class TSimilarSlicer {
private:
    const ui64 BottomLimit = 0;
public:
    TSimilarSlicer(const ui64 bottomLimit)
        : BottomLimit(bottomLimit) {

    }

    template <class TObject>
    std::vector<TVectorView<TObject>> Split(std::vector<TObject>& objects) {
        ui64 fullSize = 0;
        for (auto&& i : objects) {
            fullSize += i.GetSize();
        }
        if (fullSize <= BottomLimit) {
            return {TVectorView<TObject>(objects.begin(), objects.end())};
        }
        ui64 currentSize = 0;
        ui64 currentStart = 0;
        std::vector<TVectorView<TObject>> result;
        for (ui32 i = 0; i < objects.size(); ++i) {
            const ui64 nextSize = currentSize + objects[i].GetSize();
            const ui64 nextOtherSize = fullSize - nextSize;
            if ((nextSize >= BottomLimit && nextOtherSize >= BottomLimit) || (i + 1 == objects.size())) {
                result.emplace_back(TVectorView<TObject>(objects.begin() + currentStart, objects.begin() + i + 1));
                currentSize = 0;
                currentStart = i + 1;
            } else {
                currentSize = nextSize;
            }
        }
        return result;
    }
};

class TRBSplitLimiter {
private:
    std::deque<TBatchSerializedSlice> Slices;
    std::shared_ptr<NColumnShard::TSplitterCounters> Counters;
    std::shared_ptr<arrow::RecordBatch> Batch;
    TSplitSettings Settings;
public:
    TRBSplitLimiter(std::shared_ptr<NColumnShard::TSplitterCounters> counters,
        ISchemaDetailInfo::TPtr schemaInfo, const std::shared_ptr<arrow::RecordBatch> batch, const TSplitSettings& settings);

    bool Next(std::vector<std::vector<IPortionColumnChunk::TPtr>>& portionBlobs, std::shared_ptr<arrow::RecordBatch>& batch);
};

}
