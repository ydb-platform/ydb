#pragma once
#include "batch.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NIndexedReader {

class TGranule {
private:
    YDB_READONLY(ui64, GranuleId, 0);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::RecordBatch>>, ReadyBatches);
    YDB_FLAG_ACCESSOR(DuplicationsAvailable, false);
    YDB_READONLY_FLAG(Ready, false);
    THashMap<ui32, TBatch> Batches;
    std::set<ui32> WaitBatches;
    TIndexedReadData* Owner = nullptr;
    void OnBatchReady(const TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch);
    friend class NIndexedReader::TBatch;
public:
    TGranule(const ui64 granuleId, TIndexedReadData& owner)
        : GranuleId(granuleId)
        , Owner(&owner) {

    }

    TBatch& AddBatch(const ui32 batchNo, const TPortionInfo& portionInfo);
};

}
