#pragma once
#include "conveyor_task.h"
#include "filling_context.h"
#include "read_metadata.h"
#include <ydb/core/formats/arrow/sort_cursor.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NIndexedReader {

class TTaskGranulePreparation: public NColumnShard::IDataTasksProcessor::ITask {
private:
    using TBase = NColumnShard::IDataTasksProcessor::ITask;
    mutable std::vector<std::shared_ptr<arrow::RecordBatch>> BatchesInGranule;
    THashSet<const void*> BatchesToDedup;
    const ui64 GranuleId;
    NOlap::TReadMetadata::TConstPtr ReadMetadata;

    static std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>
        GroupInKeyRanges(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, const TIndexInfo& indexInfo);

    static std::vector<std::shared_ptr<arrow::RecordBatch>> SpecialMergeSorted(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        const TIndexInfo& indexInfo,
        const std::shared_ptr<NArrow::TSortDescription>& description,
        const THashSet<const void*>& batchesToDedup);

protected:
    virtual bool DoApply(NOlap::NIndexedReader::TGranulesFillingContext& indexedDataRead) const override;
    virtual bool DoExecuteImpl() override;
public:

    virtual TString GetTaskClassIdentifier() const override {
        return "Reading::GranulePreparation";
    }

    TTaskGranulePreparation(std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches, THashSet<const void*>&& batchesToDedup,
        const ui64 granuleId, NOlap::TReadMetadata::TConstPtr readMetadata, NColumnShard::IDataTasksProcessor::TPtr processor)
        : TBase(processor)
        , BatchesInGranule(std::move(batches))
        , BatchesToDedup(std::move(batchesToDedup))
        , GranuleId(granuleId)
        , ReadMetadata(readMetadata) {

    }
};

}
