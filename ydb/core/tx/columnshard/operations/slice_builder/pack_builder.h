#pragma once
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/library/signals/object_counter.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/operations/common/context.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NOlap::NWritingPortions {

class TWriteUnit: public NColumnShard::TMonitoringObjectsCounter<TWriteUnit> {
private:
    YDB_READONLY_DEF(std::shared_ptr<NEvWrite::TWriteData>, Data);
    YDB_READONLY_DEF(NArrow::TContainerWithIndexes<arrow::RecordBatch>, Batch);

public:
    TWriteUnit(const std::shared_ptr<NEvWrite::TWriteData>& data, const NArrow::TContainerWithIndexes<arrow::RecordBatch>& batch)
        : Data(data)
        , Batch(batch) {
        Data->MutableWriteMeta().OnStage(NEvWrite::EWriteStage::WaitFlush);
        AFL_VERIFY(Batch.HasContainer());
    }
};

class TBuildPackSlicesTask: public NConveyor::ITask, public NColumnShard::TMonitoringObjectsCounter<TBuildPackSlicesTask> {
private:
    const TInternalPathId PathId;
    const ui64 TabletId;
    const NEvWrite::EModificationType ModificationType;
    const std::vector<TWriteUnit> WriteUnits;
    const NOlap::TWritingContext Context;
    std::optional<std::vector<NArrow::TSerializedBatch>> BuildSlices();

protected:
    virtual void DoExecute(const std::shared_ptr<ITask>& taskPtr) override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "Write::ConstructBlobs::PackSlices";
    }

    TBuildPackSlicesTask(std::vector<TWriteUnit>&& writeUnits, const NOlap::TWritingContext& context, const TInternalPathId pathId, const ui64 tabletId,
        const NEvWrite::EModificationType modificationType)
        : PathId(pathId)
        , TabletId(tabletId)
        , ModificationType(modificationType)
        , WriteUnits(std::move(writeUnits))
        , Context(context) {
        AFL_VERIFY(WriteUnits.size());
        for (auto&& i : WriteUnits) {
            i.GetData()->MutableWriteMeta().OnStage(NEvWrite::EWriteStage::PackSlicesConstruction);
        }
    }
};
}   // namespace NKikimr::NOlap::NWritingPortions
