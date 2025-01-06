#pragma once
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/operations/common/context.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NOlap::NWritingPortions {

class TWriteUnit {
private:
    YDB_READONLY_DEF(std::shared_ptr<NEvWrite::TWriteData>, Data);
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Batch);

public:
    TWriteUnit(const std::shared_ptr<NEvWrite::TWriteData>& data, const std::shared_ptr<arrow::RecordBatch>& batch)
        : Data(data)
        , Batch(batch) {
        AFL_VERIFY(Data->GetWritePortions());
        AFL_VERIFY(Batch);
    }
};

class TBuildPackSlicesTask: public NConveyor::ITask, public NColumnShard::TMonitoringObjectsCounter<TBuildPackSlicesTask> {
private:
    const ui64 PathId;
    const ui64 TabletId;
    const NEvWrite::EModificationType ModificationType;
    const std::vector<TWriteUnit> WriteUnits;
    const NOlap::TWritingContext Context;
    std::optional<std::vector<NArrow::TSerializedBatch>> BuildSlices();

protected:
    virtual TConclusionStatus DoExecute(const std::shared_ptr<ITask>& taskPtr) override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "Write::ConstructBlobs::PackSlices";
    }

    TBuildPackSlicesTask(std::vector<TWriteUnit>&& writeUnits, const NOlap::TWritingContext& context, const ui64 pathId, const ui64 tabletId,
        const NEvWrite::EModificationType modificationType)
        : PathId(pathId)
        , TabletId(tabletId)
        , ModificationType(modificationType)
        , WriteUnits(std::move(writeUnits))
        , Context(context) {
    }
};
}   // namespace NKikimr::NOlap
