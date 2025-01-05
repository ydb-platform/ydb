#pragma once
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/operations/common/context.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NOlap {

class TBuildPackSlicesTask: public NConveyor::ITask, public NColumnShard::TMonitoringObjectsCounter<TBuildSlicesTask> {
private:
    const ui64 PathId;
    const EModificationType ModificationType;
    const std::vector<TWriteUnit> WriteUnits;
    const TWritingContext Context;
    std::optional<std::vector<NArrow::TSerializedBatch>> BuildSlices();
    void ReplyError(const TString& message, const NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass errorClass);

protected:
    virtual TConclusionStatus DoExecute(const std::shared_ptr<ITask>& taskPtr) override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "Write::ConstructBlobs::PackSlices";
    }

    TBuildPackSlicesTask(std::vector<TWriteUnit>&& writeUnits, const TWritingContext& context, const ui64 pathId, const EModificationType modificationType)
        : PathId(pathId)
        , ModificationType(modificationType)
        , WriteUnits(std::move(writeUnits))
        , Context(context) {
    }
};
}   // namespace NKikimr::NOlap
