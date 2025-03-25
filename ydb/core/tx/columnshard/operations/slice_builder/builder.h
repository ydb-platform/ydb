#pragma once
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/operations/common/context.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NOlap {

class TBuildSlicesTask: public NConveyor::ITask, public NColumnShard::TMonitoringObjectsCounter<TBuildSlicesTask> {
private:
    NEvWrite::TWriteData WriteData;
    const ui64 TabletId;
    std::shared_ptr<arrow::RecordBatch> OriginalBatch;
    std::optional<std::vector<NArrow::TSerializedBatch>> BuildSlices();
    const TWritingContext Context;
    void ReplyError(const TString& message, const NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass errorClass);

protected:
    virtual TConclusionStatus DoExecute(const std::shared_ptr<ITask>& taskPtr) override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "Write::ConstructBlobs::Slices";
    }

    TBuildSlicesTask(NEvWrite::TWriteData&& writeData, const std::shared_ptr<arrow::RecordBatch>& batch,
        const TWritingContext& context)
        : WriteData(std::move(writeData))
        , TabletId(context.GetTabletId())
        , OriginalBatch(batch)
        , Context(context) {
        WriteData.MutableWriteMeta().OnStage(NEvWrite::EWriteStage::BuildSlices);
    }
};
}   // namespace NKikimr::NOlap
