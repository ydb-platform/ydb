#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/data_events/write_data.h>
#include <ydb/core/formats/arrow/size_calcer.h>

namespace NKikimr::NOlap {

class TBuildSlicesTask: public NConveyor::ITask {
private:
    NEvWrite::TWriteData WriteData;
    const ui64 TabletId;
    const NActors::TActorId ParentActorId;
    const NActors::TActorId BufferActorId;
    std::shared_ptr<arrow::RecordBatch> OriginalBatch;
    std::optional<std::vector<NArrow::TSerializedBatch>> BuildSlices();
    const std::shared_ptr<ISnapshotSchema> ActualSchema;
    void ReplyError(const TString& message);
protected:
    virtual TConclusionStatus DoExecute(const std::shared_ptr<ITask>& taskPtr) override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "Write::ConstructBlobs::Slices";
    }

    TBuildSlicesTask(const ui64 tabletId, const NActors::TActorId parentActorId,
        const NActors::TActorId bufferActorId, NEvWrite::TWriteData&& writeData, const std::shared_ptr<arrow::RecordBatch>& batch,
        const std::shared_ptr<ISnapshotSchema>& actualSchema)
        : WriteData(std::move(writeData))
        , TabletId(tabletId)
        , ParentActorId(parentActorId)
        , BufferActorId(bufferActorId)
        , OriginalBatch(batch)
        , ActualSchema(actualSchema)
    {
    }
};
}
