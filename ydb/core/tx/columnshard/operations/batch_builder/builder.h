#pragma once
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/counters/columnshard.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NOlap {

class TBuildBatchesTask: public NConveyor::ITask {
private:
    NEvWrite::TWriteData WriteData;
    const ui64 TabletId;
    const NActors::TActorId ParentActorId;
    const NActors::TActorId BufferActorId;
    const std::shared_ptr<ISnapshotSchema> ActualSchema;
    const TSnapshot ActualSnapshot;
    const std::shared_ptr<NColumnShard::TWriteCounters> WritingCounters;
    void ReplyError(const TString& message, const NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass errorClass);

protected:
    virtual TConclusionStatus DoExecute(const std::shared_ptr<ITask>& taskPtr) override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "Write::ConstructBatches";
    }

    TBuildBatchesTask(const ui64 tabletId, const NActors::TActorId parentActorId, const NActors::TActorId bufferActorId,
        NEvWrite::TWriteData&& writeData, const std::shared_ptr<ISnapshotSchema>& actualSchema, const TSnapshot& actualSnapshot,
        const std::shared_ptr<NColumnShard::TWriteCounters>& writingCounters)
        : WriteData(std::move(writeData))
        , TabletId(tabletId)
        , ParentActorId(parentActorId)
        , BufferActorId(bufferActorId)
        , ActualSchema(actualSchema)
        , ActualSnapshot(actualSnapshot)
        , WritingCounters(writingCounters)
    {
    }
};
}  // namespace NKikimr::NOlap
