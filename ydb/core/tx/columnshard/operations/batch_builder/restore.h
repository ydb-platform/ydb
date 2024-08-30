#pragma once
#include "merger.h"

#include <ydb/core/tx/columnshard/data_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>

namespace NKikimr::NOlap {

class TModificationRestoreTask: public NDataReader::IRestoreTask {
private:
    using TBase = NDataReader::IRestoreTask;
    NEvWrite::TWriteData WriteData;
    const ui64 TabletId;
    const NActors::TActorId ParentActorId;
    const NActors::TActorId BufferActorId;
    std::shared_ptr<IMerger> Merger;
    const std::shared_ptr<ISnapshotSchema> ActualSchema;
    const ui64 LocalPathId;
    const TSnapshot Snapshot;
    std::shared_ptr<arrow::RecordBatch> IncomingData;
    virtual std::unique_ptr<TEvColumnShard::TEvInternalScan> DoBuildRequestInitiator() const override;

    virtual TConclusionStatus DoOnDataChunk(const std::shared_ptr<arrow::Table>& data) override;
    virtual TConclusionStatus DoOnFinished() override;
    virtual void DoOnError(const TString& errorMessage) override;
    void SendErrorMessage(const TString& errorMessage);

public:
    TModificationRestoreTask(const ui64 tabletId, const NActors::TActorId parentActorId,
        const NActors::TActorId bufferActorId, NEvWrite::TWriteData&& writeData, const std::shared_ptr<IMerger>& merger,
        const std::shared_ptr<ISnapshotSchema>& actualSchema, const TSnapshot actualSnapshot, const std::shared_ptr<arrow::RecordBatch>& incomingData);
};

}
