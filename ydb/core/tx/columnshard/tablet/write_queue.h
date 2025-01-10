#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/operations/write.h>
#include <ydb/core/tx/data_events/common/modification_type.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
class TArrowData;
class TWriteTask: TMoveOnly {
private:
    std::shared_ptr<TArrowData> ArrowData;
    NOlap::ISnapshotSchema::TPtr Schema;
    const NActors::TActorId SourceId;
    const std::optional<ui32> GranuleShardingVersionId;
    const ui64 PathId;
    const ui64 Cookie;
    const ui64 LockId;
    const NEvWrite::EModificationType ModificationType;
    const EOperationBehaviour Behaviour;
    const TMonotonic Created = TMonotonic::Now();

public:
    TWriteTask(const std::shared_ptr<TArrowData>& arrowData, const NOlap::ISnapshotSchema::TPtr& schema, const NActors::TActorId sourceId,
        const std::optional<ui32>& granuleShardingVersionId, const ui64 pathId, const ui64 cookie, const ui64 lockId,
        const NEvWrite::EModificationType modificationType, const EOperationBehaviour behaviour)
        : ArrowData(arrowData)
        , Schema(schema)
        , SourceId(sourceId)
        , GranuleShardingVersionId(granuleShardingVersionId)
        , PathId(pathId)
        , Cookie(cookie)
        , LockId(lockId)
        , ModificationType(modificationType)
        , Behaviour(behaviour) {
    }

    const TMonotonic& GetCreatedMonotonic() const {
        return Created;
    }

    bool Execute(TColumnShard* owner, const TActorContext& ctx);
};

class TWriteTasksQueue {
private:
    bool WriteTasksOverloadCheckerScheduled = false;
    std::deque<TWriteTask> WriteTasks;
    i64 PredWriteTasksSize = 0;
    TColumnShard* Owner;

public:
    TWriteTasksQueue(TColumnShard* owner)
        : Owner(owner) {
    }

    ~TWriteTasksQueue();

    void Enqueue(TWriteTask&& task);
    bool Drain(const bool onWakeup, const TActorContext& ctx);
};

}   // namespace NKikimr::NColumnShard
