#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/operations/write.h>
#include <ydb/core/tx/data_events/common/modification_type.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
class TArrowData;
class TWriteTask: public TMoveOnly {
private:
    std::shared_ptr<TArrowData> ArrowData;
    NOlap::ISnapshotSchema::TPtr Schema;
    static inline TAtomicCounter Counter = 0;
    const ui64 TaskId = Counter.Inc();
    const NActors::TActorId SourceId;
    const NActors::TActorId RecipientId;
    const std::optional<ui32> GranuleShardingVersionId;
    const TUnifiedPathId PathId;
    const ui64 Cookie;
    const NOlap::TSnapshot MvccSnapshot;
    const ui64 LockId;
    const NEvWrite::EModificationType ModificationType;
    const EOperationBehaviour Behaviour;
    const TMonotonic Created = TMonotonic::Now();
    const std::optional<TDuration> Timeout;
    const ui64 TxId;
    const bool IsBulk;
    const std::optional<ui64> OverloadSubscribeSeqNo;

public:
    bool operator<(const TWriteTask& item) const {
        return std::tie(Created, PathId.GetInternalPathId(), TaskId) < std::tie(item.Created, item.PathId.GetInternalPathId(), item.TaskId);
    }

    bool IsDeprecated(const TMonotonic now) const {
        return Timeout ? (Created + *Timeout <= now) : false;
    }

    TWriteTask(const std::shared_ptr<TArrowData>& arrowData, const NOlap::ISnapshotSchema::TPtr& schema, const NActors::TActorId sourceId, const NActors::TActorId recipientId,
        const std::optional<ui32>& granuleShardingVersionId, const TUnifiedPathId pathId, const ui64 cookie, const NOlap::TSnapshot& mvccSnapshot, const ui64 lockId,
        const NEvWrite::EModificationType modificationType, const EOperationBehaviour behaviour, const std::optional<TDuration> timeout, const ui64 txId, const bool isBulk, const std::optional<ui64>& overloadSubscribeSeqNo)
        : ArrowData(arrowData)
        , Schema(schema)
        , SourceId(sourceId)
        , RecipientId(recipientId)
        , GranuleShardingVersionId(granuleShardingVersionId)
        , PathId(pathId)
        , Cookie(cookie)
        , MvccSnapshot(mvccSnapshot)
        , LockId(lockId)
        , ModificationType(modificationType)
        , Behaviour(behaviour)
        , Timeout(timeout)
        , TxId(txId)
        , IsBulk(isBulk)
        , OverloadSubscribeSeqNo(overloadSubscribeSeqNo)
    {
    }

    const TInternalPathId& GetInternalPathId() const {
        return PathId.InternalPathId;
    }

    const TMonotonic& GetCreatedMonotonic() const {
        return Created;
    }

    bool Execute(TColumnShard* owner, const TActorContext& ctx) const;
    void Abort(TColumnShard* owner, const TString& reason, const TActorContext& ctx, const NKikimrDataEvents::TEvWriteResult::EStatus& status = NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR) const;
};

class TWriteTasksQueue {
private:
    bool WriteTasksOverloadCheckerScheduled = false;
    std::set<TWriteTask> WriteTasks;
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
