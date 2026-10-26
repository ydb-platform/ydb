#include "controller_impl.h"

#include <util/generic/algorithm.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

bool TController::TracksSourceOffsets(const TWorkerId& id) const {
    const auto replication = Find(id.ReplicationId());
    const auto* target = FindTarget(id);
    return replication && target
        && replication->GetConfig().GetConsistencySettings().HasGlobal()
        && target->GetKind() != TReplication::ETargetKind::Transfer;
}

bool TController::HasSourceOffsetMismatchError(const TReplication& replication) const {
    return replication.GetState() == TReplication::EState::Error
        && ReplicationsWithSourceOffsetMismatch.contains(replication.GetId());
}

void TController::DeleteProcessedSourceOffsets(ui64 replicationId, ui64 targetId, NIceDb::TNiceDb& db) {
    for (auto it = ProcessedSourceOffsets.begin(); it != ProcessedSourceOffsets.end();) {
        const auto& id = it->first;
        if (id.ReplicationId() == replicationId && id.TargetId() == targetId) {
            db.Table<Schema::ProcessedSourceOffsets>().Key(replicationId, targetId, id.WorkerId()).Delete();
            ProcessedSourceOffsets.erase(it++);
        } else {
            ++it;
        }
    }
}

class TController::TTxSourceOffsetReport: public TTxBase {
    TEvService::TEvSourceOffsetReport::TPtr Event;
    TWorkerId Id;
    TMaybe<ui64> PersistedNextOffset;
    bool Accepted = false;

public:
    TTxSourceOffsetReport(TController* self, TEvService::TEvSourceOffsetReport::TPtr& ev)
        : TTxBase("TxSourceOffsetReport", self)
        , Event(ev)
        , Id(TWorkerId::Parse(Event->Get()->Record.GetWorker()))
    {}

    TTxType GetTxType() const override { return TXTYPE_SOURCE_OFFSET_REPORT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Event->Get()->Record;
        if (!Self->HasWorkerSession(Id, Event->Sender.NodeId())
            || !Self->TracksSourceOffsets(Id)
            || Self->ReplicationsWithSourceOffsetMismatch.contains(Id.ReplicationId())
            || !request.HasNextOffset())
        {
            return true;
        }
        const auto* savedOffset = Self->ProcessedSourceOffsets.FindPtr(Id);
        if (!savedOffset || request.GetNextOffset() > savedOffset->NextOffset) {
            PersistedNextOffset = request.GetNextOffset();
            Self->ProcessedSourceOffsets[Id].NextOffset = *PersistedNextOffset;
            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::ProcessedSourceOffsets>().Key(Id.ReplicationId(), Id.TargetId(), Id.WorkerId()).Update(
                NIceDb::TUpdate<Schema::ProcessedSourceOffsets::NextOffset>(request.GetNextOffset()));
        }
        Accepted = true;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!Accepted) {
            return;
        }
        if (PersistedNextOffset) {
            if (auto* savedOffset = Self->ProcessedSourceOffsets.FindPtr(Id)) {
                savedOffset->PersistedNextOffset = Max(savedOffset->PersistedNextOffset.GetOrElse(0), *PersistedNextOffset);
            }
        }
        if (!Self->ReplicationsWithSourceOffsetMismatch.contains(Id.ReplicationId())) {
            ctx.Send(Event->Sender, new TEvService::TEvSourceOffsetResult(), 0, Event->Cookie);
        }
    }
};

void TController::StopWorkersForMismatch(ui64 replicationId, const TActorContext& ctx) {
    for (auto it = IndexReadyActors.begin(); it != IndexReadyActors.end();) {
        if (it->first.first == replicationId) {
            ctx.Send(it->second, new TEvents::TEvPoison());
            IndexReadyActors.erase(it++);
        } else {
            ++it;
        }
    }
    for (const auto& [key, _] : SchemaBarriers) {
        if (key.first == replicationId) {
            StopSchemaChangeDstAlter(key, ctx);
        }
    }
    for (const auto& [id, worker] : Workers) {
        if (id.ReplicationId() == replicationId && worker.HasSession()) {
            StopQueue.emplace(id, worker.GetSession());
        }
    }
    ScheduleProcessQueues();
}

void TController::Handle(TEvService::TEvSourceOffsetReport::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxSourceOffsetReport(this, ev), ctx);
}

}
