#include "datashard_impl.h"

namespace NKikimr::NDataShard {

using TFreezeState = NKikimrTxDataShard::TDatabaseFreezeState;

bool TDataShard::IsDatabaseWriteBlocked(const TOperation* op) const {
    if (!IsDatabaseFrozen()) {
        return false;
    }
    // Prepared writes may already have a distributed commit decision. Keep
    // their normal plan/readset/commit protocol alive while draining.
    return !(DatabaseFreeze.GetStage() < TFreezeState::DRAIN_CDC && op && !op->IsImmediate()
        && DatabaseFreezeAcceptedTxIds.contains(op->GetTxId())
        && TransQueue.FindTxInFly(op->GetTxId()).Get() == op);
}

bool TDataShard::HasDatabaseFreezeWrites() const {
    for (const auto& [_, op] : TransQueue.GetTxsInFly()) {
        if (!op->IsReadOnly()) {
            return true;
        }
    }
    for (const auto& [_, op] : Pipeline.GetImmediateOps()) {
        if (!op->IsReadOnly()) {
            return true;
        }
    }
    return DatabaseFreezeLocalWrites
        || bool(VolatileTxManager.GetTxMap())
        || Pipeline.HasCommittingOpsBelow(TRowVersion::Max())
        || !CommittingChangeRecords.empty();
}

bool TDataShard::HasDatabaseFreezeIndexRecords() const {
    for (const auto& [_, record] : ChangesQueue) {
        const auto table = TableInfos.find(record.TableId.LocalPathId);
        if (table == TableInfos.end() || !table->second->CdcStreams.contains(record.PathId)) {
            return true;
        }
    }
    return false;
}

bool TDataShard::HasDatabaseFreezeCdcScans() const {
    if (CdcStreamScanManager.Size()) {
        return true;
    }
    for (const auto& [_, table] : TableInfos) {
        for (const auto& [streamId, stream] : table->CdcStreams) {
            Y_UNUSED(streamId);
            if (stream.State == NKikimrSchemeOp::ECdcStreamStateScan) {
                return true;
            }
        }
    }
    return false;
}

class TDataShard::TTxDatabaseFreeze : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    TEvDataShard::TEvDatabaseFreeze::TPtr Request;
    THolder<TEvDataShard::TEvDatabaseFreezeResult> Response;
    TVector<IDataShardChangeCollector::TChange> Heartbeats;
    bool Unfreezing = false;

public:
    TTxDatabaseFreeze(TDataShard* self, TEvDataShard::TEvDatabaseFreeze::TPtr ev)
        : TBase(self)
        , Request(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_DATABASE_FREEZE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;
        Response = MakeHolder<TEvDataShard::TEvDatabaseFreezeResult>();
        auto& result = Response->Record;
        result.SetEpoch(request.GetEpoch());
        result.SetTabletId(Self->TabletID());
        result.SetStage(request.GetStage());
        result.SetStatus(Ydb::StatusIds::SUCCESS);

        auto fail = [&](Ydb::StatusIds::StatusCode status, const TString& issue) {
            result.SetStatus(status);
            result.SetIssue(issue);
            result.SetReady(true);
            return true;
        };

        if (!Self->SubDomainPathId || !Self->CurrentSchemeShardId) {
            result.SetReady(false);
            return true;
        }
        if (Self->IsFollower() || request.GetSchemeShardId() != Self->CurrentSchemeShardId
            || *Self->SubDomainPathId != TPathId(request.GetDomainOwnerId(), request.GetDomainLocalPathId()))
        {
            return fail(Ydb::StatusIds::PRECONDITION_FAILED, "Database freeze owner does not match shard owner");
        }
        if (!request.GetEpoch() || !request.HasStage()
            || request.GetStage() > TFreezeState::DRAIN_CDC)
        {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Invalid database freeze request");
        }
        if (request.GetEpoch() < Self->DatabaseFreeze.GetEpoch()) {
            return fail(Ydb::StatusIds::ABORTED, "Stale database freeze epoch");
        }
        if (!Self->IsStateNewReadAllowed()) {
            result.SetReady(false);
            return true;
        }

        const auto stage = request.GetStage();
        const bool newEpoch = request.GetEpoch() > Self->DatabaseFreeze.GetEpoch();
        const bool wasFrozen = Self->IsDatabaseFrozen();
        if (newEpoch && stage > TFreezeState::FENCE_WRITES) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Database freeze must start with a write fence");
        }
        if (!newEpoch && stage != TFreezeState::OPEN && Self->DatabaseFreeze.GetStage() == TFreezeState::OPEN) {
            return fail(Ydb::StatusIds::ABORTED, "Database freeze epoch has already been opened");
        }
        if (!newEpoch && stage == TFreezeState::OPEN && Self->IsDatabaseFrozen()) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Thaw requires a new freeze epoch");
        }

        NIceDb::TNiceDb db(txc.DB);
        if (!newEpoch && stage > Self->DatabaseFreeze.GetStage() + 1) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Database freeze stages cannot be skipped");
        }
        if (stage > TFreezeState::FENCE_WRITES && Self->HasDatabaseFreezeWrites()) {
            result.SetReady(false);
            return true;
        }
        if (stage == TFreezeState::DRAIN_CDC
            && (Self->HasDatabaseFreezeIndexRecords() || Self->HasDatabaseFreezeCdcScans()))
        {
            result.SetReady(false);
            return true;
        }
        bool changed = false;
        if (newEpoch) {
            const bool deliveryLost = stage != TFreezeState::OPEN
                && (Self->DatabaseFreeze.GetDeliveryLost() || Self->UnconfirmedChangeRecordsRemoval);
            Self->DatabaseFreeze.Clear();
            Self->DatabaseFreeze.SetEpoch(request.GetEpoch());
            Self->DatabaseFreeze.SetDeliveryLost(deliveryLost);
            Self->DatabaseFreezeAcceptedTxIds.clear();
            for (const auto& [txId, op] : Self->TransQueue.GetTxsInFly()) {
                if (!op->IsReadOnly() && !op->IsImmediate()) {
                    Self->DatabaseFreeze.AddAcceptedTxIds(txId);
                    Self->DatabaseFreezeAcceptedTxIds.insert(txId);
                }
            }
            changed = true;
        }

        if (stage == TFreezeState::OPEN) {
            Unfreezing = wasFrozen;
            result.SetReady(true);
        } else {
            if (Self->DatabaseFreeze.GetStage() < stage) {
                Self->DatabaseFreeze.SetStage(stage);
                changed = true;
            }

            bool ready = !Self->HasDatabaseFreezeWrites();
            if (stage >= TFreezeState::DRAIN_INDEXES) {
                ready = ready && !Self->HasDatabaseFreezeIndexRecords()
                    && !Self->HasDatabaseFreezeCdcScans();
            }
            if (stage == TFreezeState::DRAIN_CDC && ready) {
                if (!Self->DatabaseFreeze.GetFinalHeartbeatsEmitted()) {
                    const auto edge = Max(Self->GetMvccTxVersion(EMvccTxMode::ReadWrite),
                        Self->SnapshotManager.GetImmediateWriteEdge().Next(),
                        Self->Pipeline.GetLastCompleteTx().ToRowVersion().Next());
                    const auto heartbeats = Self->CdcStreamHeartbeatManager.EmitFinalHeartbeats(txc.DB, edge);
                    for (const auto& [streamId, info] : heartbeats) {
                        auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcHeartbeat)
                            .WithOrder(Self->AllocateChangeRecordOrder(db))
                            .WithGroup(0).WithStep(info.Last.Step).WithTxId(info.Last.TxId)
                            .WithPathId(streamId).WithTableId(info.TablePathId)
                            .WithSchemaVersion(0).Build();
                        Self->PersistChangeRecord(db, *record);
                        Heartbeats.push_back(IDataShardChangeCollector::TChange{
                            .Order = record->GetOrder(), .Group = record->GetGroup(),
                            .Step = record->GetStep(), .TxId = record->GetTxId(),
                            .PathId = streamId, .BodySize = record->GetBodySize(),
                            .TableId = record->GetTableId(), .SchemaVersion = 0,
                        });
                    }
                    Self->PromoteImmediatePostExecuteEdges(edge, EPromotePostExecuteEdges::ReadWrite, txc);
                    Self->DatabaseFreeze.SetFinalHeartbeatsEmitted(true);
                    changed = true;
                }
                // Newly persisted heartbeats enter ChangesQueue in Complete.
                // This command cannot acknowledge them before that happens.
                ready = Heartbeats.empty() && Self->ChangesQueue.empty() && !Self->RemoveChangeRecordsInFly
                    && Self->CommittedLockChangeRecords.empty();
            }
            if (Self->DatabaseFreeze.GetDeliveryLost()) {
                // Persist the fence even if delivery was lost: only an explicit
                // thaw is allowed to reopen a partially frozen database.
                result.SetStatus(Ydb::StatusIds::PRECONDITION_FAILED);
                result.SetIssue("Change records were discarded during database freeze");
                ready = true;
            }
            result.SetReady(ready);
        }

        if (changed) {
            Self->PersistSys(db, Schema::Sys_DatabaseFreeze, Self->DatabaseFreeze.SerializeAsString());
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Heartbeats) {
            Self->EnqueueChangeRecords(std::move(Heartbeats));
        }
        if (Unfreezing) {
            Self->EmitHeartbeats();
        }
        ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);
    }
};

void TDataShard::Handle(TEvDataShard::TEvDatabaseFreeze::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDatabaseFreeze(this, ev), ctx);
}

} // namespace NKikimr::NDataShard
