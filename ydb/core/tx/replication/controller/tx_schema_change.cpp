#include "controller_impl.h"
#include "dst_schema_changer.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

namespace {

bool IsValidSchemaChange(const NKikimrReplication::TSchemaChange& schema) {
    if (!schema.HasVersion() || !schema.GetSourceSchemaVersion() || !schema.ColumnsSize()
        || !schema.PrimaryKeyColumnNamesSize()) {
        return false;
    }

    THashSet<TString> columns;
    for (const auto& column : schema.GetColumns()) {
        if (!column.GetName() || !column.GetType() || !columns.insert(column.GetName()).second) {
            return false;
        }
    }
    for (const auto& key : schema.GetPrimaryKeyColumnNames()) {
        if (!columns.contains(key)) {
            return false;
        }
    }

    return true;
}

bool IsGlobalConsistency(const TReplication& replication) {
    return replication.GetConfig().GetConsistencySettings().GetLevelCase()
        == NKikimrReplication::TConsistencySettings::kGlobal;
}

bool IsNewerSchemaChange(const NKikimrReplication::TSchemaChange& lhs,
        const NKikimrReplication::TSchemaChange& rhs)
{
    const auto& l = lhs.GetVersion();
    const auto& r = rhs.GetVersion();
    return l.GetStep() > r.GetStep()
        || (l.GetStep() == r.GetStep() && l.GetTxId() > r.GetTxId())
        || (l.GetStep() == r.GetStep() && l.GetTxId() == r.GetTxId()
            && lhs.GetSourceSchemaVersion() > rhs.GetSourceSchemaVersion());
}

} // anonymous namespace

class TController::TTxSchemaChangeReport: public TTxBase {
    TEvService::TEvSchemaChangeReport::TPtr Event;
    bool StartAlter = false;
    bool StartTargetFlush = false;
    bool ReleaseReporter = false;
    bool AcknowledgeReporter = false;
    bool AcknowledgeCompleted = false;
    bool ResumeDeferredAlters = false;
    bool RecheckHeartbeats = false;
    bool Failed = false;

    TSchemaBarrier* FindOrCreateBarrier(const TWorkerId& id,
            const NKikimrReplication::TSchemaChange& schema,
            const std::pair<ui64, ui64>& key,
            NIceDb::TNiceDb& db,
            const TActorContext& ctx)
    {
        auto it = Self->SchemaBarriers.find(key);

        // Keep the last completed data-plane barrier only for idempotent
        // retries. A global barrier may still be Verifying while an assigned
        // write waits for a later heartbeat boundary. That write remains in
        // AssignedTxIds and will be included in the next barrier's target
        // flush snapshot, so it is safe to collect the next schema here.
        if (it != Self->SchemaBarriers.end()
            && (it->second.Phase == ESchemaBarrierPhase::Verifying
                || it->second.Phase == ESchemaBarrierPhase::Applied)
            && it->second.Schema.SerializeAsString() != schema.SerializeAsString()
            && IsNewerSchemaChange(schema, it->second.Schema)
            && it->second.CompletedWorkers.size() == it->second.ExpectedWorkers.size()) {
            for (const auto& workerId : it->second.ExpectedWorkers) {
                db.Table<Schema::SchemaBarrierWorkers>()
                    .Key(workerId.ReplicationId(), workerId.TargetId(), workerId.WorkerId()).Delete();
            }
            for (const auto writeTxId : it->second.TargetFlushTxIds) {
                db.Table<Schema::SchemaBarrierFlushes>().Key(key.first, key.second, writeTxId).Delete();
            }
            db.Table<Schema::SchemaBarriers>().Key(key.first, key.second).Delete();
            Self->SchemaBarriers.erase(it);
            it = Self->SchemaBarriers.end();
        }

        if (it == Self->SchemaBarriers.end()) {
            TSchemaBarrier barrier;
            barrier.Schema.CopyFrom(schema);

            // Snapshot the complete current target membership before recording
            // the first report. It is persisted in this same transaction, so
            // recovery cannot observe a partially collected barrier.
            for (const auto& [workerId, _] : Self->Workers) {
                if (workerId.ReplicationId() != id.ReplicationId()
                    || workerId.TargetId() != id.TargetId()) {
                    continue;
                }

                barrier.ExpectedWorkers.insert(workerId);
                db.Table<Schema::SchemaBarrierWorkers>()
                    .Key(workerId.ReplicationId(), workerId.TargetId(), workerId.WorkerId())
                    .Update(
                        NIceDb::TUpdate<Schema::SchemaBarrierWorkers::Reported>(false),
                        NIceDb::TUpdate<Schema::SchemaBarrierWorkers::Applied>(false),
                        NIceDb::TUpdate<Schema::SchemaBarrierWorkers::Completed>(false),
                        NIceDb::TUpdate<Schema::SchemaBarrierWorkers::Offset>(0));
            }

            if (barrier.ExpectedWorkers.empty() || !barrier.ExpectedWorkers.contains(id)) {
                YDB_LOG_ERROR_CTX(ctx, "Cannot establish schema barrier membership",
                    {"worker", id});
                return nullptr;
            }

            db.Table<Schema::SchemaBarriers>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::SchemaBarriers::Phase>(static_cast<ui8>(barrier.Phase)),
                NIceDb::TUpdate<Schema::SchemaBarriers::Schema>(barrier.Schema.SerializeAsString())
            );
            it = Self->SchemaBarriers.emplace(key, std::move(barrier)).first;
        }

        return &it->second;
    }

    void FailBarrier(TSchemaBarrier& barrier,
            const std::pair<ui64, ui64>& key,
            NIceDb::TNiceDb& db,
            TStringBuf error)
    {
        barrier.Phase = ESchemaBarrierPhase::Error;
        db.Table<Schema::SchemaBarriers>().Key(key.first, key.second).Update(
            NIceDb::TUpdate<Schema::SchemaBarriers::Phase>(static_cast<ui8>(barrier.Phase)));

        auto replication = Self->Find(key.first);
        auto* target = replication ? replication->FindTarget(key.second) : nullptr;
        if (!replication || replication->GetState() == TReplication::EState::Removing || !target) {
            return;
        }

        Failed = true;
        target->SetDstState(TReplication::EDstState::Error);
        target->SetIssue(TString(error));
        replication->SetState(TReplication::EState::Error,
            TStringBuilder() << "Error in target #" << target->GetId() << ": " << target->GetIssue());
        db.Table<Schema::Replications>().Key(key.first).Update(
            NIceDb::TUpdate<Schema::Replications::State>(replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(replication->GetIssue()));
        db.Table<Schema::Targets>().Key(key.first, key.second).Update(
            NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
            NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue()));
    }

public:
    TTxSchemaChangeReport(TController* self, TEvService::TEvSchemaChangeReport::TPtr& ev)
        : TTxBase("TxSchemaChangeReport", self)
        , Event(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_SCHEMA_CHANGE_REPORT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);

        const auto& record = Event->Get()->Record;
        const auto id = TWorkerId::Parse(record.GetWorker());
        const auto& schema = record.GetSchema();
        if (!IsValidSchemaChange(schema)) {
            YDB_LOG_ERROR_CTX(ctx, "Malformed schema change report",
                {"worker", id});
            return true;
        }

        const auto key = std::make_pair(id.ReplicationId(), id.TargetId());
        if (!Self->WorkerSnapshots.contains(key)) {
            YDB_LOG_NOTICE_CTX(ctx, "Schema report before worker snapshot is ready",
                {"worker", id});
            return true;
        }
        NIceDb::TNiceDb db(txc.DB);
        auto* barrierPtr = FindOrCreateBarrier(id, schema, key, db, ctx);
        if (!barrierPtr) {
            return true;
        }
        auto& barrier = *barrierPtr;
        const bool schemaMatchesBarrier = barrier.Schema.SerializeAsString() == schema.SerializeAsString();
        const bool schemaIsOlderThanBarrier = !schemaMatchesBarrier
            && IsNewerSchemaChange(barrier.Schema, schema);

        if (record.GetCompleted()) {
            // A newer barrier can be installed only after every worker of
            // the previous one completed. A delayed retry of that previous
            // completion is therefore safe to acknowledge by identity even
            // though the one-row barrier slot now contains the newer schema.
            if (schemaIsOlderThanBarrier) {
                AcknowledgeReporter = true;
                AcknowledgeCompleted = true;
                return true;
            }
            if (!barrier.ExpectedWorkers.contains(id)
                || !schemaMatchesBarrier
                || (barrier.Phase != ESchemaBarrierPhase::Verifying
                    && barrier.Phase != ESchemaBarrierPhase::Applied)) {
                YDB_LOG_ERROR_CTX(ctx, "Invalid schema completion report",
                    {"worker", id});
                return true;
            }
            if (!barrier.AppliedWorkers.contains(id)) {
                YDB_LOG_ERROR_CTX(ctx, "Schema completion before apply acknowledgement",
                    {"worker", id});
                return true;
            }
            if (!barrier.WorkerOffsets.contains(id) || barrier.WorkerOffsets.at(id) != record.GetOffset()) {
                YDB_LOG_ERROR_CTX(ctx, "Schema completion offset does not match barrier",
                    {"worker", id});
                return true;
            }
            if (barrier.CompletedWorkers.insert(id).second) {
                db.Table<Schema::SchemaBarrierWorkers>()
                    .Key(id.ReplicationId(), id.TargetId(), id.WorkerId())
                    .Update(NIceDb::TUpdate<Schema::SchemaBarrierWorkers::Completed>(true));
            }
            // The last completion is the durable point at which an Applied
            // barrier ceases to hold lifecycle changes.  DDL completion may
            // have checked DeferredAlters before any worker could report
            // completion, so recheck them after this transaction commits.
            ResumeDeferredAlters = barrier.CompletedWorkers.size() == barrier.ExpectedWorkers.size();
            AcknowledgeReporter = true;
            return true;
        }

        if (record.GetApplied()) {
            if (schemaIsOlderThanBarrier) {
                AcknowledgeReporter = true;
                AcknowledgeCompleted = true;
                return true;
            }
            if (!barrier.ExpectedWorkers.contains(id)
                || !schemaMatchesBarrier
                || (barrier.Phase != ESchemaBarrierPhase::Verifying
                    && barrier.Phase != ESchemaBarrierPhase::Applied)) {
                YDB_LOG_ERROR_CTX(ctx, "Invalid schema apply report",
                    {"worker", id});
                return true;
            }
            if (!barrier.WorkerOffsets.contains(id) || barrier.WorkerOffsets.at(id) != record.GetOffset()) {
                YDB_LOG_ERROR_CTX(ctx, "Schema apply offset does not match barrier",
                    {"worker", id});
                return true;
            }
            if (barrier.AppliedWorkers.insert(id).second) {
                db.Table<Schema::SchemaBarrierWorkers>()
                    .Key(id.ReplicationId(), id.TargetId(), id.WorkerId())
                    .Update(NIceDb::TUpdate<Schema::SchemaBarrierWorkers::Applied>(true));
            }
            AcknowledgeReporter = true;
            return true;
        }

        if (barrier.Phase != ESchemaBarrierPhase::Collecting) {
            // Later phases are driven from durable state.  A replayed report
            // cannot change either membership or the requested schema.
            if (!schemaMatchesBarrier && barrier.Phase == ESchemaBarrierPhase::Altering) {
                // SchemeShard may already own a durable DDL transaction. It
                // cannot be cancelled by stopping the local alterer, so keep
                // the authoritative barrier until that transaction reaches a
                // durable result. The worker will retry its report afterward.
                YDB_LOG_NOTICE_CTX(ctx, "Defer conflicting schema report until destination alter completes",
                    {"worker", id});
            } else if (!schemaMatchesBarrier && IsNewerSchemaChange(schema, barrier.Schema)
                && (barrier.Phase == ESchemaBarrierPhase::Verifying
                    || barrier.Phase == ESchemaBarrierPhase::Applied)) {
                // A partition can reach the next schema record before the
                // slowest partition durably completes this one, including
                // during the global post-DDL heartbeat quorum. In Verifying,
                // this report itself proves the worker crossed a version
                // newer than the barrier; persist it as freshness because a
                // parked worker cannot poll the subsequent heartbeat.
                if (barrier.Phase == ESchemaBarrierPhase::Verifying) {
                    const auto version = TRowVersion::FromProto(schema.GetVersion());
                    auto wi = Self->Workers.find(id);
                    Y_ABORT_UNLESS(wi != Self->Workers.end());
                    auto& worker = wi->second;
                    if (!worker.HasHeartbeat() || worker.GetHeartbeat() < version) {
                        if (worker.HasHeartbeat()) {
                            auto previous = Self->WorkersByHeartbeat.find(worker.GetHeartbeat());
                            if (previous != Self->WorkersByHeartbeat.end()) {
                                previous->second.erase(id);
                                if (previous->second.empty()) {
                                    Self->WorkersByHeartbeat.erase(previous);
                                }
                            }
                        }
                        worker.SetHeartbeat(version);
                        Self->WorkersWithHeartbeat.insert(id);
                        Self->WorkersByHeartbeat[version].insert(id);
                        db.Table<Schema::Workers>().Key(id.ReplicationId(), id.TargetId(), id.WorkerId()).Update(
                            NIceDb::TUpdate<Schema::Workers::HeartbeatVersionStep>(version.Step),
                            NIceDb::TUpdate<Schema::Workers::HeartbeatVersionTxId>(version.TxId));
                    }
                    RecheckHeartbeats = true;
                }
                YDB_LOG_NOTICE_CTX(ctx, "Defer newer schema report until active barrier completes",
                    {"worker", id});
            } else if (!schemaMatchesBarrier) {
                YDB_LOG_ERROR_CTX(ctx, "Schema report conflicts with active barrier",
                    {"worker", id});
                if (barrier.Phase != ESchemaBarrierPhase::Error) {
                    FailBarrier(barrier, key, db, "Conflicting schema change reports");
                }
            } else if (barrier.Phase == ESchemaBarrierPhase::Verifying
                || barrier.Phase == ESchemaBarrierPhase::Applied) {
                // The service retries reports after reconnecting.  Releasing
                // this one worker again is safe and makes DDL completion
                // idempotent.
                ReleaseReporter = true;
            }
            return true;
        }

        if (!barrier.ExpectedWorkers.contains(id)) {
            YDB_LOG_ERROR_CTX(ctx, "Schema report from outside barrier membership",
                {"worker", id});
            return true;
        }
        if (!schemaMatchesBarrier && IsNewerSchemaChange(schema, barrier.Schema)) {
            YDB_LOG_NOTICE_CTX(ctx, "Defer newer schema report until active barrier completes",
                {"worker", id});
            return true;
        }
        if (!schemaMatchesBarrier) {
            YDB_LOG_ERROR_CTX(ctx, "Conflicting schema reports",
                {"worker", id});
            FailBarrier(barrier, key, db, "Conflicting schema change reports");
            return true;
        }

        if (barrier.ReportedWorkers.insert(id).second) {
            barrier.WorkerOffsets[id] = record.GetOffset();
            db.Table<Schema::SchemaBarrierWorkers>()
                .Key(id.ReplicationId(), id.TargetId(), id.WorkerId())
                .Update(
                    NIceDb::TUpdate<Schema::SchemaBarrierWorkers::Reported>(true),
                    NIceDb::TUpdate<Schema::SchemaBarrierWorkers::Offset>(record.GetOffset()));
        }

        if (barrier.ReportedWorkers.size() == barrier.ExpectedWorkers.size()) {
            YDB_LOG_NOTICE_CTX(ctx, "Schema barrier fully collected",
                {"replicationId", key.first},
                {"targetId", key.second},
                {"workers", barrier.ExpectedWorkers.size()});
            auto replication = Self->Find(key.first);
            if (replication && IsGlobalConsistency(*replication) && !Self->AssignedTxIds.empty()) {
                // A schema record is after all old-schema writes for its
                // partition.  Their ids are therefore safe to flush to this
                // target, but must remain globally tracked for the next
                // all-target heartbeat commit.
                barrier.Phase = ESchemaBarrierPhase::FlushingTarget;
                barrier.TargetFlushTxIds.reserve(Self->AssignedTxIds.size());
                for (const auto& [_, writeTxId] : Self->AssignedTxIds) {
                    barrier.TargetFlushTxIds.push_back(writeTxId);
                    db.Table<Schema::SchemaBarrierFlushes>().Key(key.first, key.second, writeTxId).Update();
                }
                StartTargetFlush = !Self->CommittingTxId;
            } else {
                barrier.Phase = ESchemaBarrierPhase::Altering;
                StartAlter = true;
            }
            db.Table<Schema::SchemaBarriers>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::SchemaBarriers::Phase>(static_cast<ui8>(barrier.Phase)));
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Failed) {
            if (const auto id = TWorkerId::Parse(Event->Get()->Record.GetWorker()); auto replication = Self->Find(id.ReplicationId())) {
                replication->Progress(ctx);
            }
            return;
        }

        if (StartTargetFlush) {
            const auto id = TWorkerId::Parse(Event->Get()->Record.GetWorker());
            Self->StartSchemaChangeTargetFlush({id.ReplicationId(), id.TargetId()}, ctx);
            return;
        }

        if (ReleaseReporter) {
            const auto id = TWorkerId::Parse(Event->Get()->Record.GetWorker());
            auto wi = Self->Workers.find(id);
            if (wi != Self->Workers.end() && wi->second.HasSession()) {
                auto ev = MakeHolder<TEvService::TEvSchemaChangeResult>();
                id.Serialize(*ev->Record.MutableWorker());
                ev->Record.MutableSchema()->CopyFrom(Event->Get()->Record.GetSchema());
                ev->Record.SetOffset(Event->Get()->Record.GetOffset());
                auto& controller = *ev->Record.MutableController();
                controller.SetTabletId(Self->TabletID());
                controller.SetGeneration(Self->Executor()->Generation());
                ctx.Send(MakeReplicationServiceId(wi->second.GetSession()), ev.Release());
            }
            return;
        }

        if (AcknowledgeReporter) {
            const auto id = TWorkerId::Parse(Event->Get()->Record.GetWorker());
            auto wi = Self->Workers.find(id);
            if (wi != Self->Workers.end() && wi->second.HasSession()) {
                auto ev = MakeHolder<TEvService::TEvSchemaChangeResult>();
                id.Serialize(*ev->Record.MutableWorker());
                ev->Record.MutableSchema()->CopyFrom(Event->Get()->Record.GetSchema());
                ev->Record.SetOffset(Event->Get()->Record.GetOffset());
                auto& controller = *ev->Record.MutableController();
                controller.SetTabletId(Self->TabletID());
                controller.SetGeneration(Self->Executor()->Generation());
                ev->Record.SetApplied(true);
                ev->Record.SetCompleted(AcknowledgeCompleted || Event->Get()->Record.GetCompleted());
                ctx.Send(MakeReplicationServiceId(wi->second.GetSession()), ev.Release());
            }
        }

        if (ResumeDeferredAlters) {
            for (const auto replicationId : Self->DeferredAlters) {
                if (!Self->HasActiveSchemaBarrier(replicationId)) {
                    ctx.Send(ctx.SelfID, new TEvPrivate::TEvResumeDeferredAlter(replicationId));
                }
            }
        }

        if (RecheckHeartbeats) {
            Self->RunTxHeartbeat(ctx);
        }

        if (!StartAlter) {
            return;
        }

        const auto id = TWorkerId::Parse(Event->Get()->Record.GetWorker());
        Self->StartSchemaChangeDstAlter({id.ReplicationId(), id.TargetId()}, ctx);
    }
};

void TController::StartSchemaChangeDstAlter(const std::pair<ui64, ui64>& key, const TActorContext& ctx) {
    if (SchemaChangeDstAlterers.contains(key)) {
        return;
    }

    const auto id = TWorkerId(key.first, key.second, 0);
    auto* target = FindTarget(id);
    auto replication = Find(key.first);
    auto it = SchemaBarriers.find(key);
    if (!target || !replication || replication->GetState() == TReplication::EState::Removing
        || it == SchemaBarriers.end()) {
        return;
    }

    const auto actorId = ctx.Register(CreateSchemaChangeDstAlterer(ctx.SelfID, replication->GetSchemeShardId(),
        key.first, key.second, target->GetKind(), target->GetDstPathId(), it->second.Schema,
        it->second.DstAlterTxId));
    SchemaChangeDstAlterers.emplace(key, actorId);
}

void TController::StopSchemaChangeDstAlter(const std::pair<ui64, ui64>& key, const TActorContext& ctx) {
    const auto it = SchemaChangeDstAlterers.find(key);
    if (it == SchemaChangeDstAlterers.end()) {
        return;
    }

    ctx.Send(it->second, new TEvents::TEvPoison());
    SchemaChangeDstAlterers.erase(it);
}

class TController::TTxSchemaChangeDstAlterTxId: public TTxBase {
    TEvPrivate::TEvSchemaChangeDstAlterTxId::TPtr Event;
    ui64 TxId = 0;

public:
    TTxSchemaChangeDstAlterTxId(TController* self, TEvPrivate::TEvSchemaChangeDstAlterTxId::TPtr& ev)
        : TTxBase("TxSchemaChangeDstAlterTxId", self)
        , Event(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_SCHEMA_CHANGE_REPORT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto key = std::make_pair(Event->Get()->ReplicationId, Event->Get()->TargetId);
        auto it = Self->SchemaBarriers.find(key);
        if (it == Self->SchemaBarriers.end() || it->second.Phase != ESchemaBarrierPhase::Altering) {
            return true;
        }

        TxId = it->second.DstAlterTxId;
        if (!TxId) {
            TxId = Event->Get()->TxId;
            it->second.DstAlterTxId = TxId;
            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::SchemaBarriers>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::SchemaBarriers::DstAlterTxId>(TxId));
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (TxId) {
            ctx.Send(Event->Sender, new TEvPrivate::TEvSchemaChangeDstAlterTxIdSaved(TxId));
        }
    }
};

void TController::RunTxSchemaChangeDstAlterTxId(TEvPrivate::TEvSchemaChangeDstAlterTxId::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxSchemaChangeDstAlterTxId(this, ev), ctx);
}

void TController::StartSchemaChangeTargetFlush(const std::pair<ui64, ui64>& key, const TActorContext& ctx) {
    auto it = SchemaBarriers.find(key);
    if (it == SchemaBarriers.end() || it->second.Phase != ESchemaBarrierPhase::FlushingTarget) {
        return;
    }

    auto& barrier = it->second;
    if (ActiveSchemaTargetFlush && *ActiveSchemaTargetFlush != key) {
        return;
    }

    if (barrier.NextTargetFlushTxId == barrier.TargetFlushTxIds.size()) {
        ActiveSchemaTargetFlush.reset();
        barrier.Phase = ESchemaBarrierPhase::Altering;
        StartSchemaChangeDstAlter(key, ctx);
        return;
    }

    const ui64 writeTxId = barrier.TargetFlushTxIds[barrier.NextTargetFlushTxId];
    auto replication = Find(key.first);
    auto* target = FindTarget(TWorkerId(key.first, key.second, 0));
    if (!replication || replication->GetState() == TReplication::EState::Removing
        || !target || SchemaTargetFlushes.contains(writeTxId)) {
        return;
    }

    ActiveSchemaTargetFlush = key;
    SchemaTargetFlushes.emplace(writeTxId, key);
    TVector<TString> tables{target->GetDstPath()};
    ctx.Send(MakeTxProxyID(), MakeCommitProposal(writeTxId, tables).Release(), 0, writeTxId);
}

void TController::RunTxSchemaChangeReport(TEvService::TEvSchemaChangeReport::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxSchemaChangeReport(this, ev), ctx);
}

class TController::TTxSchemaChangeDstAlterResult : public TTxBase {
    TEvPrivate::TEvSchemaChangeDstAlterResult::TPtr Event;
    TVector<TWorkerId> Release;
    THashMap<TWorkerId, ui64> ReleaseOffsets;
    NKikimrReplication::TSchemaChange Schema;
    bool AwaitPostSchemaHeartbeats = false;
    bool Failed = false;

public:
    TTxSchemaChangeDstAlterResult(TController* self, TEvPrivate::TEvSchemaChangeDstAlterResult::TPtr& ev)
        : TTxBase("TxSchemaChangeDstAlterResult", self)
        , Event(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_SCHEMA_CHANGE_REPORT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto key = std::make_pair(Event->Get()->ReplicationId, Event->Get()->TargetId);
        auto it = Self->SchemaBarriers.find(key);
        if (it == Self->SchemaBarriers.end()
            || it->second.Phase != ESchemaBarrierPhase::Altering
            || !it->second.DstAlterTxId
            || it->second.DstAlterTxId != Event->Get()->DstAlterTxId) {
            return true;
        }
        NIceDb::TNiceDb db(txc.DB);
        if (!Event->Get()->IsSuccess()) {
            auto replication = Self->Find(key.first);
            auto* target = replication ? replication->FindTarget(key.second) : nullptr;
            it->second.Phase = ESchemaBarrierPhase::Error;
            db.Table<Schema::SchemaBarriers>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::SchemaBarriers::Phase>(static_cast<ui8>(ESchemaBarrierPhase::Error)));
            if (replication && replication->GetState() != TReplication::EState::Removing && target) {
                Failed = true;
                target->SetDstState(TReplication::EDstState::Error);
                target->SetIssue(TStringBuilder() << "Schema change error: " << Event->Get()->Error);
                replication->SetState(TReplication::EState::Error,
                    TStringBuilder() << "Error in target #" << target->GetId() << ": " << target->GetIssue());
                db.Table<Schema::Replications>().Key(key.first).Update(
                    NIceDb::TUpdate<Schema::Replications::State>(replication->GetState()),
                    NIceDb::TUpdate<Schema::Replications::Issue>(replication->GetIssue()));
                db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                    NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
                    NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue()));
            }
            return true;
        }

        auto replication = Self->Find(key.first);
        AwaitPostSchemaHeartbeats = replication && IsGlobalConsistency(*replication);
        it->second.Phase = AwaitPostSchemaHeartbeats
            ? ESchemaBarrierPhase::Verifying
            : ESchemaBarrierPhase::Applied;
        Schema.CopyFrom(it->second.Schema);
        Release.assign(it->second.ExpectedWorkers.begin(), it->second.ExpectedWorkers.end());
        ReleaseOffsets = it->second.WorkerOffsets;
        db.Table<Schema::SchemaBarriers>().Key(key.first, key.second).Update(
            NIceDb::TUpdate<Schema::SchemaBarriers::Phase>(static_cast<ui8>(it->second.Phase)));

        if (AwaitPostSchemaHeartbeats) {
            // The next quorum must be formed strictly after the schema DDL.
            // Persist the reset too, otherwise a controller restart could use
            // an old heartbeat and prematurely restore global consistency.
            Self->WorkersWithHeartbeat.clear();
            Self->WorkersByHeartbeat.clear();
            for (auto& [workerId, worker] : Self->Workers) {
                worker.ClearHeartbeat();
                db.Table<Schema::Workers>().Key(workerId.ReplicationId(), workerId.TargetId(), workerId.WorkerId()).Update(
                    NIceDb::TUpdate<Schema::Workers::HeartbeatVersionStep>(0),
                    NIceDb::TUpdate<Schema::Workers::HeartbeatVersionTxId>(0));
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        const auto key = std::make_pair(Event->Get()->ReplicationId, Event->Get()->TargetId);
        const auto alterer = Self->SchemaChangeDstAlterers.find(key);
        if (alterer != Self->SchemaChangeDstAlterers.end() && alterer->second == Event->Sender) {
            Self->SchemaChangeDstAlterers.erase(alterer);
        }

        if (Failed) {
            if (auto replication = Self->Find(Event->Get()->ReplicationId)) {
                replication->Progress(ctx);
            }
        }

        for (const auto& id : Release) {
            auto wi = Self->Workers.find(id);
            if (wi == Self->Workers.end() || !wi->second.HasSession()) {
                continue;
            }

            auto ev = MakeHolder<TEvService::TEvSchemaChangeResult>();
            id.Serialize(*ev->Record.MutableWorker());
            ev->Record.MutableSchema()->CopyFrom(Schema);
            ev->Record.SetOffset(ReleaseOffsets.at(id));

            auto& controller = *ev->Record.MutableController();
            controller.SetTabletId(Self->TabletID());
            controller.SetGeneration(Self->Executor()->Generation());
            ctx.Send(MakeReplicationServiceId(wi->second.GetSession()), ev.Release());
        }

        for (const auto& [key, barrier] : Self->SchemaBarriers) {
            if (barrier.Phase == ESchemaBarrierPhase::FlushingTarget) {
                Self->StartSchemaChangeTargetFlush(key, ctx);
            }
        }

        for (const auto replicationId : Self->DeferredAlters) {
            if (!Self->HasActiveSchemaBarrier(replicationId)) {
                ctx.Send(ctx.SelfID, new TEvPrivate::TEvResumeDeferredAlter(replicationId));
            }
        }

        Self->RunTxHeartbeat(ctx);
    }
};

void TController::RunTxSchemaChangeDstAlterResult(TEvPrivate::TEvSchemaChangeDstAlterResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxSchemaChangeDstAlterResult(this, ev), ctx);
}

} // NKikimr::NReplication::NController
