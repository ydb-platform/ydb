#include "controller_impl.h"
#include "dst_schema_changer.h"

#include <ydb/core/tx/replication/controller/protos/schema_barrier.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

namespace {

bool IsValidSchemaChange(const NKikimrReplication::TSchemaChange& schema) {
    if (!schema.HasVersion()
        || !schema.GetSourceSchemaVersion()
        || !schema.ColumnsSize()
        || !schema.PrimaryKeyColumnNamesSize())
    {
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

bool IsNewerSchemaChange(const NKikimrReplication::TSchemaChange& lhs, const NKikimrReplication::TSchemaChange& rhs) {
    const auto l = TRowVersion::FromProto(lhs.GetVersion());
    const auto r = TRowVersion::FromProto(rhs.GetVersion());
    if (l != r) {
        return l > r;
    }
    return lhs.GetSourceSchemaVersion() > rhs.GetSourceSchemaVersion();
}

bool IsGlobalConsistency(const TReplication& replication) {
    return replication.GetConfig().GetConsistencySettings().GetLevelCase()
        == NKikimrReplication::TConsistencySettings::kGlobal;
}

enum class ESchemaChangeRelation {
    Same,
    Older,
    Newer,
    Conflict,
};

ESchemaChangeRelation CompareSchemaChanges(
        const NKikimrReplication::TSchemaChange& reported,
        const NKikimrReplication::TSchemaChange& barrier)
{
    if (reported.SerializeAsString() == barrier.SerializeAsString()) {
        return ESchemaChangeRelation::Same;
    }
    if (IsNewerSchemaChange(reported, barrier)) {
        return ESchemaChangeRelation::Newer;
    }
    if (IsNewerSchemaChange(barrier, reported)) {
        return ESchemaChangeRelation::Older;
    }
    return ESchemaChangeRelation::Conflict;
}

enum class ESchemaReportStage {
    Observed,
    Applied,
    Completed,
};

ESchemaReportStage GetSchemaReportStage(const NKikimrReplication::TEvSchemaChangeReport& record) {
    if (record.GetCompleted()) {
        return ESchemaReportStage::Completed;
    }
    if (record.GetApplied()) {
        return ESchemaReportStage::Applied;
    }
    return ESchemaReportStage::Observed;
}

} // anonymous namespace

void TController::SendSchemaChangeResult(
        const TWorkerId& id,
        const NKikimrReplication::TSchemaChange& schema,
        ui64 offset,
        bool applied,
        bool completed,
        const TActorContext& ctx)
{
    const auto worker = Workers.find(id);
    if (worker == Workers.end() || !worker->second.HasSession()) {
        return;
    }

    auto event = MakeHolder<TEvService::TEvSchemaChangeResult>();
    id.Serialize(*event->Record.MutableWorker());
    event->Record.MutableSchema()->CopyFrom(schema);
    event->Record.SetOffset(offset);

    auto& controller = *event->Record.MutableController();
    controller.SetTabletId(TabletID());
    controller.SetGeneration(Executor()->Generation());
    event->Record.SetApplied(applied);
    event->Record.SetCompleted(completed);

    ctx.Send(MakeReplicationServiceId(worker->second.GetSession()), event.Release());
}

class TController::TTxSchemaChangeReport: public TTxBase {
    TEvService::TEvSchemaChangeReport::TPtr Event;
    enum class EReporterReply {
        None,
        Release,
        Applied,
        Completed,
    };

    EReporterReply ReporterReply = EReporterReply::None;
    bool StartAlter = false;
    bool StartTargetFlush = false;
    bool ResumeDeferredAlters = false;
    bool RecheckHeartbeats = false;
    bool ProgressReplication = false;

    static bool CanReplaceBarrier(const TSchemaBarrier& barrier, const NKikimrReplication::TSchemaChange& schema) {
        return barrier.IsDestinationSchemaReady()
            && CompareSchemaChanges(schema, barrier.Schema) == ESchemaChangeRelation::Newer
            && barrier.AreAllWorkersCompleted();
    }

    static bool HasExpectedOffset(const TSchemaBarrier& barrier, const TWorkerId& id, ui64 offset) {
        const auto it = barrier.WorkerOffsets.find(id);
        return it != barrier.WorkerOffsets.end() && it->second == offset;
    }

    static bool CanAcceptWorkerAck(const TSchemaBarrier& barrier, const TWorkerId& id, ESchemaChangeRelation relation) {
        return barrier.ExpectedWorkers.contains(id)
            && relation == ESchemaChangeRelation::Same
            && barrier.IsDestinationSchemaReady();
    }

    TSchemaBarrier* FindOrCreateBarrier(
            const TWorkerId& id,
            const NKikimrReplication::TSchemaChange& schema,
            const std::pair<ui64, ui64>& key,
            NIceDb::TNiceDb& db,
            const TActorContext& ctx)
    {
        auto it = Self->SchemaBarriers.find(key);

        // Keep the last completed data-plane barrier for idempotent retries;
        // replace it only when every worker has finished and a newer schema
        // record is reported.
        if (it != Self->SchemaBarriers.end() && CanReplaceBarrier(it->second, schema)) {
            for (const auto& workerId : it->second.ExpectedWorkers) {
                db.Table<Schema::SchemaBarrierWorkers>()
                    .Key(workerId.ReplicationId(), workerId.TargetId(), workerId.WorkerId()).Delete();
            }
            db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::Targets::SchemaBarrierPhase>(0),
                NIceDb::TUpdate<Schema::Targets::SchemaBarrierChange>(TString()),
                NIceDb::TUpdate<Schema::Targets::DstAlterTxId>(0),
                NIceDb::TUpdate<Schema::Targets::SchemaBarrierFlushTxIds>(TString()));
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
                if (workerId.ReplicationId() != id.ReplicationId() || workerId.TargetId() != id.TargetId()) {
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

            db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::Targets::SchemaBarrierPhase>(static_cast<ui8>(barrier.Phase)),
                NIceDb::TUpdate<Schema::Targets::SchemaBarrierChange>(barrier.Schema.SerializeAsString()),
                NIceDb::TUpdate<Schema::Targets::DstAlterTxId>(0),
                NIceDb::TUpdate<Schema::Targets::SchemaBarrierFlushTxIds>(TString())
            );

            it = Self->SchemaBarriers.emplace(key, std::move(barrier)).first;
        }

        return &it->second;
    }

    void FailBarrier(
            TSchemaBarrier& barrier,
            const std::pair<ui64, ui64>& key,
            NIceDb::TNiceDb& db,
            TStringBuf error)
    {
        barrier.Phase = ESchemaBarrierPhase::Error;
        db.Table<Schema::Targets>().Key(key.first, key.second).Update(
            NIceDb::TUpdate<Schema::Targets::SchemaBarrierPhase>(static_cast<ui8>(barrier.Phase)));

        auto replication = Self->Find(key.first);
        auto* target = replication ? replication->FindTarget(key.second) : nullptr;
        if (!replication || replication->GetState() == TReplication::EState::Removing || !target) {
            return;
        }

        ProgressReplication = true;
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

    void HandleCompletedReport(
            const TWorkerId& id,
            ESchemaChangeRelation relation,
            TSchemaBarrier& barrier,
            const NKikimrReplication::TEvSchemaChangeReport& record,
            NIceDb::TNiceDb& db,
            const TActorContext& ctx)
    {
        // A delayed completion for an older, fully retired barrier is safe to
        // acknowledge because a newer barrier can only replace a completed one.
        if (relation == ESchemaChangeRelation::Older) {
            ReporterReply = EReporterReply::Completed;
            return;
        }

        if (!CanAcceptWorkerAck(barrier, id, relation)) {
            YDB_LOG_ERROR_CTX(ctx, "Invalid schema completion report",
                {"worker", id});
            return;
        }

        if (!barrier.AppliedWorkers.contains(id)) {
            YDB_LOG_ERROR_CTX(ctx, "Schema completion before apply acknowledgement",
                {"worker", id});
            return;
        }

        if (!HasExpectedOffset(barrier, id, record.GetOffset())) {
            YDB_LOG_ERROR_CTX(ctx, "Schema completion offset does not match barrier",
                {"worker", id});
            return;
        }

        if (barrier.CompletedWorkers.insert(id).second) {
            db.Table<Schema::SchemaBarrierWorkers>()
                .Key(id.ReplicationId(), id.TargetId(), id.WorkerId())
                .Update(NIceDb::TUpdate<Schema::SchemaBarrierWorkers::Completed>(true));
        }

        // The last completion is the durable point at which an Applied
        // barrier ceases to hold lifecycle changes.
        ResumeDeferredAlters = barrier.AreAllWorkersCompleted();
        ReporterReply = EReporterReply::Completed;
    }

    void HandleAppliedReport(
            const TWorkerId& id,
            ESchemaChangeRelation relation,
            TSchemaBarrier& barrier,
            const NKikimrReplication::TEvSchemaChangeReport& record,
            NIceDb::TNiceDb& db,
            const TActorContext& ctx)
    {
        if (relation == ESchemaChangeRelation::Older) {
            ReporterReply = EReporterReply::Completed;
            return;
        }

        if (!CanAcceptWorkerAck(barrier, id, relation)) {
            YDB_LOG_ERROR_CTX(ctx, "Invalid schema apply report",
                {"worker", id});
            return;
        }

        if (!HasExpectedOffset(barrier, id, record.GetOffset())) {
            YDB_LOG_ERROR_CTX(ctx, "Schema apply offset does not match barrier",
                {"worker", id});
            return;
        }

        if (barrier.AppliedWorkers.insert(id).second) {
            db.Table<Schema::SchemaBarrierWorkers>()
                .Key(id.ReplicationId(), id.TargetId(), id.WorkerId())
                .Update(NIceDb::TUpdate<Schema::SchemaBarrierWorkers::Applied>(true));
        }

        ReporterReply = EReporterReply::Applied;
    }

    void RecordNewerSchemaAsHeartbeat(
            const TWorkerId& id,
            const NKikimrReplication::TSchemaChange& schema,
            NIceDb::TNiceDb& db)
    {
        const auto version = TRowVersion::FromProto(schema.GetVersion());
        auto workerIt = Self->Workers.find(id);
        Y_ABORT_UNLESS(workerIt != Self->Workers.end());
        auto& worker = workerIt->second;
        if (worker.HasHeartbeat() && worker.GetHeartbeat() >= version) {
            return;
        }

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

    void HandleEstablishedBarrierReport(
            const TWorkerId& id,
            ESchemaChangeRelation relation,
            TSchemaBarrier& barrier,
            const std::pair<ui64, ui64>& key,
            NIceDb::TNiceDb& db,
            const TActorContext& ctx)
    {
        switch (barrier.Phase) {
        case ESchemaBarrierPhase::FlushingTarget:
        case ESchemaBarrierPhase::Altering:
            if (relation != ESchemaChangeRelation::Same) {
                YDB_LOG_NOTICE_CTX(ctx, "Defer schema report until destination alter completes",
                    {"worker", id});
            }
            return;

        case ESchemaBarrierPhase::Verifying:
        case ESchemaBarrierPhase::Applied:
            if (relation == ESchemaChangeRelation::Same) {
                ReporterReply = EReporterReply::Release;
                return;
            }
            if (relation == ESchemaChangeRelation::Newer) {
                if (barrier.Phase == ESchemaBarrierPhase::Verifying) {
                    // A parked worker cannot emit a later heartbeat. Reaching
                    // the next schema record proves it crossed this barrier.
                    RecordNewerSchemaAsHeartbeat(id, Event->Get()->Record.GetSchema(), db);
                    RecheckHeartbeats = true;
                }
                YDB_LOG_NOTICE_CTX(ctx, "Defer newer schema report until active barrier completes",
                    {"worker", id});
                return;
            }

            YDB_LOG_ERROR_CTX(ctx, "Schema report conflicts with active barrier",
                {"worker", id});
            FailBarrier(barrier, key, db, "Conflicting schema change reports");
            return;

        case ESchemaBarrierPhase::Error:
            if (relation != ESchemaChangeRelation::Same) {
                YDB_LOG_ERROR_CTX(ctx, "Schema report conflicts with active barrier",
                    {"worker", id});
            }
            return;

        case ESchemaBarrierPhase::Collecting:
            Y_ABORT("Unexpected collecting schema barrier");
        }
    }

    void HandleCollectingReport(
            const TWorkerId& id,
            ESchemaChangeRelation relation,
            TSchemaBarrier& barrier,
            const std::pair<ui64, ui64>& key,
            const NKikimrReplication::TEvSchemaChangeReport& record,
            NIceDb::TNiceDb& db,
            const TActorContext& ctx)
    {
        if (!barrier.ExpectedWorkers.contains(id)) {
            YDB_LOG_ERROR_CTX(ctx, "Schema report from outside barrier membership",
                {"worker", id});
            return;
        }

        if (relation == ESchemaChangeRelation::Newer) {
            YDB_LOG_NOTICE_CTX(ctx, "Defer newer schema report until active barrier completes",
                {"worker", id});
            return;
        }

        if (relation != ESchemaChangeRelation::Same) {
            YDB_LOG_ERROR_CTX(ctx, "Conflicting schema reports",
                {"worker", id});
            FailBarrier(barrier, key, db, "Conflicting schema change reports");
            return;
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
            NKikimrReplicationController::TSchemaBarrierFlushTxIds flushTxIds;
            if (replication && IsGlobalConsistency(*replication) && !Self->AssignedTxIds.empty()) {
                // Commit the old-schema writes to this destination before its
                // DDL. Keep the IDs globally assigned so the next ordinary
                // commit remains an idempotent all-target operation.
                barrier.Phase = ESchemaBarrierPhase::FlushingTarget;
                barrier.TargetFlushTxIds.reserve(Self->AssignedTxIds.size());
                for (const auto& [_, writeTxId] : Self->AssignedTxIds) {
                    barrier.TargetFlushTxIds.push_back(writeTxId);
                    flushTxIds.AddWriteTxIds(writeTxId);
                }
                StartTargetFlush = !Self->CommittingTxId;
            } else {
                barrier.Phase = ESchemaBarrierPhase::Altering;
                StartAlter = true;
            }
            db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::Targets::SchemaBarrierPhase>(static_cast<ui8>(barrier.Phase)),
                NIceDb::TUpdate<Schema::Targets::SchemaBarrierFlushTxIds>(flushTxIds.SerializeAsString()));
        }
    }

    void HandleObservedReport(
            const TWorkerId& id,
            ESchemaChangeRelation relation,
            TSchemaBarrier& barrier,
            const std::pair<ui64, ui64>& key,
            const NKikimrReplication::TEvSchemaChangeReport& record,
            NIceDb::TNiceDb& db,
            const TActorContext& ctx)
    {
        if (barrier.Phase == ESchemaBarrierPhase::Collecting) {
            HandleCollectingReport(id, relation, barrier, key, record, db, ctx);
        } else {
            HandleEstablishedBarrierReport(id, relation, barrier, key, db, ctx);
        }
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
        if (!Self->CompleteWorkerSets.contains(key)) {
            YDB_LOG_NOTICE_CTX(ctx, "Schema report before complete worker set is ready",
                {"worker", id});
            return true;
        }

        auto replication = Self->Find(key.first);
        if (!replication) {
            YDB_LOG_NOTICE_CTX(ctx, "Schema report for unknown replication",
                {"worker", id});
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        auto* barrierPtr = FindOrCreateBarrier(id, schema, key, db, ctx);
        if (!barrierPtr) {
            return true;
        }

        auto& barrier = *barrierPtr;
        const auto relation = CompareSchemaChanges(schema, barrier.Schema);
        switch (GetSchemaReportStage(record)) {
        case ESchemaReportStage::Completed:
            HandleCompletedReport(id, relation, barrier, record, db, ctx);
            break;
        case ESchemaReportStage::Applied:
            HandleAppliedReport(id, relation, barrier, record, db, ctx);
            break;
        case ESchemaReportStage::Observed:
            HandleObservedReport(id, relation, barrier, key, record, db, ctx);
            break;
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (ProgressReplication) {
            if (const auto id = TWorkerId::Parse(Event->Get()->Record.GetWorker()); auto replication = Self->Find(id.ReplicationId())) {
                replication->Progress(ctx);
            }
            return;
        }

        const auto& record = Event->Get()->Record;
        const auto id = TWorkerId::Parse(record.GetWorker());
        if (ReporterReply != EReporterReply::None) {
            const bool applied = ReporterReply == EReporterReply::Applied
                || ReporterReply == EReporterReply::Completed;
            const bool completed = ReporterReply == EReporterReply::Completed;
            Self->SendSchemaChangeResult(id, record.GetSchema(), record.GetOffset(), applied, completed, ctx);
        }

        if (ReporterReply == EReporterReply::Release) {
            return;
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

        if (StartTargetFlush) {
            Self->StartSchemaChangeTargetFlush({id.ReplicationId(), id.TargetId()}, ctx);
            return;
        }

        if (!StartAlter) {
            return;
        }

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
    if (!target || !replication || replication->GetState() == TReplication::EState::Removing || it == SchemaBarriers.end()) {
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
            db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::Targets::SchemaBarrierPhase>(static_cast<ui8>(it->second.Phase)),
                NIceDb::TUpdate<Schema::Targets::DstAlterTxId>(TxId));
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
    const auto barrierIt = SchemaBarriers.find(key);
    if (barrierIt == SchemaBarriers.end() || barrierIt->second.Phase != ESchemaBarrierPhase::FlushingTarget) {
        return;
    }

    auto& barrier = barrierIt->second;
    YDB_LOG_TRACE_CTX(ctx, "Start schema change target flush",
        {"replicationId", key.first},
        {"targetId", key.second},
        {"nextTxId", barrier.NextTargetFlushTxId},
        {"txIds", barrier.TargetFlushTxIds.size()});
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
    if (!replication
        || replication->GetState() == TReplication::EState::Removing
        || !target
        || SchemaTargetFlushes.contains(writeTxId))
    {
        return;
    }

    ActiveSchemaTargetFlush = key;
    SchemaTargetFlushes.emplace(writeTxId, key);
    ctx.Send(MakeTxProxyID(), MakeCommitProposal(writeTxId, {target->GetDstPath()}).Release(), 0, writeTxId);
}

void TController::StopSchemaChangeTargetFlush(const std::pair<ui64, ui64>& key) {
    if (ActiveSchemaTargetFlush == key) {
        ActiveSchemaTargetFlush.reset();
    }

    for (auto it = SchemaTargetFlushes.begin(); it != SchemaTargetFlushes.end();) {
        if (it->second == key) {
            SchemaTargetFlushes.erase(it++);
        } else {
            ++it;
        }
    }
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
            || it->second.DstAlterTxId != Event->Get()->DstAlterTxId)
        {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        if (!Event->Get()->IsSuccess()) {
            auto replication = Self->Find(key.first);
            auto* target = replication ? replication->FindTarget(key.second) : nullptr;
            it->second.Phase = ESchemaBarrierPhase::Error;
            db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::Targets::SchemaBarrierPhase>(static_cast<ui8>(ESchemaBarrierPhase::Error)));
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
        db.Table<Schema::Targets>().Key(key.first, key.second).Update(
            NIceDb::TUpdate<Schema::Targets::SchemaBarrierPhase>(static_cast<ui8>(it->second.Phase)));

        if (AwaitPostSchemaHeartbeats) {
            // Require a quorum strictly newer than the schema DDL. Persisting
            // the reset prevents recovery from accepting the old quorum.
            Self->WorkersWithHeartbeat.clear();
            Self->WorkersByHeartbeat.clear();
            for (auto& [workerId, worker] : Self->Workers) {
                worker.ClearHeartbeat();
                db.Table<Schema::Workers>()
                    .Key(workerId.ReplicationId(), workerId.TargetId(), workerId.WorkerId())
                    .Update(
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
            Self->SendSchemaChangeResult(id, Schema, ReleaseOffsets.at(id), false, false, ctx);
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
