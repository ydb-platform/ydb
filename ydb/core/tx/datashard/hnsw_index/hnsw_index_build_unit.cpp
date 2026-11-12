#include <ydb/core/tx/datashard/backup_restore_common.h>
#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/tx/datashard/execution_unit_ctors.h>
#include <ydb/core/tx/datashard/hnsw_index/hnsw_index_build_actor.h>
#include <ydb/core/tx/datashard/hnsw_index/hnsw_index_build_unit.h>

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace NDataShard {

using namespace NActors;

namespace {

const NKikimrSchemeOp::TIndexDescription* FindHnswIndexDescription(
        const NKikimrSchemeOp::TTableDescription& alter) {
    for (const auto& index : alter.GetTableIndexes()) {
        if (index.GetType() == NKikimrSchemeOp::EIndexTypeGlobalHnsw
                && index.HasHnswIndexDescription()
                && index.GetHnswIndexDescription().HasEmbeddingColumnId()
                && index.GetHnswIndexDescription().GetSettings().has_settings()) {
            return &index;
        }
    }
    return nullptr;
}

bool IsHnswPostingTablePublication(const NKikimrTxDataShard::TFlatSchemeTransaction& schemeTx) {
    if (!schemeTx.HasAlterTable()) {
        return false;
    }
    const auto& alter = schemeTx.GetAlterTable();
    return alter.GetPartitionConfig().HasShadowData()
        && !alter.GetPartitionConfig().GetShadowData()
        && FindHnswIndexDescription(alter);
}

class THnswIndexBuildJob : public TActorBootstrapped<THnswIndexBuildJob>, public IActorExceptionHandler {
public:
    THnswIndexBuildJob(TActorId replyTo, ui64 txId, Ydb::Table::VectorIndexSettings settings,
            std::shared_ptr<std::atomic<bool>> cancelled)
        : ReplyTo(replyTo), TxId(txId), Settings(std::move(settings)), Cancelled(std::move(cancelled)) {}

    void Bootstrap() { Become(&TThis::StateScan); }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvAsyncJobComplete, HandleScan);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STFUNC(StateBuild) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvAsyncJobComplete, HandleBuild);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

    bool OnUnhandledException(const std::exception& error) override {
        LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
            "HNSW eager build job failed txId=" << TxId << ": " << error.what());
        PassAway();
        return true;
    }

    void PassAway() override {
        Cancelled->store(true, std::memory_order_relaxed);
        if (!Completed) {
            Complete(MakeHolder<THnswIndexBuildProduct>(nullptr, nullptr, "Eager build job cancelled"));
        }
        if (Worker) {
            Send(Worker, new TEvents::TEvPoison());
        }
        TActorBootstrapped::PassAway();
    }

private:
    void Complete(THolder<THnswIndexBuildProduct> product) {
        Completed = true;
        Send(ReplyTo, new TEvDataShard::TEvAsyncJobComplete(product.Release()), 0, TxId);
    }

    void HandleScan(TEvDataShard::TEvAsyncJobComplete::TPtr& ev) {
        auto* scan = CheckedCast<THnswSnapshotScanProduct*>(ev->Get()->Prod.Get());
        auto& result = scan->Result;
        if (!result.Success) {
            auto product = MakeHolder<THnswIndexBuildProduct>(nullptr, nullptr,
                "Snapshot scan aborted or exceeded its memory budget");
            product->BelowMinRows = result.BelowMinRows;
            product->Retryable = !result.BelowMinRows;
            Complete(std::move(product));
            PassAway();
            return;
        }
        const auto rowCount = result.Rows.size();
        Worker = Register(CreateHnswIndexBuildWorker(Settings, std::move(result.Rows),
            std::move(result.MemoryReservation), result.ReservedBytes,
            [replyTo = SelfId(), rowCount](THnswIndexBuildResult&& result, const TActorContext& ctx) {
                auto product = MakeHolder<THnswIndexBuildProduct>(std::move(result.Index),
                    std::move(result.MemoryReservation), std::move(result.Error));
                product->RowCount = rowCount;
                product->Retryable = !product->Index;
                ctx.Send(replyTo, new TEvDataShard::TEvAsyncJobComplete(product.Release()));
            }, result.AllowEmpty, Cancelled, std::move(result.ReduceReservation)), TMailboxType::HTSwap, AppData()->BatchPoolId);
        Become(&TThis::StateBuild);
    }

    void HandleBuild(TEvDataShard::TEvAsyncJobComplete::TPtr& ev) {
        Worker = {};
        Completed = true;
        Send(ReplyTo, new TEvDataShard::TEvAsyncJobComplete(ev->Get()->Prod.Release()), 0, TxId);
        PassAway();
    }

    const TActorId ReplyTo;
    const ui64 TxId;
    const Ydb::Table::VectorIndexSettings Settings;
    TActorId Worker;
    std::shared_ptr<std::atomic<bool>> Cancelled;
    bool Completed = false;
};

} // namespace

void AddHnswIndexBuildToExecutionPlan(TActiveTransaction& tx) {
    if (!AppData()->FeatureFlags.GetEnableHnswIndex()
            || !IsHnswPostingTablePublication(tx.GetSchemeTx())) {
        return;
    }

    // Recovery initially builds a plan without the transaction body. Insert the
    // restartable unit only after LoadTxDetails has established its eligibility,
    // before AlterTable commits and forbids further restarts in that transaction.
    const auto& plan = tx.GetExecutionPlan();
    TVector<EExecutionUnitKind> remaining(plan.begin() + tx.GetCurrentUnitIndex() + 1, plan.end());
    const auto alter = Find(remaining, EExecutionUnitKind::AlterTable);
    if (alter != remaining.end() && Find(remaining, EExecutionUnitKind::BuildHnswIndex) == remaining.end()) {
        remaining.insert(alter + 1, EExecutionUnitKind::BuildHnswIndex);
        tx.RewriteExecutionPlan(remaining);
    }
}

IActor* CreateHnswIndexBuildJob(const TActorId& replyTo, ui64 txId,
        const Ydb::Table::VectorIndexSettings& settings, std::shared_ptr<std::atomic<bool>> cancelled) {
    return new THnswIndexBuildJob(replyTo, txId, settings, std::move(cancelled));
}

// Builds the in-memory HNSW index for a vector index posting table as part of
// the ESchemeOpFinalizeBuildIndexImplTable alter that publishes it. This runs
// after TAlterMoveShadowUnit and TAlterTableUnit, so the uploaded rows are in
// the real table and its final column metadata is visible. The scheme
// transaction stays suspended until the build finishes (see
// TBackupRestoreUnitBase), so CREATE INDEX does not return before the index is
// usable, without ever blocking the tablet's transaction executor thread.
class THnswIndexBuildUnit : public TBackupRestoreUnitBase<TEvDataShard::TEvCancelBackup> {
protected:
    EExecutionStatus RunFailureStatus() const override {
        return RetryScheduled ? EExecutionStatus::Continue : EExecutionStatus::Executed;
    }

    bool IsRelevant(TActiveTransaction* tx) const override {
        if (!AppData()->FeatureFlags.GetEnableHnswIndex()
                && !tx->IsWaitingForAsyncJob() && !tx->IsWaitingForRestart()) {
            return false;
        }
        return IsHnswPostingTablePublication(tx->GetSchemeTx());
    }

    bool IsWaiting(TOperation::TPtr op) const override {
        return op->IsWaitingForAsyncJob() || op->IsWaitingForRestart();
    }

    void SetWaiting(TOperation::TPtr op) override {
        op->SetWaitingForAsyncJobFlag();
    }

    void ResetWaiting(TOperation::TPtr op) override {
        op->ResetWaitingForAsyncJobFlag();
        op->ResetWaitingForRestartFlag();
    }

    bool Run(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        RetryScheduled = false;
        if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
            return false;
        }
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        if (RetryTxId != op->GetTxId()) {
            RetryTxId = op->GetTxId();
            RetryCount = 0;
            BuildAttempts = 0;
            LocalTid = 0;
            BuildToken = 0;
            IndexId = {};
        }
        const auto& alter = tx->GetSchemeTx().GetAlterTable();

        ui64 tableId = alter.GetId_Deprecated();
        if (alter.HasPathId()) {
            Y_ENSURE(DataShard.GetPathOwnerId() == alter.GetPathId().GetOwnerId());
            tableId = alter.GetPathId().GetLocalId();
        }

        auto it = DataShard.GetUserTables().find(tableId);
        if (it == DataShard.GetUserTables().end()) {
            return false;
        }
        const TUserTable& table = *it->second;

        // The name map is updated only when this scheme transaction commits,
        // so use the column tag propagated by schemeshard directly.
        const auto* description = FindHnswIndexDescription(alter);
        Y_ENSURE(description);
        IndexId = TPathId(description->GetPathOwnerId(), description->GetLocalPathId());
        const auto& hnsw = description->GetHnswIndexDescription();
        const ui32 vectorColumnTag = hnsw.GetEmbeddingColumnId();
        if (!table.Columns.contains(vectorColumnTag)) {
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: embedding column tag=" << vectorColumnTag << " not found"
                << " in localTid=" << table.LocalTid << ", skipping index build");
            return false;
        }

        const auto& settings = hnsw.GetSettings().settings();
        if (settings.vector_type() != Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT) {
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: unsupported vector type for localTid=" << table.LocalTid
                << ", skipping index build");
            return false;
        }

        if (!DataShard.GetHnswCacheMemoryLimit()) {
            if (!DataShard.IsHnswCacheMemoryLimitKnown()) {
                // Registration and the first controller grant are asynchronous.
                // Completing here silently loses the eager build on new shards.
                return ScheduleMemoryRetry(op, ctx);
            }
            // An explicit zero grant (or absent controller) disables caching.
            return false;
        }

        BaseVersion = Max(TRowVersion(op->GetStep(), op->GetTxId()), DataShard.GetHnswBuildVersion());
        if (DataShard.GetHnswIndex(table.LocalTid, vectorColumnTag, settings, false, BaseVersion, IndexId)) {
            return false;
        }
        if (DataShard.GetVolatileTxManager().HasVolatileTxsAtSnapshot(BaseVersion)) {
            return ScheduleMemoryRetry(op, ctx);
        }
        if (!DataShard.TryStartHnswIndexBuild(table.LocalTid, vectorColumnTag, settings, BaseVersion, IndexId)) {
            const auto* entry = DataShard.FindHnswIndexEntry(table.LocalTid, IndexId);
            if (entry && !entry->Building) {
                // Eager finalization supersedes a previous lazy retry delay,
                // including a below-threshold scan that disabled lazy builds.
                DataShard.DeferHnswIndexBuild(table.LocalTid, TDuration::Zero(), IndexId);
            }
            return ScheduleMemoryRetry(op, ctx);
        }
        BuildToken = DataShard.GetHnswBuildToken(table.LocalTid, IndexId);
        DataShard.TrackHnswOpenTransactions(table.LocalTid, txc.DB, IndexId);
        LocalTid = table.LocalTid;
        VectorColumnTag = vectorColumnTag;
        Settings = settings;
        if (!DataShard.IsHnswBuildCurrent(LocalTid, BuildToken, IndexId)) {
            DataShard.HnswBuildsInFlight.erase(BuildToken);
            DataShard.DeferHnswIndexBuild(LocalTid, TDuration::Zero(), IndexId);
            return ScheduleMemoryRetry(op, ctx);
        }
        auto& entry = *DataShard.FindHnswIndexEntry(LocalTid, IndexId);
        const auto job = ctx.Register(CreateHnswIndexBuildJob(DataShard.SelfId(), op->GetTxId(), settings, entry.CancelBuild));
        entry.BuildActor = job;
        tx->SetAsyncJobActor(job);
        ScanControl = std::make_shared<THnswSnapshotScanControl>();
        DataShard.StartHnswSnapshotScan(LocalTid, it->second, BaseVersion, txc,
            [job](THnswSnapshotScanResult&& result) {
                TActivationContext::Send(new IEventHandle(job, TActorId(),
                    new TEvDataShard::TEvAsyncJobComplete(new THnswSnapshotScanProduct(std::move(result)))));
            }, IndexId, ScanControl);

        return true;
    }

    bool HasResult(TOperation::TPtr op) const override {
        return op->HasAsyncJobResult();
    }

    bool ProcessResult(TOperation::TPtr op, const TActorContext& ctx) override {
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        auto* result = CheckedCast<THnswIndexBuildProduct*>(op->AsyncJobResult().Get());
        bool retry = false;
        ScanControl.reset();
        // Empty graphs are constructions too, even when installation is rejected.
        if (result->Index || result->RowCount) {
            ++BuildAttempts;
        }
        DataShard.HnswBuildsInFlight.erase(BuildToken);
        const bool current = DataShard.IsHnswBuildCurrent(LocalTid, BuildToken, IndexId);
        if (current) {
            auto& entry = *DataShard.FindHnswIndexEntry(LocalTid, IndexId);
            entry.ScanId = 0;
            entry.BuildActor = {};
        }
        if (!current) {
            // A schema change, eviction or cancellation may already own a newer build.
            retry = AppData()->FeatureFlags.GetEnableHnswIndex() && RetryCount++ < 30;
        } else if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
            DataShard.InvalidateHnswIndex(LocalTid, IndexId);
            DataShard.DeferHnswIndexBuild(LocalTid, TDuration::Zero(), IndexId);
        } else if (result->Index) {
            LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: eager build completed for localTid=" << LocalTid
                << " size=" << result->Index->Size());
            DataShard.SetHnswIndex(LocalTid, std::move(result->Index),
                std::move(result->MemoryReservation), result->RowCount,
                VectorColumnTag, Settings, BaseVersion, BuildToken, IndexId);
            // Obsolete snapshots and limit changes can reject installation.
            // Retry a bounded number of constructions, then use scan fallback.
            retry = !DataShard.GetHnswIndex(LocalTid, VectorColumnTag,
                Settings, false, BaseVersion, IndexId);
        } else if (result->BelowMinRows) {
            const auto& entry = *DataShard.FindHnswIndexEntry(LocalTid, IndexId);
            if (entry.Changes->CountAfter(entry.BuildVersion)) {
                // Recheck a partition that changed after its snapshot rather
                // than waiting for an additional write to re-enable building.
                DataShard.DeferHnswIndexBuild(LocalTid, TDuration::Zero(), IndexId);
                retry = RetryCount++ < 30;
            } else {
                DataShard.DisableHnswIndexBuild(LocalTid, IndexId);
            }
        } else {
            DataShard.DeferHnswIndexBuild(LocalTid, TDuration::Seconds(5), IndexId);
            retry = result->Retryable && RetryCount++ < 30;
            // A failed build only costs acceleration, not correctness: reads
            // fall back to brute force.
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: eager build failed for localTid=" << LocalTid
                << ": " << result->Error);
        }

        op->SetAsyncJobResult(nullptr);
        tx->SetAsyncJobActor(TActorId());

        if (retry && BuildAttempts >= MaxBuildAttempts) {
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: eager installation rejected after " << BuildAttempts
                << " builds, completing with scan fallback");
            retry = false;
        }
        return !retry;
    }

    void Cancel(TActiveTransaction* tx, const TActorContext& ctx) override {
        if (ScanControl) {
            ScanControl->Cancelled = true;
            ScanControl.reset();
        }
        if (BuildToken && DataShard.GetHnswBuildToken(LocalTid, IndexId) == BuildToken) {
            DataShard.InvalidateHnswIndex(LocalTid, IndexId);
        }
        DataShard.HnswBuildsInFlight.erase(BuildToken);
        tx->KillAsyncJobActor(ctx);
    }

public:
    THnswIndexBuildUnit(TDataShard& self, TPipeline& pipeline)
        // Commit the new table metadata before capturing its scan snapshot.
        : TBase(EExecutionUnitKind::BuildHnswIndex, self, pipeline, true)
    {
    }

private:
    bool ScheduleMemoryRetry(TOperation::TPtr op, const TActorContext& ctx) {
        // Bound all admission/contention retries, including repeated obsolete
        // builds. HNSW is optional acceleration, so it cannot pin a scheme tx.
        constexpr ui32 MaxRetries = 30;
        if (RetryCount++ >= MaxRetries) {
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: cache admission did not succeed after "
                << MaxRetries << " retries, completing with scan fallback");
            return false;
        }
        return ScheduleRetry(op, ctx);
    }

    bool ScheduleRetry(TOperation::TPtr op, const TActorContext& ctx) {
        RetryScheduled = true;
        ScheduleRestart(op, ctx);
        return false;
    }

    static constexpr ui32 MaxBuildAttempts = 3;
    ui64 RetryTxId = 0;
    ui32 RetryCount = 0;
    ui32 BuildAttempts = 0;
    std::shared_ptr<THnswSnapshotScanControl> ScanControl;
    bool RetryScheduled = false;
    ui32 LocalTid = 0;
    TPathId IndexId;
    ui32 VectorColumnTag = 0;
    Ydb::Table::VectorIndexSettings Settings;
    TRowVersion BaseVersion = TRowVersion::Min();
    ui64 BuildToken = 0;
};

THolder<TExecutionUnit> CreateBuildHnswIndexUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new THnswIndexBuildUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
