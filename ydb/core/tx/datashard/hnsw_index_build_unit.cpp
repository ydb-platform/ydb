#include "backup_restore_common.h"
#include "datashard_impl.h"
#include "execution_unit_ctors.h"
#include "hnsw_index_build_actor.h"
#include "hnsw_index_build_unit.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace NDataShard {

using namespace NActors;

namespace {

class THnswIndexBuildJob : public TActorBootstrapped<THnswIndexBuildJob> {
public:
    THnswIndexBuildJob(TActorId replyTo, ui64 txId, Ydb::Table::VectorIndexSettings settings)
        : ReplyTo(replyTo), TxId(txId), Settings(std::move(settings)) {}

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

    void PassAway() override {
        if (Worker) {
            Send(Worker, new TEvents::TEvPoison());
        }
        TActorBootstrapped::PassAway();
    }

private:
    void HandleScan(TEvDataShard::TEvAsyncJobComplete::TPtr& ev) {
        auto* scan = CheckedCast<THnswSnapshotScanProduct*>(ev->Get()->Prod.Get());
        auto& result = scan->Result;
        if (!result.Success) {
            auto product = MakeHolder<THnswIndexBuildProduct>(nullptr, nullptr,
                "Snapshot scan aborted or exceeded its memory budget");
            product->BelowMinRows = result.BelowMinRows;
            product->Retryable = !result.BelowMinRows;
            Send(ReplyTo, new TEvDataShard::TEvAsyncJobComplete(product.Release()), 0, TxId);
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
                ctx.Send(replyTo, new TEvDataShard::TEvAsyncJobComplete(product.Release()));
            }, result.AllowEmpty), TMailboxType::HTSwap, AppData()->BatchPoolId);
        Become(&TThis::StateBuild);
    }

    void HandleBuild(TEvDataShard::TEvAsyncJobComplete::TPtr& ev) {
        Worker = {};
        Send(ReplyTo, new TEvDataShard::TEvAsyncJobComplete(ev->Get()->Prod.Release()), 0, TxId);
        PassAway();
    }

    const TActorId ReplyTo;
    const ui64 TxId;
    const Ydb::Table::VectorIndexSettings Settings;
    TActorId Worker;
};

} // namespace

IActor* CreateHnswIndexBuildJob(const TActorId& replyTo, ui64 txId,
        const Ydb::Table::VectorIndexSettings& settings) {
    return new THnswIndexBuildJob(replyTo, txId, settings);
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
        if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
            return false;
        }
        const auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasAlterTable()) {
            return false;
        }
        const auto& alter = schemeTx.GetAlterTable();
        return alter.GetVectorIndexHnsw()
            && alter.HasVectorIndexKmeansTreeDescription()
            && alter.HasVectorIndexEmbeddingColumnId();
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
        const TString& embeddingColumn = alter.GetVectorIndexEmbeddingColumn();
        const ui32 vectorColumnTag = alter.GetVectorIndexEmbeddingColumnId();
        if (!table.Columns.contains(vectorColumnTag)) {
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: embedding column '" << embeddingColumn << "' tag=" << vectorColumnTag << " not found"
                << " in localTid=" << table.LocalTid << ", skipping index build");
            return false;
        }

        const auto& settings = alter.GetVectorIndexKmeansTreeDescription().GetSettings().settings();
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

        BaseVersion = TRowVersion(op->GetStep(), op->GetTxId());
        if (!DataShard.TryStartHnswIndexBuild(table.LocalTid, vectorColumnTag, settings, BaseVersion)) {
            const auto token = DataShard.GetHnswBuildToken(table.LocalTid);
            if (!DataShard.IsHnswBuildCurrent(table.LocalTid, token)
                    && !DataShard.IsHnswIndexBuildObsolete(table.LocalTid)) {
                // Eager finalization supersedes a previous lazy retry delay,
                // including a below-threshold scan that disabled lazy builds.
                DataShard.DeferHnswIndexBuild(table.LocalTid, TDuration::Zero());
            }
            return ScheduleMemoryRetry(op, ctx);
        }
        BuildToken = DataShard.GetHnswBuildToken(table.LocalTid);
        DataShard.TrackHnswOpenTransactions(table.LocalTid, txc.DB);
        LocalTid = table.LocalTid;
        VectorColumnTag = vectorColumnTag;
        Settings = settings;
        if (!DataShard.IsHnswBuildCurrent(LocalTid, BuildToken)) {
            DataShard.DeferHnswIndexBuild(LocalTid, TDuration::Zero());
            return ScheduleMemoryRetry(op, ctx);
        }
        const auto job = ctx.Register(CreateHnswIndexBuildJob(DataShard.SelfId(), op->GetTxId(), settings));
        tx->SetAsyncJobActor(job);
        ScanId = 0;
        DataShard.StartHnswSnapshotScan(LocalTid, it->second, BaseVersion, txc,
            [job](THnswSnapshotScanResult&& result) {
                TActivationContext::Send(new IEventHandle(job, TActorId(),
                    new TEvDataShard::TEvAsyncJobComplete(new THnswSnapshotScanProduct(std::move(result)))));
            }, &ScanId);

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
        ScanId = 0;
        BuildAttempts += result->RowCount != 0;
        if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
            DataShard.InvalidateHnswIndex(LocalTid);
            DataShard.DeferHnswIndexBuild(LocalTid, TDuration::Zero());
        } else if (result->Index) {
            LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: eager build completed for localTid=" << LocalTid
                << " size=" << result->Index->Size());
            DataShard.SetHnswIndex(LocalTid, std::move(result->Index),
                std::move(result->MemoryReservation), result->RowCount,
                VectorColumnTag, Settings, BaseVersion, BuildToken);
            // Obsolete snapshots and limit changes can reject installation.
            // Retry a bounded number of constructions, then use scan fallback.
            retry = !DataShard.GetHnswIndex(LocalTid, VectorColumnTag,
                Settings, false, BaseVersion);
        } else if (result->BelowMinRows) {
            DataShard.DisableHnswIndexBuild(LocalTid);
        } else {
            DataShard.DeferHnswIndexBuild(LocalTid, TDuration::Seconds(5));
            retry = result->Retryable && RetryCount++ < 30;
            // A failed build only costs acceleration, not correctness: reads
            // fall back to brute force.
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: eager build failed for localTid=" << LocalTid
                << ": " << result->Error);
        }

        if (DataShard.IsHnswIndexBuildObsolete(LocalTid)
                && DataShard.GetHnswBuildToken(LocalTid) == BuildToken) {
            DataShard.DeferHnswIndexBuild(LocalTid, TDuration::Zero());
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
        if (ScanId) {
            DataShard.CancelScan(LocalTid, ScanId);
            ScanId = 0;
        }
        if (DataShard.GetHnswBuildToken(LocalTid) == BuildToken) {
            DataShard.DeferHnswIndexBuild(LocalTid, TDuration::Zero());
        }
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
    ui64 ScanId = 0;
    bool RetryScheduled = false;
    ui32 LocalTid = 0;
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
