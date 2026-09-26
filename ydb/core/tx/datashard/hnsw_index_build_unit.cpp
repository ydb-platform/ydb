#include "backup_restore_common.h"
#include "datashard_impl.h"
#include "execution_unit_ctors.h"
#include "hnsw_index_build_actor.h"
#include "hnsw_index_build_unit.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NDataShard {

using namespace NActors;

namespace {

// Collects (serialized key, embedding) pairs for every row of the posting table.
// Keep reservation and page-fault handling in sync with
// ScanVectorColumnForHnsw() in datashard__read_iterator.cpp.
// Runs inside the finalize scheme transaction, where a page fault is an ordinary
// transaction restart rather than a dropped attempt.
bool ScanPostingTableVectors(
    TTransactionContext& txc,
    const TUserTable& table,
    ui32 vectorColumnTag,
    const Ydb::Table::VectorIndexSettings& settings,
    TDataShard& dataShard,
    std::shared_ptr<void>& memoryReservation,
    ui64& reservedBytes,
    std::vector<std::pair<TString, TString>>& keysAndVectors, TRowVersion baseVersion)
{
    reservedBytes = 0;
    // KeyColumnIds is in key order, which is the order the read path expects
    // when it deserializes these keys back (see MaterializeHnswResults).
    std::vector<NTable::TTag> columns;
    columns.push_back(vectorColumnTag);
    for (ui32 keyColId : table.KeyColumnIds) {
        if (keyColId != vectorColumnTag) {
            columns.push_back(keyColId);
        }
    }

    auto precharge = txc.DB.Precharge(table.LocalTid, {}, {}, columns, 0, 0, 0,
        NTable::EDirection::Forward, baseVersion);
    if (!precharge.Ready) {
        return false;
    }

    const ui64 estimatedBytes = THnswIndex::EstimateMemoryBytes(
        precharge.ItemsPrecharged, settings.vector_dimension(),
        settings.has_hnsw_connectivity() ? settings.hnsw_connectivity() : 16);
    memoryReservation = dataShard.TryReserveHnswCacheMemory(estimatedBytes);
    if (!memoryReservation) {
        return true;
    }
    reservedBytes = estimatedBytes;

    keysAndVectors.clear();
    keysAndVectors.reserve(precharge.ItemsPrecharged);

    auto iter = txc.DB.IterateRange(table.LocalTid, {}, columns, baseVersion, nullptr, nullptr);
    while (true) {
        auto ready = iter->Next(NTable::ENext::All);
        if (ready == NTable::EReady::Page) {
            return false;
        }
        if (ready == NTable::EReady::Gone) {
            break;
        }

        TDbTupleRef keyData = iter->GetKey();
        if (keyData.Cells().size() == 0) {
            continue;
        }

        TDbTupleRef rowData = iter->GetValues();
        if (rowData.Cells().size() > 0 && !rowData.Cells()[0].IsNull()) {
            keysAndVectors.emplace_back(
                TSerializedCellVec::Serialize(keyData.Cells()),
                TString(rowData.Cells()[0].AsBuf()));
        }
    }

    size_t keyBytes = 0;
    for (const auto& [key, _] : keysAndVectors) {
        keyBytes += key.size();
    }
    // Precharge does not count rows held in memtables. Include the actual
    // scanned rows as well as their keys before handing the build its budget.
    const ui64 requiredBytes = THnswIndex::EstimateMemoryBytes(
        keysAndVectors.size(), settings.vector_dimension(),
        settings.has_hnsw_connectivity() ? settings.hnsw_connectivity() : 16, keyBytes);
    if (requiredBytes > reservedBytes) {
        auto additionalReservation = dataShard.TryReserveHnswCacheMemory(requiredBytes - reservedBytes);
        if (!additionalReservation) {
            keysAndVectors.clear();
            memoryReservation.reset();
            return true;
        }
        struct TCombinedReservation {
            std::shared_ptr<void> Initial;
            std::shared_ptr<void> Additional;
        };
        memoryReservation = std::make_shared<TCombinedReservation>(
            TCombinedReservation{std::move(memoryReservation), std::move(additionalReservation)});
        reservedBytes = requiredBytes;
    }

    return true;
}

} // namespace

IActor* CreateHnswIndexBuildJob(
        const TActorId& replyTo,
        ui64 txId,
        const Ydb::Table::VectorIndexSettings& settings,
        std::vector<std::pair<TString, TString>> keysAndVectors,
        std::shared_ptr<void> memoryReservation,
        ui64 maxMemoryBytes)
{
    return CreateHnswIndexBuildWorker(settings, std::move(keysAndVectors),
        std::move(memoryReservation), maxMemoryBytes,
        [replyTo, txId](THnswIndexBuildResult&& result, const TActorContext& ctx) mutable {
            TAutoPtr<IDestructable> product = new THnswIndexBuildProduct(
                std::move(result.Index), std::move(result.MemoryReservation),
                std::move(result.Error));
            ctx.Send(replyTo, new TEvDataShard::TEvAsyncJobComplete(product), 0, txId);
        });
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
        if (PageFault) {
            return EExecutionStatus::Restart;
        }
        return RetryScheduled ? EExecutionStatus::Continue : EExecutionStatus::Executed;
    }

    bool IsRelevant(TActiveTransaction* tx) const override {
        const auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasAlterTable()) {
            return false;
        }
        const auto& alter = schemeTx.GetAlterTable();
        return alter.HasVectorIndexKmeansTreeDescription()
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
        PageFault = false;
        RetryScheduled = false;
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

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
            return ScheduleRetry(op, ctx);
        }
        BuildToken = DataShard.GetHnswBuildToken(table.LocalTid);
        DataShard.TrackHnswOpenTransactions(table.LocalTid, txc.DB);
        std::vector<std::pair<TString, TString>> keysAndVectors;
        ui64 reservedBytes = 0;
        std::shared_ptr<void> memoryReservation;
        if (!ScanPostingTableVectors(txc, table, vectorColumnTag, settings, DataShard,
                memoryReservation, reservedBytes, keysAndVectors, BaseVersion)) {
            DataShard.DeferHnswIndexBuild(table.LocalTid, TDuration::Zero());
            // Page fault: this unit is re-executed after the pages are fetched.
            PageFault = true;
            return false;
        }

        if (!memoryReservation) {
            DataShard.DeferHnswIndexBuild(table.LocalTid, TDuration::Zero());
            LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: waiting for cache memory budget for localTid=" << table.LocalTid);
            // The failed reservation reports demand to the controller. Allow
            // it to grow this shard's share, but keep the index usable when
            // the configured budget cannot accommodate the optional graph.
            return ScheduleMemoryRetry(op, ctx);
        }
        MemoryRetryCount = 0;

        if (keysAndVectors.empty()) {
            DataShard.SetHnswIndexBuilding(table.LocalTid, false);
            return false;
        }
        if (keysAndVectors.size() < GetHnswMinRows(settings)) {
            DataShard.SetHnswIndexBuilding(table.LocalTid, false);
            LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: partition is below hnsw_min_rows for localTid=" << table.LocalTid
                << " rows=" << keysAndVectors.size()
                << " minimum=" << GetHnswMinRows(settings));
            return false;
        }

        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
            << " HNSW: starting eager build for localTid=" << table.LocalTid
            << " rows=" << keysAndVectors.size());

        LocalTid = table.LocalTid;
        VectorColumnTag = vectorColumnTag;
        Settings = settings;
        RowCountAtBuild = keysAndVectors.size();
        tx->SetAsyncJobActor(ctx.Register(
            CreateHnswIndexBuildJob(DataShard.SelfId(), op->GetTxId(), settings,
                std::move(keysAndVectors), std::move(memoryReservation), reservedBytes),
            TMailboxType::HTSwap,
            AppData(ctx)->BatchPoolId));

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
        if (result->Index) {
            LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: eager build completed for localTid=" << LocalTid
                << " size=" << result->Index->Size());
            DataShard.SetHnswIndex(LocalTid, std::move(result->Index),
                std::move(result->MemoryReservation), RowCountAtBuild,
                VectorColumnTag, Settings, BaseVersion, BuildToken);
            // A limit change can reject installation after construction. Such
            // a successful build must not publish completion with no cache.
            retry = !DataShard.GetHnswIndex(LocalTid, VectorColumnTag,
                Settings, false, BaseVersion);
        } else {
            DataShard.SetHnswIndexBuilding(LocalTid, false);
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

        return !retry;
    }

    void Cancel(TActiveTransaction* tx, const TActorContext& ctx) override {
        if (DataShard.IsHnswBuildCurrent(LocalTid, BuildToken)) {
            DataShard.DeferHnswIndexBuild(LocalTid, TDuration::Zero());
        }
        tx->KillAsyncJobActor(ctx);
    }

public:
    THnswIndexBuildUnit(TDataShard& self, TPipeline& pipeline)
        // The posting-table scan can page fault. Let the pipeline commit the
        // preceding AlterTable unit before entering this restartable unit.
        : TBase(EExecutionUnitKind::BuildHnswIndex, self, pipeline, true)
    {
    }

private:
    bool ScheduleMemoryRetry(TOperation::TPtr op, const TActorContext& ctx) {
        if (MemoryWaitTxId != op->GetTxId()) {
            MemoryWaitTxId = op->GetTxId();
            MemoryRetryCount = 0;
        }
        // Controller limits are refreshed once a second. Admission must not
        // hold a scheme operation forever when its graph cannot fit.
        constexpr ui32 MaxMemoryRetries = 30;
        if (MemoryRetryCount++ >= MaxMemoryRetries) {
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: cache memory admission did not succeed after "
                << MaxMemoryRetries << " retries, completing without eager cache");
            return false;
        }
        return ScheduleRetry(op, ctx);
    }

    bool ScheduleRetry(TOperation::TPtr op, const TActorContext& ctx) {
        RetryScheduled = true;
        ScheduleRestart(op, ctx);
        return false;
    }

    ui64 MemoryWaitTxId = 0;
    ui32 MemoryRetryCount = 0;
    bool RetryScheduled = false;
    ui32 LocalTid = 0;
    ui32 VectorColumnTag = 0;
    Ydb::Table::VectorIndexSettings Settings;
    ui64 RowCountAtBuild = 0;
    TRowVersion BaseVersion = TRowVersion::Min();
    ui64 BuildToken = 0;
    mutable bool PageFault = false;
};

THolder<TExecutionUnit> CreateBuildHnswIndexUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new THnswIndexBuildUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
