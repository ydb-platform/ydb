#include "backup_restore_common.h"
#include "datashard_impl.h"
#include "execution_unit_ctors.h"
#include "hnsw_index_build_unit.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace NDataShard {

using namespace NActors;

namespace {

// Collects (serialized key, embedding) pairs for every row of the posting table.
// Runs inside the finalize scheme transaction, where a page fault is an ordinary
// transaction restart rather than a dropped attempt.
bool ScanPostingTableVectors(
    TTransactionContext& txc,
    const TUserTable& table,
    ui32 vectorColumnTag,
    std::vector<std::pair<TString, TString>>& keysAndVectors)
{
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
        NTable::EDirection::Forward, TRowVersion::Max());
    if (!precharge.Ready) {
        return false;
    }

    keysAndVectors.clear();
    keysAndVectors.reserve(precharge.ItemsPrecharged);

    auto iter = txc.DB.IterateRange(table.LocalTid, {}, columns, TRowVersion::Max(), nullptr, nullptr);
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

    return true;
}

class THnswIndexBuildJob : public TActorBootstrapped<THnswIndexBuildJob> {
public:
    THnswIndexBuildJob(
            const TActorId& replyTo,
            ui64 txId,
            const Ydb::Table::VectorIndexSettings& settings,
            std::vector<std::pair<TString, TString>> keysAndVectors,
            ui64 maxMemoryBytes)
        : TActorBootstrapped(NKikimrServices::TActivity::DATASHARD_HNSW_BUILDER)
        , ReplyTo(replyTo)
        , TxId(txId)
        , Settings(settings)
        , KeysAndVectors(std::move(keysAndVectors))
        , MaxMemoryBytes(maxMemoryBytes)
    {}

    void Bootstrap(const TActorContext& ctx) {
        TString error;
        auto index = THnswIndex::Build(Settings, KeysAndVectors, MaxMemoryBytes, error);

        TAutoPtr<IDestructable> prod = new THnswIndexBuildProduct(
            index ? std::shared_ptr<THnswIndex>(std::move(index)) : nullptr,
            std::move(error));

        ctx.Send(ReplyTo, new TEvDataShard::TEvAsyncJobComplete(prod), 0, TxId);
        Die(ctx);
    }

private:
    const TActorId ReplyTo;
    const ui64 TxId;
    const Ydb::Table::VectorIndexSettings Settings;
    const std::vector<std::pair<TString, TString>> KeysAndVectors;
    const ui64 MaxMemoryBytes;
};

} // namespace

IActor* CreateHnswIndexBuildJob(
        const TActorId& replyTo,
        ui64 txId,
        const Ydb::Table::VectorIndexSettings& settings,
        std::vector<std::pair<TString, TString>> keysAndVectors,
        ui64 maxMemoryBytes)
{
    return new THnswIndexBuildJob(replyTo, txId, settings, std::move(keysAndVectors), maxMemoryBytes);
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
        return PageFault ? EExecutionStatus::Restart : EExecutionStatus::Executed;
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
        return op->IsWaitingForAsyncJob();
    }

    void SetWaiting(TOperation::TPtr op) override {
        op->SetWaitingForAsyncJobFlag();
    }

    void ResetWaiting(TOperation::TPtr op) override {
        op->ResetWaitingForAsyncJobFlag();
    }

    bool Run(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        PageFault = false;
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
        if (settings.hnsw_max_memory_bytes() == 0) {
            return false;
        }
        if (settings.vector_type() != Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT) {
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: unsupported vector type for localTid=" << table.LocalTid
                << ", skipping index build");
            return false;
        }

        std::vector<std::pair<TString, TString>> keysAndVectors;
        if (!ScanPostingTableVectors(txc, table, vectorColumnTag, keysAndVectors)) {
            // Page fault: this unit is re-executed after the pages are fetched.
            PageFault = true;
            return false;
        }

        if (keysAndVectors.empty()) {
            return false;
        }
        if (keysAndVectors.size() < settings.hnsw_min_rows()) {
            LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: partition is below hnsw_min_rows for localTid=" << table.LocalTid
                << " rows=" << keysAndVectors.size()
                << " minimum=" << settings.hnsw_min_rows());
            return false;
        }

        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
            << " HNSW: starting eager build for localTid=" << table.LocalTid
            << " rows=" << keysAndVectors.size());

        LocalTid = table.LocalTid;
        tx->SetAsyncJobActor(ctx.Register(
            CreateHnswIndexBuildJob(DataShard.SelfId(), op->GetTxId(), settings,
                std::move(keysAndVectors), settings.hnsw_max_memory_bytes()),
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
        if (result->Index) {
            LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: eager build completed for localTid=" << LocalTid
                << " size=" << result->Index->Size());
            DataShard.SetHnswIndex(LocalTid, result->Index);
        } else {
            // A failed build only costs acceleration, not correctness: reads
            // fall back to brute force.
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, DataShard.TabletID()
                << " HNSW: eager build failed for localTid=" << LocalTid
                << ": " << result->Error);
        }

        op->SetAsyncJobResult(nullptr);
        tx->SetAsyncJobActor(TActorId());

        return true;
    }

    void Cancel(TActiveTransaction* tx, const TActorContext& ctx) override {
        tx->KillAsyncJobActor(ctx);
    }

public:
    THnswIndexBuildUnit(TDataShard& self, TPipeline& pipeline)
        : TBase(EExecutionUnitKind::BuildHnswIndex, self, pipeline)
    {
    }

private:
    ui32 LocalTid = 0;
    mutable bool PageFault = false;
};

THolder<TExecutionUnit> CreateBuildHnswIndexUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new THnswIndexBuildUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
