#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/tx/datashard/datashard_pipeline.h>
#include <ydb/core/tx/datashard/execution_unit_ctors.h>
#include <ydb/core/tx/datashard/hnsw_index/hnsw_index_build_unit.h>

namespace NKikimr::NDataShard {

namespace {

const NKikimrSchemeOp::TLocalHnswIndexDescription* FindLocalHnswIndexDescription(
        const NKikimrTxDataShard::TFlatSchemeTransaction& schemeTx) {
    for (const auto& index : schemeTx.GetLocalHnswIndexes()) {
        if (index.HasIndexId()
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
        && !alter.GetPartitionConfig().GetShadowData();
}

} // namespace

void AddHnswIndexBuildToExecutionPlan(TActiveTransaction& tx, bool hasHnswMetadata) {
    if (!AppData()->FeatureFlags.GetEnableHnswIndex()
            || !IsHnswPostingTablePublication(tx.GetSchemeTx())
            || (!hasHnswMetadata && !FindLocalHnswIndexDescription(tx.GetSchemeTx()))) {
        return;
    }

    // Recovery initially builds a plan without the transaction body. Insert the
    // background-build request only after LoadTxDetails establishes eligibility.
    const auto& plan = tx.GetExecutionPlan();
    TVector<EExecutionUnitKind> remaining(plan.begin() + tx.GetCurrentUnitIndex() + 1, plan.end());
    const auto alter = Find(remaining, EExecutionUnitKind::AlterTable);
    if (alter != remaining.end() && Find(remaining, EExecutionUnitKind::BuildHnswIndex) == remaining.end()) {
        remaining.insert(alter + 1, EExecutionUnitKind::BuildHnswIndex);
        tx.RewriteExecutionPlan(remaining);
    }
}

// Only request background construction after the publishing schema is durable.
// Neither the snapshot scan nor construction/retries hold the scheme barrier.
class THnswIndexBuildUnit : public TExecutionUnit {
public:
    THnswIndexBuildUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::BuildHnswIndex, false, self, pipeline)
    {}

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext&, const TActorContext&) override {
        if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
            return EExecutionStatus::Executed;
        }
        const auto* tx = static_cast<TActiveTransaction*>(op.Get());
        if (!IsHnswPostingTablePublication(tx->GetSchemeTx())) {
            return EExecutionStatus::Executed;
        }
        const auto& alter = tx->GetSchemeTx().GetAlterTable();
        // CompletedOperations releases the transaction body before Complete().
        // The settings and column ids already belong to the persisted user table.
        PendingTables[op->GetTxId()] = alter.HasPathId()
            ? alter.GetPathId().GetLocalId() : alter.GetId_Deprecated();
        return EExecutionStatus::DelayComplete;
    }

    void Complete(TOperation::TPtr op, const TActorContext&) override {
        const auto pending = PendingTables.find(op->GetTxId());
        if (pending == PendingTables.end()) {
            return;
        }
        const auto tableId = pending->second;
        PendingTables.erase(pending);
        if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
            return;
        }
        const auto table = DataShard.GetUserTables().find(tableId);
        if (table == DataShard.GetUserTables().end() || table->second->ShadowTid) {
            return;
        }
        const auto localTid = table->second->LocalTid;
        for (const auto& [indexId, metadata] : table->second->HnswIndexes) {
            const auto* entry = DataShard.FindHnswIndexEntry(localTid, indexId);
            if (entry && !entry->Building && !entry->Index) {
                // Publishing shadow data supersedes an earlier below-threshold scan.
                DataShard.DeferHnswIndexBuild(localTid, TDuration::Zero(), indexId);
            }
            DataShard.RequestHnswIndexBuild(localTid, metadata.VectorColumnTag,
                metadata.Settings, indexId);
        }
    }

private:
    THashMap<ui64, ui64> PendingTables;
};

THolder<TExecutionUnit> CreateBuildHnswIndexUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new THnswIndexBuildUnit(dataShard, pipeline));
}

} // namespace NKikimr::NDataShard
