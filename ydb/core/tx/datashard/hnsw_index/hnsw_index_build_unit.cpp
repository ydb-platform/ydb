#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/tx/datashard/datashard_pipeline.h>
#include <ydb/core/tx/datashard/execution_unit_ctors.h>
#include <ydb/core/tx/datashard/hnsw_index/hnsw_index_build_unit.h>

namespace NKikimr::NDataShard {

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

} // namespace

void AddHnswIndexBuildToExecutionPlan(TActiveTransaction& tx) {
    if (!AppData()->FeatureFlags.GetEnableHnswIndex()
            || !IsHnswPostingTablePublication(tx.GetSchemeTx())) {
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
        const auto* description = FindHnswIndexDescription(alter);
        const auto& hnsw = description->GetHnswIndexDescription();
        // CompletedOperations releases the transaction body before Complete().
        // Keep requests separate for operations sharing this execution unit.
        PendingRequests[op->GetTxId()] = {
            alter.HasPathId() ? alter.GetPathId().GetLocalId() : alter.GetId_Deprecated(),
            TPathId(description->GetPathOwnerId(), description->GetLocalPathId()),
            hnsw.GetEmbeddingColumnId(), hnsw.GetSettings().settings()};
        return EExecutionStatus::DelayComplete;
    }

    void Complete(TOperation::TPtr op, const TActorContext&) override {
        const auto pending = PendingRequests.find(op->GetTxId());
        if (pending == PendingRequests.end()) {
            return;
        }
        auto request = std::move(pending->second);
        PendingRequests.erase(pending);
        if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
            return;
        }
        const auto table = DataShard.GetUserTables().find(request.TableId);
        if (table == DataShard.GetUserTables().end()) {
            return;
        }
        const auto localTid = table->second->LocalTid;
        const auto* entry = DataShard.FindHnswIndexEntry(localTid, request.IndexId);
        if (entry && !entry->Building && !entry->Index) {
            // Publishing shadow data supersedes an earlier below-threshold scan.
            DataShard.DeferHnswIndexBuild(localTid, TDuration::Zero(), request.IndexId);
        }
        DataShard.RequestHnswIndexBuild(localTid, request.VectorColumnTag,
            request.Settings, request.IndexId);
    }

private:
    struct TBuildRequest {
        ui64 TableId;
        TPathId IndexId;
        ui32 VectorColumnTag;
        Ydb::Table::VectorIndexSettings Settings;
    };
    THashMap<ui64, TBuildRequest> PendingRequests;
};

THolder<TExecutionUnit> CreateBuildHnswIndexUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new THnswIndexBuildUnit(dataShard, pipeline));
}

} // namespace NKikimr::NDataShard
