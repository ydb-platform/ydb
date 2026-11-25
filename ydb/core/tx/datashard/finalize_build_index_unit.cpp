#include "datashard_impl.h"
#include "cdc_schema_change.h"
#include "datashard_locks_db.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

#include <algorithm>

namespace NKikimr {
namespace NDataShard {

using NSchemeShard::TEvIndexBuilder;

void TDataShard::SendLegacyColumnBuildLookup(const TActorContext& ctx) {
    const auto& lookup = *LegacyColumnBuildLookup;
    if (lookup.TablePath.empty()) {
        // Resolve by ID because the path stored by DataShard may be stale after a move.
        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>(lookup.TableId);
        auto* options = request->Record.MutableOptions();
        options->SetReturnPartitioningInfo(false);
        options->SetReturnPartitionConfig(false);
        options->SetReturnChildren(false);
        SendViaSchemeshardPipe(ctx, CurrentSchemeShardId, BuildIndexPipe, std::move(request), lookup.TxId);
    } else {
        // The GET request accepts any resolved path in the build's domain.
        auto request = MakeHolder<TEvIndexBuilder::TEvGetRequest>(lookup.TablePath, lookup.BuildId);
        SendViaSchemeshardPipe(ctx, CurrentSchemeShardId, BuildIndexPipe, std::move(request), lookup.TxId);
    }
}

void TDataShard::Handle(TEvPrivate::TEvRetryLegacyColumnBuildLookup::TPtr& ev, const TActorContext& ctx) {
    if (!LegacyColumnBuildLookup || LegacyColumnBuildLookup->TxId != ev->Get()->TxId || LegacyColumnBuildLookup->Cancelled.has_value()) {
        return;
    }

    const auto op = Pipeline.FindOp(LegacyColumnBuildLookup->TxId);
    if (!op || op->GetCurrentUnit() != EExecutionUnitKind::FinalizeBuildIndex) {
        LegacyColumnBuildLookup.reset();
        return;
    }

    SendLegacyColumnBuildLookup(ctx);
    ctx.Schedule(TDuration::Seconds(1), new TEvPrivate::TEvRetryLegacyColumnBuildLookup(op->GetTxId()));
}

bool TDataShard::HandleLegacyColumnBuildPath(const NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
    if (!LegacyColumnBuildLookup || LegacyColumnBuildLookup->TxId != ev->Cookie) {
        return false;
    }

    auto& lookup = *LegacyColumnBuildLookup;
    if (lookup.Cancelled.has_value() || !lookup.TablePath.empty()) {
        return true;
    }

    const auto& record = ev->Get()->GetRecord();
    if (record.GetStatus() == NKikimrScheme::StatusPathDoesNotExist) {
        // A force drop may remove the table and its build while finalization is pending.
        CompleteLegacyColumnBuildLookup(true, ctx);
    } else if (record.GetStatus() == NKikimrScheme::StatusSuccess &&
               TPathId(record.GetPathOwnerId(), record.GetPathId()) == lookup.TableId && !record.GetPath().empty()) {
        lookup.TablePath = record.GetPath();
        SendLegacyColumnBuildLookup(ctx);
    }
    return true;
}

void TDataShard::Handle(TEvIndexBuilder::TEvGetResponse::TPtr& ev, const TActorContext& ctx) {
    if (!LegacyColumnBuildLookup || LegacyColumnBuildLookup->TxId != ev->Cookie || LegacyColumnBuildLookup->Cancelled.has_value()) {
        return;
    }

    auto& lookup = *LegacyColumnBuildLookup;
    const auto& record = ev->Get()->Record;
    if (record.GetStatus() == Ydb::StatusIds::PRECONDITION_FAILED) {
        // The owning SchemeShard no longer has this build.
        CompleteLegacyColumnBuildLookup(true, ctx);
        return;
    }
    if (record.GetStatus() == Ydb::StatusIds::BAD_REQUEST) {
        lookup.TablePath.clear();
        return;
    }
    const auto& build = record.GetIndexBuild();
    if (record.GetStatus() != Ydb::StatusIds::SUCCESS || build.GetId() != lookup.BuildId ||
        !build.GetSettings().has_column_build_operation()) {
        return;
    }
    if (build.GetSettings().source_path() != lookup.TablePath) {
        lookup.TablePath.clear();
        return;
    }

    // SchemeShard keeps the outcome in the build state until its finalization completes.
    switch (build.GetState()) {
        case Ydb::Table::IndexBuildState::STATE_APPLYING:
            CompleteLegacyColumnBuildLookup(false, ctx);
            return;
        case Ydb::Table::IndexBuildState::STATE_CANCELLATION:
        case Ydb::Table::IndexBuildState::STATE_CANCELLED:
        case Ydb::Table::IndexBuildState::STATE_REJECTION:
        case Ydb::Table::IndexBuildState::STATE_REJECTED:
            CompleteLegacyColumnBuildLookup(true, ctx);
            return;
        default:
            return;
    }
}

void TDataShard::CompleteLegacyColumnBuildLookup(bool cancelled, const TActorContext& ctx) {
    auto& lookup = *LegacyColumnBuildLookup;
    lookup.Cancelled = cancelled;
    if (auto op = Pipeline.FindOp(lookup.TxId); op && op->GetCurrentUnit() == EExecutionUnitKind::FinalizeBuildIndex) {
        Pipeline.AddCandidateOp(op);
        PlanQueue.Progress(ctx);
    } else {
        LegacyColumnBuildLookup.reset();
    }
}

class TFinalizeBuildIndexUnit : public TExecutionUnit {
    THolder<TEvChangeExchange::TEvRemoveSender> RemoveSender;

public:
    TFinalizeBuildIndexUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::FinalizeBuildIndex, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        const auto& lookup = DataShard.LegacyColumnBuildLookup;
        return !lookup || lookup->TxId != op->GetTxId() || lookup->Cancelled.has_value();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ENSURE(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasFinalizeBuildIndex()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetFinalizeBuildIndex();

        const auto pathId = TPathId::FromProto(params.GetPathId());
        Y_ENSURE(pathId.OwnerId == DataShard.GetPathOwnerId());

        const auto version = params.GetTableSchemaVersion();
        Y_ENSURE(version);

        bool legacyCancelled = false;
        if (!params.HasOutcome() && !params.GetColumnBuildSucceeded()) {
            const auto table = DataShard.FindUserTable(pathId);
            Y_ENSURE(table);
            if (std::ranges::any_of(table->Columns, [](const auto& entry) { return entry.second.IsBuildInProgress; })) {
                auto& lookup = DataShard.LegacyColumnBuildLookup;
                if (!lookup || lookup->TxId != op->GetTxId()) {
                    lookup = TDataShard::TLegacyColumnBuildLookup{op->GetTxId(), pathId, params.GetBuildIndexId(), {}, {}};
                    DataShard.SendLegacyColumnBuildLookup(ctx);
                    ctx.Schedule(TDuration::Seconds(1), new TDataShard::TEvPrivate::TEvRetryLegacyColumnBuildLookup(op->GetTxId()));
                    return EExecutionStatus::Continue;
                }
                legacyCancelled = lookup->Cancelled.value();
                lookup.reset();
            }
        }

        TUserTable::TPtr tableInfo;
        bool schemaChanged = false;
        if (params.HasOutcome() && params.GetOutcome().HasApply()) {
            const auto indexPathId = TPathId::FromProto(params.GetOutcome().GetApply().GetIndexPathId());

            const auto oldTable = DataShard.FindUserTable(pathId);
            if (const auto it = oldTable->Indexes.find(indexPathId); it != oldTable->Indexes.end()) {
                schemaChanged = it->second.State != NKikimrSchemeOp::EIndexStateReady;
            }

            tableInfo = DataShard.AlterTableSwitchIndexState(ctx, txc, pathId, version, indexPathId, NKikimrSchemeOp::EIndexStateReady);
        } else if (legacyCancelled || (params.HasOutcome() && params.GetOutcome().HasCancel())) {
            const auto& cancel = params.GetOutcome().GetCancel();
            if (cancel.HasIndexPathId()) {
                const auto indexPathId = TPathId::FromProto(cancel.GetIndexPathId());

                const auto& userTables = DataShard.GetUserTables();
                Y_ENSURE(userTables.contains(pathId.LocalPathId));
                userTables.at(pathId.LocalPathId)->ForAsyncIndex(indexPathId, [&](const auto&) {
                    RemoveSender.Reset(new TEvChangeExchange::TEvRemoveSender(indexPathId));
                });

                tableInfo = DataShard.AlterTableDropIndex(ctx, txc, pathId, version, indexPathId);
            } else {
                tableInfo = DataShard.AlterTableSchemaVersion(ctx, txc, pathId, version, true);
            }
        } else {
            // Successful column builds carry no outcome.
            tableInfo = DataShard.AlterTableSchemaVersion(ctx, txc, pathId, version, false);
            schemaChanged = tableInfo->FinishColumnBuilding();
            NIceDb::TNiceDb db(txc.DB);
            DataShard.PersistUserTable(db, pathId.LocalPathId, *tableInfo);
        }

        Y_ENSURE(tableInfo);
        TDataShardLocksDb locksDb(DataShard, txc);
        DataShard.ReplaceUserTable(pathId, tableInfo, locksDb);

        if (tableInfo->NeedSchemaSnapshots()) {
            DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, ctx);
        }

        if (schemaChanged) {
            PersistCdcSchemaChange(DataShard, txc, op, pathId, *tableInfo);
        }

        ui64 step = params.GetSnapshotStep();
        ui64 txId = params.GetSnapshotTxId();
        Y_ENSURE(step != 0);

        const ui64 buildIndexId = params.GetBuildIndexId();
        if (const auto* record = DataShard.GetScanManager().Get(buildIndexId)) {
            for (auto scanId : record->ScanIds) {
                DataShard.CancelScan(tableInfo->LocalTid, scanId);
            }
            DataShard.GetScanManager().Drop(buildIndexId);
        }

        NIceDb::TNiceDb db(txc.DB);
        const auto& scans = DataShard.GetBuildIndexScanManager().GetScans();
        if (const auto* info = scans.FindPtr(buildIndexId)) {
            DataShard.GetBuildIndexScanManager().PersistRemove(db, buildIndexId, info->SeqNoGeneration, info->SeqNoRound);
        }
        DataShard.ClearPendingBuildIndexFinalResponse(buildIndexId);

        const TSnapshotKey key(pathId, step, txId);
        DataShard.GetSnapshotManager().RemoveSnapshot(txc.DB, key);

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        DataShard.EnqueueChangeRecords(std::move(op->ChangeRecords()));
        if (RemoveSender) {
            ctx.Send(DataShard.GetChangeSender(), RemoveSender.Release());
        }
    }
};

THolder<TExecutionUnit> CreateFinalizeBuildIndexUnit(
    TDataShard& dataShard,
    TPipeline& pipeline)
{
    return THolder(new TFinalizeBuildIndexUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
