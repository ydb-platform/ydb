#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h" // for TransactionTemplate

#include "schemeshard_impl.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/subdomain.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;


/*
The meaning of the function is that in the code below, you need to iterate through
all the DataShards that relate to the children of the main table several times.
*/
template<typename F>
concept TDoFunc = requires(F f, TOperationContext& context, TPathId implTableChildPathId) {
    { f(context, implTableChildPathId) } -> std::same_as<void>;
};

void DoFuncOnTableChilds(TOperationContext& context, const TPath& tablePath, TDoFunc auto&& doFunc) {
    for (const auto& [childName, childPathId] : tablePath.Base()->GetChildren()) {
        Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
        TPath childPath = tablePath.Child(childName);

        if (childPath.IsDeleted() || !childPath->IsTableIndex()) {
            continue;
        }

        for (const auto& [implTableName, implTablePathId] : childPath.Base()->GetChildren()) {
            if (!context.SS->Tables.contains(implTablePathId)) {
                continue;
            }

            Y_UNUSED(implTableName);

            doFunc(context, implTablePathId);
        }
    }
}

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TTruncateTable TConfigureParts"
            << " operationId# " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvProposeTransactionResult"
                               << " at tabletId# " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvProposeTransactionResult"
                                << " message# " << ev->Get()->Record.ShortDebugString());

        return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateTable);

        if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " UpdatePartitioningForTableModification");
            NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
        }

        txState->ClearShardsInProgress();

        TPath tablePath = TPath::Init(txState->TargetPathId, context.SS);
        Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));

        struct TShardIdxHash {
            size_t operator()(const TShardIdx& idx) const {
                return idx.Hash();
            }
        };

        absl::flat_hash_map<TShardIdx, TPathId, TShardIdxHash> childPathIdByShardIdx;

        { // Fill childPathIdByShardIdx
            size_t sumSize = 0;

            auto countSum = [&](TOperationContext& context, TPathId implTablePathId) {
                TTableInfo::TPtr indexTable = context.SS->Tables.at(implTablePathId);
                sumSize += indexTable->GetPartitions().size();
            };

            DoFuncOnTableChilds(context, tablePath, std::move(countSum));

            childPathIdByShardIdx.max_load_factor(0.25);
            childPathIdByShardIdx.reserve(sumSize);

            auto fillChildPathIdByShardIdx = [&](TOperationContext& context, TPathId implTablePathId) {
                TTableInfo::TPtr indexTable = context.SS->Tables.at(implTablePathId);
                for (const auto& partition : indexTable->GetPartitions()) {
                    childPathIdByShardIdx[partition.ShardIdx] = implTablePathId;
                }
            };

            DoFuncOnTableChilds(context, tablePath, std::move(fillChildPathIdByShardIdx));
        }

        Y_ABORT_UNLESS(txState->Shards.size());
        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            auto idx = txState->Shards[i].Idx;
            auto datashardId = context.SS->ShardInfos[idx].TabletID;

            auto it = childPathIdByShardIdx.find(idx);

            TPathId targetPathId = txState->TargetPathId;
            if (it != childPathIdByShardIdx.end()) { // means that target is index
                targetPathId = it->second;
            }

            TString txBody;
            {
                auto seqNo = context.SS->StartRound(*txState);

                NKikimrTxDataShard::TFlatSchemeTransaction tx;
                context.SS->FillSeqNo(tx, seqNo);
                auto truncateTable = tx.MutableTruncateTable();
                targetPathId.ToProto(truncateTable->MutablePathId());
                Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);
            }

            auto event = context.SS->MakeDataShardProposal(targetPathId, OperationId, txBody, context.Ctx);
            context.OnComplete.BindMsgToPipe(OperationId, datashardId, idx, event.Release());
        }

        txState->UpdateShardsInProgress(TTxState::ConfigureParts);
        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TTruncateTable TPropose"
            << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvDataShard::TEvProposeTransactionResult::EventType});
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

       LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint()
                     << " HandleReply TEvSchemaChanged"
                     << " at tablet: " << ssId);
 
        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", stepId: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateTable);

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxTruncateTable);

        TSet<TTabletId> shardSet;
        for (const auto& shard : txState->Shards) {
            TShardIdx idx = shard.Idx;
            TTabletId tablet = context.SS->ShardInfos.at(idx).TabletID;
            shardSet.insert(tablet);
        }

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, shardSet);
        return false;
    }
};

class TTruncateTable: public TSubOperation {
public:
    using TSubOperation::TSubOperation;
    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::ProposedWaitParts;
        case TTxState::ProposedWaitParts:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::ProposedWaitParts:
            return MakeHolder<NTableState::TProposedWaitParts>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        THolder<TProposeResponse> result;
        result.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
            NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId)));

        if (!AppData()->FeatureFlags.GetEnableTruncateTable()) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, "TRUNCATE TABLE statement is not supported");
            return result;
        }

        TString errStr;
        const auto& op = Transaction.GetTruncateTable();
        const auto stringTablePath = NKikimr::JoinPath({Transaction.GetWorkingDir(), op.GetTableName()});
        TPath tablePath = TPath::Resolve(stringTablePath, context.SS);
        {
            if (tablePath->IsColumnTable()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "Column tables don`t support TRUNCATE TABLE");
                return result;
            }

            if (!tablePath->IsTable()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "You can use TRUNCATE TABLE only on tables");
                return result;
            }

            TPath::TChecker checks = tablePath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsResolved()
                .NotDeleted()
                .NotBackupTable()
                .NotUnderTheSameOperation(OperationId.GetTxId())
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }

            // In the initial implementation, we forbid the table to have cdc streams and asynchronous indexes.
            // It will be fixed in the near future.
            for (const auto& [childName, childPathId] : tablePath.Base()->GetChildren()) {
                Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
                TPath srcChildPath = tablePath.Child(childName);

                if (srcChildPath.IsDeleted()) {
                    continue;
                }

                if (srcChildPath->IsCdcStream()) {
                    result->SetError(NKikimrScheme::StatusPreconditionFailed,
                        "Cannot truncate table with CDC streams");
                    return result;
                }
                
                if (srcChildPath.IsTableIndex(NKikimrSchemeOp::EIndexTypeGlobalAsync)) {
                    result->SetError(NKikimrScheme::StatusPreconditionFailed,
                        "Cannot truncate table with async indexes");
                    return result;
                }

            }
        }

        if (!context.SS->CheckLocks(tablePath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
        TTableInfo::TPtr table = context.SS->Tables.at(tablePath.Base()->PathId);
        Y_ABORT_UNLESS(table->GetPartitions().size());

        {
            tablePath.Base()->PathState = TPathElement::EPathState::EPathStateAlter;
            tablePath.Base()->LastTxId = OperationId.GetTxId();

            NIceDb::TNiceDb db(context.GetDB());

            TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxTruncateTable, tablePath.Base()->PathId);
            context.SS->ChangeTxState(db, OperationId, TTxState::ConfigureParts);

            context.SS->PersistPath(db, tablePath.Base()->PathId);

            auto persistShard = [&](const TTableShardInfo& shard) {
                auto shardIdx = shard.ShardIdx;
                TShardInfo& shardInfo = context.SS->ShardInfos[shardIdx];
                txState.Shards.emplace_back(shardIdx, ETabletType::DataShard, TTxState::ConfigureParts);
                shardInfo.CurrentTxId = OperationId.GetTxId();
                context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
            };

            for (const auto& shard : table->GetPartitions()) {
                persistShard(shard);
            }

            auto persistIndexShards = [&](TOperationContext& context, TPathId implTablePathId) {
                TTableInfo::TPtr indexTable = context.SS->Tables.at(implTablePathId);
                for (const auto& shard : indexTable->GetPartitions()) {
                    persistShard(shard);
                }
            };

            DoFuncOnTableChilds(context, tablePath, std::move(persistIndexShards));

            context.SS->PersistTxState(db, OperationId);

            context.OnComplete.ActivateTx(OperationId);
            SetState(NextState());
            Y_ABORT_UNLESS(txState.Shards.size());
        }

        // for (auto splitTx: table->GetSplitOpsInFlight()) {
        //     context.OnComplete.Dependence(splitTx.GetTxId(), opId.GetTxId());
        // }

        // IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, path, context.SS, context.OnComplete);

        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTruncateTable AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        AbortUnsafeDropOperation(OperationId, forceDropTxId, context);
    }
};

} // anonymous namespace

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateTruncateTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TTruncateTable>(id, tx);
}

ISubOperation::TPtr CreateTruncateTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TTruncateTable>(id, state);
}

}

