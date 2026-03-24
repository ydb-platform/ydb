#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/core/base/subdomain.h>

namespace NKikimr::NSchemeShard {

namespace {

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TReadOnlyCopyColumnTable TConfigureParts"
            << ", operationId: " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override  {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvProposeTransactionResult"
                               << " at tabletId# " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvProposeTransactionResult"
                                << " message# " << ev->Get()->Record.ShortDebugString());

        if (!NTableState::CollectProposeTransactionResults(OperationId, ev, context)) {
            return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxReadOnlyCopyColumnTable);
        Y_ABORT_UNLESS(txState->MinStep); // we have to have right minstep

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at tablet# " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxReadOnlyCopyColumnTable);
        Y_ABORT_UNLESS(txState->SourcePathId);

        TPath dstPath = TPath::Init(txState->TargetPathId, context.SS);
        Y_ABORT_UNLESS(dstPath.IsResolved());
        TPath srcPath = TPath::Init(txState->SourcePathId, context.SS);
        Y_ABORT_UNLESS(srcPath.IsResolved());
        Y_ABORT_UNLESS(srcPath->IsColumnTable());

        NIceDb::TNiceDb db(context.GetDB());

        // txState catches table shards
        if (!txState->Shards) {
            std::vector<TShardIdx> shardIdxs;
            const auto& srcTable =context.SS->ColumnTables.GetVerified(srcPath.Base()->PathId);
            shardIdxs.reserve(srcTable->GetShardIdsSet().size());
            for (const auto& id: srcTable->GetShardIdsSet()) {
                shardIdxs.emplace_back(context.SS->TabletIdToShardIdx.at(TTabletId(id)));
            }
            const auto tabletType = ETabletType::ColumnShard;
            for (const auto& shardIdx : shardIdxs) {
                TShardInfo& shardInfo = context.SS->ShardInfos[shardIdx];

                txState->Shards.emplace_back(shardIdx, tabletType, TTxState::ConfigureParts);

                shardInfo.CurrentTxId = OperationId.GetTxId();
                context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
                context.SS->SharedShards[shardIdx].insert(txState->TargetPathId);
                context.SS->PersistAddSharedShard(db, shardIdx, txState->TargetPathId);
            }
            context.SS->PersistTxState(db, OperationId);
        }
        Y_ABORT_UNLESS(txState->Shards.size());

        const auto& seqNo = context.SS->StartRound(*txState);

        TString txBody;
        NKikimrTxColumnShard::TSchemaTxBody tx;
        context.SS->FillSeqNo(tx, seqNo);
        auto copy = tx.MutableCopyTable();
        copy->SetSrcPathId(srcPath->PathId.LocalPathId);
        copy->SetDstPathId(dstPath->PathId.LocalPathId);
        Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);
        // send messages
        txState->ClearShardsInProgress();
        for (const auto& shard: txState->Shards) {
            auto idx = shard.Idx;
            auto tabletId = context.SS->ShardInfos[idx].TabletID;
            auto event = context.SS->MakeShardProposal(dstPath, OperationId, seqNo, txBody, context.Ctx);
            context.OnComplete.BindMsgToPipe(OperationId, tabletId, idx, event.Release());
        }
        txState->UpdateShardsInProgress(TTxState::ConfigureParts);
        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;
    TTxState::ETxState& NextState;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TReadOnlyCopyColumnTable TPropose"
            << ", operationId: " << OperationId;
    }
public:
    TPropose(TOperationId id, TTxState::ETxState& nextState)
        : OperationId(id)
        , NextState(nextState)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvDataShard::TEvProposeTransactionResult::EventType, TEvColumnShard::TEvProposeTransactionResult::EventType});
    }

    bool HandleReply(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply " << TEvSchemaChangedTraits<TEvColumnShard::TEvNotifyTxCompletionResult::TPtr>::GetName()
                     << " at tablet: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply " << TEvSchemaChangedTraits<TEvColumnShard::TEvNotifyTxCompletionResult::TPtr>::GetName()
                     << " triggered early"
                     << ", message: " << evRecord.ShortDebugString());

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxReadOnlyCopyColumnTable);

        auto srcPath = TPath::Init(txState->SourcePathId, context.SS);
        auto dstPath = TPath::Init(txState->TargetPathId, context.SS);

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        Y_ABORT_UNLESS(!context.SS->ColumnTables.contains(dstPath.Base()->PathId));
        auto srcTable = context.SS->ColumnTables.GetVerified(srcPath.Base()->PathId);
        auto tableInfo = context.SS->ColumnTables.BuildNew(dstPath.Base()->PathId, srcTable.GetPtr());
        tableInfo->AlterVersion += 1;
        context.SS->PersistColumnTable(db, dstPath.Base()->PathId, *tableInfo, false);
        context.SS->SetPartitioning(dstPath.Base()->PathId, tableInfo.GetPtr());
        context.SS->IncrementPathDbRefCount(dstPath.Base()->PathId, "copy table info");

        dstPath->StepCreated = step;
        context.SS->PersistCreateStep(db, dstPath.Base()->PathId, step);
        dstPath.DomainInfo()->IncPathsInside(context.SS);

        dstPath.Activate();
        IncParentDirAlterVersionWithRepublish(OperationId, dstPath, context);

        NextState = TTxState::WaitShadowPathPublication;
        context.SS->ChangeTxState(db, OperationId, TTxState::WaitShadowPathPublication);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxReadOnlyCopyColumnTable);
        Y_ABORT_UNLESS(txState->SourcePathId);
        Y_ABORT_UNLESS(txState->MinStep);

        TSet<TTabletId> shardSet;
        for (const auto& shard : txState->Shards) {
            TShardIdx idx = shard.Idx;
            TTabletId tablet = context.SS->ShardInfos.at(idx).TabletID;
            auto event = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(ui64(OperationId.GetTxId()));
            context.OnComplete.BindMsgToPipe(OperationId, tablet, shard.Idx, event.release());
            shardSet.insert(tablet);
        }

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, std::move(shardSet));
        return false;
    }
};

class TWaitCopiedPathPublication: public TSubOperationState {
private:
    TOperationId OperationId;

    TPathId ActivePathId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TReadOnlyCopyColumnTable TWaitCopiedPathPublication"
                << " operationId: " << OperationId;
    }

public:
    TWaitCopiedPathPublication(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvHive::TEvCreateTabletReply::EventType,
            TEvDataShard::TEvProposeTransactionResult::EventType,
            TEvColumnShard::TEvProposeTransactionResult::EventType,
            TEvPrivate::TEvOperationPlan::EventType,
            TEvPrivate::TEvCompletePublication::EventType,
        });
    }

    bool HandleReply(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply " << TEvSchemaChangedTraits<TEvColumnShard::TEvNotifyTxCompletionResult::TPtr>::GetName()
                               << ", save it"
                               << ", at schemeshard: " << ssId);

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvCompletePublication::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate::TEvCompletePublication"
                               << ", msg: " << ev->Get()->ToString()
                               << ", at tablet# " << ssId);

        Y_ABORT_UNLESS(ActivePathId == ev->Get()->PathId);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        context.OnComplete.RouteByTabletsFromOperation(OperationId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet# " << ssId);

        TPath srcPath = TPath::Init(txState->SourcePathId, context.SS);

        if (srcPath.IsActive()) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << ", no renaming has been detected for this operation");

            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Done);
            return true;
        }

        auto activePath = TPath::Resolve(srcPath.PathString(), context.SS);
        Y_ABORT_UNLESS(activePath.IsResolved());

        Y_ABORT_UNLESS(activePath != srcPath);

        ActivePathId = activePath->PathId;
        context.OnComplete.PublishAndWaitPublication(OperationId, activePath->PathId);

        return false;
    }
};

class TDone: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TReadOnlyCopyColumnTable TDone"
            << ", operationId: " << OperationId;
    }
public:
    TDone(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), AllIncomingEvents());
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxReadOnlyCopyColumnTable);

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", SourcePathId: " << txState->SourcePathId
                               << ", TargetPathId: " << txState->TargetPathId
                               << ", at schemeshard: " << ssId);

        NIceDb::TNiceDb db(context.GetDB());
        TPathElement::TPtr srcPath = context.SS->PathsById.at(txState->SourcePathId);
        context.OnComplete.ReleasePathState(OperationId, srcPath->PathId, TPathElement::EPathState::EPathStateNoChanges);

        TPathElement::TPtr dstPath = context.SS->PathsById.at(txState->TargetPathId);
        context.OnComplete.ReleasePathState(OperationId, dstPath->PathId, TPathElement::EPathState::EPathStateNoChanges);

        context.OnComplete.DoneOperation(OperationId);
        return true;
    }
};

class TReadOnlyCopyColumnTable: public TSubOperation {
    TTxState::ETxState AfterPropose = TTxState::Invalid;

    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return AfterPropose;
        case TTxState::WaitShadowPathPublication:
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
            return MakeHolder<TPropose>(OperationId, AfterPropose);
        case TTxState::WaitShadowPathPublication:
            return MakeHolder<TWaitCopiedPathPublication>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const auto& opDescr = Transaction.GetCreateColumnTable();

        const TString& parentPath = Transaction.GetWorkingDir();
        const TString& srcPathStr = opDescr.GetCopyFromTable();
        const TString& dstPathStr = opDescr.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TReadOnlyCopyColumnTable Propose"
                         << ", from: "<< srcPathStr
                         << ", to: " << dstPathStr
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        THolder<TProposeResponse> result;
        result.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
            NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId)));
            
        if (!AppData()->FeatureFlags.GetEnableColumnTablesBackup()) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, "Read-Only Copy Column Table is supported only for backups. Backups are disabled for the database.");
            return result;
        }

        if (!opDescr.GetIsBackup()) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, "Read-Only Copy Column Table is supported for backup only.");
            return result;
        }

        TString errStr;

        TPath srcPath = TPath::Resolve(srcPathStr, context.SS);
        {
            TPath::TChecker checks = srcPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotBackupTable()
                .NotAsyncReplicaTable()
                .NotUnderTheSameOperation(OperationId.GetTxId())
                .NotUnderOperation()
                .IsColumnTable();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TPath parent = TPath::Resolve(parentPath, context.SS);
        {
            TPath::TChecker checks = parent.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .FailOnRestrictedCreateInTempZone(Transaction.GetAllowCreateInTempDir());

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TPath dstPath = parent.Child(dstPathStr);
        TPath dstParent = dstPath.Parent();

        {
            TPath::TChecker checks = dstParent.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .FailOnRestrictedCreateInTempZone(Transaction.GetAllowCreateInTempDir());

                if (dstParent.IsUnderDeleting()) {
                    checks
                        .IsUnderDeleting()
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else if (dstParent.IsUnderMoving()) {
                    // it means that dstPath is free enough to be the copy destination
                    checks
                        .IsUnderMoving()
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else if (dstParent.IsUnderCreating()) {
                    checks
                        .IsUnderCreating()
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else {
                    checks
                        .NotUnderOperation();
                }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        if (dstParent.IsUnderOperation()) {
            dstPath = TPath::ResolveWithInactive(OperationId, dstPathStr, context.SS);
            dstParent = dstPath.Parent();
        }

        {
            TPath::TChecker checks = dstPath.Check();
            checks.IsAtLocalSchemeShard();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved();

                if (dstPath.IsUnderDeleting()) {
                    checks
                        .IsUnderDeleting()
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else if (dstPath.IsUnderMoving()) {
                    // it means that dstPath is free enough to be the copy destination
                    checks
                        .IsUnderMoving()
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else {
                    checks
                        .NotUnderTheSameOperation(OperationId.GetTxId())
                        .FailOnExist(TPathElement::EPathType::EPathTypeColumnTable, acceptExisted);
                }
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .DepthLimit()
                    .IsValidLeafName(context.UserToken.Get());
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!context.SS->CheckLocks(srcPath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        auto guard = context.DbGuard();
        TPathId allocatedPathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, allocatedPathId);
        context.MemChanges.GrabPath(context.SS, dstParent.Base()->PathId);
        context.MemChanges.GrabPath(context.SS, srcPath.Base()->PathId);
        context.MemChanges.GrabPath(context.SS, srcPath.Base()->ParentPathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabNewIndex(context.SS, allocatedPathId);

        context.DbChanges.PersistPath(allocatedPathId);
        context.DbChanges.PersistPath(dstParent.Base()->PathId);
        context.DbChanges.PersistPath(srcPath.Base()->PathId);
        context.DbChanges.PersistPath(srcPath.Base()->ParentPathId);
        context.DbChanges.PersistApplyUserAttrs(allocatedPathId);
        context.DbChanges.PersistTxState(OperationId);
        
        // copy attrs without any checks
        TUserAttributes::TPtr userAttrs = new TUserAttributes(1);
        userAttrs->Attrs = srcPath.Base()->UserAttrs->Attrs;

        // create new path and inherit properties from src
        dstPath.MaterializeLeaf(srcPath.Base()->Owner, allocatedPathId, /*allowInactivePath*/ true);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
        dstPath.Base()->CreateTxId = OperationId.GetTxId();
        dstPath.Base()->LastTxId = OperationId.GetTxId();
        dstPath.Base()->PathState = TPathElement::EPathState::EPathStateCreate;
        dstPath.Base()->PathType = srcPath.Base()->PathType;
        dstPath.Base()->UserAttrs->AlterData = userAttrs;
        dstPath.Base()->ACL = srcPath.Base()->ACL;

        IncAliveChildrenSafeWithUndo(OperationId, dstParent, context); // for correct discard of ChildrenExist prop

        // create tx state, do not catch shards right now
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxReadOnlyCopyColumnTable, dstPath.Base()->PathId, srcPath.Base()->PathId);
        txState.State = TTxState::ConfigureParts;

        srcPath->PathState = TPathElement::EPathState::EPathStateCopying;
        srcPath->LastTxId = OperationId.GetTxId();

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);
        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, srcPath, context.SS, context.OnComplete);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TReadOnlyCopyColumnTable AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TReadOnlyCopyColumnTable AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

ISubOperation::TPtr CreateReadOnlyCopyColumnTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TReadOnlyCopyColumnTable>(id, tx);
}

ISubOperation::TPtr CreateReadOnlyCopyColumnTable(TOperationId id, TTxState::ETxState txState) {
    Y_ABORT_UNLESS(txState != TTxState::Invalid);
    return MakeSubOperation<TReadOnlyCopyColumnTable>(id, txState);
}

}
