#include <ydb/core/tx/schemeshard/olap/operations/local_index_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TMoveLocalIndex TPropose"
            << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveLocalIndex);
        Y_ABORT_UNLESS(txState->State == TTxState::Propose);

        NIceDb::TNiceDb db(context.GetDB());

        txState->PlanStep = step;
        context.SS->PersistTxPlanStep(db, OperationId, step);

        // The target path is the NEW (destination) index
        TPathId dstPathId = txState->TargetPathId;
        TPathElement::TPtr dstPath = context.SS->PathsById.at(dstPathId);

        // Finalize the new index: set StepCreated and persist
        dstPath->StepCreated = step;
        context.SS->PersistCreateStep(db, dstPathId, step);

        Y_ABORT_UNLESS(context.SS->Indexes.contains(dstPathId));
        TTableIndexInfo::TPtr indexData = context.SS->Indexes.at(dstPathId);
        context.SS->PersistTableIndex(db, dstPathId);
        context.SS->Indexes[dstPathId] = indexData->AlterData;

        // Drop the source index path (stored in SourcePathId)
        TPathId srcPathId = txState->SourcePathId;
        if (context.SS->PathsById.contains(srcPathId)) {
            TPathElement::TPtr srcPath = context.SS->PathsById.at(srcPathId);
            if (!srcPath->Dropped()) {
                srcPath->SetDropped(step, OperationId.GetTxId());
                context.SS->PersistDropStep(db, srcPathId, step, OperationId);
                context.SS->PersistRemoveTableIndex(db, srcPathId);

                context.SS->TabletCounters->Simple()[COUNTER_TABLE_INDEXES_COUNT].Sub(1);

                context.SS->TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Sub(srcPath->UserAttrs->Size());
                context.SS->PersistUserAttributes(db, srcPathId, srcPath->UserAttrs, nullptr);

                auto domainInfo = context.SS->ResolveDomainInfo(srcPathId);
                domainInfo->DecPathsInside(context.SS);

                auto parentDir = TPath::Init(srcPathId, context.SS).Parent();
                DecAliveChildrenDirect(OperationId, parentDir.Base(), context);
            }
        }

        // Rename the index in the column table's schema (both proto and in-memory)
        TPath parentPath = TPath::Init(dstPathId, context.SS).Parent();
        TPathId tablePathId = parentPath->PathId;
        if (context.SS->ColumnTables.contains(tablePathId)) {
            auto tableInfo = context.SS->ColumnTables.GetVerifiedPtr(tablePathId);
            auto* schema = tableInfo->Description.MutableSchema();
            const TString srcName = TPath::Init(srcPathId, context.SS).LeafName();
            const TString dstName = TPath::Init(dstPathId, context.SS).LeafName();
            for (auto& indexProto : *schema->MutableIndexes()) {
                if (indexProto.GetName() == srcName) {
                    indexProto.SetName(dstName);
                    break;
                }
            }
            context.SS->PersistColumnTable(db, tablePathId, *tableInfo);
        }

        // Publish changes
        ++parentPath->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath.Base());

        context.SS->ClearDescribePathCaches(parentPath.Base());
        context.SS->ClearDescribePathCaches(dstPath);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentPath->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, dstPathId);
            if (context.SS->PathsById.contains(srcPathId)) {
                context.OnComplete.PublishToSchemeBoard(OperationId, srcPathId);
            }
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMoveLocalIndex);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TMoveLocalIndex: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& moving = Transaction.GetMoveIndex();
        const TString& mainTableStr = moving.GetTablePath();
        const TString& srcName = moving.GetSrcPath();
        const TString& dstName = moving.GetDstPath();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TMoveLocalIndex Propose"
                         << ", table: " << mainTableStr
                         << ", src: " << srcName
                         << ", dst: " << dstName
                         << ", operationId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        TPath mainTablePath = TPath::Resolve(mainTableStr, context.SS);
        {
            TPath::TChecker checks = mainTablePath.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted()
                .IsColumnTable()
                .IsUnderOperation()
                .IsUnderTheSameOperation(OperationId.GetTxId());

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TPath srcPath = mainTablePath.Child(srcName);
        {
            TPath::TChecker checks = srcPath.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted()
                .IsTableIndex()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        Y_ABORT_UNLESS(context.SS->Indexes.contains(srcPath.Base()->PathId));
        TTableIndexInfo::TPtr srcIndexInfo = context.SS->Indexes.at(srcPath.Base()->PathId);

        TPath dstPath = mainTablePath.Child(dstName);
        {
            TPath::TChecker checks = dstPath.Check();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeTableIndex, false);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .PathsLimit()
                    .IsValidLeafName(context.UserToken.Get());
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        // Create new index as a copy of source using NotExistedYet + CreateNextVersion pattern
        TTableIndexInfo::TPtr newIndexData = TTableIndexInfo::NotExistedYet(srcIndexInfo->Type);
        TTableIndexInfo::TPtr alterData = newIndexData->CreateNextVersion();
        alterData->IndexKeys = srcIndexInfo->IndexKeys;
        alterData->IndexDataColumns = srcIndexInfo->IndexDataColumns;
        alterData->State = srcIndexInfo->State;
        alterData->SpecializedIndexDescription = srcIndexInfo->SpecializedIndexDescription;

        auto guard = context.DbGuard();
        TPathId allocatedPathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, allocatedPathId);
        context.MemChanges.GrabPath(context.SS, mainTablePath.Base()->PathId);
        context.MemChanges.GrabPath(context.SS, srcPath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabNewIndex(context.SS, allocatedPathId);

        context.DbChanges.PersistPath(allocatedPathId);
        context.DbChanges.PersistPath(mainTablePath.Base()->PathId);
        context.DbChanges.PersistPath(srcPath.Base()->PathId);
        context.DbChanges.PersistAlterIndex(allocatedPathId);
        context.DbChanges.PersistTxState(OperationId);

        dstPath.MaterializeLeaf(owner, allocatedPathId);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        auto newIndexPath = dstPath.Base();

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxMoveLocalIndex, newIndexPath->PathId);
        txState.State = TTxState::Propose;
        txState.SourcePathId = srcPath.Base()->PathId;
        context.SS->IncrementPathDbRefCount(srcPath.Base()->PathId);

        newIndexPath->PathState = NKikimrSchemeOp::EPathStateCreate;
        newIndexPath->CreateTxId = OperationId.GetTxId();
        newIndexPath->LastTxId = OperationId.GetTxId();
        newIndexPath->PathType = TPathElement::EPathType::EPathTypeTableIndex;

        srcPath.Base()->PathState = TPathElement::EPathState::EPathStateDrop;
        srcPath.Base()->DropTxId = OperationId.GetTxId();
        srcPath.Base()->LastTxId = OperationId.GetTxId();

        context.SS->Indexes[newIndexPath->PathId] = newIndexData;
        context.SS->IncrementPathDbRefCount(newIndexPath->PathId);

        context.OnComplete.ActivateTx(OperationId);

        dstPath.DomainInfo()->IncPathsInside(context.SS);
        IncAliveChildrenSafeWithUndo(OperationId, mainTablePath, context);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TMoveLocalIndex AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TMoveLocalIndex AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

} // anonymous namespace

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateMoveLocalIndex(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TMoveLocalIndex>(id, tx);
}

ISubOperation::TPtr CreateMoveLocalIndex(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TMoveLocalIndex>(id, state);
}

TVector<ISubOperation::TPtr> CreateConsistentMoveLocalIndex(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpMoveIndex);

    TVector<ISubOperation::TPtr> result;

    if (!context.SS->EnableMoveIndex) {
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, "Move index is not supported yet")};
    }

    const auto& moving = tx.GetMoveIndex();
    const auto& mainTable = moving.GetTablePath();
    const auto& srcIndex = moving.GetSrcPath();
    const auto& dstIndex = moving.GetDstPath();

    {
        TString errStr;
        if (!context.SS->CheckApplyIf(tx, errStr)) {
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, errStr)};
        }
    }

    TPath mainTablePath = TPath::Resolve(mainTable, context.SS);
    {
        TPath::TChecker checks = mainTablePath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsColumnTable()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsCommonSensePath();

        if (!checks) {
            return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
        }
    }

    TPath srcIndexPath = mainTablePath.Child(srcIndex);
    {
        TPath::TChecker checks = srcIndexPath.Check();
        checks
            .NotEmpty()
            .IsResolved()
            .NotDeleted()
            .IsTableIndex()
            .NotUnderDeleting()
            .NotUnderOperation();

        if (!checks) {
            return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
        }
    }

    // Put the parent column table under operation with an empty alter
    {
        TPath workingDir = mainTablePath.Parent();
        auto alterTx = TransactionTemplate(workingDir.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnTable);
        alterTx.SetInternal(true);
        auto* alter = alterTx.MutableAlterColumnTable();
        alter->SetName(mainTablePath.LeafName());
        alter->MutableAlterSchema();
        result.push_back(CreateAlterColumnTable(NextPartId(nextId, result), alterTx));
    }

    // Check destination doesn't already exist (as any path type)
    TPath dstIndexPath = mainTablePath.Child(dstIndex);
    if (dstIndexPath.IsResolved() && !dstIndexPath.IsDeleted()) {
        TString errStr = TStringBuilder()
            << "Path '" << dstIndex << "' already exists under '" << mainTable << "'";
        return {CreateReject(nextId, NKikimrScheme::StatusSchemeError, errStr)};
    }

    // The move sub-operation
    result.push_back(CreateMoveLocalIndex(NextPartId(nextId, result), tx));

    return result;
}

} // namespace NKikimr::NSchemeShard
