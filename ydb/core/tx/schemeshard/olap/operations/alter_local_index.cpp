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
            << "TAlterLocalIndex TPropose"
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterLocalIndex);
        Y_ABORT_UNLESS(txState->State == TTxState::Propose);

        NIceDb::TNiceDb db(context.GetDB());

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Indexes.contains(path->PathId));
        TTableIndexInfo::TPtr indexData = context.SS->Indexes.at(path->PathId);
        context.SS->PersistTableIndex(db, path->PathId);
        context.SS->Indexes[path->PathId] = indexData->AlterData;

        path->PathState = TPathElement::EPathState::EPathStateNoChanges;
        context.SS->PersistPath(db, path->PathId);

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterLocalIndex);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TAlterLocalIndex: public TSubOperation {
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
            return MakeHolder<TDone>(OperationId, TPathElement::EPathState::EPathStateNoChanges);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& tableIndexCreation = Transaction.GetCreateTableIndex();
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = tableIndexCreation.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterLocalIndex Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", operationId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        NSchemeShard::TPath indexPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS).Dive(name);
        {
            NSchemeShard::TPath::TChecker checks = indexPath.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        Y_ABORT_UNLESS(indexPath.Base()->IsTableIndex());

        auto indexIt = context.SS->Indexes.find(indexPath.Base()->PathId);
        if (indexIt == context.SS->Indexes.end() || indexIt->second->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Index is already being altered");
            return result;
        }

        TString errStr;
        TTableIndexInfo::TPtr newIndexData = TTableIndexInfo::Create(tableIndexCreation, errStr);
        if (!newIndexData) {
            result->SetError(TEvSchemeShard::EStatus::StatusInvalidParameter, errStr);
            return result;
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, indexPath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabIndex(context.SS, indexPath.Base()->PathId);

        context.DbChanges.PersistPath(indexPath.Base()->PathId);
        context.DbChanges.PersistAlterIndex(indexPath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        TTableIndexInfo::TPtr alterData = indexIt->second->CreateNextVersion();
        if (!newIndexData->AlterData->IndexKeys.empty()) {
            alterData->IndexKeys = newIndexData->AlterData->IndexKeys;
        }
        alterData->State = NKikimrSchemeOp::EIndexStateReady;

        switch (indexIt->second->Type) {
            case NKikimrSchemeOp::EIndexTypeLocalBloomFilter: {
                std::get<NKikimrSchemeOp::TBloomFilter>(alterData->SpecializedIndexDescription).MergeFrom(
                    std::get<NKikimrSchemeOp::TBloomFilter>(newIndexData->AlterData->SpecializedIndexDescription));
                break;
            }
            case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter: {
                std::get<NKikimrSchemeOp::TBloomNGrammFilter>(alterData->SpecializedIndexDescription).MergeFrom(
                    std::get<NKikimrSchemeOp::TBloomNGrammFilter>(newIndexData->AlterData->SpecializedIndexDescription));
                break;
            }
            default: {
                Y_ABORT("unexpected index type in TAlterLocalIndex::Propose");
            }
        }

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterLocalIndex, indexPath.Base()->PathId);
        txState.State = TTxState::Propose;

        indexPath.Base()->PathState = TPathElement::EPathState::EPathStateAlter;
        indexPath.Base()->LastTxId = OperationId.GetTxId();

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterLocalIndex AbortPropose"
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterLocalIndex AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

} // anonymous namespace

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterLocalIndex(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterLocalIndex>(id, tx);
}

ISubOperation::TPtr CreateAlterLocalIndex(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TAlterLocalIndex>(id, state);
}

} // namespace NKikimr::NSchemeShard
