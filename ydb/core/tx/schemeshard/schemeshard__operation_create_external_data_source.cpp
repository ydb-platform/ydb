#include "schemeshard__operation_common_external_data_source.h"
#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateExternalDataSource TPropose"
            << ", operationId: " << OperationId;
    }

public:
    explicit TPropose(TOperationId id)
        : OperationId(std::move(id))
    {
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateExternalDataSource);

        const auto pathId = txState->TargetPathId;
        const auto path = TPath::Init(pathId, context.SS);
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);

        context.SS->TabletCounters->Simple()[COUNTER_EXTERNAL_DATA_SOURCE_COUNT].Add(1);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        IncParentDirAlterVersionWithRepublish(OperationId, path, context);

        context.SS->ClearDescribePathCaches(pathPtr);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateExternalDataSource);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TCreateExternalDataSource : public TSubOperation {
    static TTxState::ETxState NextState() { return TTxState::Propose; }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result,
                                const TPath& dstPath,
                                const TString& acl,
                                bool acceptExisted) {
        const auto checks = dstPath.Check();
        checks.IsAtLocalSchemeShard();
        if (dstPath.IsResolved()) {
            checks
                .IsResolved()
                .NotUnderDeleting()
                .FailOnExist(TPathElement::EPathType::EPathTypeExternalDataSource, acceptExisted);
        } else {
            checks
                .NotEmpty()
                .NotResolved();
        }

        if (checks) {
            checks
                .IsValidLeafName()
                .DepthLimit()
                .PathsLimit()
                .DirChildrenLimit()
                .IsValidACL(acl);
        }

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
            if (dstPath.IsResolved()) {
                result->SetPathCreateTxId(static_cast<ui64>(dstPath.Base()->CreateTxId));
                result->SetPathId(dstPath.Base()->PathId.LocalPathId);
            }
        }

        return static_cast<bool>(checks);
    }

    bool IsApplyIfChecksPassed(const THolder<TProposeResponse>& result,
                               const TOperationContext& context) const {
        TString errorMessage;
        if (!context.SS->CheckApplyIf(Transaction, errorMessage)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errorMessage);
            return false;
        }
        return true;
    }

    static bool IsDescriptionValid(
        const THolder<TProposeResponse>& result,
        const NKikimrSchemeOp::TExternalDataSourceDescription& desc,
        const NExternalSource::IExternalSourceFactory::TPtr& factory) {
        TString errorMessage;
        if (!NExternalDataSource::Validate(desc, factory, errorMessage)) {
            result->SetError(NKikimrScheme::StatusSchemeError, errorMessage);
            return false;
        }
        return true;
    }

    static void AddPathInSchemeShard(
        const THolder<TProposeResponse>& result, TPath& dstPath, const TString& owner) {
        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
    }

    TPathElement::TPtr CreateExternalDataSourcePathElement(const TPath& dstPath) const {
        TPathElement::TPtr externalDataSource = dstPath.Base();

        externalDataSource->CreateTxId = OperationId.GetTxId();
        externalDataSource->PathType = TPathElement::EPathType::EPathTypeExternalDataSource;
        externalDataSource->PathState = TPathElement::EPathState::EPathStateCreate;
        externalDataSource->LastTxId  = OperationId.GetTxId();

        return externalDataSource;
    }

    void CreateTransaction(const TOperationContext &context,
                           const TPathId &externalDataSourcePathId) const {
        TTxState& txState = context.SS->CreateTx(OperationId,
                                                 TTxState::TxCreateExternalDataSource,
                                                 externalDataSourcePathId);
        txState.Shards.clear();
    }

    void RegisterParentPathDependencies(const TOperationContext& context,
                                        const TPath& parentPath) const {
        if (parentPath.Base()->HasActiveChanges()) {
            const TTxId parentTxId = parentPath.Base()->PlannedToCreate()
                                         ? parentPath.Base()->CreateTxId
                                         : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }
    }

    void AdvanceTransactionStateToPropose(const TOperationContext& context,
                                          NIceDb::TNiceDb& db) const {
        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
        context.OnComplete.ActivateTx(OperationId);
    }

    void PersistExternalDataSource(
        const TOperationContext& context,
        NIceDb::TNiceDb& db,
        const TPathElement::TPtr& externalDataSourcePath,
        const TExternalDataSourceInfo::TPtr& externalDataSourceInfo,
        const TString& acl) const {
        const auto& externalDataSourcePathId = externalDataSourcePath->PathId;

        context.SS->ExternalDataSources[externalDataSourcePathId] = externalDataSourceInfo;
        context.SS->IncrementPathDbRefCount(externalDataSourcePathId);

        if (!acl.empty()) {
            externalDataSourcePath->ApplyACL(acl);
        }
        context.SS->PersistPath(db, externalDataSourcePathId);

        context.SS->PersistExternalDataSource(db,
                                              externalDataSourcePathId,
                                              externalDataSourceInfo);
        context.SS->PersistTxState(db, OperationId);
    }

    static void UpdatePathSizeCounts(const TPath& parentPath, const TPath& dstPath) {
        dstPath.DomainInfo()->IncPathsInside();
        parentPath.Base()->IncAliveChildren();
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner,
                                      TOperationContext& context) override {
        const auto acceptExisted     = !Transaction.GetFailOnExist();
        const auto ssId              = context.SS->SelfTabletId();
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& externalDataSourceDescription =
            Transaction.GetCreateExternalDataSource();
        const TString& name = externalDataSourceDescription.GetName();

        LOG_N("TCreateExternalDataSource Propose"
              << ": opId# " << OperationId << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(ssId));

        const TPath parentPath = TPath::Resolve(parentPathStr, context.SS);
        RETURN_RESULT_UNLESS(NExternalDataSource::IsParentPathValid(
            result, parentPath, Transaction, /* isCreate */ true));

        const TString acl = Transaction.GetModifyACL().GetDiffACL();
        TPath dstPath     = parentPath.Child(name);

        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath, acl, acceptExisted));
        RETURN_RESULT_UNLESS(IsApplyIfChecksPassed(result, context));
        RETURN_RESULT_UNLESS(IsDescriptionValid(result,
                                                externalDataSourceDescription,
                                                context.SS->ExternalSourceFactory));

        const TExternalDataSourceInfo::TPtr externalDataSourceInfo =
            NExternalDataSource::CreateExternalDataSource(externalDataSourceDescription, 1);
        Y_ABORT_UNLESS(externalDataSourceInfo);

        AddPathInSchemeShard(result, dstPath, owner);
        const TPathElement::TPtr externalDataSource =
            CreateExternalDataSourcePathElement(dstPath);
        CreateTransaction(context, externalDataSource->PathId);

        NIceDb::TNiceDb db(context.GetDB());

        RegisterParentPathDependencies(context, parentPath);

        AdvanceTransactionStateToPropose(context, db);

        PersistExternalDataSource(context, db, externalDataSource,
                                  externalDataSourceInfo, acl);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId,
                                                          dstPath,
                                                          context.SS,
                                                          context.OnComplete);

        UpdatePathSizeCounts(parentPath, dstPath);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateExternalDataSource AbortPropose"
              << ": opId# " << OperationId);
        Y_ABORT("no AbortPropose for TCreateExternalDataSource");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateExternalDataSource AbortUnsafe"
              << ": opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

} // namespace

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateNewExternalDataSource(TOperationId id,
                                                         const TTxTransaction& tx,
                                                         TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateExternalDataSource);

    LOG_I("CreateNewExternalDataSource, opId " << id << ", feature flag EnableReplaceIfExistsForExternalEntities "
                                               << context.SS->EnableReplaceIfExistsForExternalEntities << ", tx "
                                               << tx.ShortDebugString());

    auto errorResult = [&id](NKikimrScheme::EStatus status, const TStringBuf& msg) -> TVector<ISubOperation::TPtr> {
        return {CreateReject(id, status, TStringBuilder() << "Invalid TCreateExternalDataSource request: " << msg)};
    };

    const auto &operation = tx.GetCreateExternalDataSource();
    const auto replaceIfExists = operation.GetReplaceIfExists();
    const TString &name = operation.GetName();

    if (replaceIfExists && !context.SS->EnableReplaceIfExistsForExternalEntities) {
        return errorResult(NKikimrScheme::StatusPreconditionFailed, "Unsupported: feature flag EnableReplaceIfExistsForExternalEntities is off");
    }

    const TString& parentPathStr = tx.GetWorkingDir();
    const TPath parentPath = TPath::Resolve(parentPathStr, context.SS);

    {
        const auto checks = NExternalDataSource::IsParentPathValid(parentPath, tx, /* isCreate */ true);
        if (!checks) {
            return errorResult(checks.GetStatus(), checks.GetError());
        }
    }

    if (replaceIfExists) {
        const TPath dstPath = parentPath.Child(name);
        const auto isAlreadyExists =
            dstPath.Check()
                .IsResolved()
                .NotUnderDeleting();
        if (isAlreadyExists) {
            return {CreateAlterExternalDataSource(id, tx)};
        }
    }

    return {MakeSubOperation<TCreateExternalDataSource>(id, tx)};
}
ISubOperation::TPtr CreateNewExternalDataSource(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TCreateExternalDataSource>(id, state);
}

}
