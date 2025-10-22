#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define RETURN_RESULT_UNLESS(x) if (!(x)) return result;

namespace NKikimr::NSchemeShard {

namespace NStreamingQuery {

namespace {

class TPropose : public TSubOperationState {
public:
    explicit TPropose(TOperationId id)
        : OperationId(std::move(id))
    {}

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);
        LOG_I(DebugHint() << "HandleReply TEvOperationPlan: step# " << step);

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateStreamingQuery);

        context.SS->TabletCounters->Simple()[COUNTER_STREAMING_QUERY_COUNT].Add(1);

        const auto& pathId = txState->TargetPathId;
        const auto& path = TPath::Init(pathId, context.SS);
        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);
        IncParentDirAlterVersionWithRepublish(OperationId, path, context);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateStreamingQuery);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TCreateStreamingQuery TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TCreateStreamingQuery : public TSubOperation {
    static constexpr ui64 MAX_PROTOBUF_SIZE = 2_MB;

    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

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

    static bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath) {
        const auto checks = IsParentPathValid(parentPath);
        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
        }

        return static_cast<bool>(checks);
    }

    bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TPath& dstPath, const TOperationContext& context) const {
        const auto checks = dstPath.Check();
        checks.IsAtLocalSchemeShard();

        if (dstPath.IsResolved()) {
            checks.IsResolved()
                .NotUnderDeleting()
                .FailOnExist(TPathElement::EPathType::EPathTypeStreamingQuery, !Transaction.GetFailOnExist());
        } else {
            checks.NotEmpty()
                .NotResolved();
        }

        if (checks) {
            checks.IsValidLeafName(context.UserToken.Get())
                .DepthLimit()
                .PathsLimit()
                .DirChildrenLimit()
                .IsValidACL(Transaction.GetModifyACL().GetDiffACL());
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

    bool IsApplyIfChecksPassed(const THolder<TProposeResponse>& result, const TOperationContext& context) const {
        if (TString errorStr; !context.SS->CheckApplyIf(Transaction, errorStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errorStr);
            return false;
        }

        return true;
    }

    bool IsDescriptionValid(const THolder<TProposeResponse>& result) const {
        if (const ui64 propertiesSize = Transaction.GetCreateStreamingQuery().GetProperties().ByteSizeLong(); propertiesSize > MAX_PROTOBUF_SIZE) {
            result->SetError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Maximum size of properties must be less or equal equal to " << MAX_PROTOBUF_SIZE << " but got " << propertiesSize);
            return false;
        }

        return true;
    }

    void PersistCreateStreamingQuery(const TPathId& parentPathId, const TPathId& streamingQueryPathId, const TOperationContext& context) const {
        context.MemChanges.GrabNewPath(context.SS, streamingQueryPathId);
        context.MemChanges.GrabNewStreamingQuery(context.SS, streamingQueryPathId);
        context.MemChanges.GrabPath(context.SS, parentPathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(streamingQueryPathId);
        context.DbChanges.PersistPath(parentPathId);
        context.DbChanges.PersistStreamingQuery(streamingQueryPathId);
        context.DbChanges.PersistTxState(OperationId);
    }

    void AddPathIntoSchemeShard(const THolder<TProposeResponse>& result, TPath& dstPath, const TPathId& newPathId, const TString& owner, TOperationContext& context) const {
        dstPath.MaterializeLeaf(owner, newPathId);
        dstPath.DomainInfo()->IncPathsInside(context.SS);
        IncAliveChildrenSafeWithUndo(OperationId, dstPath.Parent(), context);

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
    }

    void CreateTransaction(const TPath& dstPath, const TOperationContext& context) const {
        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateStreamingQuery, dstPath.Base()->PathId);
        txState.Shards.clear();
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);
        context.OnComplete.ActivateTx(OperationId);

        if (const auto parent = dstPath.Parent().Base(); parent->HasActiveChanges()) {
            const TTxId parentTxId = parent->PlannedToCreate() ? parent->CreateTxId : parent->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }
    }

    void CreateStreamingQueryPathElement(const TPath& dstPath, const TOperationContext& context) const {
        TPathElement::TPtr streamingQuery = dstPath.Base();

        streamingQuery->CreateTxId = OperationId.GetTxId();
        streamingQuery->PathType = TPathElement::EPathType::EPathTypeStreamingQuery;
        streamingQuery->PathState = TPathElement::EPathState::EPathStateCreate;
        streamingQuery->LastTxId  = OperationId.GetTxId();

        if (const auto& acl = Transaction.GetModifyACL().GetDiffACL()) {
            streamingQuery->ApplyACL(acl);
        }

        const auto streamingQueryInfo = MakeIntrusive<TStreamingQueryInfo>(TStreamingQueryInfo{
            .AlterVersion = 1,
            .Properties = Transaction.GetCreateStreamingQuery().GetProperties(),
        });
        const auto [it, inserted] = context.SS->StreamingQueries.emplace(dstPath.Base()->PathId, streamingQueryInfo);
        if (inserted) {
            context.SS->IncrementPathDbRefCount(dstPath.Base()->PathId);
        } else {
            it->second = streamingQueryInfo;
        }
    }

public:
    using TSubOperation::TSubOperation;

    static TPath::TChecker IsParentPathValid(const TPath& parentPath) {
        auto checks = parentPath.Check();
        checks.NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .NotUnderDeleting()
            .IsCommonSensePath()
            .IsLikeDirectory()
            .FailOnRestrictedCreateInTempZone();

        return checks;
    }

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = Transaction.GetCreateStreamingQuery().GetName();
        LOG_N("TCreateStreamingQuery Propose: opId# " << OperationId << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        const TPath& parentPath = TPath::Resolve(parentPathStr, context.SS);
        RETURN_RESULT_UNLESS(IsParentPathValid(result, parentPath));

        TPath dstPath = parentPath.Child(name);
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath, context));
        RETURN_RESULT_UNLESS(IsApplyIfChecksPassed(result, context));
        RETURN_RESULT_UNLESS(IsDescriptionValid(result));

        const auto guard = context.DbGuard();
        const auto newPathId = context.SS->AllocatePathId();
        PersistCreateStreamingQuery(parentPath.Base()->PathId, newPathId, context);
        AddPathIntoSchemeShard(result, dstPath, newPathId, owner, context);
        CreateTransaction(dstPath, context);
        CreateStreamingQueryPathElement(dstPath, context);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateStreamingQuery AbortPropose: opId# " << OperationId);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateStreamingQuery AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

using TTag = TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateStreamingQuery>;

}  // anonymous namespace

}  // namespace NStreamingQuery


namespace NOperation {

template <>
std::optional<TString> GetTargetName<NStreamingQuery::TTag>(NStreamingQuery::TTag, const TTxTransaction& tx) {
    return tx.GetCreateStreamingQuery().GetName();
}

template <>
bool SetName<NStreamingQuery::TTag>(NStreamingQuery::TTag, TTxTransaction& tx, const TString& name) {
    tx.MutableCreateStreamingQuery()->SetName(name);
    return true;
}

} // namespace NOperation

ISubOperation::TPtr CreateNewStreamingQuery(TOperationId id, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateStreamingQuery);

    LOG_I("CreateNewStreamingQuery, opId# " << id  << ", tx# " << tx.ShortDebugString());

    const TPath parentPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    if (const auto checks = NStreamingQuery::TCreateStreamingQuery::IsParentPathValid(parentPath); !checks) {
        return CreateReject(id, checks.GetStatus(), TStringBuilder() << "Invalid CreateStreamingQuery request: " << checks.GetError());
    }

    if (tx.GetReplaceIfExists()) {
        const TPath dstPath = parentPath.Child(tx.GetCreateStreamingQuery().GetName());
        const auto isAlreadyExists = dstPath.Check()
            .IsResolved()
            .NotUnderDeleting();

        if (isAlreadyExists) {
            return CreateAlterStreamingQuery(id, tx);
        }
    }

    return MakeSubOperation<NStreamingQuery::TCreateStreamingQuery>(id, tx);
}

ISubOperation::TPtr CreateNewStreamingQuery(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<NStreamingQuery::TCreateStreamingQuery>(id, state);
}

}  // namespace NKikimr::NSchemeShard
