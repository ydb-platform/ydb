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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterStreamingQuery);

        const TPathId& pathId = txState->TargetPathId;
        const TPath& path = TPath::Init(pathId, context.SS);
        NIceDb::TNiceDb db(context.GetDB());

        IncParentDirAlterVersionWithRepublish(OperationId, path, context);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterStreamingQuery);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TAlterStreamingQuery TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TAlterStreamingQuery : public TSubOperation {
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
        const auto checks = parentPath.Check();
        checks.NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .NotUnderDeleting()
            .IsCommonSensePath()
            .IsLikeDirectory();

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
        }

        return static_cast<bool>(checks);
    }

    static bool IsDestinationPathValid(const THolder<TProposeResponse>& result, const TPath& dstPath) {
        const auto checks = dstPath.Check();
        checks.IsAtLocalSchemeShard()
            .IsResolved()
            .NotUnderDeleting()
            .NotUnderOperation()
            .FailOnWrongType(TPathElement::EPathType::EPathTypeStreamingQuery);

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

    TStreamingQueryInfo::TPtr GetAlteredQueryInfo(const TPath& dstPath, const TOperationContext& context) const {
        const auto& oldStreamingQueryInfo = context.SS->StreamingQueries.Value(dstPath->PathId, nullptr);
        Y_ABORT_UNLESS(oldStreamingQueryInfo);
        auto streamingQueryInfo = MakeIntrusive<TStreamingQueryInfo>(TStreamingQueryInfo{
            .AlterVersion = oldStreamingQueryInfo->AlterVersion + 1,
            .Properties = Transaction.GetCreateStreamingQuery().GetProperties(),
        });

        if (!Transaction.GetReplaceIfExists()) {
            auto& properties = *streamingQueryInfo->Properties.MutableProperties();
            for (const auto& [property, value] : oldStreamingQueryInfo->Properties.GetProperties()) {
                properties.emplace(property, value);
            }
        }

        return streamingQueryInfo;
    }

    bool IsDescriptionValid(const THolder<TProposeResponse>& result, TStreamingQueryInfo::TPtr queryInfo) const {
        if (const ui64 propertiesSize = queryInfo->Properties.ByteSizeLong(); propertiesSize > MAX_PROTOBUF_SIZE) {
            result->SetError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Maximum size of properties must be less or equal equal to " << MAX_PROTOBUF_SIZE << " but got " << propertiesSize << " after alter");
            return false;
        }

        return true;
    }

    void PersistAlterStreamingQuery(const TPath& dstPath, const TOperationContext& context) const {
        const TPathId& pathId = dstPath.Base()->PathId;

        context.MemChanges.GrabPath(context.SS, dstPath->ParentPathId);
        context.MemChanges.GrabStreamingQuery(context.SS, pathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistStreamingQuery(pathId);
        context.DbChanges.PersistTxState(OperationId);
    }

    void CreateTransaction(const TPath& dstPath, const TOperationContext& context) const {
        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterStreamingQuery, dstPath.Base()->PathId);
        txState.Shards.clear();
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);
        context.OnComplete.ActivateTx(OperationId);

        if (const auto parent = dstPath.Parent().Base(); parent->HasActiveChanges()) {
            const TTxId parentTxId = parent->PlannedToCreate() ? parent->CreateTxId : parent->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }
    }

    void AlterStreamingQueryPathElement(const TPath& dstPath, TStreamingQueryInfo::TPtr queryInfo, const TOperationContext& context) const {
        TPathElement::TPtr streamingQuery = dstPath.Base();

        streamingQuery->PathState = TPathElement::EPathState::EPathStateAlter;
        streamingQuery->LastTxId  = OperationId.GetTxId();

        if (const auto& acl = Transaction.GetModifyACL().GetDiffACL()) {
            streamingQuery->ApplyACL(acl);
        }

        context.SS->StreamingQueries[dstPath.Base()->PathId] = queryInfo;
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        Y_UNUSED(owner);

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& streamingQueryDescription = Transaction.GetCreateStreamingQuery();
        const TString& name = streamingQueryDescription.GetName();
        LOG_N("TAlterStreamingQuery Propose: opId# " << OperationId << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        const TPath& parentPath = TPath::Resolve(parentPathStr, context.SS);
        RETURN_RESULT_UNLESS(IsParentPathValid(result, parentPath));

        const TPath& dstPath = parentPath.Child(name);
        RETURN_RESULT_UNLESS(IsDestinationPathValid(result, dstPath));
        RETURN_RESULT_UNLESS(IsApplyIfChecksPassed(result, context));
        const auto queryInfo = GetAlteredQueryInfo(dstPath, context);
        RETURN_RESULT_UNLESS(IsDescriptionValid(result, queryInfo));

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        const auto guard = context.DbGuard();
        PersistAlterStreamingQuery(dstPath, context);
        CreateTransaction(dstPath, context);
        AlterStreamingQueryPathElement(dstPath, queryInfo, context);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TAlterStreamingQuery AbortPropose: opId# " << OperationId);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TAlterStreamingQuery AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}  // anonymous namespace

}  // namespace NStreamingQuery

ISubOperation::TPtr CreateAlterStreamingQuery(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<NStreamingQuery::TAlterStreamingQuery>(id, tx);
}

ISubOperation::TPtr CreateAlterStreamingQuery(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<NStreamingQuery::TAlterStreamingQuery>(id, state);
}

}  // namespace NKikimr::NSchemeShard
