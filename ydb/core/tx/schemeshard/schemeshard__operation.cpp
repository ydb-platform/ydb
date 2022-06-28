#include "schemeshard__operation.h"

#include "schemeshard__operation_side_effects.h"
#include "schemeshard__operation_memory_changes.h"
#include "schemeshard__operation_db_changes.h"

#include "schemeshard_impl.h"

#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <util/generic/algorithm.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxOperationProposeCancelTx: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvCancelTx::TPtr Ev;

    TSideEffects OnComplete;
    TMemoryChanges MemChanges;
    TStorageChanges DbChanges;

    TTxOperationProposeCancelTx(TSchemeShard* self, TEvSchemeShard::TEvCancelTx::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_CANCEL_BACKUP_IMPL; }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Ev->Get()->Record;

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxOperationProposeCancelTx Execute"
                        << ", at schemeshard: " << Self->TabletID()
                        << ", message: " << record.ShortDebugString());

        txc.DB.NoMoreReadsForTx();

        ISubOperationBase::TPtr part = CreateTxCancelTx(Ev);
        TOperationContext context{Self, txc, ctx, OnComplete, MemChanges, DbChanges};
        auto fakeResponse = part->Propose(TString(), context);
        Y_UNUSED(fakeResponse);

        OnComplete.ApplyOnExecute(Self, txc, ctx);
        DbChanges.Apply(Self, txc, ctx);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxOperationProposeCancelTx Complete"
                        << ", at schemeshard: " << Self->TabletID());

        OnComplete.ApplyOnComplete(Self, ctx);
    }
};

NKikimrScheme::TEvModifySchemeTransaction GetRecordForPrint(const NKikimrScheme::TEvModifySchemeTransaction& record) {
    auto recordForPrint = record;
    if (record.HasUserToken()) {
        recordForPrint.SetUserToken("***hide token***");
    }
    return recordForPrint;
}

THolder<TProposeResponse> TSchemeShard::IgniteOperation(TProposeRequest& request, TOperationContext& context) {
    THolder<TProposeResponse> response = nullptr;

    auto selfId = SelfTabletId();
    auto& record = request.Record;
    auto txId = TTxId(record.GetTxId());

    if (Operations.contains(txId)) {
        response.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
            NKikimrScheme::StatusAccepted, ui64(txId), ui64(selfId)));

        response->SetError(NKikimrScheme::StatusAccepted, "There is operation with the same txId has been found in flight. Actually that shouldn't have happened."
                                                                " Note that tx body equality isn't granted. StatusAccepted is just returned on retries.");
        return std::move(response);
    }

    TOperation::TPtr operation = new TOperation(txId);
    Operations[operation->GetTxId()] = operation; //record is erased at ApplyOnExecute if all parts are done at propose

    if (record.GetUserToken()) {
         NACLibProto::TUserToken tokenPb;
         bool parseOk = tokenPb.ParseFromString(record.GetUserToken());
         if (!parseOk) {
             response.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
                 NKikimrScheme::StatusInvalidParameter, ui64(txId), ui64(selfId)));
             response->SetError(NKikimrScheme::StatusInvalidParameter, "Failed to parse user token");
             return std::move(response);
         }
         context.UserToken.Reset(new NACLib::TUserToken(tokenPb));
    }

    for (const auto& transaction: record.GetTransaction()) {
        auto quotaResult = operation->ConsumeQuota(transaction, context);
        if (quotaResult.Status != NKikimrScheme::StatusSuccess) {
            response.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
                quotaResult.Status, ui64(txId), ui64(selfId)));
            response->SetError(quotaResult.Status, quotaResult.Reason);
            Operations.erase(operation->GetTxId());
            return std::move(response);
        }
    }

    if (record.HasFailOnExist()) {
        // inherit FailOnExist from TEvModifySchemeTransaction into TModifyScheme
        for (auto transaction: *record.MutableTransaction()) {
            if (!transaction.HasFailOnExist()) {
                transaction.SetFailOnExist(record.GetFailOnExist());
            }
        }
    }

    TVector<TTxTransaction> transactions;
    for (const auto& transaction: record.GetTransaction()) {
        auto splitResult = operation->SplitIntoTransactions(transaction, context);
        if (splitResult.Status != NKikimrScheme::StatusSuccess) {
            response.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
                splitResult.Status, ui64(txId), ui64(selfId)));
            response->SetError(splitResult.Status, splitResult.Reason);
            Operations.erase(operation->GetTxId());
            return std::move(response);
        }

        transactions.insert(transactions.end(), splitResult.Transactions.begin(), splitResult.Transactions.end());
    }

    //for all tx in transactions
    for (const auto& transaction: transactions) {
        const TOperationId pathOpId = TOperationId(txId, operation->Parts.size());
        TVector<ISubOperationBase::TPtr> parts = operation->ConstructParts(transaction, context);

        if (parts.size() > 1) {
            // les't allow altering impl index tables as part of consistent operation
            context.IsAllowedPrivateTables = true;
        }

        const TString owner = record.HasOwner() ? record.GetOwner() : BUILTIN_ACL_ROOT;

        for (auto& part: parts) {
            response = part->Propose(owner, context);
            Y_VERIFY(response);

            LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "IgniteOperation"
                             << ", opId: " << pathOpId
                             << ", propose status:" << NKikimrScheme::EStatus_Name(response->Record.GetStatus())
                             << ", reason: " << response->Record.GetReason()
                             << ", at schemeshard: " << selfId);

            if (response->IsDone()) {
                operation->AddPart(part); //at ApplyOnExecute parts is erased
                context.OnComplete.DoneOperation(pathOpId); //mark it here by self for sure
            } else if (response->IsConditionalAccepted()) {
                //happens on retries, we answer like AlreadyExist or StatusSuccess with error message and do nothing in operation
                operation->AddPart(part); //at ApplyOnExecute parts is erased
                context.OnComplete.DoneOperation(pathOpId); //mark it here by self for sure
            } else if (response->IsAccepted()) {
                operation->AddPart(part);
                //context.OnComplete.ActivateTx(pathOpId) ///TODO maybe it is good idea
            } else {

                if (!operation->Parts.empty()) {
                    LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                                "Abort operation: IgniteOperation fail to propose a part"
                                    << ", opId: " << pathOpId
                                    << ", at schemeshard:  " << selfId
                                    << ", already accepted parts: " << operation->Parts.size()
                                    << ", propose result status: " << NKikimrScheme::EStatus_Name(response->Record.GetStatus())
                                    << ", with reason: " << response->Record.GetReason()
                                    << ", tx message: " << GetRecordForPrint(record).ShortDebugString());
                }

                Y_VERIFY_S(context.IsUndoChangesSafe(),
                           "Operation is aborted and all changes should be reverted"
                               << ", but context.IsUndoChangesSafe is false, which means some direct writes have been done"
                               << ", opId: " << pathOpId
                               << ", at schemeshard:  " << selfId
                               << ", already accepted parts: " << operation->Parts.size()
                               << ", propose result status: " << NKikimrScheme::EStatus_Name(response->Record.GetStatus())
                               << ", with reason: " << response->Record.GetReason()
                               << ", tx message: " << GetRecordForPrint(record).ShortDebugString());


                context.OnComplete = {}; // recreate
                context.DbChanges = {};

                for (auto& toAbort: operation->Parts) {
                    toAbort->AbortPropose(context);
                }

                context.MemChanges.UnDo(context.SS);
                context.OnComplete.ApplyOnExecute(context.SS, context.GetTxc(), context.Ctx);
                Operations.erase(operation->GetTxId());
                return std::move(response);
            }
        }
    }

    return std::move(response);
}

struct TSchemeShard::TTxOperationPropose: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    using TBase = NTabletFlatExecutor::TTransactionBase<TSchemeShard>;

    TProposeRequest::TPtr Request;
    THolder<TProposeResponse> Response = nullptr;

    TSideEffects OnComplete;

    TTxOperationPropose(TSchemeShard* self, TProposeRequest::TPtr request)
        : TBase(self)
        , Request(request)
    {}

    TTxType GetTxType() const override { return TXTYPE_PROPOSE; }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
        TTabletId selfId = Self->SelfTabletId();

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxOperationPropose Execute"
                        << ", message: " << GetRecordForPrint(Request->Get()->Record).ShortDebugString()
                        << ", at schemeshard: " << selfId);

        txc.DB.NoMoreReadsForTx();

        TMemoryChanges memChanges;
        TStorageChanges dbChanges;
        auto context = TOperationContext{Self, txc, ctx, OnComplete, memChanges, dbChanges};

        Response = Self->IgniteOperation(*Request->Get(), context);

        OnComplete.ApplyOnExecute(Self, txc, ctx);
        dbChanges.Apply(Self, txc, ctx);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_VERIFY(Response);

        const auto& record = Request->Get()->Record;
        const auto txId = TTxId(record.GetTxId());

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxOperationPropose Complete"
                        << ", txId: " << txId
                        << ", response: " << Response->Record.ShortDebugString()
                        << ", at schemeshard: " << Self->TabletID());

        const TActorId sender = Request->Sender;
        const ui64 cookie = Request->Cookie;
        ctx.Send(sender, Response.Release(), 0, cookie);

        OnComplete.ApplyOnComplete(Self, ctx);
    }
};

struct TSchemeShard::TTxOperationProgress: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TOperationId OpId;
    TSideEffects OnComplete;
    TMemoryChanges MemChanges;
    TStorageChanges DbChanges;

    TTxOperationProgress(TSchemeShard* self, TOperationId id)
        : TBase(self)
        , OpId(id)
    {}

    TTxType GetTxType() const override { return TXTYPE_PROGRESS_OP; }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxOperationProgress Execute"
                        << ", operationId: " << OpId
                        << ", at schemeshard: " << Self->TabletID());

        if (!Self->Operations.contains(OpId.GetTxId())) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "TTxOperationProgress Execute"
                           << " for unknown txId " << OpId.GetTxId());
            return true;
        }

        TOperation::TPtr operation = Self->Operations.at(OpId.GetTxId());
        if (operation->DoneParts.contains(OpId.GetSubTxId())) {
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "TTxOperationProgress Execute"
                           << " operation already done"
                           << ", operationId: " << OpId
                           << ", at schemeshard: " << Self->TabletID());
            return true;
        }

        ISubOperationBase::TPtr part = operation->Parts.at(ui64(OpId.GetSubTxId()));

        TOperationContext context{Self, txc, ctx, OnComplete, MemChanges, DbChanges};

        part->ProgressState(context);

        OnComplete.ApplyOnExecute(Self, txc, ctx);
        DbChanges.Apply(Self, txc, ctx);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        OnComplete.ApplyOnComplete(Self, ctx);
    }
};


template <class TEvType>
struct TSchemeShard::TTxOperationReply {};

#define DefineTTxOperationReply(TEvType, TxType) \
    template<> \
    struct TSchemeShard::TTxOperationReply<TEvType>: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> { \
        TOperationId OperationId; \
        TEvType::TPtr EvReply; \
        TSideEffects OnComplete; \
        TMemoryChanges MemChanges; \
        TStorageChanges DbChanges; \
\
        TTxType GetTxType() const override { return TxType; } \
\
        TTxOperationReply(TSchemeShard* self, TOperationId id, TEvType::TPtr& ev) \
            : TBase(self) \
            , OperationId(id) \
            , EvReply(ev) \
        { \
            Y_VERIFY(TEvType::EventType != TEvPrivate::TEvOperationPlan::EventType); \
            Y_VERIFY(TEvType::EventType != TEvTxProcessing::TEvPlanStep::EventType); \
        } \
\
        bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override { \
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, \
                        "TTxOperationReply<" #TEvType "> execute " \
                            << ", operationId: " << OperationId \
                            << ", at schemeshard: " << Self->TabletID() \
                            << ", message: " << IOperationBase::DebugReply(EvReply)); \
            if (!Self->Operations.contains(OperationId.GetTxId())) { \
                return true; \
            } \
            TOperation::TPtr operation = Self->Operations.at(OperationId.GetTxId()); \
            if (operation->DoneParts.contains(OperationId.GetSubTxId())) { \
                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, \
                            "TTxOperationReply<" #TEvType "> execute " \
                               << ", operation already done" \
                               << ", operationId: " << OperationId \
                               << ", at schemeshard: " << Self->TabletID()); \
                return true; \
            } \
            ISubOperationBase::TPtr part = operation->Parts.at(ui64(OperationId.GetSubTxId())); \
            TOperationContext context{Self, txc, ctx, OnComplete, MemChanges, DbChanges}; \
            Y_VERIFY(EvReply); \
            part->HandleReply(EvReply, context); \
            OnComplete.ApplyOnExecute(Self, txc, ctx); \
            DbChanges.Apply(Self, txc, ctx); \
            return true; \
        } \
        void Complete(const TActorContext& ctx) override { \
            OnComplete.ApplyOnComplete(Self, ctx); \
        } \
    };

    SCHEMESHARD_INCOMING_EVENTS(DefineTTxOperationReply)
#undef DefineTxOperationReply

struct TSchemeShard::TTxOperationPlanStep: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvTxProcessing::TEvPlanStep::TPtr Ev;
    TSideEffects OnComplete;
    TMemoryChanges MemChanges;
    TStorageChanges DbChanges;

    TTxOperationPlanStep(TSchemeShard* self, TEvTxProcessing::TEvPlanStep::TPtr ev)
        : TBase(self)
        , Ev(ev)

    {}

    TTxType GetTxType() const override { return TXTYPE_PLAN_STEP; }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
        const NKikimrTx::TEvMediatorPlanStep& record = Ev->Get()->Record;
        const auto step = TStepId(record.GetStep());
        const size_t txCount = record.TransactionsSize();

        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxOperationPlanStep Execute"
                         << ", stepId: " << step
                         << ", transactions count in step: " << txCount
                         << ", at schemeshard: " << Self->TabletID());
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TTxOperationPlanStep Execute"
                        << ", message: " << record.ShortDebugString()
                        << ", at schemeshard: " << Self->TabletID());

        for (size_t i = 0; i < txCount; ++i) {
            const auto txId = TTxId(record.GetTransactions(i).GetTxId());
            const auto coordinator = ActorIdFromProto(record.GetTransactions(i).GetAckTo());
            const auto coordinatorId = TTabletId(record.GetTransactions(i).GetCoordinator());

            if (!Self->Operations.contains(txId)) {
                    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                               "TTxOperationPlanStep Execute"
                                   << " unknown operation, assumed as already done"
                                   << ", transaction Id: " << txId);

                OnComplete.CoordinatorAck(coordinator, step, txId);
                continue;
            }

            TOperation::TPtr operation = Self->Operations.at(txId);

            for (ui32 partIdx = 0; partIdx < operation->Parts.size(); ++partIdx) {
                auto opId = TOperationId(txId, partIdx);

                if (operation->DoneParts.contains(TSubTxId(partIdx))) {
                    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                                "TTxOperationPlanStep Execute"
                                    << " operation part is already done"
                                    << ", operationId: " << opId);
                    continue;
                }

                TOperationContext context{Self, txc, ctx, OnComplete, MemChanges, DbChanges};
                THolder<TEvPrivate::TEvOperationPlan> msg = MakeHolder<TEvPrivate::TEvOperationPlan>(ui64(step), ui64(txId));
                TEvPrivate::TEvOperationPlan::TPtr personalEv = (TEventHandle<TEvPrivate::TEvOperationPlan>*) new IEventHandle(
                            context.SS->SelfId(), context.SS->SelfId(), msg.Release());

                operation->Parts.at(partIdx)->HandleReply(personalEv, context);
            }

            OnComplete.CoordinatorAck(coordinator, step, txId);
            OnComplete.UnbindMsgFromPipe(TOperationId(txId, InvalidSubTxId), coordinatorId, TPipeMessageId(0, txId));
        }

        const TActorId mediator = Ev->Sender;
        OnComplete.MediatorAck(mediator, step);

        OnComplete.ApplyOnExecute(Self, txc, ctx);
        DbChanges.Apply(Self, txc, ctx);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        OnComplete.ApplyOnComplete(Self, ctx);
    }
};


NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxOperationPropose(TEvSchemeShard::TEvCancelTx::TPtr& ev) {
    return new TTxOperationProposeCancelTx(this, ev);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxOperationPropose(TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
    return new TTxOperationPropose(this, ev);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxOperationPlanStep(TEvTxProcessing::TEvPlanStep::TPtr& ev) {
    return new TTxOperationPlanStep(this, ev);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxOperationProgress(TOperationId opId) {
    return new TTxOperationProgress(this, opId);
}

#define DefineCreateTxOperationReply(TEvType, TxType)          \
    NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxOperationReply(TOperationId id, TEvType::TPtr& ev) { \
        return new TTxOperationReply<TEvType>(this, id, ev);    \
    }

    SCHEMESHARD_INCOMING_EVENTS(DefineCreateTxOperationReply)
#undef DefineTxOperationReply

TString JoinPath(const TString& workingDir, const TString& name) {
    Y_VERIFY(!name.StartsWith('/') && !name.EndsWith('/'));
    return TStringBuilder()
               << workingDir
               << (workingDir.EndsWith('/') ? "" : "/")
               << name;
}

TOperation::TConsumeQuotaResult TOperation::ConsumeQuota(const TTxTransaction& tx, TOperationContext& context) {
    TConsumeQuotaResult result;

    // Internal operations never consume quota
    if (tx.GetInternal()) {
        return result;
    }

    // These operations never consume quota
    switch (tx.GetOperationType()) {
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropUnsafe:
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExtSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExtSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropExtSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomainDecision:
        return result;
    default:
        break;
    }

    const TString workingDir = tx.GetWorkingDir();
    TPath path = TPath::Resolve(workingDir, context.SS);

    // Find the first directory that actually exists
    path.RiseUntilExisted();

    // Don't fail on some completely invalid path
    if (!path.IsResolved()) {
        return result;
    }

    auto domainId = path.DomainId();
    auto domainInfo = path.DomainInfo();
    if (!domainInfo->TryConsumeSchemeQuota(context.Ctx.Now())) {
        result.Status = NKikimrScheme::StatusQuotaExceeded;
        result.Reason = "Request exceeded a limit on the number of schema operations, try again later.";
    }

    // Even if operation fails later we want to persist updated/consumed quotas
    NIceDb::TNiceDb db(context.GetTxc().DB); // write quotas directly in db even if operation fails
    context.SS->PersistSubDomainSchemeQuotas(db, domainId, *domainInfo);
    return result;
}

TOperation::TSplitTransactionsResult TOperation::SplitIntoTransactions(const TTxTransaction& tx, const TOperationContext& context) {
    TSplitTransactionsResult result;

    const TPath parentPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    {
        TPath::TChecker checks = parentPath.Check();
        checks
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .NotUnderDeleting()
            .IsCommonSensePath()
            .IsLikeDirectory();

        if (!checks) {
            result.Transactions.push_back(tx);
            return result;
        }
    }

    TString targetName;

    switch (tx.GetOperationType()) {
    case NKikimrSchemeOp::EOperationType::ESchemeOpMkDir:
        targetName = tx.GetMkDir().GetName();
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable:
        if (tx.GetCreateTable().HasCopyFromTable()) {
            result.Transactions.push_back(tx);
            return result;
        }
        targetName = tx.GetCreateTable().GetName();
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup:
        targetName = tx.GetCreatePersQueueGroup().GetName();
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExtSubDomain:
        targetName = tx.GetSubDomain().GetName();
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateRtmrVolume:
        targetName = tx.GetCreateRtmrVolume().GetName();
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlockStoreVolume:
        targetName = tx.GetCreateBlockStoreVolume().GetName();
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateFileStore:
        targetName = tx.GetCreateFileStore().GetName();
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateKesus:
        targetName = tx.GetKesus().GetName();
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSolomonVolume:
        targetName = tx.GetCreateSolomonVolume().GetName();
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable:
        targetName = tx.GetCreateIndexedTable().GetTableDescription().GetName();
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore:
        targetName = tx.GetCreateColumnStore().GetName();
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable:
        targetName = tx.GetCreateColumnTable().GetName();
        break;
    default:
        result.Transactions.push_back(tx);
        return result;
    }

    if (!targetName || targetName.StartsWith('/') || targetName.EndsWith('/')) {
        result.Transactions.push_back(tx);
        return result;
    }

    TPath path = TPath::Resolve(JoinPath(tx.GetWorkingDir(), targetName), context.SS);
    {
        TPath::TChecker checks = path.Check();
        checks.IsAtLocalSchemeShard();

        bool exists = false;
        if (path.IsResolved()) {
            checks.IsResolved();
            exists = !path.IsDeleted();
        } else {
            checks
                .NotEmpty()
                .NotResolved();
        }

        if (checks && !exists) {
            checks
                .IsValidLeafName()
                .DepthLimit()
                .PathsLimit();
        }

        if (checks && !exists && path.Parent().IsResolved()) {
            checks.DirChildrenLimit();
        }

        if (!checks) {
            result.Status = checks.GetStatus(&result.Reason);
            result.Transactions.push_back(tx);
            return result;
        }

        const TString name = path.LeafName();
        path.Rise();

        TTxTransaction create(tx);
        create.SetWorkingDir(path.PathString());
        create.SetFailOnExist(tx.GetFailOnExist());

        switch (tx.GetOperationType()) {
        case NKikimrSchemeOp::EOperationType::ESchemeOpMkDir:
            create.MutableMkDir()->SetName(name);
            break;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable:
            create.MutableCreateTable()->SetName(name);
            break;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup:
            create.MutableCreatePersQueueGroup()->SetName(name);
            break;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExtSubDomain:
            create.MutableSubDomain()->SetName(name);
            break;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateRtmrVolume:
            create.MutableCreateRtmrVolume()->SetName(name);
            break;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlockStoreVolume:
            create.MutableCreateBlockStoreVolume()->SetName(name);
            break;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateFileStore:
            create.MutableCreateFileStore()->SetName(name);
            break;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateKesus:
            create.MutableKesus()->SetName(name);
            break;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSolomonVolume:
            create.MutableCreateSolomonVolume()->SetName(name);
            break;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable:
            create.MutableCreateIndexedTable()->MutableTableDescription()->SetName(name);
            break;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore:
            create.MutableCreateColumnStore()->SetName(name);
            break;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable:
            create.MutableCreateColumnTable()->SetName(name);
            break;
        default:
            Y_UNREACHABLE();
        }

        result.Transactions.push_back(create);

        if (exists) {
            return result;
        }
    }

    while (path != parentPath) {
        TPath::TChecker checks = path.Check();
        checks
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard();

        if (path.IsResolved()) {
            checks.IsResolved();

            if (path.IsDeleted()) {
                checks.IsDeleted();
            } else {
                checks
                    .NotDeleted()
                    .NotUnderDeleting()
                    .IsCommonSensePath()
                    .IsLikeDirectory();

                if (checks) {
                    break;
                }
            }
        } else {
            checks
                .NotEmpty()
                .NotResolved();
        }

        if (checks) {
            checks
                .IsValidLeafName()
                .DepthLimit()
                .PathsLimit(result.Transactions.size() + 1);
        }

        if (checks && path.Parent().IsResolved()) {
            checks.DirChildrenLimit();
        }

        if (!checks) {
            result.Status = checks.GetStatus(&result.Reason);
            result.Transactions.clear();
            result.Transactions.push_back(tx);
            return result;
        }

        const TString name = path.LeafName();
        path.Rise();

        TTxTransaction mkdir;
        mkdir.SetFailOnExist(true);
        mkdir.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
        mkdir.SetWorkingDir(path.PathString());
        mkdir.MutableMkDir()->SetName(name);
        result.Transactions.push_back(mkdir);
    }

    Reverse(result.Transactions.begin(), result.Transactions.end());
    return result;
}

ISubOperationBase::TPtr TOperation::RestorePart(TTxState::ETxType txType, TTxState::ETxState txState) {
    switch (txType) {
        case TTxState::ETxType::TxMkDir:
            return CreateMkDir(NextPartId(), txState);
        case TTxState::ETxType::TxRmDir:
            return CreateRmDir(NextPartId(), txState);
        case TTxState::ETxType::TxModifyACL:
            return CreateModifyACL(NextPartId(), txState);
        case TTxState::ETxType::TxAlterUserAttributes:
            return CreateAlterUserAttrs(NextPartId(), txState);
        case TTxState::ETxType::TxCreateTable:
            return CreateNewTable(NextPartId(), txState);
        case TTxState::ETxType::TxCopyTable:
            return CreateCopyTable(NextPartId(), txState);
        case TTxState::ETxType::TxAlterTable:
            return CreateAlterTable(NextPartId(), txState);
        case TTxState::ETxType::TxSplitTablePartition:
        case TTxState::ETxType::TxMergeTablePartition:
            return CreateSplitMerge(NextPartId(), txState);
        case TTxState::ETxType::TxBackup:
            return CreateBackup(NextPartId(), txState);
        case TTxState::ETxType::TxRestore:
            return CreateRestore(NextPartId(), txState);
        case TTxState::ETxType::TxDropTable:
            return CreateDropTable(NextPartId(), txState);
        case TTxState::ETxType::TxCreateTableIndex:
            return CreateNewTableIndex(NextPartId(), txState);
        case TTxState::ETxType::TxDropTableIndex:
            return CreateDropTableIndex(NextPartId(), txState);
        case TTxState::ETxType::TxCreateRtmrVolume:
            return CreateNewRTMR(NextPartId(), txState);
        case TTxState::ETxType::TxCreateOlapStore:
            return CreateNewOlapStore(NextPartId(), txState);
        case TTxState::ETxType::TxAlterOlapStore:
            return CreateAlterOlapStore(NextPartId(), txState);
        case TTxState::ETxType::TxDropOlapStore:
            return CreateDropOlapStore(NextPartId(), txState);
        case TTxState::ETxType::TxCreateColumnTable:
            return CreateNewColumnTable(NextPartId(), txState);
        case TTxState::ETxType::TxAlterColumnTable:
            return CreateAlterColumnTable(NextPartId(), txState);
        case TTxState::ETxType::TxDropColumnTable:
            return CreateDropColumnTable(NextPartId(), txState);
        case TTxState::ETxType::TxCreatePQGroup:
            return CreateNewPQ(NextPartId(), txState);
        case TTxState::ETxType::TxAlterPQGroup:
            return CreateAlterPQ(NextPartId(), txState);
        case TTxState::ETxType::TxDropPQGroup:
            return CreateDropPQ(NextPartId(), txState);
        case TTxState::ETxType::TxCreateSolomonVolume:
            return CreateNewSolomon(NextPartId(), txState);
        case TTxState::ETxType::TxDropSolomonVolume:
            return CreateDropSolomon(NextPartId(), txState);
        case TTxState::ETxType::TxCreateSubDomain:
            return CreateSubDomain(NextPartId(), txState);
        case TTxState::ETxType::TxAlterSubDomain:
            return CreateAlterSubDomain(NextPartId(), txState);
        case TTxState::ETxType::TxUpgradeSubDomain:
            return CreateUpgradeSubDomain(NextPartId(), txState);
        case TTxState::ETxType::TxUpgradeSubDomainDecision:
            return CreateUpgradeSubDomainDecision(NextPartId(), txState);
        case TTxState::ETxType::TxDropSubDomain:
            return CreateDropSubdomain(NextPartId(), txState);
        case TTxState::ETxType::TxForceDropSubDomain:
            return CreateFroceDropSubDomain(NextPartId(), txState);
        case TTxState::ETxType::TxCreateExtSubDomain:
            return CreateExtSubDomain(NextPartId(), txState);
        case TTxState::ETxType::TxCreateKesus:
            return CreateNewKesus(NextPartId(), txState);
        case TTxState::ETxType::TxAlterKesus:
            return CreateAlterKesus(NextPartId(), txState);
        case TTxState::ETxType::TxDropKesus:
            return CreateDropKesus(NextPartId(), txState);
        case TTxState::ETxType::TxAlterExtSubDomain:
            return CreateAlterExtSubDomain(NextPartId(), txState);
        case TTxState::ETxType::TxForceDropExtSubDomain:
            return CreateFroceDropExtSubDomain(NextPartId(), txState);
        case TTxState::ETxType::TxInitializeBuildIndex:
            return CreateInitializeBuildIndexMainTable(NextPartId(), txState);
        case TTxState::ETxType::TxFinalizeBuildIndex:
            return CreateFinalizeBuildIndexMainTable(NextPartId(), txState);
        case TTxState::ETxType::TxDropTableIndexAtMainTable:
            return CreateDropTableIndexAtMainTable(NextPartId(), txState);
        case TTxState::ETxType::TxUpdateMainTableOnIndexMove:
            return CreateUpdateMainTableOnIndexMove(NextPartId(), txState);
        case TTxState::ETxType::TxCreateLockForIndexBuild:
            return CreateLockForIndexBuild(NextPartId(), txState);
        case TTxState::ETxType::TxDropLock:
            return DropLock(NextPartId(), txState);
        case TTxState::ETxType::TxAlterTableIndex:
            return CreateAlterTableIndex(NextPartId(), txState);
        case TTxState::ETxType::TxAlterSolomonVolume:
            return CreateAlterSolomon(NextPartId(), txState);

        // BlockStore
        case TTxState::ETxType::TxCreateBlockStoreVolume:
            return CreateNewBSV(NextPartId(), txState);
        case TTxState::ETxType::TxAssignBlockStoreVolume:
            return CreateAssignBSV(NextPartId(), txState);
        case TTxState::ETxType::TxAlterBlockStoreVolume:
            return CreateAlterBSV(NextPartId(), txState);
        case TTxState::ETxType::TxDropBlockStoreVolume:
            return CreateDropBSV(NextPartId(), txState);

        // FileStore
        case TTxState::ETxType::TxCreateFileStore:
            return CreateNewFileStore(NextPartId(), txState);
        case TTxState::ETxType::TxAlterFileStore:
            return CreateAlterFileStore(NextPartId(), txState);
        case TTxState::ETxType::TxDropFileStore:
            return CreateDropFileStore(NextPartId(), txState);

        // CDC
        case TTxState::ETxType::TxCreateCdcStream:
            return CreateNewCdcStreamImpl(NextPartId(), txState);
        case TTxState::ETxType::TxCreateCdcStreamAtTable:
            return CreateNewCdcStreamAtTable(NextPartId(), txState);
        case TTxState::ETxType::TxAlterCdcStream:
            return CreateAlterCdcStreamImpl(NextPartId(), txState);
        case TTxState::ETxType::TxAlterCdcStreamAtTable:
            return CreateAlterCdcStreamAtTable(NextPartId(), txState);
        case TTxState::ETxType::TxDropCdcStream:
            return CreateDropCdcStreamImpl(NextPartId(), txState);
        case TTxState::ETxType::TxDropCdcStreamAtTable:
            return CreateDropCdcStreamAtTable(NextPartId(), txState);

        // Sequences
        case TTxState::ETxType::TxCreateSequence:
            return CreateNewSequence(NextPartId(), txState);
        case TTxState::ETxType::TxAlterSequence:
            Y_FAIL("TODO: implement");
        case TTxState::ETxType::TxDropSequence:
            return CreateDropSequence(NextPartId(), txState);

        case TTxState::ETxType::TxFillIndex:
            Y_FAIL("deprecated");

        case TTxState::ETxType::TxMoveTable:
            return CreateMoveTable(NextPartId(), txState);
        case TTxState::ETxType::TxMoveTableIndex:
            return CreateMoveTableIndex(NextPartId(), txState);

        // Replication
        case TTxState::ETxType::TxCreateReplication:
            return CreateNewReplication(NextPartId(), txState);
        case TTxState::ETxType::TxAlterReplication:
            Y_FAIL("TODO: implement");
        case TTxState::ETxType::TxDropReplication:
            return CreateDropReplication(NextPartId(), txState);

        // BlobDepot
        case TTxState::ETxType::TxCreateBlobDepot:
            return CreateNewBlobDepot(NextPartId(), txState);
        case TTxState::ETxType::TxAlterBlobDepot:
            return CreateAlterBlobDepot(NextPartId(), txState);
        case TTxState::ETxType::TxDropBlobDepot:
            return CreateDropBlobDepot(NextPartId(), txState);

        case TTxState::ETxType::TxInvalid:
            Y_UNREACHABLE();
    }

    Y_UNREACHABLE();
}

ISubOperationBase::TPtr TOperation::ConstructPart(NKikimrSchemeOp::EOperationType opType, const TTxTransaction& tx) {
    switch (opType) {
    case NKikimrSchemeOp::EOperationType::ESchemeOpMkDir:
        return CreateMkDir(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpRmDir:
        return CreateRmDir(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpModifyACL:
        return CreateModifyACL(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterUserAttributes:
        return CreateAlterUserAttrs(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropUnsafe:
        return CreateFroceDropUnsafe(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable:
        return CreateNewTable(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable:
        Y_FAIL("in general, alter table is multipart operation now due table indexes");
    case NKikimrSchemeOp::EOperationType::ESchemeOpSplitMergeTablePartitions:
        return CreateSplitMerge(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpBackup:
        return CreateBackup(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpRestore:
        return CreateRestore(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTable:
        Y_FAIL("in general, drop table is multipart operation now due table indexes");
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable:
        Y_FAIL("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex:
        Y_FAIL("is handled as part of ESchemeOpCreateIndexedTable");
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex:
        Y_FAIL("is handled as part of ESchemeOpDropTable");
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables:
        Y_FAIL("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateRtmrVolume:
        return CreateNewRTMR(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore:
        return CreateNewOlapStore(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnStore:
        return CreateAlterOlapStore(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnStore:
        return CreateDropOlapStore(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable:
        return CreateNewColumnTable(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnTable:
        return CreateAlterColumnTable(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnTable:
        return CreateDropColumnTable(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup:
        return CreateNewPQ(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup:
        return CreateAlterPQ(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup:
        return CreateDropPQ(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSolomonVolume:
        return CreateNewSolomon(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSolomonVolume:
        return CreateAlterSolomon(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSolomonVolume:
        return CreateDropSolomon(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain:
        return CreateSubDomain(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain:
        Y_FAIL("run in compatible");
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSubDomain:
        return CreateDropSubdomain(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropSubDomain:
        Y_FAIL("run in compatible");
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExtSubDomain:
        return CreateExtSubDomain(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExtSubDomain:
        return CreateAlterExtSubDomain(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropExtSubDomain:
        return CreateFroceDropExtSubDomain(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateKesus:
        return CreateNewKesus(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterKesus:
        return CreateAlterKesus(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropKesus:
        return CreateDropKesus(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomain:
        return CreateUpgradeSubDomain(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomainDecision:
        return CreateUpgradeSubDomainDecision(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexBuild:
        Y_FAIL("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateLockForIndexBuild:
        return CreateLockForIndexBuild(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropLock:
        return DropLock(NextPartId(), tx);

    // BlockStore
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlockStoreVolume:
        return CreateNewBSV(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAssignBlockStoreVolume:
        return CreateAssignBSV(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBlockStoreVolume:
        return CreateAlterBSV(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropBlockStoreVolume:
        return CreateDropBSV(NextPartId(), tx);

    // FileStore
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateFileStore:
        return CreateNewFileStore(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterFileStore:
        return CreateAlterFileStore(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropFileStore:
        return CreateDropFileStore(NextPartId(), tx);

    // Login
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin:
        return CreateAlterLogin(NextPartId(), tx);

    // Sequence
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence:
        return CreateNewSequence(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSequence:
        Y_FAIL("TODO: implement");
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence:
        return CreateDropSequence(NextPartId(), tx);

    // Index
    case NKikimrSchemeOp::EOperationType::ESchemeOpApplyIndexBuild:
        Y_FAIL("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTableIndex:
        Y_FAIL("multipart operations are handled before, also they require transaction details");

    case NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexImplTable:
        Y_FAIL("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexImplTable:
        Y_FAIL("multipart operations are handled before, also they require transaction details");

    case NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexMainTable:
        Y_FAIL("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexMainTable:
        Y_FAIL("multipart operations are handled before, also they require transaction details");

    case NKikimrSchemeOp::EOperationType::ESchemeOpCancelIndexBuild:
        Y_FAIL("multipart operations are handled before, also they require transaction details");

    case NKikimrSchemeOp::EOperationType::ESchemeOpDropIndex:
        Y_FAIL("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndexAtMainTable:
        Y_FAIL("multipart operations are handled before, also they require transaction details");

    // CDC
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream:
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamImpl:
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamAtTable:
        Y_FAIL("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStream:
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamImpl:
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamAtTable:
        Y_FAIL("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStream:
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamImpl:
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamAtTable:
        Y_FAIL("multipart operations are handled before, also they require transaction details");

    case NKikimrSchemeOp::EOperationType::ESchemeOp_DEPRECATED_35:
        Y_FAIL("imposible");

    // Move
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveTable:
        return CreateMoveTable(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveTableIndex:
        return CreateMoveTableIndex(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveIndex:
        Y_FAIL("imposible");

    // Replication
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateReplication:
        return CreateNewReplication(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterReplication:
        Y_FAIL("TODO: implement");
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropReplication:
        return CreateDropReplication(NextPartId(), tx);

    // BlobDepot
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlobDepot:
        return CreateNewBlobDepot(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBlobDepot:
        return CreateAlterBlobDepot(NextPartId(), tx);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropBlobDepot:
        return CreateDropBlobDepot(NextPartId(), tx);
    }

    Y_UNREACHABLE();
}

TVector<ISubOperationBase::TPtr> TOperation::ConstructParts(const TTxTransaction& tx, TOperationContext& context) {
    const auto& opType = tx.GetOperationType();

    switch (opType) {
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable:
        if (tx.GetCreateTable().HasCopyFromTable()) {
            return {CreateCopyTable(NextPartId(), tx, context)}; // Copy indexes table as well as common table
        }
        return {ConstructPart(opType, tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable:
        return CreateIndexedTable(NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables:
        return CreateConsistentCopyTables(NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTable:
        return CreateDropIndexedTable(NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropSubDomain:
        return {CreateCompatibleSubdomainDrop(context.SS, NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexBuild:
        return CreateBuildIndex(NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpApplyIndexBuild:
        return ApplyBuildIndex(NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropIndex:
        return CreateDropIndex(NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCancelIndexBuild:
        return CancelBuildIndex(NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain:
        return {CreateCompatibleSubdomainAlter(context.SS, NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream:
        return CreateNewCdcStream(NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStream:
        return CreateAlterCdcStream(NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStream:
        return CreateDropCdcStream(NextPartId(), tx, context);
    case  NKikimrSchemeOp::EOperationType::ESchemeOpMoveTable:
        return CreateConsistentMoveTable(NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable:
        return CreateConsistentAlterTable(NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveIndex:
        return CreateConsistentMoveIndex(NextPartId(), tx, context);

    default:
        return {ConstructPart(opType, tx)};
    }
}

bool TOperation::AddPublishingPath(TPathId pathId, ui64 version) {
    Y_VERIFY(!IsReadyToNotify());
    return Publications.emplace(pathId, version).second;
}

bool TOperation::IsPublished() const {
    return Publications.empty();
}

void TOperation::ReadyToNotifyPart(TSubTxId partId) {
    ReadyToNotifyParts.insert(partId);
}

bool TOperation::IsReadyToNotify(const TActorContext& ctx) const {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TOperation IsReadyToNotify"
                    << ", TxId: " << TxId
                    << ", ready parts: " << ReadyToNotifyParts.size() << "/" << Parts.size()
                    << ", is published: " << (IsPublished() ? "true" : "false"));

    return IsReadyToNotify();
}

bool TOperation::IsReadyToNotify() const {
    return IsPublished() && ReadyToNotifyParts.size() == Parts.size();
}

void TOperation::AddNotifySubscriber(const TActorId& actorId) {
    Y_VERIFY(!IsReadyToNotify());
    Subscribers.insert(actorId);
}

void TOperation::DoNotify(TSchemeShard*, TSideEffects& sideEffects, const TActorContext& ctx) {
    Y_VERIFY(IsReadyToNotify());

    for (auto& subsriber: Subscribers) {
        THolder<TEvSchemeShard::TEvNotifyTxCompletionResult> msg = MakeHolder<TEvSchemeShard::TEvNotifyTxCompletionResult>(ui64(TxId));
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TOperation DoNotify"
                        << " send TEvNotifyTxCompletionResult"
                        << " to actorId: " << subsriber
                        << " message: " << msg->Record.ShortDebugString());

        sideEffects.Send(subsriber, msg.Release(), ui64(TxId));
    }

    Subscribers.clear();
}

bool TOperation::IsReadyToDone(const TActorContext& ctx) const {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TOperation IsReadyToDone "
                    << " TxId: " << TxId
                    << " ready parts: " << DoneParts.size() << "/" << Parts.size());

    return DoneParts.size() == Parts.size();
}

bool TOperation::IsReadyToPropose(const TActorContext& ctx) const {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TOperation IsReadyToPropose "
                    << ", TxId: " << TxId
                    << " ready parts: " << ReadyToProposeParts.size() << "/" << Parts.size());

    return IsReadyToPropose();
}

bool TOperation::IsReadyToPropose() const {
    return ReadyToProposeParts.size() == Parts.size();
}

void TOperation::ProposePart(TSubTxId partId, TPathId pathId, TStepId minStep) {
    Proposes.push_back(TProposeRec(partId, pathId, minStep));
    ReadyToProposeParts.insert(partId);
}

void TOperation::ProposePart(TSubTxId partId, TTabletId tableId) {
    ShardsProposes.push_back(TProposeShards(partId, tableId));
    ReadyToProposeParts.insert(partId);
}

void TOperation::DoPropose(TSchemeShard* ss, TSideEffects& sideEffects, const TActorContext& ctx) const {
    Y_VERIFY(IsReadyToPropose());

    //agregate
    TTabletId selfTabletId = ss->SelfTabletId();
    TTabletId coordinatorId = InvalidTabletId; //common for all part
    TStepId effectiveMinStep = TStepId(0);

    for (auto& rec: Proposes) {
        TSubTxId partId = InvalidSubTxId;
        TPathId pathId = InvalidPathId;
        TStepId minStep = InvalidStepId;
        std::tie(partId, pathId, minStep) = rec;

        {
            TTabletId curCoordinatorId = ss->SelectCoordinator(TxId, pathId);
            if (coordinatorId == InvalidTabletId) {
                coordinatorId = curCoordinatorId;
            }
            Y_VERIFY(coordinatorId == curCoordinatorId);
        }

        effectiveMinStep = Max<TStepId>(effectiveMinStep, minStep);
    }

    TSet<TTabletId> shards;
    for (auto& rec: ShardsProposes) {
        TSubTxId partId = InvalidSubTxId;
        TTabletId shard = InvalidTabletId;
        std::tie(partId, shard) = rec;
        shards.insert(shard);

        sideEffects.RouteByTablet(TOperationId(TxId, partId), shard);
    }

    shards.insert(selfTabletId);

    {
        const ui8 execLevel = 0;
        const TStepId maxStep = TStepId(Max<ui64>());
        THolder<TEvTxProxy::TEvProposeTransaction> message(
            new TEvTxProxy::TEvProposeTransaction(ui64(coordinatorId), ui64(TxId), execLevel, ui64(effectiveMinStep), ui64(maxStep)));
        auto* proposal = message->Record.MutableTransaction();
        auto* reqAffectedSet = proposal->MutableAffectedSet();
        reqAffectedSet->Reserve(shards.size());
        for (auto affectedTablet : shards) {
            auto* x = reqAffectedSet->Add();
            x->SetTabletId(ui64(affectedTablet));
            x->SetFlags(2 /*todo: use generic enum*/);
        }

        // TODO: probably want this for drops only
        proposal->SetIgnoreLowDiskSpace(true);

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TOperation DoPropose"
                        << " send propose"
                        << " to coordinator: " << coordinatorId
                        << " message:" << message->Record.ShortDebugString());

        sideEffects.BindMsgToPipe(TOperationId(TxId, InvalidSubTxId), coordinatorId, TPipeMessageId(0, TxId), message.Release());
    }
}

void TOperation::RegisterRelationByTabletId(TSubTxId partId, TTabletId tablet, const TActorContext& ctx) {
    if (RelationsByTabletId.contains(tablet)) {
        if (RelationsByTabletId.at(tablet) != partId) {
            // it is Ok if Hive otherwise it is error
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TOperation RegisterRelationByTabletId"
                            << " collision in routes has found"
                            << ", TxId: " << TxId
                            << ", partId: " << partId
                            << ", prev tablet: " << RelationsByTabletId.at(tablet)
                            << ", new tablet: " << tablet);

            RelationsByTabletId.erase(tablet);
        }
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TOperation RegisterRelationByTabletId"
                    << ", TxId: " << TxId
                    << ", partId: " << partId
                    << ", tablet: " << tablet);

    RelationsByTabletId[tablet] = partId;
}

TSubTxId TOperation::FindRelatedPartByTabletId(TTabletId tablet, const TActorContext& ctx) {
    auto partIdPtr = RelationsByTabletId.FindPtr(tablet);
    auto partId = partIdPtr == nullptr ? InvalidSubTxId : *partIdPtr;

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TOperation FindRelatedPartByTabletId"
                    << ", TxId: " << TxId
                    << ", tablet: " << tablet
                    << ", partId: " << partId);

    return partId;
}

void TOperation::RegisterRelationByShardIdx(TSubTxId partId, TShardIdx shardIdx, const TActorContext& ctx) {
    if (RelationsByShardIdx.contains(shardIdx)) {
        Y_VERIFY(RelationsByShardIdx.at(shardIdx) == partId);
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TOperation RegisterRelationByShardIdx"
                    << ", TxId: " << TxId
                    << ", shardIdx: " << shardIdx
                    << ", partId: " << partId);

    RelationsByShardIdx[shardIdx] = partId;
}


TSubTxId TOperation::FindRelatedPartByShardIdx(TShardIdx shardIdx, const TActorContext& ctx) {
    auto partIdPtr = RelationsByShardIdx.FindPtr(shardIdx);
    auto partId = partIdPtr == nullptr ? InvalidSubTxId : *partIdPtr;

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TOperation FindRelatedPartByShardIdx"
                    << ", TxId: " << TxId
                    << ", shardIdx: " << shardIdx
                    << ", partId: " << partId);

    return partId;
}

void TOperation::WaitShardCreated(TShardIdx shardIdx, TSubTxId partId) {
    WaitingShardCreatedByShard[shardIdx].insert(partId);
    WaitingShardCreatedByPart[partId].insert(shardIdx);
}

TVector<TSubTxId> TOperation::ActivateShardCreated(TShardIdx shardIdx) {
    TVector<TSubTxId> parts;

    auto it = WaitingShardCreatedByShard.find(shardIdx);
    if (it != WaitingShardCreatedByShard.end()) {
        for (auto partId : it->second) {
            auto itByPart = WaitingShardCreatedByPart.find(partId);
            Y_VERIFY(itByPart != WaitingShardCreatedByPart.end());
            itByPart->second.erase(shardIdx);
            if (itByPart->second.empty()) {
                WaitingShardCreatedByPart.erase(itByPart);
                parts.push_back(partId);
            }
        }
        WaitingShardCreatedByShard.erase(it);
    }

    return parts;
}

void TOperation::RegisterWaitPublication(TSubTxId partId, TPathId pathId, ui64 pathVersion) {
    auto publication = TPublishPath(pathId, pathVersion);
    WaitingPublicationsByPart[partId].insert(publication);
    WaitingPublicationsByPath[publication].insert(partId);
}

TSet<TOperationId> TOperation::ActivatePartsWaitPublication(TPathId pathId, ui64 pathVersion) {
    TSet<TOperationId> activateParts;

    auto it = WaitingPublicationsByPath.lower_bound({pathId, 0}); // iterate all path version [0; pathVersion]
    while (it != WaitingPublicationsByPath.end()
           && it->first.first == pathId && it->first.second <= pathVersion)
    {
        auto waitVersion = it->first.second;

        for (const auto& partId: it->second) {
            LOG_INFO_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "ActivateWaitPublication, publication confirmed"
                           << ", opId: " << TOperationId(TxId, partId)
                           << ", pathId: " << pathId
                           << ", version: " << waitVersion);

            WaitingPublicationsByPart[partId].erase(TPublishPath(pathId, waitVersion));
            if (WaitingPublicationsByPart.at(partId).empty()) {
                WaitingPublicationsByPart.erase(partId);
            }

            activateParts.insert(TOperationId(TxId, partId)); // activate on every path
        }

        it = WaitingPublicationsByPath.erase(it); // move iterator it forwart to the next element
    }

    return activateParts;
}

ui64 TOperation::CountWaitPublication(TOperationId opId) {
    if (WaitingPublicationsByPart.contains(opId.GetSubTxId())) {
        return WaitingPublicationsByPart.at(opId.GetSubTxId()).size();
    }
    return 0;
}

}
}
