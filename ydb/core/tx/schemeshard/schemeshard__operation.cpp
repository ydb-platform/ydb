#include "schemeshard__operation.h"

#include "schemeshard__dispatch_op.h"
#include "schemeshard__operation_db_changes.h"
#include "schemeshard__operation_memory_changes.h"
#include "schemeshard__operation_part.h"
#include "schemeshard__operation_side_effects.h"
#include "schemeshard_audit_log.h"
#include "schemeshard_impl.h"
#include "schemeshard_operation_factory.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/schemeshard/generated/dispatch_op.h>

#include <ydb/library/protobuf_printer/security_printer.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

std::tuple<TMaybe<NACLib::TUserToken>, bool> ParseUserToken(const TString& tokenStr) {
    TMaybe<NACLib::TUserToken> result;
    bool parseError = false;

    if (!tokenStr.empty()) {
        NACLibProto::TUserToken tokenPb;
        if (tokenPb.ParseFromString(tokenStr)) {
            result = NACLib::TUserToken(tokenPb);
        } else {
            parseError = true;
        }
    }

    return std::make_tuple(result, parseError);
}

TString FormatSourceLocationInfo(const NKikimr::NCompat::TSourceLocation& location) {
    TString locationInfo = "";
    if (const char* fileName = location.file_name(); fileName && *fileName && location.line() > 0) {
        locationInfo = TStringBuilder() << " (GetDB first called at " << fileName << ":" << location.line() << ")";
    }
    return locationInfo;
}

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

        ISubOperation::TPtr part = CreateTxCancelTx(Ev);
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

bool TSchemeShard::ProcessOperationParts(
    const TVector<ISubOperation::TPtr>& parts,
    const TTxId& txId,
    const NKikimrScheme::TEvModifySchemeTransaction& record,
    bool prevProposeUndoSafe,
    TOperation::TPtr& operation,
    THolder<TProposeResponse>& response,
    TOperationContext& context)
{
    auto selfId = SelfTabletId();
    const TString owner = record.HasOwner() ? record.GetOwner() : BUILTIN_ACL_ROOT;

    if (parts.size() > 1) {
        // allow altering impl index tables as part of consistent operation
        context.IsAllowedPrivateTables = true;
    }

    for (auto& part : parts) {
        TString errStr;
        if (!context.SS->CheckInFlightLimit(part->GetTransaction().GetOperationType(), errStr)) {
            response.Reset(new TProposeResponse(NKikimrScheme::StatusResourceExhausted, ui64(txId), ui64(selfId)));
            response->SetError(NKikimrScheme::StatusResourceExhausted, errStr);
        } else {
            response = part->Propose(owner, context);
        }

        Y_ABORT_UNLESS(response);

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "IgniteOperation"
                            << ", opId: " << operation->NextPartId()
                            << ", propose status:" << NKikimrScheme::EStatus_Name(response->Record.GetStatus())
                            << ", reason: " << response->Record.GetReason()
                            << ", at schemeshard: " << selfId);

        if (response->IsDone()) {
            operation->AddPart(part); //at ApplyOnExecute parts is erased
            context.OnComplete.DoneOperation(part->GetOperationId()); //mark it here by self for sure
        } else if (response->IsConditionalAccepted()) {
            //happens on retries, we answer like AlreadyExist or StatusSuccess with error message and do nothing in operation
            operation->AddPart(part); //at ApplyOnExecute parts is erased
            context.OnComplete.DoneOperation(part->GetOperationId()); //mark it here by self for sure
        } else if (response->IsAccepted()) {
            operation->AddPart(part);
            //context.OnComplete.ActivateTx(partOpId) ///TODO maybe it is good idea
        } else {
            if (!operation->Parts.empty()) {
                LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Abort operation: IgniteOperation fail to propose a part"
                                << ", opId: " << part->GetOperationId()
                                << ", at schemeshard:  " << selfId
                                << ", already accepted parts: " << operation->Parts.size()
                                << ", propose result status: " << NKikimrScheme::EStatus_Name(response->Record.GetStatus())
                                << ", with reason: " << response->Record.GetReason()
                                << ", tx message: " << SecureDebugString(record));
            }

            auto firstGetDbLocation = context.GetFirstGetDbLocation();
            TString locationInfo = FormatSourceLocationInfo(firstGetDbLocation);

            Y_VERIFY_S(context.IsUndoChangesSafe(),
                        "Operation is aborted and all changes should be reverted"
                            << ", but context.IsUndoChangesSafe is false, which means some direct writes have been done"
                            << ", opId: " << part->GetOperationId()
                            << ", at schemeshard:  " << selfId
                            << ", already accepted parts: " << operation->Parts.size()
                            << ", propose result status: " << NKikimrScheme::EStatus_Name(response->Record.GetStatus())
                            << ", with reason: " << response->Record.GetReason()
                            << ", first GetDB called at: " << locationInfo
                            << ", tx message: " << SecureDebugString(record));

            AbortOperationPropose(txId, context);

            return false;
        }

        // Check suboperations for undo safety. Log first unsafe suboperation in the schema transaction.
        if (prevProposeUndoSafe && !context.IsUndoChangesSafe()) {
            prevProposeUndoSafe = false;

            auto firstGetDbLocation = context.GetFirstGetDbLocation();
            TString locationInfo = FormatSourceLocationInfo(firstGetDbLocation);

            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Operation part proposed ok, but propose itself is undo unsafe"
                    << ", suboperation type: " << NKikimrSchemeOp::EOperationType_Name(part->GetTransaction().GetOperationType())
                    << ", opId: " << part->GetOperationId()
                    << ", at schemeshard:  " << selfId
                    << ", first GetDB called at: " << locationInfo
            );
        }
    }

    return true;
}

THolder<TProposeResponse> TSchemeShard::IgniteOperation(TProposeRequest& request, TOperationContext& context) {
    using namespace NGenerated;
    THolder<TProposeResponse> response = nullptr;

    auto selfId = SelfTabletId();
    auto& record = request.Record;
    auto txId = TTxId(record.GetTxId());

    if (Operations.contains(txId)) {
        response.Reset(new TProposeResponse(NKikimrScheme::StatusAccepted, ui64(txId), ui64(selfId)));
        response->SetError(NKikimrScheme::StatusAccepted, "There is operation with the same txId has been found in flight."
            " Actually that shouldn't have happened."
            " Note that tx body equality isn't granted."
            " StatusAccepted is just returned on retries.");
        return std::move(response);
    }

    TOperation::TPtr operation = new TOperation(txId);

    for (const auto& transaction : record.GetTransaction()) {
        auto quotaResult = operation->ConsumeQuota(transaction, context);
        if (quotaResult.Status != NKikimrScheme::StatusSuccess) {
            response.Reset(new TProposeResponse(quotaResult.Status, ui64(txId), ui64(selfId)));
            response->SetError(quotaResult.Status, quotaResult.Reason);
            return std::move(response);
        }
    }

    if (record.HasFailOnExist()) {
        // inherit FailOnExist from TEvModifySchemeTransaction into TModifyScheme
        for (auto& transaction : *record.MutableTransaction()) {
            if (!transaction.HasFailOnExist()) {
                transaction.SetFailOnExist(record.GetFailOnExist());
            }
        }
    }

    //

    TVector<TTxTransaction> rewrittenTransactions;

    // # Phase Zero
    // Rewrites transactions.
    // It may fill or clear particular fields based on some runtime SS state.

    for (auto tx : record.GetTransaction()) {
        if (DispatchOp(tx, [&](auto traits) { return traits.NeedRewrite && !Rewrite(traits, tx); })) {
            response.Reset(new TProposeResponse(NKikimrScheme::StatusPreconditionFailed, ui64(txId), ui64(selfId)));
            response->SetError(NKikimrScheme::StatusPreconditionFailed, "Invalid schema rewrite rule.");
            return std::move(response);
        }

        rewrittenTransactions.push_back(std::move(tx));
    }

    //

    TVector<TTxTransaction> transactions;
    TVector<TTxTransaction> generatedTransactions;

    // # Phase One
    // Generate MkDir transactions based on object name.

    for (const auto& transaction : rewrittenTransactions) {
        auto splitResult = operation->SplitIntoTransactions(transaction, context);
        if (splitResult.Status != NKikimrScheme::StatusSuccess) {
            response.Reset(new TProposeResponse(splitResult.Status, ui64(txId), ui64(selfId)));
            response->SetError(splitResult.Status, splitResult.Reason);
            return std::move(response);
        }

        std::move(splitResult.Transactions.begin(), splitResult.Transactions.end(), std::back_inserter(generatedTransactions));
        if (splitResult.Transaction) {
            transactions.push_back(*splitResult.Transaction);
        }
    }

    //

    Operations[txId] = operation; //record is erased at ApplyOnExecute if all parts are done at propose
    bool prevProposeUndoSafe = true;

    // # Phase Two
    // For generated MkDirs parts are constructed and proposed.
    // It is done to simplify checks in dependent (splitted) transactions

    for (const auto& transaction : generatedTransactions) {
        auto parts = operation->ConstructParts(transaction, context);
        operation->PreparedParts += parts.size();

        if (!ProcessOperationParts(parts, txId, record, prevProposeUndoSafe, operation, response, context)) {
            return std::move(response);
        }
    }

    // # Phase Three
    // For all initial transactions parts are constructed and proposed

    for (const auto& transaction : transactions) {
        auto parts = operation->ConstructParts(transaction, context);
        operation->PreparedParts += parts.size();

        if (!ProcessOperationParts(parts, txId, record, prevProposeUndoSafe, operation, response, context)) {
            return std::move(response);
        }
    }

    //

    return std::move(response);
}

void TSchemeShard::AbortOperationPropose(const TTxId txId, TOperationContext& context) {
    Y_ABORT_UNLESS(Operations.contains(txId));
    TOperation::TPtr operation = Operations.at(txId);

    // Drop operation side effects, undo memory changes
    // (Local db changes were already applied)
    context.OnComplete = {};
    context.DbChanges = {};

    for (auto& i : operation->Parts) {
        i->AbortPropose(context);
    }

    context.MemChanges.UnDo(context.SS);

    // And remove aborted operation from existence
    Operations.erase(txId);
}

void AbortOperation(TOperationContext& context, const TTxId txId, const TString& reason) {
    LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxOperationPropose Execute"
        << ", txId: " << txId
        << ", operation is rejected and all changes reverted"
        << ", " << reason
        << ", at schemeshard: " << context.SS->SelfTabletId()
    );

    context.GetTxc().DB.RollbackChanges();
    context.SS->AbortOperationPropose(txId, context);
}

bool IsCommitRedoSizeOverLimit(TString* reason, TOperationContext& context) {
    // MaxCommitRedoMB is the ICB control shared with NTabletFlatExecutor::TExecutor.
    // We subtract from MaxCommitRedoMB additional 1MB for anything extra
    // that executor/tablet may (or may not) add under the hood
    const ui64 limitBytes = (context.SS->MaxCommitRedoMB - 1) << 20;  // MB to bytes
    const ui64 commitRedoBytes = context.GetTxc().DB.GetCommitRedoBytes();
    if (commitRedoBytes >= limitBytes) {
        *reason = TStringBuilder()
            << "local tx commit redo size generated by IgniteOperation() is more than allowed limit: "
            << "commit redo size " << commitRedoBytes
            << ", limit " << limitBytes
            << ", excess " << (commitRedoBytes - limitBytes)
        ;
        return true;
    }
    return false;
}

struct TSchemeShard::TTxOperationPropose: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    using TBase = NTabletFlatExecutor::TTransactionBase<TSchemeShard>;

    TProposeRequest::TPtr Request;
    THolder<TProposeResponse> Response = nullptr;

    TString PeerName;
    TString UserSID;
    TString SanitizedToken;

    TSideEffects OnComplete;

    TTxOperationPropose(TSchemeShard* self, TProposeRequest::TPtr request)
        : TBase(self)
        , Request(request)
    {}

    TTxType GetTxType() const override { return TXTYPE_PROPOSE; }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
        TTabletId selfId = Self->SelfTabletId();
        auto txId = TTxId(Request->Get()->Record.GetTxId());

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxOperationPropose Execute"
                        << ", message: " << SecureDebugString(Request->Get()->Record)
                        << ", at schemeshard: " << selfId);

        txc.DB.NoMoreReadsForTx();

        auto [userToken, tokenParseError] = ParseUserToken(Request->Get()->Record.GetUserToken());
        if (tokenParseError) {
            Response = MakeHolder<TProposeResponse>(NKikimrScheme::StatusInvalidParameter, ui64(txId), ui64(selfId), "Failed to parse user token");
            return true;
        }
        if (userToken) {
            UserSID = userToken->GetUserSID();
            SanitizedToken = userToken->GetSanitizedToken();
        }
        PeerName = Request->Get()->Record.GetPeerName();

        TMemoryChanges memChanges;
        TStorageChanges dbChanges;
        TOperationContext context{Self, txc, ctx, OnComplete, memChanges, dbChanges, std::move(userToken)};
        context.PeerName = PeerName;

        //NOTE: Successful IgniteOperation will leave created operation in Self->Operations and accumulated changes in the context.
        // Unsuccessful IgniteOperation will leave no operation and context will also be clean.
        Response = Self->IgniteOperation(*Request->Get(), context);

        //NOTE: Successfully created operation also must be checked for the size of this local tx.
        //
        // Limitation on a commit redo size of local transactions is imposed at the tablet executor level
        // (See ydb/core/tablet_flat/flat_executor.cpp, NTabletFlatExecutor::TExecutor::ExecuteTransaction()).
        // And a tablet violating that limit is considered broken and will be stopped unconditionally and immediately.
        //
        // So even if operation was ignited successfully, it's local tx size still must be checked
        // as a precaution measure to avoid infinite loop of schemeshard restarting, attempting to propose
        // persisted operation again, hitting commit redo size limit and restarting again.
        //
        // On unsuccessful check, local tx should be rolled back, operation should be rejected and
        // all accumulated changes dropped or reverted.
        //

        // Actually build commit redo (dbChanges could be empty)
        dbChanges.Apply(Self, txc, ctx);

        if (Self->Operations.contains(txId)) {
            Y_ABORT_UNLESS(Response->IsDone() || Response->IsAccepted() || Response->IsConditionalAccepted());

            // Check local tx commit redo size
            TString reason;
            if (IsCommitRedoSizeOverLimit(&reason, context)) {
                Response = MakeHolder<TProposeResponse>(NKikimrScheme::StatusSchemeError, ui64(txId), ui64(selfId), reason);

                AbortOperation(context, txId, reason);

                if (!context.IsUndoChangesSafe()) {
                    LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxOperationPropose Execute"
                        << ", opId: " << txId
                        << ", operation should be rejected and all changes be reverted"
                        << ", but context.IsUndoChangesSafe is false, which means some direct writes have been done"
                        << ", message: " << SecureDebugString(Request->Get()->Record)
                        << ", at schemeshard: " << context.SS->SelfTabletId()
                    );
                }
            }
        }

        // Apply accumulated changes (changes could be empty)
        OnComplete.ApplyOnExecute(Self, txc, ctx);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_ABORT_UNLESS(Response);

        const auto& record = Request->Get()->Record;
        const auto txId = TTxId(record.GetTxId());

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxOperationPropose Complete"
                        << ", txId: " << txId
                        << ", response: " << Response->Record.ShortDebugString()
                        << ", at schemeshard: " << Self->TabletID());

        AuditLogModifySchemeTransaction(record, Response->Record, Self, PeerName, UserSID, SanitizedToken);

        //NOTE: Double audit output into the common log as a way to ease
        // transition to a new auditlog stream.
        // Should be removed when no longer needed.
        AuditLogModifySchemeTransactionDeprecated(record, Response->Record, Self, UserSID);

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

        ISubOperation::TPtr part = operation->Parts.at(ui64(OpId.GetSubTxId()));

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


//NOTE: There is certain time frame between initial event handling at TSchemeShard::Handle(*)
// and actual event processing at TTxOperationReply*::Execute() method.
// While TSchemeShard::Handle() tries to determine suboperation/operation part it should
// route event processing to, TTxOperationReply*::Execute() checks if those operation
// and suboperation are still exist and active.
// And it is perfectly ok if (sub)operation will manage to change their state within that
// time frame between TSchemeShard::Handle(*) and TTxOperationReply*::Execute().
// And it is generally ok if in that situation TTxOperationReply*::Execute() will skip
// to do anything with an incoming event.
//
// Generally, but not in all cases.
//
// There could be communication patterns that require other party to receive reaction
// from schemeshard (ack or reply) unconditionally, no matter what, possibly out-of-scope
// of any (sub)operation that initiated communication in the first place.
//
// Right now there is the case with DataShard's TEvDataShard::TEvSchemaChanged event.
// See below.
//
template <class TEvType>
void OutOfScopeEventHandler(const typename TEvType::TPtr&, TOperationContext&) {
    // Do nothing by default
}

// DataShard should receive reply on any single TEvDataShard::TEvSchemaChanged it will send
// to schemeshard even if that particular DataShard goes offline and back online right at
// perfect peculiar moments during (sub)operations.
// Not serving reply to a TEvDataShard::TEvSchemaChanged will leave Datashard
// in some transitional state which, for example, will prevent it from being stopped
// and deleted until schemeshard or datashard restart.
//
template <>
void OutOfScopeEventHandler<TEvDataShard::TEvSchemaChanged>(const TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) {
    const auto txId = ev->Get()->Record.GetTxId();
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "TTxOperationReply<" <<  ev->GetTypeName() << "> execute"
            << ", at schemeshard: " << context.SS->TabletID()
            << ", send out-of-scope reply, for txId " << txId
    );
    const TActorId ackTo = ev->Get()->GetSource();

    auto event = MakeHolder<TEvDataShard::TEvSchemaChangedResult>(txId);
    context.OnComplete.Send(ackTo, event.Release());
}

template <class TEvType>
struct TTxTypeFrom;

#define DefineTxTypeFromSpecialization(NS, TEvType, TxTypeValue) \
    template <> \
    struct TTxTypeFrom<::NKikimr::NS::TEvType> { \
        static constexpr TTxType TxType = TxTypeValue; \
    };

    SCHEMESHARD_INCOMING_EVENTS(DefineTxTypeFromSpecialization)
#undef DefineTxTypeFromSpecialization


template <class TEvType>
struct TTxOperationReply : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TOperationId OperationId;
    typename TEvType::TPtr EvReply;
    TSideEffects OnComplete;
    TMemoryChanges MemChanges;
    TStorageChanges DbChanges;

    TTxType GetTxType() const override {
        return TTxTypeFrom<TEvType>::TxType;
    }

    TTxOperationReply(TSchemeShard* self, TOperationId id, typename TEvType::TPtr& ev)
        : TBase(self)
        , OperationId(id)
        , EvReply(ev)
    {
        Y_ABORT_UNLESS(TEvType::EventType != TEvPrivate::TEvOperationPlan::EventType);
        Y_ABORT_UNLESS(TEvType::EventType != TEvTxProcessing::TEvPlanStep::EventType);
    }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {

        auto findActiveSubOperation = [this](const TOperationId& operationId) -> ISubOperation::TPtr {
            if (auto found = Self->Operations.find(operationId.GetTxId()); found != Self->Operations.cend()) {
                const auto operation = found->second;
                const auto subOperationId = operationId.GetSubTxId();
                if (!operation->DoneParts.contains(subOperationId)) {
                    return operation->Parts.at(subOperationId);
                }
            }
            return nullptr;
        };

        ISubOperation::TPtr part = findActiveSubOperation(OperationId);

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxOperationReply<" <<  EvReply->GetTypeName() << "> execute"
            << ", operationId: " << OperationId
            << ", at schemeshard: " << Self->TabletID()
            << ", message: " << ISubOperationState::DebugReply(EvReply)
        );

        {
            TOperationContext context{Self, txc, ctx, OnComplete, MemChanges, DbChanges};

            if (part) {
                part->HandleReply(EvReply, context);

            } else {
                LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxOperationReply<" <<  EvReply->GetTypeName() << "> execute"
                    << ", operationId: " << OperationId
                    << ", at schemeshard: " << Self->TabletID()
                    << ", unknown operation or suboperation is already done, event is out-of-scope"
                );

                OutOfScopeEventHandler<TEvType>(EvReply, context);
            }
        }
        OnComplete.ApplyOnExecute(Self, txc, ctx);
        DbChanges.Apply(Self, txc, ctx);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxOperationReply<" << EvReply->GetTypeName() << "> complete"
            << ", operationId: " << OperationId
            << ", at schemeshard: " << Self->TabletID()
        );
        OnComplete.ApplyOnComplete(Self, ctx);
    }
};


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

template <EventBasePtr TEvPtr>
NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxOperationReply(TOperationId id, TEvPtr& ev) {
    using TEvType = typename EventTypeFromTEvPtr<TEvPtr>::type;
    return new TTxOperationReply<TEvType>(this, id, ev);
}

#define DefineCreateTxOperationReplyFunc(NS, TEvType, ...) \
    template NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxOperationReply(TOperationId id, ::NKikimr::NS::TEvType ## __HandlePtr& ev);

    SCHEMESHARD_INCOMING_EVENTS(DefineCreateTxOperationReplyFunc)
#undef DefineCreateTxOperationReplyFunc


TString JoinPath(const TString& workingDir, const TString& name) {
    Y_ABORT_UNLESS(!name.StartsWith('/') && !name.EndsWith('/'), "%s", name.c_str());
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

    auto domainPathId = path.GetPathIdForDomain();
    auto domainInfo = path.DomainInfo();
    if (!domainInfo->TryConsumeSchemeQuota(context.Ctx.Now())) {
        result.Status = NKikimrScheme::StatusQuotaExceeded;
        result.Reason = "Request exceeded a limit on the number of schema operations, try again later.";
    }

    // Even if operation fails later we want to persist updated/consumed quotas
    NIceDb::TNiceDb db(context.GetTxc().DB); // write quotas directly in db even if operation fails
    context.SS->PersistSubDomainSchemeQuotas(db, domainPathId, *domainInfo);
    return result;
}

bool CreateDirs(const TTxTransaction& tx, const TPath& parentPath, TPath path, THashSet<TString>& createdPaths, TOperation::TSplitTransactionsResult& result) {
    auto initialSize = result.Transactions.size();

    while (path != parentPath) {
        if (createdPaths.contains(path.PathString())) {
            path.Rise();
            continue;
        }

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

        if (!checks) {
            result.Status = checks.GetStatus();
            result.Reason = checks.GetError();
            result.Transactions.clear();
            result.Transaction = tx;
            return false;
        }

        createdPaths.emplace(path.PathString());
        const TString name = path.LeafName();
        path.Rise();

        TTxTransaction mkdir;
        mkdir.SetFailOnExist(true);
        mkdir.SetAllowCreateInTempDir(tx.GetAllowCreateInTempDir());
        mkdir.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
        mkdir.SetWorkingDir(path.PathString());
        mkdir.MutableMkDir()->SetName(name);
        result.Transactions.push_back(mkdir);
    }

    Reverse(result.Transactions.begin() + initialSize, result.Transactions.end());

    return true;
}

// # Generates additional MkDirs for transactions
TOperation::TSplitTransactionsResult TOperation::SplitIntoTransactions(const TTxTransaction& tx, const TOperationContext& context) {
    TSplitTransactionsResult result;
    THashSet<TString> createdPaths;

    // # Generates MkDirs based on WorkingDir and path
    //     WorkingDir  |        TxType.ObjectName
    //  /Root/some_dir | other_dir/another_dir/object_name
    //                  ^---------^----------^
    // MkDir('/Root/some_dir', 'other_dir') and MkDir('/Root/some_dir/other_dir', 'another_dir') will be generated
    // tx.WorkingDir will be changed to '/Root/some_dir/other_dir/another_dir'
    // tx.TxType.ObjectName will be changed to 'object_name'
    if (DispatchOp(tx, [&](auto traits) { return traits.CreateDirsFromName; })) {
        TString targetName;

        if (!DispatchOp(tx, [&](auto traits) {
                auto name = GetTargetName(traits, tx);
                if (name) {
                    targetName = *name;
                    return true;
                }
                return false;
            }))
        {
            result.Transaction = tx;
            return result;
        }

        if (!targetName || targetName.StartsWith('/') || targetName.EndsWith('/')) {
            result.Transaction = tx;
            return result;
        }

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
                result.Transaction = tx;
                return result;
            }
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

            if (!checks) {
                result.Status = checks.GetStatus();
                result.Reason = checks.GetError();
                result.Transaction = tx;
                return result;
            }

            const TString name = path.LeafName();
            path.Rise();

            TTxTransaction create(tx);
            create.SetWorkingDir(path.PathString());
            create.SetFailOnExist(tx.GetFailOnExist());

            if (!DispatchOp(tx, [&](auto traits) {
                    return SetName(traits, create, name);
                }))
            {
                Y_ABORT("Invariant violation");
            }

            result.Transaction = create;

            if (exists) {
                return result;
            }
        }

        if (!CreateDirs(tx, parentPath, path, createdPaths, result)) {
            return result;
        }
    }

    // # Generates MkDirs based on transaction-specific requirements
    if (DispatchOp(tx, [&](auto traits) { return traits.CreateAdditionalDirs; })) {
        if (auto requiredPaths = DispatchOp(tx, [&](auto traits) { return GetRequiredPaths(traits, tx, context); }); requiredPaths) {
            for (const auto& [parentPathStr, pathStrs] : *requiredPaths) {
                const TPath parentPath = TPath::Resolve(parentPathStr, context.SS);
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
                        result.Transaction = tx;
                        return result;
                    }
                }

                for (const auto& pathStr : pathStrs) {
                    TPath path = TPath::Resolve(JoinPath(parentPathStr, pathStr), context.SS);
                    if (!CreateDirs(tx, parentPath, path, createdPaths, result)) {
                        return result;
                    }
                }
            }
        }
    }

    if (!result.Transaction) {
        result.Transaction = tx;
    }

    return result;
}

ISubOperation::TPtr TOperation::RestorePart(TTxState::ETxType txType, TTxState::ETxState txState, TOperationContext& context) const {
    TTxState* state = context.SS->FindTx(NextPartId());
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
        return CreateCopyTable(NextPartId(), txState, state);
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
    case TTxState::ETxType::TxAllocatePQ:
        Y_ABORT("deprecated");

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
        return CreateForceDropSubDomain(NextPartId(), txState);
    case TTxState::ETxType::TxCreateKesus:
        return CreateNewKesus(NextPartId(), txState);
    case TTxState::ETxType::TxAlterKesus:
        return CreateAlterKesus(NextPartId(), txState);
    case TTxState::ETxType::TxDropKesus:
        return CreateDropKesus(NextPartId(), txState);
    case TTxState::ETxType::TxInitializeBuildIndex:
        return CreateInitializeBuildIndexMainTable(NextPartId(), txState);
    case TTxState::ETxType::TxFinalizeBuildIndex:
        return CreateFinalizeBuildIndexMainTable(NextPartId(), txState);
    case TTxState::ETxType::TxDropTableIndexAtMainTable:
        return CreateDropTableIndexAtMainTable(NextPartId(), txState);
    case TTxState::ETxType::TxUpdateMainTableOnIndexMove:
        return CreateUpdateMainTableOnIndexMove(NextPartId(), txState);
    case TTxState::ETxType::TxCreateLock:
        return CreateLock(NextPartId(), txState);
    case TTxState::ETxType::TxDropLock:
        return DropLock(NextPartId(), txState);
    case TTxState::ETxType::TxAlterTableIndex:
        return CreateAlterTableIndex(NextPartId(), txState);
    case TTxState::ETxType::TxAlterSolomonVolume:
        return CreateAlterSolomon(NextPartId(), txState);

    // ExtSubDomain
    case TTxState::ETxType::TxCreateExtSubDomain:
        return CreateExtSubDomain(NextPartId(), txState);
    case TTxState::ETxType::TxAlterExtSubDomain:
        return CreateAlterExtSubDomain(NextPartId(), txState);
    case TTxState::ETxType::TxAlterExtSubDomainCreateHive:
        return CreateAlterExtSubDomainCreateHive(NextPartId(), txState);
    case TTxState::ETxType::TxForceDropExtSubDomain:
        return CreateForceDropExtSubDomain(NextPartId(), txState);

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
        return CreateNewCdcStreamAtTable(NextPartId(), txState, false);
    case TTxState::ETxType::TxCreateCdcStreamAtTableWithInitialScan:
        return CreateNewCdcStreamAtTable(NextPartId(), txState, true);
    case TTxState::ETxType::TxAlterCdcStream:
        return CreateAlterCdcStreamImpl(NextPartId(), txState);
    case TTxState::ETxType::TxAlterCdcStreamAtTable:
        return CreateAlterCdcStreamAtTable(NextPartId(), txState, false);
    case TTxState::ETxType::TxAlterCdcStreamAtTableDropSnapshot:
        return CreateAlterCdcStreamAtTable(NextPartId(), txState, true);
    case TTxState::ETxType::TxDropCdcStream:
        return CreateDropCdcStreamImpl(NextPartId(), txState);
    case TTxState::ETxType::TxDropCdcStreamAtTable:
        return CreateDropCdcStreamAtTable(NextPartId(), txState, false);
    case TTxState::ETxType::TxDropCdcStreamAtTableDropSnapshot:
        return CreateDropCdcStreamAtTable(NextPartId(), txState, true);
    case TTxState::ETxType::TxRotateCdcStream:
        return CreateRotateCdcStreamImpl(NextPartId(), txState);
    case TTxState::ETxType::TxRotateCdcStreamAtTable:
        return CreateRotateCdcStreamAtTable(NextPartId(), txState);

    // Sequences
    case TTxState::ETxType::TxCreateSequence:
        return CreateNewSequence(NextPartId(), txState);
    case TTxState::ETxType::TxAlterSequence:
        return CreateAlterSequence(NextPartId(), txState);
    case TTxState::ETxType::TxDropSequence:
        return CreateDropSequence(NextPartId(), txState);
    case TTxState::ETxType::TxCopySequence:
        return CreateCopySequence(NextPartId(), txState);
    case TTxState::ETxType::TxMoveSequence:
        return CreateMoveSequence(NextPartId(), txState);

    case TTxState::ETxType::TxFillIndex:
        Y_ABORT("deprecated");

    case TTxState::ETxType::TxMoveTable:
        return CreateMoveTable(NextPartId(), txState);
    case TTxState::ETxType::TxMoveTableIndex:
        return CreateMoveTableIndex(NextPartId(), txState);

    // Replication
    case TTxState::ETxType::TxCreateReplication:
        return CreateNewReplication(NextPartId(), txState);
    case TTxState::ETxType::TxAlterReplication:
        return CreateAlterReplication(NextPartId(), txState);
    case TTxState::ETxType::TxDropReplication:
        return CreateDropReplication(NextPartId(), txState, false);
    case TTxState::ETxType::TxDropReplicationCascade:
        return CreateDropReplication(NextPartId(), txState, true);

    // Transfer
    case TTxState::ETxType::TxCreateTransfer:
        return CreateNewTransfer(NextPartId(), txState);
    case TTxState::ETxType::TxAlterTransfer:
        return CreateAlterTransfer(NextPartId(), txState);
    case TTxState::ETxType::TxDropTransfer:
        return CreateDropTransfer(NextPartId(), txState, false);
    case TTxState::ETxType::TxDropTransferCascade:
        return CreateDropTransfer(NextPartId(), txState, true);

    // BlobDepot
    case TTxState::ETxType::TxCreateBlobDepot:
        return CreateNewBlobDepot(NextPartId(), txState);
    case TTxState::ETxType::TxAlterBlobDepot:
        return CreateAlterBlobDepot(NextPartId(), txState);
    case TTxState::ETxType::TxDropBlobDepot:
        return CreateDropBlobDepot(NextPartId(), txState);
    case TTxState::ETxType::TxCreateExternalTable:
        return CreateNewExternalTable(NextPartId(), txState);
    case TTxState::ETxType::TxDropExternalTable:
        return CreateDropExternalTable(NextPartId(), txState);
    case TTxState::ETxType::TxAlterExternalTable:
        return CreateAlterExternalTable(NextPartId(), txState);
    case TTxState::ETxType::TxCreateExternalDataSource:
        return CreateNewExternalDataSource(NextPartId(), txState);
    case TTxState::ETxType::TxDropExternalDataSource:
        return CreateDropExternalDataSource(NextPartId(), txState);
    case TTxState::ETxType::TxAlterExternalDataSource:
        return CreateAlterExternalDataSource(NextPartId(), txState);

    // View
    case TTxState::ETxType::TxCreateView:
        return CreateNewView(NextPartId(), txState);
    case TTxState::ETxType::TxDropView:
        return CreateDropView(NextPartId(), txState);
    case TTxState::ETxType::TxAlterView:
        Y_ABORT("TODO: implement");
    // Continuous Backup
    // Now these txs won't be called because we presist only cdc txs internally
    case TTxState::ETxType::TxCreateContinuousBackup:
        Y_ABORT("TODO: implement");
    case TTxState::ETxType::TxAlterContinuousBackup:
        Y_ABORT("TODO: implement");
    case TTxState::ETxType::TxDropContinuousBackup:
        Y_ABORT("TODO: implement");

    // ResourcePool
    case TTxState::ETxType::TxCreateResourcePool:
        return CreateNewResourcePool(NextPartId(), txState);
    case TTxState::ETxType::TxDropResourcePool:
        return CreateDropResourcePool(NextPartId(), txState);
    case TTxState::ETxType::TxAlterResourcePool:
        return CreateAlterResourcePool(NextPartId(), txState);

    case TTxState::ETxType::TxRestoreIncrementalBackupAtTable:
        return CreateRestoreIncrementalBackupAtTable(NextPartId(), txState);

    // BackupCollection
    case TTxState::ETxType::TxCreateBackupCollection:
        return CreateNewBackupCollection(NextPartId(), txState);
    case TTxState::ETxType::TxAlterBackupCollection:
        Y_ABORT("TODO: implement");
    case TTxState::ETxType::TxDropBackupCollection:
        return CreateDropBackupCollection(NextPartId(), txState);

    // SysView
    case TTxState::ETxType::TxCreateSysView:
        return CreateNewSysView(NextPartId(), txState);
    case TTxState::ETxType::TxDropSysView:
        return CreateDropSysView(NextPartId(), txState);

    // ChangePathState
    case TTxState::ETxType::TxChangePathState:
        return CreateChangePathState(NextPartId(), txState);

    case TTxState::ETxType::TxCreateLongIncrementalRestoreOp:
        return CreateLongIncrementalRestoreOpControlPlane(NextPartId(), txState);

    case TTxState::ETxType::TxInvalid:
        Y_UNREACHABLE();
    }

    Y_UNUSED(context); // TODO(Enjection): will be used by complex operations later

    Y_UNREACHABLE();
}

class TDefaultOperationFactory: public IOperationFactory {
public:
    TVector<ISubOperation::TPtr> MakeOperationParts(
        const TOperation& op,
        const TTxTransaction& tx,
        TOperationContext& context) const override;
};

IOperationFactory* DefaultOperationFactory() {
    return new TDefaultOperationFactory();
}

TVector<ISubOperation::TPtr> TDefaultOperationFactory::MakeOperationParts(
        const TOperation& op,
        const TTxTransaction& tx,
        TOperationContext& context) const
{
    const auto& opType = tx.GetOperationType();
    switch (opType) {
    case NKikimrSchemeOp::EOperationType::ESchemeOpMkDir:
        return {CreateMkDir(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpRmDir:
        return {CreateRmDir(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpModifyACL:
        return {CreateModifyACL(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterUserAttributes:
        return {CreateAlterUserAttrs(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropUnsafe:
        return {CreateForceDropUnsafe(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable:
        if (tx.GetCreateTable().HasCopyFromTable()) {
            return CreateCopyTable(op.NextPartId(), tx, context); // Copy indexes table as well as common table
        }
        return {CreateNewTable(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable:
        return CreateConsistentAlterTable(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpSplitMergeTablePartitions:
        return {CreateSplitMerge(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpBackup:
        return {CreateBackup(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpRestore:
        return {CreateRestore(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTable:
        return CreateDropIndexedTable(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable:
        return CreateIndexedTable(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex:
        Y_ABORT("is handled as part of ESchemeOpCreateIndexedTable");
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex:
        Y_ABORT("is handled as part of ESchemeOpDropTable");
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables:
        return CreateConsistentCopyTables(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateRtmrVolume:
        return {CreateNewRTMR(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore:
        return {CreateNewOlapStore(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnStore:
        return {CreateAlterOlapStore(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnStore:
        return {CreateDropOlapStore(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable:
        return {CreateNewColumnTable(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnTable:
        return {CreateAlterColumnTable(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnTable:
        return {CreateDropColumnTable(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup:
        return {CreateNewPQ(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup:
        return {CreateAlterPQ(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup:
        return {CreateDropPQ(op.NextPartId(), tx)};

    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSolomonVolume:
        return {CreateNewSolomon(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSolomonVolume:
        return {CreateAlterSolomon(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSolomonVolume:
        return {CreateDropSolomon(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain:
        return {CreateSubDomain(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain:
        return CreateCompatibleSubdomainAlter(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSubDomain:
        return {CreateDropSubdomain(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropSubDomain:
        return {CreateCompatibleSubdomainDrop(context.SS, op.NextPartId(), tx)};

    // ExtSubDomain
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExtSubDomain:
        return {CreateExtSubDomain(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExtSubDomain:
        return CreateCompatibleAlterExtSubDomain(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExtSubDomainCreateHive:
        Y_ABORT("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropExtSubDomain:
        return {CreateForceDropExtSubDomain(op.NextPartId(), tx)};

    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateKesus:
        return {CreateNewKesus(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterKesus:
        return {CreateAlterKesus(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropKesus:
        return {CreateDropKesus(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomain:
        return {CreateUpgradeSubDomain(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomainDecision:
        return {CreateUpgradeSubDomainDecision(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnBuild:
        return CreateBuildColumn(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexBuild:
        return CreateBuildIndex(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock:
        return {CreateLock(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropLock:
        return {DropLock(op.NextPartId(), tx)};

    // BlockStore
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlockStoreVolume:
        return {CreateNewBSV(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAssignBlockStoreVolume:
        return {CreateAssignBSV(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBlockStoreVolume:
        return {CreateAlterBSV(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropBlockStoreVolume:
        return {CreateDropBSV(op.NextPartId(), tx)};

    // FileStore
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateFileStore:
        return {CreateNewFileStore(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterFileStore:
        return {CreateAlterFileStore(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropFileStore:
        return {CreateDropFileStore(op.NextPartId(), tx)};

    // Login
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin:
        return {CreateAlterLogin(op.NextPartId(), tx)};

    // Sequence
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence:
        return {CreateNewSequence(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSequence:
        return {CreateAlterSequence(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence:
        return {CreateDropSequence(op.NextPartId(), tx)};

    // Index
    case NKikimrSchemeOp::EOperationType::ESchemeOpApplyIndexBuild:
        return ApplyBuildIndex(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTableIndex:
        Y_ABORT("multipart operations are handled before, also they require transaction details");

    case NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexImplTable:
        return {CreateInitializeBuildIndexImplTable(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexImplTable:
        Y_ABORT("multipart operations are handled before, also they require transaction details");

    case NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexMainTable:
        Y_ABORT("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexMainTable:
        Y_ABORT("multipart operations are handled before, also they require transaction details");

    case NKikimrSchemeOp::EOperationType::ESchemeOpCancelIndexBuild:
        return CancelBuildIndex(op.NextPartId(), tx, context);

    case NKikimrSchemeOp::EOperationType::ESchemeOpDropIndex:
        return CreateDropIndex(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndexAtMainTable:
        Y_ABORT("multipart operations are handled before, also they require transaction details");

    // CDC
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream:
        return CreateNewCdcStream(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamImpl:
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamAtTable:
        Y_ABORT("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStream:
        return CreateAlterCdcStream(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamImpl:
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamAtTable:
        Y_ABORT("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStream:
        return CreateDropCdcStream(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamImpl:
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamAtTable:
        Y_ABORT("multipart operations are handled before, also they require transaction details");
    case NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStream:
        return CreateRotateCdcStream(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStreamImpl:
    case NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStreamAtTable:
        Y_ABORT("multipart operations are handled before, also they require transaction details");

    case NKikimrSchemeOp::EOperationType::ESchemeOp_DEPRECATED_35:
        Y_ABORT("impossible");

    // Move
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveTable:
        return CreateConsistentMoveTable(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveTableIndex:
        return {CreateMoveTableIndex(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveIndex:
        return CreateConsistentMoveIndex(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveSequence:
        return {CreateMoveSequence(op.NextPartId(), tx)};

    // Replication
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateReplication:
        return {CreateNewReplication(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterReplication:
        return {CreateAlterReplication(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropReplication:
        return {CreateDropReplication(op.NextPartId(), tx, false)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropReplicationCascade:
        return {CreateDropReplication(op.NextPartId(), tx, true)};

    // Transfer
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTransfer:
        return {CreateNewTransfer(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTransfer:
        return {CreateAlterTransfer(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTransfer:
        return {CreateDropTransfer(op.NextPartId(), tx, false)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTransferCascade:
        return {CreateDropTransfer(op.NextPartId(), tx, true)};

    // BlobDepot
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlobDepot:
        return {CreateNewBlobDepot(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBlobDepot:
        return {CreateAlterBlobDepot(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropBlobDepot:
        return {CreateDropBlobDepot(op.NextPartId(), tx)};

    // ExternalTable
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalTable:
        return CreateNewExternalTable(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalTable:
        return {CreateDropExternalTable(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExternalTable:
        Y_ABORT("TODO: implement");

    // ExternalDataSource
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalDataSource:
        return CreateNewExternalDataSource(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalDataSource:
        return {CreateDropExternalDataSource(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExternalDataSource:
        Y_ABORT("TODO: implement");

    // View
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateView:
        return {CreateNewView(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropView:
        return {CreateDropView(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterView:
        Y_ABORT("TODO: implement");

    // CDC
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateContinuousBackup:
        return CreateNewContinuousBackup(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterContinuousBackup:
        return CreateAlterContinuousBackup(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropContinuousBackup:
        return CreateDropContinuousBackup(op.NextPartId(), tx, context);

    // ResourcePool
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateResourcePool:
        return {CreateNewResourcePool(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropResourcePool:
        return {CreateDropResourcePool(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterResourcePool:
        return {CreateAlterResourcePool(op.NextPartId(), tx)};

    // IncrementalBackup
    case NKikimrSchemeOp::EOperationType::ESchemeOpRestoreMultipleIncrementalBackups:
        return CreateRestoreMultipleIncrementalBackups(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpRestoreIncrementalBackupAtTable:
        Y_ABORT("multipart operations are handled before, also they require transaction details");

    // BackupCollection
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBackupCollection:
        return {CreateNewBackupCollection(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBackupCollection:
        Y_ABORT("TODO: implement");
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropBackupCollection:
        return CreateDropBackupCollectionCascade(op.NextPartId(), tx, context);

    case NKikimrSchemeOp::EOperationType::ESchemeOpBackupBackupCollection:
        return CreateBackupBackupCollection(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpBackupIncrementalBackupCollection:
        return CreateBackupIncrementalBackupCollection(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpRestoreBackupCollection:
        return CreateRestoreBackupCollection(op.NextPartId(), tx, context);
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateLongIncrementalRestoreOp:
        return {CreateLongIncrementalRestoreOpControlPlane(op.NextPartId(), tx)};

    // SysView
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSysView:
        return {CreateNewSysView(op.NextPartId(), tx)};
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSysView:
        return {CreateDropSysView(op.NextPartId(), tx)};

    // ChangePathState
    case NKikimrSchemeOp::EOperationType::ESchemeOpChangePathState:
        return CreateChangePathState(op.NextPartId(), tx, context);
    }

    Y_UNREACHABLE();
}

TVector<ISubOperation::TPtr> TOperation::ConstructParts(const TTxTransaction& tx, TOperationContext& context) const {
    return AppData()->SchemeOperationFactory->MakeOperationParts(*this, tx, context);
}

void TOperation::AddPart(ISubOperation::TPtr part) {
    Parts.push_back(part);
}

bool TOperation::AddPublishingPath(TPathId pathId, ui64 version) {
    Y_ABORT_UNLESS(!IsReadyToNotify());
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
    Y_ABORT_UNLESS(!IsReadyToNotify());
    Subscribers.insert(actorId);
}

void TOperation::DoNotify(TSchemeShard*, TSideEffects& sideEffects, const TActorContext& ctx) {
    Y_ABORT_UNLESS(IsReadyToNotify());

    for (auto& subscriber: Subscribers) {
        THolder<TEvSchemeShard::TEvNotifyTxCompletionResult> msg = MakeHolder<TEvSchemeShard::TEvNotifyTxCompletionResult>(ui64(TxId));
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TOperation DoNotify"
                        << " send TEvNotifyTxCompletionResult"
                        << " to actorId: " << subscriber
                        << " message: " << msg->Record.ShortDebugString());

        sideEffects.Send(subscriber, msg.Release(), ui64(TxId));
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
    Y_ABORT_UNLESS(IsReadyToPropose());

    //aggregate
    TTabletId selfTabletId = ss->SelfTabletId();
    TTabletId coordinatorId = InvalidTabletId; //common for all parts
    TStepId effectiveMinStep = TStepId(0);

    for (auto [_, pathId, minStep]: Proposes) {
        {
            TTabletId curCoordinatorId = ss->SelectCoordinator(TxId, pathId);
            if (coordinatorId == InvalidTabletId) {
                coordinatorId = curCoordinatorId;
            }
            Y_ABORT_UNLESS(coordinatorId == curCoordinatorId);
        }

        effectiveMinStep = Max<TStepId>(effectiveMinStep, minStep);
    }

    TSet<TTabletId> shards;
    for (auto [partId, shard]: ShardsProposes) {
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
            TSubTxId prevPartId = RelationsByTabletId.at(tablet);
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TOperation RegisterRelationByTabletId"
                            << " collision in routes has found"
                            << ", TxId# " << TxId
                            << ", partId# " << partId
                            << ", prevPartId# " << prevPartId
                            << ", tablet# " << tablet
                            << ", guessDefaultRootHive# " << (tablet == TTabletId(72057594037968897) ? "yes" : "no")
                            << ", prevTx# " << (prevPartId < Parts.size() ? Parts[prevPartId]->GetTransaction().ShortDebugString() : TString("unknown"))
                            << ", newTx# " << (partId < Parts.size() ? Parts[partId]->GetTransaction().ShortDebugString() : TString("unknown"))
                            );

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

TSubTxId TOperation::FindRelatedPartByTabletId(TTabletId tablet, const TActorContext& ctx) const {
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
        Y_ABORT_UNLESS(RelationsByShardIdx.at(shardIdx) == partId);
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TOperation RegisterRelationByShardIdx"
                    << ", TxId: " << TxId
                    << ", shardIdx: " << shardIdx
                    << ", partId: " << partId);

    RelationsByShardIdx[shardIdx] = partId;
}


TSubTxId TOperation::FindRelatedPartByShardIdx(TShardIdx shardIdx, const TActorContext& ctx) const {
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
            Y_ABORT_UNLESS(itByPart != WaitingShardCreatedByPart.end());
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

        it = WaitingPublicationsByPath.erase(it); // move iterator it forward to the next element
    }

    return activateParts;
}

ui64 TOperation::CountWaitPublication(TOperationId opId) const {
    auto it = WaitingPublicationsByPart.find(opId.GetSubTxId());
    if (it == WaitingPublicationsByPart.end()) {
        return 0;
    }

    return it->second.size();
}

}
