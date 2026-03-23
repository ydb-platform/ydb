#include "schemeshard_build_index.h"
#include "schemeshard_build_index_tx_base.h"
#include "schemeshard_impl.h"
#include "schemeshard_set_column_constraint.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_xxport__helpers.h"

#include "schemeshard_build_index_common.h"

#include <ydb/public/api/protos/ydb_status_codes.pb.h>


namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxProgressSetColumnConstraint
    : public TSchemeShard::TIndexBuilder::TTxBase
{
public:
    explicit TTxProgressSetColumnConstraint(TSelf* self, TIndexBuildId operationId)
        : TTxBase(self, operationId, TXTYPE_CREATE_SET_COLUMN_CONSTRAINT)
    {}

    bool DoExecute(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        LOG_D("TTxProgressSetColumnConstraint::DoExecute, id# " << BuildId);

        auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(BuildId);
        Y_ENSURE(operationInfoPtr);
        auto& operationInfo = *operationInfoPtr->get();

        switch (operationInfo.OperationState) {
            case TSetColumnConstraintOperationInfo::EOperationState::Invalid: {
                Y_UNREACHABLE();
                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::LockTableOnSchemaOps: {
                if (operationInfo.LockTxId == InvalidTxId) {
                    AllocateTxId(BuildId);
                } else if (operationInfo.LockTxStatus == NKikimrScheme::StatusSuccess) {
                    Send(Self->SelfId(), LockPropose(Self, operationInfo, operationInfo.LockTxId, TPath::Init(operationInfo.TablePathId, Self)), 0, ui64(BuildId));
                } else if (!operationInfo.LockTxDone) {
                    Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(operationInfo.LockTxId)));
                } else {
                    ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::LockNullWrites);
                    Progress(BuildId);
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::LockNullWrites: {
                LOG_N("TTxProgressSetColumnConstraint::DoExecute"
                    ": LockNullWrites not implemented yet"
                    ", id# " << BuildId);

                auto response = MakeHolder<TEvSetColumnConstraint::TEvCreateResponse>(ui64(BuildId));
                response->Record.SetStatus(Ydb::StatusIds::UNSUPPORTED);
                AddIssue(response->Record.MutableIssues(),
                    "SetColumnConstraint operation is not yet implemented");

                LOG_N("TTxProgressSetColumnConstraint::DoExecute: replying UNSUPPORTED"
                    << ", id# " << BuildId
                    << ", replyTo# " << operationInfo.CreateSender.ToString());

                Send(operationInfo.CreateSender, std::move(response), 0, operationInfo.SenderCookie);

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Validate: {

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::UnlockNullWrites: {

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::UnlockTableOnSchemaOps: {

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Done: {
                SendNotificationsIfFinished(operationInfo);
                break;
            }
        }

        return true;
    }

    void DoComplete(const TActorContext& /*ctx*/) override {
    }

    void OnUnhandledException(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/,
        TIndexBuildInfo* operationInfo, const std::exception& exc) override
    {
        if (!operationInfo) {
            LOG_N("TTxProgressSetColumnConstraint: OnUnhandledException: id not found"
                ", id# " << BuildId);
            return;
        }
        LOG_E("TTxProgressSetColumnConstraint: OnUnhandledException"
            ", id# " << BuildId
            << ", exception: " << exc.what());
    }
};

ITransaction* TSchemeShard::CreateTxSetColumnConstraintProgress(TIndexBuildId operationId) {
    return new TIndexBuilder::TTxProgressSetColumnConstraint(this, operationId);
}

struct TTxReplyAllocateSetColumnConstraint : public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TEvTxAllocatorClient::TEvAllocateResult::TPtr AllocateResult;

public:
    explicit TTxReplyAllocateSetColumnConstraint(
        TSchemeShard* self,
        TEvTxAllocatorClient::TEvAllocateResult::TPtr& allocateResult)
        : TTxBase(self, TIndexBuildId(allocateResult->Cookie), TXTYPE_CREATE_SET_COLUMN_CONSTRAINT)
        , AllocateResult(allocateResult)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        const auto txId = TTxId(AllocateResult->Get()->TxIds.front());

        auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(BuildId);
        if (!operationInfoPtr) {
            LOG_I("TTxReplyAllocateSetColumnConstraint: operation not found"
                ", cookie# " << AllocateResult->Cookie
                << ", txId# " << txId);
            return true;
        }

        auto& operationInfo = *operationInfoPtr->get();
        LOG_I("TTxReplyAllocateSetColumnConstraint, id# " << BuildId << ", txId# " << txId);

        if (!operationInfo.LockTxId) {
            NIceDb::TNiceDb db(txc.DB);
            operationInfo.LockTxId = txId;
            Self->PersistSetColumnConstraintLockTxId(db, operationInfo);
            Self->TxIdToSetColumnConstraintOperations[txId] = BuildId;
        }

        Progress(BuildId);
        return true;
    }

    void DoComplete(const TActorContext& /*ctx*/) override {}

    void OnUnhandledException(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/,
        TIndexBuildInfo* /*operationInfo*/, const std::exception& exc) override
    {
        LOG_E("TTxReplyAllocateSetColumnConstraint: OnUnhandledException"
            ", id# " << BuildId << ", exception: " << exc.what());
    }
};

struct TTxReplyModifySetColumnConstraint : public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr ModifyResult;

public:
    explicit TTxReplyModifySetColumnConstraint(
        TSchemeShard* self,
        TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& modifyResult)
        : TTxBase(self, InvalidIndexBuildId, TXTYPE_CREATE_SET_COLUMN_CONSTRAINT)
        , ModifyResult(modifyResult)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        const auto& record = ModifyResult->Get()->Record;
        const auto txId = TTxId(record.GetTxId());

        auto* operationIdPtr = Self->TxIdToSetColumnConstraintOperations.FindPtr(txId);
        if (!operationIdPtr) {
            LOG_I("TTxReplyModifySetColumnConstraint: operation not found, txId# " << txId);
            return true;
        }

        BuildId = *operationIdPtr;
        auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(BuildId);
        if (!operationInfoPtr) {
            LOG_I("TTxReplyModifySetColumnConstraint: operation not found by BuildId"
                ", id# " << BuildId << ", txId# " << txId);
            return true;
        }

        auto& operationInfo = *operationInfoPtr->get();
        LOG_I("TTxReplyModifySetColumnConstraint, id# " << BuildId
            << ", txId# " << txId
            << ", status# " << NKikimrScheme::EStatus_Name(record.GetStatus()));

        Y_ENSURE(txId == operationInfo.LockTxId);
        NIceDb::TNiceDb db(txc.DB);
        operationInfo.LockTxStatus = record.GetStatus();
        Self->PersistSetColumnConstraintLockTxStatus(db, operationInfo);

        Progress(BuildId);
        return true;
    }

    void DoComplete(const TActorContext& /*ctx*/) override {}

    void OnUnhandledException(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/,
        TIndexBuildInfo* /*operationInfo*/, const std::exception& exc) override
    {
        LOG_E("TTxReplyModifySetColumnConstraint: OnUnhandledException"
            ", id# " << BuildId << ", exception: " << exc.what());
    }
};

struct TTxReplyCompletedSetColumnConstraint : public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TTxId CompletedTxId;

public:
    explicit TTxReplyCompletedSetColumnConstraint(TSchemeShard* self, TTxId completedTxId)
        : TTxBase(self, InvalidIndexBuildId, TXTYPE_CREATE_SET_COLUMN_CONSTRAINT)
        , CompletedTxId(completedTxId)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        const auto txId = CompletedTxId;

        auto* operationIdPtr = Self->TxIdToSetColumnConstraintOperations.FindPtr(txId);
        if (!operationIdPtr) {
            LOG_I("TTxReplyCompletedSetColumnConstraint: operation not found, txId# " << txId);
            return true;
        }

        BuildId = *operationIdPtr;
        auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(BuildId);
        if (!operationInfoPtr) {
            LOG_I("TTxReplyCompletedSetColumnConstraint: operation not found by BuildId"
                ", id# " << BuildId << ", txId# " << txId);
            return true;
        }

        auto& operationInfo = *operationInfoPtr->get();
        LOG_I("TTxReplyCompletedSetColumnConstraint, id# " << BuildId << ", txId# " << txId);

        Y_ENSURE(txId == operationInfo.LockTxId);
        NIceDb::TNiceDb db(txc.DB);
        operationInfo.LockTxDone = true;
        Self->PersistSetColumnConstraintLockTxDone(db, operationInfo);

        Progress(BuildId);
        return true;
    }

    void DoComplete(const TActorContext& /*ctx*/) override {}

    void OnUnhandledException(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/,
        TIndexBuildInfo* /*operationInfo*/, const std::exception& exc) override
    {
        LOG_E("TTxReplyCompletedSetColumnConstraint: OnUnhandledException"
            ", id# " << BuildId << ", exception: " << exc.what());
    }
};

ITransaction* TSchemeShard::CreateTxReplyAllocateSetColumnConstraint(
    TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev)
{
    return new TTxReplyAllocateSetColumnConstraint(this, ev);
}

ITransaction* TSchemeShard::CreateTxReplyModifySetColumnConstraint(
    TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev)
{
    return new TTxReplyModifySetColumnConstraint(this, ev);
}

ITransaction* TSchemeShard::CreateTxReplyCompletedSetColumnConstraint(TTxId completedTxId) {
    return new TTxReplyCompletedSetColumnConstraint(this, completedTxId);
}

} // namespace NSchemeShard
} // namespace NKikimr
