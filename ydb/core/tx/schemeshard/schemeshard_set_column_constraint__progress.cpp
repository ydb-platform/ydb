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

namespace NSetColumnConstraint {

THolder<TEvSchemeShard::TEvModifySchemeTransaction> AlterMainTableLockNullWritesPropose(
    TSchemeShard* ss, const TSetColumnConstraintOperationInfo& operationInfo)
{
    Y_ENSURE(operationInfo.IsSetColumnConstraint(), "Unknown operation kind while building AlterMainTableLockPropose");

    auto doFunc = [](const TSetColumnConstraintOperationInfo& operationInfo, NKikimrSchemeOp::TModifyScheme& modifyScheme) -> void {
        Y_UNUSED(operationInfo);
        Y_UNUSED(modifyScheme);
    };

    return AlterMainTableProposeTemplate(ss, operationInfo, doFunc);
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> AlterMainTableUnlockNullWritesPropose(
    TSchemeShard* ss, const TSetColumnConstraintOperationInfo& operationInfo)
{
    Y_ENSURE(operationInfo.IsSetColumnConstraint(), "Unknown operation kind while building AlterMainTableUnlockPropose");

    auto doFunc = [](const TSetColumnConstraintOperationInfo& operationInfo, NKikimrSchemeOp::TModifyScheme& modifyScheme) -> void {
        Y_UNUSED(operationInfo);
        Y_UNUSED(modifyScheme);
    };

    return AlterMainTableProposeTemplate(ss, operationInfo, doFunc);
}

struct TTxReplyAllocate : public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TEvTxAllocatorClient::TEvAllocateResult::TPtr AllocateResult;

public:
    explicit TTxReplyAllocate(
        TSchemeShard* self,
        TEvTxAllocatorClient::TEvAllocateResult::TPtr& allocateResult)
        : TTxBase(self, TIndexBuildId(allocateResult->Cookie), TXTYPE_CREATE_SET_COLUMN_CONSTRAINT)
        , AllocateResult(allocateResult)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        const auto txId = TTxId(AllocateResult->Get()->TxIds.front());

        auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(BuildId);
        if (!operationInfoPtr) {
            LOG_I("TTxReplyAllocate: operation not found"
                ", cookie# " << AllocateResult->Cookie
                << ", txId# " << txId);
            return true;
        }

        auto& operationInfo = *operationInfoPtr->get();
        LOG_I("TTxReplyAllocate, id# " << BuildId << ", txId# " << txId);

        NIceDb::TNiceDb db(txc.DB);
        if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::LockTableOnSchemaOps) {
            if (!operationInfo.LockTxId) {
                operationInfo.LockTxId = txId;
                Self->PersistSetColumnConstraintLockTxId(db, operationInfo);
                Self->TxIdToSetColumnConstraintOperations[txId] = BuildId;
            }
        } else if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::LockNullWrites) {
            if (!operationInfo.LockNullWritesTxId) {
                operationInfo.LockNullWritesTxId = txId;
                Self->PersistSetColumnConstraintLockNullWritesTxId(db, operationInfo);
                Self->TxIdToSetColumnConstraintOperations[txId] = BuildId;
            }
        } else if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::UnlockNullWrites) {
            if (!operationInfo.UnlockNullWritesTxId) {
                operationInfo.UnlockNullWritesTxId = txId;
                Self->PersistSetColumnConstraintUnlockNullWritesTxId(db, operationInfo);
                Self->TxIdToSetColumnConstraintOperations[txId] = BuildId;
            }
        } else if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::UnlockTableOnSchemaOps) {
            if (!operationInfo.UnlockTxId) {
                operationInfo.UnlockTxId = txId;
                Self->PersistSetColumnConstraintUnlockTxId(db, operationInfo);
                Self->TxIdToSetColumnConstraintOperations[txId] = BuildId;
            }
        } else {
            Y_UNREACHABLE();
        }

        Progress(BuildId);
        return true;
    }

    void DoComplete(const TActorContext& /*ctx*/) override {}

    void OnUnhandledException(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/,
        TIndexBuildInfo* /*operationInfo*/, const std::exception& exc) override
    {
        LOG_E("TTxReplyAllocate: OnUnhandledException"
            ", id# " << BuildId << ", exception: " << exc.what());
    }
};

struct TTxReplyModify : public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr ModifyResult;

public:
    explicit TTxReplyModify(
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
            LOG_I("TTxReplyModify: operation not found, txId# " << txId);
            return true;
        }

        BuildId = *operationIdPtr;
        auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(BuildId);
        if (!operationInfoPtr) {
            LOG_I("TTxReplyModify: operation not found by BuildId"
                ", id# " << BuildId << ", txId# " << txId);
            return true;
        }

        auto& operationInfo = *operationInfoPtr->get();
        LOG_I("TTxReplyModify, id# " << BuildId
            << ", txId# " << txId
            << ", status# " << NKikimrScheme::EStatus_Name(record.GetStatus()));

        NIceDb::TNiceDb db(txc.DB);
        if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::LockTableOnSchemaOps) {
            Y_ENSURE(txId == operationInfo.LockTxId);
            operationInfo.LockTxStatus = record.GetStatus();
            Self->PersistSetColumnConstraintLockTxStatus(db, operationInfo);
        } else if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::LockNullWrites) {
            Y_ENSURE(txId == operationInfo.LockNullWritesTxId);
            operationInfo.LockNullWritesTxStatus = record.GetStatus();
            Self->PersistSetColumnConstraintLockNullWritesTxStatus(db, operationInfo);
        } else if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::UnlockNullWrites) {
            Y_ENSURE(txId == operationInfo.UnlockNullWritesTxId);
            operationInfo.UnlockNullWritesTxStatus = record.GetStatus();
            Self->PersistSetColumnConstraintUnlockNullWritesTxStatus(db, operationInfo);
        } else if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::UnlockTableOnSchemaOps) {
            Y_ENSURE(txId == operationInfo.UnlockTxId);
            operationInfo.UnlockTxStatus = record.GetStatus();
            Self->PersistSetColumnConstraintUnlockTxStatus(db, operationInfo);
        } else {
            Y_UNREACHABLE();
        }

        Progress(BuildId);
        return true;
    }

    void DoComplete(const TActorContext& /*ctx*/) override {}

    void OnUnhandledException(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/,
        TIndexBuildInfo* /*operationInfo*/, const std::exception& exc) override
    {
        LOG_E("TTxReplyModify: OnUnhandledException"
            ", id# " << BuildId << ", exception: " << exc.what());
    }
};

struct TTxReplyCompleted : public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TTxId CompletedTxId;

public:
    explicit TTxReplyCompleted(TSchemeShard* self, TTxId completedTxId)
        : TTxBase(self, InvalidIndexBuildId, TXTYPE_CREATE_SET_COLUMN_CONSTRAINT)
        , CompletedTxId(completedTxId)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        const auto txId = CompletedTxId;

        auto* operationIdPtr = Self->TxIdToSetColumnConstraintOperations.FindPtr(txId);
        if (!operationIdPtr) {
            LOG_I("TTxReplyCompleted: operation not found, txId# " << txId);
            return true;
        }

        BuildId = *operationIdPtr;
        auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(BuildId);
        if (!operationInfoPtr) {
            LOG_I("TTxReplyCompleted: operation not found by BuildId"
                ", id# " << BuildId << ", txId# " << txId);
            return true;
        }

        auto& operationInfo = *operationInfoPtr->get();
        LOG_I("TTxReplyCompleted, id# " << BuildId << ", txId# " << txId);

        NIceDb::TNiceDb db(txc.DB);
        if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::LockTableOnSchemaOps) {
            Y_ENSURE(txId == operationInfo.LockTxId);
            operationInfo.LockTxDone = true;
            Self->PersistSetColumnConstraintLockTxDone(db, operationInfo);
        } else if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::LockNullWrites) {
            Y_ENSURE(txId == operationInfo.LockNullWritesTxId);
            operationInfo.LockNullWritesTxDone = true;
            Self->PersistSetColumnConstraintLockNullWritesTxDone(db, operationInfo);
        } else if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::UnlockNullWrites) {
            Y_ENSURE(txId == operationInfo.UnlockNullWritesTxId);
            operationInfo.UnlockNullWritesTxDone = true;
            Self->PersistSetColumnConstraintUnlockNullWritesTxDone(db, operationInfo);
        } else if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::UnlockTableOnSchemaOps) {
            Y_ENSURE(txId == operationInfo.UnlockTxId);
            operationInfo.UnlockTxDone = true;
            Self->PersistSetColumnConstraintUnlockTxDone(db, operationInfo);
        } else {
            Y_UNREACHABLE();
        }

        Progress(BuildId);
        return true;
    }

    void DoComplete(const TActorContext& /*ctx*/) override {}

    void OnUnhandledException(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/,
        TIndexBuildInfo* /*operationInfo*/, const std::exception& exc) override
    {
        LOG_E("TTxReplyCompleted: OnUnhandledException"
            ", id# " << BuildId << ", exception: " << exc.what());
    }
};

}

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
                if (operationInfo.LockNullWritesTxId == InvalidTxId) {
                    AllocateTxId(BuildId);
                } else if (operationInfo.LockNullWritesTxStatus == NKikimrScheme::StatusSuccess) {
                    Send(Self->SelfId(), NSetColumnConstraint::AlterMainTableLockNullWritesPropose(Self, operationInfo), 0, ui64(BuildId));
                } else if (!operationInfo.LockNullWritesTxDone) {
                    Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(operationInfo.LockNullWritesTxId)));
                } else {
                    ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::Validate);
                    Progress(BuildId);
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Validate: {
                ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::UnlockNullWrites);
                Progress(BuildId);

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::UnlockNullWrites: {
                if (operationInfo.UnlockNullWritesTxId == InvalidTxId) {
                    AllocateTxId(BuildId);
                } else if (operationInfo.UnlockNullWritesTxStatus == NKikimrScheme::StatusSuccess) {
                    Send(Self->SelfId(), NSetColumnConstraint::AlterMainTableUnlockNullWritesPropose(Self, operationInfo), 0, ui64(BuildId));
                } else if (!operationInfo.UnlockNullWritesTxDone) {
                    Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(operationInfo.UnlockNullWritesTxId)));
                } else {
                    ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::UnlockTableOnSchemaOps);
                    Progress(BuildId);
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::UnlockTableOnSchemaOps: {
                if (operationInfo.UnlockTxId == InvalidTxId) {
                    AllocateTxId(BuildId);
                } else if (operationInfo.UnlockTxStatus == NKikimrScheme::StatusSuccess) {
                    Send(Self->SelfId(), UnlockPropose(Self, operationInfo), 0, ui64(BuildId));
                } else if (!operationInfo.UnlockTxDone) {
                    Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(operationInfo.UnlockTxId)));
                } else {
                    ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::Done);
                    Progress(BuildId);
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Done: {
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

                // SendNotificationsIfFinished(operationInfo);
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

ITransaction* TSchemeShard::CreateTxReplyAllocateSetColumnConstraint(
    TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev)
{
    return new NSetColumnConstraint::TTxReplyAllocate(this, ev);
}

ITransaction* TSchemeShard::CreateTxReplyModifySetColumnConstraint(
    TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev)
{
    return new NSetColumnConstraint::TTxReplyModify(this, ev);
}

ITransaction* TSchemeShard::CreateTxReplyCompletedSetColumnConstraint(TTxId completedTxId) {
    return new NSetColumnConstraint::TTxReplyCompleted(this, completedTxId);
}

} // namespace NSchemeShard
} // namespace NKikimr
