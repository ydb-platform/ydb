#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/index/build_index_helpers.h>
#include <ydb/core/tx/schemeshard/index/build_index_tx_base.h>
#include <ydb/core/tx/schemeshard/index/common.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>
#include <ydb/core/tx/schemeshard/schemeshard_xxport__helpers.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>


namespace NKikimr {
namespace NSchemeShard {

namespace NSetColumnConstraint {

THolder<TEvSchemeShard::TEvModifySchemeTransaction> AlterMainTableLockNullWritesPropose(
    TSchemeShard* ss, const TSetColumnConstraintOperationInfo& operationInfo)
{
    Y_ENSURE(operationInfo.IsSetColumnConstraint(), "Unknown operation kind while building AlterMainTableLockPropose");

    auto doFunc = [](const TSetColumnConstraintOperationInfo& operationInfo, NKikimrSchemeOp::TModifyScheme& modifyScheme) -> void {
        for (const auto& columnName : operationInfo.NotNullColumns) {
            auto col = modifyScheme.MutableAlterTable()->AddColumns();
            col->SetName(TString(columnName));
            col->SetNotNull(true);
        }
    };

    return AlterMainTableProposeTemplate(ss, operationInfo, operationInfo.LockNullWritesTxId, doFunc);
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> AlterMainTableUnlockNullWritesPropose(
    TSchemeShard* ss, const TSetColumnConstraintOperationInfo& operationInfo)
{
    Y_ENSURE(operationInfo.IsSetColumnConstraint(), "Unknown operation kind while building AlterMainTableUnlockPropose");

    auto doFunc = [](const TSetColumnConstraintOperationInfo& operationInfo, NKikimrSchemeOp::TModifyScheme& modifyScheme) -> void {
        for (const auto& columnName : operationInfo.NotNullColumns) {
            auto col = modifyScheme.MutableAlterTable()->AddColumns();
            col->SetName(TString(columnName));
            col->SetNotNull(false);
        }
    };

    return AlterMainTableProposeTemplate(ss, operationInfo, operationInfo.UnlockNullWritesTxId, doFunc);
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

    void ReplyOnCreation(const TSetColumnConstraintOperationInfo& operationInfo,
                         const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        auto responseEv = MakeHolder<TEvSetColumnConstraint::TEvCreateResponse>(ui64(operationInfo.Id));

        auto& response = responseEv->Record;
        response.SetStatus(status);

        if (operationInfo.GetIssue()) {
            AddIssue(response.MutableIssues(), operationInfo.GetIssue());
        }

        LOG_N("TTxReplyModify: ReplyOnCreation"
              << ", id: " << operationInfo.Id
              << ", status: " << Ydb::StatusIds::StatusCode_Name(status)
              << ", error: " << operationInfo.GetIssue()
              << ", replyTo: " << operationInfo.CreateSender.ToString());

        Send(operationInfo.CreateSender, std::move(responseEv), 0, operationInfo.SenderCookie);
    }

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
        
        auto replyOnCreation = [&] {
            auto statusCode = TranslateStatusCode(record.GetStatus());

            if (statusCode != Ydb::StatusIds::SUCCESS) {
                // TODO(flown4qqqq): persist issue
                // TODO(flown4qqqq): forget operation on error
                // TODO(flown4qqqq): EraseBuildInfo(operationInfo);
            }

            ReplyOnCreation(operationInfo, statusCode);
            return true;
        };
        
        if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::LockTableOnSchemaOps) {
            Y_ENSURE(txId == operationInfo.LockTxId);
            operationInfo.LockTxStatus = record.GetStatus();
            Self->PersistSetColumnConstraintLockTxStatus(db, operationInfo);
            
            if (!replyOnCreation()) {
                return false;
            }
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

struct TTxReplyValidateRowCondition : public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TEvDataShard::TEvValidateRowConditionResponse::TPtr Response;

public:
    explicit TTxReplyValidateRowCondition(
        TSchemeShard* self,
        TIndexBuildId operationId,
        TEvDataShard::TEvValidateRowConditionResponse::TPtr& response)
        : TTxBase(self, operationId, TXTYPE_CREATE_SET_COLUMN_CONSTRAINT)
        , Response(response)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        const auto& record = Response->Get()->Record;
        const TTabletId tabletId = TTabletId(record.GetTabletId());

        LOG_I("TTxReplyValidateRowCondition: operationId# " << BuildId
            << ", tabletId# " << tabletId
            << ", status# " << record.GetStatus()
            << ", isValid# " << record.GetIsValid());

        auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(BuildId);
        if (!operationInfoPtr) {
            LOG_W("TTxReplyValidateRowCondition: operation not found, id# " << BuildId);
            return true;
        }

        auto& operationInfo = *operationInfoPtr->get();

        TShardIdx shardIdx;
        bool found = false;
        for (const auto& [idx, shardStatus] : operationInfo.ValidationShards) {
            if (Self->ShardInfos.at(idx).TabletID == tabletId) {
                shardIdx = idx;
                found = true;
                break;
            }
        }

        if (!found) {
            LOG_W("TTxReplyValidateRowCondition: shard not found for tabletId# " << tabletId);
            return true;
        }

        auto& shardStatus = operationInfo.ValidationShards.at(shardIdx);

        if (record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::DONE) {
            shardStatus.Status = NKikimrIndexBuilder::EBuildStatus::DONE;
            
            if (!record.GetIsValid()) {
                LOG_N("TTxReplyValidateRowCondition: validation failed on shard# " << shardIdx);
                operationInfo.ValidationFailed = true;
                
                for (const auto& issue : record.GetIssues()) {
                    NIceDb::TNiceDb db(txc.DB);
                    // todo: persist issue
                    LOG_N("TTxReplyValidateRowCondition: issue: " << issue.message());
                }
            }
            
            operationInfo.InProgressValidationShards.erase(shardIdx);
            operationInfo.DoneValidationShards.emplace_back(shardIdx);
            
            // todo: persist shard status
            
            Progress(BuildId);
            
        } else if (record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR ||
                   record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST) {
            LOG_E("TTxReplyValidateRowCondition: error on shard# " << shardIdx
                << ", status# " << record.GetStatus());
            
            shardStatus.Status = record.GetStatus();
            operationInfo.ValidationFailed = true;
            
            for (const auto& issue : record.GetIssues()) {
                NIceDb::TNiceDb db(txc.DB);
                // todo: persist issue
                LOG_E("TTxReplyValidateRowCondition: error issue: " << issue.message());
            }
            
            operationInfo.InProgressValidationShards.erase(shardIdx);
            operationInfo.DoneValidationShards.emplace_back(shardIdx);
            
            // todo: persist shard status
            
            Progress(BuildId);
            
        } else {
            LOG_D("TTxReplyValidateRowCondition: shard# " << shardIdx
                << " still in progress, status# " << record.GetStatus());
            shardStatus.Status = record.GetStatus();
            // todo: persist shard status
        }

        return true;
    }

    void DoComplete(const TActorContext& /*ctx*/) override {
    }

    void OnUnhandledException(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/,
        TIndexBuildInfo* operationInfo, const std::exception& exc) override
    {
        if (!operationInfo) {
            LOG_N("TTxReplyValidateRowCondition: OnUnhandledException: id not found"
                ", id# " << BuildId);
            return;
        }
        LOG_E("TTxReplyValidateRowCondition: OnUnhandledException"
            ", id# " << BuildId << ", exception: " << exc.what());
    }
};

}

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxProgressSetColumnConstraint
    : public TSchemeShard::TIndexBuilder::TTxBase
{
private:
    TMap<TTabletId, THolder<IEventBase>> ToTabletSend;

    bool InitiateValidationShards([[maybe_unused]] NIceDb::TNiceDb& db, TSetColumnConstraintOperationInfo& operationInfo) {
        LOG_D("InitiateValidationShards, id# " << BuildId);

        Y_ENSURE(operationInfo.ValidationShards.empty());
        Y_ENSURE(operationInfo.ToValidateShards.empty());
        Y_ENSURE(operationInfo.InProgressValidationShards.empty());
        Y_ENSURE(operationInfo.DoneValidationShards.empty());

        TPath path = TPath::Init(operationInfo.TablePathId, Self);
        if (!path.IsLocked()) {
            LOG_E("InitiateValidationShards: table is not locked, id# " << BuildId);
            return false;
        }
        Y_ENSURE(path.LockedBy() == operationInfo.LockTxId);

        TTableInfo::TPtr table = Self->Tables.at(path->PathId);

        for (const auto& partition : table->GetPartitions()) {
            Y_ENSURE(Self->ShardInfos.contains(partition.ShardIdx));
            
            // For validation, we scan the entire shard, so use empty range and lastKeyAck
            TIndexBuildShardStatus shardStatus(TSerializedTableRange{}, "");
            shardStatus.Status = NKikimrIndexBuilder::EBuildStatus::INVALID;
            
            auto [it, emplaced] = operationInfo.ValidationShards.emplace(partition.ShardIdx, std::move(shardStatus));
            Y_ENSURE(emplaced);
            
            operationInfo.ToValidateShards.emplace_back(partition.ShardIdx);
            
            // todo: persist shard status
            LOG_D("InitiateValidationShards: added shard " << partition.ShardIdx);
        }

        return true;
    }

    void SendValidateRowConditionRequest(TShardIdx shardIdx, TSetColumnConstraintOperationInfo& operationInfo) {
        auto ev = MakeHolder<TEvDataShard::TEvValidateRowConditionRequest>();
        auto& record = ev->Record;

        record.SetId(ui64(BuildId));
        
        TTabletId shardId = Self->ShardInfos.at(shardIdx).TabletID;
        record.SetTabletId(ui64(shardId));

        record.SetOwnerId(operationInfo.TablePathId.OwnerId);
        record.SetPathId(operationInfo.TablePathId.LocalPathId);

        auto& shardStatus = operationInfo.ValidationShards.at(shardIdx);
        record.SetSeqNoGeneration(Self->Generation());
        record.SetSeqNoRound(++shardStatus.SeqNoRound);

        for (const auto& columnName : operationInfo.NotNullColumns) {
            record.AddNotNullColumns(TString(columnName));
        }

        LOG_N("TTxProgressSetColumnConstraint: TEvValidateRowConditionRequest: " << record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    template<typename Send>
    bool SendToValidationShards(TSetColumnConstraintOperationInfo& operationInfo, Send&& send) {
        while (!operationInfo.ToValidateShards.empty() &&
               operationInfo.InProgressValidationShards.size() < operationInfo.MaxInProgressValidationShards) {
            auto shardIdx = operationInfo.ToValidateShards.front();
            operationInfo.ToValidateShards.pop_front();
            operationInfo.InProgressValidationShards.emplace(shardIdx);
            send(shardIdx);
        }

        return operationInfo.InProgressValidationShards.empty() && operationInfo.ToValidateShards.empty();
    }

    bool ValidateRowCondition(TTransactionContext& txc, TSetColumnConstraintOperationInfo& operationInfo) {
        LOG_D("ValidateRowCondition Start, id# " << BuildId);

        if (operationInfo.ValidationShards.empty()) {
            NIceDb::TNiceDb db(txc.DB);
            if (!InitiateValidationShards(db, operationInfo)) {
                return false;
            }
        }

        auto done = SendToValidationShards(operationInfo, [&](TShardIdx shardIdx) {
            SendValidateRowConditionRequest(shardIdx, operationInfo);
        }) && operationInfo.DoneValidationShards.size() == operationInfo.ValidationShards.size();

        if (done) {
            LOG_D("ValidateRowCondition Done, id# " << BuildId);
        }

        return done;
    }

public:
    explicit TTxProgressSetColumnConstraint(TSelf* self, TIndexBuildId operationId)
        : TTxBase(self, operationId, TXTYPE_CREATE_SET_COLUMN_CONSTRAINT)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(BuildId);
        Y_ENSURE(operationInfoPtr);
        auto& operationInfo = *operationInfoPtr->get();

        LOG_D("TTxProgressSetColumnConstraint::DoExecute, id# " << BuildId << "; OperationState = " << operationInfo.OperationState.GetStringValue());

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
                if (ValidateRowCondition(txc, operationInfo)) {
                    if (operationInfo.ValidationFailed) {
                        ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::UnlockNullWrites);
                    } else {
                        ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::UnlockTableOnSchemaOps);
                    }
                    Progress(BuildId);
                }

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
                SendNotificationsIfFinished(operationInfo);
                break;
            }
        }

        return true;
    }

    void DoComplete(const TActorContext& ctx) override {
        // Send all prepared messages via pipes (similar to build_index)
        for (auto& [shardId, ev]: ToTabletSend) {
            Self->IndexBuildPipes.Send(BuildId, shardId, std::move(ev), ctx);
        }
        ToTabletSend.clear();
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

ITransaction* TSchemeShard::CreateTxReplyValidateRowCondition(
    TIndexBuildId operationId,
    TEvDataShard::TEvValidateRowConditionResponse::TPtr& ev)
{
    return new NSetColumnConstraint::TTxReplyValidateRowCondition(this, operationId, ev);
}

} // namespace NSchemeShard
} // namespace NKikimr
