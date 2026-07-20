#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/index/build_index_helpers.h>
#include <ydb/core/tx/schemeshard/index/build_index_tx_base.h>
#include <ydb/core/tx/schemeshard/index/common.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>
#include <ydb/core/tx/schemeshard/schemeshard_xxport__helpers.h>

#include <ydb/core/protos/set_column_constraint.pb.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>


namespace NKikimr {
namespace NSchemeShard {

namespace NSetColumnConstraint {

THolder<TEvSchemeShard::TEvModifySchemeTransaction> AlterMainTableLockNullWritesPropose(
    TSchemeShard* ss, const TSetColumnConstraintOperationInfo& operationInfo)
{
    Y_ENSURE(operationInfo.IsSetColumnConstraint(), "Unknown operation kind while building AlterMainTableLockPropose");

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(operationInfo.LockNullWritesTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    auto modifyScheme = AlterMainTableTemplate(ss, operationInfo);

    for (const auto& columnName : operationInfo.SetNotNullColumns) {
        auto col = modifyScheme.MutableAlterTable()->AddColumns();
        col->SetName(TString(columnName));
        col->SetSetNotNullInProgress(true);
    }

    *propose->Record.AddTransaction() = modifyScheme;

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> AlterMainTableUnlockNullWritesPropose(
    TSchemeShard* ss, const TSetColumnConstraintOperationInfo& operationInfo)
{
    Y_ENSURE(operationInfo.IsSetColumnConstraint(), "Unknown operation kind while building AlterMainTableUnlockPropose");

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(operationInfo.UnlockNullWritesTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    auto modifyScheme = AlterMainTableTemplate(ss, operationInfo);

    for (const auto& columnName : operationInfo.SetNotNullColumns) {
        auto col = modifyScheme.MutableAlterTable()->AddColumns();
        col->SetName(TString(columnName));
        col->SetSetNotNullInProgress(false);

        if (!operationInfo.ValidationFailed && !operationInfo.IsCancelled) {
            col->SetNotNull(true);
        }
    }

    *propose->Record.AddTransaction() = modifyScheme;

    return propose;
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
        switch (operationInfo.OperationState) {
            case TSetColumnConstraintOperationInfo::EOperationState::Locking: {
                if (!operationInfo.LockTxId) {
                    operationInfo.LockTxId = txId;
                    Self->PersistSetColumnConstraintLockTxId(db, operationInfo);
                    Self->TxIdToSetColumnConstraintOperations[txId] = BuildId;
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::LockingNullWrites: {
                if (!operationInfo.LockNullWritesTxId) {
                    operationInfo.LockNullWritesTxId = txId;
                    Self->PersistSetColumnConstraintLockNullWritesTxId(db, operationInfo);
                    Self->TxIdToSetColumnConstraintOperations[txId] = BuildId;
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Finishing: {
                if (!operationInfo.UnlockNullWritesTxId) {
                    operationInfo.UnlockNullWritesTxId = txId;
                    Self->PersistSetColumnConstraintUnlockNullWritesTxId(db, operationInfo);
                    Self->TxIdToSetColumnConstraintOperations[txId] = BuildId;
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Unlocking: {
                if (!operationInfo.UnlockTxId) {
                    operationInfo.UnlockTxId = txId;
                    Self->PersistSetColumnConstraintUnlockTxId(db, operationInfo);
                    Self->TxIdToSetColumnConstraintOperations[txId] = BuildId;
                }

                break;
            }

            case TSetColumnConstraintOperationInfo::EOperationState::Invalid:
            case TSetColumnConstraintOperationInfo::EOperationState::Validating:
            case TSetColumnConstraintOperationInfo::EOperationState::Done: {
                // We dont need to get TxId on these states
                Y_UNREACHABLE();
            }
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
                Self->ForgetSetColumnConstraint(db, operationInfo);
            }

            ReplyOnCreation(operationInfo, statusCode);
            return true;
        };

        // Most modifications of the main table are forbidden while SetColumnConstraint
        // holds its lock, however copying the table (e.g. for a backup) is allowed to
        // proceed concurrently (see TCopyTable::Propose: CheckLocks is skipped for
        // IsBackup copies). While such a copy is in flight the table has
        // PathStateCopying, and our own internal sub-transactions (which are gated by
        // NotUnderOperation()) may transiently fail with StatusMultipleModifications.
        // Just like build_index does, retry the very same proposal once the copy is
        // done instead of failing the whole operation.
        // Waiting for the conflicting operation's completion notification, not for
        // an immediate self-retry - see the comment on shouldRetry() below for why.
        bool waitingForDependency = false;

        auto shouldRetry = [&]() {
            if (record.GetStatus() != NKikimrScheme::StatusMultipleModifications) {
                return false;
            }

            auto it = Self->PathsById.find(operationInfo.TablePathId);
            if (it == Self->PathsById.end() || it->second->PathState != NKikimrSchemeOp::EPathStateCopying) {
                return false;
            }

            auto copyTxId = it->second->LastTxId;
            LOG_I("TTxReplyModify : Waiting for txId " << copyTxId << " to retry SetColumnConstraint id# " << BuildId);
            operationInfo.DependencyTxIds.insert(copyTxId);
            Self->TxIdToDependentSetColumnConstraint[copyTxId].insert(BuildId);
            Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(copyTxId)));
            return true;
        };

        switch (operationInfo.OperationState) {
            case TSetColumnConstraintOperationInfo::EOperationState::Locking: {
                Y_ENSURE(txId == operationInfo.LockTxId);
                operationInfo.LockTxStatus = record.GetStatus();
                Self->PersistSetColumnConstraintLockTxStatus(db, operationInfo);

                if (!replyOnCreation()) {
                    return false;
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::LockingNullWrites: {
                Y_ENSURE(txId == operationInfo.LockNullWritesTxId);
                if (shouldRetry()) {
                    operationInfo.LockNullWritesTxId = InvalidTxId;
                    Self->PersistSetColumnConstraintResetSubState(db, operationInfo);
                    waitingForDependency = true;
                } else {
                    operationInfo.LockNullWritesTxStatus = record.GetStatus();
                    Self->PersistSetColumnConstraintLockNullWritesTxStatus(db, operationInfo);
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Finishing: {
                Y_ENSURE(txId == operationInfo.UnlockNullWritesTxId);
                if (shouldRetry()) {
                    operationInfo.UnlockNullWritesTxId = InvalidTxId;
                    Self->PersistSetColumnConstraintResetSubState(db, operationInfo);
                    waitingForDependency = true;
                } else {
                    operationInfo.UnlockNullWritesTxStatus = record.GetStatus();
                    Self->PersistSetColumnConstraintUnlockNullWritesTxStatus(db, operationInfo);
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Unlocking: {
                Y_ENSURE(txId == operationInfo.UnlockTxId);
                if (shouldRetry()) {
                    operationInfo.UnlockTxId = InvalidTxId;
                    Self->PersistSetColumnConstraintResetSubState(db, operationInfo);
                    waitingForDependency = true;
                } else {
                    operationInfo.UnlockTxStatus = record.GetStatus();
                    Self->PersistSetColumnConstraintUnlockTxStatus(db, operationInfo);
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Invalid:
            case TSetColumnConstraintOperationInfo::EOperationState::Validating:
            case TSetColumnConstraintOperationInfo::EOperationState::Done: {
                Y_UNREACHABLE();
            }
        }

        if (!waitingForDependency) {
            Progress(BuildId);
        }
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

        if (Self->TxIdToDependentSetColumnConstraint.contains(txId)) {
            THashSet<TIndexBuildId> deps = std::move(Self->TxIdToDependentSetColumnConstraint.at(txId));
            Self->TxIdToDependentSetColumnConstraint.erase(txId);
            for (auto& dependentBuildId : deps) {
                LOG_I("TTxReplyCompleted txId: " << txId << " : trying to resume dependent SetColumnConstraint " << dependentBuildId);
                if (auto* dependentOperationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(dependentBuildId)) {
                    auto& dependentOperationInfo = **dependentOperationInfoPtr;
                    dependentOperationInfo.DependencyTxIds.erase(txId);
                    Progress(dependentBuildId);
                }
            }
            if (!operationIdPtr) {
                return true;
            }
        }

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
        if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::Locking) {
            Y_ENSURE(txId == operationInfo.LockTxId);
            operationInfo.LockTxDone = true;
            Self->PersistSetColumnConstraintLockTxDone(db, operationInfo);
        } else if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::LockingNullWrites) {
            Y_ENSURE(txId == operationInfo.LockNullWritesTxId);
            operationInfo.LockNullWritesTxDone = true;
            Self->PersistSetColumnConstraintLockNullWritesTxDone(db, operationInfo);
        } else if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::Finishing) {
            Y_ENSURE(txId == operationInfo.UnlockNullWritesTxId);
            operationInfo.UnlockNullWritesTxDone = true;
            Self->PersistSetColumnConstraintUnlockNullWritesTxDone(db, operationInfo);
        } else if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::Unlocking) {
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

        if (operationInfo.IsCancelled) {
            LOG_I("TTxReplyValidateRowCondition: operation is cancelled, ignoring message, id# " << BuildId);
            return true;
        }

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

        // There is a scenario where, even when using a pipe,
        // we may send a TEvValidateRowConditionRequest to a DataShard twice.
        // As a result, we may receive two responses.
        // Therefore, we should ignore extra responses so that
        // `DoneValidationShards` does not end up containing duplicates.
        if (!operationInfo.InProgressValidationShards.contains(shardIdx)) {
            LOG_N("TTxReplyValidateRowCondition: superfluous shard event, id# " << BuildId
                << ", shardIdx# " << shardIdx);
            return true;
        }

        auto oldValidationFailedValue = operationInfo.ValidationFailed;
        TString cancellationReason;

        auto& shardStatus = operationInfo.ValidationShards.at(shardIdx);
        shardStatus.ValidateStatus = record.GetStatus();

        if (record.IssuesSize() > 0) {
            TStringBuilder issuesText;
            for (size_t i = 0; i < record.IssuesSize(); ++i) {
                if (i > 0) {
                    issuesText << "; ";
                }
                issuesText << record.GetIssues(i).message();
            }
            shardStatus.DebugMessage = issuesText;
        }

        if (record.GetStatus() == NKikimrSetColumnConstraint::EValidateStatus::DONE) {
            if (!record.GetIsValid()) {
                LOG_N("TTxReplyValidateRowCondition: validation failed on shard# " << shardIdx);
                operationInfo.ValidationFailed = true;
                cancellationReason = "Validation failed: found NULL values in column(s) being set to NOT NULL";
            }

            operationInfo.InProgressValidationShards.erase(shardIdx);
            operationInfo.DoneValidationShards.insert(shardIdx);

            Progress(BuildId);

        } else if (record.GetStatus() == NKikimrSetColumnConstraint::EValidateStatus::BAD_REQUEST) {
            LOG_E("TTxReplyValidateRowCondition: error on shard# " << shardIdx
                << ", status# " << record.GetStatus());

            operationInfo.ValidationFailed = true;
            cancellationReason = "Validation failed: internal error";

            operationInfo.InProgressValidationShards.erase(shardIdx);
            operationInfo.DoneValidationShards.insert(shardIdx);

            Progress(BuildId);

        } else {
            LOG_D("TTxReplyValidateRowCondition: shard# " << shardIdx
                << " still in progress, status# " << record.GetStatus());
        }

        {
            NIceDb::TNiceDb db(txc.DB);

            if (oldValidationFailedValue != operationInfo.ValidationFailed) {
                Self->PersistSetColumnConstraintValidationFailedValue(db, operationInfo);

                operationInfo.MarkAsCancelled(std::move(cancellationReason));
                Self->PersistSetColumnConstraintCancellation(db, operationInfo);
            }

            Self->PersistSetColumnConstraintShardDone(db, BuildId, shardIdx, operationInfo);
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

struct TTxReplyRetrySetColumnConstraint : public TSchemeShard::TIndexBuilder::TTxBase
{
private:
    TTabletId ShardId;

public:
    explicit TTxReplyRetrySetColumnConstraint(TSelf* self, TIndexBuildId operationId, TTabletId shardId)
        : TTxBase(self, operationId, TXTYPE_CREATE_SET_COLUMN_CONSTRAINT)
        , ShardId(shardId)
    {}

    bool DoExecute([[maybe_unused]] TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& shardIdx = Self->GetShardIdx(ShardId);

        LOG_N("TTxReplyRetrySetColumnConstraint: PipeRetry"
            << ", id# " << BuildId
            << ", shardId# " << ShardId
            << ", shardIdx# " << shardIdx);

        auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(BuildId);
        if (!operationInfoPtr) {
            LOG_I("TTxReplyRetrySetColumnConstraint: operation not found, id# " << BuildId);
            return true;
        }

        auto& operationInfo = *operationInfoPtr->get();

        if (operationInfo.OperationState != TSetColumnConstraintOperationInfo::EOperationState::Validating) {
            LOG_I("TTxReplyRetrySetColumnConstraint: superfluous event, id# " << BuildId
                << ", state# " << ToString(operationInfo.OperationState));
            return true;
        }

        if (!operationInfo.ValidationShards.contains(shardIdx)) {
            LOG_I("TTxReplyRetrySetColumnConstraint: shard not found in ValidationShards"
                << ", id# " << BuildId
                << ", shardIdx# " << shardIdx);
            return true;
        }

        if (operationInfo.InProgressValidationShards.erase(shardIdx)) {
            operationInfo.ToValidateShards.emplace_front(shardIdx);

            Self->SetColumnConstraintPipes.Close(BuildId, ShardId, ctx);

            Progress(BuildId);
        }

        return true;
    }

    void DoComplete(const TActorContext& /*ctx*/) override {}

    void OnUnhandledException(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/,
        TIndexBuildInfo* /*operationInfo*/, const std::exception& exc) override
    {
        LOG_E("TTxReplyRetrySetColumnConstraint: OnUnhandledException"
            ", id# " << BuildId << ", exception: " << exc.what());
    }
};

} // NSetColumnConstraint

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxProgressSetColumnConstraint
    : public TSchemeShard::TIndexBuilder::TTxBase
{
private:
    TMap<TTabletId, THolder<IEventBase>> ToTabletSend;

    bool InitiateValidationShards(TSetColumnConstraintOperationInfo& operationInfo) {
        LOG_D("InitiateValidationShards, id# " << BuildId);

        Y_ENSURE(operationInfo.ToValidateShards.empty());
        Y_ENSURE(operationInfo.InProgressValidationShards.empty());

        TPath path = TPath::Init(operationInfo.TablePathId, Self);
        if (!path.IsLocked()) {
            LOG_E("InitiateValidationShards: table is not locked, id# " << BuildId);
            return false;
        }
        Y_ENSURE(path.LockedBy() == operationInfo.LockTxId);

        TTableInfo::TPtr table = Self->Tables.at(path->PathId);

        for (const auto* partition : table->GetPartitions()) {
            // We can initate shards after schemeshard's reboot.
            if (operationInfo.DoneValidationShards.contains(partition->ShardIdx)) {
                continue;
            }

            Y_ENSURE(Self->ShardInfos.contains(partition->ShardIdx));

            // For validation, we scan the entire shard, so use empty range and lastKeyAck
            TValidateColumnConstraintShardStatus shardStatus(TSerializedTableRange{}, "");
            shardStatus.Status = NKikimrIndexBuilder::EBuildStatus::INVALID;

            auto [it, emplaced] = operationInfo.ValidationShards.emplace(partition->ShardIdx, std::move(shardStatus));
            Y_ENSURE(emplaced);

            operationInfo.ToValidateShards.emplace_back(partition->ShardIdx);
            LOG_D("InitiateValidationShards: added shard " << partition->ShardIdx);
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

        for (const auto& columnName : operationInfo.SetNotNullColumns) {
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

    bool DriveToSendMessageToPartOfShards(TSetColumnConstraintOperationInfo& operationInfo) {
        LOG_D("DriveToSendMessageToPartOfShards Start, id# " << BuildId);

        if (operationInfo.NeedToCalculateValidationShards) {
            operationInfo.NeedToCalculateValidationShards = false;
            if (!InitiateValidationShards(operationInfo)) {
                return false;
            }
        }

        auto done = SendToValidationShards(operationInfo, [&](TShardIdx shardIdx) {
            SendValidateRowConditionRequest(shardIdx, operationInfo);
        }) && operationInfo.DoneValidationShards.size() == operationInfo.ValidationShards.size();

        if (done) {
            LOG_D("DriveToSendMessageToPartOfShards Done, id# " << BuildId);
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

        LOG_D("TTxProgressSetColumnConstraint::DoExecute, id# " << BuildId
            << "; OperationState = " << ToString(operationInfo.OperationState)
            << "; IsCancelled = " << operationInfo.IsCancelled);

        switch (operationInfo.OperationState) {
            case TSetColumnConstraintOperationInfo::EOperationState::Invalid: {
                Y_UNREACHABLE();
                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Locking: {
                if (operationInfo.LockTxId == InvalidTxId) {
                    AllocateTxId(BuildId);
                } else if (operationInfo.LockTxStatus == NKikimrScheme::StatusSuccess) {
                    Send(Self->SelfId(), LockPropose(Self, operationInfo, operationInfo.LockTxId, TPath::Init(operationInfo.TablePathId, Self)), 0, ui64(BuildId));
                } else if (!operationInfo.LockTxDone) {
                    Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(operationInfo.LockTxId)));
                } else {
                    ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::LockingNullWrites);
                    Progress(BuildId);
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::LockingNullWrites: {
                // If cancelled, skip to Finishing to release locks without setting constraint
                if (operationInfo.IsCancelled) {
                    LOG_I("TTxProgressSetColumnConstraint: operation cancelled in LockingNullWrites, jumping to Finishing, id# " << BuildId);
                    NIceDb::TNiceDb db(txc.DB);
                    ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::Finishing);
                    Progress(BuildId);
                    break;
                }

                if (operationInfo.LockNullWritesTxId == InvalidTxId) {
                    AllocateTxId(BuildId);
                } else if (operationInfo.LockNullWritesTxStatus == NKikimrScheme::StatusSuccess) {
                    Send(Self->SelfId(), NSetColumnConstraint::AlterMainTableLockNullWritesPropose(Self, operationInfo), 0, ui64(BuildId));
                } else if (!operationInfo.LockNullWritesTxDone) {
                    Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(operationInfo.LockNullWritesTxId)));
                } else {
                    ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::Validating);
                    Progress(BuildId);
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Validating: {
                // If cancelled, skip to Finishing to release locks without setting constraint
                if (operationInfo.IsCancelled) {
                    LOG_I("TTxProgressSetColumnConstraint: operation cancelled in Validating, jumping to Finishing, id# " << BuildId);
                    NIceDb::TNiceDb db(txc.DB);
                    ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::Finishing);
                    Progress(BuildId);
                    break;
                }

                if (DriveToSendMessageToPartOfShards(operationInfo)) {
                    ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::Finishing);
                    Progress(BuildId);
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Finishing: {
                if (operationInfo.UnlockNullWritesTxId == InvalidTxId) {
                    AllocateTxId(BuildId);
                } else if (operationInfo.UnlockNullWritesTxStatus == NKikimrScheme::StatusSuccess) {
                    Send(Self->SelfId(), NSetColumnConstraint::AlterMainTableUnlockNullWritesPropose(Self, operationInfo), 0, ui64(BuildId));
                } else if (!operationInfo.UnlockNullWritesTxDone) {
                    Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(operationInfo.UnlockNullWritesTxId)));
                } else {
                    ChangeState(BuildId, TSetColumnConstraintOperationInfo::EOperationState::Unlocking);
                    Progress(BuildId);
                }

                break;
            }
            case TSetColumnConstraintOperationInfo::EOperationState::Unlocking: {
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
            Self->SetColumnConstraintPipes.Send(BuildId, shardId, std::move(ev), ctx);
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

ITransaction* TSchemeShard::CreatePipeRetrySetColumnConstraint(TIndexBuildId operationId, TTabletId tabletId) {
    return new NSetColumnConstraint::TTxReplyRetrySetColumnConstraint(this, operationId, tabletId);
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

