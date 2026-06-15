#include "schemeshard_impl.h"
#include "schemeshard_set_column_constraint.h"
#include <ydb/core/tx/schemeshard/index/build_index_helpers.h>

namespace NKikimr {
namespace NSchemeShard {

namespace {
    // TODO(flown4qqqq): Implement all Persist* functions in the next pull requests.
    // For now we intentionally keep them as stubs so current tests work,
    // because they do not cover restart scenarios yet.
    // This is used for LOG_E macros
    const TString LogPrefix = "[SetColumnConstraint]";
} // anonymous namespace

void TSchemeShard::PersistCreateSetColumnConstraint(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistCreateSetColumnConstraint is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintState(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintState is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintLockTxId(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintLockTxId is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintLockTxStatus(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintLockTxStatus is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintLockTxDone(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintLockTxDone is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintLockNullWritesTxId(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintLockNullWritesTxId is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintLockNullWritesTxStatus(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintLockNullWritesTxStatus is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintLockNullWritesTxDone(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintLockNullWritesTxDone is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintUnlockNullWritesTxId(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintUnlockNullWritesTxId is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintUnlockNullWritesTxStatus(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintUnlockNullWritesTxStatus is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintUnlockNullWritesTxDone(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintUnlockNullWritesTxDone is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintUnlockTxId(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintUnlockTxId is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintUnlockTxStatus(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintUnlockTxStatus is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintUnlockTxDone(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintUnlockTxDone is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintValidationSnapshot(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    Y_UNUSED(db);
    Y_UNUSED(operationInfo);
    LOG_E("PersistSetColumnConstraintValidationSnapshot is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintValidationShardStatus(NIceDb::TNiceDb& db, TShardIdx shardIdx, const TIndexBuildShardStatus& status) {
    Y_UNUSED(db);
    Y_UNUSED(shardIdx);
    Y_UNUSED(status);
    LOG_E("PersistSetColumnConstraintValidationShardStatus is called, but is not implemented yet");
}

void TSchemeShard::PersistSetColumnConstraintValidationIssue(NIceDb::TNiceDb& db, const TString& issue) {
    Y_UNUSED(db);
    Y_UNUSED(issue);
    LOG_E("PersistSetColumnConstraintValidationIssue is called, but is not implemented yet");
}

void TSchemeShard::Handle(TEvSetColumnConstraint::TEvCreateRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCreateSetColumnConstraint(ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvValidateRowConditionResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    TIndexBuildId operationId = TIndexBuildId(record.GetId());
    Execute(CreateTxReplyValidateRowCondition(operationId, ev), ctx);
}

} // NSchemeShard
} // NKikimr
