#include "schemeshard_impl.h"
#include "schemeshard_set_column_constraint.h"
#include <ydb/core/tx/schemeshard/index/build_index_helpers.h>

namespace NKikimr {
namespace NSchemeShard {

TString SerializeSetColumnConstraintColumnNames(const std::vector<std::string>& columns) {
    return JoinSeq("$", columns);
}

std::vector<TString> DeserializeSetColumnConstraintColumnNames(const TString& serialized) {
    TVector<TString> parts;
    StringSplitter(serialized).Split('$').CollectInto(&parts);
    return std::vector<TString>(parts.begin(), parts.end());
}

void TSchemeShard::PersistCreateSetColumnConstraint(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    TString serializedColumnNames = SerializeSetColumnConstraintColumnNames(operationInfo.SetNotNullColumns);
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::TableOwnerId>(operationInfo.TablePathId.OwnerId),
        NIceDb::TUpdate<Schema::SetColumnConstraint::TableLocalId>(operationInfo.TablePathId.LocalPathId),
        NIceDb::TUpdate<Schema::SetColumnConstraint::SerializedColumnNames>(serializedColumnNames),
        NIceDb::TUpdate<Schema::SetColumnConstraint::ValidationFailed>(operationInfo.ValidationFailed),
        NIceDb::TUpdate<Schema::SetColumnConstraint::OperationState>(ui32(operationInfo.OperationState))
    );
}

void TSchemeShard::PersistSetColumnConstraintState(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::OperationState>(ui32(operationInfo.OperationState))
    );
}

void TSchemeShard::PersistSetColumnConstraintLockTxId(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxId>(ui64(operationInfo.LockTxId))
    );
}

void TSchemeShard::PersistSetColumnConstraintLockTxStatus(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxStatus>(ui32(operationInfo.LockTxStatus))
    );
}

void TSchemeShard::PersistSetColumnConstraintLockTxDone(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxDone>(operationInfo.LockTxDone)
    );
}

void TSchemeShard::PersistSetColumnConstraintLockNullWritesTxId(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxId>(ui64(operationInfo.LockNullWritesTxId))
    );
}

void TSchemeShard::PersistSetColumnConstraintLockNullWritesTxStatus(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxStatus>(ui32(operationInfo.LockNullWritesTxStatus))
    );
}

void TSchemeShard::PersistSetColumnConstraintLockNullWritesTxDone(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxDone>(operationInfo.LockNullWritesTxDone)
    );
}

void TSchemeShard::PersistSetColumnConstraintUnlockNullWritesTxId(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxId>(ui64(operationInfo.UnlockNullWritesTxId))
    );
}

void TSchemeShard::PersistSetColumnConstraintUnlockNullWritesTxStatus(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxStatus>(ui32(operationInfo.UnlockNullWritesTxStatus))
    );
}

void TSchemeShard::PersistSetColumnConstraintUnlockNullWritesTxDone(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxDone>(operationInfo.UnlockNullWritesTxDone)
    );
}

void TSchemeShard::PersistSetColumnConstraintUnlockTxId(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxId>(ui64(operationInfo.UnlockTxId))
    );
}

void TSchemeShard::PersistSetColumnConstraintUnlockTxStatus(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxStatus>(ui32(operationInfo.UnlockTxStatus))
    );
}

void TSchemeShard::PersistSetColumnConstraintUnlockTxDone(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxDone>(operationInfo.UnlockTxDone)
    );
}

void TSchemeShard::PersistSetColumnConstraintValidationShardStatus(
    NIceDb::TNiceDb& db,
    TIndexBuildId operationId,
    TShardIdx shardIdx,
    const TValidateColumnConstraintShardStatus& status)
{
    db.Table<Schema::SetColumnConstraintDatashardStatuses>()
        .Key(ui64(operationId), shardIdx.GetOwnerId(), shardIdx.GetLocalId())
        .Update(
            NIceDb::TUpdate<Schema::SetColumnConstraintDatashardStatuses::Status>(status.ValidateStatus)
        );
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
