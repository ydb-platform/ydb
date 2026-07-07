#include "schemeshard_impl.h"
#include "schemeshard_set_column_constraint.h"
#include <util/string/split.h>
#include <ydb/core/tx/schemeshard/index/index_build_info.h>

namespace NKikimr {
namespace NSchemeShard {

TString SerializeSetColumnConstraintColumnNames(const std::vector<TString>& columns) {
    NKikimrSetColumnConstraint::TSetColumnConstraintSettings data;
    for (const auto& column : columns) {
        data.AddNotNullColumns(column);
    }

    return data.SerializeAsString();
}

std::vector<TString> DeserializeSetColumnConstraintColumnNames(const TString& serialized) {
    std::vector<TString> result;

    NKikimrSetColumnConstraint::TSetColumnConstraintSettings data;
    if (data.ParseFromString(serialized)) {
        for (const auto& column : data.GetNotNullColumns()) {
            result.push_back(column);
        }
    }

    return result;
}

namespace {

float CalcSetColumnConstraintValidationProgress(const TSetColumnConstraintOperationInfo& operationInfo) {
    const ui64 total = operationInfo.ValidationShards.size();
    if (total == 0) {
        return 0.0f;
    }
    const ui64 done = operationInfo.DoneValidationShards.size();
    return static_cast<float>(done) / static_cast<float>(total) * 100.0f;
}

}

void FillSetColumnConstraint(
    NKikimrSetColumnConstraint::TSetColumnConstraint& proto,
    const TSetColumnConstraintOperationInfo& operationInfo,
    TSchemeShard* self)
{
    proto.SetId(ui64(operationInfo.Id));

    if (operationInfo.UserSID) {
        proto.SetUserSID(*operationInfo.UserSID);
    }
    if (operationInfo.StartTime != TInstant::Zero()) {
        *proto.MutableStartTime() = SecondsToProtoTimeStamp(operationInfo.StartTime.Seconds());
    }
    if (operationInfo.EndTime != TInstant::Zero()) {
        *proto.MutableEndTime() = SecondsToProtoTimeStamp(operationInfo.EndTime.Seconds());
    }

    // Map internal state to proto state
    using EState = TSetColumnConstraintOperationInfo::EOperationState;
    using ProtoState = Ydb::Table::SetNotNullState;
    switch (operationInfo.OperationState) {
    case EState::Locking:
    case EState::LockingNullWrites:
        proto.SetState(ProtoState::STATE_PREPARING);
        proto.SetProgress(0.0f);
        break;
    case EState::Validating:
        proto.SetState(ProtoState::STATE_VALIDATING);
        proto.SetProgress(CalcSetColumnConstraintValidationProgress(operationInfo));
        break;
    case EState::Finishing:
    case EState::Unlocking:
        proto.SetState(ProtoState::STATE_APPLYING);
        proto.SetProgress(99.9f);
        break;
    case EState::Done:
        proto.SetState(operationInfo.ValidationFailed
            ? ProtoState::STATE_CANCELLED
            : ProtoState::STATE_DONE);
        proto.SetProgress(100.0f);
        break;
    case EState::Invalid:
        proto.SetState(ProtoState::STATE_UNSPECIFIED);
        break;
    }

    auto* settings = proto.MutableSettings();
    TPath tablePath = TPath::Init(operationInfo.TablePathId, self);
    settings->SetTablePath(tablePath.PathString());
    for (const auto& col : operationInfo.SetNotNullColumns) {
        settings->AddNotNullColumns(TString(col));
    }
}

void TSchemeShard::PersistCreateSetColumnConstraint(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    TString serializedColumnNames = SerializeSetColumnConstraintColumnNames(operationInfo.SetNotNullColumns);
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::TableOwnerId>(operationInfo.TablePathId.OwnerId),
        NIceDb::TUpdate<Schema::SetColumnConstraint::TableLocalId>(operationInfo.TablePathId.LocalPathId),
        NIceDb::TUpdate<Schema::SetColumnConstraint::SerializedColumnNames>(serializedColumnNames),
        NIceDb::TUpdate<Schema::SetColumnConstraint::ValidationFailed>(operationInfo.ValidationFailed),
        NIceDb::TUpdate<Schema::SetColumnConstraint::OperationState>(ui32(operationInfo.OperationState)),
        NIceDb::TUpdate<Schema::SetColumnConstraint::StartTime>(operationInfo.StartTime.Seconds())
    );
    if (operationInfo.UserSID) {
        db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
            NIceDb::TUpdate<Schema::SetColumnConstraint::UserSID>(*operationInfo.UserSID)
        );
    }
}

void TSchemeShard::PersistSetColumnConstraintState(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::OperationState>(ui32(operationInfo.OperationState))
    );
    if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::Done) {
        db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
            NIceDb::TUpdate<Schema::SetColumnConstraint::EndTime>(operationInfo.EndTime.Seconds())
        );
    }
}

void TSchemeShard::PersistSetColumnConstraintResetSubState(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TNull<Schema::SetColumnConstraint::SubStateTxId>(),
        NIceDb::TNull<Schema::SetColumnConstraint::SubStateTxStatus>(),
        NIceDb::TNull<Schema::SetColumnConstraint::SubStateTxDone>()
    );
}

void TSchemeShard::PersistSetColumnConstraintLockTxId(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxId>(operationInfo.LockTxId),
        NIceDb::TUpdate<Schema::SetColumnConstraint::LockTxId>(operationInfo.LockTxId)
    );
}

void TSchemeShard::PersistSetColumnConstraintLockTxStatus(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxStatus>(operationInfo.LockTxStatus)
    );
}

void TSchemeShard::PersistSetColumnConstraintLockTxDone(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxDone>(operationInfo.LockTxDone)
    );
}

void TSchemeShard::PersistSetColumnConstraintLockNullWritesTxId(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxId>(operationInfo.LockNullWritesTxId)
    );
}

void TSchemeShard::PersistSetColumnConstraintLockNullWritesTxStatus(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxStatus>(operationInfo.LockNullWritesTxStatus)
    );
}

void TSchemeShard::PersistSetColumnConstraintLockNullWritesTxDone(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxDone>(operationInfo.LockNullWritesTxDone)
    );
}

void TSchemeShard::PersistSetColumnConstraintUnlockNullWritesTxId(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxId>(operationInfo.UnlockNullWritesTxId)
    );
}

void TSchemeShard::PersistSetColumnConstraintUnlockNullWritesTxStatus(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxStatus>(operationInfo.UnlockNullWritesTxStatus)
    );
}

void TSchemeShard::PersistSetColumnConstraintUnlockNullWritesTxDone(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxDone>(operationInfo.UnlockNullWritesTxDone)
    );
}

void TSchemeShard::PersistSetColumnConstraintUnlockTxId(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxId>(operationInfo.UnlockTxId)
    );
}

void TSchemeShard::PersistSetColumnConstraintUnlockTxStatus(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxStatus>(operationInfo.UnlockTxStatus)
    );
}

void TSchemeShard::PersistSetColumnConstraintUnlockTxDone(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& operationInfo) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::SubStateTxDone>(operationInfo.UnlockTxDone)
    );
}

void TSchemeShard::PersistSetColumnConstraintValidationFailedValue(
    NIceDb::TNiceDb& db,
    const TSetColumnConstraintOperationInfo& operationInfo)
{
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::ValidationFailed>(operationInfo.ValidationFailed)
    );
}

void TSchemeShard::PersistSetColumnConstraintCancellation(
    NIceDb::TNiceDb& db,
    const TSetColumnConstraintOperationInfo& operationInfo)
{
    db.Table<Schema::SetColumnConstraint>().Key(ui64(operationInfo.Id)).Update(
        NIceDb::TUpdate<Schema::SetColumnConstraint::IsCancelled>(operationInfo.IsCancelled),
        NIceDb::TUpdate<Schema::SetColumnConstraint::CancellationReason>(operationInfo.CancellationReason)
    );
}

void TSchemeShard::PersistSetColumnConstraintShardDone(
    NIceDb::TNiceDb& db,
    TIndexBuildId operationId,
    TShardIdx shardIdx,
    const TSetColumnConstraintOperationInfo& operationInfo)
{
    auto status = operationInfo.ValidationShards.at(shardIdx);
    db.Table<Schema::SetColumnConstraintShardStatus>()
        .Key(operationId, shardIdx.GetOwnerId(), shardIdx.GetLocalId())
        .Update(
            NIceDb::TUpdate<Schema::SetColumnConstraintShardStatus::Status>(status.ValidateStatus),
            NIceDb::TUpdate<Schema::SetColumnConstraintShardStatus::Issue>(status.DebugMessage)
        );
}

void TSchemeShard::PersistSetColumnConstraintForget(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& info) {
    db.Table<Schema::SetColumnConstraint>().Key(ui64(info.Id)).Delete();

    for (const auto& [shardIdx, _] : info.ValidationShards) {
        db.Table<Schema::SetColumnConstraintShardStatus>()
            .Key(info.Id, shardIdx.GetOwnerId(), shardIdx.GetLocalId())
            .Delete();
    }
}

void TSchemeShard::ForgetSetColumnConstraint(NIceDb::TNiceDb& db, const TSetColumnConstraintOperationInfo& info) {
    const auto id = info.Id;
    const auto byTimeKey = std::make_pair(info.StartTime, id);

    PersistSetColumnConstraintForget(db, info);

    SetColumnConstraintOperationsByTime.erase(byTimeKey);
    if (info.Uid) {
        SetColumnConstraintOperationsByUid.erase(info.Uid);
    }

    TxIdToSetColumnConstraintOperations.erase(info.LockTxId);
    TxIdToSetColumnConstraintOperations.erase(info.LockNullWritesTxId);
    TxIdToSetColumnConstraintOperations.erase(info.UnlockNullWritesTxId);
    TxIdToSetColumnConstraintOperations.erase(info.UnlockTxId);

    SetColumnConstraintOperations.erase(id);
}

void TSchemeShard::Handle(TEvSetColumnConstraint::TEvCreateRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCreateSetColumnConstraint(ev), ctx);
}

void TSchemeShard::Handle(TEvSetColumnConstraint::TEvGetRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxGetSetColumnConstraint(ev), ctx);
}

void TSchemeShard::Handle(TEvSetColumnConstraint::TEvListRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxListSetColumnConstraint(ev), ctx);
}

void TSchemeShard::Handle(TEvSetColumnConstraint::TEvCancelRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxSetColumnConstraintCancel(ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvValidateRowConditionResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    TIndexBuildId operationId = TIndexBuildId(record.GetId());
    Execute(CreateTxReplyValidateRowCondition(operationId, ev), ctx);
}

void TSchemeShard::Handle(TEvSetColumnConstraint::TEvForgetRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxForgetSetColumnConstraint(ev), ctx);
}

} // NSchemeShard
} // NKikimr
