#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/tx_processing.h>

namespace NKikimr {
namespace NSchemeShard {

TSet<ui32> AllIncomingEvents();

void IncParentDirAlterVersionWithRepublishSafeWithUndo(const TOperationId& opId, const TPath& path, TSchemeShard* ss, TSideEffects& onComplete);
void IncParentDirAlterVersionWithRepublish(const TOperationId& opId, const TPath& path, TOperationContext& context);

NKikimrSchemeOp::TModifyScheme MoveTableTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst);
NKikimrSchemeOp::TModifyScheme MoveTableIndexTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst);

THolder<TEvHive::TEvCreateTablet> CreateEvCreateTablet(TPathElement::TPtr targetPath, TShardIdx shardIdx, TOperationContext& context);

void AbortUnsafeDropOperation(const TOperationId& operationId, const TTxId& txId, TOperationContext& context);

namespace NTableState {

bool CollectProposeTransactionResults(const TOperationId& operationId, const TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context);
bool CollectProposeTransactionResults(const TOperationId& operationId, const TEvColumnShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context);
bool CollectSchemaChanged(const TOperationId& operationId, const TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context);

void SendSchemaChangedNotificationAck(const TOperationId& operationId, TActorId ackTo, TShardIdx shardIdx, TOperationContext& context);
void AckAllSchemaChanges(const TOperationId& operationId, TTxState& txState, TOperationContext& context);

bool CheckPartitioningChangedForTableModification(TTxState& txState, TOperationContext& context);
void UpdatePartitioningForTableModification(TOperationId txId, TTxState& txState, TOperationContext& context);

TVector<TTableShardInfo> ApplyPartitioningCopyTable(const TShardInfo& templateDatashardInfo, TTableInfo::TPtr srcTableInfo, TTxState& txState, TSchemeShard* ss);

bool SourceTablePartitioningChangedForCopyTable(const TTxState& txState, TOperationContext& context);
void UpdatePartitioningForCopyTable(TOperationId operationId, TTxState& txState, TOperationContext& context);

class TProposedWaitParts: public TSubOperationState {
private:
    TOperationId OperationId;
    const TTxState::ETxState NextState;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NTableState::TProposedWaitParts"
                << " operationId# " << OperationId;
    }

public:
    TProposedWaitParts(TOperationId id, TTxState::ETxState nextState = TTxState::Done)
        : OperationId(id)
        , NextState(nextState)
    {
        IgnoreMessages(DebugHint(),
            { TEvHive::TEvCreateTabletReply::EventType
            , TEvDataShard::TEvProposeTransactionResult::EventType
            , TEvPrivate::TEvOperationPlan::EventType }
        );
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSchemaChanged"
                               << " at tablet: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvSchemaChanged"
                                << " at tablet: " << ssId
                                << " message: " << evRecord.ShortDebugString());

        if (!NTableState::CollectSchemaChanged(OperationId, ev, context)) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " HandleReply TEvSchemaChanged"
                                    << " CollectSchemaChanged: false");
            return false;
        }

        Y_ABORT_UNLESS(context.SS->FindTx(OperationId));
        TTxState& txState = *context.SS->FindTx(OperationId);

        if (!txState.ReadyForNotifications) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " HandleReply TEvSchemaChanged"
                                    << " ReadyForNotifications: false");
            return false;
        }

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " ProgressState"
                     << " at tablet: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);

        NIceDb::TNiceDb db(context.GetDB());

        txState->ClearShardsInProgress();
        for (TTxState::TShardOperation& shard : txState->Shards) {
            if (shard.Operation < TTxState::ProposedWaitParts) {
                shard.Operation = TTxState::ProposedWaitParts;
                context.SS->PersistUpdateTxShard(db, OperationId, shard.Idx, shard.Operation);
            }
            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.Idx));
            context.OnComplete.RouteByTablet(OperationId,  context.SS->ShardInfos.at(shard.Idx).TabletID);
        }
        txState->UpdateShardsInProgress(TTxState::ProposedWaitParts);

        // Move all notifications that were already received
        // NOTE: SchemeChangeNotification is sent form DS after it has got PlanStep from coordinator and the schema tx has completed
        // At that moment the SS might not have received PlanStep from coordinator yet (this message might be still on its way to SS)
        // So we are going to accumulate SchemeChangeNotification that are received before this Tx switches to WaitParts state
        txState->AcceptPendingSchemeNotification();

        // Got notifications from all datashards?
        if (txState->ShardsInProgress.empty()) {
            NTableState::AckAllSchemaChanges(OperationId, *txState, context);
            context.SS->ChangeTxState(db, OperationId, NextState);
            return true;
        }

        return false;
    }
};

} // namespace NTableState

class TCreateParts: public TSubOperationState {
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder() << "TCreateParts"
            << " opId# " << OperationId;
    }

public:
    explicit TCreateParts(const TOperationId& id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvHive::TEvAdoptTabletReply::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvAdoptTablet"
                   << ", at tabletId: " << context.SS->TabletID());
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvAdoptTablet"
                    << ", message% " << DebugReply(ev));

        NIceDb::TNiceDb db(context.GetDB());

        const TString& explain = ev->Get()->Record.GetExplain();
        TTabletId tabletId = TTabletId(ev->Get()->Record.GetTabletID()); // global id from hive
        auto shardIdx = context.SS->MakeLocalId(TLocalShardIdx(ev->Get()->Record.GetOwnerIdx())); // global id from hive
        TTabletId hive = TTabletId(ev->Get()->Record.GetOrigin());

        Y_ABORT_UNLESS(context.SS->TabletID() == ev->Get()->Record.GetOwner());

        NKikimrProto::EReplyStatus status = ev->Get()->Record.GetStatus();
        Y_VERIFY_S(status ==  NKikimrProto::OK || status == NKikimrProto::ALREADY,
                   "Unexpected status " << NKikimrProto::EReplyStatus_Name(status)
                                        << " in AdoptTabletReply for tabletId " << tabletId
                                        << " with explain " << explain);

        // Note that HIVE might send duplicate TTxAdoptTabletReply in case of restarts
        // So we just ignore the event if we cannot find the Tx or if it is in a different
        // state

        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

        if (!context.SS->AdoptedShards.contains(shardIdx)) {
            LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint() << " HandleReply TEvAdoptTablet"
                       << " Got TTxAdoptTabletReply for shard but it is not present in AdoptedShards"
                       << ", shardIdx: " << shardIdx
                       << ", tabletId:" << tabletId);
            return false;
        }

        TShardInfo& shardInfo = context.SS->ShardInfos[shardIdx];
        Y_ABORT_UNLESS(shardInfo.TabletID == InvalidTabletId || shardInfo.TabletID == tabletId);

        Y_ABORT_UNLESS(tabletId != InvalidTabletId);
        shardInfo.TabletID = tabletId;
        context.SS->TabletIdToShardIdx[tabletId] = shardIdx;

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->State == TTxState::CreateParts);

        txState->ShardsInProgress.erase(shardIdx);

        context.SS->AdoptedShards.erase(shardIdx);
        context.SS->PersistDeleteAdopted(db, shardIdx);
        context.SS->PersistShardMapping(db, shardIdx, tabletId, shardInfo.PathId, OperationId.GetTxId(), shardInfo.TabletType);

        context.OnComplete.UnbindMsgFromPipe(OperationId, hive, shardIdx);
        context.OnComplete.ActivateShardCreated(shardIdx, OperationId.GetTxId());

        // If all datashards have been created
        if (txState->ShardsInProgress.empty()) {
            context.SS->ChangeTxState(db, OperationId, TTxState::ConfigureParts);
            return true;
        }

        return false;
    }

    bool HandleReply(TEvHive::TEvCreateTabletReply::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvCreateTabletReply"
                   << ", at tabletId: " << context.SS->TabletID());
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvCreateTabletReply"
                    << ", message: " << DebugReply(ev));

        NIceDb::TNiceDb db(context.GetDB());

        auto shardIdx = TShardIdx(ev->Get()->Record.GetOwner(),
                                  TLocalShardIdx(ev->Get()->Record.GetOwnerIdx()));

        auto tabletId = TTabletId(ev->Get()->Record.GetTabletID()); // global id from hive
        auto hive = TTabletId(ev->Get()->Record.GetOrigin());

        NKikimrProto::EReplyStatus status = ev->Get()->Record.GetStatus();

        Y_VERIFY_S(status ==  NKikimrProto::OK
                       || status == NKikimrProto::ALREADY
                       || status ==  NKikimrProto::INVALID_OWNER
                       || status == NKikimrProto::BLOCKED,
                   "Unexpected status " << NKikimrProto::EReplyStatus_Name(status)
                                        << " in CreateTabletReply shard idx " << shardIdx << " tabletId " << tabletId);

        if (status ==  NKikimrProto::BLOCKED) {
            Y_ABORT_UNLESS(!context.SS->IsDomainSchemeShard);

            LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         DebugHint() << " CreateRequest BLOCKED "
                                     << " at Hive: " << hive
                                     << " msg: " << DebugReply(ev));

            // do not unsubscribe message
            // context.OnComplete.UnbindMsgFromPipe(OperationId, hive, shardIdx);

            // just stay calm and hung until tenant schemeshard is deleted
            return false;
        }

        TTxState& txState = *context.SS->FindTx(OperationId);

        TShardInfo& shardInfo = context.SS->ShardInfos.at(shardIdx);
        Y_ABORT_UNLESS(shardInfo.TabletID == InvalidTabletId || shardInfo.TabletID == tabletId);

        if (status ==  NKikimrProto::INVALID_OWNER) {
            auto redirectTo = TTabletId(ev->Get()->Record.GetForwardRequest().GetHiveTabletId());
            Y_ABORT_UNLESS(redirectTo);
            Y_ABORT_UNLESS(tabletId);

            context.OnComplete.UnbindMsgFromPipe(OperationId, hive, shardIdx);

            auto path = context.SS->PathsById.at(txState.TargetPathId);
            auto request = CreateEvCreateTablet(path, shardIdx, context);

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " CreateRequest"
                                    << " Redirect from Hive: " << hive
                                    << " to Hive: " << redirectTo
                                    << " msg:  " << request->Record.DebugString());

            context.OnComplete.BindMsgToPipe(OperationId, redirectTo, shardIdx, request.Release());
            return false;
        }

        if (shardInfo.TabletID == InvalidTabletId) {
            switch (shardInfo.TabletType) {
            case ETabletType::DataShard:
                context.SS->TabletCounters->Simple()[COUNTER_TABLE_SHARD_ACTIVE_COUNT].Add(1);
                break;
            case ETabletType::Coordinator:
                context.SS->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_COORDINATOR_COUNT].Add(1);
                break;
            case ETabletType::Mediator:
                context.SS->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_MEDIATOR_COUNT].Add(1);
                break;
            case ETabletType::Hive:
                context.SS->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_HIVE_COUNT].Add(1);
                break;
            case ETabletType::SysViewProcessor:
                context.SS->TabletCounters->Simple()[COUNTER_SYS_VIEW_PROCESSOR_COUNT].Add(1);
                break;
            case ETabletType::StatisticsAggregator:
                context.SS->TabletCounters->Simple()[COUNTER_STATISTICS_AGGREGATOR_COUNT].Add(1);
                break;
            case ETabletType::BackupController:
                context.SS->TabletCounters->Simple()[COUNTER_BACKUP_CONTROLLER_TABLET_COUNT].Add(1);
                break;
            default:
                break;
            }
        }

        Y_ABORT_UNLESS(tabletId != InvalidTabletId);
        shardInfo.TabletID = tabletId;
        context.SS->TabletIdToShardIdx[tabletId] = shardIdx;

        Y_ABORT_UNLESS(OperationId.GetTxId() == shardInfo.CurrentTxId);

        txState.ShardsInProgress.erase(shardIdx);

        context.SS->PersistShardMapping(db, shardIdx, tabletId, shardInfo.PathId, OperationId.GetTxId(), shardInfo.TabletType);
        context.OnComplete.UnbindMsgFromPipe(OperationId, hive, shardIdx);
        context.OnComplete.ActivateShardCreated(shardIdx, OperationId.GetTxId());

        // If all datashards have been created
        if (txState.ShardsInProgress.empty()) {
            context.SS->ChangeTxState(db, OperationId, TTxState::ConfigureParts);
            return true;
        }

        return false;
    }

    THolder<TEvHive::TEvAdoptTablet> AdoptRequest(TShardIdx shardIdx, TOperationContext& context) {
        Y_ABORT_UNLESS(context.SS->AdoptedShards.contains(shardIdx));
        auto& adoptedShard = context.SS->AdoptedShards[shardIdx];
        auto& shard = context.SS->ShardInfos[shardIdx];

        THolder<TEvHive::TEvAdoptTablet> ev = MakeHolder<TEvHive::TEvAdoptTablet>(
            ui64(shard.TabletID),
            adoptedShard.PrevOwner, ui64(adoptedShard.PrevShardIdx),
            shard.TabletType,
            context.SS->TabletID(), ui64(shardIdx.GetLocalId()));

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " AdoptRequest"
                    << " Event to Hive: " << ev->Record.DebugString().c_str());

        return ev;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet" << ssId);

        if (txState->TxType == TTxState::TxDropTable
            || txState->TxType == TTxState::TxAlterTable
            || txState->TxType == TTxState::TxBackup
            || txState->TxType == TTxState::TxRestore) {
            if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
                LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           DebugHint() << " ProgressState"
                                       << " SourceTablePartitioningChangedForModification"
                                       << ", tx type: " << TTxState::TypeName(txState->TxType));
                NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
            }
        } else if (txState->TxType == TTxState::TxCopyTable) {
            if (NTableState::SourceTablePartitioningChangedForCopyTable(*txState, context)) {
                LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           DebugHint() << " ProgressState"
                           << " SourceTablePartitioningChangedForCopyTable"
                           << ", tx type: " << TTxState::TypeName(txState->TxType));
                NTableState::UpdatePartitioningForCopyTable(OperationId, *txState, context);
            }
        }

        txState->ClearShardsInProgress();

        bool nothingToDo = true;
        for (const auto& shard : txState->Shards) {
            if (shard.Operation != TTxState::CreateParts) {
                continue;
            }
            nothingToDo = false;

            if (context.SS->AdoptedShards.contains(shard.Idx)) {
                auto ev = AdoptRequest(shard.Idx, context);
                context.OnComplete.BindMsgToPipe(OperationId, context.SS->GetGlobalHive(context.Ctx), shard.Idx, ev.Release());
            } else {
                auto path = context.SS->PathsById.at(txState->TargetPathId);
                auto ev = CreateEvCreateTablet(path, shard.Idx, context);

                auto hiveToRequest = context.SS->ResolveHive(shard.Idx, context.Ctx);

                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            DebugHint() << " CreateRequest"
                                        << " Event to Hive: " << hiveToRequest
                                        << " msg:  "<< ev->Record.DebugString().c_str());

                context.OnComplete.BindMsgToPipe(OperationId, hiveToRequest, shard.Idx, ev.Release());
            }
            context.OnComplete.RouteByShardIdx(OperationId, shard.Idx);
        }

        if (nothingToDo) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " no shards to create, do next state");

            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::ConfigureParts);
            return true;
        }

        txState->UpdateShardsInProgress(TTxState::CreateParts);
        return false;
    }
};

class TDeleteParts: public TSubOperationState {
protected:
    const TOperationId OperationId;
    const TTxState::ETxState NextState;

    TString DebugHint() const override {
        return TStringBuilder() << "TDeleteParts"
            << " opId# " << OperationId << " ";
    }

    void DeleteShards(TOperationContext& context) {
        const auto* txState = context.SS->FindTx(OperationId);

        // Initiate asynchronous deletion of all shards
        for (const auto& shard : txState->Shards) {
            context.OnComplete.DeleteShard(shard.Idx);
        }
    }

public:
    explicit TDeleteParts(const TOperationId& id, TTxState::ETxState nextState = TTxState::Propose)
        : OperationId(id)
        , NextState(nextState)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[" << context.SS->TabletID() << "] " << DebugHint() << "ProgressState");
        DeleteShards(context);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, NextState);
        return true;
    }
};

class TDeletePartsAndDone: public TDeleteParts {
public:
    explicit TDeletePartsAndDone(const TOperationId& id)
        : TDeleteParts(id)
    {
        Y_UNUSED(NextState);
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[" << context.SS->TabletID() << "] " << DebugHint() << "ProgressState");
        DeleteShards(context);

        context.OnComplete.DoneOperation(OperationId);
        return true;
    }
};

class TDone: public TSubOperationState {
protected:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder() << "TDone"
            << " opId# " << OperationId;
    }

public:
    explicit TDone(const TOperationId& id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), AllIncomingEvents());
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[" << context.SS->TabletID() << "] " << DebugHint() << "ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);

        const auto& pathId = txState->TargetPathId;
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        Y_VERIFY_S(path->PathState != TPathElement::EPathState::EPathStateNoChanges, "with context"
            << ", PathState: " << NKikimrSchemeOp::EPathState_Name(path->PathState)
            << ", PathId: " << path->PathId
            << ", PathName: " << path->Name);

        if (path->IsPQGroup() && txState->IsCreate()) {
            TPathElement::TPtr parentDir = context.SS->PathsById.at(path->ParentPathId);
            // can't be removed until KIKIMR-8861
            // at least lets wrap it into condition
            // it helps to actualize PATHSTATE inside children listing
            context.SS->ClearDescribePathCaches(parentDir);
        }

        if (txState->IsDrop()) {
            context.OnComplete.ReleasePathState(OperationId, path->PathId, TPathElement::EPathState::EPathStateNotExist);
        } else {
            context.OnComplete.ReleasePathState(OperationId, path->PathId, TPathElement::EPathState::EPathStateNoChanges);
        }

        if (txState->SourcePathId != InvalidPathId) {
            Y_ABORT_UNLESS(context.SS->PathsById.contains(txState->SourcePathId));
            TPathElement::TPtr srcPath = context.SS->PathsById.at(txState->SourcePathId);
            if (srcPath->PathState == TPathElement::EPathState::EPathStateCopying) {
                context.OnComplete.ReleasePathState(OperationId, srcPath->PathId, TPathElement::EPathState::EPathStateNoChanges);
            }
        }

        // OlapStore tracks all tables that are under operation, make sure to unlink
        if (context.SS->ColumnTables.contains(pathId)) {
            auto tableInfo = context.SS->ColumnTables.at(pathId);
            if (!tableInfo->IsStandalone()) {
                const auto storePathId = tableInfo->GetOlapStorePathIdVerified();
                if (context.SS->OlapStores.contains(storePathId)) {
                    auto storeInfo = context.SS->OlapStores.at(storePathId);
                    storeInfo->ColumnTablesUnderOperation.erase(pathId);
                }
            }
        }

        context.OnComplete.DoneOperation(OperationId);
        return true;
    }
};

namespace NPQState {

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NPQState::TConfigureParts"
                << " operationId#" << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override;

    bool HandleReply(TEvPersQueue::TEvUpdateConfigResponse::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply TEvUpdateConfigResponse"
                     << " at tablet" << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply TEvUpdateConfigResponse"
                     << " message: " << ev->Get()->Record.ShortUtf8DebugString()
                     << " at tablet" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreatePQGroup || txState->TxType == TTxState::TxAlterPQGroup);

        TTabletId tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        NKikimrPQ::EStatus status = ev->Get()->Record.GetStatus();

        // Schemeshard always sends a valid config to the PQ tablet and PQ tablet is not supposed to reject it
        // Also Schemeshard never send multiple different config updates simultaneously (same config
        // can be sent more than once in case of retries due to restarts or disconnects)
        // If PQ tablet is not able to save a valid config it should kill itself and restart without
        // sending error response
        Y_VERIFY_S(status == NKikimrPQ::OK || status == NKikimrPQ::ERROR_UPDATE_IN_PROGRESS,
                   "Unexpected error in UpdateConfigResponse,"
                       << " status: " << NKikimrPQ::EStatus_Name(status)
                       << " Tx " << OperationId
                       << " tablet "<< tabletId
                       << " at schemeshard: " << ssId);

        if (status == NKikimrPQ::ERROR_UPDATE_IN_PROGRESS) {
            LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "PQ reconfiguration is in progress. We'll try to finish it later."
                            << " Tx " << OperationId
                            << " tablet " << tabletId
                            << " at schemeshard: " << ssId);
            return false;
        }

        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        TShardIdx idx = context.SS->MustGetShardIdx(tabletId);
        txState->ShardsInProgress.erase(idx);

        // Detach datashard pipe
        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, idx);

        if (txState->ShardsInProgress.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
            return true;
        }

        return false;
    }


    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint()
                       << " HandleReply ProgressState"
                       << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreatePQGroup || txState->TxType == TTxState::TxAlterPQGroup);

        txState->ClearShardsInProgress();

        TString topicName = context.SS->PathsById.at(txState->TargetPathId)->Name;
        Y_VERIFY_S(topicName.size(),
                   "topicName is empty"
                       <<", pathId: " << txState->TargetPathId);

        TTopicInfo::TPtr pqGroup = context.SS->Topics[txState->TargetPathId];
        Y_VERIFY_S(pqGroup,
                   "pqGroup is null"
                       << ", pathId " << txState->TargetPathId);

        const TPathElement::TPtr dbRootEl = context.SS->PathsById.at(context.SS->RootPathId());
        TString cloudId;
        if (dbRootEl->UserAttrs->Attrs.contains("cloud_id")) {
            cloudId = dbRootEl->UserAttrs->Attrs.at("cloud_id");
        }
        TString folderId;
        if (dbRootEl->UserAttrs->Attrs.contains("folder_id")) {
           folderId = dbRootEl->UserAttrs->Attrs.at("folder_id");
        }
        TString databaseId;
        if (dbRootEl->UserAttrs->Attrs.contains("database_id")) {
            databaseId = dbRootEl->UserAttrs->Attrs.at("database_id");
        }

        TString databasePath = TPath::Init(context.SS->RootPathId(), context.SS).PathString();
        auto topicPath = TPath::Init(txState->TargetPathId, context.SS);

        std::optional<NKikimrPQ::TBootstrapConfig> bootstrapConfig;
        if (txState->TxType == TTxState::TxCreatePQGroup && topicPath.Parent().IsCdcStream()) {
            bootstrapConfig.emplace();

            auto tablePath = topicPath.Parent().Parent(); // table/cdc_stream/topic
            Y_ABORT_UNLESS(tablePath.IsResolved());

            Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
            auto table = context.SS->Tables.at(tablePath.Base()->PathId);

            const auto& partitions = table->GetPartitions();

            for (ui32 i = 0; i < partitions.size(); ++i) {
                const auto& cur = partitions.at(i);

                Y_ABORT_UNLESS(context.SS->ShardInfos.contains(cur.ShardIdx));
                const auto& shard = context.SS->ShardInfos.at(cur.ShardIdx);

                auto& mg = *bootstrapConfig->AddExplicitMessageGroups();
                mg.SetId(NPQ::NSourceIdEncoding::EncodeSimple(ToString(shard.TabletID)));

                if (i != partitions.size() - 1) {
                    mg.MutableKeyRange()->SetToBound(cur.EndOfRange);
                }

                if (i) {
                    const auto& prev = partitions.at(i - 1);
                    mg.MutableKeyRange()->SetFromBound(prev.EndOfRange);
                }
            }
        }

        for (auto shard : txState->Shards) {
            TShardIdx idx = shard.Idx;
            TTabletId tabletId = context.SS->ShardInfos.at(idx).TabletID;

            if (shard.TabletType == ETabletType::PersQueue) {
                TTopicTabletInfo::TPtr pqShard = pqGroup->Shards.at(idx);
                Y_VERIFY_S(pqShard, "pqShard is null, idx is " << idx << " has was "<< THash<TShardIdx>()(idx));

                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Propose configure PersQueue"
                                << ", opId: " << OperationId
                                << ", tabletId: " << tabletId
                                << ", Partitions size: " << pqShard->Partitions.size()
                                << ", at schemeshard: " << ssId);

                THolder<NActors::IEventBase> event;
                if (context.SS->EnablePQConfigTransactionsAtSchemeShard) {
                    event = MakeEvProposeTransaction(OperationId.GetTxId(),
                                                     *pqGroup,
                                                     *pqShard,
                                                     topicName,
                                                     topicPath.PathString(),
                                                     bootstrapConfig,
                                                     cloudId,
                                                     folderId,
                                                     databaseId,
                                                     databasePath,
                                                     txState->TxType,
                                                     context);
                } else {
                    event = MakeEvUpdateConfig(OperationId.GetTxId(),
                                               *pqGroup,
                                               *pqShard,
                                               topicName,
                                               topicPath.PathString(),
                                               bootstrapConfig,
                                               cloudId,
                                               folderId,
                                               databaseId,
                                               databasePath,
                                               txState->TxType,
                                               context);
                }

                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Propose configure PersQueue"
                                << ", opId: " << OperationId
                                << ", tabletId: " << tabletId
                                << ", at schemeshard: " << ssId);

                context.OnComplete.BindMsgToPipe(OperationId, tabletId, idx, event.Release());
            } else {
                Y_ABORT_UNLESS(shard.TabletType == ETabletType::PersQueueReadBalancer);

                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Propose configure PersQueueReadBalancer"
                                << ", opId: " << OperationId
                                << ", tabletId: " << tabletId
                                << ", at schemeshard: " << ssId);

                pqGroup->BalancerTabletID = tabletId;
                if (pqGroup->AlterData) {
                    pqGroup->AlterData->BalancerTabletID = tabletId;
                }

                if (pqGroup->AlterData) {
                    pqGroup->AlterData->BalancerShardIdx = idx;
                }

                TAutoPtr<TEvPersQueue::TEvUpdateBalancerConfig> event(new TEvPersQueue::TEvUpdateBalancerConfig());
                event->Record.SetTxId(ui64(OperationId.GetTxId()));

                ParsePQTabletConfig(*event->Record.MutableTabletConfig(), *pqGroup);

                Y_ABORT_UNLESS(pqGroup->AlterData);

                event->Record.SetTopicName(topicName);
                event->Record.SetPathId(txState->TargetPathId.LocalPathId);
                event->Record.SetPath(TPath::Init(txState->TargetPathId, context.SS).PathString());
                event->Record.SetPartitionPerTablet(pqGroup->AlterData ? pqGroup->AlterData->MaxPartsPerTablet : pqGroup->MaxPartsPerTablet);
                event->Record.SetSchemeShardId(context.SS->TabletID());

                event->Record.SetTotalGroupCount(pqGroup->AlterData ? pqGroup->AlterData->TotalGroupCount : pqGroup->TotalGroupCount);
                event->Record.SetNextPartitionId(pqGroup->AlterData ? pqGroup->AlterData->NextPartitionId : pqGroup->NextPartitionId);

                event->Record.SetVersion(pqGroup->AlterData->AlterVersion);

                for (const auto& p : pqGroup->Shards) {
                    const auto& pqShard = p.second;
                    const auto& tabletId = context.SS->ShardInfos[p.first].TabletID;
                    auto tablet = event->Record.AddTablets();
                    tablet->SetTabletId(ui64(tabletId));
                    tablet->SetOwner(context.SS->TabletID());
                    tablet->SetIdx(ui64(p.first.GetLocalId()));
                    for (const auto& pq : pqShard->Partitions) {
                        auto info = event->Record.AddPartitions();
                        info->SetPartition(pq->PqId);
                        info->SetTabletId(ui64(tabletId));
                        info->SetGroup(pq->GroupId);
                        info->SetCreateVersion(pq->CreateVersion);

                        if (pq->KeyRange) {
                            pq->KeyRange->SerializeToProto(*info->MutableKeyRange());
                        }
                        info->SetStatus(pq->Status);
                        info->MutableParentPartitionIds()->Reserve(pq->ParentPartitionIds.size());
                        for (const auto parent : pq->ParentPartitionIds) {
                            info->MutableParentPartitionIds()->AddAlreadyReserved(parent);
                        }
                        info->MutableChildPartitionIds()->Reserve(pq->ChildPartitionIds.size());
                        for (const auto children : pq->ChildPartitionIds) {
                            info->MutableChildPartitionIds()->AddAlreadyReserved(children);
                        }
                    }
                }

                if (const ui64 subDomainPathId = context.SS->ResolvePathIdForDomain(txState->TargetPathId).LocalPathId) {
                    event->Record.SetSubDomainPathId(subDomainPathId);
                }

                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Propose configure PersQueueReadBalancer"
                                << ", opId: " << OperationId
                                << ", tabletId: " << tabletId
                                << ", message: " << event->Record.ShortUtf8DebugString()
                                << ", at schemeshard: " << ssId);

                context.OnComplete.BindMsgToPipe(OperationId, tabletId, idx, event.Release());
            }
        }

        txState->UpdateShardsInProgress();
        return false;
    }

private:
    static void FillPartition(NKikimrPQ::TPQTabletConfig::TPartition& partition, const TTopicTabletInfo::TTopicPartitionInfo* pq, ui64 tabletId) {
        partition.SetPartitionId(pq->PqId);
        partition.SetCreateVersion(pq->CreateVersion);
        if (pq->KeyRange) {
            pq->KeyRange->SerializeToProto(*partition.MutableKeyRange());
        }
        partition.SetStatus(pq->Status);
        partition.MutableParentPartitionIds()->Reserve(pq->ParentPartitionIds.size());
        for (const auto parent : pq->ParentPartitionIds) {
            partition.MutableParentPartitionIds()->AddAlreadyReserved(parent);
        }
        partition.MutableChildPartitionIds()->Reserve(pq->ChildPartitionIds.size());
        for (const auto children : pq->ChildPartitionIds) {
            partition.MutableChildPartitionIds()->AddAlreadyReserved(children);
        }
        partition.SetTabletId(tabletId);
    }

    static void MakePQTabletConfig(const TOperationContext& context,
                                   NKikimrPQ::TPQTabletConfig& config,
                                   const TTopicInfo& pqGroup,
                                   const TTopicTabletInfo& pqShard,
                                   const TString& topicName,
                                   const TString& topicPath,
                                   const TString& cloudId,
                                   const TString& folderId,
                                   const TString& databaseId,
                                   const TString& databasePath)
    {
        ParsePQTabletConfig(config, pqGroup);

        config.SetTopicName(topicName);
        config.SetTopicPath(topicPath);
        config.MutablePartitionConfig()->SetTotalPartitions(pqGroup.AlterData ? pqGroup.AlterData->TotalGroupCount : pqGroup.TotalGroupCount);

        config.SetYcCloudId(cloudId);
        config.SetYcFolderId(folderId);
        config.SetYdbDatabaseId(databaseId);
        config.SetYdbDatabasePath(databasePath);

        if (pqGroup.AlterData) {
            config.SetVersion(pqGroup.AlterData->AlterVersion);
        }

        for(const auto& pq : pqShard.Partitions) {
            config.AddPartitionIds(pq->PqId);

            auto& partition = *config.AddPartitions();
            FillPartition(partition, pq.Get(), 0);
        }

        for(const auto& p : pqGroup.Shards) {
            const auto& pqShard = p.second;
            const auto& tabletId = context.SS->ShardInfos[p.first].TabletID;
            for (const auto& pq : pqShard->Partitions) {
                auto& partition = *config.AddAllPartitions();
                FillPartition(partition, pq.Get(), ui64(tabletId));
            }
        }
    }

    static void ParsePQTabletConfig(NKikimrPQ::TPQTabletConfig& config,
                                    const TTopicInfo& pqGroup)
    {
        const TString* source = &pqGroup.TabletConfig;
        if (pqGroup.AlterData) {
            if (!pqGroup.AlterData->TabletConfig.empty())
                source = &pqGroup.AlterData->TabletConfig;
        }

        if (!source->empty()) {
            Y_ABORT_UNLESS(ParseFromStringNoSizeLimit(config, *source));
        }
    }

    static THolder<TEvPersQueue::TEvProposeTransaction>
        MakeEvProposeTransaction(TTxId txId,
                                 const TTopicInfo& pqGroup,
                                 const TTopicTabletInfo& pqShard,
                                 const TString& topicName,
                                 const TString& topicPath,
                                 const std::optional<NKikimrPQ::TBootstrapConfig>& bootstrapConfig,
                                 const TString& cloudId,
                                 const TString& folderId,
                                 const TString& databaseId,
                                 const TString& databasePath,
                                 TTxState::ETxType txType,
                                 const TOperationContext& context);
    static THolder<TEvPersQueue::TEvUpdateConfig>
        MakeEvUpdateConfig(TTxId txId,
                           const TTopicInfo& pqGroup,
                           const TTopicTabletInfo& pqShard,
                           const TString& topicName,
                           const TString& topicPath,
                           const std::optional<NKikimrPQ::TBootstrapConfig>& bootstrapConfig,
                           const TString& cloudId,
                           const TString& folderId,
                           const TString& databaseId,
                           const TString& databasePath,
                           TTxState::ETxType txType,
                           const TOperationContext& context);
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NPQState::TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvPersQueue::TEvUpdateConfigResponse::EventType});
    }

    bool HandleReply(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override;
    bool HandleReply(TEvPersQueue::TEvProposeTransactionAttachResult::TPtr& ev, TOperationContext& context) override;

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint()
                       << " HandleReply TEvOperationPlan"
                       << ", step: " << step
                       << ", at tablet: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreatePQGroup || txState->TxType == TTxState::TxAlterPQGroup);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        if (path->StepCreated == InvalidStepId) {
            path->StepCreated = step;
            context.SS->PersistCreateStep(db, pathId, step);
        }

        return TryPersistState(context);
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "NPQState::TPropose ProgressState"
                       << ", operationId: " << OperationId
                       << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreatePQGroup || txState->TxType == TTxState::TxAlterPQGroup);

        //
        // If the program works according to the new scheme, then we must add PQ tablets to the list for
        // the Coordinator. At this stage, we cannot rely on the value of
        // the EnablePQConfigTransactionsAtSchemeShard flag. Because the operation could have started on one tablet
        // and moved to another by that time.
        //
        // Therefore, here we check the value of the minStep field, which is filled in in
        // the TEvProposeTransactionResult handler
        //
        TSet<TTabletId> shardSet;
        if (ui64(txState->MinStep) > 0) {
            PrepareShards(*txState, shardSet, context);
        }
        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, shardSet);

        return false;
    }

private:
    bool CanPersistState(const TTxState& txState,
                         TOperationContext& context);
    void PersistState(const TTxState& txState,
                      TOperationContext& context) const;
    bool TryPersistState(TOperationContext& context);
    void SendEvProposeTransactionAttach(TShardIdx shard, TTabletId tablet,
                                        TOperationContext& context);

    void PrepareShards(TTxState& txState, TSet<TTabletId>& shardSet, TOperationContext& context);

    TPathId PathId;
    TPathElement::TPtr Path;
};

} // NPQState

namespace NBSVState {

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "NBSVState::TConfigureParts"
            << " operationId: " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSetConfigResult"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateBlockStoreVolume || txState->TxType == TTxState::TxAlterBlockStoreVolume);
        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        TTabletId tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        NKikimrBlockStore::EStatus status = ev->Get()->Record.GetStatus();

        // Schemeshard never sends invalid or outdated configs
        Y_VERIFY_S(status == NKikimrBlockStore::OK || status == NKikimrBlockStore::ERROR_UPDATE_IN_PROGRESS,
                   "Unexpected error in UpdateVolumeConfigResponse,"
                       << " status " << NKikimrBlockStore::EStatus_Name(status)
                       << " Tx " << OperationId
                       << " tablet " << tabletId);

        if (status == NKikimrBlockStore::ERROR_UPDATE_IN_PROGRESS) {
            LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "BlockStore reconfiguration is in progress. We'll try to finish it later."
                            << " Tx " << OperationId
                            << " tablet " << tabletId);
            return false;
        }

        TShardIdx idx = context.SS->MustGetShardIdx(tabletId);
        txState->ShardsInProgress.erase(idx);

        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, idx);

        if (txState->ShardsInProgress.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateBlockStoreVolume || txState->TxType == TTxState::TxAlterBlockStoreVolume);
        Y_ABORT_UNLESS(!txState->Shards.empty());

        txState->ClearShardsInProgress();

        TBlockStoreVolumeInfo::TPtr volume = context.SS->BlockStoreVolumes[txState->TargetPathId];
        Y_VERIFY_S(volume, "volume is null. PathId: " << txState->TargetPathId);

        ui64 version = volume->AlterVersion;
        const auto* volumeConfig = &volume->VolumeConfig;
        if (volume->AlterData) {
            version = volume->AlterData->AlterVersion;
            volumeConfig = &volume->AlterData->VolumeConfig;
        }

        for (auto shard : txState->Shards) {
            if (shard.TabletType == ETabletType::BlockStorePartition ||
                shard.TabletType == ETabletType::BlockStorePartition2) {
                continue;
            }

            Y_ABORT_UNLESS(shard.TabletType == ETabletType::BlockStoreVolume);
            TShardIdx shardIdx = shard.Idx;
            TTabletId tabletId = context.SS->ShardInfos[shardIdx].TabletID;

            volume->VolumeTabletId = tabletId;
            if (volume->AlterData) {
                volume->AlterData->VolumeTabletId = tabletId;
                volume->AlterData->VolumeShardIdx = shardIdx;
            }

            TAutoPtr<TEvBlockStore::TEvUpdateVolumeConfig> event(new TEvBlockStore::TEvUpdateVolumeConfig());
            event->Record.SetTxId(ui64(OperationId.GetTxId()));

            event->Record.MutableVolumeConfig()->CopyFrom(*volumeConfig);
            event->Record.MutableVolumeConfig()->SetVersion(version);

            for (const auto& p : volume->Shards) {
                const auto& part = p.second;
                const auto& partTabletId = context.SS->ShardInfos[p.first].TabletID;
                auto info = event->Record.AddPartitions();
                info->SetPartitionId(part->PartitionId);
                info->SetTabletId(ui64(partTabletId));
            }

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, shardIdx, event.Release());

            // Wait for results from this shard
            txState->ShardsInProgress.insert(shardIdx);
        }

        return false;
    }
};


class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NBSVState::TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvBlockStore::TEvUpdateVolumeConfigResponse::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        if (!txState) {
            return false;
        }
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateBlockStoreVolume || txState->TxType == TTxState::TxAlterBlockStoreVolume);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        if (path->StepCreated == InvalidStepId) {
            path->StepCreated = step;
            context.SS->PersistCreateStep(db, pathId, step);
        }

        TBlockStoreVolumeInfo::TPtr volume = context.SS->BlockStoreVolumes.at(pathId);

        auto oldVolumeSpace = volume->GetVolumeSpace();
        volume->FinishAlter();
        auto newVolumeSpace = volume->GetVolumeSpace();
        // Decrease in occupied space is applied on tx finish
        auto domainDir = context.SS->PathsById.at(context.SS->ResolvePathIdForDomain(path));
        Y_ABORT_UNLESS(domainDir);
        domainDir->ChangeVolumeSpaceCommit(newVolumeSpace, oldVolumeSpace);

        context.SS->PersistBlockStoreVolume(db, pathId, volume);
        context.SS->PersistRemoveBlockStoreVolumeAlter(db, pathId);

        if (txState->TxType == TTxState::TxCreateBlockStoreVolume) {
            auto parentDir = context.SS->PathsById.at(path->ParentPathId);
            ++parentDir->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, parentDir);
            context.SS->ClearDescribePathCaches(parentDir);
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
        }

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateBlockStoreVolume || txState->TxType == TTxState::TxAlterBlockStoreVolume);


        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

} // NBSVState

namespace NCdcStreamState {

class TConfigurePartsAtTable: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "NCdcStreamState::TConfigurePartsAtTable"
            << " operationId: " << OperationId;
    }

    static bool IsExpectedTxType(TTxState::ETxType txType) {
        switch (txType) {
        case TTxState::TxCreateCdcStreamAtTable:
        case TTxState::TxCreateCdcStreamAtTableWithInitialScan:
        case TTxState::TxAlterCdcStreamAtTable:
        case TTxState::TxAlterCdcStreamAtTableDropSnapshot:
        case TTxState::TxDropCdcStreamAtTable:
        case TTxState::TxDropCdcStreamAtTableDropSnapshot:
            return true;
        default:
            return false;
        }
    }

protected:
    virtual void FillNotice(const TPathId& pathId, NKikimrTxDataShard::TFlatSchemeTransaction& tx, TOperationContext& context) const = 0;

public:
    explicit TConfigurePartsAtTable(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->TabletID());

        auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(IsExpectedTxType(txState->TxType));
        const auto& pathId = txState->TargetPathId;

        if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
            NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
        }

        NKikimrTxDataShard::TFlatSchemeTransaction tx;
        context.SS->FillSeqNo(tx, context.SS->StartRound(*txState));
        FillNotice(pathId, tx, context);

        txState->ClearShardsInProgress();
        Y_ABORT_UNLESS(txState->Shards.size());

        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            const auto& idx = txState->Shards[i].Idx;
            const auto datashardId = context.SS->ShardInfos[idx].TabletID;
            auto ev = context.SS->MakeDataShardProposal(pathId, OperationId, tx.SerializeAsString(), context.Ctx);
            context.OnComplete.BindMsgToPipe(OperationId, datashardId, idx, ev.Release());
        }

        txState->UpdateShardsInProgress(TTxState::ConfigureParts);
        return false;
    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply " << ev->Get()->ToString()
                               << ", at schemeshard: " << context.SS->TabletID());

        if (!NTableState::CollectProposeTransactionResults(OperationId, ev, context)) {
            return false;
        }

        return true;
    }

private:
    const TOperationId OperationId;

}; // TConfigurePartsAtTable

class TProposeAtTable: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "NCdcStreamState::TProposeAtTable"
            << " operationId: " << OperationId;
    }

    static bool IsExpectedTxType(TTxState::ETxType txType) {
        switch (txType) {
        case TTxState::TxCreateCdcStreamAtTable:
        case TTxState::TxCreateCdcStreamAtTableWithInitialScan:
        case TTxState::TxAlterCdcStreamAtTable:
        case TTxState::TxAlterCdcStreamAtTableDropSnapshot:
        case TTxState::TxDropCdcStreamAtTable:
        case TTxState::TxDropCdcStreamAtTableDropSnapshot:
            return true;
        default:
            return false;
        }
    }

public:
    explicit TProposeAtTable(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvDataShard::TEvProposeTransactionResult::EventType});
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->TabletID());

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(IsExpectedTxType(txState->TxType));

        TSet<TTabletId> shardSet;
        for (const auto& shard : txState->Shards) {
            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.Idx));
            shardSet.insert(context.SS->ShardInfos.at(shard.Idx).TabletID);
        }

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, shardSet);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << ev->Get()->StepId
                               << ", at schemeshard: " << context.SS->TabletID());

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(IsExpectedTxType(txState->TxType));
        const auto& pathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);

        table->AlterVersion += 1;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistTableAlterVersion(db, pathId, table);

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " TEvDataShard::TEvSchemaChanged"
                               << " triggers early, save it"
                               << ", at schemeshard: " << context.SS->TabletID());

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

protected:
    const TOperationId OperationId;

}; // TProposeAtTable

class TProposeAtTableDropSnapshot: public TProposeAtTable {
public:
    using TProposeAtTable::TProposeAtTable;

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TProposeAtTable::HandleReply(ev, context);

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        const auto& pathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->TablesWithSnapshots.contains(pathId));
        const auto snapshotTxId = context.SS->TablesWithSnapshots.at(pathId);

        auto it = context.SS->SnapshotTables.find(snapshotTxId);
        if (it != context.SS->SnapshotTables.end()) {
            it->second.erase(pathId);
            if (it->second.empty()) {
                context.SS->SnapshotTables.erase(it);
            }
        }

        context.SS->SnapshotsStepIds.erase(snapshotTxId);
        context.SS->TablesWithSnapshots.erase(pathId);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistDropSnapshot(db, snapshotTxId, pathId);

        context.SS->TabletCounters->Simple()[COUNTER_SNAPSHOTS_COUNT].Sub(1);
        return true;
    }

}; // TProposeAtTableDropSnapshot

} // NCdcStreamState

namespace NForceDrop {
void ValidateNoTransactionOnPaths(TOperationId operationId, const THashSet<TPathId>& paths, TOperationContext& context);

void CollectShards(const THashSet<TPathId>& paths, TOperationId operationId, TTxState* txState, TOperationContext& context);
} // namespace NForceDrop

} // namespace NSchemeShard
} // namespace NKikimr
