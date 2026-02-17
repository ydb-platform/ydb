#include "schemeshard__operation_common.h"

#include "schemeshard__shred_manager.h"

#include <ydb/core/blob_depot/events.h>
#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/filestore/core/filestore.h>
#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/replication/controller/public_events.h>
#include <ydb/core/tx/sequenceshard/public/events.h>


namespace NKikimr {
namespace NSchemeShard {

THolder<TEvHive::TEvCreateTablet> CreateEvCreateTablet(TPathElement::TPtr targetPath, TShardIdx shardIdx, TOperationContext& context)
{
    auto tablePartitionConfig = context.SS->GetTablePartitionConfigWithAlterData(targetPath->PathId);
    const auto& shard = context.SS->ShardInfos[shardIdx];

    if (shard.TabletType == ETabletType::BlockStorePartition ||
        shard.TabletType == ETabletType::BlockStorePartition2 ||
        shard.TabletType == ETabletType::BlockStorePartitionDirect)
    {
        auto it = context.SS->BlockStoreVolumes.FindPtr(targetPath->PathId);
        Y_ABORT_UNLESS(it, "Missing BlockStoreVolume while creating BlockStorePartition tablet");
        auto volume = *it;
        /*const auto* volumeConfig = &volume->VolumeConfig;
        if (volume->AlterData) {
            volumeConfig = &volume->AlterData->VolumeConfig;
        }*/
    }

    THolder<TEvHive::TEvCreateTablet> ev = MakeHolder<TEvHive::TEvCreateTablet>(ui64(shardIdx.GetOwnerId()), ui64(shardIdx.GetLocalId()), shard.TabletType, shard.BindedChannels);

    TPathId domainId = context.SS->ResolvePathIdForDomain(targetPath);

    TPathElement::TPtr domainEl = context.SS->PathsById.at(domainId);
    auto objectDomain = ev->Record.MutableObjectDomain();
    if (domainEl->IsRoot()) {
        objectDomain->SetSchemeShard(context.SS->ParentDomainId.OwnerId);
        objectDomain->SetPathId(context.SS->ParentDomainId.LocalPathId);
    } else {
        objectDomain->SetSchemeShard(domainId.OwnerId);
        objectDomain->SetPathId(domainId.LocalPathId);
    }

    Y_ABORT_UNLESS(context.SS->SubDomains.contains(domainId));
    TSubDomainInfo::TPtr subDomain = context.SS->SubDomains.at(domainId);

    TPathId resourcesDomainId;
    if (subDomain->GetResourcesDomainId()) {
        resourcesDomainId = subDomain->GetResourcesDomainId();
    } else if (subDomain->GetAlter() && subDomain->GetAlter()->GetResourcesDomainId()) {
        resourcesDomainId = subDomain->GetAlter()->GetResourcesDomainId();
    } else {
        Y_ABORT("Cannot retrieve resources domain id");
    }

    auto allowedDomain = ev->Record.AddAllowedDomains();
    allowedDomain->SetSchemeShard(resourcesDomainId.OwnerId);
    allowedDomain->SetPathId(resourcesDomainId.LocalPathId);

    if (tablePartitionConfig) {
        if (tablePartitionConfig->FollowerGroupsSize()) {
            ev->Record.MutableFollowerGroups()->CopyFrom(tablePartitionConfig->GetFollowerGroups());
        } else {
            if (tablePartitionConfig->HasAllowFollowerPromotion()) {
                ev->Record.SetAllowFollowerPromotion(tablePartitionConfig->GetAllowFollowerPromotion());
            }

            if (tablePartitionConfig->HasCrossDataCenterFollowerCount()) {
                ev->Record.SetCrossDataCenterFollowerCount(tablePartitionConfig->GetCrossDataCenterFollowerCount());
            } else if (tablePartitionConfig->HasFollowerCount()) {
                ev->Record.SetFollowerCount(tablePartitionConfig->GetFollowerCount());
            }
        }
    }

    if (shard.TabletType == ETabletType::BlockStorePartition   ||
        shard.TabletType == ETabletType::BlockStorePartition2 ||
        shard.TabletType == ETabletType::RTMRPartition) {
        // These partitions should never be booted by local.
        // BlockStorePartitionDirect is intentionally omitted and may be booted local for now.
        ev->Record.SetTabletBootMode(NKikimrHive::TABLET_BOOT_MODE_EXTERNAL);
    }

    ev->Record.SetObjectId(targetPath->PathId.LocalPathId);

    if (shard.TabletID) {
        ev->Record.SetTabletID(ui64(shard.TabletID));
    }

    return ev;
}

// TCreateParts
//
TCreateParts::TCreateParts(const TOperationId& id)
    : OperationId(id)
{
    IgnoreMessages(DebugHint(), {});
}

bool TCreateParts::HandleReply(TEvHive::TEvAdoptTabletReply::TPtr& ev, TOperationContext& context) {
    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " HandleReply TEvAdoptTablet"
                << ", at tabletId: " << context.SS->SelfTabletId());
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " HandleReply TEvAdoptTablet"
                << ", message% " << DebugReply(ev));

    NIceDb::TNiceDb db(context.GetDB());

    const TString& explain = ev->Get()->Record.GetExplain();
    TTabletId tabletId = TTabletId(ev->Get()->Record.GetTabletID()); // global id from hive
    auto shardIdx = context.SS->MakeLocalId(TLocalShardIdx(ev->Get()->Record.GetOwnerIdx())); // global id from hive
    TTabletId hive = TTabletId(ev->Get()->Record.GetOrigin());

    Y_ABORT_UNLESS(ui64(context.SS->SelfTabletId()) == ev->Get()->Record.GetOwner());

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

bool TCreateParts::HandleReply(TEvHive::TEvCreateTabletReply::TPtr& ev, TOperationContext& context) {
    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " HandleReply TEvCreateTabletReply"
                << ", at tabletId: " << context.SS->SelfTabletId());
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

THolder<TEvHive::TEvAdoptTablet> TCreateParts::AdoptRequest(TShardIdx shardIdx, TOperationContext& context) {
    Y_ABORT_UNLESS(context.SS->AdoptedShards.contains(shardIdx));
    auto& adoptedShard = context.SS->AdoptedShards[shardIdx];
    auto& shard = context.SS->ShardInfos[shardIdx];

    THolder<TEvHive::TEvAdoptTablet> ev = MakeHolder<TEvHive::TEvAdoptTablet>(
        ui64(shard.TabletID),
        adoptedShard.PrevOwner, ui64(adoptedShard.PrevShardIdx),
        shard.TabletType,
        ui64(context.SS->SelfTabletId()), ui64(shardIdx.GetLocalId()));

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " AdoptRequest"
                << " Event to Hive: " << ev->Record.DebugString().c_str());

    return ev;
}

bool TCreateParts::ProgressState(TOperationContext& context) {
    TTabletId ssId = context.SS->SelfTabletId();

    TTxState* txState = context.SS->FindTx(OperationId);
    Y_ABORT_UNLESS(txState);

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " ProgressState"
                            << ", operation type: " << TTxState::TypeName(txState->TxType)
                            << ", at tablet# " << ssId);

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
            context.OnComplete.BindMsgToPipe(OperationId, context.SS->GetGlobalHive(), shard.Idx, ev.Release());
        } else {
            auto path = context.SS->PathsById.at(txState->TargetPathId);
            auto ev = CreateEvCreateTablet(path, shard.Idx, context);

            auto hiveToRequest = context.SS->ResolveHive(shard.Idx);

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

// TDeleteParts
//
TDeleteParts::TDeleteParts(const TOperationId& id, TTxState::ETxState nextState)
    : OperationId(id)
    , NextState(nextState)
{
    IgnoreMessages(DebugHint(), {});
}

void TDeleteParts::DeleteShards(TOperationContext& context) {
    const auto* txState = context.SS->FindTx(OperationId);

    // Initiate asynchronous deletion of all shards
    for (const auto& shard : txState->Shards) {
        context.OnComplete.DeleteShard(shard.Idx);
    }
}

bool TDeleteParts::ProgressState(TOperationContext& context) {
    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[" << context.SS->SelfTabletId() << "] " << DebugHint() << " ProgressState");
    DeleteShards(context);

    NIceDb::TNiceDb db(context.GetDB());
    context.SS->ChangeTxState(db, OperationId, NextState);
    return true;
}

// TDeletePartsAndDone
//
TDeletePartsAndDone::TDeletePartsAndDone(const TOperationId& id)
    : TDeleteParts(id)
{
    Y_UNUSED(NextState);
}

bool TDeletePartsAndDone::ProgressState(TOperationContext& context) {
    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[" << context.SS->SelfTabletId() << "] " << DebugHint() << " ProgressState");
    DeleteShards(context);

    context.OnComplete.DoneOperation(OperationId);
    return true;
}

// TDone
//
TDone::TDone(const TOperationId& id)
    : OperationId(id)
{
    IgnoreMessages(DebugHint(), AllIncomingEvents());
}

TDone::TDone(const TOperationId& id, TPathElement::EPathState targetState)
    : OperationId(id)
    , TargetState(targetState)
{
    IgnoreMessages(DebugHint(), AllIncomingEvents());
}

bool TDone::Process(TOperationContext& context) {
    const auto* txState = context.SS->FindTx(OperationId);

    const auto& pathId = txState->TargetPathId;
    Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
    TPathElement::TPtr path = context.SS->PathsById.at(pathId);
    Y_VERIFY_S(TargetState || path->PathState != TPathElement::EPathState::EPathStateNoChanges, "with context"
        << ", PathState: " << NKikimrSchemeOp::EPathState_Name(path->PathState)
        << ", PathId: " << path->PathId
        << ", TargetState: " << (TargetState ? NKikimrSchemeOp::EPathState_Name(*TargetState) : "null")
        << ", OperationId: " << OperationId);

    if (path->IsPQGroup() && txState->IsCreate()) {
        TPathElement::TPtr parentDir = context.SS->PathsById.at(path->ParentPathId);
        // can't be removed until KIKIMR-8861
        // at least lets wrap it into condition
        // it helps to actualize PATHSTATE inside children listing
        context.SS->ClearDescribePathCaches(parentDir);
    }

    if (txState->IsDrop()) {
        context.OnComplete.ReleasePathState(OperationId, path->PathId, TPathElement::EPathState::EPathStateNotExist);
    } else if (TargetState) {
        context.OnComplete.ReleasePathState(OperationId, path->PathId, *TargetState);
    } else {
        context.OnComplete.ReleasePathState(OperationId, path->PathId, TPathElement::EPathState::EPathStateNoChanges);
    }

    if (txState->SourcePathId != InvalidPathId) {
        Y_ABORT_UNLESS(context.SS->PathsById.contains(txState->SourcePathId));
        TPathElement::TPtr srcPath = context.SS->PathsById.at(txState->SourcePathId);
        if (srcPath->PathState == TPathElement::EPathState::EPathStateCopying) {
            context.OnComplete.ReleasePathState(OperationId, srcPath->PathId, TPathElement::EPathState::EPathStateNoChanges);
        }
        if (txState->TxType == TTxState::TxRotateCdcStream) {
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

bool TDone::ProgressState(TOperationContext& context) {
    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[" << context.SS->SelfTabletId() << "] " << DebugHint() << " ProgressState");

    return Process(context);
}

namespace {

template <typename T, typename TFuncCheck, typename TFuncToString>
bool CollectProposeTxResults(
        const T& ev,
        const NKikimr::NSchemeShard::TOperationId& operationId,
        NKikimr::NSchemeShard::TOperationContext& context,
        TFuncCheck checkPrepared,
        TFuncToString toString)
{
    auto ssId = context.SS->SelfTabletId();

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TEvProposeTransactionResult at tablet: " << ssId);

    auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
    auto shardMinStep = TStepId(ev->Get()->Record.GetMinStep());
    auto status = ev->Get()->Record.GetStatus();

    // Ignore COMPLETE
    if (!checkPrepared(status)) {
        LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Ignore TEvProposeTransactionResult as not prepared"
                        << ", shard: " << tabletId
                        << ", operationId: " << operationId
                        << ", result status: " << toString(status)
                        << ", at schemeshard: " << ssId);
        return false;
    }

    NIceDb::TNiceDb db(context.GetDB());

    TTxState& txState = *context.SS->FindTx(operationId);

    if (txState.MinStep < shardMinStep) {
        txState.MinStep = shardMinStep;
        context.SS->PersistTxMinStep(db, operationId, txState.MinStep);
    }

    auto shardIdx = context.SS->MustGetShardIdx(tabletId);

    // Ignore if this is a repeated message
    if (!txState.ShardsInProgress.contains(shardIdx)) {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Ignore TEvProposeTransactionResult as duplicate"
                        << ", shard: " << tabletId
                        << ", shardIdx: " << shardIdx
                        << ", operationId: " << operationId
                        << ", at schemeshard: " << ssId);
        return false;
    }

    txState.ShardsInProgress.erase(shardIdx);
    context.OnComplete.UnbindMsgFromPipe(operationId, tabletId, shardIdx);

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "CollectProposeTransactionResults accept TEvProposeTransactionResult"
                    << ", shard: " << tabletId
                    << ", shardIdx: " << shardIdx
                    << ", operationId: " << operationId
                    << ", left await: " << txState.ShardsInProgress.size()
                    << ", at schemeshard: " << ssId);

    if (txState.ShardsInProgress.empty()) {
        // All datashards have replied so we can proceed with this transaction
        context.SS->ChangeTxState(db, operationId, TTxState::Propose);
        return true;
    }

    return false;
}

} // anonymous namespace

namespace NTableState {

bool CollectProposeTransactionResults(
        const NKikimr::NSchemeShard::TOperationId &operationId,
        const TEvDataShard::TEvProposeTransactionResult::TPtr &ev,
        NKikimr::NSchemeShard::TOperationContext &context)
{
    auto prepared = [](NKikimrTxDataShard::TEvProposeTransactionResult::EStatus status) -> bool {
        return status == NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED;
    };

    auto toString = [](NKikimrTxDataShard::TEvProposeTransactionResult::EStatus status) -> TString {
        return NKikimrTxDataShard::TEvProposeTransactionResult_EStatus_Name(status);
    };

    return CollectProposeTxResults(ev, operationId, context, prepared, toString);
}

bool CollectProposeTransactionResults(
        const NKikimr::NSchemeShard::TOperationId& operationId,
        const TEvColumnShard::TEvProposeTransactionResult::TPtr& ev,
        NKikimr::NSchemeShard::TOperationContext& context)
{
    auto prepared = [](NKikimrTxColumnShard::EResultStatus status) -> bool {
        return status == NKikimrTxColumnShard::EResultStatus::PREPARED;
    };

    auto toString = [](NKikimrTxColumnShard::EResultStatus status) -> TString {
        return NKikimrTxColumnShard::EResultStatus_Name(status);
    };

    return CollectProposeTxResults(ev, operationId, context, prepared, toString);
}

namespace {

template<typename TEvent>
bool CollectSchemaChangedImpl(
        const TOperationId& operationId,
        const TEvent& ev,
        TOperationContext& context)
{
    auto ssId = context.SS->SelfTabletId();

    const auto& evRecord = ev->Get()->Record;
    const TActorId ackTo = TEvSchemaChangedTraits<TEvent>::GetSource(ev);

    auto shardId = TTabletId(evRecord.GetOrigin());

    Y_ABORT_UNLESS(context.SS->FindTx(operationId));
    TTxState& txState = *context.SS->FindTx(operationId);

    auto shardIdx = context.SS->MustGetShardIdx(shardId);
    Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

    // Save this notification if was received earlier than the Tx switched to ProposedWaitParts state
    const auto& generation = TEvSchemaChangedTraits<TEvent>::GetGeneration(ev);
    auto pTablet = txState.SchemeChangeNotificationReceived.FindPtr(shardIdx);
    if (pTablet && generation && (pTablet->second >= *generation)) {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "CollectSchemaChanged Ignore " << TEvSchemaChangedTraits<TEvent>::GetName() << " as outdated"
                        << ", operationId: " << operationId
                        << ", shardIdx: " << shardIdx
                        << ", shard " << shardId
                        << ", event generation: " << generation
                        << ", known generation: " << pTablet->second
                        << ", at schemeshard: " << ssId);
        return false;
    }

    txState.SchemeChangeNotificationReceived[shardIdx] = std::make_pair(ackTo, generation ? *generation : 0);


    if (TEvSchemaChangedTraits<TEvent>::HasOpResult(ev)) {
        // TODO: remove TxBackup handling
        Y_DEBUG_ABORT_UNLESS(txState.TxType == TTxState::TxBackup || txState.TxType == TTxState::TxRestore);
    }

    if (!txState.ReadyForNotifications) {
        return false;
    }
    if (txState.TxType == TTxState::TxBackup || txState.TxType == TTxState::TxRestore) {
        Y_ABORT_UNLESS(txState.State == TTxState::ProposedWaitParts || txState.State == TTxState::Aborting);
    } else {
        Y_ABORT_UNLESS(txState.State == TTxState::ProposedWaitParts);
    }

    txState.ShardsInProgress.erase(shardIdx);

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "CollectSchemaChanged accept " << TEvSchemaChangedTraits<TEvent>::GetName()
                    << ", operationId: " << operationId
                    << ", shardIdx: " << shardIdx
                    << ", shard: " << shardId
                    << ", left await: " << txState.ShardsInProgress.size()
                    << ", txState.State: " << TTxState::StateName(txState.State)
                    << ", txState.ReadyForNotifications: " << txState.ReadyForNotifications
                    << ", at schemeshard: " << ssId);

    if (txState.ShardsInProgress.empty()) {
        AckAllSchemaChanges(operationId, txState, context);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, operationId, TTxState::Done);
        return true;
    }

    return false;
}

} //namespace

bool CollectSchemaChanged(
        const TOperationId& operationId,
        const TEvDataShard::TEvSchemaChanged::TPtr& ev,
        TOperationContext& context)
{
    return CollectSchemaChangedImpl<>(operationId, ev, context);
}

bool CollectSchemaChanged(
        const TOperationId& operationId,
        const TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev,
        TOperationContext& context)
{
    return CollectSchemaChangedImpl(operationId, ev, context);
}

void AckAllSchemaChanges(const TOperationId &operationId, TTxState &txState, TOperationContext &context) {
    TTabletId ssId = context.SS->SelfTabletId();

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "all shard schema changes has been received"
                    << ", operationId: " << operationId
                    << ", at schemeshard: " << ssId);

    // Ack to all participating datashards
    for (const auto& items : txState.SchemeChangeNotificationReceived) {
        const TActorId ackTo = items.second.first;
        const auto shardIdx = items.first;
        const auto tabletId = context.SS->ShardInfos[shardIdx].TabletID;

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "send schema changes ack message"
                        << ", operation: " << operationId
                        << ", datashard: " << tabletId
                        << ", at schemeshard: " << ssId);

        auto event = MakeHolder<TEvDataShard::TEvSchemaChangedResult>();
        event->Record.SetTxId(ui64(operationId.GetTxId()));

        context.OnComplete.Send(ackTo, std::move(event), ui64(shardIdx.GetLocalId()));
    }
}

bool CheckPartitioningChangedForTableModificationImpl(TTxState &txState, TOperationContext &context) {
    Y_ABORT_UNLESS(context.SS->Tables.contains(txState.TargetPathId));
    TTableInfo::TPtr table = context.SS->Tables.at(txState.TargetPathId);

    THashSet<TShardIdx> shardIdxsLeft;
    for (auto& shard : table->GetPartitions()) {
        shardIdxsLeft.insert(shard.ShardIdx);
    }

    for (auto& shardOp : txState.Shards) {
        // Is this shard still on the list of partitions?
        if (shardIdxsLeft.erase(shardOp.Idx) == 0)
            return true;
    }

    // Any new partitions?
    return !shardIdxsLeft.empty();
}

bool CheckPartitioningChangedForColumnTableModificationImpl(TTxState &txState, TOperationContext &context) {
    Y_ABORT_UNLESS(context.SS->ColumnTables.contains(txState.TargetPathId));
    auto table = context.SS->ColumnTables.at(txState.TargetPathId);

    THashSet<TShardIdx> shardIdxsLeft;
    for (auto& shard : table->GetOwnedColumnShardsVerified()) {
        TShardIdx shardIdx = TShardIdx::BuildFromProto(shard).DetachResult();
        shardIdxsLeft.insert(shardIdx);
    }

    for (auto& shardOp : txState.Shards) {
        // Is this shard still on the list of partitions?
        if (shardIdxsLeft.erase(shardOp.Idx) == 0)
            return true;
    }

    // Any new partitions?
    return !shardIdxsLeft.empty();
}

bool CheckPartitioningChangedForTableModification(TTxState &txState, TOperationContext &context) {
    TPath dstPath = TPath::Init(txState.TargetPathId, context.SS);
    if (dstPath->IsColumnTable()) {
        return CheckPartitioningChangedForColumnTableModificationImpl(txState, context);
    }
    return CheckPartitioningChangedForTableModificationImpl(txState, context);
}

void UpdatePartitioningForTableModification(TOperationId operationId, TTxState &txState, TOperationContext &context) {
    Y_ABORT_UNLESS(!txState.TxShardsListFinalized, "Rebuilding the list of shards must not happen twice");

    NIceDb::TNiceDb db(context.GetDB());

    THashSet<TShardIdx> prevAlterCreateParts;

    // Delete old tx shards from db
    for (const auto& shard : txState.Shards) {
        if (txState.TxType == TTxState::TxAlterTable && shard.Operation == TTxState::CreateParts) {
            // Remember alter table parts that had CreateParts set (possible channel bindings change)
            prevAlterCreateParts.insert(shard.Idx);
        }
        context.SS->PersistRemoveTxShard(db, operationId, shard.Idx);
    }
    txState.Shards.clear();
    Y_ABORT_UNLESS(txState.ShardsInProgress.empty());

    Y_ABORT_UNLESS(context.SS->Tables.contains(txState.TargetPathId));
    TTableInfo::TPtr table = context.SS->Tables.at(txState.TargetPathId);
    TTxState::ETxState commonShardOp = TTxState::CreateParts;

    if (txState.TxType == TTxState::TxAlterTable) {
        commonShardOp = table->NeedRecreateParts()
                    ? TTxState::CreateParts
                    : TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxDropTable) {
        commonShardOp = TTxState::DropParts;
    } else if (txState.TxType == TTxState::TxBackup) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxRestore) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxInitializeBuildIndex) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxFinalizeBuildIndex) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxDropTableIndexAtMainTable) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxUpdateMainTableOnIndexMove) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxCreateCdcStreamAtTable) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxCreateCdcStreamAtTableWithInitialScan) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxAlterCdcStreamAtTable) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxAlterCdcStreamAtTableDropSnapshot) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxDropCdcStreamAtTable) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxDropCdcStreamAtTableDropSnapshot) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxRotateCdcStreamAtTable) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxRestoreIncrementalBackupAtTable) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxTruncateTable) {
        commonShardOp = TTxState::ConfigureParts;
    } else {
        Y_ABORT("UNREACHABLE");
    }

    TBindingsRoomsChanges bindingChanges;

    bool tryApplyBindingChanges = (
        txState.TxType == TTxState::TxAlterTable &&
        table->AlterData->IsFullPartitionConfig() &&
        context.SS->IsStorageConfigLogic(table));

    if (tryApplyBindingChanges) {
        TString errStr;
        auto dstPath = context.SS->PathsById.at(txState.TargetPathId);
        bool isOk = context.SS->GetBindingsRoomsChanges(
                dstPath->DomainPathId,
                table->GetPartitions(),
                table->AlterData->PartitionConfigFull(),
                bindingChanges,
                errStr);
        if (!isOk) {
            Y_ABORT("Unexpected failure to rebind column families to storage pools: %s", errStr.c_str());
        }
    }

    // Fill new list of tx shards
    for (auto& shard : table->GetPartitions()) {
        auto shardIdx = shard.ShardIdx;
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));
        auto& shardInfo = context.SS->ShardInfos.at(shardIdx);

        auto shardOp = commonShardOp;
        if (txState.TxType == TTxState::TxAlterTable) {
            if (tryApplyBindingChanges && shardInfo.BindedChannels) {
                auto it = bindingChanges.find(GetPoolsMapping(shardInfo.BindedChannels));
                if (it != bindingChanges.end()) {
                    if (it->second.ChannelsBindingsUpdated) {
                        // We must recreate this shard to apply new channel bindings
                        shardOp = TTxState::CreateParts;
                        shardInfo.BindedChannels = it->second.ChannelsBindings;
                        context.SS->PersistChannelsBinding(db, shardIdx, shardInfo.BindedChannels);
                    }

                    table->PerShardPartitionConfig[shardIdx].CopyFrom(it->second.PerShardConfig);
                    context.SS->PersistAddTableShardPartitionConfig(db, shardIdx, it->second.PerShardConfig);
                }
            }

            if (prevAlterCreateParts.contains(shardIdx)) {
                // Make sure shards that don't have channel changes this time
                // still go through their CreateParts round to apply any
                // previously changed ChannelBindings
                shardOp = TTxState::CreateParts;
            }
        }

        txState.Shards.emplace_back(shardIdx, ETabletType::DataShard, shardOp);

        shardInfo.CurrentTxId = operationId.GetTxId();
        context.SS->PersistShardTx(db, shardIdx, operationId.GetTxId());
        context.SS->PersistUpdateTxShard(db, operationId, shardIdx, shardOp);
    }
    txState.TxShardsListFinalized = true;
}

bool SourceTablePartitioningChangedForCopyTable(const TTxState &txState, TOperationContext &context) {
    Y_ABORT_UNLESS(txState.SourcePathId != InvalidPathId);
    Y_ABORT_UNLESS(txState.TargetPathId != InvalidPathId);
    const TTableInfo::TPtr srcTableInfo = *context.SS->Tables.FindPtr(txState.SourcePathId);

    THashSet<TShardIdx> srcShardIdxsLeft;
    for (const auto& p : srcTableInfo->GetPartitions()) {
        srcShardIdxsLeft.insert(p.ShardIdx);
    }

    for (const auto& shard : txState.Shards) {
        // Skip shards of the new table
        if (shard.Operation == TTxState::CreateParts)
            continue;

        Y_ABORT_UNLESS(shard.Operation == TTxState::ConfigureParts);
        // Is this shard still present in src table partitioning?
        if (srcShardIdxsLeft.erase(shard.Idx) == 0)
            return true;
    }

    // Any new shards were added to src table?
    return !srcShardIdxsLeft.empty();
}

void UpdatePartitioningForCopyTable(TOperationId operationId, TTxState &txState, TOperationContext &context) {
    Y_ABORT_UNLESS(!txState.TxShardsListFinalized, "CopyTable can adjust partitioning only once");

    // Source table must not be altered or drop while we are performing copying. So we put it into a special state.
    Y_ABORT_UNLESS(context.SS->PathsById.contains(txState.SourcePathId));
    Y_ABORT_UNLESS(context.SS->PathsById.at(txState.SourcePathId)->PathState == TPathElement::EPathState::EPathStateCopying);
    Y_ABORT_UNLESS(context.SS->PathsById.contains(txState.TargetPathId));
    auto dstPath = context.SS->PathsById.at(txState.TargetPathId);
    auto domainInfo = context.SS->SubDomains.at(dstPath->DomainPathId);

    auto srcTableInfo = context.SS->Tables.at(txState.SourcePathId);
    auto dstTableInfo = context.SS->Tables.at(txState.TargetPathId);

    NIceDb::TNiceDb db(context.GetDB());

    // Erase previous partitioning as we are going to generate new one
    context.SS->PersistTablePartitioningDeletion(db, txState.TargetPathId, dstTableInfo);

    // Remove old shardIdx info and old txShards
    for (const auto& shard : txState.Shards) {
        context.SS->PersistRemoveTxShard(db, operationId, shard.Idx);
        if (shard.Operation == TTxState::CreateParts) {
            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.Idx));
            Y_ABORT_UNLESS(context.SS->ShardInfos[shard.Idx].TabletID == InvalidTabletId, "Dst shard must not exist yet");
            auto pathId = context.SS->ShardInfos[shard.Idx].PathId;
            dstTableInfo->PerShardPartitionConfig.erase(shard.Idx);
            context.SS->PersistShardDeleted(db, shard.Idx, context.SS->ShardInfos[shard.Idx].BindedChannels);
            context.SS->ShardInfos.erase(shard.Idx);
            domainInfo->RemoveInternalShard(shard.Idx, context.SS);
            context.SS->DecrementPathDbRefCount(pathId, "remove shard from txState");
            context.SS->OnShardRemoved(shard.Idx);
        }
    }
    txState.Shards.clear();

    TChannelsBindings channelsBinding;

    bool storePerShardConfig = false;
    NKikimrSchemeOp::TPartitionConfig perShardConfig;

    if (context.SS->IsStorageConfigLogic(dstTableInfo)) {
        TVector<TStorageRoom> storageRooms;
        storageRooms.emplace_back(0);
        THashMap<ui32, ui32> familyRooms;

        TString errStr;
        bool isOk = context.SS->GetBindingsRooms(dstPath->DomainPathId, dstTableInfo->PartitionConfig(), storageRooms, familyRooms, channelsBinding, errStr);
        if (!isOk) {
            errStr = TString("database must have required storage pools to create tablet with storage config, details: ") + errStr;
            Y_ABORT("%s", errStr.c_str());
        }

        storePerShardConfig = true;
        for (const auto& room : storageRooms) {
            perShardConfig.AddStorageRooms()->CopyFrom(room);
        }
        for (const auto& familyRoom : familyRooms) {
            auto* protoFamily = perShardConfig.AddColumnFamilies();
            protoFamily->SetId(familyRoom.first);
            protoFamily->SetRoom(familyRoom.second);
        }
    } else if (context.SS->IsCompatibleChannelProfileLogic(dstPath->DomainPathId, dstTableInfo)) {
        TString errStr;
        bool isOk = context.SS->GetChannelsBindings(dstPath->DomainPathId, dstTableInfo, channelsBinding, errStr);
        if (!isOk) {
            errStr = TString("database must have required storage pools to create tablet with channel profile, details: ") + errStr;
            Y_ABORT("%s", errStr.c_str());
        }
    }

    TShardInfo datashardInfo = TShardInfo::DataShardInfo(operationId.GetTxId(), txState.TargetPathId);
    datashardInfo.BindedChannels = channelsBinding;

    auto newPartitioning = ApplyPartitioningCopyTable(datashardInfo, srcTableInfo, txState, context.SS);
    TVector<TShardIdx> newShardsIdx;
    newShardsIdx.reserve(newPartitioning.size());
    for (const auto& part : newPartitioning) {
        newShardsIdx.push_back(part.ShardIdx);
    }
    context.SS->SetPartitioning(txState.TargetPathId, dstTableInfo, std::move(newPartitioning));
    if (context.SS->EnableShred && context.SS->ShredManager->GetStatus() == EShredStatus::IN_PROGRESS) {
        context.OnComplete.Send(context.SS->SelfId(), new TEvPrivate::TEvAddNewShardToShred(std::move(newShardsIdx)));
    }

    ui32 newShardCout = dstTableInfo->GetPartitions().size();

    dstPath->SetShardsInside(newShardCout);
    domainInfo->AddInternalShards(txState, context.SS);

    context.SS->PersistTable(db, txState.TargetPathId);
    context.SS->PersistTxState(db, operationId);

    context.SS->PersistUpdateNextPathId(db);
    context.SS->PersistUpdateNextShardIdx(db);
    // Persist new shards info
    for (const auto& shard : dstTableInfo->GetPartitions()) {
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.ShardIdx), "shard info is set before");
        const auto tabletType = context.SS->ShardInfos[shard.ShardIdx].TabletType;
        context.SS->PersistShardMapping(db, shard.ShardIdx, InvalidTabletId, txState.TargetPathId, operationId.GetTxId(), tabletType);
        context.SS->PersistChannelsBinding(db, shard.ShardIdx, channelsBinding);

        if (storePerShardConfig) {
            dstTableInfo->PerShardPartitionConfig[shard.ShardIdx].CopyFrom(perShardConfig);
            context.SS->PersistAddTableShardPartitionConfig(db, shard.ShardIdx, perShardConfig);
        }
    }

    txState.TxShardsListFinalized = true;
}

TVector<TTableShardInfo> ApplyPartitioningCopyTable(const TShardInfo &templateDatashardInfo, TTableInfo::TPtr srcTableInfo, TTxState &txState, TSchemeShard *ss) {
    TVector<TTableShardInfo> dstPartitions = srcTableInfo->GetPartitions();

    // Source table must not be altered or drop while we are performing copying. So we put it into a special state.
    ui64 count = dstPartitions.size();
    txState.Shards.reserve(count*2);
    for (ui64 i = 0; i < count; ++i) {
        // Source shards need to get "Send parts" transaction
        auto srcShardIdx = srcTableInfo->GetPartitions()[i].ShardIdx;
        Y_ABORT_UNLESS(ss->ShardInfos.contains(srcShardIdx), "Source table shard not found");
        auto srcTabletId = ss->ShardInfos[srcShardIdx].TabletID;
        Y_ABORT_UNLESS(srcTabletId != InvalidTabletId);
        txState.Shards.emplace_back(srcShardIdx, ETabletType::DataShard, TTxState::ConfigureParts);
        // Destination shards need to be created, configured and then they will receive parts
        auto idx = ss->RegisterShardInfo(templateDatashardInfo);
        txState.Shards.emplace_back(idx, ETabletType::DataShard, TTxState::CreateParts);
        // Properly set new shard idx
        dstPartitions[i].ShardIdx = idx;
        // clear lag to avoid counter underflow
        dstPartitions[i].LastCondEraseLag.Clear();
    }

    return dstPartitions;
}

// NTableState::TProposedWaitParts
// Must be in sync with NTableState::TMoveTableProposedWaitParts
TProposedWaitParts::TProposedWaitParts(TOperationId id, TTxState::ETxState nextState)
    : OperationId(id)
    , NextState(nextState)
{
    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD, DebugHint() << " Constructed");
    IgnoreMessages(DebugHint(),
        { TEvHive::TEvCreateTabletReply::EventType
        , TEvDataShard::TEvProposeTransactionResult::EventType
        , TEvColumnShard::TEvProposeTransactionResult::EventType
        , TEvPrivate::TEvOperationPlan::EventType }
    );
}

template<typename TEvent>
bool TProposedWaitParts::HandleReplyImpl(const TEvent& ev, TOperationContext& context) {
    TTabletId ssId = context.SS->SelfTabletId();
    const auto& evRecord = ev->Get()->Record;

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " HandleReply " << TEvSchemaChangedTraits<TEvent>::GetName()
                            << " at tablet: " << ssId);
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " HandleReply " << TEvSchemaChangedTraits<TEvent>::GetName()
                            << " at tablet: " << ssId
                            << " message: " << evRecord.ShortDebugString());

    if (!CollectSchemaChanged(OperationId, ev, context)) {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply " << TEvSchemaChangedTraits<TEvent>::GetName()
                                << " CollectSchemaChanged: false");
        return false;
    }

    Y_ABORT_UNLESS(context.SS->FindTx(OperationId));
    TTxState& txState = *context.SS->FindTx(OperationId);

    if (!txState.ReadyForNotifications) {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply " << TEvSchemaChangedTraits<TEvent>::GetName()
                                << " ReadyForNotifications: false");
        return false;
    }

    return true;
}

bool TProposedWaitParts::HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) {
    return HandleReplyImpl(ev, context);
}

bool TProposedWaitParts::HandleReply(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev, TOperationContext& context) {
    return HandleReplyImpl(ev, context);
}

bool TProposedWaitParts::ProgressState(TOperationContext& context) {
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
        AckAllSchemaChanges(OperationId, *txState, context);
        context.SS->ChangeTxState(db, OperationId, NextState);
        return true;
    }

    return false;
}

}  // namespace NTableState

namespace NPQState {

bool CollectProposeTransactionResults(const TOperationId& operationId,
                                      const TEvPersQueue::TEvProposeTransactionResult::TPtr& ev,
                                      TOperationContext& context)
{
    auto prepared = [](NKikimrPQ::TEvProposeTransactionResult::EStatus status) -> bool {
        return status == NKikimrPQ::TEvProposeTransactionResult::PREPARED;
    };

    auto toString = [](NKikimrPQ::TEvProposeTransactionResult::EStatus status) -> TString {
        return NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(status);
    };

    return CollectProposeTxResults(ev, operationId, context, prepared, toString);
}

}  // namespace NPQState

TSet<ui32> AllIncomingEvents() {
    TSet<ui32> result;

#define AddToList(NS, TEvType, ...) \
    result.insert(::NKikimr::NS::TEvType::EventType);

    SCHEMESHARD_INCOMING_EVENTS(AddToList)
#undef AddToList

    return result;
}

namespace NForceDrop {

void CollectShards(const THashSet<TPathId>& paths, TOperationId operationId, TTxState *txState, TOperationContext &context) {
    NIceDb::TNiceDb db(context.GetDB());

    auto shards = context.SS->CollectAllShards(paths);
    for (auto shardIdx: shards) {
        Y_VERIFY_S(context.SS->ShardInfos.contains(shardIdx), "Unknown shardIdx " << shardIdx);
        auto& shardInfo = context.SS->ShardInfos.at(shardIdx);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Collect shard"
                    << ", shard idx: " << shardIdx
                    << ", tabletID: " << shardInfo.TabletID
                    << ", path id: " << shardInfo.PathId);

        txState->Shards.emplace_back(shardIdx, shardInfo.TabletType, txState->State);

        shardInfo.CurrentTxId = operationId.GetTxId();
        context.SS->PersistShardTx(db, shardIdx, operationId.GetTxId());

        if (TTabletTypes::DataShard == shardInfo.TabletType) {
            context.SS->TabletCounters->Simple()[COUNTER_TABLE_SHARD_ACTIVE_COUNT].Sub(1);
            context.SS->TabletCounters->Simple()[COUNTER_TABLE_SHARD_INACTIVE_COUNT].Add(1);
        }
    }

    context.SS->PersistTxState(db, operationId);
}

void ValidateNoTransactionOnPaths(TOperationId operationId, const THashSet<TPathId>& paths, TOperationContext &context) {
    // No transaction should materialize in a subdomain that is being deleted --
    // -- all operations should be checking parent dir status at Propose stage.
    // However, it is better to verify that, just in case.
    auto transactions = context.SS->GetRelatedTransactions(paths, context.Ctx);
    for (auto otherTxId: transactions) {
        if (otherTxId == operationId.GetTxId()) {
            continue;
        }
        Y_VERIFY_S(false, "unexpected transaction: " << otherTxId << " found on the subdomain being deleted by transaction " << operationId.GetTxId());
    }
}

}  // namespace NForceDrop

void IncParentDirAlterVersionWithRepublishSafeWithUndo(const TOperationId& opId, const TPath& path, TSchemeShard* ss, TSideEffects& onComplete) {
    auto parent = path.Parent();
    if (parent.Base()->IsDirectory() || parent.Base()->IsDomainRoot()) {
        ++parent.Base()->DirAlterVersion;
    }

    if (parent.IsActive()) {
        ss->ClearDescribePathCaches(parent.Base());
        onComplete.PublishToSchemeBoard(opId, parent->PathId);
    }

    if (path.IsActive()) {
        ss->ClearDescribePathCaches(path.Base());
        onComplete.PublishToSchemeBoard(opId, path->PathId);
    }
}

void IncParentDirAlterVersionWithRepublish(const TOperationId& opId, const TPath& path, TOperationContext &context) {
    IncParentDirAlterVersionWithRepublishSafeWithUndo(opId, path, context.SS, context.OnComplete);

    auto parent = path.Parent();
    if (parent.Base()->IsDirectory() || parent.Base()->IsDomainRoot()) {
        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistPathDirAlterVersion(db, parent.Base());
    }
}

void IncAliveChildrenSafeWithUndo(const TOperationId& opId, const TPath& parentPath, TOperationContext& context, bool isBackup) {
    parentPath.Base()->IncAliveChildrenPrivate(isBackup);
    if (parentPath.Base()->GetAliveChildren() == 1 && !parentPath.Base()->IsDomainRoot()) {
        auto grandParent = parentPath.Parent();
        if (grandParent.Base()->IsLikeDirectory()) {
            ++grandParent.Base()->DirAlterVersion;
            context.MemChanges.GrabPath(context.SS, grandParent.Base()->PathId);
            context.DbChanges.PersistPath(grandParent.Base()->PathId);
        }

        if (grandParent.IsActive()) {
            context.SS->ClearDescribePathCaches(grandParent.Base());
            context.OnComplete.PublishToSchemeBoard(opId, grandParent->PathId);
        }
    }
}

void IncAliveChildrenDirect(const TOperationId& opId, const TPath& parentPath, TOperationContext& context, bool isBackup) {
    parentPath.Base()->IncAliveChildrenPrivate(isBackup);
    if (parentPath.Base()->GetAliveChildren() == 1 && !parentPath.Base()->IsDomainRoot()) {
        auto grandParent = parentPath.Parent();
        if (grandParent.Base()->IsLikeDirectory()) {
            ++grandParent.Base()->DirAlterVersion;
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->PersistPathDirAlterVersion(db, grandParent.Base());
        }

        if (grandParent.IsActive()) {
            context.SS->ClearDescribePathCaches(grandParent.Base());
            context.OnComplete.PublishToSchemeBoard(opId, grandParent->PathId);
        }
    }
}

void DecAliveChildrenDirect(const TOperationId& opId, TPathElement::TPtr parentPath, TOperationContext& context, bool isBackup) {
    parentPath->DecAliveChildrenPrivate(isBackup);
    if (parentPath->GetAliveChildren() == 0 && !parentPath->IsDomainRoot()) {
        auto grandParentDir = context.SS->PathsById.at(parentPath->ParentPathId);
        if (grandParentDir->IsLikeDirectory()) {
            ++grandParentDir->DirAlterVersion;
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->PersistPathDirAlterVersion(db, grandParentDir);
            context.SS->ClearDescribePathCaches(grandParentDir);
            context.OnComplete.PublishToSchemeBoard(opId, grandParentDir->PathId);
        }
    }
}

NKikimrSchemeOp::TModifyScheme MoveTableTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst) {
    NKikimrSchemeOp::TModifyScheme scheme;

    scheme.SetWorkingDir(dst.Parent().PathString());
    scheme.SetFailOnExist(true);
    scheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMoveTable);
    auto operation = scheme.MutableMoveTable();
    operation->SetSrcPath(src.PathString());
    operation->SetDstPath(dst.PathString());

    return scheme;
}

NKikimrSchemeOp::TModifyScheme MoveTableIndexTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst) {
    NKikimrSchemeOp::TModifyScheme scheme;

    scheme.SetWorkingDir(dst.Parent().PathString());
    scheme.SetFailOnExist(true);
    scheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMoveTableIndex);
    auto operation = scheme.MutableMoveTableIndex();
    operation->SetSrcPath(src.PathString());
    operation->SetDstPath(dst.PathString());

    return scheme;
}

void AbortUnsafeDropOperation(const TOperationId& opId, const TTxId& txId, TOperationContext& context) {
    TTxState* txState = context.SS->FindTx(opId);
    Y_ABORT_UNLESS(txState);

    LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, ""
        << TTxState::TypeName(txState->TxType) << " AbortUnsafe"
        << ": opId# " << opId
        << ", txId# " << txId
        << ", ssId# " << context.SS->TabletID());

    const auto& pathId = txState->TargetPathId;
    Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
    auto path = context.SS->PathsById.at(pathId);
    Y_ABORT_UNLESS(path);

    if (path->Dropped()) {
        for (const auto& shard : txState->Shards) {
            context.OnComplete.DeleteShard(shard.Idx);
        }
    }

    context.OnComplete.DoneOperation(opId);
}

}
}

namespace NKikimr::NSchemeShard::NTableIndexVersion {

TVector<TPathId> SyncChildIndexVersions(
    TPathElement::TPtr path,
    TTableInfo::TPtr table,
    ui64 targetVersion,
    TOperationId operationId,
    TOperationContext& context,
    NIceDb::TNiceDb& db,
    bool skipPlannedToDrop)
{
    Y_UNUSED(table);
    TVector<TPathId> publishedIndexes;

    for (const auto& [childName, childPathId] : path->GetChildren()) {
        if (!context.SS->PathsById.contains(childPathId)) {
            continue;
        }
        auto childPath = context.SS->PathsById.at(childPathId);
        if (!childPath->IsTableIndex() || childPath->Dropped()) {
            continue;
        }
        if (skipPlannedToDrop && childPath->PlannedToDrop()) {
            continue;
        }
        if (!context.SS->Indexes.contains(childPathId)) {
            continue;
        }
        auto index = context.SS->Indexes.at(childPathId);
        if (index->AlterVersion < targetVersion) {
            index->AlterVersion = targetVersion;
            // If there's ongoing alter operation, also update alterData version to converge
            if (index->AlterData && index->AlterData->AlterVersion < targetVersion) {
                index->AlterData->AlterVersion = targetVersion;
                context.SS->PersistTableIndexAlterData(db, childPathId);
            }
            context.SS->PersistTableIndexAlterVersion(db, childPathId, index);
            context.SS->ClearDescribePathCaches(childPath);
            context.OnComplete.PublishToSchemeBoard(operationId, childPathId);
            publishedIndexes.push_back(childPathId);
        }
    }

    return publishedIndexes;
}

}  // NKikimr::NSchemeShard::NTableIndexVersion
