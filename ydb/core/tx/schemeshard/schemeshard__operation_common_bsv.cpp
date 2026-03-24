#include "schemeshard__operation_common.h"
#include "schemeshard_private.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/blockstore/core/blockstore.h>


namespace NKikimr::NSchemeShard::NBSVState {

// NBSVState::TConfigureParts
//
TConfigureParts::TConfigureParts(TOperationId id)
    : OperationId(id)
{
    IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
}

bool TConfigureParts::HandleReply(TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev, TOperationContext& context) {
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

bool TConfigureParts::ProgressState(TOperationContext& context) {
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
            shard.TabletType == ETabletType::BlockStorePartition2 ||
            shard.TabletType == ETabletType::BlockStorePartitionDirect) {
            continue;
        }

        Y_ABORT_UNLESS(shard.TabletType == ETabletType::BlockStoreVolume
            || shard.TabletType == ETabletType::BlockStoreVolumeDirect);
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


// NBSVState::TPropose
//
TPropose::TPropose(TOperationId id)
    : OperationId(id)
{
    IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvBlockStore::TEvUpdateVolumeConfigResponse::EventType});
}

bool TPropose::HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) {
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

bool TPropose::ProgressState(TOperationContext& context) {
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

}  // NKikimr::NSchemeShard::NBSVState
