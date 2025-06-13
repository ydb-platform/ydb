#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TConfigureDestination: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TSplitMerge TConfigureDestination"
                << " operationId#" << OperationId;
    }

public:
    TConfigureDestination(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvDataShard::TEvInitSplitMergeDestinationAck::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvInitSplitMergeDestinationAck"
                               << ", operationId: " << OperationId
                               << ", at schemeshard: " << ssId
                               << " message# " << ev->Get()->Record.ShortDebugString());

        NIceDb::TNiceDb db(context.GetDB());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxSplitTablePartition || txState->TxType == TTxState::TxMergeTablePartition);
        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        TTabletId tabletId = TTabletId(ev->Get()->Record.GetTabletId());
        TShardIdx idx = context.SS->MustGetShardIdx(tabletId);
        if (!idx) {
            LOG_ERROR(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                      "Tablet %" PRIu64 " is not known in TxId %" PRIu64,
                      tabletId, OperationId.GetTxId());
            return false;
        }

        if (!context.SS->ShardInfos.contains(idx)) {
            LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint() << " Got InitSplitMergeDestinationAck"
                       << " for unknown shard idx " << idx
                       << " tabletId " << tabletId);
            return false;
        }

        txState->ShardsInProgress.erase(idx);

        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, idx);

        // If all dst datashards have been initialized
        if (txState->ShardsInProgress.empty()) {
            context.SS->ChangeTxState(db, OperationId, TTxState::TransferData);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TSplitMerge TConfigureDestination ProgressState"
                       << ", operationId: " << OperationId
                       << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->TxInFlight.FindPtr(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxSplitTablePartition || txState->TxType == TTxState::TxMergeTablePartition);
        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        txState->ClearShardsInProgress();

        // A helper hash map translating tablet id to destination range index
        THashMap<TTabletId, ui32> dstTabletToRangeIdx;

        auto getDstRangeIdx = [&dstTabletToRangeIdx] (TTabletId tabletId) -> ui32 {
            auto it = dstTabletToRangeIdx.find(tabletId);
            Y_VERIFY_S(it != dstTabletToRangeIdx.end(),
                       "Cannot find range info for destination tablet " << tabletId);
            return it->second;
        };

        // Fill tablet ids in split description
        NKikimrTxDataShard::TSplitMergeDescription& splitDescr = *txState->SplitDescription;
        for (ui32 i = 0; i < splitDescr.DestinationRangesSize(); ++i) {
            auto* rangeDescr = splitDescr.MutableDestinationRanges()->Mutable(i);
            auto shardIdx = context.SS->MakeLocalId(TLocalShardIdx(rangeDescr->GetShardIdx()));
            auto datashardId = context.SS->ShardInfos[shardIdx].TabletID;
            rangeDescr->SetTabletID(ui64(datashardId));
            dstTabletToRangeIdx[datashardId] = i;
        }

        // Save updated split description
        TString extraData;
        bool serializeRes = txState->SplitDescription->SerializeToString(&extraData);
        Y_ABORT_UNLESS(serializeRes);
        NIceDb::TNiceDb db(context.GetDB());
        db.Table<Schema::TxInFlightV2>().Key(OperationId.GetTxId(), OperationId.GetSubTxId()).Update(
                    NIceDb::TUpdate<Schema::TxInFlightV2::ExtraBytes>(extraData));

        const auto tableInfo = context.SS->Tables.FindPtr(txState->TargetPathId);
        Y_ABORT_UNLESS(tableInfo);

        const ui64 alterVersion = (*tableInfo)->AlterVersion;

        const ui64 subDomainPathId = context.SS->ResolvePathIdForDomain(txState->TargetPathId).LocalPathId;

        for (const auto& shard: txState->Shards) {
            // Skip src shard
            if (shard.Operation != TTxState::CreateParts) {
                continue;
            }

            TTabletId datashardId = context.SS->ShardInfos[shard.Idx].TabletID;

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Initializing scheme "
                            << "on dst datashard: " << datashardId
                            << " splitOp: " << OperationId
                            << " alterVersion: " << alterVersion
                            << " at tablet: " << context.SS->TabletID());

            const ui32 rangeIdx = getDstRangeIdx(datashardId);
            const auto& rangeDescr = splitDescr.GetDestinationRanges(rangeIdx);

            // For each destination shard we construct an individual description
            // that contains all src shards and only this one dst shard with its range
            NKikimrTxDataShard::TSplitMergeDescription splitDescForShard;
            splitDescForShard.MutableSourceRanges()->CopyFrom(txState->SplitDescription->GetSourceRanges());
            splitDescForShard.AddDestinationRanges()->CopyFrom(rangeDescr);

            Y_ABORT_UNLESS(txState->SplitDescription);
            auto event = MakeHolder<TEvDataShard::TEvInitSplitMergeDestination>(
                ui64(OperationId.GetTxId()),
                context.SS->TabletID(),
                subDomainPathId,
                splitDescForShard,
                context.SS->SelectProcessingParams(txState->TargetPathId));

            // Add a new-style CreateTable with correct per-shard settings
            // WARNING: legacy datashard will ignore this and use the schema
            // received from some source datashard instead, so schemas must not
            // diverge during a migration period. That's ok though, since
            // schemas may only become incompatible after column family storage
            // configuration is altered, and it's protected with a feature flag.
            auto tableDesc = event->Record.MutableCreateTable();
            context.SS->FillTableDescriptionForShardIdx(
                txState->TargetPathId,
                shard.Idx,
                tableDesc,
                rangeDescr.GetKeyRangeBegin(),
                rangeDescr.GetKeyRangeEnd(),
                true, false);
            context.SS->FillTableSchemaVersion(alterVersion, tableDesc);

            context.OnComplete.BindMsgToPipe(OperationId, datashardId, shard.Idx, event.Release());
        }

        txState->UpdateShardsInProgress(TTxState::CreateParts);
        return false;
    }
};

class TTransferData: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TSplitMerge TTransferData"
                << " operationId#" << OperationId;
    }

public:
    TTransferData(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvDataShard::TEvInitSplitMergeDestinationAck::EventType});
    }

    bool HandleReply(TEvDataShard::TEvSplitAck::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSplitAck"
                               << ", at schemeshard: " << ssId
                               << ", message: " << ev->Get()->Record.ShortDebugString());

        NIceDb::TNiceDb db(context.GetDB());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxSplitTablePartition || txState->TxType == TTxState::TxMergeTablePartition);
        Y_ABORT_UNLESS(txState->State == TTxState::TransferData);

        auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
        auto srcShardIdx = context.SS->GetShardIdx(tabletId);
        if (!srcShardIdx) {
            LOG_ERROR(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                      "Tablet %" PRIu64 " is not known in TxId %" PRIu64,
                      tabletId, OperationId.GetTxId());
            return false;
        }

        if (!context.SS->ShardInfos.contains(srcShardIdx)) {
            LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint() << " Got SplitAck for unknown shard"
                       << " idx " << srcShardIdx
                       << " tabletId " << tabletId);
            return false;
        }

        txState->ShardsInProgress.erase(srcShardIdx);
        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, srcShardIdx);

        if (!txState->ShardsInProgress.empty()) {
            // TODO: verify that we only wait for Src shards
            return false;
        }

        // Switch table partitioning: exclude src shard and include all dst shards
        TPathId tableId = txState->TargetPathId;
        TTableInfo::TPtr tableInfo = *context.SS->Tables.FindPtr(tableId);
        Y_ABORT_UNLESS(tableInfo);

        // Replace all Src datashard(s) with Dst datashard(s)
        TVector<TTableShardInfo> newPartitioning;
        THashSet<TShardIdx> allSrcShardIdxs;
        for (const auto& txShard : txState->Shards) {
            if (txShard.Operation == TTxState::TransferData)
                allSrcShardIdxs.insert(txShard.Idx);
        }

        bool dstAdded = false;
        const auto now = context.Ctx.Now();
        for (const auto& shard : tableInfo->GetPartitions()) {
            if (allSrcShardIdxs.contains(shard.ShardIdx)) {
                if (auto& lag = shard.LastCondEraseLag) {
                    context.SS->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].DecrementFor(lag->Seconds());
                    lag.Clear();
                }

                if (dstAdded) {
                    continue;
                }

                for (const auto& txShard : txState->Shards) {
                    if (txShard.Operation != TTxState::CreateParts)
                        continue;

                    // TODO: make sure dst are sorted by range end
                    Y_ABORT_UNLESS(context.SS->ShardInfos.contains(txShard.Idx));
                    TTableShardInfo dst(txShard.Idx, txShard.RangeEnd);

                    if (tableInfo->IsTTLEnabled()) {
                        auto& lag = dst.LastCondEraseLag;
                        Y_DEBUG_ABORT_UNLESS(!lag.Defined());

                        lag = now - dst.LastCondErase;
                        context.SS->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].IncrementFor(lag->Seconds());
                    }

                    newPartitioning.push_back(dst);
                }

                dstAdded = true;
            } else {
                newPartitioning.push_back(shard);
            }
        }

        auto oldAggrStats = tableInfo->GetStats().Aggregated;

        // Delete the whole old partitioning and persist the whole new partitioning as the indexes have changed
        context.SS->PersistTablePartitioningDeletion(db, tableId, tableInfo);
        context.SS->SetPartitioning(tableId, tableInfo, std::move(newPartitioning));
        context.SS->PersistTablePartitioning(db, tableId, tableInfo);
        context.SS->PersistTablePartitionStats(db, tableId, tableInfo);

        context.SS->TabletCounters->Simple()[COUNTER_TABLE_SHARD_ACTIVE_COUNT].Sub(allSrcShardIdxs.size());
        context.SS->TabletCounters->Simple()[COUNTER_TABLE_SHARD_INACTIVE_COUNT].Add(allSrcShardIdxs.size());

        if (!tableInfo->IsBackup && !tableInfo->IsShardsStatsDetached()) {
            auto newAggrStats = tableInfo->GetStats().Aggregated;
            auto subDomainId = context.SS->ResolvePathIdForDomain(tableId);
            auto subDomainInfo = context.SS->ResolveDomainInfo(tableId);
            subDomainInfo->AggrDiskSpaceUsage(context.SS, newAggrStats, oldAggrStats);
            if (subDomainInfo->CheckDiskSpaceQuotas(context.SS)) {
                context.SS->PersistSubDomainState(db, subDomainId, *subDomainInfo);
                context.OnComplete.PublishToSchemeBoard(OperationId, subDomainId);
            }
        }

        Y_ABORT_UNLESS(txState->ShardsInProgress.empty(), "All shards should have already completed their steps");

        context.SS->ChangeTxState(db, OperationId, TTxState::NotifyPartitioningChanged);
        context.OnComplete.ActivateTx(OperationId);

        context.OnComplete.PublishToSchemeBoard(OperationId, tableId);

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->TxInFlight.FindPtr(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxSplitTablePartition || txState->TxType == TTxState::TxMergeTablePartition);
        Y_ABORT_UNLESS(txState->State == TTxState::TransferData);

        txState->ClearShardsInProgress();

        for (const auto& shard : txState->Shards) {
            // Skip Dst shards
            if (shard.Operation != TTxState::TransferData)
                continue;

            auto datashardId = context.SS->ShardInfos[shard.Idx].TabletID;

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " Starting split on src datashard " << datashardId
                        << " splitOpId# " << OperationId
                        << " at tablet " << context.SS->TabletID());

            auto event = MakeHolder<TEvDataShard::TEvSplit>(ui64(OperationId.GetTxId()));

            Y_ABORT_UNLESS(txState->SplitDescription);
            event->Record.MutableSplitDescription()->CopyFrom(*txState->SplitDescription);

            context.OnComplete.BindMsgToPipe(OperationId, datashardId, shard.Idx, event.Release());
        }

        txState->UpdateShardsInProgress(TTxState::TransferData);
        return false;
    }
};

class TNotifySrc: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TSplitMerge TNotifySrc"
                << ", operationId: " << OperationId;
    }
public:
    TNotifySrc(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvDataShard::TEvInitSplitMergeDestinationAck::EventType,  TEvDataShard::TEvSplitAck::EventType});
    }


    bool HandleReply(TEvDataShard::TEvSplitPartitioningChangedAck::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSplitPartitioningChangedAck"
                               << ", from datashard: " << tabletId
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxSplitTablePartition || txState->TxType == TTxState::TxMergeTablePartition);
        Y_ABORT_UNLESS(txState->State == TTxState::NotifyPartitioningChanged);


        auto idx = context.SS->GetShardIdx(tabletId);
        if (!idx) {
            LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Datashard is not listed in tablet to shard map"
                            << ", datashard: " << tabletId
                            << ", opId: " << OperationId);
            return false;
        }

        if (!txState->ShardsInProgress.contains(idx)) {
            // TODO: verify that this is a repeated event from known Src shard, not a random one
            LOG_INFO(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "Got SplitPartitioningChangedAck from shard %" PRIu64 " that is not a  part ot Tx %" PRIu64, tabletId, OperationId.GetTxId());
            return false;
        }

        txState->ShardsInProgress.erase(idx);
        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, idx);

        if (!txState->ShardsInProgress.empty()) {
            // TODO: verify that we only wait for Src shards
            return false;
        }

        context.SS->DeleteSplitOp(OperationId, *txState);

        context.OnComplete.DoneOperation(OperationId);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        //By this moment the partitioning scheme of the table has been switched and is visible to the users
        //We just want notify Src shard that it should reject all new transactions and return SchemeChanged error
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->TxInFlight.FindPtr(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxSplitTablePartition || txState->TxType == TTxState::TxMergeTablePartition);
        Y_ABORT_UNLESS(txState->State == TTxState::NotifyPartitioningChanged);

//        Y_ABORT_UNLESS(txState->Notify.Empty(), "All notifications for split op shouldn't have been sent before switching to NotifyPartitioningChanged state");

        txState->ClearShardsInProgress();

        bool needToNotifySrc = false;

        for (const auto& shard : txState->Shards) {
            // Skip Dst shards
            if (shard.Operation != TTxState::TransferData) {
                continue;
            }

            if (!context.SS->ShardInfos.contains(shard.Idx) || context.SS->ShardDeleter.Has(shard.Idx)) {
                LOG_DEBUG(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                          "Src datashard idx %" PRIu64 " for splitOp# %" PRIu64 " is already deleted or is intended to at tablet %" PRIu64,
                          shard.Idx, OperationId.GetTxId(), context.SS->TabletID());
                continue;
            }

            auto datashardId = context.SS->ShardInfos[shard.Idx].TabletID;

            needToNotifySrc = true;
            LOG_DEBUG(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                      "Notify src datashard %" PRIu64 " on partitioning changed splitOp# %" PRIu64 " at tablet %" PRIu64,
                      datashardId, OperationId.GetTxId(), context.SS->TabletID());

            THolder<TEvDataShard::TEvSplitPartitioningChanged> event = MakeHolder<TEvDataShard::TEvSplitPartitioningChanged>(ui64(OperationId.GetTxId()));

            context.OnComplete.BindMsgToPipe(OperationId, datashardId, shard.Idx, event.Release());

            txState->ShardsInProgress.insert(shard.Idx);
        }

        if (!needToNotifySrc) {
            // The Src datashard could have already completed all its work, reported Offline state and got deleted
            // In this case the transaction is finished because this was the last step
            context.SS->DeleteSplitOp(OperationId, *txState);

            context.OnComplete.DoneOperation(OperationId);
            return true;
        }

        return false;
    }
};

class TSplitMerge: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::TransferData;
        case TTxState::TransferData:
            return TTxState::NotifyPartitioningChanged;
        case TTxState::NotifyPartitioningChanged:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::CreateParts:
            return MakeHolder<TCreateParts>(OperationId);
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureDestination>(OperationId);
        case TTxState::TransferData:
            return MakeHolder<TTransferData>(OperationId);
        case TTxState::NotifyPartitioningChanged:
            return MakeHolder<TNotifySrc>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    bool AllocateDstForMerge(
            const NKikimrSchemeOp::TSplitMergeTablePartitions& info,
            TTxId txId,
            const TPathId& pathId,
            const TVector<ui64>& srcPartitionIdxs,
            const TTableInfo::TCPtr tableInfo,
            TTxState& op,
            const TChannelsBindings& channels,
            TString& errStr,
            TOperationContext& context)
    {
        // N source shards are merged into 1
        Y_ABORT_UNLESS(srcPartitionIdxs.size() > 1);
        Y_ABORT_UNLESS(info.SplitBoundarySize() == 0);

        if (tableInfo->GetExpectedPartitionCount() + 1 - srcPartitionIdxs.size() < tableInfo->GetMinPartitionsCount()) {
            errStr = "Reached MinPartitionsCount limit: " + ToString(tableInfo->GetMinPartitionsCount());
            return false;
        }

        // Check that partitions for merge are consecutive in tableInfo->Partitions (e.g. don't allow to merge #1 with #3 tree and leave #2)
        for (ui32 i = 1; i < srcPartitionIdxs.size(); ++i) {
            ui64 pi = srcPartitionIdxs[i];
            ui64 piPrev = srcPartitionIdxs[i-1];

            if (pi != piPrev + 1) {
                auto shardIdx = tableInfo->GetPartitions()[pi].ShardIdx;
                auto shardIdxPrev = tableInfo->GetPartitions()[piPrev].ShardIdx;

                errStr = TStringBuilder()
                    << "Partitions are not consecutive at index " << i << " : #" << piPrev << "(" << context.SS->ShardInfos[shardIdxPrev].TabletID << ")"
                    << " then #" << pi << "(" << context.SS->ShardInfos[shardIdx].TabletID << ")";
                return false;
            }
        }

        TString firstRangeBegin;
        if (srcPartitionIdxs[0] != 0) {
            // Take the end of previous shard
            firstRangeBegin = tableInfo->GetPartitions()[srcPartitionIdxs[0]-1].EndOfRange;
        } else {
            TVector<TCell> firstKey;
            ui32 keyColCount = 0;
            for (const auto& col : tableInfo->Columns) {
                if (col.second.IsKey()) {
                    ++keyColCount;
                }
            }
            // Or start from (NULL, NULL, .., NULL)
            firstKey.resize(keyColCount);
            firstRangeBegin = TSerializedCellVec::Serialize(firstKey);
        }

        op.SplitDescription = std::make_shared<NKikimrTxDataShard::TSplitMergeDescription>();
        // Fill src shards
        TString prevRangeEnd = firstRangeBegin;
        for (ui64 pi : srcPartitionIdxs) {
            auto* srcRange = op.SplitDescription->AddSourceRanges();
            auto shardIdx = tableInfo->GetPartitions()[pi].ShardIdx;
            srcRange->SetShardIdx(ui64(shardIdx.GetLocalId()));
            srcRange->SetTabletID(ui64(context.SS->ShardInfos[shardIdx].TabletID));
            srcRange->SetKeyRangeBegin(prevRangeEnd);
            TString rangeEnd = tableInfo->GetPartitions()[pi].EndOfRange;
            srcRange->SetKeyRangeEnd(rangeEnd);
            prevRangeEnd = rangeEnd;
        }

        // Fill dst shard
        TShardInfo datashardInfo = TShardInfo::DataShardInfo(txId, pathId);
        datashardInfo.BindedChannels = channels;

        auto idx = context.SS->RegisterShardInfo(datashardInfo);

        ui64 lastSrcPartition = srcPartitionIdxs.back();
        TString lastRangeEnd = tableInfo->GetPartitions()[lastSrcPartition].EndOfRange;

        TTxState::TShardOperation dstShardOp(idx, ETabletType::DataShard, TTxState::CreateParts);
        dstShardOp.RangeEnd = lastRangeEnd;
        op.Shards.push_back(dstShardOp);

        auto* dstRange = op.SplitDescription->AddDestinationRanges();
        dstRange->SetShardIdx(ui64(idx.GetLocalId()));
        dstRange->SetKeyRangeBegin(firstRangeBegin);
        dstRange->SetKeyRangeEnd(lastRangeEnd);

        return true;
    }

    bool AllocateDstForSplit(
            const NKikimrSchemeOp::TSplitMergeTablePartitions& info,
            TTxId txId,
            const TPathId& pathId,
            ui64 srcPartitionIdx,
            const TTableInfo::TCPtr tableInfo,
            TTxState& op,
            const TChannelsBindings& channels,
            TString& errStr,
            TOperationContext& context)
    {
        // n split points produce n+1 parts
        ui64 count = info.SplitBoundarySize() + 1;
        if (count == 1) {
            errStr = "No split boundaries specified";
            return false;
        }

        auto srcShardIdx = tableInfo->GetPartitions()[srcPartitionIdx].ShardIdx;
        const auto forceShardSplitSettings = context.SS->SplitSettings.GetForceShardSplitSettings();

        if (tableInfo->GetExpectedPartitionCount() + count - 1 > tableInfo->GetMaxPartitionsCount() &&
            !tableInfo->IsForceSplitBySizeShardIdx(srcShardIdx, forceShardSplitSettings))
        {
            errStr = "Reached MaxPartitionsCount limit: " + ToString(tableInfo->GetMaxPartitionsCount());
            return false;
        }

        TShardInfo datashardInfo = TShardInfo::DataShardInfo(txId, pathId);
        datashardInfo.BindedChannels = channels;

        // Build vector of key column types
        TVector<NScheme::TTypeInfo> keyColTypeIds;
        for (const auto& col : tableInfo->Columns) {
            if (!col.second.IsKey())
                continue;
            size_t keyIdx = col.second.KeyOrder;
            keyColTypeIds.resize(Max(keyColTypeIds.size(), keyIdx+1));
            keyColTypeIds[keyIdx] = col.second.PType;
        }

        TVector<TString> rangeEnds;
        if (!TSchemeShard::FillSplitPartitioning(rangeEnds, keyColTypeIds, info.GetSplitBoundary(), errStr)) {
            return false;
        }

        // Last dst shard ends where src shard used to end
        rangeEnds.push_back(tableInfo->GetPartitions()[srcPartitionIdx].EndOfRange);

        op.SplitDescription = std::make_shared<NKikimrTxDataShard::TSplitMergeDescription>();
        auto* srcRange = op.SplitDescription->AddSourceRanges();
        srcRange->SetShardIdx(ui64(srcShardIdx.GetLocalId()));
        srcRange->SetTabletID(ui64(context.SS->ShardInfos[srcShardIdx].TabletID));
        srcRange->SetKeyRangeEnd(tableInfo->GetPartitions()[srcPartitionIdx].EndOfRange);

        // Check that ranges are sorted in ascending order
        TVector<TCell> prevKey;
        if (srcPartitionIdx != 0) {
            // Take the end of previous shard
            TSerializedCellVec key(tableInfo->GetPartitions()[srcPartitionIdx-1].EndOfRange);
            prevKey.assign(key.GetCells().begin(), key.GetCells().end());
        } else {
            // Or start from (NULL, NULL, .., NULL)
            prevKey.resize(keyColTypeIds.size());
        }
        TString firstRangeBegin = TSerializedCellVec::Serialize(prevKey);
        srcRange->SetKeyRangeBegin(firstRangeBegin);

        for (ui32 i = 0; i < rangeEnds.size(); ++i) {
            TSerializedCellVec key(rangeEnds[i]);
            if (CompareBorders<true, true>(prevKey, key.GetCells(), true, true, keyColTypeIds) >= 0) {
                errStr = Sprintf("Partition ranges are not sorted at index %u", i);
                return false;
            }
            prevKey.assign(key.GetCells().begin(), key.GetCells().end());
        }

        // Allocate new datashard ids
        TString rangeBegin = firstRangeBegin;
        for (ui64 i = 0; i < count; ++i) {
            const auto idx = context.SS->RegisterShardInfo(datashardInfo);

            TString rangeEnd = rangeEnds[i];
            TTxState::TShardOperation dstShardOp(idx, ETabletType::DataShard, TTxState::CreateParts);
            dstShardOp.RangeEnd = rangeEnd;
            op.Shards.push_back(dstShardOp);

            auto* dstRange = op.SplitDescription->AddDestinationRanges();
            dstRange->SetShardIdx(ui64(idx.GetLocalId()));
            dstRange->SetKeyRangeBegin(rangeBegin);
            dstRange->SetKeyRangeEnd(rangeEnd);

            rangeBegin = rangeEnd;
        }

        return true;
    }

    bool AllocateDstForOneToOne(
            const NKikimrSchemeOp::TSplitMergeTablePartitions& info,
            TTxId txId,
            const TPathId& pathId,
            const TVector<ui64>& srcPartitionIdxs,
            const TTableInfo::TCPtr tableInfo,
            TTxState& op,
            const TChannelsBindings& channels,
            TString& errStr,
            TOperationContext& context)
    {
        Y_UNUSED(errStr);

        // 1 source shard is split/merged into 1 shard
        Y_ABORT_UNLESS(srcPartitionIdxs.size() == 1);
        Y_ABORT_UNLESS(info.SplitBoundarySize() == 0);

        TString firstRangeBegin;
        if (srcPartitionIdxs[0] != 0) {
            // Take the end of previous shard
            firstRangeBegin = tableInfo->GetPartitions()[srcPartitionIdxs[0]-1].EndOfRange;
        } else {
            TVector<TCell> firstKey;
            ui32 keyColCount = 0;
            for (const auto& col : tableInfo->Columns) {
                if (col.second.IsKey()) {
                    ++keyColCount;
                }
            }
            // Or start from (NULL, NULL, .., NULL)
            firstKey.resize(keyColCount);
            firstRangeBegin = TSerializedCellVec::Serialize(firstKey);
        }

        op.SplitDescription = std::make_shared<NKikimrTxDataShard::TSplitMergeDescription>();
        // Fill src shards
        TString prevRangeEnd = firstRangeBegin;
        for (ui64 pi : srcPartitionIdxs) {
            auto* srcRange = op.SplitDescription->AddSourceRanges();
            auto shardIdx = tableInfo->GetPartitions()[pi].ShardIdx;
            srcRange->SetShardIdx(ui64(shardIdx.GetLocalId()));
            srcRange->SetTabletID(ui64(context.SS->ShardInfos[shardIdx].TabletID));
            srcRange->SetKeyRangeBegin(prevRangeEnd);
            TString rangeEnd = tableInfo->GetPartitions()[pi].EndOfRange;
            srcRange->SetKeyRangeEnd(rangeEnd);
            prevRangeEnd = rangeEnd;
        }

        // Fill dst shard
        TShardInfo datashardInfo = TShardInfo::DataShardInfo(txId, pathId);
        datashardInfo.BindedChannels = channels;

        auto idx = context.SS->RegisterShardInfo(datashardInfo);

        ui64 lastSrcPartition = srcPartitionIdxs.back();
        TString lastRangeEnd = tableInfo->GetPartitions()[lastSrcPartition].EndOfRange;

        TTxState::TShardOperation dstShardOp(idx, ETabletType::DataShard, TTxState::CreateParts);
        dstShardOp.RangeEnd = lastRangeEnd;
        op.Shards.push_back(dstShardOp);

        auto* dstRange = op.SplitDescription->AddDestinationRanges();
        dstRange->SetShardIdx(ui64(idx.GetLocalId()));
        dstRange->SetKeyRangeBegin(firstRangeBegin);
        dstRange->SetKeyRangeEnd(lastRangeEnd);

        return true;
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& info = Transaction.GetSplitMergeTablePartitions();
        const ui64 dstCount = info.SplitBoundarySize() + 1;
        const ui64 srcCount = info.SourceTabletIdSize();

        TPathId pathId = InvalidPathId;
        if (info.HasTableOwnerId()) {
            pathId = TPathId(TOwnerId(info.GetTableOwnerId()),
                             TLocalPathId(info.GetTableLocalId()));
        } else if (info.HasTableLocalId()) {
            pathId = context.SS->MakeLocalId(TLocalPathId(info.GetTableLocalId()));
        }

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TSplitMerge Propose"
                         << ", tableStr: " << info.GetTablePath()
                         << ", tableId: " << pathId
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));
        TString errStr;

        if (!info.HasTablePath() && !info.HasTableLocalId()) {
            errStr = "Neither table name nor pathId in SplitMergeInfo";
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        TPath path = pathId
            ? TPath::Init(pathId, context.SS)
            : TPath::Resolve(info.GetTablePath(), context.SS);
        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTable()
                .NotUnderOperation();

            if (checks) {
                if (dstCount >= srcCount) { //allow over commit for merge
                    checks
                        .ShardsLimit(dstCount)
                        .PathShardsLimit(dstCount);
                }
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!context.SS->CheckLocks(path.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        Y_ABORT_UNLESS(context.SS->Tables.contains(path.Base()->PathId));
        TTableInfo::TCPtr tableInfo = context.SS->Tables.at(path.Base()->PathId);
        Y_ABORT_UNLESS(tableInfo);

        if (tableInfo->IsBackup) {
            TString errMsg = TStringBuilder()
                << "cannot split/merge backup table " << info.GetTablePath();
            result->SetError(NKikimrScheme::StatusInvalidParameter, errMsg);
            return result;
        }

        if (tableInfo->IsRestore) {
            TString errMsg = TStringBuilder()
                << "cannot split/merge restore table " << info.GetTablePath();
            result->SetError(NKikimrScheme::StatusInvalidParameter, errMsg);
            return result;
        }

        const THashMap<TShardIdx, ui64>& shardIdx2partition = tableInfo->GetShard2PartitionIdx();

        TVector<ui64> srcPartitionIdxs;
        i64 totalSrcPartCount = 0;
        for (ui32 si = 0; si < info.SourceTabletIdSize(); ++si) {
            auto srcTabletId = TTabletId(info.GetSourceTabletId(si));
            auto srcShardIdx = context.SS->GetShardIdx(srcTabletId);
            if (!srcShardIdx) {
                TString errMsg = TStringBuilder() << "Unknown SourceTabletId: " << srcTabletId;
                result->SetError(NKikimrScheme::StatusInvalidParameter, errMsg);
                return result;
            }

            if (!context.SS->ShardInfos.contains(srcShardIdx)) {
                TString errMsg = TStringBuilder()
                    << "shard doesn't present at schemeshard at all"
                    << ", tablet: " << srcTabletId
                    << ", srcShardIdx: " << srcShardIdx
                    << ", pathId: " << path.Base()->PathId;
                result->SetError(NKikimrScheme::StatusInvalidParameter, errMsg);
                return result;
            }

            if (!shardIdx2partition.contains(srcShardIdx)) {
                TString errMsg = TStringBuilder()
                    << "shard doesn't present at schemeshard at table"
                    << ", tablet: " << srcTabletId
                    << ", srcShardIdx: " << srcShardIdx
                    << ", pathId: " << path.Base()->PathId;
                result->SetError(NKikimrScheme::StatusInvalidParameter, errMsg);
                return result;
            }

            if (context.SS->ShardInfos.FindPtr(srcShardIdx)->PathId != path.Base()->PathId || !shardIdx2partition.contains(srcShardIdx)) {
                TString errMsg = TStringBuilder() << "TabletId " << srcTabletId << " is not a partition of table " << info.GetTablePath();
                result->SetError(NKikimrScheme::StatusInvalidParameter, errMsg);
                return result;
            }


            if (context.SS->ShardIsUnderSplitMergeOp(srcShardIdx)) {
                TString errMsg = TStringBuilder() << "TabletId " << srcTabletId << " is already in process of split";
                result->SetError(NKikimrScheme::StatusMultipleModifications, errMsg);
                return result;
            }

            if (context.SS->SplitSettings.SplitMergePartCountLimit != -1) {
                const auto* stats = tableInfo->GetStats().PartitionStats.FindPtr(srcShardIdx);
                if (!stats || stats->ShardState != NKikimrTxDataShard::Ready) {
                    TString errMsg = TStringBuilder() << "Src TabletId " << srcTabletId << " is not in Ready state";
                    result->SetError(NKikimrScheme::StatusNotAvailable, errMsg);
                    return result;
                }

                totalSrcPartCount += stats->PartCount;
            }

            auto pi = shardIdx2partition.at(srcShardIdx);
            Y_VERIFY_S(pi < tableInfo->GetPartitions().size(), "pi: " << pi << " partitions.size: " << tableInfo->GetPartitions().size());
            srcPartitionIdxs.push_back(pi);
        }

        if (context.SS->SplitSettings.SplitMergePartCountLimit != -1 &&
            totalSrcPartCount >= context.SS->SplitSettings.SplitMergePartCountLimit)
        {
            result->SetError(NKikimrScheme::StatusNotAvailable,
                             Sprintf("Split/Merge operation involves too many parts: %" PRIu64, totalSrcPartCount));

            LOG_CRIT_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Cannot start split/merge operation"
                           << " for table \"" << info.GetTablePath() << "\" (id " << path.Base()->PathId << ")"
                           << " at tablet " << context.SS->TabletID()
                           << " because the operation involves too many parts: " << totalSrcPartCount);
            return result;
        }

        if (srcPartitionIdxs.empty()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, TStringBuilder() << "No source partitions specified for split/merge TxId " << OperationId.GetTxId());
            return result;
        }

        TChannelsBindings channelsBinding;

        bool storePerShardConfig = false;
        NKikimrSchemeOp::TPartitionConfig perShardConfig;

        if (context.SS->IsStorageConfigLogic(tableInfo)) {
            TVector<TStorageRoom> storageRooms;
            THashMap<ui32, ui32> familyRooms;
            storageRooms.emplace_back(0);

            if (!context.SS->GetBindingsRooms(path.GetPathIdForDomain(), tableInfo->PartitionConfig(), storageRooms, familyRooms, channelsBinding, errStr)) {
                errStr = TString("database doesn't have required storage pools to create tablet with storage config, details: ") + errStr;
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
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
        } else if (context.SS->IsCompatibleChannelProfileLogic(path.GetPathIdForDomain(), tableInfo)) {
            if (!context.SS->GetChannelsBindings(path.GetPathIdForDomain(), tableInfo, channelsBinding, errStr)) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }
        }

        TTxState op;

        op.TxType = TTxState::TxSplitTablePartition;
        op.TargetPathId = path.Base()->PathId;
        op.State = TTxState::CreateParts;

        // Fill Src shards for tx
        for (ui64 pi : srcPartitionIdxs) {
            auto srcShardIdx = tableInfo->GetPartitions()[pi].ShardIdx;
            op.Shards.emplace_back(srcShardIdx, ETabletType::DataShard, TTxState::TransferData);
        }

        if (srcPartitionIdxs.size() == 1 && dstCount > 1) {
            // This is Split operation, allocate new shards for split Dsts
            if (!AllocateDstForSplit(info, OperationId.GetTxId(), path.Base()->PathId, srcPartitionIdxs[0], tableInfo, op, channelsBinding, errStr, context)) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }
        } else if (dstCount == 1 && srcPartitionIdxs.size() > 1) {
            // This is merge, allocate 1 Dst shard
            if (!AllocateDstForMerge(info, OperationId.GetTxId(), path.Base()->PathId, srcPartitionIdxs, tableInfo, op, channelsBinding, errStr, context)) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }
        } else if (srcPartitionIdxs.size() == 1 && dstCount == 1 && info.GetAllowOneToOneSplitMerge()) {
            // This is one-to-one split/merge
            if (!AllocateDstForOneToOne(info, OperationId.GetTxId(), path.Base()->PathId, srcPartitionIdxs, tableInfo, op, channelsBinding, errStr, context)) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }
        } else {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Invalid request: only 1->N or N->1 are supported");
            return result;
        }

        ///////////
        /// Accept operation
        ///

        auto guard = context.DbGuard();
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabDomain(context.SS, path.GetPathIdForDomain());
        context.MemChanges.GrabPath(context.SS, path->PathId);
        context.MemChanges.GrabTable(context.SS, path->PathId);

        context.DbChanges.PersistTxState(OperationId);
        for (const auto& shard : op.Shards) {
            if (shard.Operation == TTxState::CreateParts) {
                context.MemChanges.GrabNewShard(context.SS, shard.Idx);
            } else {
                context.MemChanges.GrabShard(context.SS, shard.Idx);
            }
            context.DbChanges.PersistShard(shard.Idx);
        }

        TTableInfo::TPtr mutableTableInfo = context.SS->Tables.at(path->PathId);

        mutableTableInfo->RegisterSplitMergeOp(OperationId, op);
        context.SS->CreateTx(OperationId, TTxState::TxSplitTablePartition, path->PathId) = op;
        context.OnComplete.ActivateTx(OperationId);

        for (const auto& shard : op.Shards) {
            Y_ABORT_UNLESS(shard.Operation == TTxState::TransferData || shard.Operation == TTxState::CreateParts);
            // Add new (DST) shards to the list of all shards and update LastTxId for the old (SRC) shards
            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.Idx));
            TShardInfo& shardInfo = context.SS->ShardInfos[shard.Idx];
            shardInfo.CurrentTxId = OperationId.GetTxId();

            if (shard.Operation == TTxState::CreateParts) {
                if (storePerShardConfig) {
                    mutableTableInfo->PerShardPartitionConfig[shard.Idx].CopyFrom(perShardConfig);
                }
            }
        }

        path.DomainInfo()->AddInternalShards(op, context.SS); //allow over commit for merge
        path->IncShardsInside(dstCount);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TSplitMerge");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TSplitMerge AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        TPathId pathId = txState->TargetPathId;
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        Y_ABORT_UNLESS(path);

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        TTableInfo::TPtr tableInfo = context.SS->Tables.at(pathId);
        Y_ABORT_UNLESS(tableInfo);
        tableInfo->AbortSplitMergeOp(OperationId);

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateSplitMerge(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TSplitMerge>(id, tx);
}

ISubOperation::TPtr CreateSplitMerge(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TSplitMerge>(id, state);
}

}
