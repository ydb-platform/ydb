#include "schemeshard_impl.h"

#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/schemeshard/schemeshard__tenant_shred_manager.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxDeleteTabletReply : public TSchemeShard::TRwTxBase {
    NKikimrProto::EReplyStatus Status = NKikimrProto::EReplyStatus::UNKNOWN;
    TShardIdx ShardIdx = InvalidShardIdx;
    TTabletId TabletId = InvalidTabletId;
    TTabletId HiveId = InvalidTabletId;
    TTabletId ForwardToHiveId = InvalidTabletId;

    TTxDeleteTabletReply(TSelf* self, TEvHive::TEvDeleteTabletReply::TPtr& ev)
        : TRwTxBase(self)
    {
        const auto& record = ev->Get()->Record;
        Status = record.GetStatus();
        HiveId = TTabletId(record.GetOrigin());

        Y_ABORT_UNLESS(record.HasShardOwnerId());
        Y_ABORT_UNLESS(record.ShardLocalIdxSize() == 1);
        ShardIdx = TShardIdx(record.GetShardOwnerId(), record.GetShardLocalIdx(0));

        if (record.HasForwardRequest()) {
            ForwardToHiveId = TTabletId(record.GetForwardRequest().GetHiveTabletId());
        }
    }

    TTxType GetTxType() const override { return TXTYPE_FREE_TABLET_RESULT; }

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        if (Status != NKikimrProto::OK) {
            if (Status == NKikimrProto::INVALID_OWNER) {
                YDB_LOG_CTX_WARN(ctx, "Got DeleteTabletReply with Forward response from Hive to Hive shardIdx",
                    {"#_HiveId", HiveId},
                    {"#_ForwardToHiveId", ForwardToHiveId},
                    {"#_ShardIdx", ShardIdx});
                Y_ABORT_UNLESS(ForwardToHiveId);
                Self->ShardDeleter.RedirectDeleteRequest(HiveId, ForwardToHiveId, ShardIdx, Self->ShardInfos, ctx);
                return;
            }
            // WTF could happen that hive failed to delete the freaking tablet?
            YDB_LOG_CTX_ALERT(ctx, "Got DeleteTabletReply from Hive shardIdx status",
                {"#_HiveId", HiveId},
                {"#_ShardIdx", ShardIdx},
                {"#_Status", Status});
            return;
        }

        // "Forget" the deleted shard
        if (Self->ShardInfos.contains(ShardIdx)) {
            auto tabletType = Self->ShardInfos[ShardIdx].TabletType;
            switch (tabletType) {
            case ETabletType::DataShard:
                Self->TabletCounters->Simple()[COUNTER_TABLE_SHARD_INACTIVE_COUNT].Sub(1);
                break;
            case ETabletType::Coordinator:
                Self->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_COORDINATOR_COUNT].Sub(1);
                break;
            case ETabletType::Mediator:
                Self->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_MEDIATOR_COUNT].Sub(1);
                break;
            case ETabletType::SchemeShard:
                Self->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_SCHEME_SHARD_COUNT].Sub(1);
                break;
            case ETabletType::Hive:
                Self->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_HIVE_COUNT].Sub(1);
                break;
            case ETabletType::BlockStoreVolume:
                Self->TabletCounters->Simple()[COUNTER_BLOCKSTORE_VOLUME_SHARD_COUNT].Sub(1);
                break;
            case ETabletType::BlockStorePartition:
                Self->TabletCounters->Simple()[COUNTER_BLOCKSTORE_PARTITION_SHARD_COUNT].Sub(1);
                break;
            case ETabletType::BlockStorePartition2:
                Self->TabletCounters->Simple()[COUNTER_BLOCKSTORE_PARTITION2_SHARD_COUNT].Sub(1);
                break;
            case ETabletType::BlockStoreVolumeDirect:
                Self->TabletCounters->Simple()[COUNTER_BLOCKSTORE_VOLUME_DIRECT_SHARD_COUNT].Sub(1);
                break;
            case ETabletType::BlockStorePartitionDirect:
                Self->TabletCounters->Simple()[COUNTER_BLOCKSTORE_PARTITION_DIRECT_SHARD_COUNT].Sub(1);
                break;
            case ETabletType::FileStore:
                Self->TabletCounters->Simple()[COUNTER_FILESTORE_SHARD_COUNT].Sub(1);
                break;
            case ETabletType::Kesus:
                Self->TabletCounters->Simple()[COUNTER_KESUS_SHARD_COUNT].Sub(1);
                break;
            case ETabletType::KeyValue:
                Self->TabletCounters->Simple()[COUNTER_SOLOMON_PARTITIONS_COUNT].Sub(1);
                break;
            case ETabletType::PersQueue:
                Self->TabletCounters->Simple()[COUNTER_PQ_SHARD_COUNT].Sub(1);
                break;
            case ETabletType::PersQueueReadBalancer:
                Self->TabletCounters->Simple()[COUNTER_PQ_RB_SHARD_COUNT].Sub(1);
                break;
            case ETabletType::SysViewProcessor:
                Self->TabletCounters->Simple()[COUNTER_SYS_VIEW_PROCESSOR_COUNT].Sub(1);
                break;
            case ETabletType::ColumnShard:
                Self->TabletCounters->Simple()[COUNTER_COLUMN_SHARDS].Sub(1);
                break;
            case ETabletType::SequenceShard:
                Self->TabletCounters->Simple()[COUNTER_SEQUENCESHARD_COUNT].Sub(1);
                break;
            case ETabletType::ReplicationController:
                Self->TabletCounters->Simple()[COUNTER_REPLICATION_CONTROLLER_COUNT].Sub(1);
                break;
            case ETabletType::BlobDepot:
                Self->TabletCounters->Simple()[COUNTER_BLOB_DEPOT_COUNT].Sub(1);
                break;
            case ETabletType::StatisticsAggregator:
                Self->TabletCounters->Simple()[COUNTER_STATISTICS_AGGREGATOR_COUNT].Sub(1);
                break;
            case ETabletType::GraphShard:
                Self->TabletCounters->Simple()[COUNTER_GRAPHSHARD_COUNT].Sub(1);
                break;
            case ETabletType::BackupController:
                Self->TabletCounters->Simple()[COUNTER_BACKUP_CONTROLLER_TABLET_COUNT].Sub(1);
                break;
            default:
                Y_FAIL_S("Unknown TabletType"
                         << ", ShardIdx " << ShardIdx
                         << ", (ui32)TabletType" << (ui32)tabletType);
            };

            auto& shardInfo = Self->ShardInfos.at(ShardIdx);

            auto pathId = shardInfo.PathId;
            auto it = Self->Tables.find(pathId);
            if (it != Self->Tables.end()) {
                it->second->PerShardPartitionConfig.erase(ShardIdx);
            }

            NIceDb::TNiceDb db(txc.DB);
            Self->PersistShardDeleted(db, ShardIdx, shardInfo.BindedChannels);

            Y_VERIFY_S(Self->PathsById.contains(pathId), "pathid: " << pathId);
            auto path = Self->PathsById.at(pathId);
            path->DecShardsInside();

            auto domain = Self->ResolveDomainInfo(path);
            domain->RemoveInternalShard(ShardIdx, Self);
            switch (tabletType) {
            case ETabletType::SequenceShard:
                domain->RemoveSequenceShard(ShardIdx);
                break;
            default:
                break;
            }

            TabletId = shardInfo.TabletID;
            Self->TabletIdToShardIdx[TabletId] = ShardIdx;

            Self->ShardInfos.erase(ShardIdx);

            Self->DecrementPathDbRefCount(pathId, "shard deleted");

            // This is for tests, so it's kinda ok to reply from execute
            auto itSubscribers = Self->ShardDeletionSubscribers.find(ShardIdx);
            if (itSubscribers != Self->ShardDeletionSubscribers.end()) {
                for (const auto& subscriber : itSubscribers->second) {
                    ctx.Send(subscriber, new TEvPrivate::TEvNotifyShardDeleted(ShardIdx));
                }
                Self->ShardDeletionSubscribers.erase(itSubscribers);
            }
        } else {
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistUnknownShardDeleted(db, ShardIdx);
        }

        Self->OnShardRemoved(ShardIdx);
    }

    void DoComplete(const TActorContext &ctx) override {
        if (Status == NKikimrProto::OK) {
            YDB_LOG_CTX_DEBUG(ctx, "Deleted shardIdx",
                {"#_ShardIdx", ShardIdx});

            Self->ShardDeleter.ShardDeleted(ShardIdx, ctx);

            if (TabletId != InvalidTabletId) {
                YDB_LOG_CTX_DEBUG(ctx, "Close pipe to deleted shardIdx tabletId",
                    {"#_ShardIdx", ShardIdx},
                    {"#_TabletId", TabletId});
                Self->PipeClientCache->ForceClose(ctx, ui64(TabletId));
            }
            if (Self->EnableShred && Self->TenantShredManager->GetStatus() == EShredStatus::IN_PROGRESS) {
                Self->Execute(Self->CreateTxCancelShredShards({ShardIdx}));
            }
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxDeleteTabletReply(TEvHive::TEvDeleteTabletReply::TPtr& ev) {
    return new TTxDeleteTabletReply(this, ev);
}

void TSchemeShard::Handle(TEvPrivate::TEvSubscribeToShardDeletion::TPtr& ev, const TActorContext& ctx) {
    auto shardIdx = ev->Get()->ShardIdx;
    if (ShardInfos.contains(shardIdx)) {
        ShardDeletionSubscribers[shardIdx].push_back(ev->Sender);
        return;
    }

    // This is for tests, so it's kinda ok to reply from handler
    ctx.Send(ev->Sender, new TEvPrivate::TEvNotifyShardDeleted(shardIdx));
}

}}
