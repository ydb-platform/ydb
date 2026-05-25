#include "schemeshard_shard_deleter.h"

#include <ydb/core/mind/hive/hive.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

void TShardDeleter::Shutdown(const NActors::TActorContext &ctx) {
    for (auto& info : PerHiveDeletions) {
        NTabletPipe::CloseClient(ctx, info.second.PipeToHive);
    }
    PerHiveDeletions.clear();
}

void TShardDeleter::SendDeleteRequests(TTabletId hiveTabletId,
        const THashSet<TShardIdx> &shardsToDelete,
        const THashMap<NKikimr::NSchemeShard::TShardIdx,
        NKikimr::NSchemeShard::TShardInfo>& shardsInfos,
        const NActors::TActorContext &ctx
    ) {
    YDB_LOG_CTX_DEBUG(ctx, "SendDeleteRequests, shardsToDelete, to hive, at schemeshard",
        {"size", shardsToDelete.size()},
        {"hiveTabletId", hiveTabletId},
        {"MyTabletID", MyTabletID});

    if (shardsToDelete.empty())
        return;

    TPerHiveDeletions& info = PerHiveDeletions[hiveTabletId];
    if (!info.PipeToHive) {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = HivePipeRetryPolicy;
        info.PipeToHive = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, ui64(hiveTabletId), clientConfig));
    }
    info.ShardsToDelete.insert(shardsToDelete.begin(), shardsToDelete.end());

    for (auto shardIdx : shardsToDelete) {
        ShardHive[shardIdx] = hiveTabletId;
        TAutoPtr<TEvHive::TEvDeleteTablet> event = new TEvHive::TEvDeleteTablet(shardIdx.GetOwnerId(), ui64(shardIdx.GetLocalId()), /* TxId_Deprecated */ 0);
        auto itShard = shardsInfos.find(shardIdx);
        if (itShard != shardsInfos.end()) {
            TTabletId shardTabletId = itShard->second.TabletID;
            if (shardTabletId) {
                event->Record.AddTabletID(ui64(shardTabletId));
            }
        }

        Y_ABORT_UNLESS(shardIdx);

        YDB_LOG_CTX_DEBUG(ctx, "Free shard hive at ss",
            {"shardIdx", shardIdx},
            {"hiveTabletId", hiveTabletId},
            {"MyTabletID", MyTabletID});

        NTabletPipe::SendData(ctx, info.PipeToHive, event.Release());
    }
}

void TShardDeleter::ResendDeleteRequests(TTabletId hiveTabletId, const THashMap<TShardIdx, TShardInfo>& shardsInfos, const NActors::TActorContext &ctx) {
    YDB_LOG_CTX_NOTICE(ctx, "Resending tablet deletion requests from to",
        {"MyTabletID", MyTabletID},
        {"hiveTabletId", hiveTabletId});

    auto itPerHive = PerHiveDeletions.find(hiveTabletId);
    if (itPerHive == PerHiveDeletions.end()) {
        YDB_LOG_CTX_WARN(ctx, "Hive not found for delete requests",
            {"hiveTabletId", hiveTabletId});
        return;
    }

    THashSet<TShardIdx> toResend(std::move(itPerHive->second.ShardsToDelete));
    PerHiveDeletions.erase(itPerHive);

    SendDeleteRequests(hiveTabletId, toResend, shardsInfos, ctx);
}

void TShardDeleter::ResendDeleteRequest(TTabletId hiveTabletId,
                                        const THashMap<TShardIdx, TShardInfo>& shardsInfos,
                                        TShardIdx shardIdx,
                                        const NActors::TActorContext &ctx) {
    YDB_LOG_CTX_NOTICE(ctx, "Resending tablet deletion request from to",
        {"MyTabletID", MyTabletID},
        {"hiveTabletId", hiveTabletId});

    auto itPerHive = PerHiveDeletions.find(hiveTabletId);
    if (itPerHive == PerHiveDeletions.end())
        return;

    auto itShardIdx = itPerHive->second.ShardsToDelete.find(shardIdx);
    if (itShardIdx != itPerHive->second.ShardsToDelete.end()) {
        THashSet<TShardIdx> toResend({shardIdx});
        itPerHive->second.ShardsToDelete.erase(itShardIdx);
        if (itPerHive->second.ShardsToDelete.empty()) {
            PerHiveDeletions.erase(itPerHive);
        }
        SendDeleteRequests(hiveTabletId, toResend, shardsInfos, ctx);
    } else {
        YDB_LOG_CTX_WARN(ctx, "Shard not found for delete request for Hive",
            {"shardIdx", shardIdx},
            {"hiveTabletId", hiveTabletId});
    }
}

void TShardDeleter::RedirectDeleteRequest(TTabletId hiveFromTabletId,
                                          TTabletId hiveToTabletId,
                                          TShardIdx shardIdx,
                                          const THashMap<TShardIdx, TShardInfo>& shardsInfos,
                                          const NActors::TActorContext &ctx) {
    YDB_LOG_CTX_NOTICE(ctx, "Redirecting tablet deletion requests from to",
        {"hiveFromTabletId", hiveFromTabletId},
        {"hiveToTabletId", hiveToTabletId});
    auto itFromHive = PerHiveDeletions.find(hiveFromTabletId);
    if (itFromHive != PerHiveDeletions.end()) {
        auto& toHive(PerHiveDeletions[hiveToTabletId]);
        auto itShardIdx = itFromHive->second.ShardsToDelete.find(shardIdx);
        if (itShardIdx != itFromHive->second.ShardsToDelete.end()) {
            toHive.ShardsToDelete.emplace(*itShardIdx);
            itFromHive->second.ShardsToDelete.erase(itShardIdx);
        } else {
            YDB_LOG_CTX_WARN(ctx, "Shard not found for delete request for Hive",
                {"shardIdx", shardIdx},
                {"hiveFromTabletId", hiveFromTabletId});
        }
        if (itFromHive->second.ShardsToDelete.empty()) {
            PerHiveDeletions.erase(itFromHive);
        }
    }

    ResendDeleteRequest(hiveToTabletId, shardsInfos, shardIdx, ctx);
}

void TShardDeleter::ShardDeleted(TShardIdx shardIdx, const NActors::TActorContext &ctx) {
    if (!ShardHive.contains(shardIdx))
        return;

    TTabletId hiveTabletId = ShardHive[shardIdx];
    ShardHive.erase(shardIdx);
    PerHiveDeletions[hiveTabletId].ShardsToDelete.erase(shardIdx);

    if (PerHiveDeletions[hiveTabletId].ShardsToDelete.empty()) {
        NTabletPipe::CloseClient(ctx, PerHiveDeletions[hiveTabletId].PipeToHive);
        PerHiveDeletions.erase(hiveTabletId);
    }
}

bool TShardDeleter::Has(TTabletId hiveTabletId, TActorId pipeClientActorId) const {
    return PerHiveDeletions.contains(hiveTabletId) && PerHiveDeletions.at(hiveTabletId).PipeToHive == pipeClientActorId;
}

bool TShardDeleter::Has(TShardIdx shardIdx) const {
    return ShardHive.contains(shardIdx);
}

bool TShardDeleter::Empty() const {
    return PerHiveDeletions.empty();
}

}  // namespace
