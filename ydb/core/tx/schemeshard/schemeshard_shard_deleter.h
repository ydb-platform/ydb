#pragma once
#include "schemeshard_identificators.h"  // for TShardIdx
#include "schemeshard_info_types.h"  // for TShardInfo

#include <ydb/core/base/tablet_pipe.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>


namespace NActors {
    struct TActorContext;
}

namespace NKikimr::NSchemeShard {

class TShardDeleter {
    struct TPerHiveDeletions {
        TActorId PipeToHive;
        THashSet<TShardIdx> ShardsToDelete;
    };

    ui64 MyTabletID;
    // Hive TabletID -> non-acked deletions
    THashMap<TTabletId, TPerHiveDeletions> PerHiveDeletions;
    // Tablet -> Hive TabletID
    THashMap<TShardIdx, TTabletId> ShardHive;
    NTabletPipe::TClientRetryPolicy HivePipeRetryPolicy;

public:
    explicit TShardDeleter(ui64 myTabletId)
        : MyTabletID(myTabletId)
        , HivePipeRetryPolicy({})
    {}

    TShardDeleter(const TShardDeleter&) = delete;
    TShardDeleter& operator=(const TShardDeleter&) = delete;

    void Shutdown(const TActorContext& ctx);
    void SendDeleteRequests(TTabletId hiveTabletId, const THashSet<TShardIdx>& shardsToDelete,
                            const THashMap<TShardIdx, TShardInfo>& shardsInfos, const TActorContext& ctx);
    void ResendDeleteRequests(TTabletId hiveTabletId,
                              const THashMap<TShardIdx, TShardInfo>& shardsInfos, const TActorContext& ctx);
    void ResendDeleteRequest(TTabletId hiveTabletId,
                             const THashMap<TShardIdx, TShardInfo>& shardsInfos, TShardIdx shardIdx, const TActorContext& ctx);
    void RedirectDeleteRequest(TTabletId hiveFromTabletId, TTabletId hiveToTabletId, TShardIdx shardIdx,
                               const THashMap<TShardIdx, TShardInfo>& shardsInfos, const TActorContext& ctx);
    void ShardDeleted(TShardIdx shardIdx, const TActorContext& ctx);
    bool Has(TTabletId hiveTabletId, TActorId pipeClientActorId) const;
    bool Has(TShardIdx shardIdx) const;
    bool Empty() const;
};

} // namespace NKikimr::NSchemeShard
