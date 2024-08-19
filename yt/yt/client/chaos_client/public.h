#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

using TReplicationCardId = NObjectClient::TObjectId;
using TReplicationCardCollocationId = NObjectClient::TObjectId;
using TReplicaId = NObjectClient::TObjectId;
using TReplicationEra = ui64;
using TReplicaIdIndex = ui16;

constexpr TReplicationEra InvalidReplicationEra = static_cast<TReplicationEra>(-1);
constexpr TReplicationEra InitialReplicationEra = 0;

constexpr int MaxReplicasPerReplicationCard = 128;

DECLARE_REFCOUNTED_STRUCT(TReplicationCard)

DECLARE_REFCOUNTED_STRUCT(IReplicationCardCache)
DECLARE_REFCOUNTED_CLASS(TReplicationCardCacheConfig)

struct TReplicationProgress;
struct TReplicaHistoryItem;
struct TReplicaInfo;
struct TReplicationCardFecthOptions;

YT_DEFINE_ERROR_ENUM(
    ((ReplicationCardNotKnown)         (3200))
    ((ReplicationCardMigrated)         (3201))
    ((ChaosCellSuspended)              (3202))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
