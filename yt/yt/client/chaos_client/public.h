#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

using TChaosObjectId = NObjectClient::TObjectId;
using TReplicationCardId = TChaosObjectId;
using TReplicationCardCollocationId = TChaosObjectId;
using TChaosLeaseId = TChaosObjectId;
using TReplicaId = NObjectClient::TObjectId;
using TReplicationEra = ui64;
using TReplicaIdIndex = ui16;

constexpr TReplicationEra InvalidReplicationEra = static_cast<TReplicationEra>(-1);
constexpr TReplicationEra InitialReplicationEra = 0;

constexpr int MaxReplicasPerReplicationCard = 128;

DECLARE_REFCOUNTED_STRUCT(TReplicationCard)

DECLARE_REFCOUNTED_STRUCT(IReplicationCardCache)
DECLARE_REFCOUNTED_STRUCT(TChaosCacheChannelConfig)
DECLARE_REFCOUNTED_STRUCT(TReplicationCardCacheConfig)
DECLARE_REFCOUNTED_STRUCT(TReplicationCardCacheDynamicConfig)

struct TReplicationProgress;
struct TReplicaHistoryItem;
struct TReplicaInfo;
struct TReplicationCardFetchOptions;

YT_DEFINE_ERROR_ENUM(
    ((ReplicationCardNotKnown)           (3200))
    ((ReplicationCardMigrated)           (3201))
    ((ChaosCellSuspended)                (3202))
    ((ReplicationCollocationNotKnown)    (3203))
    ((ReplicationCollocationIsMigrating) (3204))
    ((ShortcutNotFound)                  (3206))
    ((ShortcutHasDifferentEra)           (3207))
    ((ShortcutRevoked)                   (3208))
    ((ChaosCellIsNotEnabled)             (3209))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
