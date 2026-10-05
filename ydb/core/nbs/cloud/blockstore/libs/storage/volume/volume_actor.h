#pragma once

#include "volume_counters.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/api/volume.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;
using namespace NKikimr;

class TVolumeActorTestAccessor;

// NBS 2.0 volume tablet. It stores the single partition tablet id from
// UpdateVolumeConfig in the local database, so a restart can still reach
// that partition. Pipes open only after that id is loaded.
class TVolumeActor
    : public TActorBootstrapped<TVolumeActor>
    , public NBlockStore::NStorage::TTabletBase<TVolumeActor>
{
    enum EState
    {
        STATE_BOOT,
        STATE_INIT,
        STATE_WORK,
        STATE_ZOMBIE,
        STATE_MAX,
    };

    struct TUpdateVolumeConfigRequest
    {
        TRequestInfoPtr RequestInfo;
        ui64 TxId = 0;
        THashMap<ui64, TActorId> PartitionPipes;   // tabletId -> pipeClientId
        THashSet<ui64> PendingPartitions;          // tabletId
    };

    THashMap<ui64, TUpdateVolumeConfigRequest>
        UpdateVolumeConfigRequests;   // txId -> request

    // Tablet id of the single partition, as last received in
    // UpdateVolumeConfig. 0 means not known yet: the volume has not
    // received any UpdateVolumeConfig.
    ui64 PartitionTabletId = 0;

    friend class TVolumeActorTestAccessor;

public:
    TVolumeActor(const TActorId& tablet, NKikimr::TTabletStorageInfo* info);
    void Bootstrap(const TActorContext& ctx);

    static constexpr ui32 LogComponent = NKikimrServices::NBS_VOLUME;
    using TCounters = TVolumeCounters;

private:
    STFUNC(StateWork);

    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(
        TEvTablet::TEvTabletDead::TPtr& ev,
        const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;

    void HandleServerConnected(
        const NKikimr::TEvTabletPipe::TEvServerConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDisconnected(
        const NKikimr::TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDestroyed(
        const NKikimr::TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVolumeConfig(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Sends UpdateVolumeConfig to its partition. Called once the
    // partition tablet id is durable.
    void ForwardUpdateVolumeConfig(
        const NActors::TActorContext& ctx,
        const NKikimrBlockStore::TUpdateVolumeConfig& record);

    void HandleUpdateVolumeConfigResponse(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    // TODO: NBS-7763 answer with the volume state; nbsd gets an empty OK.
    void HandleStatVolume(
        const NNbs1CompatApi::NBlockStore::TEvService::TEvStatVolumeRequest::
            TPtr& ev,
        const NActors::TActorContext& ctx);

    // TODO: NBS-7763 wait for the partition; nbsd gets an empty OK.
    void HandleWaitReady(
        const NNbs1CompatApi::NBlockStore::TEvVolume::TEvWaitReadyRequest::TPtr&
            ev,
        const NActors::TActorContext& ctx);

    void ReportTabletState(const TActorContext& ctx);

    BLOCKSTORE_VOLUME_TRANSACTIONS(BLOCKSTORE_IMPLEMENT_TRANSACTION, TTxVolume)
};

}   // namespace NYdb::NBS::NStorage
