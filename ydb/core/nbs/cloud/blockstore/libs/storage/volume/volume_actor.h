#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;
using namespace NKikimr;

class TVolumeActor
    : public TActorBootstrapped<TVolumeActor>
    , NKikimr::NTabletFlatExecutor::TTabletExecutedFlat
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

public:
    TVolumeActor(const TActorId& tablet, NKikimr::TTabletStorageInfo* info);
    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev,
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

    void HandleUpdateVolumeConfigResponse(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ReportTabletState(const TActorContext& ctx);
};

}   // namespace NYdb::NBS::NStorage
