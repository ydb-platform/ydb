#pragma once

#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>
#include <ydb/core/blockstore/core/blockstore.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TPartitionActor
    : public NActors::TActor<TPartitionActor>
    , public NKikimr::NTabletFlatExecutor::TTabletExecutedFlat
{
    enum EState
    {
        STATE_BOOT,
        STATE_INIT,
        STATE_WORK,
        STATE_ZOMBIE,
        STATE_MAX,
    };

private:
    NYdb::NBS::NProto::TStorageServiceConfig StorageConfig;
    NKikimrBlockStore::TVolumeConfig VolumeConfig;

    NActors::TActorId BSControllerPipeClient;

    NActors::TActorId LoadActorAdapter;


public:
    TPartitionActor(const NActors::TActorId& tablet, NKikimr::TTabletStorageInfo* info);

private:
    void StateInit(TAutoPtr<NActors::IEventHandle>& ev);
    STFUNC(StateWork);

    void OnDetach(const NActors::TActorContext& ctx) override;
    void OnTabletDead(NKikimr::TEvTablet::TEvTabletDead::TPtr& ev, const NActors::TActorContext& ctx) override;
    void OnActivateExecutor(const NActors::TActorContext& ctx) override;
    void DefaultSignalTabletActive(const NActors::TActorContext& ctx) override;

    void HandleServerConnected(
        const NKikimr::TEvTabletPipe::TEvServerConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDisconnected(
        const NKikimr::TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDestroyed(
        const NKikimr::TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ReportTabletState(const NActors::TActorContext& ctx);

    void CreateBSControllerPipeClient(const NActors::TActorContext& ctx);

    void AllocateDDiskBlockGroup(const NActors::TActorContext& ctx);

    void HandleControllerAllocateDDiskBlockGroupResult(
        const NKikimr::TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetLoadActorAdapterActorId(
        const NYdb::NBS::NBlockStore::TEvService::TEvGetLoadActorAdapterActorIdRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVolumeConfig(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
