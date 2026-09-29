#pragma once

#include "volume_counters.h"
#include "volume_tx.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/api/volume.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

// The volume tablet of a direct (NBS 2.0) disk. It stores the partition tablet
// id in its local DB and forwards to the partition SchemeShard's
// UpdateVolumeConfig and the NBS 1.0 StatVolume and WaitReady requests that
// nbsd sends over a tablet pipe.
class TVolumeActor final
    : public NActors::TActor<TVolumeActor>
    , public NBlockStore::NStorage::TTabletBase<TVolumeActor>
{
public:
    static constexpr ui32 LogComponent = NKikimrServices::NBS_VOLUME;
    using TCounters = TVolumeCounters;

    TVolumeActor(
        const NActors::TActorId& tablet,
        NKikimr::TTabletStorageInfo* info);

private:
    using TNbs1Service = NNbs1CompatApi::NBlockStore::TEvService;
    using TNbs1Volume = NNbs1CompatApi::NBlockStore::TEvVolume;

    // The kind of an NBS 1.0 request forwarded to the partition.
    enum class EForwardedRequestKind
    {
        // TEvService::TEvStatVolumeRequest.
        StatVolume,
        // TEvVolume::TEvWaitReadyRequest.
        WaitReady,
    };

    // An UpdateVolumeConfig forwarded to the partition and not answered yet.
    struct TUpdateVolumeConfigRequest
    {
        TRequestInfoPtr RequestInfo;
        ui64 PartitionTabletId = 0;
        NActors::TActorId PartitionPipe;
    };

    // An NBS 1.0 request forwarded to the partition and not answered yet.
    struct TForwardedRequest
    {
        NActors::TActorId Sender;
        ui64 Cookie = 0;
        EForwardedRequestKind Kind = EForwardedRequestKind::StatVolume;
    };

    // Waits for the tablet executor to boot.
    void StateInit(TAutoPtr<NActors::IEventHandle>& ev);
    // Waits for LoadState; the requests are postponed.
    STFUNC(StateBoot);
    // Serves requests.
    STFUNC(StateWork);

    void OnDetach(const NActors::TActorContext& ctx) override;
    void OnTabletDead(
        NKikimr::TEvTablet::TEvTabletDead::TPtr& ev,
        const NActors::TActorContext& ctx) override;
    void OnActivateExecutor(const NActors::TActorContext& ctx) override;
    void DefaultSignalTabletActive(const NActors::TActorContext& ctx) override;

    // Closes the pipes to the partition and rejects the forwarded requests.
    void CleanupResources(const NActors::TActorContext& ctx);

    void ReportTabletState(const NActors::TActorContext& ctx);

    // Handles the events common to StateBoot and StateWork.
    void HandleCommonEvents(TAutoPtr<NActors::IEventHandle>& ev);

    void HandleServerConnected(
        const NKikimr::TEvTabletPipe::TEvServerConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDisconnected(
        const NKikimr::TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDestroyed(
        const NKikimr::TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleClientConnected(
        NKikimr::TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleClientDestroyed(
        NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Closes the long-lived pipe to the partition and rejects the requests
    // forwarded through it so that nbsd retries them.
    void DropPartitionPipe(
        const NActors::TActorContext& ctx,
        const TString& reason);

    // Re-sends to self the events postponed in StateBoot.
    void SendPostponedEvents(const NActors::TActorContext& ctx);

    // Forwards the config to the partition; the reply to SchemeShard follows
    // the partition's answer and the commit of a new partition tablet id.
    void HandleUpdateVolumeConfig(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVolumeConfigResponse(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Replies to SchemeShard's UpdateVolumeConfig.
    void ReplyUpdateVolumeConfig(
        const NActors::TActorContext& ctx,
        const TRequestInfo& requestInfo,
        ui64 txId,
        NKikimrBlockStore::EStatus status);

    void HandleStatVolume(
        const TNbs1Service::TEvStatVolumeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleStatVolumeResponse(
        const TNbs1Service::TEvStatVolumeResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWaitReady(
        const TNbs1Volume::TEvWaitReadyRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWaitReadyResponse(
        const TNbs1Volume::TEvWaitReadyResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Sends a copy of the request to the partition over the long-lived pipe;
    // E_REJECTED while the partition tablet id is not known.
    template <typename TRequest>
    void ForwardToPartition(
        const NActors::TActorContext& ctx,
        const typename TRequest::TPtr& ev,
        EForwardedRequestKind kind);

    // Relays the partition's response to the sender of the forwarded request.
    template <typename TResponse>
    void RelayPartitionResponse(
        const NActors::TActorContext& ctx,
        const typename TResponse::TPtr& ev);

    // Answers the request with the error in the response type of its kind.
    void RejectForwardedRequest(
        const NActors::TActorContext& ctx,
        const TForwardedRequest& request,
        const NProto::TError& error);

    BLOCKSTORE_VOLUME_TRANSACTIONS(BLOCKSTORE_IMPLEMENT_TRANSACTION, TTxVolume)

private:
    // 0 until the first UpdateVolumeConfig is answered by the partition.
    ui64 PartitionTabletId = 0;

    // Events received before LoadState completed.
    TVector<std::unique_ptr<NActors::IEventHandle>> PostponedEvents;

    // txId -> request
    THashMap<ui64, TUpdateVolumeConfigRequest> UpdateVolumeConfigRequests;

    // The pipe to the partition that carries the NBS 1.0 requests; created on
    // first use and kept until it breaks.
    NActors::TActorId PartitionPipe;

    // The cookie of the last request sent through PartitionPipe.
    ui64 LastForwardCookie = 0;

    // own cookie -> request
    THashMap<ui64, TForwardedRequest> ForwardedRequests;
};

}   // namespace NYdb::NBS::NStorage
