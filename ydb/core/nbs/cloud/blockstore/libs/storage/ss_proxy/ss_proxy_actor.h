#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSSProxyActor final: public NActors::TActorBootstrapped<TSSProxyActor>
{
    struct TSchemeShardState
    {
        NActors::TActorId ReplyProxy;
        THashMap<ui64, TDeque<TRequestInfoPtr>> TxToRequests;
    };

private:
    const NProto::TStorageServiceConfig NbsStorageConfig;
    std::unique_ptr<NKikimr::NTabletPipe::IClientCache> ClientCache;
    NActors::TActorId PathDescriptionBackup;

    THashMap<ui64, TSchemeShardState> SchemeShardStates;

public:
    explicit TSSProxyActor(
        const NProto::TStorageServiceConfig& nbsStorageConfig);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendWaitTxRequest(const NActors::TActorContext& ctx, ui64 schemeShard,
                           ui64 txId);

    void OnConnectionError(const NActors::TActorContext& ctx,
                           const NProto::TError& error, ui64 schemeShard);

private:
    STFUNC(StateWork);

    void HandleConnect(NKikimr::TEvTabletPipe::TEvClientConnected::TPtr& ev,
                       const NActors::TActorContext& ctx);

    void HandleDisconnect(NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
                          const NActors::TActorContext& ctx);

    void HandleTxRegistered(
        const NKikimr::NSchemeShard::TEvSchemeShard::
            TEvNotifyTxCompletionRegistered::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleTxResult(
        const NKikimr::NSchemeShard::TEvSchemeShard::
            TEvNotifyTxCompletionResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);

    BLOCKSTORE_SS_PROXY_REQUESTS(BLOCKSTORE_IMPLEMENT_REQUEST, TEvSSProxy)
};

////////////////////////////////////////////////////////////////////////////////

NProto::TError GetErrorFromPreconditionFailed(const NProto::TError& error);
NProto::TError TranslateTxProxyError(NProto::TError error);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TEvSSProxy::TEvModifySchemeRequest>
CreateModifySchemeRequestForAlterVolume(
    TString path, ui64 pathId, ui64 version,
    const NKikimrBlockStore::TVolumeConfig& volumeConfig);

}   // namespace NYdb::NBS::NStorage
