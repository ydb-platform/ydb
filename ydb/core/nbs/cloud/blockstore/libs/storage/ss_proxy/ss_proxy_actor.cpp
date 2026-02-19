#include "ss_proxy_actor.h"

#include <ydb/core/nbs/cloud/storage/core/libs/kikimr/helpers.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

const THashSet<ui32> RetriableTxProxyErrors{
    NKikimr::NTxProxy::TResultStatus::ProxyNotReady,
    NKikimr::NTxProxy::TResultStatus::ProxyShardNotAvailable,
    NKikimr::NTxProxy::TResultStatus::ProxyShardTryLater,
    NKikimr::NTxProxy::TResultStatus::ProxyShardOverloaded,
    NKikimr::NTxProxy::TResultStatus::ExecTimeout,
    NKikimr::NTxProxy::TResultStatus::ExecResultUnavailable};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NTabletPipe::IClientCache> CreateTabletPipeClientCache(
    const NProto::TStorageServiceConfig& config)
{
    NTabletPipe::TClientConfig clientConfig;
    NTabletPipe::TClientRetryPolicy retryPolicy;
    retryPolicy.RetryLimitCount = config.GetPipeClientRetryCount();
    retryPolicy.MinRetryTime =
        TDuration::Seconds(config.GetPipeClientMinRetryTime());
    retryPolicy.MaxRetryTime =
        TDuration::Seconds(config.GetPipeClientMaxRetryTime());
    clientConfig.RetryPolicy = retryPolicy;

    return std::unique_ptr<NTabletPipe::IClientCache>(
        NTabletPipe::CreateUnboundedClientCache(clientConfig));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSSProxyActor::TSSProxyActor(
    const NProto::TStorageServiceConfig& nbsStorageConfig)
    : NbsStorageConfig(nbsStorageConfig)
    , ClientCache(CreateTabletPipeClientCache(nbsStorageConfig))
{}

void TSSProxyActor::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    TThis::Become(&TThis::StateWork);

    // const auto& filepath = Config->GetPathDescriptionBackupFilePath();
    // if (filepath) {
    //     auto cache = std::make_unique<TPathDescriptionBackup>(
    //         filepath, false /* readOnlyMode */);
    //     PathDescriptionBackup = ctx.Register(
    //         cache.release(), TMailboxType::HTSwap, AppData()->IOPoolId);
    // }
}

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev,
                                  const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (!ClientCache->OnConnect(ev)) {
        auto error = MakeKikimrError(
            msg->Status,
            TStringBuilder()
                << "Connect to schemeshard " << msg->TabletId << " failed");

        OnConnectionError(ctx, error, msg->TabletId);
    }
}

void TSSProxyActor::HandleDisconnect(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ClientCache->OnDisconnect(ev);

    auto error = MakeError(
        E_REJECTED,
        TStringBuilder() << "Disconnected from schemeshard " << msg->TabletId);

    OnConnectionError(ctx, error, msg->TabletId);
}

void TSSProxyActor::OnConnectionError(
    const TActorContext& ctx, const NProto::TError& error, ui64 schemeShard)
{
    Y_UNUSED(error);

    // SchemeShard is a tablet, so it should eventually get up
    // Re-send all outstanding requests
    if (auto* state = SchemeShardStates.FindPtr(schemeShard)) {
        for (const auto& kv: state->TxToRequests) {
            ui64 txId = kv.first;
            SendWaitTxRequest(ctx, schemeShard, txId);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TSSProxyActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_SS_PROXY_REQUESTS(BLOCKSTORE_HANDLE_REQUEST, TEvSSProxy)

        default:
            return false;
    }

    return true;
}

STFUNC(TSSProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);

        HFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered,
              HandleTxRegistered);
        HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, HandleTxResult);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(ev, NKikimrServices::NBS_SS_PROXY,
                                      __PRETTY_FUNCTION__);
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleBackupPathDescriptions(
    const TEvSSProxy::TEvBackupPathDescriptionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (PathDescriptionBackup) {
        ctx.Send(ev->Forward(PathDescriptionBackup));
    } else {
        auto response =
            std::make_unique<TEvSSProxy::TEvBackupPathDescriptionsResponse>(
                MakeError(S_FALSE));
        NYdb::NBS::Reply(ctx, *ev, std::move(response));
    }
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError GetErrorFromPreconditionFailed(const NProto::TError& error)
{
    NProto::TError result = error;
    const auto& msg = error.GetMessage();

    if (msg.Contains("Wrong version in")) {
        // ConfigVersion is different from current one in SchemeShard
        // return E_ABORTED to client to read
        // updated config (StatVolume) and issue new request
        result.SetCode(E_ABORTED);
        result.SetMessage("Config version mismatch");
    } else if (msg.Contains("path version mistmach")) {
        // Just path version mismatch. Return E_REJECTED
        // so durable client will retry request
        result.SetCode(E_REJECTED);
    }
    return result;
}

NProto::TError TranslateTxProxyError(NProto::TError error)
{
    if (FACILITY_FROM_CODE(error.GetCode()) != FACILITY_TXPROXY) {
        return error;
    }

    auto status =
        static_cast<NKikimr::NTxProxy::TResultStatus::EStatus>(STATUS_FROM_CODE(error.GetCode()));
    if (RetriableTxProxyErrors.count(status)) {
        error.SetCode(E_REJECTED);
    }
    return error;
}

}   // namespace NYdb::NBS::NStorage
