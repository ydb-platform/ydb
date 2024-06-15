#include "msgbus_server.h"
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_auth.pb.h>

#include <ydb/core/protos/auth.pb.h>

namespace NKikimr {
namespace NMsgBusProxy {

using namespace NSchemeShard;

class TMessageBusLoginRequest : public TActorBootstrapped<TMessageBusLoginRequest>, public TMessageBusSessionIdentHolder {
    THolder<TBusLoginRequest> Request;
    THolder<TEvSchemeShard::TEvLoginResult> Result;
    TDuration Timeout = TDuration::MilliSeconds(60000);
    TActorId PipeClient;
    NMsgBusProxy::EResponseStatus Status;
public:
    TMessageBusLoginRequest(TBusMessageContext& msg)
        : TMessageBusSessionIdentHolder(msg)
        , Request(static_cast<TBusLoginRequest*>(msg.ReleaseMessage()))
    {
    }

    NTabletPipe::TClientConfig GetPipeClientConfig() {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        return clientConfig;
    }

    void Bootstrap() {
        TString domainName = "/" + AppData()->DomainsInfo->GetDomain()->Name;
        TString path = AppData()->AuthConfig.GetDomainLoginOnly() ? domainName : TString();//Request.Get()->GetRecord()->GetDatabaseName();
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = path;
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = ::NKikimr::SplitPath(path);
        entry.RedirectRequired = false;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));

        Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
    }

    void HandleTimeout() {
        Status = MSTATUS_TIMEOUT;
        ReplyAndPassAway();
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* response = ev->Get()->Request.Get();
        if (response->ResultSet.size() == 1) {
            const NSchemeCache::TSchemeCacheNavigate::TEntry& entry = response->ResultSet.front();
            ui64 schemeShardTabletId = entry.DomainInfo->ExtractSchemeShard();
            IActor* pipe = NTabletPipe::CreateClient(SelfId(), schemeShardTabletId, GetPipeClientConfig());
            PipeClient = RegisterWithSameMailbox(pipe);
            THolder<TEvSchemeShard::TEvLogin> request = MakeHolder<TEvSchemeShard::TEvLogin>();
            request.Get()->Record.SetUser(Request.Get()->Record.GetUser());
            request.Get()->Record.SetPassword(Request.Get()->Record.GetPassword());
            NTabletPipe::SendData(SelfId(), PipeClient, request.Release());
            return;
        }
        Status = MSTATUS_ERROR;
        ReplyAndPassAway();
    }

    void HandleResult(TEvSchemeShard::TEvLoginResult::TPtr& ev) {
        Status = MSTATUS_OK;
        Result = ev->Release();
        ReplyAndPassAway();
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&) {
        Status = MSTATUS_NOTREADY;
        ReplyAndPassAway();
    }

    void HandleConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Status = MSTATUS_NOTREADY;
            ReplyAndPassAway();
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvUndelivered, HandleUndelivered);
            hFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            hFunc(TEvSchemeShard::TEvLoginResult, HandleResult);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void ReplyAndPassAway() {
        if (PipeClient) {
            NTabletPipe::CloseClient(SelfId(), PipeClient);
        }
        THolder<TBusResponse> response(new TBusResponse());
        if (Result) {
            const NKikimrScheme::TEvLoginResult& record = Result->Record;
            if (record.error()) {
                response->Record.SetErrorReason(record.error());
                response->Record.SetStatus(MSTATUS_ERROR);
                Status = MSTATUS_ERROR;
            }
            if (record.token()) {
                response->Record.SetUserToken(record.token());
                response->Record.SetStatus(MSTATUS_OK);
                Status = MSTATUS_OK;
            }
        }
        SendReplyMove(response.Release());
        PassAway();
    }
};

IActor* CreateMessageBusLoginRequest(TBusMessageContext& msg) {
    return new TMessageBusLoginRequest(msg);
}

}
}
