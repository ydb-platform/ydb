#include "grpc_request_proxy.h"

#include "rpc_calls.h"
#include "rpc_kqp_base.h"
#include "rpc_request_base.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NSchemeShard;

class TLoginRPC : public TRpcRequestActor<TLoginRPC, TEvLoginRequest, true> {
public:
    using TRpcRequestActor::TRpcRequestActor;

    THolder<TEvSchemeShard::TEvLoginResult> Result;
    Ydb::StatusIds_StatusCode Status = Ydb::StatusIds::SUCCESS;
    TDuration Timeout = TDuration::MilliSeconds(60000);
    TActorId PipeClient;

    NTabletPipe::TClientConfig GetPipeClientConfig() {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        return clientConfig;
    }

    void Bootstrap() {
        TString domainName = "/" + AppData()->DomainsInfo->Domains.begin()->second->Name;
        TString path = AppData()->AuthConfig.GetDomainLoginOnly() ? domainName : DatabaseName;
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
        Status = Ydb::StatusIds::TIMEOUT;
        ReplyAndPassAway();
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& resultSet = ev->Get()->Request.Get()->ResultSet;
        if (resultSet.size() == 1 && resultSet.front().Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            const auto domainInfo = resultSet.front().DomainInfo;
            if (domainInfo != nullptr) {
                IActor* pipe = NTabletPipe::CreateClient(SelfId(), domainInfo->ExtractSchemeShard(), GetPipeClientConfig());
                PipeClient = RegisterWithSameMailbox(pipe);
                THolder<TEvSchemeShard::TEvLogin> request = MakeHolder<TEvSchemeShard::TEvLogin>();
                const Ydb::Auth::LoginRequest* protoRequest = GetProtoRequest();
                request.Get()->Record.SetUser(protoRequest->user());
                request.Get()->Record.SetPassword(protoRequest->password());
                NTabletPipe::SendData(SelfId(), PipeClient, request.Release());
                return;
            }
        }
        Status = Ydb::StatusIds::SCHEME_ERROR;
        ReplyAndPassAway();
    }

    void HandleResult(TEvSchemeShard::TEvLoginResult::TPtr& ev) {
        Status = Ydb::StatusIds::SUCCESS;
        Result = ev->Release();
        ReplyAndPassAway();
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&) {
        Status = Ydb::StatusIds::UNAVAILABLE;
        ReplyAndPassAway();
    }

    void HandleConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Status = Ydb::StatusIds::UNAVAILABLE;
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
        TResponse response;
        Ydb::Operations::Operation& operation = *response.mutable_operation();
        if (Result) {
            const NKikimrScheme::TEvLoginResult& record = Result->Record;
            if (record.error()) {
                Ydb::Issue::IssueMessage* issue = operation.add_issues();
                issue->set_message(record.error());
                issue->set_issue_code(Ydb::StatusIds::UNAUTHORIZED);
                Status = Ydb::StatusIds::UNAUTHORIZED;
            }
            if (record.token()) {
                Ydb::Auth::LoginResult result;
                result.set_token(record.token());
                operation.mutable_result()->PackFrom(result);
            }
        }
        operation.set_ready(true);
        operation.set_status(Status);
        return Reply(response);
    }
};

void TGRpcRequestProxyHandleMethods::Handle(TEvLoginRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Register(new TLoginRPC(ev->Release().Release()));
}

} // namespace NGRpcService
} // namespace NKikimr
