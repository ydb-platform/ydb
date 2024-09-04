#include "grpc_request_proxy.h"

#include "rpc_calls.h"
#include "rpc_kqp_base.h"
#include "rpc_request_base.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/core/security/ldap_auth_provider/ldap_auth_provider.h>
#include <ydb/core/security/login_shared_func.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NSchemeShard;

using TEvLoginRequest = TGRpcRequestWrapperNoAuth<TRpcServices::EvLogin, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>;

class TLoginRPC : public TRpcRequestActor<TLoginRPC, TEvLoginRequest, true> {
private:
    TAuthCredentials Credentials;
    TString PathToDatabase;

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
        const Ydb::Auth::LoginRequest* protoRequest = GetProtoRequest();
        Credentials = PrepareCredentials(protoRequest->user(), protoRequest->password(), AppData()->AuthConfig);
        TString domainName = "/" + AppData()->DomainsInfo->Domains.begin()->second->Name;
        PathToDatabase = AppData()->AuthConfig.GetDomainLoginOnly() ? domainName : DatabaseName;
        auto sendParameters = GetSendParameters(Credentials, PathToDatabase);
        Send(sendParameters.Recipient, sendParameters.Event.Release());
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
                request.Get()->Record = CreateLoginRequest(Credentials, AppData()->AuthConfig);
                request.Get()->Record.SetPeerName(Request->GetPeerName());
                NTabletPipe::SendData(SelfId(), PipeClient, request.Release());
                return;
            }
        }
        Status = Ydb::StatusIds::SCHEME_ERROR;
        ReplyAndPassAway();
    }

    void Handle(TEvLdapAuthProvider::TEvAuthenticateResponse::TPtr& ev) {
        TEvLdapAuthProvider::TEvAuthenticateResponse* response = ev->Get();
        if (response->Status == TEvLdapAuthProvider::EStatus::SUCCESS) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(CreateNavigateKeySetRequest(PathToDatabase).Release()));
        } else {
            TResponse loginResponse;
            Ydb::Operations::Operation& operation = *loginResponse.mutable_operation();
            Ydb::Issue::IssueMessage* issue = operation.add_issues();
            issue->set_message(response->Error.Message);
            Status = ConvertLdapStatus(response->Status);
            issue->set_issue_code(Status);
            operation.set_ready(true);
            operation.set_status(Status);
            Reply(loginResponse);
        }
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
            hFunc(TEvLdapAuthProvider::TEvAuthenticateResponse, Handle);
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

private:
    static Ydb::StatusIds::StatusCode ConvertLdapStatus(const TEvLdapAuthProvider::EStatus& status) {
        switch (status) {
            case NKikimr::TEvLdapAuthProvider::EStatus::SUCCESS:
                return Ydb::StatusIds::SUCCESS;
            case NKikimr::TEvLdapAuthProvider::EStatus::UNAUTHORIZED:
                return Ydb::StatusIds::UNAUTHORIZED;
            case NKikimr::TEvLdapAuthProvider::EStatus::UNAVAILABLE:
                return Ydb::StatusIds::UNAVAILABLE;
            case NKikimr::TEvLdapAuthProvider::EStatus::BAD_REQUEST:
                return Ydb::StatusIds::BAD_REQUEST;
        }
    }
};

void TGRpcRequestProxyHandleMethods::Handle(TEvLoginRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Register(new TLoginRPC(ev->Release().Release()));
}

template<>
IActor* TEvLoginRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TLoginRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
