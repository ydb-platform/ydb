#include "grpc_request_proxy.h"
#include "service_auth.h"
#include "audit_logins.h"

#include "rpc_request_base.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/core/security/ldap_auth_provider/ldap_auth_provider.h>
#include <ydb/core/security/login_shared_func.h>

#include <ydb/public/api/protos/ydb_auth.pb.h>

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
        TString domainName = "/" + AppData()->DomainsInfo->GetDomain()->Name;
        PathToDatabase = AppData()->AuthConfig.GetDomainLoginOnly() ? domainName : DatabaseName;
        auto sendParameters = GetSendParameters(Credentials, PathToDatabase);
        Send(sendParameters.Recipient, sendParameters.Event.Release());
        Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
    }

    void HandleTimeout() {
        ReplyErrorAndPassAway(Ydb::StatusIds::TIMEOUT, "Login timeout");
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
        ReplyErrorAndPassAway(Ydb::StatusIds::SCHEME_ERROR, "No database found");
    }

    void Handle(TEvLdapAuthProvider::TEvAuthenticateResponse::TPtr& ev) {
        const TEvLdapAuthProvider::TEvAuthenticateResponse& response = *ev->Get();
        if (response.Status == TEvLdapAuthProvider::EStatus::SUCCESS) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(CreateNavigateKeySetRequest(PathToDatabase).Release()));
        } else {
            ReplyErrorAndPassAway(ConvertLdapStatus(response.Status), response.Error.Message, response.Error.LogMessage);
        }
    }

    void HandleResult(TEvSchemeShard::TEvLoginResult::TPtr& ev) {
        const NKikimrScheme::TEvLoginResult& loginResult = ev->Get()->Record;
        if (loginResult.error()) {
            // explicit error takes precedence
            ReplyErrorAndPassAway(Ydb::StatusIds::UNAUTHORIZED, loginResult.error(), /*loginResult.details()*/ TString());
        } else if (loginResult.token().empty()) {
            // empty token is still an error
            ReplyErrorAndPassAway(Ydb::StatusIds::INTERNAL_ERROR, "Failed to produce a token");
        } else {
            // success = token + no errors
            ReplyAndPassAway(loginResult.token());
        }
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&) {
        ReplyErrorAndPassAway(Ydb::StatusIds::UNAVAILABLE, "SchemeShard is unreachable");
    }

    void HandleConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            ReplyErrorAndPassAway(Ydb::StatusIds::UNAVAILABLE, "SchemeShard is unavailable");
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

    void ReplyAndPassAway(const TString& resultToken) {
        TResponse response;

        Ydb::Operations::Operation& operation = *response.mutable_operation();
        operation.set_ready(true);
        operation.set_status(Ydb::StatusIds::SUCCESS);
        // Pack result to google::protobuf::Any
        {
            Ydb::Auth::LoginResult result;
            result.set_token(resultToken);
            operation.mutable_result()->PackFrom(result);
        }

        AuditLogLogin(Request.Get(), PathToDatabase, *GetProtoRequest(), response, /* errorDetails */ TString());

        return CleanupAndReply(response);
    }

    void ReplyErrorAndPassAway(const Ydb::StatusIds_StatusCode status, const TString& error, const TString& reason = "") {
        TResponse response;

        Ydb::Operations::Operation& operation = *response.mutable_operation();
        operation.set_ready(true);
        operation.set_status(status);
        if (error) {
            Ydb::Issue::IssueMessage* issue = operation.add_issues();
            issue->set_issue_code(status);
            issue->set_message(error);
        }

        AuditLogLogin(Request.Get(), PathToDatabase, *GetProtoRequest(), response, reason);

        return CleanupAndReply(response);
    }

    void CleanupAndReply(const TResponse& response) {
        if (PipeClient) {
            NTabletPipe::CloseClient(SelfId(), PipeClient);
        }

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

void DoLoginRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TLoginRPC(p.release()));
}

template<>
IActor* TEvLoginRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TLoginRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
