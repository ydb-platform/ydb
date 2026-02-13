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
#include <ydb/core/security/sasl/events.h>
#include <ydb/core/security/sasl/plain_auth_actor.h>
#include <ydb/core/security/sasl/plain_ldap_auth_proxy_actor.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/login/login.h>
#include <ydb/library/login/sasl/plain.h>

#include <ydb/public/api/protos/ydb_auth.pb.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NSasl;
using namespace NSchemeShard;

using TEvLoginRequest = TGRpcRequestWrapperNoAuth<TRpcServices::EvLogin, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>;

class TLoginRPC : public TRpcRequestActor<TLoginRPC, TEvLoginRequest, true> {
private:
    TString PathToDatabase;

public:
    using TRpcRequestActor::TRpcRequestActor;

    TDuration Timeout = TDuration::MilliSeconds(60000);

    void Bootstrap(const TActorContext &ctx) {
        TString domainName = "/" + AppData()->DomainsInfo->GetDomain()->Name;
        PathToDatabase = AppData()->AuthConfig.GetDomainLoginOnly() ? domainName : GetDatabaseName();

        std::unique_ptr<IActor> authActor;
        const Ydb::Auth::LoginRequest* protoRequest = GetProtoRequest();
        if (IsUsernameFromLdapAuthDomain(protoRequest->user(), AppData()->AuthConfig)) {
            const TString ldapUsername = PrepareLdapUsername(protoRequest->user(), AppData()->AuthConfig);
            const TString saslPlainAuthMsg = NLogin::NSasl::BuildSaslPlainAuthMsg(ldapUsername, protoRequest->password());
            authActor = CreatePlainLdapAuthProxyActor(ctx.SelfID, PathToDatabase, saslPlainAuthMsg, Request->GetPeerName());
        } else {
            const TString saslPlainAuthMsg = NLogin::NSasl::BuildSaslPlainAuthMsg(protoRequest->user(), protoRequest->password());
            authActor = CreatePlainAuthActor(ctx.SelfID, PathToDatabase, saslPlainAuthMsg, Request->GetPeerName());
        }

        Register(authActor.release());
        Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
    }

    void HandleTimeout() {
        ReplyErrorAndPassAway(Ydb::StatusIds::TIMEOUT, "Login timeout");
    }

    void ProcessLoginResult(const NYql::TIssue& issue, const std::string& reason, const std::string& token,
        const std::string& sanitizedToken, bool isAdmin)
    {
        switch (issue.GetCode()) {
        case NKikimrIssues::TIssuesIds::SUCCESS:
            return ReplyAndPassAway(token, sanitizedToken, isAdmin);
        case NKikimrIssues::TIssuesIds::ACCESS_DENIED:
            return ReplyErrorAndPassAway(Ydb::StatusIds::UNAUTHORIZED, issue.GetMessage(), TString(reason));
        case NKikimrIssues::TIssuesIds::UNEXPECTED:
            return ReplyErrorAndPassAway(Ydb::StatusIds::INTERNAL_ERROR, issue.GetMessage());
        case NKikimrIssues::TIssuesIds::DATABASE_NOT_EXIST:
            return ReplyErrorAndPassAway(Ydb::StatusIds::SCHEME_ERROR, "No database found");
        case NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE:
            return ReplyErrorAndPassAway(Ydb::StatusIds::UNAVAILABLE, "SchemeShard is unreachable");
        case NKikimrIssues::TIssuesIds::DEFAULT_ERROR:
            return ReplyErrorAndPassAway(Ydb::StatusIds::INTERNAL_ERROR, issue.GetMessage());
        case NKikimrIssues::TIssuesIds::YDB_AUTH_UNAVAILABLE:
            return ReplyErrorAndPassAway(Ydb::StatusIds::UNAVAILABLE, issue.GetMessage(), TString(reason));
        case NKikimrIssues::TIssuesIds::YDB_API_VALIDATION_ERROR:
            return ReplyErrorAndPassAway(Ydb::StatusIds::BAD_REQUEST, issue.GetMessage(), TString(reason));
        default:
            return ReplyErrorAndPassAway(Ydb::StatusIds::GENERIC_ERROR, issue.GetMessage());
        }
    }

    void HandleResult(TEvSasl::TEvSaslPlainLoginResponse::TPtr& ev) {
        const auto& loginResult = *ev->Get();
        ProcessLoginResult(loginResult.Issue, "", loginResult.Token, loginResult.SanitizedToken,
            loginResult.IsAdmin);
    }

    void HandleResult(TEvSasl::TEvSaslPlainLdapLoginResponse::TPtr& ev) {
        const auto& loginResult = *ev->Get();
        ProcessLoginResult(loginResult.Issue, loginResult.Reason, loginResult.Token, loginResult.SanitizedToken,
            loginResult.IsAdmin);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSasl::TEvSaslPlainLoginResponse, HandleResult);
            hFunc(TEvSasl::TEvSaslPlainLdapLoginResponse, HandleResult);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void ReplyAndPassAway(const std::string& token, const std::string& sanitizedToken, bool isAdmin) {
        TResponse response;
        Ydb::Operations::Operation& operation = *response.mutable_operation();
        operation.set_ready(true);
        operation.set_status(Ydb::StatusIds::SUCCESS);
        // Pack result to google::protobuf::Any
        {
            Ydb::Auth::LoginResult result;
            result.set_token(token);
            operation.mutable_result()->PackFrom(result);
        }

        AuditLogLogin(Request.Get(), PathToDatabase, *GetProtoRequest(), response,
            /* errorDetails */ "", TString(sanitizedToken), isAdmin);

        return Reply(response);
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

        AuditLogLogin(Request.Get(), PathToDatabase, *GetProtoRequest(), response, reason, {});

        return Reply(response);
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
