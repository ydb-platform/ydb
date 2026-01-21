#include "plain_ldap_auth_proxy_actor.h"

#include <ydb/core/security/ldap_auth_provider/ldap_auth_provider.h>
#include <ydb/core/security/login_shared_func.h>
#include <ydb/core/security/sasl/base_auth_actors.h>
#include <ydb/core/security/sasl/events.h>

using namespace NLoginProto;

namespace NKikimr::NSasl {

using namespace NActors;
using namespace NLogin;
using namespace NSchemeShard;

class TPlainLdapAuthProxyActor : public TPlainAuthActorBase {
public:
    TPlainLdapAuthProxyActor(TActorId sender, const std::string& database, const std::string& authMsg, const std::string& peerName)
        : TPlainAuthActorBase(sender, database, authMsg, peerName)
    {
    }

    STATEFN(StateLdapAuthentication) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLdapAuthProvider::TEvAuthenticateResponse, Handle);
        }
    }

    virtual void Bootstrap(const TActorContext &ctx) override final {
        ProcessAuthMsg(ctx);

        auto event = std::make_unique<TEvLdapAuthProvider::TEvAuthenticateRequest>(TString(AuthcId), TString(Passwd));
        ctx.Send(MakeLdapAuthProviderID(), event.release());
        Become(&TPlainLdapAuthProxyActor::StateLdapAuthentication);
        return;
    }

    void Handle(TEvLdapAuthProvider::TEvAuthenticateResponse::TPtr& ev, const TActorContext &ctx) {
        const TEvLdapAuthProvider::TEvAuthenticateResponse& response = *ev->Get();

        LOG_DEBUG_S(ctx, NKikimrServices::SASL_AUTH,
            ActorName << "# " << ctx.SelfID.ToString() <<
            " Handle TEvLdapAuthProvider::TEvAuthenticateResponse" <<
            ", " << "status: " <<  static_cast<ui64>(response.Status) <<
            ", " << "error: " << response.Error;
        );

        if (response.Status == TEvLdapAuthProvider::EStatus::SUCCESS) {
            ResolveSchemeShard(ctx);
        } else {
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << "LDAP authentication failed: "<< response.Error
            );
            SendError(ConvertLdapStatus(response.Status), response.Error.Message,
                NLogin::NSasl::EScramServerError::OtherError, response.Error.LogMessage);
            return CleanupAndDie(ctx);
        }
    }

private:
    virtual NKikimrScheme::TEvLogin CreateLoginRequest() const override final {
        return CreatePlainLdapLoginRequest(TString(AuthcId), TString(PeerName), AppData()->AuthConfig);
    }

    virtual void SendIssuedToken(const NKikimrScheme::TEvLoginResult& loginResult) const override final {
        auto response = std::make_unique<TEvSasl::TEvSaslPlainLdapLoginResponse>();
        response->Issue = MakeIssue(NKikimrIssues::TIssuesIds::SUCCESS);
        response->Token = loginResult.GetToken();
        response->SanitizedToken = loginResult.GetSanitizedToken();
        response->IsAdmin = loginResult.GetIsAdmin();

        SendResponse(std::move(response));
    }

    virtual void SendError(NKikimrIssues::TIssuesIds::EIssueCode issueCode, const std::string& message,
        [[maybe_unused]] NLogin::NSasl::EScramServerError scramErrorCode = NLogin::NSasl::EScramServerError::OtherError,
        const std::string& reason = "") const override final
    {
        auto response = std::make_unique<TEvSasl::TEvSaslPlainLdapLoginResponse>();
        response->Issue = MakeIssue(issueCode, TString(message));
        response->Reason = reason;
        SendResponse(std::move(response));
    }

    static NKikimrIssues::TIssuesIds::EIssueCode ConvertLdapStatus(const TEvLdapAuthProvider::EStatus& status) {
        switch (status) {
            case NKikimr::TEvLdapAuthProvider::EStatus::SUCCESS:
                return NKikimrIssues::TIssuesIds::SUCCESS;
            case NKikimr::TEvLdapAuthProvider::EStatus::UNAUTHORIZED:
                return NKikimrIssues::TIssuesIds::ACCESS_DENIED;
            case NKikimr::TEvLdapAuthProvider::EStatus::UNAVAILABLE:
                return NKikimrIssues::TIssuesIds::YDB_AUTH_UNAVAILABLE;
            case NKikimr::TEvLdapAuthProvider::EStatus::BAD_REQUEST:
                return NKikimrIssues::TIssuesIds::YDB_API_VALIDATION_ERROR;
        }
    }

    void SendResponse(std::unique_ptr<TEvSasl::TEvSaslPlainLdapLoginResponse> response) const {
        Send(Sender, response.release());
    }

private:
    static constexpr std::string_view ActorName = "TPlainLdapAuthProxyActor";
};

std::unique_ptr<IActor> CreatePlainLdapAuthProxyActor(
    TActorId sender, const std::string& database, const std::string& saslPlainAuthMsg, const std::string& peerName)
{
    return std::make_unique<TPlainLdapAuthProxyActor>(sender, database, saslPlainAuthMsg, peerName);
}

} // namespace NKikimr::NSasl
