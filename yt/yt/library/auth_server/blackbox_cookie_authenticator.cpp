#include "blackbox_cookie_authenticator.h"

#include "blackbox_service.h"
#include "config.h"
#include "cookie_authenticator.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/core/crypto/crypto.h>

#include <util/string/split.h>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NYPath;
using namespace NCrypto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

// TODO(sandello): Indicate to end-used that cookie must be resigned.
class TBlackboxCookieAuthenticator
    : public ICookieAuthenticator
{
public:
    TBlackboxCookieAuthenticator(
        TBlackboxCookieAuthenticatorConfigPtr config,
        IBlackboxServicePtr blackboxService)
        : Config_(std::move(config))
        , BlackboxService_(std::move(blackboxService))
    { }

    const std::vector<TStringBuf>& GetCookieNames() const override
    {
        static const std::vector<TStringBuf> cookieNames{
            BlackboxSessionIdCookieName,
            BlackboxSslSessionIdCookieName,
        };
        return cookieNames;
    }

    bool CanAuthenticate(const TCookieCredentials& credentials) const override
    {
        return credentials.Cookies.contains(BlackboxSessionIdCookieName);
    }

    TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) override
    {
        const auto& cookies = credentials.Cookies;
        auto sessionId = GetOrCrash(cookies, BlackboxSessionIdCookieName);

        std::optional<TString> sslSessionId;
        auto cookieIt = cookies.find(BlackboxSslSessionIdCookieName);
        if (cookieIt != cookies.end()) {
            sslSessionId = cookieIt->second;
        }

        auto sessionIdMD5 = GetMD5HexDigestUpperCase(sessionId);
        auto sslSessionIdMD5 = GetMD5HexDigestUpperCase(sslSessionId.value_or(""));
        auto userIP = FormatUserIP(credentials.UserIP);

        YT_LOG_DEBUG(
            "Authenticating user via session cookie (SessionIdMD5: %v, SslSessionIdMD5: %v, UserIP: %v)",
            sessionIdMD5,
            sslSessionIdMD5,
            userIP);

        THashMap<TString, TString> params{
            {"sessionid", sessionId},
            {"host", Config_->Domain},
            {"userip", userIP},
        };

        if (Config_->GetUserTicket) {
            params["get_user_ticket"] = "yes";
        }

        if (sslSessionId) {
            params["sslsessionid"] = *sslSessionId;
        }

        return BlackboxService_->Call("sessionid", params)
            .Apply(BIND(
                &TBlackboxCookieAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(sessionIdMD5),
                std::move(sslSessionIdMD5)));
    }

private:
    const TBlackboxCookieAuthenticatorConfigPtr Config_;
    const IBlackboxServicePtr BlackboxService_;

private:
    TFuture<TAuthenticationResult> OnCallResult(
        const TString& sessionIdMD5,
        const TString& sslSessionIdMD5,
        const INodePtr& data)
    {
        auto result = OnCallResultImpl(data);
        if (!result.IsOK()) {
            YT_LOG_DEBUG(result, "Authentication failed (SessionIdMD5: %v, SslSessionIdMD5: %v)", sessionIdMD5, sslSessionIdMD5);
            result.MutableAttributes()->Set("sessionid_md5", sessionIdMD5);
            result.MutableAttributes()->Set("sslsessionid_md5", sslSessionIdMD5);
        } else {
            YT_LOG_DEBUG(
                "Authentication successful (SessionIdMD5: %v, SslSessionIdMD5: %v, Login: %v, Realm: %v)",
                sessionIdMD5,
                sslSessionIdMD5,
                result.Value().Login,
                result.Value().Realm);
        }
        return MakeFuture(result);
    }

    TErrorOr<TAuthenticationResult> OnCallResultImpl(const INodePtr& data)
    {
        auto statusId = GetByYPath<i64>(data, "/status/id");
        if (!statusId.IsOK()) {
            return TError("Blackbox returned invalid response");
        }

        auto status = static_cast<EBlackboxStatus>(statusId.Value());
        if (status != EBlackboxStatus::Valid && status != EBlackboxStatus::NeedReset) {
            auto error = GetByYPath<TString>(data, "/error");
            auto reason = error.IsOK() ? error.Value() : "unknown";
            return TError(NRpc::EErrorCode::InvalidCredentials, "Blackbox rejected session cookie")
                << TErrorAttribute("reason", reason);
        }

        auto login = BlackboxService_->GetLogin(data);

        // Sanity checks.
        if (!login.IsOK()) {
            return TError("Blackbox returned invalid response")
                << login;
        }

        TAuthenticationResult result;
        result.Login = login.Value();
        result.Realm = "blackbox:cookie";
        auto userTicket = GetByYPath<TString>(data, "/user_ticket");
        if (userTicket.IsOK()) {
            result.UserTicket = userTicket.Value();
        } else if (Config_->GetUserTicket) {
            return TError("Failed to retrieve user ticket");
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

ICookieAuthenticatorPtr CreateBlackboxCookieAuthenticator(
    TBlackboxCookieAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackboxService)
{
    return New<TBlackboxCookieAuthenticator>(std::move(config), std::move(blackboxService));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
