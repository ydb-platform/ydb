#include "oauth_cookie_authenticator.h"

#include "config.h"
#include "cookie_authenticator.h"
#include "cypress_user_manager.h"
#include "helpers.h"
#include "oauth_service.h"
#include "private.h"

#include <yt/yt/core/crypto/crypto.h>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NYPath;
using namespace NCrypto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TOAuthCookieAuthenticator
    : public ICookieAuthenticator
{
public:
    TOAuthCookieAuthenticator(
        TOAuthCookieAuthenticatorConfigPtr config,
        IOAuthServicePtr oauthService,
        ICypressUserManagerPtr userManager)
        : Config_(std::move(config))
        , OAuthService_(std::move(oauthService))
        , UserManager_(std::move(userManager))
    { }

    const std::vector<TStringBuf>& GetCookieNames() const override
    {
        static const std::vector<TStringBuf> cookieNames{
            OAuthAccessTokenCookieName,
        };
        return cookieNames;
    }

    bool CanAuthenticate(const TCookieCredentials& credentials) const override
    {
        return credentials.Cookies.contains(OAuthAccessTokenCookieName);
    }

    TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) override
    {
        const auto& cookies = credentials.Cookies;
        auto accessToken = GetOrCrash(cookies, OAuthAccessTokenCookieName);
        auto accessTokenMD5 = GetMD5HexDigestUpperCase(accessToken);
        auto userIP = FormatUserIP(credentials.UserIP);

        YT_LOG_DEBUG(
            "Authenticating user via OAuth cookie (AccessTokenMD5: %v, UserIP: %v)",
            accessTokenMD5,
            userIP);

        return OAuthService_->GetUserInfo(accessToken)
            .Apply(BIND(
                &TOAuthCookieAuthenticator::OnGetUserInfo,
                MakeStrong(this),
                std::move(accessTokenMD5)));
    }

private:
    const TOAuthCookieAuthenticatorConfigPtr Config_;
    const IOAuthServicePtr OAuthService_;
    const ICypressUserManagerPtr UserManager_;

    TFuture<TAuthenticationResult> OnGetUserInfo(
        const TString& accessTokenMD5,
        const TOAuthUserInfoResult& userInfo)
    {
        auto result = OnGetUserInfoImpl(userInfo);
        if (result.IsOK()) {
            YT_LOG_DEBUG(
                "Authentication via OAuth successful (AccessTokenMD5: %v, Login: %v, Realm: %v)",
                accessTokenMD5,
                result.Value().Login,
                result.Value().Realm);
        } else {
            YT_LOG_DEBUG(result, "Authentication via OAuth failed (AccessTokenMD5: %v)", accessTokenMD5);
            result.MutableAttributes()->Set("access_token_md5", accessTokenMD5);
        }

        return MakeFuture(std::move(result));
    }

    TErrorOr<TAuthenticationResult> OnGetUserInfoImpl(const TOAuthUserInfoResult& userInfo)
    {
        auto result = WaitFor(UserManager_->CreateUser(userInfo.Login));
        if (!result.IsOK()) {
            auto error = TError("Failed to create user")
                << TErrorAttribute("name", userInfo.Login)
                << std::move(result);
            YT_LOG_WARNING(error);
            return error;
        }

        return TAuthenticationResult{
            .Login = userInfo.Login,
            .Realm = TString(OAuthCookieRealm)
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

ICookieAuthenticatorPtr CreateOAuthCookieAuthenticator(
    TOAuthCookieAuthenticatorConfigPtr config,
    IOAuthServicePtr oauthService,
    ICypressUserManagerPtr userManager)
{
    return New<TOAuthCookieAuthenticator>(
        std::move(config),
        std::move(oauthService),
        std::move(userManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
