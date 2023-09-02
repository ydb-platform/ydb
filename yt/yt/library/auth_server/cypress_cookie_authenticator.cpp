#include "cypress_cookie_authenticator.h"

#include "config.h"
#include "cypress_cookie_store.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/library/auth_server/cookie_authenticator.h>
#include <yt/yt/library/auth_server/helpers.h>
#include <yt/yt/library/auth_server/private.h>

#include <yt/yt/core/crypto/crypto.h>

namespace NYT::NAuth {

using namespace NApi;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressCookieAuthenticator
    : public ICookieAuthenticator
{
public:
    TCypressCookieAuthenticator(
        TCypressCookieGeneratorConfigPtr config,
        ICypressCookieStorePtr cookieStore,
        IClientPtr client)
        : Config_(std::move(config))
        , CookieStore_(std::move(cookieStore))
        , Client_(std::move(client))
    { }

    const std::vector<TStringBuf>& GetCookieNames() const override
    {
        static const std::vector<TStringBuf> cookieNames{
            CypressCookieName,
        };
        return cookieNames;
    }

    bool CanAuthenticate(const TCookieCredentials& credentials) const override
    {
        return credentials.Cookies.contains(CypressCookieName);
    }

    TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) override
    {
        const auto& cookieValue = GetOrCrash(credentials.Cookies, CypressCookieName);

        YT_LOG_DEBUG(
            "Authenticating user via native cookie (CookieMD5: %v, UserIP: %v)",
            GetMD5HexDigestUpperCase(cookieValue),
            FormatUserIP(credentials.UserIP));

        return CookieStore_->GetCookie(cookieValue)
            .Apply(BIND(&TCypressCookieAuthenticator::OnGotCookie, MakeStrong(this)))
            .Apply(BIND([] (const TErrorOr<TAuthenticationResult>& resultOrError) -> TErrorOr<TAuthenticationResult> {
                if (resultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    return TError(
                        NRpc::EErrorCode::InvalidCredentials,
                        "Unknown credentials")
                        << resultOrError;
                }

                return resultOrError;
            }));
    }

private:
    const TCypressCookieGeneratorConfigPtr Config_;

    const ICypressCookieStorePtr CookieStore_;

    const IClientPtr Client_;

    TFuture<ui64> GetUserPasswordRevision(const TString& user)
    {
        auto path = Format("//sys/users/%v", ToYPathLiteral(user));

        constexpr TStringBuf PasswordRevisionAttribute = "password_revision";

        TGetNodeOptions options;
        options.Attributes = std::vector<TString>({
            TString{PasswordRevisionAttribute},
        });

        return Client_->GetNode(path, options)
            .Apply(BIND([=] (const TYsonString& rsp) {
                auto rspNode = ConvertToNode(rsp);
                return rspNode->Attributes().Get<ui64>(PasswordRevisionAttribute);
            }));
    }

    TFuture<TAuthenticationResult> OnGotCookie(const TCypressCookiePtr& cookie)
    {
        return GetUserPasswordRevision(cookie->User)
            .Apply(BIND(&TCypressCookieAuthenticator::OnGotPasswordRevision, MakeStrong(this), cookie));
    }

    TAuthenticationResult OnGotPasswordRevision(
        const TCypressCookiePtr& cookie,
        ui64 passwordRevision)
    {
        if (cookie->PasswordRevision != passwordRevision) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                "Native cookie was issued for previous password revision")
                << TErrorAttribute("cookie_password_revision", cookie->PasswordRevision)
                << TErrorAttribute("password_revision", passwordRevision);
        }

        auto now = TInstant::Now();
        if (cookie->ExpiresAt < now) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                "Native cookie expired")
                << TErrorAttribute("cookie_expiration_time", cookie->ExpiresAt);
        }

        const auto& user = cookie->User;
        TAuthenticationResult result{
            .Login = user,
        };

        if (cookie->ExpiresAt < now + Config_->CookieRenewalPeriod) {
            auto latestCookie = CookieStore_->GetLastCookieForUser(user);

            // Very unlikely, but might happen during cookie duration reconfiguration.
            if (latestCookie && latestCookie->PasswordRevision != passwordRevision) {
                CookieStore_->RemoveLastCookieForUser(user);
                latestCookie.Reset();
            }

            if (latestCookie && latestCookie->ExpiresAt > now + Config_->CookieRenewalPeriod) {
                result.SetCookie = latestCookie->ToHeader(Config_);
            } else {
                auto newCookie = New<TCypressCookie>();
                newCookie->Value = GenerateCookieValue();
                newCookie->User = user;
                newCookie->PasswordRevision = passwordRevision;
                newCookie->ExpiresAt = TInstant::Now() + Config_->CookieExpirationTimeout;

                YT_LOG_DEBUG("Issuing new cookie for renewal "
                    "(User: %v, CookieMD5: %v, PasswordRevision: %v, ExpiresAt: %v)",
                    user,
                    GetMD5HexDigestUpperCase(newCookie->Value),
                    passwordRevision,
                    newCookie->ExpiresAt);

                auto error = WaitFor(CookieStore_->RegisterCookie(newCookie));
                if (error.IsOK()) {
                    YT_LOG_DEBUG("Issued new cookie for renewal (User: %v, CookieMD5: %v)",
                        user,
                        GetMD5HexDigestUpperCase(newCookie->Value));
                    result.SetCookie = newCookie->ToHeader(Config_);
                } else {
                    // NB: Cookie creation failure should not lead to authentication error.
                    YT_LOG_DEBUG(error, "Failed to issue new cookie for renewal (User: %v, CookieMD5: %v)",
                        user,
                        GetMD5HexDigestUpperCase(newCookie->Value));
                }
            }
        }

        std::optional<TString> setCookieMD5;
        if (auto setCookie = result.SetCookie) {
            setCookieMD5 = GetMD5HexDigestUpperCase(*setCookie);
        }

        YT_LOG_DEBUG("User authenticated (User: %v, CookieMD5: %v, SetCookieMD5: %v)",
            user,
            GetMD5HexDigestUpperCase(cookie->Value),
            setCookieMD5);

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

ICookieAuthenticatorPtr CreateCypressCookieAuthenticator(
    TCypressCookieGeneratorConfigPtr config,
    ICypressCookieStorePtr cookieStore,
    IClientPtr client)
{
    return New<TCypressCookieAuthenticator>(
        std::move(config),
        std::move(cookieStore),
        std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
