#include "cypress_token_authenticator.h"

#include "token_authenticator.h"
#include "private.h"

#include <yt/yt/client/api/client.h>

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

class TCypressTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    explicit TCypressTokenAuthenticator(IClientPtr client)
        : Client_(std::move(client))
    { }

    TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) override
    {
        const auto& token = credentials.Token;
        const auto& userIP = credentials.UserIP;
        auto tokenHash = GetSha256HexDigestLowerCase(token);
        YT_LOG_DEBUG("Authenticating user with Cypress token (TokenHash: %v, UserIP: %v)",
            tokenHash,
            userIP);

        auto path = Format("//sys/cypress_tokens/%v/@user", ToYPathLiteral(tokenHash));
        return Client_->GetNode(path, /*options*/ {})
            .Apply(BIND(
                &TCypressTokenAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(tokenHash)));
    }

private:
    const IClientPtr Client_;

    TAuthenticationResult OnCallResult(const TString& tokenHash, const TErrorOr<TYsonString>& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                YT_LOG_DEBUG(rspOrError, "Token is missing in Cypress (TokenHash: %v)",
                    tokenHash);
                THROW_ERROR_EXCEPTION("Token is missing in Cypress")
                    << TErrorAttribute("token_hash", tokenHash)
                    << rspOrError;
            } else {
                YT_LOG_DEBUG(rspOrError, "Cypress authentication failed (TokenHash: %v)",
                    tokenHash);
                THROW_ERROR_EXCEPTION("Cypress authentication failed")
                    << TErrorAttribute("token_hash", tokenHash)
                    << rspOrError;
            }
        }

        const auto& rsp = rspOrError.Value();
        return TAuthenticationResult{
            .Login = ConvertTo<TString>(rsp),
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

ITokenAuthenticatorPtr CreateCypressTokenAuthenticator(IClientPtr client)
{
    return New<TCypressTokenAuthenticator>(std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
