#include "mvp_startup_options.h"
#include "cracked_page.h"
#include "utils.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NMVP {
namespace {

void RequireNonEmptyField(TStringBuf value, TStringBuf fieldName, TStringBuf context) {
    if (value.empty()) {
        ythrow yexception() << CONFIG_ERROR_PREFIX << fieldName << " is required " << context << ".";
    }
}

NMvp::TOAuth2Exchange::TCredentials::EType InferCredsType(const NMvp::TOAuth2Exchange::TCredentials& creds) {
    using TCreds = NMvp::TOAuth2Exchange::TCredentials;
    if (!creds.GetAlg().empty() || !creds.GetPrivateKey().empty()) {
        return TCreds::JWT;
    }
    if (!creds.GetToken().empty() || !creds.GetTokenFile().empty()) {
        return TCreds::FIXED;
    }
    return TCreds::TYPE_UNSPECIFIED;
}

} // namespace

void TMvpStartupOptions::ValidateOAuth2ExchangeTokenEndpointScheme(
    const google::protobuf::RepeatedPtrField<NMvp::TOAuth2Exchange>& oauth2Exchange,
    const TString& configSource)
{
    for (const auto& tokenExchangeInfo : oauth2Exchange) {
        if (tokenExchangeInfo.GetTokenEndpoint().empty()) {
            continue;
        }
        const TCrackedPage cracked(tokenExchangeInfo.GetTokenEndpoint());
        if (!cracked.IsGrpcSchemeAllowed()) {
            ythrow yexception() << CONFIG_ERROR_PREFIX << "'token_endpoint' must use grpc in " << configSource << ".";
        }
    }
}

void TMvpStartupOptions::ValidateTokensFromConfig(const NMvp::TTokensConfig& tokensOverride) {
    if (AccessServiceTypeFromConfig && *AccessServiceTypeFromConfig != NMvp::nebius_v1) {
        ythrow yexception() << CONFIG_ERROR_PREFIX << "auth.tokens overrides are only supported for Nebius access service type";
    }
    ValidateOAuth2ExchangeTokenNames(tokensOverride.GetOAuth2Exchange(), "'auth.tokens.oauth2_exchange'");
    ValidateOAuth2ExchangeTokenEndpointScheme(tokensOverride.GetOAuth2Exchange(), "'auth.tokens.oauth2_exchange'");
}

void TMvpStartupOptions::ValidateOAuth2ExchangeTokenNames(
    const google::protobuf::RepeatedPtrField<NMvp::TOAuth2Exchange>& oauth2Exchange,
    const TString& configSource)
{
    for (const auto& tokenExchangeInfo : oauth2Exchange) {
        RequireNonEmptyField(tokenExchangeInfo.GetName(), "'name'", TStringBuilder() << "in " << configSource);
    }
}

void TMvpStartupOptions::ValidateOAuth2CredentialsFields(
    const NMvp::TOAuth2Exchange::TCredentials& creds,
    const TString& credsRole,
    const TString& tokenName)
{
    using TCreds = NMvp::TOAuth2Exchange::TCredentials;

    TCreds::EType credsType = creds.GetType();
    if (credsType == TCreds::TYPE_UNSPECIFIED) {
        credsType = InferCredsType(creds);
        if (credsType == TCreds::TYPE_UNSPECIFIED) {
            ythrow yexception() << CONFIG_ERROR_PREFIX << credsRole
                                << " credentials type is required for oauth2_exchange token " << tokenName << ".";
        }
    }

    if (credsType == TCreds::FIXED) {
        if (creds.GetToken().empty() && creds.GetTokenFile().empty()) {
            ythrow yexception() << CONFIG_ERROR_PREFIX << credsRole
                                << " credentials require either token or token_file for oauth2_exchange token " << tokenName << ".";
        }
        if (!creds.GetToken().empty() && !creds.GetTokenFile().empty()) {
            ythrow yexception() << CONFIG_ERROR_PREFIX << credsRole
                                << " credentials must not set both token and token_file for oauth2_exchange token " << tokenName << ".";
        }
        return;
    }

    if (credsType == TCreds::JWT) {
        const TString context = TStringBuilder() << "for JWT " << credsRole << " credentials in oauth2_exchange token " << tokenName;
        RequireNonEmptyField(creds.GetAlg(), "alg", context);
        RequireNonEmptyField(creds.GetPrivateKey(), "private_key", context);
        RequireNonEmptyField(creds.GetTokenType(), "token_type", context);
        RequireNonEmptyField(creds.GetIss(), "iss", context);
        return;
    }

    ythrow yexception() << CONFIG_ERROR_PREFIX << "unsupported " << credsRole
                        << " credentials type for oauth2_exchange token " << tokenName << ".";
}

void TMvpStartupOptions::ValidateTokensConfig() {
    for (const auto& tokenExchangeInfo : Tokens.GetOAuth2Exchange()) {
        RequireNonEmptyField(tokenExchangeInfo.GetTokenEndpoint(), "'token_endpoint'", "in oauth2_exchange token config");
        if (!tokenExchangeInfo.HasSubjectCredentials()) {
            ythrow yexception() << CONFIG_ERROR_PREFIX
                                << "'subject_credentials' must be specified in oauth2_exchange token config for token "
                                << tokenExchangeInfo.GetName() << ".";
        }
        ValidateOAuth2CredentialsFields(tokenExchangeInfo.GetSubjectCredentials(), "subject", tokenExchangeInfo.GetName());
        if (tokenExchangeInfo.HasActorCredentials()) {
            ValidateOAuth2CredentialsFields(tokenExchangeInfo.GetActorCredentials(), "actor", tokenExchangeInfo.GetName());
        }
    }
}

} // namespace NMVP
