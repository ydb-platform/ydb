#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/fwd.h>

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/client/ydb_types/fluent_settings_helpers.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NYdb::inline V2 {

class ITokenSource {
public:
    struct TToken {
        TString Token;

        // token type according to OAuth 2.0 token exchange protocol
	    // https://www.rfc-editor.org/rfc/rfc8693#TokenTypeIdentifiers
	    // for example urn:ietf:params:oauth:token-type:jwt
        TString TokenType;
    };

    virtual ~ITokenSource() = default;
    virtual TToken GetToken() const = 0;
};

std::shared_ptr<ITokenSource> CreateFixedTokenSource(const TString& token, const TString& tokenType);

#define FLUENT_SETTING_VECTOR_OR_SINGLE_DEPRECATED(type, name) \
    FLUENT_SETTING_VECTOR_DEPRECATED(type, name);              \
    TSelf& name(const type& value) {                \
        name##_.resize(1);                          \
        name##_[0] = value;                         \
        return static_cast<TSelf&>(*this);          \
    }

struct TOauth2TokenExchangeParams {
    using TSelf = TOauth2TokenExchangeParams;

    FLUENT_SETTING_DEPRECATED(TString, TokenEndpoint);

    FLUENT_SETTING_DEFAULT_DEPRECATED(TString, GrantType, "urn:ietf:params:oauth:grant-type:token-exchange");

    FLUENT_SETTING_VECTOR_OR_SINGLE_DEPRECATED(TString, Resource);
    FLUENT_SETTING_VECTOR_OR_SINGLE_DEPRECATED(TString, Audience);
    FLUENT_SETTING_VECTOR_OR_SINGLE_DEPRECATED(TString, Scope);

    FLUENT_SETTING_DEFAULT_DEPRECATED(TString, RequestedTokenType, "urn:ietf:params:oauth:token-type:access_token");

    FLUENT_SETTING_DEPRECATED(std::shared_ptr<ITokenSource>, SubjectTokenSource);
    FLUENT_SETTING_DEPRECATED(std::shared_ptr<ITokenSource>, ActorTokenSource);

    FLUENT_SETTING_DEFAULT_DEPRECATED(TDuration, SocketTimeout, TDuration::Seconds(5));
    FLUENT_SETTING_DEFAULT_DEPRECATED(TDuration, ConnectTimeout, TDuration::Seconds(30));
    FLUENT_SETTING_DEFAULT_DEPRECATED(TDuration, SyncUpdateTimeout, TDuration::Seconds(20));
};

// Creates OAuth 2.0 token exchange credentials provider factory that exchanges token using standard protocol
// https://www.rfc-editor.org/rfc/rfc8693
std::shared_ptr<ICredentialsProviderFactory> CreateOauth2TokenExchangeCredentialsProviderFactory(const TOauth2TokenExchangeParams& params);

} // namespace NYdb
