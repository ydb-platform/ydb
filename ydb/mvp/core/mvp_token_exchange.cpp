#include "mvp_token_exchange.h"
#include "mvp_log.h"
#include "utils.h"

#include <contrib/libs/jwt-cpp/include/jwt-cpp/jwt.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/generic/yexception.h>
#include <util/datetime/base.h>

#include <set>

namespace NMVP {
namespace {

static constexpr TStringBuf DEFAULT_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange";
static constexpr TStringBuf DEFAULT_REQUESTED_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token";
static constexpr TStringBuf JWT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt";
static constexpr TStringBuf SUBJECT_IDENTIFIER_TOKEN_TYPE = "urn:nebius:params:oauth:token-type:subject_identifier";

struct TLessNoCase {
    bool operator()(TStringBuf l, TStringBuf r) const {
        auto ll = l.length();
        auto rl = r.length();
        if (ll != rl) {
            return ll < rl;
        }
        return strnicmp(l.data(), r.data(), ll) < 0;
    }
};

using TOauthCredentials = NMvp::TOAuthExchange::TCredentials;
using TJwtSignerFn = TString(*)(const TOauthCredentials&, const std::chrono::system_clock::time_point&);

size_t Base64OutputLen(TStringBuf input) {
    while (!input.empty() && (input.back() == '=' || input.back() == ',')) {
        input.remove_suffix(1);
    }
    const size_t inputLen = input.size();
    const size_t tailEncoded = inputLen % 4;
    if (tailEncoded == 1) {
        ythrow yexception() << "invalid Base64 encoded data size: " << input.size();
    }
    const size_t mainSize = (inputLen / 4) * 3;
    size_t tailSize = 0;
    switch (tailEncoded) {
        case 2:
            tailSize = 1;
            break;
        case 3:
            tailSize = 2;
            break;
    }
    return mainSize + tailSize;
}

template <class TAlg>
TString SignJwtWithAsymmetricAlg(const TOauthCredentials& creds, const std::chrono::system_clock::time_point& now) {
    TDuration ttl = TDuration::Hours(1);
    if (!creds.ttl().empty()) {
        ttl = FromString<TDuration>(creds.ttl());
    }
    auto expiresAt = now + std::chrono::seconds(ttl.Seconds());
    auto builder = jwt::create()
        .set_issued_at(now)
        .set_expires_at(expiresAt);
    if (!creds.kid().empty()) {
        builder.set_key_id(creds.kid());
    }
    if (!creds.iss().empty()) {
        builder.set_issuer(creds.iss());
    }
    if (!creds.sub().empty()) {
        builder.set_subject(creds.sub());
    }
    if (!creds.jti().empty()) {
        builder.set_id(creds.jti());
    }
    if (creds.aud_size() > 0) {
        std::set<std::string> audience;
        for (const auto& aud : creds.aud()) {
            audience.insert(aud);
        }
        builder.set_audience(audience);
    }
    auto alg = TAlg("", creds.privatekey());
    return TString(builder.sign(alg));
}

template <class TAlg>
TString SignJwtWithHmacAlg(const TOauthCredentials& creds, const std::chrono::system_clock::time_point& now) {
    TDuration ttl = TDuration::Hours(1);
    if (!creds.ttl().empty()) {
        ttl = FromString<TDuration>(creds.ttl());
    }
    auto expiresAt = now + std::chrono::seconds(ttl.Seconds());
    auto builder = jwt::create()
        .set_issued_at(now)
        .set_expires_at(expiresAt);
    if (!creds.kid().empty()) {
        builder.set_key_id(creds.kid());
    }
    if (!creds.iss().empty()) {
        builder.set_issuer(creds.iss());
    }
    if (!creds.sub().empty()) {
        builder.set_subject(creds.sub());
    }
    if (!creds.jti().empty()) {
        builder.set_id(creds.jti());
    }
    if (creds.aud_size() > 0) {
        std::set<std::string> audience;
        for (const auto& aud : creds.aud()) {
            audience.insert(aud);
        }
        builder.set_audience(audience);
    }

    const size_t expectedLen = Base64OutputLen(creds.privatekey());
    TString decodedKey;
    decodedKey.resize(Base64DecodeBufSize(creds.privatekey().size()));
    const size_t decodedBytes = Base64DecodeUneven(const_cast<char*>(decodedKey.data()), creds.privatekey());
    if (decodedBytes != expectedLen) {
        ythrow yexception() << "failed to decode HMAC secret from Base64";
    }
    decodedKey.resize(decodedBytes);

    auto alg = TAlg(decodedKey);
    return TString(builder.sign(alg));
}

const std::map<TString, TJwtSignerFn, TLessNoCase> JwtAlgorithmsFactory = {
    {"RS256", &SignJwtWithAsymmetricAlg<jwt::algorithm::rs256>},
    {"RS384", &SignJwtWithAsymmetricAlg<jwt::algorithm::rs384>},
    {"RS512", &SignJwtWithAsymmetricAlg<jwt::algorithm::rs512>},
    {"ES256", &SignJwtWithAsymmetricAlg<jwt::algorithm::es256>},
    {"ES384", &SignJwtWithAsymmetricAlg<jwt::algorithm::es384>},
    {"ES512", &SignJwtWithAsymmetricAlg<jwt::algorithm::es512>},
    {"PS256", &SignJwtWithAsymmetricAlg<jwt::algorithm::ps256>},
    {"PS384", &SignJwtWithAsymmetricAlg<jwt::algorithm::ps384>},
    {"PS512", &SignJwtWithAsymmetricAlg<jwt::algorithm::ps512>},
    {"HS256", &SignJwtWithHmacAlg<jwt::algorithm::hs256>},
    {"HS384", &SignJwtWithHmacAlg<jwt::algorithm::hs384>},
    {"HS512", &SignJwtWithHmacAlg<jwt::algorithm::hs512>},
};

TString JoinAlgorithmsForError() {
    TStringBuilder sb;
    bool first = true;
    for (const auto& [alg, _] : JwtAlgorithmsFactory) {
        if (!first) {
            sb << ", ";
        }
        first = false;
        sb << alg;
    }
    return sb;
}

bool ResolveFixedTokenValue(const TOauthCredentials& creds, const TString& tokenName, TString& token, TString& error) {
    if (!creds.token().empty()) {
        token = creds.token();
        if (!creds.tokenfile().empty()) {
            BLOG_D("Both token and token_file are set for token " << tokenName << ", token will be used");
        }
        return true;
    }
    if (!creds.tokenfile().empty()) {
        return NMVP::TryLoadTokenFromFile(creds.tokenfile(), token, error, tokenName);
    }
    error = TStringBuilder() << "OAuth2 token exchange credentials require either token or token_file for token " << tokenName;
    return false;
}

TOauthCredentials::EType InferCredsType(const TOauthCredentials& creds) {
    if (!creds.alg().empty() || !creds.privatekey().empty()) {
        return TOauthCredentials::JWT;
    }
    if (!creds.token().empty() || !creds.tokenfile().empty()) {
        return TOauthCredentials::FIXED;
    }
    return TOauthCredentials::TYPE_UNSPECIFIED;
}

} // anonymous namespace

bool BuildTokenExchangeRequestFromConfig(const NMvp::TOAuthExchange* tokenExchangeInfo,
                                         nebius::iam::v1::ExchangeTokenRequest& request,
                                         TString& error) {
    request.set_grant_type(tokenExchangeInfo->granttype().empty() ? TString(DEFAULT_GRANT_TYPE) : tokenExchangeInfo->granttype());
    request.set_requested_token_type(tokenExchangeInfo->requestedtokentype().empty() ? TString(DEFAULT_REQUESTED_TOKEN_TYPE) : tokenExchangeInfo->requestedtokentype());

    if (!tokenExchangeInfo->aud().empty()) {
        request.set_audience(tokenExchangeInfo->aud(0));
        if (tokenExchangeInfo->aud_size() > 1) {
            BLOG_D("Only first aud value is used for token " << tokenExchangeInfo->name());
        }
    }
    for (const auto& scope : tokenExchangeInfo->scope()) {
        request.add_scopes(scope);
    }
    for (const auto& res : tokenExchangeInfo->res()) {
        request.add_resource(res);
    }

    auto setCreds = [&](const TOauthCredentials& creds, bool subject) -> bool {
        TString tokenValue;
        TString tokenType;
        TOauthCredentials::EType credsType = creds.type();
        if (credsType == TOauthCredentials::TYPE_UNSPECIFIED) {
            credsType = InferCredsType(creds);
            if (credsType == TOauthCredentials::TYPE_UNSPECIFIED) {
                error = TStringBuilder() << "OAuth2 token exchange credentials type is required for token " << tokenExchangeInfo->name();
                return false;
            }
        }

        if (credsType == TOauthCredentials::FIXED) {
            if (!ResolveFixedTokenValue(creds, tokenExchangeInfo->name(), tokenValue, error)) {
                return false;
            }
            tokenType = creds.tokentype();
            if (tokenType.empty()) {
                tokenType = subject ? TString(SUBJECT_IDENTIFIER_TOKEN_TYPE) : TString(JWT_TOKEN_TYPE);
            }
        } else if (credsType == TOauthCredentials::JWT) {
            if (creds.alg().empty()) {
                error = TStringBuilder() << "alg is required for JWT credentials for token " << tokenExchangeInfo->name();
                return false;
            }
            if (creds.privatekey().empty()) {
                error = TStringBuilder() << "private-key is required for JWT credentials for token " << tokenExchangeInfo->name();
                return false;
            }

            auto algIt = JwtAlgorithmsFactory.find(creds.alg());
            if (algIt == JwtAlgorithmsFactory.end()) {
                error = TStringBuilder()
                    << "Algorithm \"" << creds.alg() << "\" is not supported for token " << tokenExchangeInfo->name()
                    << ". Supported algorithms are: " << JoinAlgorithmsForError();
                return false;
            }
            try {
                tokenValue = algIt->second(creds, std::chrono::system_clock::now());
            } catch (const std::exception& ex) {
                error = TStringBuilder() << "Failed to build JWT credentials for token " << tokenExchangeInfo->name() << ": " << ex.what();
                return false;
            }
            tokenType = creds.tokentype().empty() ? TString(JWT_TOKEN_TYPE) : creds.tokentype();
        } else {
            error = TStringBuilder() << "Unsupported OAuth2 token exchange credentials type for token "
                << tokenExchangeInfo->name() << ". Supported types are JWT and FIXED";
            return false;
        }

        if (subject) {
            request.set_subject_token(tokenValue);
            request.set_subject_token_type(tokenType);
        } else {
            request.set_actor_token(tokenValue);
            request.set_actor_token_type(tokenType);
        }
        return true;
    };

    if (!tokenExchangeInfo->HasSubjectCredentials()) {
        error = TStringBuilder() << "subject-credentials are required for token " << tokenExchangeInfo->name();
        return false;
    }
    if (!setCreds(tokenExchangeInfo->subjectcredentials(), true)) {
        return false;
    }
    if (tokenExchangeInfo->HasActorCredentials() && !setCreds(tokenExchangeInfo->actorcredentials(), false)) {
        return false;
    }

    return true;
}

} // namespace NMVP
