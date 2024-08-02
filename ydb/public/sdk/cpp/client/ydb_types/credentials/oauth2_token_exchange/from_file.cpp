#include "from_file.h"
#include "credentials.h"
#include "jwt_token_source.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/map.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <jwt-cpp/jwt.h>

namespace NYdb {

namespace {

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

template <class TAlg>
void ApplyAsymmetricAlg(TJwtTokenSourceParams* params, const TString& privateKey) {
    // Alg with first param as public key, second param as private key
    params->SigningAlgorithm<TAlg>(std::string{}, privateKey);
}

size_t Base64OutputLen(TStringBuf input) {
    while (input && (input.back() == '=' || input.back() == ',')) { // padding
        input.remove_suffix(1);
    }
    const size_t inputLen = input.size();
    const size_t tailEncoded = inputLen % 4;
    if (tailEncoded == 1) {
        throw std::runtime_error(TStringBuilder() << "invalid Base64 encoded data size: " << input.size());
    }
    const size_t mainSize = (inputLen / 4) * 3;
    size_t tailSize = 0;
    switch (tailEncoded) {
        case 2: // 12 bit => 1 byte
            tailSize = 1;
            break;
        case 3: // 18 bits -> 2 bytes
            tailSize = 2;
            break;
    }
    return mainSize + tailSize;
}

template <class TAlg>
void ApplyHmacAlg(TJwtTokenSourceParams* params, const TString& key) {
    // HMAC keys are encoded in base64 encoding
    const size_t base64OutputSize = Base64OutputLen(key); // throws
    TString binaryKey;
    binaryKey.ReserveAndResize(Base64DecodeBufSize(key.size()));
    // allows strings without padding
    const size_t decodedBytes = Base64DecodeUneven(const_cast<char*>(binaryKey.data()), key);
    if (decodedBytes != base64OutputSize) {
        throw std::runtime_error("failed to decode HMAC secret from Base64");
    }
    binaryKey.resize(decodedBytes);
    // Alg with first param as key
    params->SigningAlgorithm<TAlg>(binaryKey);
}

const TMap<TString, void(*)(TJwtTokenSourceParams*, const TString& privateKey), TLessNoCase> JwtAlgorithmsFactory = {
    {"RS256", &ApplyAsymmetricAlg<jwt::algorithm::rs256>},
    {"RS384", &ApplyAsymmetricAlg<jwt::algorithm::rs384>},
    {"RS512", &ApplyAsymmetricAlg<jwt::algorithm::rs512>},
    {"ES256", &ApplyAsymmetricAlg<jwt::algorithm::es256>},
    {"ES384", &ApplyAsymmetricAlg<jwt::algorithm::es384>},
    {"ES512", &ApplyAsymmetricAlg<jwt::algorithm::es512>},
    {"PS256", &ApplyAsymmetricAlg<jwt::algorithm::ps256>},
    {"PS384", &ApplyAsymmetricAlg<jwt::algorithm::ps384>},
    {"PS512", &ApplyAsymmetricAlg<jwt::algorithm::ps512>},
    {"HS256", &ApplyHmacAlg<jwt::algorithm::hs256>},
    {"HS384", &ApplyHmacAlg<jwt::algorithm::hs384>},
    {"HS512", &ApplyHmacAlg<jwt::algorithm::hs512>},
};

bool IsAsciiEqualUpper(const TString& jsonParam, const TStringBuf& constantInUpperCase) {
    if (jsonParam.size() != constantInUpperCase.size()) {
        return false;
    }
    for (size_t i = 0; i < constantInUpperCase.size(); ++i) {
        char c = jsonParam[i];
        if (c >= 'a' && c <= 'z') {
            c += 'A' - 'a';
        }
        if (c != constantInUpperCase[i]) {
            return false;
        }
    }
    return true;
}

// throws if not string
#define PROCESS_JSON_STRING_PARAM(jsonParamName, paramName, required)      \
    try {                                                                  \
        if (const NJson::TJsonValue* value = map.FindPtr(jsonParamName)) { \
            result.paramName(value->GetStringSafe());                      \
        } else if (required) {                                             \
            throw std::runtime_error(                                      \
                "No \"" jsonParamName "\" parameter");                     \
        }                                                                  \
    } catch (const std::exception& ex) {                                   \
        throw std::runtime_error(TStringBuilder()                          \
            << "Failed to parse \"" jsonParamName "\": " << ex.what());    \
    }

// throws if not string or array of strings
#define PROCESS_JSON_ARRAY_PARAM(jsonParamName, paramName)                 \
    try {                                                                  \
        if (const NJson::TJsonValue* value = map.FindPtr(jsonParamName)) { \
            if (value->IsArray()) {                                        \
                for (const NJson::TJsonValue& v : value->GetArraySafe()) { \
                    result.Append ## paramName(v.GetStringSafe());         \
                }                                                          \
            } else {                                                       \
                result.paramName(value->GetStringSafe());                  \
            }                                                              \
        }                                                                  \
    } catch (const std::exception& ex) {                                   \
        throw std::runtime_error(TStringBuilder()                          \
            << "Failed to parse \"" jsonParamName "\": " << ex.what());    \
    }

#define PROCESS_CREDS_PARAM(jsonParamName, paramName)                      \
    try {                                                                  \
        if (const NJson::TJsonValue* value = map.FindPtr(jsonParamName)) { \
            result.paramName(ParseTokenSource(*value));                    \
        }                                                                  \
    } catch (const std::exception& ex) {                                   \
        throw std::runtime_error(TStringBuilder()                          \
            << "Failed to parse \"" jsonParamName "\": " << ex.what());    \
    }

// special struct to apply macros
struct TFixedTokenSourceParamsForParsing {
    using TSelf = TFixedTokenSourceParamsForParsing;

    FLUENT_SETTING(TString, Token);
    FLUENT_SETTING(TString, TokenType);
};

// special struct to apply macros
struct TJwtTokenSourceParamsForParsing : public TJwtTokenSourceParams {
    FLUENT_SETTING(TString, AlgStr);
    FLUENT_SETTING(TString, PrivateKeyStr);
    FLUENT_SETTING(TString, TtlStr);
};

std::shared_ptr<ITokenSource> ParseFixedTokenSource(const NJson::TJsonValue& cfg) {
    const auto& map = cfg.GetMapSafe();
    TFixedTokenSourceParamsForParsing result;
    PROCESS_JSON_STRING_PARAM("token", Token, true); // required
    PROCESS_JSON_STRING_PARAM("token-type", TokenType, true); // required
    return CreateFixedTokenSource(result.Token_, result.TokenType_);
}

std::shared_ptr<ITokenSource> ParseJwtTokenSource(const NJson::TJsonValue& cfg) {
    const auto& map = cfg.GetMapSafe();
    TJwtTokenSourceParamsForParsing result;
    PROCESS_JSON_STRING_PARAM("kid", KeyId, false);
    PROCESS_JSON_STRING_PARAM("iss", Issuer, false);
    PROCESS_JSON_STRING_PARAM("sub", Subject, false);
    PROCESS_JSON_ARRAY_PARAM("aud", Audience);
    PROCESS_JSON_STRING_PARAM("jti", Id, false);

    // special fields
    PROCESS_JSON_STRING_PARAM("ttl", TtlStr, false);
    PROCESS_JSON_STRING_PARAM("alg", AlgStr, true); // required
    PROCESS_JSON_STRING_PARAM("private-key", PrivateKeyStr, true); // required

    try {
        if (result.TtlStr_) {
            result.TokenTtl(FromString<TDuration>(result.TtlStr_));
        }
    } catch (const std::exception& ex) {
        throw std::runtime_error(TStringBuilder()
            << "Failed to parse \"ttl\": " << ex.what());
    }

    const auto jwtAlgIt = JwtAlgorithmsFactory.find(result.AlgStr_);
    if (jwtAlgIt == JwtAlgorithmsFactory.end()) {
        TStringBuilder err;
        err << "Algorithm \"" << result.AlgStr_ << "\" is not supported. Supported algorithms are: ";
        bool first = true;
        for (const TString& alg : GetSupportedOauth2TokenExchangeJwtAlgorithms()) {
            if (!first) {
                err << ", ";
            }
            first = false;
            err << alg;
        }
        throw std::runtime_error(err);
    }
    jwtAlgIt->second(&result, result.PrivateKeyStr_);
    return CreateJwtTokenSource(result);
}

std::shared_ptr<ITokenSource> ParseTokenSource(const NJson::TJsonValue& cfg) {
    const auto& map = cfg.GetMapSafe();
    const NJson::TJsonValue* type = map.FindPtr("type");
    if (!type) {
        throw std::runtime_error("No \"type\" parameter");
    }
    if (IsAsciiEqualUpper(type->GetStringSafe(), "JWT")) {
        return ParseJwtTokenSource(cfg);
    } else if (IsAsciiEqualUpper(type->GetStringSafe(), "FIXED")) {
        return ParseFixedTokenSource(cfg);
    } else {
        throw std::runtime_error("Incorrect \"type\" parameter: only \"JWT\" and \"FIXED\" are supported");
    }
}

TOauth2TokenExchangeParams ReadOauth2ConfigJson(const TString& configJson, const TString& tokenEndpoint) {
    try {
        NJson::TJsonValue cfg;
        NJson::ReadJsonTree(configJson, &cfg, true);
        const auto& map = cfg.GetMapSafe();

        TOauth2TokenExchangeParams result;
        if (tokenEndpoint) {
            result.TokenEndpoint(tokenEndpoint);
        } else {
            PROCESS_JSON_STRING_PARAM("token-endpoint", TokenEndpoint, false); // there is an explicit check in provider params after parsing
        }

        PROCESS_JSON_STRING_PARAM("grant-type", GrantType, false);
        PROCESS_JSON_ARRAY_PARAM("res", Resource);
        PROCESS_JSON_STRING_PARAM("requested-token-type", RequestedTokenType, false);
        PROCESS_JSON_ARRAY_PARAM("aud", Audience);
        PROCESS_JSON_ARRAY_PARAM("scope", Scope);
        PROCESS_CREDS_PARAM("subject-credentials", SubjectTokenSource);
        PROCESS_CREDS_PARAM("actor-credentials", ActorTokenSource);
        return result;
    } catch (const std::exception& ex) {
        throw std::runtime_error(TStringBuilder() << "Failed to parse config file for OAuth 2.0 token exchange: " << ex.what());
    }
}

TOauth2TokenExchangeParams ReadOauth2ConfigFile(const TString& configFilePath, const TString& tokenEndpoint) {
    return ReadOauth2ConfigJson(TFileInput(configFilePath).ReadAll(), tokenEndpoint);
}

} // namespace

std::vector<TString> GetSupportedOauth2TokenExchangeJwtAlgorithms() {
    std::vector<TString> result;
    result.reserve(JwtAlgorithmsFactory.size());
    for (const auto& [alg, _] : JwtAlgorithmsFactory) {
        result.push_back(alg);
    }
    return result;
}

std::shared_ptr<ICredentialsProviderFactory> CreateOauth2TokenExchangeFileCredentialsProviderFactory(const TString& configFilePath, const TString& tokenEndpoint) {
    return CreateOauth2TokenExchangeCredentialsProviderFactory(ReadOauth2ConfigFile(configFilePath, tokenEndpoint));
}

} // namespace NYdb
