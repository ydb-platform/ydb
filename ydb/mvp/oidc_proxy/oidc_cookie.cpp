#include "oidc_cookie.h"

#include "oidc_settings.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NMVP::NOIDC {

namespace {

struct TOidcCookieData {
    TVector<TString> Tokens;
    TString RequestedAddress;
};

TString HmacSHA256(TStringBuf key, TStringBuf data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    ui32 hashLength = SHA256_DIGEST_LENGTH;
    const auto* result = HMAC(
        EVP_sha256(),
        key.data(),
        key.size(),
        reinterpret_cast<const unsigned char*>(data.data()),
        data.size(),
        hash,
        &hashLength
    );
    Y_ENSURE(result);
    Y_ENSURE(hashLength == SHA256_DIGEST_LENGTH);
    return TString(reinterpret_cast<const char*>(result), hashLength);
}

void NormalizeOidcCookieData(TOidcCookieData& data) {
    THashSet<TString> seen;
    TVector<TString> normalizedTokens;

    normalizedTokens.reserve(data.Tokens.size());
    for (const TString& token : data.Tokens) {
        if (token.empty()) {
            continue;
        }
        if (!seen.insert(token).second) {
            continue;
        }
        normalizedTokens.push_back(token);
    }

    data.Tokens = std::move(normalizedTokens);
}

NJson::TJsonValue BuildOidcCookieDataJson(const TOidcCookieData& data) {
    NJson::TJsonValue root(NJson::JSON_MAP);
    NJson::TJsonValue tokens(NJson::JSON_ARRAY);
    for (const TString& token : data.Tokens) {
        tokens.AppendValue(token);
    }
    root["tokens"] = std::move(tokens);
    if (!data.RequestedAddress.empty()) {
        root["requested_address"] = data.RequestedAddress;
    }
    return root;
}

TString BuildOidcCookieValue(const TOidcCookieData& data, const TString& key) {
    const NJson::TJsonValue oidcCookieData = BuildOidcCookieDataJson(data);
    const TString oidcCookieDataJson = NJson::WriteJson(oidcCookieData, false);

    NJson::TJsonValue root(NJson::JSON_MAP);
    root["data"] = oidcCookieData;
    root["digest"] = Base64Encode(HmacSHA256(key, oidcCookieDataJson));
    return Base64Encode(NJson::WriteJson(root, false));
}

bool TryParseOidcCookieDataJson(const NJson::TJsonValue& jsonValue, TOidcCookieData& data) {
    const NJson::TJsonValue* jsonTokens = nullptr;
    if (!jsonValue.GetValuePointer("tokens", &jsonTokens) || jsonTokens->GetType() != NJson::JSON_ARRAY) {
        return false;
    }

    data.Tokens.clear();
    data.RequestedAddress.clear();
    for (const NJson::TJsonValue& tokenValue : jsonTokens->GetArraySafe()) {
        if (tokenValue.IsString()) {
            data.Tokens.push_back(tokenValue.GetStringSafe());
        }
    }

    const NJson::TJsonValue* jsonRequestedAddress = nullptr;
    if (jsonValue.GetValuePointer("requested_address", &jsonRequestedAddress) && jsonRequestedAddress->IsString()) {
        data.RequestedAddress = jsonRequestedAddress->GetString();
    }

    NormalizeOidcCookieData(data);
    return true;
}

bool TryDecodeCompactOidcCookieValue(const NJson::TJsonValue& signedCookieJson,
                                     const TString& key,
                                     TOidcCookieData& data) {
    const NJson::TJsonValue* jsonData = nullptr;
    if (!signedCookieJson.GetValuePointer("data", &jsonData) || jsonData->GetType() != NJson::JSON_MAP) {
        return false;
    }

    TString expectedDigest;
    const NJson::TJsonValue* jsonDigest = nullptr;
    if (signedCookieJson.GetValuePointer("digest", &jsonDigest) && jsonDigest->IsString()) {
        try {
            expectedDigest = Base64Decode(jsonDigest->GetString());
        } catch (const std::exception&) {
            return false;
        }
    }
    if (expectedDigest.empty()) {
        return false;
    }

    const TString dataJson = NJson::WriteJson(*jsonData, false);
    const TString digest = HmacSHA256(key, dataJson);
    if (expectedDigest != digest) {
        return false;
    }

    return TryParseOidcCookieDataJson(*jsonData, data);
}

bool TryDecodeContainerOidcCookieValue(const NJson::TJsonValue& signedCookieJson,
                                       const TString& key,
                                       TOidcCookieData& data) {
    TString oidcCookieContainer;
    const NJson::TJsonValue* jsonContainer = nullptr;
    if (signedCookieJson.GetValuePointer("container", &jsonContainer) && jsonContainer->IsString()) {
        try {
            oidcCookieContainer = Base64Decode(jsonContainer->GetString());
        } catch (const std::exception&) {
            return false;
        }
    }
    if (oidcCookieContainer.empty()) {
        return false;
    }

    TString expectedDigest;
    const NJson::TJsonValue* jsonDigest = nullptr;
    if (signedCookieJson.GetValuePointer("digest", &jsonDigest) && jsonDigest->IsString()) {
        try {
            expectedDigest = Base64Decode(jsonDigest->GetString());
        } catch (const std::exception&) {
            return false;
        }
    }
    if (expectedDigest.empty()) {
        return false;
    }

    const TString digest = HmacSHA256(key, oidcCookieContainer);
    if (expectedDigest != digest) {
        return false;
    }

    NJson::TJsonValue jsonValue;
    NJson::TJsonReaderConfig jsonConfig;
    if (!NJson::ReadJsonTree(oidcCookieContainer, &jsonConfig, &jsonValue)) {
        return false;
    }

    return TryParseOidcCookieDataJson(jsonValue, data);
}

bool TryDecodeOidcCookieValue(TStringBuf cookieValue,
                              const TString& key,
                              TOidcCookieData& data) {
    if (cookieValue.empty()) {
        return false;
    }

    TString signedCookie;
    try {
        signedCookie = Base64Decode(cookieValue);
    } catch (const std::exception&) {
        return false;
    }

    NJson::TJsonValue jsonValue;
    NJson::TJsonReaderConfig jsonConfig;
    if (!NJson::ReadJsonTree(signedCookie, &jsonConfig, &jsonValue)) {
        return false;
    }

    if (TryDecodeCompactOidcCookieValue(jsonValue, key, data)) {
        return true;
    }

    return TryDecodeContainerOidcCookieValue(jsonValue, key, data);
}

void TouchToken(TOidcCookieData& data, TStringBuf antiForgeryToken) {
    TVector<TString> updatedTokens;
    updatedTokens.reserve(data.Tokens.size() + 1);
    updatedTokens.push_back(TString(antiForgeryToken));
    for (const TString& token : data.Tokens) {
        if (token != antiForgeryToken) {
            updatedTokens.push_back(token);
        }
    }
    data.Tokens = std::move(updatedTokens);
}

void RemoveToken(TOidcCookieData& data, TStringBuf antiForgeryToken) {
    TVector<TString> updatedTokens;
    updatedTokens.reserve(data.Tokens.size());
    for (const TString& token : data.Tokens) {
        if (token != antiForgeryToken) {
            updatedTokens.push_back(token);
        }
    }
    data.Tokens = std::move(updatedTokens);
}

} // anonymous namespace

TString CreateAuthFlowCookie(TStringBuf cookieValue) {
    return TStringBuilder()
        << TOpenIdConnectSettings::YDB_OIDC_COOKIE << "=" << cookieValue
        << "; Path=/auth; Secure; HttpOnly; SameSite=None"
        << "; Max-Age=" << TOpenIdConnectSettings::DEFAULT_AUTH_STATE_LIFETIME.Seconds();
}

TString ClearAuthFlowCookie() {
    return TStringBuilder()
        << TOpenIdConnectSettings::YDB_OIDC_COOKIE
        << "=; Path=/auth; Secure; HttpOnly; SameSite=None; Max-Age=0";
}

TString UpdateAuthFlowCookieValue(TStringBuf currentCookieValue,
                                  const TString& key,
                                  TStringBuf antiForgeryToken,
                                  TStringBuf requestedAddress) {
    TOidcCookieData data;
    TryDecodeOidcCookieValue(currentCookieValue, key, data);

    data.RequestedAddress = TString(requestedAddress);
    TouchToken(data, antiForgeryToken);

    TString cookieValue = BuildOidcCookieValue(data, key);
    while (cookieValue.size() > TOpenIdConnectSettings::MAX_AUTH_START_COOKIE_VALUE_SIZE && data.Tokens.size() > 1) {
        data.Tokens.pop_back();
        cookieValue = BuildOidcCookieValue(data, key);
    }

    return cookieValue;
}

bool HasAuthFlowCookieNonce(TStringBuf cookieValue, const TString& key, TStringBuf antiForgeryToken) {
    TOidcCookieData data;
    if (!TryDecodeOidcCookieValue(cookieValue, key, data)) {
        return false;
    }

    for (const TString& token : data.Tokens) {
        if (token == antiForgeryToken) {
            return true;
        }
    }

    return false;
}

TString RemoveAuthFlowCookieNonce(TStringBuf currentCookieValue, const TString& key, TStringBuf antiForgeryToken) {
    TOidcCookieData data;
    if (!TryDecodeOidcCookieValue(currentCookieValue, key, data)) {
        return {};
    }

    RemoveToken(data, antiForgeryToken);
    if (data.Tokens.empty()) {
        return {};
    }

    return BuildOidcCookieValue(data, key);
}

TString RestoreRequestedAddressFromAuthFlowCookie(TStringBuf cookieValue, const TString& key) {
    TOidcCookieData data;
    if (!TryDecodeOidcCookieValue(cookieValue, key, data)) {
        return {};
    }

    return data.RequestedAddress;
}

} // NMVP::NOIDC
