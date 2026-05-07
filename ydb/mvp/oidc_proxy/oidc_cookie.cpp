#include "oidc_cookie.h"

#include "oidc_settings.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/hex.h>

namespace NMVP::NOIDC {

namespace {

struct TOidcCookieData {
    THashMap<TString, TString> ReturnTo;
    TVector<TString> Recent;
};

TString HmacSHA256(TStringBuf key, TStringBuf data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    ui32 hl = SHA256_DIGEST_LENGTH;
    const auto* res = HMAC(EVP_sha256(), key.data(), key.size(), reinterpret_cast<const unsigned char*>(data.data()), data.size(), hash, &hl);
    Y_ENSURE(res);
    Y_ENSURE(hl == SHA256_DIGEST_LENGTH);
    return TString{reinterpret_cast<const char*>(res), hl};
}

void NormalizeOidcCookieData(TOidcCookieData& data) {
    THashSet<TString> seen;
    TVector<TString> normalizedRecent;

    normalizedRecent.reserve(data.ReturnTo.size());
    for (const TString& flowId : data.Recent) {
        if (!data.ReturnTo.contains(flowId) || !seen.insert(flowId).second) {
            continue;
        }
        normalizedRecent.push_back(flowId);
    }

    for (const auto& entry : data.ReturnTo) {
        const TString& flowId = entry.first;
        if (seen.insert(flowId).second) {
            normalizedRecent.push_back(flowId);
        }
    }

    data.Recent = std::move(normalizedRecent);
}

TString BuildOidcCookieContainer(const TOidcCookieData& data) {
    NJson::TJsonValue root(NJson::JSON_MAP);
    NJson::TJsonValue returnTo(NJson::JSON_MAP);
    NJson::TJsonValue recent(NJson::JSON_ARRAY);

    for (const auto& [flowId, requestedAddress] : data.ReturnTo) {
        returnTo[flowId] = requestedAddress;
    }
    for (const TString& flowId : data.Recent) {
        recent.AppendValue(flowId);
    }

    root["return_to"] = std::move(returnTo);
    root["recent"] = std::move(recent);
    return NJson::WriteJson(root, false);
}

TString BuildOidcCookieValue(const TOidcCookieData& data, const TString& key) {
    const TString oidcCookieContainer = BuildOidcCookieContainer(data);

    NJson::TJsonValue root(NJson::JSON_MAP);
    root["container"] = Base64Encode(oidcCookieContainer);
    root["digest"] = Base64Encode(HmacSHA256(key, oidcCookieContainer));
    return Base64Encode(NJson::WriteJson(root, false));
}

bool TryDecodeOidcCookieValue(TStringBuf cookieValue,
                              const TString& key,
                              TOidcCookieData& data,
                              TString& errorMessage) {
    TString signedCookie;
    try {
        signedCookie = Base64Decode(cookieValue);
    } catch (const std::exception&) {
        errorMessage = "OIDC context cookie payload is not valid base64";
        return false;
    }

    TString oidcCookieContainer;
    TString expectedDigest;
    NJson::TJsonValue jsonValue;
    NJson::TJsonReaderConfig jsonConfig;
    if (!NJson::ReadJsonTree(signedCookie, &jsonConfig, &jsonValue)) {
        errorMessage = "OIDC context cookie payload is not valid JSON";
        return false;
    }

    const NJson::TJsonValue* jsonContainer = nullptr;
    if (jsonValue.GetValuePointer("container", &jsonContainer) && jsonContainer->IsString()) {
        try {
            oidcCookieContainer = Base64Decode(jsonContainer->GetString());
        } catch (const std::exception&) {
            errorMessage = "OIDC context cookie container is not valid base64";
            return false;
        }
    }
    if (oidcCookieContainer.empty()) {
        errorMessage = "OIDC context cookie container is missing";
        return false;
    }

    const NJson::TJsonValue* jsonDigest = nullptr;
    if (jsonValue.GetValuePointer("digest", &jsonDigest) && jsonDigest->IsString()) {
        try {
            expectedDigest = Base64Decode(jsonDigest->GetString());
        } catch (const std::exception&) {
            errorMessage = "OIDC context cookie digest is not valid base64";
            return false;
        }
    }
    if (expectedDigest.empty()) {
        errorMessage = "OIDC context cookie digest is missing";
        return false;
    }

    const TString digest = HmacSHA256(key, oidcCookieContainer);
    if (expectedDigest != digest) {
        errorMessage = "OIDC context cookie digest mismatch";
        return false;
    }

    if (!NJson::ReadJsonTree(oidcCookieContainer, &jsonConfig, &jsonValue)) {
        errorMessage = "OIDC context cookie container is not valid JSON";
        return false;
    }

    const NJson::TJsonValue* jsonReturnTo = nullptr;
    if (!jsonValue.GetValuePointer("return_to", &jsonReturnTo) || jsonReturnTo->GetType() != NJson::JSON_MAP) {
        errorMessage = "OIDC context cookie `return_to` map is missing";
        return false;
    }

    data.ReturnTo.clear();
    for (const auto& [flowId, requestedAddressValue] : jsonReturnTo->GetMapSafe()) {
        if (requestedAddressValue.IsString()) {
            data.ReturnTo[flowId] = requestedAddressValue.GetStringSafe();
        }
    }
    if (data.ReturnTo.empty()) {
        errorMessage = "OIDC context cookie `return_to` map is empty";
        return false;
    }

    data.Recent.clear();
    const NJson::TJsonValue* jsonRecent = nullptr;
    if (jsonValue.GetValuePointer("recent", &jsonRecent) && jsonRecent->GetType() == NJson::JSON_ARRAY) {
        for (const NJson::TJsonValue& recentFlowId : jsonRecent->GetArraySafe()) {
            if (recentFlowId.IsString()) {
                data.Recent.push_back(recentFlowId.GetStringSafe());
            }
        }
    }

    NormalizeOidcCookieData(data);
    return true;
}

void RemoveFlowIdFromRecent(TVector<TString>& recent, TStringBuf flowId) {
    TVector<TString> filteredRecent;
    filteredRecent.reserve(recent.size());

    for (const TString& recentFlowId : recent) {
        if (recentFlowId != flowId) {
            filteredRecent.push_back(recentFlowId);
        }
    }

    recent = std::move(filteredRecent);
}

void TouchFlowId(TOidcCookieData& data, TStringBuf flowId) {
    RemoveFlowIdFromRecent(data.Recent, flowId);
    data.Recent.insert(data.Recent.begin(), TString(flowId));
}

TMaybe<TString> FindRequestedAddress(const TOidcCookieData& data, TStringBuf flowId) {
    if (!flowId.empty()) {
        if (auto it = data.ReturnTo.find(TString(flowId)); it != data.ReturnTo.end()) {
            return it->second;
        }
    }

    for (const TString& recentFlowId : data.Recent) {
        if (auto it = data.ReturnTo.find(recentFlowId); it != data.ReturnTo.end()) {
            return it->second;
        }
    }

    return Nothing();
}

TString FindFallbackFlowIdToEvict(const TVector<TString>& recent, TStringBuf currentFlowId) {
    for (auto it = recent.rbegin(); it != recent.rend(); ++it) {
        if (*it != currentFlowId) {
            return *it;
        }
    }
    return {};
}

} // anonymous namespace

TString CreateFlowId(TStringBuf key, TStringBuf requestedAddress) {
    static constexpr size_t FLOW_ID_BYTES = 5;

    const TString digest = HmacSHA256(key, requestedAddress);
    return HexEncode(TStringBuf(digest.data(), FLOW_ID_BYTES));
}

TString CreateSharedOidcCookieName() {
    return TString(TOpenIdConnectSettings::SHARED_OIDC_COOKIE);
}

TString CreateSharedOidcCookie(const TString& key, TStringBuf currentCookieValue, TStringBuf requestedAddress) {
    return TStringBuilder() << CreateSharedOidcCookieName() << "="
                            << UpdateSharedOidcCookieValue(currentCookieValue, key, CreateFlowId(key, requestedAddress), requestedAddress) << ";"
                            << " Path=/auth;"
                            " Max-Age=" << TOpenIdConnectSettings::DEFAULT_OIDC_FLOW_LIFETIME.Seconds() << ";"
                            " SameSite=None; Secure";
}

bool HasSharedOidcCookieEntry(TStringBuf cookieValue, const TString& key, TStringBuf flowId, TStringBuf requestedAddress) {
    if (cookieValue.empty()) {
        return false;
    }

    TOidcCookieData data;
    TString errorMessage;
    if (!TryDecodeOidcCookieValue(cookieValue, key, data, errorMessage)) {
        return false;
    }

    if (auto it = data.ReturnTo.find(TString(flowId)); it != data.ReturnTo.end()) {
        return it->second == requestedAddress;
    }

    return false;
}

TString UpdateSharedOidcCookieValue(TStringBuf currentCookieValue, const TString& key, TStringBuf flowId, TStringBuf requestedAddress) {
    TOidcCookieData data;
    TString errorMessage;
    if (!currentCookieValue.empty()) {
        TryDecodeOidcCookieValue(currentCookieValue, key, data, errorMessage);
    }

    data.ReturnTo[TString(flowId)] = TString(requestedAddress);
    TouchFlowId(data, flowId);

    TString cookieValue = BuildOidcCookieValue(data, key);
    while (cookieValue.size() > TOpenIdConnectSettings::MAX_AUTH_FLOW_COOKIE_VALUE_SIZE) {
        TString flowIdToEvict = FindFallbackFlowIdToEvict(data.Recent, flowId);
        if (flowIdToEvict.empty()) {
            break;
        }
        data.ReturnTo.erase(flowIdToEvict);
        RemoveFlowIdFromRecent(data.Recent, flowIdToEvict);
        cookieValue = BuildOidcCookieValue(data, key);
    }

    return cookieValue;
}

TFindRequestedAddressInOidcCookieResult FindRequestedAddressInSharedOidcCookieValue(TStringBuf cookieValue, const TString& key, TStringBuf flowId) {
    TOidcCookieData data;
    TString errorMessage;
    if (!TryDecodeOidcCookieValue(cookieValue, key, data, errorMessage)) {
        return {
            .IsSuccess = false,
            .RequestedAddress = "",
            .ErrorMessage = errorMessage,
        };
    }

    TMaybe<TString> requestedAddress = FindRequestedAddress(data, flowId);
    if (!requestedAddress) {
        return {
            .IsSuccess = false,
            .RequestedAddress = "",
            .ErrorMessage = "Requested address is not found in the OIDC context cookie",
        };
    }

    return {
        .IsSuccess = true,
        .RequestedAddress = *requestedAddress,
        .ErrorMessage = "",
    };
}

} // NMVP::NOIDC
