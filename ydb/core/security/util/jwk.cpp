#include "jwk.h"

#include <util/string/hex.h>
#include <ydb/core/security/certificate_check/cert_auth_utils.h>

#include <library/cpp/string_utils/base64/base64.h>

namespace NKikimr::NSecurity {

namespace {

std::optional<std::string> ParseStr(const NJson::TJsonValue& jwk, const std::string& name) {
    if (!jwk.Has(name) || !jwk[name].IsString()) {
        return std::nullopt;
    }
    return jwk[name].GetString();
}

// The only MUST parameter
std::optional<TJWK> ParseKeyType(const NJson::TJsonValue& jwk) {
    const auto kty = ParseStr(jwk, "kty");
    if (!kty.has_value()) {
        return std::nullopt;
    }

    if (kty.value() == "RSA") {
        return TJWK(TKeyType::RSA);
    } else if (kty.value() == "EC") {
        return TJWK(TKeyType::EC);
    } else {
        return std::nullopt;
    }
}

std::optional<TUsage> ParseUsage(const NJson::TJsonValue& jwk) {
    const auto usage = ParseStr(jwk, "use");
    if (!usage.has_value()) {
        return std::nullopt;
    }

    if (usage.value() == "sig") {
        return TUsage::SIG;
    } else if (usage.value() == "enc") {
        return TUsage::ENC;
    } else {
        return std::nullopt;
    }
}

std::vector<TKeyOps> ParseKeyOps(const NJson::TJsonValue& jwk) {
    static constexpr TStringBuf KEY_OPS = "key_ops";

    if (!jwk.Has(KEY_OPS) || !jwk[KEY_OPS].IsArray()) {
        return {};
    }

    std::vector<TKeyOps> keyOps;

    for (const auto& op : jwk[KEY_OPS].GetArray()) {
        if (!op.IsString()) {
            continue;
        }

        auto keyOp = op.GetString();
        if (keyOp == "sign") {
            keyOps.push_back(TKeyOps::SIGN);
        } else if (keyOp == "verify") {
            keyOps.push_back(TKeyOps::VERIFY);
        } else if (keyOp == "encrypt") {
            keyOps.push_back(TKeyOps::ENCRYPT);
        } else if (keyOp == "decrypt") {
            keyOps.push_back(TKeyOps::DECRYPT);
        } else if (keyOp == "wrapKey") {
            keyOps.push_back(TKeyOps::WRAP_KEY);
        } else if (keyOp == "unwrapKey") {
            keyOps.push_back(TKeyOps::UNWRAP_KEY);
        } else if (keyOp == "deriveKey") {
            keyOps.push_back(TKeyOps::DERIVE_KEY);
        } else if (keyOp == "deriveBits") {
            keyOps.push_back(TKeyOps::DERIVE_BITS);
        }
    }

    return keyOps;
}

std::optional<TAlg> ParseAlg(const NJson::TJsonValue& jwk) {
    const auto alg = ParseStr(jwk, "alg");
    if (!alg.has_value()) {
        return std::nullopt;
    }

    if (alg.value() == "none") {
        return TAlg::NONE;
    } else if (alg.value() == "HS256") {
        return TAlg::HS256;
    } else if (alg.value() == "HS384") {
        return TAlg::HS384;
    } else if (alg.value() == "HS512") {
        return TAlg::HS512;
    } else if (alg.value() == "RS256") {
        return TAlg::RS256;
    } else if (alg.value() == "RS384") {
        return TAlg::RS384;
    } else if (alg.value() == "RS512") {
        return TAlg::RS512;
    } else if (alg.value() == "ES256") {
        return TAlg::ES256;
    } else if (alg.value() == "ES384") {
        return TAlg::ES384;
    } else if (alg.value() == "ES512") {
        return TAlg::ES512;
    } else if (alg.value() == "PS256") {
        return TAlg::PS256;
    } else if (alg.value() == "PS384") {
        return TAlg::PS384;
    } else if (alg.value() == "PS512") {
        return TAlg::PS512;
    } else {
        return std::nullopt;
    }
}

std::string ParseKid(const NJson::TJsonValue& jwk) {
    const auto kid = ParseStr(jwk, "kid");
    return kid.has_value() ? kid.value() : "";
}

std::string ParseX5U(const NJson::TJsonValue& jwk) {
    const auto x5u = ParseStr(jwk, "x5u");
    return x5u.has_value() ? x5u.value() : "";
}

std::vector<std::string> ParseX5C(const NJson::TJsonValue& jwk) {
    static constexpr TStringBuf X5C = "x5c";

    if (!jwk.Has(X5C) || !jwk[X5C].IsArray()) {
        return {};
    }

    std::vector<std::string> x5c;
    for (const auto& cert : jwk[X5C].GetArray()) {
        if (!cert.IsString()) {
            continue;
        }
        try {
            x5c.push_back(Base64Decode(cert.GetString()));
        } catch (const std::exception&) {
            continue;
        }
    }

    return x5c;
}

std::string ParseX5T(const NJson::TJsonValue& jwk) {
    const auto x5t = ParseStr(jwk, "x5t");
    if (!x5t.has_value()) {
        return "";
    }
    try {
        return Base64DecodeUneven(x5t.value());
    } catch (const std::exception&) {
        return "";
    }
}

std::string ParseX5TS256(const NJson::TJsonValue& jwk) {
    const auto x5ts256 = ParseStr(jwk, "x5t#S256");
    if (!x5ts256.has_value()) {
        return  "";
    }
    try {
        return Base64DecodeUneven(x5ts256.value());
    } catch (const std::exception&) {
        return "";
    }
}

// Parsing based on https://datatracker.ietf.org/doc/html/rfc7517
std::optional<TJWK> ParseJwkRfc7517(const NJson::TJsonValue& jwk) {
    auto res = ParseKeyType(jwk);
    if (!res.has_value()) {
        return std::nullopt;
    }

    res->Usage = ParseUsage(jwk);
    res->KeyOperations = ParseKeyOps(jwk);
    res->Algorithm = ParseAlg(jwk);
    res->KeyId = ParseKid(jwk);
    res->X509Url = ParseX5U(jwk);
    res->X509Chain = ParseX5C(jwk);
    res->X509CertificateSha1Thumbprint = ParseX5T(jwk);
    res->X509CertificateSha256Thumbprint = ParseX5TS256(jwk);

    return res;
}

TString GetPublicKeyFromX5C(const std::string& cert) {
    // TODO(vlad-serikov): check thumbprint
    return GetCertificatePublicKey(cert, false);
}

} // namespace

TJWK::TJWK(TKeyType type)
    : Type(type)
{}

std::string TJWK::CalculatePublicKey() const {
    if (!X509Chain.empty()) {
        return GetPublicKeyFromX5C(X509Chain.front());
    }

    // TODO(vlad-serikov): write code to generate public key from RSA/EC parameters
    return "";
}

std::optional<TJWK> ParseJWK(const NJson::TJsonValue& jwk) {
    return ParseJwkRfc7517(jwk);
}

std::optional<TJWKSet> ParseJWKSet(const NJson::TJsonValue& jwk) {
    static constexpr TStringBuf KEYS = "keys";

    if (!jwk.Has(KEYS) || !jwk[KEYS].IsArray()) {
        return std::nullopt;
    }

    TJWKSet jwkSet;
    for (const auto& key : jwk[KEYS].GetArray()) {
        auto jwk = ParseJWK(key);
        if (jwk.has_value()) {
            jwkSet.Keys.push_back(std::move(jwk.value()));
        }
    }
    return jwkSet;
}

} // namespace NKikimr::NSecurity
