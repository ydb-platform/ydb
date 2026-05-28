#include "jwk.h"

#include <ydb/core/security/certificate_check/cert_auth_utils.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <openssl/sha.h>

#include <array>
#include <string_view>

namespace NKikimr::NSecurity {

namespace {

static constexpr std::string_view KTY = "kty";
static constexpr std::string_view KTY_RSA = "RSA";
static constexpr std::string_view KTY_EC = "EC";
static constexpr std::string_view USE = "use";
static constexpr std::string_view USE_SIG = "sig";
static constexpr std::string_view USE_ENC = "enc";
static constexpr std::string_view KEY_OPS = "key_ops";
static constexpr std::string_view KEY_OP_SIGN = "sign";
static constexpr std::string_view KEY_OP_VERIFY = "verify";
static constexpr std::string_view KEY_OP_ENCRYPT = "encrypt";
static constexpr std::string_view KEY_OP_DECRYPT = "decrypt";
static constexpr std::string_view KEY_OP_WRAP_KEY = "wrapKey";
static constexpr std::string_view KEY_OP_UNWRAP_KEY = "unwrapKey";
static constexpr std::string_view KEY_OP_DERIVE_KEY = "deriveKey";
static constexpr std::string_view KEY_OP_DERIVE_BITS = "deriveBits";
static constexpr std::string_view ALG = "alg";
static constexpr std::string_view ALG_RS256 = "RS256";
static constexpr std::string_view ALG_RS384 = "RS384";
static constexpr std::string_view ALG_RS512 = "RS512";
static constexpr std::string_view ALG_ES256 = "ES256";
static constexpr std::string_view ALG_ES384 = "ES384";
static constexpr std::string_view ALG_ES512 = "ES512";
static constexpr std::string_view ALG_PS256 = "PS256";
static constexpr std::string_view ALG_PS384 = "PS384";
static constexpr std::string_view ALG_PS512 = "PS512";
static constexpr std::string_view KID = "kid";
static constexpr std::string_view X5U = "x5u";
static constexpr std::string_view X5C = "x5c";
static constexpr std::string_view X5T = "x5t";
static constexpr std::string_view X5T_S256 = "x5t#S256";
static constexpr std::string_view KEYS = "keys";

std::optional<std::string> ParseStr(const NJson::TJsonValue& jwk, const std::string_view name) {
    if (!jwk.Has(name) || !jwk[name].IsString()) {
        return std::nullopt;
    }
    return jwk[name].GetString();
}

// The only MUST parameter
std::optional<TJWK> ParseKeyType(const NJson::TJsonValue& jwk) {
    const auto kty = ParseStr(jwk, KTY);
    if (!kty.has_value()) {
        return std::nullopt;
    }

    if (kty.value() == KTY_RSA) {
        return TJWK(TKeyType::RSA);
    } else if (kty.value() == KTY_EC) {
        return TJWK(TKeyType::EC);
    } else {
        return std::nullopt;
    }
}

std::optional<TUsage> ParseUsage(const NJson::TJsonValue& jwk) {
    const auto usage = ParseStr(jwk, USE);
    if (!usage.has_value()) {
        return std::nullopt;
    }

    if (usage.value() == USE_SIG) {
        return TUsage::SIG;
    } else if (usage.value() == USE_ENC) {
        return TUsage::ENC;
    } else {
        return std::nullopt;
    }
}

std::vector<TKeyOps> ParseKeyOps(const NJson::TJsonValue& jwk) {
    if (!jwk.Has(KEY_OPS) || !jwk[KEY_OPS].IsArray()) {
        return {};
    }

    std::vector<TKeyOps> keyOps;

    for (const auto& op : jwk[KEY_OPS].GetArray()) {
        if (!op.IsString()) {
            continue;
        }

        auto keyOp = op.GetString();
        if (keyOp == KEY_OP_SIGN) {
            keyOps.push_back(TKeyOps::SIGN);
        } else if (keyOp == KEY_OP_VERIFY) {
            keyOps.push_back(TKeyOps::VERIFY);
        } else if (keyOp == KEY_OP_ENCRYPT) {
            keyOps.push_back(TKeyOps::ENCRYPT);
        } else if (keyOp == KEY_OP_DECRYPT) {
            keyOps.push_back(TKeyOps::DECRYPT);
        } else if (keyOp == KEY_OP_WRAP_KEY) {
            keyOps.push_back(TKeyOps::WRAP_KEY);
        } else if (keyOp == KEY_OP_UNWRAP_KEY) {
            keyOps.push_back(TKeyOps::UNWRAP_KEY);
        } else if (keyOp == KEY_OP_DERIVE_KEY) {
            keyOps.push_back(TKeyOps::DERIVE_KEY);
        } else if (keyOp == KEY_OP_DERIVE_BITS) {
            keyOps.push_back(TKeyOps::DERIVE_BITS);
        }
    }

    return keyOps;
}

std::optional<TAlg> ParseAlg(const NJson::TJsonValue& jwk) {
    const auto alg = ParseStr(jwk, ALG);
    if (!alg.has_value()) {
        return std::nullopt;
    }

    if (alg.value() == ALG_RS256) {
        return TAlg::RS256;
    } else if (alg.value() == ALG_RS384) {
        return TAlg::RS384;
    } else if (alg.value() == ALG_RS512) {
        return TAlg::RS512;
    } else if (alg.value() == ALG_ES256) {
        return TAlg::ES256;
    } else if (alg.value() == ALG_ES384) {
        return TAlg::ES384;
    } else if (alg.value() == ALG_ES512) {
        return TAlg::ES512;
    } else if (alg.value() == ALG_PS256) {
        return TAlg::PS256;
    } else if (alg.value() == ALG_PS384) {
        return TAlg::PS384;
    } else if (alg.value() == ALG_PS512) {
        return TAlg::PS512;
    } else {
        return std::nullopt;
    }
}

bool IsCompatibleAlgorithm(TKeyType keyType, TAlg algorithm) {
    switch (keyType) {
        case TKeyType::RSA: {
            return algorithm == TAlg::RS256
                || algorithm == TAlg::RS384
                || algorithm == TAlg::RS512
                || algorithm == TAlg::PS256
                || algorithm == TAlg::PS384
                || algorithm == TAlg::PS512;
        }
        case TKeyType::EC: {
            return algorithm == TAlg::ES256
                || algorithm == TAlg::ES384
                || algorithm == TAlg::ES512;
        }
        default: Y_UNREACHABLE();
    }
}

std::optional<TAlg> ParseCompatibleAlg(const NJson::TJsonValue& jwk, TKeyType keyType) {
    const auto algorithm = ParseAlg(jwk);
    if (!algorithm.has_value() || !IsCompatibleAlgorithm(keyType, algorithm.value())) {
        return std::nullopt;
    }
    return algorithm;
}

std::string ParseKid(const NJson::TJsonValue& jwk) {
    const auto kid = ParseStr(jwk, KID);
    return kid.has_value() ? kid.value() : std::string{};
}

std::string ParseX5U(const NJson::TJsonValue& jwk) {
    const auto x5u = ParseStr(jwk, X5U);
    return x5u.has_value() ? x5u.value() : std::string{};
}

// Return std::nullopt if x5c is present but malformed.
std::optional<std::vector<std::string>> ParseX5C(const NJson::TJsonValue& jwk) {
    if (!jwk.Has(X5C)) {
        return std::vector<std::string>{};
    }

    if (!jwk[X5C].IsArray()) {
        return std::nullopt;
    }

    std::vector<std::string> x5c;
    for (const auto& cert : jwk[X5C].GetArray()) {
        if (!cert.IsString()) {
            return std::nullopt;
        }
        try {
            x5c.push_back(Base64StrictDecode(cert.GetString()));
        } catch (const std::exception&) {
            return std::nullopt;
        }
    }

    return x5c;
}

// this is copy-paste from Base64DecodeUneven except it uses Base64StrictDecode
TString Base64StrictDecodeUneven(const TStringBuf s) {
    const size_t tail = s.length() % 4;
    if (tail == 0) {
        return Base64StrictDecode(s);
    }
    return Base64StrictDecode(TString(s) + TString(4 - tail, '='));
}

// Return std::nullopt if the thumbprint is present but malformed.
std::optional<std::string> ParseThumbprint(
    const NJson::TJsonValue& jwk, const std::string_view name, size_t expectedLength)
{
    if (!jwk.Has(name)) {
        return std::string{};
    }

    const auto thumbprint = ParseStr(jwk, name);
    if (!thumbprint.has_value()) {
        return std::nullopt;
    }

    try {
        auto decoded = Base64StrictDecodeUneven(thumbprint.value());
        if (decoded.size() != expectedLength) {
            return std::nullopt;
        }
        return decoded;
    } catch (const std::exception&) {
        return std::nullopt;
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

    if (jwk.Has(ALG)) {
        auto algorithm = ParseCompatibleAlg(jwk, res->Type);
        if (!algorithm.has_value()) {
            return std::nullopt;
        }
        res->Algorithm = algorithm;
    }

    res->KeyId = ParseKid(jwk);
    res->X509Url = ParseX5U(jwk);

    if (auto x5c = ParseX5C(jwk); !x5c.has_value()) {
        return std::nullopt;
    } else {
        res->X509Chain = std::move(x5c.value());
    }

    if (auto x5t = ParseThumbprint(jwk, X5T, SHA_DIGEST_LENGTH); !x5t.has_value()) {
        return std::nullopt;
    } else {
        res->X509CertificateSha1ThumbprintBytes = std::move(x5t.value());
    }

    if (auto x5ts256 = ParseThumbprint(jwk, X5T_S256, SHA256_DIGEST_LENGTH); !x5ts256.has_value()) {
        return std::nullopt;
    } else {
        res->X509CertificateSha256ThumbprintBytes = std::move(x5ts256.value());
    }

    return res;
}

template <auto HASH, size_t LENGTH>
std::string CalculateThumbprint(const std::string& cert) {
    std::array<unsigned char, LENGTH> hash;
    HASH(reinterpret_cast<const unsigned char*>(cert.data()), cert.size(), hash.data());
    return std::string(reinterpret_cast<const char*>(hash.data()), LENGTH);
}

bool CheckCertificateThumbprints(const TJWK& jwk, const std::string& cert) {
    if (!jwk.X509CertificateSha1ThumbprintBytes.empty()
        && jwk.X509CertificateSha1ThumbprintBytes != CalculateThumbprint<SHA1, SHA_DIGEST_LENGTH>(cert))
    {
        return false;
    }

    if (!jwk.X509CertificateSha256ThumbprintBytes.empty()
        && jwk.X509CertificateSha256ThumbprintBytes != CalculateThumbprint<SHA256, SHA256_DIGEST_LENGTH>(cert))
    {
        return false;
    }

    return true;
}

std::optional<std::string> GetPublicKeyFromX5C(const TJWK& jwk) {
    if (jwk.X509Chain.empty()) {
        return std::nullopt;
    }

    const auto& keyCert = jwk.X509Chain.front();
    if (!CheckCertificateThumbprints(jwk, keyCert)) {
        return std::nullopt;
    }

    // TODO(vlad-serikov): validate certificate chain if possible
    // TODO(vlad-serikov): validate certificate key algorithm base on kty

    const auto publicKey = GetCertificatePublicKey(keyCert, ECertificateFormat::DER);
    if (publicKey.empty()) {
        return std::nullopt;
    }

    return publicKey;
}

} // namespace

TJWK::TJWK(TKeyType type)
    : Type(type)
{}

// Currently this implementation is x5c-only: a public key can be derived
// only from the first certificate in the X.509 chain. RFC 7518 RSA/EC
// parameter-based key construction is not implemented here yet.
std::optional<std::string> TJWK::CalculatePublicKey() const {
    // TODO(vlad-serikov): write code to generate public key from RSA/EC parameters
    // TODO(vlad-serikov): validate cert key matches key from RSA/EC parameters
    return GetPublicKeyFromX5C(*this);
}

std::optional<TJWK> ParseJWK(const NJson::TJsonValue& jwk) {
    return ParseJwkRfc7517(jwk);
}

std::optional<TJWKSet> ParseJWKSet(const NJson::TJsonValue& jwkSet) {
    if (!jwkSet.Has(KEYS) || !jwkSet[KEYS].IsArray()) {
        return std::nullopt;
    }

    TJWKSet res;
    for (const auto& key : jwkSet[KEYS].GetArray()) {
        auto jwk = ParseJWK(key);
        if (!jwk.has_value()) {
            // Unsupported JWK doesn't mean, that we cannot use other JWK from the current set
            continue;
        }
        res.Keys.push_back(std::move(jwk.value()));
    }
    return res;
}

} // namespace NKikimr::NSecurity
