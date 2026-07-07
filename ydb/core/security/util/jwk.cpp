#include "jwk.h"

#include <ydb/core/security/certificate_check/cert_auth_utils.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/string/cast.h>

#include <openssl/sha.h>

#include <string>
#include <string_view>

namespace NKikimr::NSecurity {

namespace {

inline constexpr std::string_view KTY = "kty";
inline constexpr std::string_view USE = "use";
inline constexpr std::string_view KEY_OPS = "key_ops";
inline constexpr std::string_view ALG = "alg";
inline constexpr std::string_view KID = "kid";
inline constexpr std::string_view X5U = "x5u";
inline constexpr std::string_view X5C = "x5c";
inline constexpr std::string_view X5T = "x5t";
inline constexpr std::string_view X5T_S256 = "x5t#S256";
inline constexpr std::string_view KEYS = "keys";

std::optional<std::string> ParseStr(const NJson::TJsonValue& jwk, const std::string_view name) {
    if (!jwk.Has(name) || !jwk[name].IsString()) {
        return std::nullopt;
    }
    return jwk[name].GetString();
}

// The only MUST parameter
std::optional<TJwk> ParseKeyType(const NJson::TJsonValue& jwk) {
    const auto kty = ParseStr(jwk, KTY);
    if (!kty.has_value()) {
        return std::nullopt;
    }

    EJwkKeyType res = EJwkKeyType::RSA;
    return TryFromString(kty.value(), res) ? std::make_optional(TJwk{res}) : std::nullopt;
}

std::optional<EJwkUsage> ParseUsage(const NJson::TJsonValue& jwk) {
    const auto usage = ParseStr(jwk, USE);
    if (!usage.has_value()) {
        return std::nullopt;
    }

    EJwkUsage res = EJwkUsage::SIG;
    return TryFromString(usage.value(), res) ? std::make_optional(res) : std::nullopt;
}

std::vector<EJwkKeyOps> ParseKeyOps(const NJson::TJsonValue& jwk) {
    if (!jwk.Has(KEY_OPS) || !jwk[KEY_OPS].IsArray()) {
        return {};
    }

    std::vector<EJwkKeyOps> keyOps;

    for (const auto& op : jwk[KEY_OPS].GetArray()) {
        if (!op.IsString()) {
            continue;
        }

        EJwkKeyOps res = EJwkKeyOps::SIGN;
        if (!TryFromString(op.GetString(), res)) {
            continue;
        }

        keyOps.push_back(res);
    }

    return keyOps;
}

std::optional<EJwkAlg> ParseAlg(const NJson::TJsonValue& jwk) {
    const auto alg = ParseStr(jwk, ALG);
    if (!alg.has_value()) {
        return std::nullopt;
    }

    EJwkAlg res = EJwkAlg::RS256;
    return TryFromString(alg.value(), res) ? std::make_optional(res) : std::nullopt;
}

bool IsCompatibleAlgorithm(EJwkKeyType keyType, EJwkAlg algorithm) {
    switch (keyType) {
        case EJwkKeyType::RSA: {
            return algorithm == EJwkAlg::RS256
                || algorithm == EJwkAlg::RS384
                || algorithm == EJwkAlg::RS512
                || algorithm == EJwkAlg::PS256
                || algorithm == EJwkAlg::PS384
                || algorithm == EJwkAlg::PS512;
        }
        case EJwkKeyType::EC: {
            return algorithm == EJwkAlg::ES256
                || algorithm == EJwkAlg::ES384
                || algorithm == EJwkAlg::ES512;
        }
        default: return false;
    }
}

std::optional<EJwkAlg> ParseCompatibleAlg(const NJson::TJsonValue& jwk, EJwkKeyType keyType) {
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
std::optional<TJwk> ParseJwkRfc7517(const NJson::TJsonValue& jwk) {
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
    std::string hash(LENGTH, '\0');
    HASH(reinterpret_cast<const unsigned char*>(cert.data()), cert.size(),
        reinterpret_cast<unsigned char*>(hash.data()));
    return hash;
}

bool CheckCertificateThumbprints(const TJwk& jwk, const std::string& cert) {
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

std::optional<std::string> GetPublicKeyFromX5C(const TJwk& jwk) {
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

TJwk::TJwk(EJwkKeyType type)
    : Type(type)
{}

std::optional<EJwkKeyType> GetKeyType(EJwkAlg alg) {
    switch (alg) {
        case EJwkAlg::RS256:
        case EJwkAlg::RS384:
        case EJwkAlg::RS512:
        case EJwkAlg::PS256:
        case EJwkAlg::PS384:
        case EJwkAlg::PS512:
            return EJwkKeyType::RSA;
        case EJwkAlg::ES256:
        case EJwkAlg::ES384:
        case EJwkAlg::ES512:
            return EJwkKeyType::EC;
        default: return std::nullopt;
    }
}

// Currently this implementation is x5c-only: a public key can be derived
// only from the first certificate in the X.509 chain. RFC 7518 RSA/EC
// parameter-based key construction is not implemented here yet.
std::optional<std::string> TJwk::CalculatePublicKey() const {
    // TODO(vlad-serikov): write code to generate public key from RSA/EC parameters
    // TODO(vlad-serikov): validate cert key matches key from RSA/EC parameters
    return GetPublicKeyFromX5C(*this);
}

std::optional<TJwk> ParseJwk(const NJson::TJsonValue& jwk) {
    return ParseJwkRfc7517(jwk);
}

std::optional<TJwkSet> ParseJwkSet(const NJson::TJsonValue& jwkSet) {
    if (!jwkSet.Has(KEYS) || !jwkSet[KEYS].IsArray()) {
        return std::nullopt;
    }

    TJwkSet res;
    for (const auto& key : jwkSet[KEYS].GetArray()) {
        auto jwk = ParseJwk(key);
        if (!jwk.has_value()) {
            // Unsupported JWK doesn't mean, that we cannot use other JWK from the current set
            continue;
        }
        res.Keys.push_back(std::move(jwk.value()));
    }
    return res;
}

} // namespace NKikimr::NSecurity
