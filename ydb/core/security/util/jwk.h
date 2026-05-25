#pragma once

#include <library/cpp/json/writer/json_value.h>

#include <optional>
#include <string>
#include <vector>

namespace NKikimr::NSecurity {

enum class TKeyType : ui8 {
    RSA,
    EC,
};

enum class TUsage : ui8 {
    SIG,
    ENC,
};

enum class TKeyOps : ui8 {
    SIGN,
    VERIFY,
    ENCRYPT,
    DECRYPT,
    WRAP_KEY,
    UNWRAP_KEY,
    DERIVE_KEY,
    DERIVE_BITS,
};

enum class TAlg : ui8 {
    NONE,
    HS256,
    HS384,
    HS512,
    RS256,
    RS384,
    RS512,
    ES256,
    ES384,
    ES512,
    PS256,
    PS384,
    PS512,
};

// {kty, kid} - Unique identifier
// https://datatracker.ietf.org/doc/html/rfc7517#section-4
struct TJWK {
    TKeyType Type;
    std::optional<TUsage> Usage;
    std::vector<TKeyOps> KeyOperations;
    std::optional<TAlg> Algorithm;
    std::string KeyId;
    std::string X509Url;
    std::vector<std::string> X509Chain; // In DER format
    std::string X509CertificateSha1Thumbprint;
    std::string X509CertificateSha256Thumbprint;

    explicit TJWK(TKeyType type);

    std::string CalculatePublicKey() const;
};

// https://datatracker.ietf.org/doc/html/rfc7517#section-5
struct TJWKSet {
    std::vector<TJWK> Keys;
};

std::optional<TJWK> ParseJWK(const NJson::TJsonValue& jwk);
std::optional<TJWKSet> ParseJWKSet(const NJson::TJsonValue& jwk);

} // namespace NKikimr::NSecurity
