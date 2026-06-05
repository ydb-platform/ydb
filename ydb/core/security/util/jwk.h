#pragma once

#include <library/cpp/json/writer/json_value.h>

#include <util/system/types.h>

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

// Use for asymmetric JWS: https://datatracker.ietf.org/doc/html/rfc7518#section-3
// TODO(vlad-serikov): Also implement for JWE: https://datatracker.ietf.org/doc/html/rfc7518#section-4
enum class TAlg : ui8 {
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
    TKeyType Type; // `kty`
    std::optional<TUsage> Usage; // `use`
    std::vector<TKeyOps> KeyOperations; // `key_ops`
    std::optional<TAlg> Algorithm; // `alg`
    std::string KeyId; // `kid`
    std::string X509Url; // `x5u`
    std::vector<std::string> X509Chain; // decoded `x5c` (in DER format)
    std::string X509CertificateSha1ThumbprintBytes; // decoded `x5t`
    std::string X509CertificateSha256ThumbprintBytes; // decoded `x5t#S256`

    explicit TJWK(TKeyType type);

    // Returns std::nullopt if parameters are missing or if validation/parsing failed.
    // Otherwise, returns the extracted public key.
    std::optional<std::string> CalculatePublicKey() const;
};

// https://datatracker.ietf.org/doc/html/rfc7517#section-5
struct TJWKSet {
    std::vector<TJWK> Keys; // `keys`
};

std::optional<TJWK> ParseJWK(const NJson::TJsonValue& jwk);
std::optional<TJWKSet> ParseJWKSet(const NJson::TJsonValue& jwkSet);

} // namespace NKikimr::NSecurity
