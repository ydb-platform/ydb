#pragma once

#include <library/cpp/json/writer/json_value.h>

#include <util/system/types.h>

#include <optional>
#include <string>
#include <vector>

namespace NKikimr::NSecurity {

enum class EJwkKeyType : ui8 {
    RSA,
    EC,
};

enum class EJwkUsage : ui8 {
    SIG /* "sig" */,
    ENC /* "enc" */,
};

enum class EJwkKeyOps : ui8 {
    SIGN /* "sign" */,
    VERIFY /* "verify" */,
    ENCRYPT /* "encrypt" */,
    DECRYPT /* "decrypt" */,
    WRAP_KEY /* "wrapKey" */,
    UNWRAP_KEY /* "unwrapKey" */,
    DERIVE_KEY /* "deriveKey" */,
    DERIVE_BITS /* "deriveBits" */,
};

// Use for asymmetric JWS: https://datatracker.ietf.org/doc/html/rfc7518#section-3
// TODO(vlad-serikov): Also implement for JWE: https://datatracker.ietf.org/doc/html/rfc7518#section-4
enum class EJwkAlg : ui8 {
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
struct TJwk {
    EJwkKeyType Type; // `kty`
    std::optional<EJwkUsage> Usage; // `use`
    std::vector<EJwkKeyOps> KeyOperations; // `key_ops`
    std::optional<EJwkAlg> Algorithm; // `alg`
    std::string KeyId; // `kid`
    std::string X509Url; // `x5u`
    std::vector<std::string> X509Chain; // decoded `x5c` (in DER format)
    std::string X509CertificateSha1ThumbprintBytes; // decoded `x5t`
    std::string X509CertificateSha256ThumbprintBytes; // decoded `x5t#S256`

    explicit TJwk(EJwkKeyType type);

    // Returns std::nullopt if parameters are missing or if validation/parsing failed.
    // Otherwise, returns the extracted public key.
    std::optional<std::string> CalculatePublicKey() const;
};

// https://datatracker.ietf.org/doc/html/rfc7517#section-5
struct TJwkSet {
    std::vector<TJwk> Keys; // `keys`
};

std::optional<EJwkKeyType> GetKeyType(EJwkAlg alg);
std::optional<TJwk> ParseJwk(const NJson::TJsonValue& jwk);
std::optional<TJwkSet> ParseJwkSet(const NJson::TJsonValue& jwkSet);

} // namespace NKikimr::NSecurity
