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

// Asymmetric JWS and JWE algorithms (RFC 7518, Sections 3 and 4).
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
    RSA1_5,
    RSA_OAEP /* "RSA-OAEP" */,
    RSA_OAEP_256 /* "RSA-OAEP-256" */,
    ECDH_ES /* "ECDH-ES" */,
    ECDH_ES_A128KW /* "ECDH-ES+A128KW" */,
    ECDH_ES_A192KW /* "ECDH-ES+A192KW" */,
    ECDH_ES_A256KW /* "ECDH-ES+A256KW" */,
};

// {kty, kid} - Unique identifier
// https://datatracker.ietf.org/doc/html/rfc7517#section-4
struct TJwk {
    struct TRsaParameters {
        std::string Modulus; // decoded `n` (unsigned, big endian)
        std::string Exponent; // decoded `e` (unsigned, big endian)
    };

    struct TEcParameters {
        std::string Curve; // `crv`
        std::string X; // decoded `x` (unsigned, big endian)
        std::string Y; // decoded `y` (unsigned, big endian)
    };

    EJwkKeyType Type; // `kty`
    std::optional<EJwkUsage> Usage; // `use`
    std::vector<EJwkKeyOps> KeyOperations; // `key_ops`
    std::optional<EJwkAlg> Algorithm; // `alg`
    std::string KeyId; // `kid`
    std::string X509Url; // `x5u`
    std::vector<std::string> X509Chain; // decoded `x5c` (in DER format)
    std::string X509CertificateSha1ThumbprintBytes; // decoded `x5t`
    std::string X509CertificateSha256ThumbprintBytes; // decoded `x5t#S256`
    std::optional<TRsaParameters> RsaParameters;
    std::optional<TEcParameters> EcParameters;

    explicit TJwk(EJwkKeyType type);

    // Returns std::nullopt if parameters are missing or if validation/parsing failed.
    // Otherwise, returns the public key in PEM format. If both x5c and key
    // parameters are present, they must represent the same key.
    // Validates the supplied certificate path, including validity periods and
    // signatures for which an issuer is available. The last certificate need
    // not be a root: trust in the JWK must come from its authenticated source,
    // not from this consistency check. Does not fetch x5u or check revocation.
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
