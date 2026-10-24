#include "jwk.h"

#include <library/cpp/string_utils/base64/base64.h>

#include <util/string/cast.h>

#include <openssl/ec.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/sha.h>
#include <openssl/x509v3.h>

#include <limits>
#include <memory>
#include <set>
#include <string>
#include <string_view>

namespace NKikimr::NSecurity {

namespace {

constexpr std::string_view KTY = "kty";
constexpr std::string_view USE = "use";
constexpr std::string_view KEY_OPS = "key_ops";
constexpr std::string_view ALG = "alg";
constexpr std::string_view KID = "kid";
constexpr std::string_view X5U = "x5u";
constexpr std::string_view X5C = "x5c";
constexpr std::string_view X5T = "x5t";
constexpr std::string_view X5T_S256 = "x5t#S256";
constexpr std::string_view KEYS = "keys";

constexpr size_t MAX_CERTIFICATE_CHAIN_LENGTH = 100;

template <auto Free>
struct TOpenSslDeleter {
    template <typename T>
    void operator()(T* ptr) const {
        Free(ptr);
    }
};

template <typename T, auto Free>
using TOpenSslPtr = std::unique_ptr<T, TOpenSslDeleter<Free>>;

using TKeyPtr = TOpenSslPtr<EVP_PKEY, EVP_PKEY_free>;
using TCertPtr = TOpenSslPtr<X509, X509_free>;

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

std::optional<std::vector<EJwkKeyOps>> ParseKeyOps(const NJson::TJsonValue& jwk) {
    if (!jwk[KEY_OPS].IsArray()) {
        return std::nullopt;
    }

    std::vector<EJwkKeyOps> keyOps;
    std::set<EJwkKeyOps> seen;

    for (const auto& op : jwk[KEY_OPS].GetArray()) {
        if (!op.IsString()) {
            return std::nullopt;
        }

        EJwkKeyOps res = EJwkKeyOps::SIGN;
        if (!TryFromString(op.GetString(), res) || !seen.insert(res).second) {
            return std::nullopt;
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
    const auto algorithmKeyType = GetKeyType(algorithm);
    return algorithmKeyType.has_value() && algorithmKeyType.value() == keyType;
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

    if (!jwk[X5C].IsArray() || jwk[X5C].GetArray().empty() || jwk[X5C].GetArray().size() > MAX_CERTIFICATE_CHAIN_LENGTH) {
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

std::optional<std::string> ParseKeyBytes(const NJson::TJsonValue& jwk, std::string_view name) {
    const auto encoded = ParseStr(jwk, name);
    if (!encoded.has_value() || encoded->empty()) {
        return std::nullopt;
    }
    try {
        const auto decoded = Base64StrictDecodeUneven(encoded.value());
        // Require canonical, unpadded base64url; the generic decoder accepts
        // both alphabets, padding within the input and nonzero unused bits.
        if (Base64EncodeUrlNoPadding(decoded) != encoded.value()) {
            return std::nullopt;
        }
        return decoded;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

bool ParseKeyParameters(const NJson::TJsonValue& jwk, TJwk& result) {
    if (result.Type == EJwkKeyType::RSA && (jwk.Has("n") || jwk.Has("e"))) {
        auto modulus = ParseKeyBytes(jwk, "n");
        if (!modulus.has_value() || modulus->front() == '\0') {
            return false;
        }
        auto exponent = ParseKeyBytes(jwk, "e");
        if (!exponent.has_value() || exponent->front() == '\0') {
            return false;
        }
        result.RsaParameters = TJwk::TRsaParameters{
            .Modulus = std::move(modulus.value()),
            .Exponent = std::move(exponent.value()),
        };
    }
    if (result.Type == EJwkKeyType::EC && (jwk.Has("crv") || jwk.Has("x") || jwk.Has("y"))) {
        auto curve = ParseStr(jwk, "crv");
        if (!curve.has_value()) {
            return false;
        }
        auto x = ParseKeyBytes(jwk, "x");
        if (!x.has_value()) {
            return false;
        }
        auto y = ParseKeyBytes(jwk, "y");
        if (!y.has_value()) {
            return false;
        }
        result.EcParameters = TJwk::TEcParameters{
            .Curve = std::move(curve.value()),
            .X = std::move(x.value()),
            .Y = std::move(y.value()),
        };
    }
    return true;
}

// Return std::nullopt if the thumbprint is present but malformed.
std::optional<std::string> ParseThumbprint(
    const NJson::TJsonValue& jwk, const std::string_view name, size_t expectedLength)
{
    if (!jwk.Has(name)) {
        return std::string{};
    }

    auto thumbprint = ParseKeyBytes(jwk, name);
    if (!thumbprint.has_value() || thumbprint->size() != expectedLength) {
        return std::nullopt;
    }
    return thumbprint;
}

// Parsing based on https://datatracker.ietf.org/doc/html/rfc7517
std::optional<TJwk> ParseJwkRfc7517(const NJson::TJsonValue& jwk) {
    auto res = ParseKeyType(jwk);
    if (!res.has_value()) {
        return std::nullopt;
    }

    res->Usage = ParseUsage(jwk);
    if (jwk.Has(KEY_OPS)) {
        auto keyOps = ParseKeyOps(jwk);
        if (!keyOps.has_value()) {
            return std::nullopt;
        }
        res->KeyOperations = std::move(keyOps.value());
    }

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

    if (!ParseKeyParameters(jwk, res.value())) {
        return std::nullopt;
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

bool CheckCertificateKeyUsage(const TJwk& jwk, X509* cert) {
    const auto usage = X509_get_key_usage(cert);
    unsigned int required = 0;

    const auto encryptionUsage = (jwk.Type == EJwkKeyType::RSA) ? KU_KEY_ENCIPHERMENT : KU_KEY_AGREEMENT;
    if (jwk.Usage.has_value()) {
        required |= (jwk.Usage.value() == EJwkUsage::SIG) ? KU_DIGITAL_SIGNATURE : encryptionUsage;
    }
    if (jwk.Algorithm.has_value()) {
        switch (jwk.Algorithm.value()) {
            case EJwkAlg::RSA1_5:
            case EJwkAlg::RSA_OAEP:
            case EJwkAlg::RSA_OAEP_256:
            case EJwkAlg::ECDH_ES:
            case EJwkAlg::ECDH_ES_A128KW:
            case EJwkAlg::ECDH_ES_A192KW:
            case EJwkAlg::ECDH_ES_A256KW:
                required |= encryptionUsage;
                break;
            default:
                required |= KU_DIGITAL_SIGNATURE;
                break;
        }
    }

    if (!jwk.KeyOperations.has_value()) {
        return (usage & required) == required;
    }
    for (const auto operation : jwk.KeyOperations.value()) {
        switch (operation) {
            case EJwkKeyOps::SIGN:
            case EJwkKeyOps::VERIFY:
                required |= KU_DIGITAL_SIGNATURE;
                break;
            case EJwkKeyOps::ENCRYPT:
            case EJwkKeyOps::DECRYPT:
            case EJwkKeyOps::WRAP_KEY:
            case EJwkKeyOps::UNWRAP_KEY:
                required |= encryptionUsage;
                break;
            case EJwkKeyOps::DERIVE_KEY:
            case EJwkKeyOps::DERIVE_BITS:
                required |= KU_KEY_AGREEMENT;
                break;
        }
    }
    // OpenSSL returns all bits set when the KeyUsage extension is absent.
    return (usage & required) == required;
}

std::string CertificateError(size_t index, const std::string& reason) {
    return "x5c certificate index " + std::to_string(index) + ": " + reason;
}

TKeyPtr GetPublicKeyFromX5C(const TJwk& jwk, std::string& error) {
    if (jwk.X509Chain.empty() || jwk.X509Chain.size() > MAX_CERTIFICATE_CHAIN_LENGTH) {
        error = "x5c must contain between 1 and " + std::to_string(MAX_CERTIFICATE_CHAIN_LENGTH) + " certificates";
        return {};
    }

    const auto& keyCert = jwk.X509Chain.front();
    if (!CheckCertificateThumbprints(jwk, keyCert)) {
        error = CertificateError(0, "thumbprint mismatch");
        return {};
    }

    std::vector<TCertPtr> certificates;
    for (const auto& der : jwk.X509Chain) {
        if (der.empty() || der.size() > std::numeric_limits<long>::max()) {
            error = CertificateError(certificates.size(), "invalid DER length");
            return {};
        }
        const auto* begin = reinterpret_cast<const unsigned char*>(der.data());
        const auto* cursor = begin;
        TCertPtr cert(d2i_X509(nullptr, &cursor, der.size()));
        if (cert == nullptr || cursor != begin + der.size()) {
            error = CertificateError(certificates.size(), "invalid DER encoding or trailing data");
            return {};
        }
        if (!certificates.empty()) {
            const auto code = X509_check_issued(cert.get(), certificates.back().get());
            if (code != X509_V_OK) {
                error = CertificateError(certificates.size(), "invalid issuer: "
                    + std::string(X509_verify_cert_error_string(code)) + " (code " + std::to_string(code) + ")");
                return {};
            }
        }
        certificates.push_back(std::move(cert));
    }

    // JWKS is authenticated by its caller. Pin the last supplied certificate
    // for path validation, allowing providers to omit the root/issuer.
    TOpenSslPtr<X509_STORE, X509_STORE_free> store(X509_STORE_new());
    TOpenSslPtr<STACK_OF(X509), sk_X509_free> chain(sk_X509_new_null());
    TOpenSslPtr<X509_STORE_CTX, X509_STORE_CTX_free> ctx(X509_STORE_CTX_new());
    if (store == nullptr || chain == nullptr || ctx == nullptr
            || X509_STORE_add_cert(store.get(), certificates.back().get()) != 1)
    {
        error = "Failed to initialize x5c certificate store";
        return {};
    }
    for (size_t i = 1; i < certificates.size(); ++i) {
        if (sk_X509_push(chain.get(), certificates[i].get()) == 0) {
            error = "Failed to build x5c verification chain";
            return {};
        }
    }
    if (X509_STORE_CTX_init(ctx.get(), store.get(), certificates.front().get(), chain.get()) != 1
            || X509_VERIFY_PARAM_set_flags(X509_STORE_CTX_get0_param(ctx.get()), X509_V_FLAG_PARTIAL_CHAIN) != 1)
    {
        error = "Failed to initialize x5c verification context";
        return {};
    }
    if (X509_verify_cert(ctx.get()) != 1) {
        const auto code = X509_STORE_CTX_get_error(ctx.get());
        error = CertificateError(X509_STORE_CTX_get_error_depth(ctx.get()),
            std::string(X509_verify_cert_error_string(code)) + " (code " + std::to_string(code) + ")");
        return {};
    }
    // Do not silently ignore extra or duplicate certificates when OpenSSL
    // builds a shorter path to the pinned certificate.
    const auto* verified = X509_STORE_CTX_get0_chain(ctx.get());
    if (static_cast<size_t>(sk_X509_num(verified)) != certificates.size()) {
        error = "x5c contains extra or duplicate certificates";
        return {};
    }
    auto* last = certificates.back().get();
    // A self-issued rollover CA can have the same DN as its issuer but a
    // different key. OpenSSL also checks authority/subject key identifiers.
    if ((X509_get_extension_flags(last) & EXFLAG_SS) && X509_verify(last, X509_get0_pubkey(last)) != 1) {
        error = CertificateError(certificates.size() - 1, "invalid self-signature");
        return {};
    }

    if (!CheckCertificateKeyUsage(jwk, certificates.front().get())) {
        error = CertificateError(0, "key usage is incompatible with the JWK");
        return {};
    }
    TKeyPtr publicKey(X509_get_pubkey(certificates.front().get()));
    if (publicKey == nullptr) {
        error = CertificateError(0, "missing or unsupported public key");
    }
    return publicKey;
}

int GetCurveId(std::string_view curve) {
    if (curve == "P-256") {
        return NID_X9_62_prime256v1;
    }
    if (curve == "P-384") {
        return NID_secp384r1;
    }
    if (curve == "P-521") {
        return NID_secp521r1;
    }
    return NID_undef;
}

TOpenSslPtr<BIGNUM, BN_free> DecodeNumber(const std::string& bytes) {
    if (bytes.empty() || bytes.size() > std::numeric_limits<int>::max()) {
        return {};
    }
    return TOpenSslPtr<BIGNUM, BN_free>(BN_bin2bn(
        reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), nullptr));
}

TKeyPtr GetPublicKeyFromParameters(const TJwk& jwk, std::string& error) {
    TKeyPtr key(EVP_PKEY_new());
    if (key == nullptr) {
        error = "Failed to allocate public key";
        return {};
    }
    if (jwk.Type == EJwkKeyType::RSA && jwk.RsaParameters.has_value()) {
        auto modulus = DecodeNumber(jwk.RsaParameters->Modulus);
        auto exponent = DecodeNumber(jwk.RsaParameters->Exponent);
        TOpenSslPtr<RSA, RSA_free> rsa(RSA_new());
        if (modulus == nullptr || exponent == nullptr || rsa == nullptr
                || RSA_set0_key(rsa.get(), modulus.get(), exponent.get(), nullptr) != 1)
        {
            error = "Invalid RSA parameters";
            return {};
        }
        // RSA_set0_key takes ownership of the modulus and exponent on success.
        modulus.release();
        exponent.release();
        if (EVP_PKEY_set1_RSA(key.get(), rsa.get()) != 1) {
            error = "Failed to construct RSA public key";
            return {};
        }
    } else if (jwk.Type == EJwkKeyType::EC && jwk.EcParameters.has_value()) {
        const auto& params = jwk.EcParameters.value();
        const int curve = GetCurveId(params.Curve);
        if (curve == NID_undef) {
            error = "Unsupported EC curve: expected P-256, P-384 or P-521";
            return {};
        }
        TOpenSslPtr<EC_KEY, EC_KEY_free> ec(EC_KEY_new_by_curve_name(curve));
        if (ec == nullptr) {
            error = "Failed to allocate EC public key";
            return {};
        }
        const size_t size = (EC_GROUP_get_degree(EC_KEY_get0_group(ec.get())) + 7) / 8;
        if (params.X.size() != size || params.Y.size() != size) {
            error = "Invalid EC coordinate length";
            return {};
        }
        auto x = DecodeNumber(params.X);
        auto y = DecodeNumber(params.Y);
        if (x == nullptr || y == nullptr || EC_KEY_set_public_key_affine_coordinates(ec.get(), x.get(), y.get()) != 1
            || EVP_PKEY_set1_EC_KEY(key.get(), ec.get()) != 1)
        {
            error = "Invalid EC public point";
            return {};
        }
    } else {
        error = "Missing public key parameters for the specified key type";
        return {};
    }
    return key;
}

bool CheckRsaPssRestrictions(const TJwk& jwk, EVP_PKEY* key) {
    if (!jwk.Algorithm.has_value()) {
        return false;
    }
    const EVP_MD* digest = nullptr;
    switch (jwk.Algorithm.value()) {
        case EJwkAlg::PS256:
            digest = EVP_sha256();
            break;
        case EJwkAlg::PS384:
            digest = EVP_sha384();
            break;
        case EJwkAlg::PS512:
            digest = EVP_sha512();
            break;
        default:
            return false;
    }
    // OpenSSL enforces the SPKI restrictions when configuring verification.
    // JWA requires the same hash for MGF1 and a salt as long as the digest.
    TOpenSslPtr<EVP_PKEY_CTX, EVP_PKEY_CTX_free> ctx(EVP_PKEY_CTX_new(key, nullptr));
    return ctx != nullptr && EVP_PKEY_verify_init(ctx.get()) == 1
        && EVP_PKEY_CTX_set_rsa_padding(ctx.get(), RSA_PKCS1_PSS_PADDING) == 1
        && EVP_PKEY_CTX_set_signature_md(ctx.get(), digest) == 1
        && EVP_PKEY_CTX_set_rsa_mgf1_md(ctx.get(), digest) == 1
        && EVP_PKEY_CTX_set_rsa_pss_saltlen(ctx.get(), EVP_MD_size(digest)) == 1;
}

bool CheckPublicKey(const TJwk& jwk, EVP_PKEY* key, std::string& error) {
    if (jwk.Algorithm.has_value() && !IsCompatibleAlgorithm(jwk.Type, jwk.Algorithm.value())) {
        error = "JWK algorithm is incompatible with the key type";
        return false;
    }
    if (jwk.Type == EJwkKeyType::RSA) {
        const auto type = EVP_PKEY_base_id(key);
        if (type != EVP_PKEY_RSA && type != EVP_PKEY_RSA_PSS) {
            error = "Expected an RSA public key";
            return false;
        }
        if (type == EVP_PKEY_RSA_PSS && !CheckRsaPssRestrictions(jwk, key)) {
            error = "RSA-PSS public key requires a compatible PS256, PS384 or PS512 algorithm, hash and salt length";
            return false;
        }
        const auto* rsa = EVP_PKEY_get0_RSA(key);
        const auto* modulus = RSA_get0_n(rsa);
        const auto* exponent = RSA_get0_e(rsa);
        if (modulus == nullptr || exponent == nullptr || BN_is_negative(modulus) || BN_is_negative(exponent)
            || !BN_is_odd(modulus) || !BN_is_odd(exponent) || BN_cmp(exponent, BN_value_one()) <= 0
            || BN_cmp(exponent, modulus) >= 0)
        {
            error = "Invalid RSA modulus or exponent";
            return false;
        }
        if (BN_num_bits(modulus) > OPENSSL_RSA_MAX_MODULUS_BITS) {
            error = "RSA modulus exceeds the OpenSSL size limit";
            return false;
        }
        return true;
    }
    if (jwk.Type != EJwkKeyType::EC || EVP_PKEY_base_id(key) != EVP_PKEY_EC) {
        error = "Expected an EC public key";
        return false;
    }
    const auto* ec = EVP_PKEY_get0_EC_KEY(key);
    if (ec == nullptr || EC_KEY_check_key(ec) != 1) {
        error = "Invalid EC public key";
        return false;
    }
    const int curve = EC_GROUP_get_curve_name(EC_KEY_get0_group(ec));
    if (curve != NID_X9_62_prime256v1 && curve != NID_secp384r1 && curve != NID_secp521r1) {
        error = "Unsupported EC curve: expected P-256, P-384 or P-521";
        return false;
    }
    if (jwk.Algorithm.has_value()) {
        int expectedCurve = NID_undef;
        switch (jwk.Algorithm.value()) {
            case EJwkAlg::ES256:
                expectedCurve = NID_X9_62_prime256v1;
                break;
            case EJwkAlg::ES384:
                expectedCurve = NID_secp384r1;
                break;
            case EJwkAlg::ES512:
                expectedCurve = NID_secp521r1;
                break;
            default:
                break;
        }
        if (expectedCurve != NID_undef && curve != expectedCurve) {
            error = "EC curve is incompatible with the JWK algorithm";
            return false;
        }
    }
    return true;
}

bool EqualPublicKeys(EJwkKeyType type, EVP_PKEY* parameters, EVP_PKEY* certificate) {
    if (type == EJwkKeyType::RSA) {
        // RSA parameters and an RSA-PSS SPKI have different EVP key types.
        // Both keys have already passed validation, including PSS restrictions.
        const auto* lhs = EVP_PKEY_get0_RSA(parameters);
        const auto* rhs = EVP_PKEY_get0_RSA(certificate);
        return BN_cmp(RSA_get0_n(lhs), RSA_get0_n(rhs)) == 0 && BN_cmp(RSA_get0_e(lhs), RSA_get0_e(rhs)) == 0;
    }
    return EVP_PKEY_cmp(parameters, certificate) == 1;
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
        case EJwkAlg::RSA1_5:
        case EJwkAlg::RSA_OAEP:
        case EJwkAlg::RSA_OAEP_256:
            return EJwkKeyType::RSA;
        case EJwkAlg::ES256:
        case EJwkAlg::ES384:
        case EJwkAlg::ES512:
        case EJwkAlg::ECDH_ES:
        case EJwkAlg::ECDH_ES_A128KW:
        case EJwkAlg::ECDH_ES_A192KW:
        case EJwkAlg::ECDH_ES_A256KW:
            return EJwkKeyType::EC;
        default:
            return std::nullopt;
    }
}

std::optional<std::string> TJwk::CalculatePublicKey(std::string& error) const {
    error.clear();
    TKeyPtr key;
    if (RsaParameters.has_value() || EcParameters.has_value()) {
        key = GetPublicKeyFromParameters(*this, error);
        if (key == nullptr || !CheckPublicKey(*this, key.get(), error)) {
            return std::nullopt;
        }
    }
    if (!X509Chain.empty()) {
        auto certificateKey = GetPublicKeyFromX5C(*this, error);
        if (certificateKey == nullptr || !CheckPublicKey(*this, certificateKey.get(), error)) {
            return std::nullopt;
        }
        if (key != nullptr && !EqualPublicKeys(Type, key.get(), certificateKey.get())) {
            error = "Public key parameters do not match x5c certificate index 0";
            return std::nullopt;
        }
        key = std::move(certificateKey);
    }
    if (key == nullptr) {
        error = "Missing public key parameters and x5c certificates";
        return std::nullopt;
    }
    TOpenSslPtr<BIO, BIO_free> bio(BIO_new(BIO_s_mem()));
    if (bio == nullptr || PEM_write_bio_PUBKEY(bio.get(), key.get()) != 1) {
        error = "Failed to serialize public key as PEM";
        return std::nullopt;
    }
    char* data = nullptr;
    const auto size = BIO_get_mem_data(bio.get(), &data);
    if (size <= 0) {
        error = "Empty public key PEM";
        return std::nullopt;
    }
    return std::string(data, size);
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
