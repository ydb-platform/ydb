#include "jwk.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

#include <openssl/ec.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509v3.h>

#include <memory>

namespace NKikimr::NSecurity {

namespace {

NJson::TJsonValue ParseJson(const TString& json) {
    NJson::TJsonValue value;
    NJson::ReadJsonTree(json, &value, true);
    return value;
}

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

TKeyPtr GenerateKey(int curve = NID_undef, int bits = 2048) {
    TOpenSslPtr<EVP_PKEY_CTX, EVP_PKEY_CTX_free> ctx(
        EVP_PKEY_CTX_new_id(curve == NID_undef ? EVP_PKEY_RSA : EVP_PKEY_EC, nullptr));
    UNIT_ASSERT(ctx != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(EVP_PKEY_keygen_init(ctx.get()), 1);
    if (curve == NID_undef) {
        UNIT_ASSERT_VALUES_EQUAL(EVP_PKEY_CTX_set_rsa_keygen_bits(ctx.get(), bits), 1);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(EVP_PKEY_CTX_set_ec_paramgen_curve_nid(ctx.get(), curve), 1);
    }
    EVP_PKEY* key = nullptr;
    UNIT_ASSERT_VALUES_EQUAL(EVP_PKEY_keygen(ctx.get(), &key), 1);
    return TKeyPtr(key);
}

TKeyPtr GeneratePssKey(const EVP_MD* digest = nullptr, const EVP_MD* mgf = nullptr, int saltLength = -1) {
    TOpenSslPtr<EVP_PKEY_CTX, EVP_PKEY_CTX_free> ctx(EVP_PKEY_CTX_new_id(EVP_PKEY_RSA_PSS, nullptr));
    UNIT_ASSERT(ctx != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(EVP_PKEY_keygen_init(ctx.get()), 1);
    UNIT_ASSERT_VALUES_EQUAL(EVP_PKEY_CTX_set_rsa_keygen_bits(ctx.get(), 2048), 1);
    if (digest != nullptr) {
        UNIT_ASSERT_VALUES_EQUAL(EVP_PKEY_CTX_set_rsa_pss_keygen_md(ctx.get(), digest), 1);
    }
    if (mgf != nullptr) {
        UNIT_ASSERT_VALUES_EQUAL(EVP_PKEY_CTX_set_rsa_pss_keygen_mgf1_md(ctx.get(), mgf), 1);
    }
    if (saltLength >= 0) {
        UNIT_ASSERT_VALUES_EQUAL(EVP_PKEY_CTX_set_rsa_pss_keygen_saltlen(ctx.get(), saltLength), 1);
    }
    EVP_PKEY* key = nullptr;
    UNIT_ASSERT_VALUES_EQUAL(EVP_PKEY_keygen(ctx.get(), &key), 1);
    return TKeyPtr(key);
}

TString EncodeNumber(const BIGNUM* number, int size = 0) {
    std::string bytes(size ? size : BN_num_bytes(number), '\0');
    UNIT_ASSERT_VALUES_EQUAL(BN_bn2binpad(number,
        reinterpret_cast<unsigned char*>(bytes.data()), bytes.size()), bytes.size());
    return Base64EncodeUrlNoPadding(bytes);
}

NJson::TJsonValue KeyParameters(EVP_PKEY* key, const char* curve = nullptr) {
    NJson::TJsonValue json(NJson::JSON_MAP);
    if (curve == nullptr) {
        json["kty"] = "RSA";
        const auto* rsa = EVP_PKEY_get0_RSA(key);
        json["n"] = EncodeNumber(RSA_get0_n(rsa));
        json["e"] = EncodeNumber(RSA_get0_e(rsa));
    } else {
        json["kty"] = "EC";
        json["crv"] = curve;
        const auto* ec = EVP_PKEY_get0_EC_KEY(key);
        const auto* group = EC_KEY_get0_group(ec);
        TOpenSslPtr<BIGNUM, BN_free> x(BN_new());
        TOpenSslPtr<BIGNUM, BN_free> y(BN_new());
        UNIT_ASSERT(x != nullptr && y != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(EC_POINT_get_affine_coordinates(group,
            EC_KEY_get0_public_key(ec), x.get(), y.get(), nullptr), 1);
        const int size = (EC_GROUP_get_degree(group) + 7) / 8;
        json["x"] = EncodeNumber(x.get(), size);
        json["y"] = EncodeNumber(y.get(), size);
    }
    return json;
}

std::string PublicKeyPem(EVP_PKEY* key) {
    TOpenSslPtr<BIO, BIO_free> bio(BIO_new(BIO_s_mem()));
    UNIT_ASSERT(bio != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(PEM_write_bio_PUBKEY(bio.get(), key), 1);
    char* data = nullptr;
    const auto size = BIO_get_mem_data(bio.get(), &data);
    return std::string(data, size);
}

TCertPtr MakeCertificate(EVP_PKEY* key, const char* name, X509* issuer = nullptr,
    EVP_PKEY* issuerKey = nullptr, const char* constraints = "critical,CA:FALSE",
    long notBefore = -3600, long notAfter = 3600, const char* usage = "digitalSignature")
{
    TCertPtr cert(X509_new());
    UNIT_ASSERT(cert != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(X509_set_version(cert.get(), 2), 1);
    UNIT_ASSERT_VALUES_EQUAL(ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1), 1);
    UNIT_ASSERT(X509_gmtime_adj(X509_getm_notBefore(cert.get()), notBefore) != nullptr);
    UNIT_ASSERT(X509_gmtime_adj(X509_getm_notAfter(cert.get()), notAfter) != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(X509_set_pubkey(cert.get(), key), 1);
    auto* subject = X509_get_subject_name(cert.get());
    UNIT_ASSERT_VALUES_EQUAL(X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
        reinterpret_cast<const unsigned char*>(name), -1, -1, 0), 1);
    UNIT_ASSERT_VALUES_EQUAL(X509_set_issuer_name(cert.get(),
        issuer != nullptr ? X509_get_subject_name(issuer) : subject), 1);
    for (const auto& [nid, value] : {std::pair{NID_basic_constraints, constraints},
                                   std::pair{NID_key_usage, usage}}) {
        TOpenSslPtr<X509_EXTENSION, X509_EXTENSION_free> ext(
            X509V3_EXT_conf_nid(nullptr, nullptr, nid, const_cast<char*>(value)));
        UNIT_ASSERT(ext != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(X509_add_ext(cert.get(), ext.get(), -1), 1);
    }
    X509V3_CTX ctx;
    X509V3_set_ctx(&ctx, issuer != nullptr ? issuer : cert.get(), cert.get(), nullptr, nullptr, 0);
    for (const auto& [nid, value] : {std::pair{NID_subject_key_identifier, "hash"},
                                   std::pair{NID_authority_key_identifier, "keyid:always"}}) {
        TOpenSslPtr<X509_EXTENSION, X509_EXTENSION_free> ext(
            X509V3_EXT_conf_nid(nullptr, &ctx, nid, const_cast<char*>(value)));
        UNIT_ASSERT(ext != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(X509_add_ext(cert.get(), ext.get(), -1), 1);
    }
    UNIT_ASSERT(X509_sign(cert.get(), issuerKey != nullptr ? issuerKey : key, EVP_sha256()) > 0);
    return cert;
}

std::string CertificateDer(X509* cert) {
    const auto size = i2d_X509(cert, nullptr);
    UNIT_ASSERT(size > 0);
    std::string der(size, '\0');
    auto* data = reinterpret_cast<unsigned char*>(der.data());
    UNIT_ASSERT_VALUES_EQUAL(i2d_X509(cert, &data), size);
    return der;
}

void SetChain(NJson::TJsonValue& json, std::initializer_list<X509*> certs) {
    json["x5c"] = NJson::TJsonValue(NJson::JSON_ARRAY);
    for (auto* cert : certs) {
        json["x5c"].AppendValue(Base64Encode(CertificateDer(cert)));
    }
}

void AssertInvalidKey(const NJson::TJsonValue& json, const TString& reason = {}) {
    const auto jwk = ParseJwk(json);
    if (!jwk.has_value()) {
        UNIT_ASSERT_C(reason.empty(), "Expected a public key validation error, but JWK parsing failed");
        return;
    }
    std::string error;
    UNIT_ASSERT(!jwk.value().CalculatePublicKey(error).has_value());
    UNIT_ASSERT(!error.empty());
    if (!reason.empty()) {
        UNIT_ASSERT_STRING_CONTAINS(error, reason);
    }
}

void AssertPublicKey(const TJwk& jwk, EVP_PKEY* key) {
    std::string error = "previous error";
    const auto publicKey = jwk.CalculatePublicKey(error);
    UNIT_ASSERT_C(publicKey.has_value(), error);
    UNIT_ASSERT(error.empty());
    UNIT_ASSERT_VALUES_EQUAL(publicKey.value(), PublicKeyPem(key));
}

} // namespace

Y_UNIT_TEST_SUITE(TParseJwkTest) {

    // RFC 7517 Section 4.1 — "kty" (Key Type) Parameter
    // The "kty" parameter is REQUIRED.

    Y_UNIT_TEST(KtyRSA) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Type, EJwkKeyType::RSA);
    }

    Y_UNIT_TEST(KtyEC) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "EC"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Type, EJwkKeyType::EC);
    }

    Y_UNIT_TEST(KtyMissing) {
        const auto jwk = ParseJwk(ParseJson(R"({})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(KtyUnknown) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "oct"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(KtyNotString) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": 123})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    // RFC 7517 Section 4.2 — "use" (Public Key Use) Parameter

    Y_UNIT_TEST(UseSig) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "use": "sig"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->Usage.has_value());
        UNIT_ASSERT_EQUAL(jwk->Usage.value(), EJwkUsage::SIG);
    }

    Y_UNIT_TEST(UseEnc) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "use": "enc"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->Usage.has_value());
        UNIT_ASSERT_EQUAL(jwk->Usage.value(), EJwkUsage::ENC);
    }

    Y_UNIT_TEST(UseMissing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(!jwk->Usage.has_value());
    }

    Y_UNIT_TEST(UseUnknown) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "use": "unknown"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(!jwk->Usage.has_value());
    }

    // RFC 7517 Section 4.3 — "key_ops" (Key Operations) Parameter

    Y_UNIT_TEST(KeyOpsAllValues) {
        const auto jwk = ParseJwk(ParseJson(R"({
            "kty": "RSA",
            "key_ops": ["sign", "verify", "encrypt", "decrypt", "wrapKey", "unwrapKey", "deriveKey", "deriveBits"]
        })"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk.value().KeyOperations.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk.value().KeyOperations.value().size(), 8);
        UNIT_ASSERT_EQUAL(jwk.value().KeyOperations.value()[0], EJwkKeyOps::SIGN);
        UNIT_ASSERT_EQUAL(jwk.value().KeyOperations.value()[1], EJwkKeyOps::VERIFY);
        UNIT_ASSERT_EQUAL(jwk.value().KeyOperations.value()[2], EJwkKeyOps::ENCRYPT);
        UNIT_ASSERT_EQUAL(jwk.value().KeyOperations.value()[3], EJwkKeyOps::DECRYPT);
        UNIT_ASSERT_EQUAL(jwk.value().KeyOperations.value()[4], EJwkKeyOps::WRAP_KEY);
        UNIT_ASSERT_EQUAL(jwk.value().KeyOperations.value()[5], EJwkKeyOps::UNWRAP_KEY);
        UNIT_ASSERT_EQUAL(jwk.value().KeyOperations.value()[6], EJwkKeyOps::DERIVE_KEY);
        UNIT_ASSERT_EQUAL(jwk.value().KeyOperations.value()[7], EJwkKeyOps::DERIVE_BITS);
    }

    Y_UNIT_TEST(KeyOpsMissing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(!jwk.value().KeyOperations.has_value());
    }

    Y_UNIT_TEST(KeyOpsEmpty) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "key_ops": []})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk.value().KeyOperations.has_value());
        UNIT_ASSERT(jwk.value().KeyOperations.value().empty());
    }

    Y_UNIT_TEST(KeyOpsUnknownValuesRejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "key_ops": ["sign", "unknown", "verify"]})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(KeyOpsNonStringValuesRejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "key_ops": ["sign", 123, "verify"]})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(KeyOpsNotArray) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "key_ops": "sign"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(KeyOpsDuplicatesRejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "key_ops": ["verify", "verify"]})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(KeyOpsExtensionOnlyRejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "key_ops": ["extension"]})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(KeyOpsNullRejected) {
        UNIT_ASSERT(!ParseJwk(ParseJson(R"({"kty": "RSA", "key_ops": null})")).has_value());
    }

    Y_UNIT_TEST(ThumbprintsRequireCanonicalBase64Url) {
        for (const auto& [name, size] : {std::pair{"x5t", 20}, std::pair{"x5t#S256", 32}}) {
            auto json = ParseJson(R"({"kty": "RSA"})");
            const auto canonical = Base64EncodeUrlNoPadding(std::string(size, '\xff'));
            json[name] = canonical;
            UNIT_ASSERT(ParseJwk(json).has_value());
            json[name] = canonical + "=";
            UNIT_ASSERT(!ParseJwk(json).has_value());
            json[name] = Base64Encode(std::string(size, '\xff'));
            UNIT_ASSERT(!ParseJwk(json).has_value());
            auto nonCanonical = canonical;
            nonCanonical.back() = '_'; // Nonzero unused bits, same decoded bytes.
            json[name] = nonCanonical;
            UNIT_ASSERT(!ParseJwk(json).has_value());
        }
    }

    Y_UNIT_TEST(CertificateChainLengthBound) {
        auto json = ParseJson(R"({"kty": "RSA", "x5c": []})");
        for (size_t i = 0; i < 100; ++i) {
            json["x5c"].AppendValue(Base64Encode("certificate"));
        }
        UNIT_ASSERT(ParseJwk(json).has_value());
        json["x5c"].AppendValue(Base64Encode("certificate"));
        UNIT_ASSERT(!ParseJwk(json).has_value());
    }

    Y_UNIT_TEST(CertificateChainLengthBoundForConstructedJwk) {
        TJwk jwk(EJwkKeyType::RSA);
        jwk.X509Chain.resize(101, "invalid DER");
        std::string error;
        UNIT_ASSERT(!jwk.CalculatePublicKey(error).has_value());
        UNIT_ASSERT_STRING_CONTAINS(error, "between 1 and 100");
    }

    // RFC 7517 Section 4.4 — "alg" (Algorithm) Parameter

    Y_UNIT_TEST(AlgNoneRejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": "none"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgHS256Rejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": "HS256"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgHS384Rejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": "HS384"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgHS512Rejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": "HS512"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgRS256) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": "RS256"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), EJwkAlg::RS256);
    }

    Y_UNIT_TEST(AlgRS384) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": "RS384"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), EJwkAlg::RS384);
    }

    Y_UNIT_TEST(AlgRS512) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": "RS512"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), EJwkAlg::RS512);
    }

    Y_UNIT_TEST(AlgES256) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "EC", "alg": "ES256"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), EJwkAlg::ES256);
    }

    Y_UNIT_TEST(AlgES384) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "EC", "alg": "ES384"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), EJwkAlg::ES384);
    }

    Y_UNIT_TEST(AlgES512) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "EC", "alg": "ES512"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), EJwkAlg::ES512);
    }

    Y_UNIT_TEST(AlgPS256) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": "PS256"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), EJwkAlg::PS256);
    }

    Y_UNIT_TEST(AlgPS384) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": "PS384"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), EJwkAlg::PS384);
    }

    Y_UNIT_TEST(AlgPS512) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": "PS512"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), EJwkAlg::PS512);
    }

    Y_UNIT_TEST(AlgMissing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(!jwk->Algorithm.has_value());
    }

    Y_UNIT_TEST(AlgUnknownRejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": "UNKNOWN"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgNotStringRejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": 123})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgRSAWithECAlgorithmRejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "alg": "ES256"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgECWithRSAAlgorithmRejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "EC", "alg": "RS256"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgECWithPSAlgorithmRejected) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "EC", "alg": "PS256"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    // RFC 7517 Section 4.5 — "kid" (Key ID) Parameter

    Y_UNIT_TEST(Kid) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "kid": "my-key-id"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->KeyId, "my-key-id");
    }

    Y_UNIT_TEST(KidMissing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->KeyId.empty());
    }

    // RFC 7517 Section 4.6 — "x5u" (X.509 URL) Parameter

    Y_UNIT_TEST(X5U) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "x5u": "https://example.com/cert"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Url, "https://example.com/cert");
    }

    Y_UNIT_TEST(X5UMissing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->X509Url.empty());
    }

    // RFC 7517 Section 4.7 — "x5c" (X.509 Certificate Chain) Parameter
    // Values are base64-encoded and decoded during parsing.

    Y_UNIT_TEST(X5C) {
        // "cert-data-1" -> base64 "Y2VydC1kYXRhLTE="
        // "cert-data-2" -> base64 "Y2VydC1kYXRhLTI="
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "x5c": ["Y2VydC1kYXRhLTE=", "Y2VydC1kYXRhLTI="]})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Chain.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Chain[0], "cert-data-1");
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Chain[1], "cert-data-2");
    }

    Y_UNIT_TEST(X5CMissing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->X509Chain.empty());
    }

    Y_UNIT_TEST(X5CEmpty) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "x5c": []})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(X5CNonStringElementFailsParsing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "x5c": ["Y2VydC1kYXRhLTE=", 123]})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(X5CNotArrayFailsParsing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "x5c": "Y2VydC1kYXRhLTE="})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(X5CInvalidBase64FailsParsing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "x5c": ["not-base64!"]})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    // RFC 7517 Section 4.8 — "x5t" (X.509 Certificate SHA-1 Thumbprint) Parameter
    // Value is base64url-encoded and decoded during parsing.

    Y_UNIT_TEST(X5T) {
        // "12345678901234567890" -> base64url "MTIzNDU2Nzg5MDEyMzQ1Njc4OTA"
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "x5t": "MTIzNDU2Nzg5MDEyMzQ1Njc4OTA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509CertificateSha1ThumbprintBytes, "12345678901234567890");
    }

    Y_UNIT_TEST(X5TMissing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->X509CertificateSha1ThumbprintBytes.empty());
    }

    Y_UNIT_TEST(X5TInvalidBase64FailsParsing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "x5t": "not-base64!"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(X5TWrongLengthFailsParsing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "x5t": "d3Jvbmc"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    // RFC 7517 Section 4.9 — "x5t#S256" (X.509 Certificate SHA-256 Thumbprint) Parameter
    // Value is base64url-encoded and decoded during parsing.

    Y_UNIT_TEST(X5TS256) {
        // "12345678901234567890123456789012" -> base64url "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI"
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "x5t#S256": "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509CertificateSha256ThumbprintBytes, "12345678901234567890123456789012");
    }

    Y_UNIT_TEST(X5TS256Missing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->X509CertificateSha256ThumbprintBytes.empty());
    }

    Y_UNIT_TEST(X5TS256InvalidBase64FailsParsing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "x5t#S256": "not-base64!"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(X5TS256WrongLengthFailsParsing) {
        const auto jwk = ParseJwk(ParseJson(R"({"kty": "RSA", "x5t#S256": "d3Jvbmc"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    // Full JWK with all parameters

    Y_UNIT_TEST(AllParameters) {
        // x5c: "cert-data" -> base64 "Y2VydC1kYXRh"
        // x5t: "12345678901234567890" -> base64url "MTIzNDU2Nzg5MDEyMzQ1Njc4OTA"
        // x5t#S256: "12345678901234567890123456789012" -> base64url "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI"
        const auto jwk = ParseJwk(ParseJson(R"({
            "kty": "RSA",
            "use": "sig",
            "key_ops": ["sign", "verify"],
            "alg": "RS256",
            "kid": "my-key-id",
            "x5u": "https://example.com/cert",
            "x5c": ["Y2VydC1kYXRh"],
            "x5t": "MTIzNDU2Nzg5MDEyMzQ1Njc4OTA",
            "x5t#S256": "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI"
        })"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Type, EJwkKeyType::RSA);
        UNIT_ASSERT_EQUAL(jwk->Usage.value(), EJwkUsage::SIG);
        UNIT_ASSERT(jwk.value().KeyOperations.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk.value().KeyOperations.value().size(), 2);
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), EJwkAlg::RS256);
        UNIT_ASSERT_VALUES_EQUAL(jwk->KeyId, "my-key-id");
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Url, "https://example.com/cert");
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Chain.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Chain[0], "cert-data");
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509CertificateSha1ThumbprintBytes, "12345678901234567890");
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509CertificateSha256ThumbprintBytes, "12345678901234567890123456789012");
    }
}

Y_UNIT_TEST_SUITE(TParseJwkSetTest) {

    // RFC 7517 Section 5 — JWK Set Format
    // A JWK Set is a JSON object that contains a "keys" member whose value is an array of JWK objects.

    Y_UNIT_TEST(SingleKey) {
        const auto jwkSet = ParseJwkSet(ParseJson(R"({
            "keys": [{"kty": "RSA", "kid": "key1"}]
        })"));
        UNIT_ASSERT(jwkSet.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys.size(), 1);
        UNIT_ASSERT_EQUAL(jwkSet->Keys[0].Type, EJwkKeyType::RSA);
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys[0].KeyId, "key1");
    }

    Y_UNIT_TEST(MultipleKeys) {
        const auto jwkSet = ParseJwkSet(ParseJson(R"({
            "keys": [
                {"kty": "RSA", "kid": "rsa-key"},
                {"kty": "EC", "kid": "ec-key"}
            ]
        })"));
        UNIT_ASSERT(jwkSet.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys.size(), 2);
        UNIT_ASSERT_EQUAL(jwkSet->Keys[0].Type, EJwkKeyType::RSA);
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys[0].KeyId, "rsa-key");
        UNIT_ASSERT_EQUAL(jwkSet->Keys[1].Type, EJwkKeyType::EC);
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys[1].KeyId, "ec-key");
    }

    Y_UNIT_TEST(EmptyKeysArray) {
        const auto jwkSet = ParseJwkSet(ParseJson(R"({"keys": []})"));
        UNIT_ASSERT(jwkSet.has_value());
        UNIT_ASSERT(jwkSet->Keys.empty());
    }

    Y_UNIT_TEST(KeysMissing) {
        const auto jwkSet = ParseJwkSet(ParseJson(R"({})"));
        UNIT_ASSERT(!jwkSet.has_value());
    }

    Y_UNIT_TEST(KeysNotArray) {
        const auto jwkSet = ParseJwkSet(ParseJson(R"({"keys": "not-an-array"})"));
        UNIT_ASSERT(!jwkSet.has_value());
    }

    Y_UNIT_TEST(InvalidKeyIgnored) {
        const auto jwkSet = ParseJwkSet(ParseJson(R"({
            "keys": [
                {"kty": "RSA", "kid": "valid"},
                {"kty": "unknown"},
                {"kty": "EC", "kid": "also-valid"}
            ]
        })"));
        UNIT_ASSERT(jwkSet.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys.size(), 2);
        UNIT_ASSERT_EQUAL(jwkSet->Keys[0].Type, EJwkKeyType::RSA);
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys[0].KeyId, "valid");
        UNIT_ASSERT_EQUAL(jwkSet->Keys[1].Type, EJwkKeyType::EC);
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys[1].KeyId, "also-valid");
    }

}

Y_UNIT_TEST_SUITE(TPublicKeysTest) {

    Y_UNIT_TEST(CalculateCorrectPublicKeys) {
        const auto jwkSet = ParseJwkSet(ParseJson(R"({
            "keys": [
              {
                "kid": "bDn9Wp5nJoyppnjEWdjhfpK6nCLmBMHZxbVjcxA33tI",
                "kty": "RSA",
                "x5c": [
                  "MIICozCCAYsCBgGeOZ43AjANBgkqhkiG9w0BAQsFADAVMRMwEQYDVQQDDApwcm9kdWN0aW9uMB4XDTI2MDUxODA1NDM1MFoXDTM2MDUxODA1NDUzMFowFTETMBEGA1UEAwwKcHJvZHVjdGlvbjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAPGH/axeTkeHg0sUq/M6ut+YoKG2V77o8F+Nq0PWQO+EQLzm8v/hGVJhizULQYHfVBhPIyzejLYDvcUtNWXwa5Mos2vVcA5SrtZWsikjKOJOhpNo0l3qYvq6xGltyLX+yB4slIT6SYSm1/rOzW2XjYP0GI8eJYGw+kVxZvB3I15Q29EaShULNCUnDltaOEPVI6gV8h7i0Okjhosc5G/rij2z29xwqpFYs+DnzWMiJHvdLValnuWy/8fDNraaBIopxE3sDOMTMkqBzM/wxPbDpRygIdv1FWfvBnMrnYKPumcYRTsRxBQBlcbSCEgvvkjJ3DTTxoVIAOOgbzsFNjZm3usCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAoeb91xEPgnBtSmOuVKUCGER0hXJlT1iRCMyxzb2uA/IoT//GGGOCUhlt2wKkOHR8NZS6QFzBO0/tB1t9/AXORjJyme8H1Xg6+1EwpzwBO2+Om6iJsNnga0eLL0xh9UvIciQzwVF6rSS6wSqxvVaozjMHD7b0CfI7Kezdz3sJKT1TmYA9xVcdVyTyxRU4JjGrLtvKGqjgywXlzCKKkPpcoUtMSKVzNIgN92U/v/47Y+cFAGVZ7k4mIuGbCRe4gxgK39tYuyAoAKNxDzp5qAQ3Pbs41pojRNEtetsOuR57sN/7GIlLkjhoOFzgpZ/ZZ6bdtYsqbPEKzS/sVz6JEhFsvA=="
                ],
                "x5t": "cV52LLmVDLw0mv2s1yx7dFYsQKQ",
                "x5t#S256": "W-a6UnSQH4EX5bKR5e_2i55-llXgKq-KTLZrAnC-pHA"
              },
              {
                "kid": "8CZLx-P9fm6B2O1u7y_lYVclluzc0vustvRYtkepOxw",
                "kty": "RSA",
                "x5c": [
                  "MIICozCCAYsCBgGeOZ43ujANBgkqhkiG9w0BAQsFADAVMRMwEQYDVQQDDApwcm9kdWN0aW9uMB4XDTI2MDUxODA1NDM1MFoXDTM2MDUxODA1NDUzMFowFTETMBEGA1UEAwwKcHJvZHVjdGlvbjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAM4m+v0v2JDLLSh9O5ElG9wFfb3j5gF+AAV3YNzteU8DsHBxn5IzlV7GUnpwsDe7Kq2jQl6RzmW8N4PuZROPhUSf75Qj+YD94faaFJ62ef4b74ovnU5lh0K6ypoACsSSPegTfeiAE3FDuxs5NQHdoGBu39jDWDLu7ojVk/lxbnjDpRTMwvZ/RxO4JgvUPnq69cza0FZAHQQjBdCrzrlAcruZDnJ9zyPeRTrsQu7w/xXqHmY5FALNDYrp+QIcSpBRbTOIQM9Ml8A9c8EJI6x19oo6aL98eWJUrHnkbyX6hmXSrJHGHzrCIMrQPdWvHV+APe8gJ0eX6UDeAfWI9UuiWPMCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAAMfKVQ9sc3kEKNSKcA6bKCRX1wqSHyHbAM1NnKaYXU7sWJQDhdpoPdLAFyVU9i9EBpP+3GpFkwrkTBQhE6f76ZMmRjz4t33f81FZquv5UbkQA90ULhzCpiUt6DzGrZnciZ1VvLcqn/sc88tjIKimN+12fPRpP9AXs+wvfSeT4NfsfzS7ccUSllFS/28p3Y0Z1S8zr7a/Nikhua2yfmExVzR6AeFBtzhXk516z4Dd6e9nejMcP9Ua13wG1goyduj52E3ddTySoiMWSyfwU1dt1k1THDY/OUneJ8Ah0A85yuzfjXz9ntmeuXOpd3DACf/nP+gwuE/0SzclCgDcjvfDMw=="
                ],
                "x5t#S256": "aSZekK2LXbQ-wR7C9cb12dZbnQIHW5v6QA6O97Kxr88"
              }
            ]
        })"));

        UNIT_ASSERT(jwkSet.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys.size(), 2);
        std::string error;
        const auto firstPublicKey = jwkSet->Keys[0].CalculatePublicKey(error);
        UNIT_ASSERT_C(firstPublicKey.has_value(), error);
        UNIT_ASSERT_STRINGS_EQUAL(
            firstPublicKey.value(),
            "-----BEGIN PUBLIC KEY-----\n"
            "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA8Yf9rF5OR4eDSxSr8zq6\n"
            "35igobZXvujwX42rQ9ZA74RAvOby/+EZUmGLNQtBgd9UGE8jLN6MtgO9xS01ZfBr\n"
            "kyiza9VwDlKu1layKSMo4k6Gk2jSXepi+rrEaW3Itf7IHiyUhPpJhKbX+s7NbZeN\n"
            "g/QYjx4lgbD6RXFm8HcjXlDb0RpKFQs0JScOW1o4Q9UjqBXyHuLQ6SOGixzkb+uK\n"
            "PbPb3HCqkViz4OfNYyIke90tVqWe5bL/x8M2tpoEiinETewM4xMySoHMz/DE9sOl\n"
            "HKAh2/UVZ+8Gcyudgo+6ZxhFOxHEFAGVxtIISC++SMncNNPGhUgA46BvOwU2Nmbe\n"
            "6wIDAQAB\n"
            "-----END PUBLIC KEY-----\n");
        const auto secondPublicKey = jwkSet->Keys[1].CalculatePublicKey(error);
        UNIT_ASSERT_C(secondPublicKey.has_value(), error);
        UNIT_ASSERT_VALUES_EQUAL(
            secondPublicKey.value(),
            "-----BEGIN PUBLIC KEY-----\n"
            "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzib6/S/YkMstKH07kSUb\n"
            "3AV9vePmAX4ABXdg3O15TwOwcHGfkjOVXsZSenCwN7sqraNCXpHOZbw3g+5lE4+F\n"
            "RJ/vlCP5gP3h9poUnrZ5/hvvii+dTmWHQrrKmgAKxJI96BN96IATcUO7Gzk1Ad2g\n"
            "YG7f2MNYMu7uiNWT+XFueMOlFMzC9n9HE7gmC9Q+err1zNrQVkAdBCMF0KvOuUBy\n"
            "u5kOcn3PI95FOuxC7vD/FeoeZjkUAs0Niun5AhxKkFFtM4hAz0yXwD1zwQkjrHX2\n"
            "ijpov3x5YlSseeRvJfqGZdKskcYfOsIgytA91a8dX4A97yAnR5fpQN4B9Yj1S6JY\n"
            "8wIDAQAB\n"
            "-----END PUBLIC KEY-----\n");
    }

    Y_UNIT_TEST(CalculatePublicKeyWithoutThumbprint) {
        const auto jwk = ParseJwk(ParseJson(R"({
            "kty": "RSA",
            "alg": "RS256",
            "x5c": [
                "MIICozCCAYsCBgGeOZ43AjANBgkqhkiG9w0BAQsFADAVMRMwEQYDVQQDDApwcm9kdWN0aW9uMB4XDTI2MDUxODA1NDM1MFoXDTM2MDUxODA1NDUzMFowFTETMBEGA1UEAwwKcHJvZHVjdGlvbjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAPGH/axeTkeHg0sUq/M6ut+YoKG2V77o8F+Nq0PWQO+EQLzm8v/hGVJhizULQYHfVBhPIyzejLYDvcUtNWXwa5Mos2vVcA5SrtZWsikjKOJOhpNo0l3qYvq6xGltyLX+yB4slIT6SYSm1/rOzW2XjYP0GI8eJYGw+kVxZvB3I15Q29EaShULNCUnDltaOEPVI6gV8h7i0Okjhosc5G/rij2z29xwqpFYs+DnzWMiJHvdLValnuWy/8fDNraaBIopxE3sDOMTMkqBzM/wxPbDpRygIdv1FWfvBnMrnYKPumcYRTsRxBQBlcbSCEgvvkjJ3DTTxoVIAOOgbzsFNjZm3usCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAoeb91xEPgnBtSmOuVKUCGER0hXJlT1iRCMyxzb2uA/IoT//GGGOCUhlt2wKkOHR8NZS6QFzBO0/tB1t9/AXORjJyme8H1Xg6+1EwpzwBO2+Om6iJsNnga0eLL0xh9UvIciQzwVF6rSS6wSqxvVaozjMHD7b0CfI7Kezdz3sJKT1TmYA9xVcdVyTyxRU4JjGrLtvKGqjgywXlzCKKkPpcoUtMSKVzNIgN92U/v/47Y+cFAGVZ7k4mIuGbCRe4gxgK39tYuyAoAKNxDzp5qAQ3Pbs41pojRNEtetsOuR57sN/7GIlLkjhoOFzgpZ/ZZ6bdtYsqbPEKzS/sVz6JEhFsvA=="
            ]
        })"));

        UNIT_ASSERT(jwk.has_value());
        std::string error;
        const auto publicKey = jwk->CalculatePublicKey(error);
        UNIT_ASSERT_C(publicKey.has_value(), error);
        UNIT_ASSERT_STRINGS_EQUAL(
            publicKey.value(),
            "-----BEGIN PUBLIC KEY-----\n"
            "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA8Yf9rF5OR4eDSxSr8zq6\n"
            "35igobZXvujwX42rQ9ZA74RAvOby/+EZUmGLNQtBgd9UGE8jLN6MtgO9xS01ZfBr\n"
            "kyiza9VwDlKu1layKSMo4k6Gk2jSXepi+rrEaW3Itf7IHiyUhPpJhKbX+s7NbZeN\n"
            "g/QYjx4lgbD6RXFm8HcjXlDb0RpKFQs0JScOW1o4Q9UjqBXyHuLQ6SOGixzkb+uK\n"
            "PbPb3HCqkViz4OfNYyIke90tVqWe5bL/x8M2tpoEiinETewM4xMySoHMz/DE9sOl\n"
            "HKAh2/UVZ+8Gcyudgo+6ZxhFOxHEFAGVxtIISC++SMncNNPGhUgA46BvOwU2Nmbe\n"
            "6wIDAQAB\n"
            "-----END PUBLIC KEY-----\n");
    }

    Y_UNIT_TEST(CalculatePublicKeyWithTooSmallRsaModulusReturnsNullopt) {
        const auto jwk = ParseJwk(ParseJson(R"({
            "kty": "RSA",
            "alg": "RS256",
            "n": "sXchDaQpG81NwH8YFkBM2fScS9bCqV8e3X5R2Ua3h2Y",
            "e": "AQAB"
        })"));

        UNIT_ASSERT(jwk.has_value());
        std::string error;
        UNIT_ASSERT(!jwk->CalculatePublicKey(error).has_value());
        UNIT_ASSERT(!error.empty());
    }

    Y_UNIT_TEST(CalculatePublicKeyWithInvalidX5CReturnsNullopt) {
        const auto jwk = ParseJwk(ParseJson(R"({
            "kty": "RSA",
            "alg": "RS256",
            "x5c": ["Y2VydC1kYXRh"]
        })"));

        UNIT_ASSERT(jwk.has_value());
        std::string error;
        UNIT_ASSERT(!jwk->CalculatePublicKey(error).has_value());
        UNIT_ASSERT(!error.empty());
    }

    Y_UNIT_TEST(CalculatePublicKeyWithWrongSha1ThumbprintReturnsNullopt) {
        const auto jwk = ParseJwk(ParseJson(R"({
            "kty": "RSA",
            "alg": "RS256",
            "x5c": [
                "MIICozCCAYsCBgGeOZ43AjANBgkqhkiG9w0BAQsFADAVMRMwEQYDVQQDDApwcm9kdWN0aW9uMB4XDTI2MDUxODA1NDM1MFoXDTM2MDUxODA1NDUzMFowFTETMBEGA1UEAwwKcHJvZHVjdGlvbjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAPGH/axeTkeHg0sUq/M6ut+YoKG2V77o8F+Nq0PWQO+EQLzm8v/hGVJhizULQYHfVBhPIyzejLYDvcUtNWXwa5Mos2vVcA5SrtZWsikjKOJOhpNo0l3qYvq6xGltyLX+yB4slIT6SYSm1/rOzW2XjYP0GI8eJYGw+kVxZvB3I15Q29EaShULNCUnDltaOEPVI6gV8h7i0Okjhosc5G/rij2z29xwqpFYs+DnzWMiJHvdLValnuWy/8fDNraaBIopxE3sDOMTMkqBzM/wxPbDpRygIdv1FWfvBnMrnYKPumcYRTsRxBQBlcbSCEgvvkjJ3DTTxoVIAOOgbzsFNjZm3usCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAoeb91xEPgnBtSmOuVKUCGER0hXJlT1iRCMyxzb2uA/IoT//GGGOCUhlt2wKkOHR8NZS6QFzBO0/tB1t9/AXORjJyme8H1Xg6+1EwpzwBO2+Om6iJsNnga0eLL0xh9UvIciQzwVF6rSS6wSqxvVaozjMHD7b0CfI7Kezdz3sJKT1TmYA9xVcdVyTyxRU4JjGrLtvKGqjgywXlzCKKkPpcoUtMSKVzNIgN92U/v/47Y+cFAGVZ7k4mIuGbCRe4gxgK39tYuyAoAKNxDzp5qAQ3Pbs41pojRNEtetsOuR57sN/7GIlLkjhoOFzgpZ/ZZ6bdtYsqbPEKzS/sVz6JEhFsvA=="
            ],
            "x5t": "MTIzNDU2Nzg5MDEyMzQ1Njc4OTA"
        })"));

        UNIT_ASSERT(jwk.has_value());
        std::string error;
        UNIT_ASSERT(!jwk->CalculatePublicKey(error).has_value());
        UNIT_ASSERT(!error.empty());
    }

    Y_UNIT_TEST(CalculatePublicKeyWithWrongSha256ThumbprintReturnsNullopt) {
        const auto jwk = ParseJwk(ParseJson(R"({
            "kty": "RSA",
            "alg": "RS256",
            "x5c": [
                "MIICozCCAYsCBgGeOZ43AjANBgkqhkiG9w0BAQsFADAVMRMwEQYDVQQDDApwcm9kdWN0aW9uMB4XDTI2MDUxODA1NDM1MFoXDTM2MDUxODA1NDUzMFowFTETMBEGA1UEAwwKcHJvZHVjdGlvbjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAPGH/axeTkeHg0sUq/M6ut+YoKG2V77o8F+Nq0PWQO+EQLzm8v/hGVJhizULQYHfVBhPIyzejLYDvcUtNWXwa5Mos2vVcA5SrtZWsikjKOJOhpNo0l3qYvq6xGltyLX+yB4slIT6SYSm1/rOzW2XjYP0GI8eJYGw+kVxZvB3I15Q29EaShULNCUnDltaOEPVI6gV8h7i0Okjhosc5G/rij2z29xwqpFYs+DnzWMiJHvdLValnuWy/8fDNraaBIopxE3sDOMTMkqBzM/wxPbDpRygIdv1FWfvBnMrnYKPumcYRTsRxBQBlcbSCEgvvkjJ3DTTxoVIAOOgbzsFNjZm3usCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAoeb91xEPgnBtSmOuVKUCGER0hXJlT1iRCMyxzb2uA/IoT//GGGOCUhlt2wKkOHR8NZS6QFzBO0/tB1t9/AXORjJyme8H1Xg6+1EwpzwBO2+Om6iJsNnga0eLL0xh9UvIciQzwVF6rSS6wSqxvVaozjMHD7b0CfI7Kezdz3sJKT1TmYA9xVcdVyTyxRU4JjGrLtvKGqjgywXlzCKKkPpcoUtMSKVzNIgN92U/v/47Y+cFAGVZ7k4mIuGbCRe4gxgK39tYuyAoAKNxDzp5qAQ3Pbs41pojRNEtetsOuR57sN/7GIlLkjhoOFzgpZ/ZZ6bdtYsqbPEKzS/sVz6JEhFsvA=="
            ],
            "x5t#S256": "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI"
        })"));

        UNIT_ASSERT(jwk.has_value());
        std::string error;
        UNIT_ASSERT(!jwk->CalculatePublicKey(error).has_value());
        UNIT_ASSERT(!error.empty());
    }

}

Y_UNIT_TEST_SUITE(TAlgToKtyTest) {

    Y_UNIT_TEST(GetKtyFromAlg) {
        {
            const auto algs =
                {EJwkAlg::RS256, EJwkAlg::RS384, EJwkAlg::RS512, EJwkAlg::PS256, EJwkAlg::PS384, EJwkAlg::PS512};
            for (const auto alg : algs) {
                const auto kty = GetKeyType(alg);
                UNIT_ASSERT(kty.has_value());
                UNIT_ASSERT_EQUAL(kty.value(), EJwkKeyType::RSA);
            }
        }

        {
            const auto algs = {EJwkAlg::ES256, EJwkAlg::ES384, EJwkAlg::ES512};
            for (const auto alg : algs) {
                const auto kty = GetKeyType(alg);
                UNIT_ASSERT(kty.has_value());
                UNIT_ASSERT_EQUAL(kty.value(), EJwkKeyType::EC);
            }
        }
    }

    Y_UNIT_TEST(GetKtyFromUnknownAlg) {
        const auto unknownAlg = static_cast<EJwkAlg>(255);
        UNIT_ASSERT(!GetKeyType(unknownAlg).has_value());
    }

}

Y_UNIT_TEST_SUITE(TJwkCryptoTest) {
    Y_UNIT_TEST(JweAlgorithms) {
        for (const auto* alg : {"RSA1_5", "RSA-OAEP", "RSA-OAEP-256",
                               "ECDH-ES", "ECDH-ES+A128KW", "ECDH-ES+A192KW", "ECDH-ES+A256KW"}) {
            NJson::TJsonValue json(NJson::JSON_MAP);
            const bool rsa = TStringBuf(alg).StartsWith("RSA");
            json["kty"] = rsa ? "RSA" : "EC";
            json["alg"] = alg;
            const auto jwk = ParseJwk(json);
            UNIT_ASSERT_C(jwk.has_value() && jwk.value().Algorithm.has_value(), alg);
            UNIT_ASSERT_VALUES_EQUAL(ToString(jwk.value().Algorithm.value()), alg);
            const auto keyType = GetKeyType(jwk.value().Algorithm.value());
            UNIT_ASSERT(keyType.has_value());
            UNIT_ASSERT_EQUAL(keyType.value(), jwk.value().Type);
            json["kty"] = rsa ? "EC" : "RSA";
            UNIT_ASSERT(!ParseJwk(json).has_value());
        }
    }

    Y_UNIT_TEST(RsaParameters) {
        const auto key = GenerateKey();
        const auto jwk = ParseJwk(KeyParameters(key.get()));
        UNIT_ASSERT(jwk.has_value());
        std::string error;
        const auto pem = jwk.value().CalculatePublicKey(error);
        UNIT_ASSERT_C(pem.has_value(), error);
        UNIT_ASSERT_VALUES_EQUAL(pem.value(), PublicKeyPem(key.get()));
    }

    Y_UNIT_TEST(EcParameters) {
        for (const auto& [curve, nid] : {std::pair{"P-256", NID_X9_62_prime256v1},
                                       std::pair{"P-384", NID_secp384r1},
                                       std::pair{"P-521", NID_secp521r1}}) {
            const auto key = GenerateKey(nid);
            const auto jwk = ParseJwk(KeyParameters(key.get(), curve));
            UNIT_ASSERT(jwk.has_value());
            std::string error;
            const auto pem = jwk.value().CalculatePublicKey(error);
            UNIT_ASSERT_C(pem.has_value(), curve);
            UNIT_ASSERT_VALUES_EQUAL(pem.value(), PublicKeyPem(key.get()));
        }
    }

    Y_UNIT_TEST(RejectInvalidRsaParameters) {
        const auto key = GenerateKey();
        const auto valid = KeyParameters(key.get());
        for (const auto* field : {"n", "e"}) {
            for (const auto* value : {"", "!", "A", "AA", "AQ", "Ag", "Ax", "AQAB=", "AQ+_", "AQABAA==AQAB"}) {
                auto json = valid;
                json[field] = value;
                AssertInvalidKey(json);
            }
            auto json = valid;
            json[field] = 123;
            AssertInvalidKey(json);
            json.EraseValue(field);
            AssertInvalidKey(json);
        }
        auto padded = valid;
        std::string modulus = Base64DecodeUneven(valid["n"].GetString());
        modulus.insert(modulus.begin(), '\0');
        padded["n"] = Base64EncodeUrlNoPadding(modulus);
        AssertInvalidKey(padded);
    }

    Y_UNIT_TEST(RejectInvalidEcParameters) {
        const auto key = GenerateKey(NID_X9_62_prime256v1);
        const auto valid = KeyParameters(key.get(), "P-256");
        for (const auto* field : {"crv", "x", "y"}) {
            auto json = valid;
            json.EraseValue(field);
            AssertInvalidKey(json);
            json[field] = 123;
            AssertInvalidKey(json);
        }
        for (const auto* value : {"", "not-base64!", "AA", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}) {
            auto json = valid;
            json["x"] = value;
            AssertInvalidKey(json);
        }
        auto json = valid;
        json["crv"] = "unknown";
        AssertInvalidKey(json);
        json = valid;
        json["alg"] = "ES384";
        AssertInvalidKey(json);
    }

    Y_UNIT_TEST(CertificateMustMatchKeyTypeAndParameters) {
        const auto key = GenerateKey();
        const auto cert = MakeCertificate(key.get(), "signing key");
        auto json = KeyParameters(key.get());
        SetChain(json, {cert.get()});
        auto jwk = ParseJwk(json);
        UNIT_ASSERT(jwk.has_value());
        AssertPublicKey(jwk.value(), key.get());
        json["e"] = "Aw";
        AssertInvalidKey(json);
        json.EraseValue("n");
        json.EraseValue("e");
        json["kty"] = "EC";
        AssertInvalidKey(json);
    }

    Y_UNIT_TEST(RejectInvalidCertificatesWithoutParameterFallback) {
        const auto key = GenerateKey();
        const auto cert = MakeCertificate(key.get(), "signing key");
        const auto valid = CertificateDer(cert.get());
        auto corrupt = valid;
        corrupt.back() ^= 1;
        for (const auto& der : {std::string{}, std::string{"invalid"}, valid + "trailing", corrupt}) {
            auto json = KeyParameters(key.get());
            json["x5c"] = NJson::TJsonValue(NJson::JSON_ARRAY);
            json["x5c"].AppendValue(Base64Encode(der));
            AssertInvalidKey(json);
        }
        auto emptyChain = KeyParameters(key.get());
        emptyChain["x5c"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        AssertInvalidKey(emptyChain);
        for (const auto& [begin, end] : {std::pair{-7200L, -3600L}, std::pair{3600L, 7200L}}) {
            const auto expired = MakeCertificate(key.get(), "invalid validity", nullptr, nullptr,
                "critical,CA:FALSE", begin, end);
            auto json = KeyParameters(key.get());
            SetChain(json, {expired.get()});
            AssertInvalidKey(json);
        }
    }

    Y_UNIT_TEST(CertificateChain) {
        const auto rootKey = GenerateKey();
        const auto root = MakeCertificate(rootKey.get(), "root", nullptr, nullptr,
            "critical,CA:TRUE,pathlen:1", -3600, 3600, "keyCertSign");
        const auto issuerKey = GenerateKey();
        const auto issuer = MakeCertificate(issuerKey.get(), "issuer", root.get(), rootKey.get(),
            "critical,CA:TRUE,pathlen:0", -3600, 3600, "keyCertSign");
        const auto key = GenerateKey();
        const auto leaf = MakeCertificate(key.get(), "leaf", issuer.get(), issuerKey.get());
        auto json = KeyParameters(key.get());
        SetChain(json, {leaf.get(), issuer.get(), root.get()});
        auto jwk = ParseJwk(json);
        UNIT_ASSERT(jwk.has_value());
        AssertPublicKey(jwk.value(), key.get());
        auto corruptLeaf = CertificateDer(leaf.get());
        corruptLeaf.back() ^= 1;
        json["x5c"][0] = Base64Encode(corruptLeaf);
        AssertInvalidKey(json);
        // The root and even the issuer may be omitted by an authenticated IdP.
        SetChain(json, {leaf.get(), issuer.get()});
        jwk = ParseJwk(json);
        UNIT_ASSERT(jwk.has_value());
        AssertPublicKey(jwk.value(), key.get());
        SetChain(json, {leaf.get()});
        jwk = ParseJwk(json);
        UNIT_ASSERT(jwk.has_value());
        AssertPublicKey(jwk.value(), key.get());
        SetChain(json, {leaf.get(), root.get(), issuer.get()});
        AssertInvalidKey(json, "certificate index 1: invalid issuer:");
        SetChain(json, {leaf.get(), issuer.get(), root.get(), root.get()});
        AssertInvalidKey(json);
        const auto wrongIssuer = MakeCertificate(rootKey.get(), "issuer", root.get(), rootKey.get(),
            "critical,CA:TRUE", -3600, 3600, "keyCertSign");
        SetChain(json, {leaf.get(), wrongIssuer.get(), root.get()});
        AssertInvalidKey(json);
        const auto nonCa = MakeCertificate(issuerKey.get(), "issuer", root.get(), rootKey.get());
        SetChain(json, {leaf.get(), nonCa.get(), root.get()});
        AssertInvalidKey(json);
        const auto expiredIssuer = MakeCertificate(issuerKey.get(), "issuer", root.get(), rootKey.get(),
            "critical,CA:TRUE", -7200, -3600, "keyCertSign");
        SetChain(json, {leaf.get(), expiredIssuer.get(), root.get()});
        AssertInvalidKey(json, "certificate index 1: certificate has expired (code 10)");
        const auto shortRoot = MakeCertificate(rootKey.get(), "root", nullptr, nullptr,
            "critical,CA:TRUE,pathlen:0", -3600, 3600, "keyCertSign");
        SetChain(json, {leaf.get(), issuer.get(), shortRoot.get()});
        AssertInvalidKey(json);
        const auto noCertSign = MakeCertificate(issuerKey.get(), "issuer", root.get(), rootKey.get(),
            "critical,CA:TRUE", -3600, 3600, "digitalSignature");
        SetChain(json, {leaf.get(), noCertSign.get(), root.get()});
        AssertInvalidKey(json);
    }

    Y_UNIT_TEST(CertificateKeyUsage) {
        const auto key = GenerateKey();
        const auto cert = MakeCertificate(key.get(), "encryption key", nullptr, nullptr,
            "critical,CA:FALSE", -3600, 3600, "keyEncipherment");
        auto json = KeyParameters(key.get());
        SetChain(json, {cert.get()});
        json["use"] = "enc";
        json["alg"] = "RSA-OAEP-256";
        auto jwk = ParseJwk(json);
        UNIT_ASSERT(jwk.has_value());
        AssertPublicKey(jwk.value(), key.get());
        json.EraseValue("alg");
        json["use"] = "sig";
        AssertInvalidKey(json);
        json.EraseValue("use");
        json["alg"] = "RS256";
        AssertInvalidKey(json);
        json.EraseValue("alg");
        json["key_ops"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        json["key_ops"].AppendValue("verify");
        AssertInvalidKey(json);
    }

    Y_UNIT_TEST(SelfIssuedRolloverCertificate) {
        const auto oldKey = GenerateKey();
        const auto oldCa = MakeCertificate(oldKey.get(), "CA", nullptr, nullptr,
            "critical,CA:TRUE", -3600, 3600, "keyCertSign");
        const auto newKey = GenerateKey();
        const auto rollover = MakeCertificate(newKey.get(), "CA", oldCa.get(), oldKey.get(),
            "critical,CA:TRUE", -3600, 3600, "keyCertSign");
        const auto key = GenerateKey();
        const auto leaf = MakeCertificate(key.get(), "leaf", rollover.get(), newKey.get());
        auto json = KeyParameters(key.get());
        SetChain(json, {leaf.get(), rollover.get(), oldCa.get()});
        auto jwk = ParseJwk(json);
        UNIT_ASSERT(jwk.has_value());
        AssertPublicKey(jwk.value(), key.get());
        SetChain(json, {leaf.get(), rollover.get()});
        jwk = ParseJwk(json);
        UNIT_ASSERT(jwk.has_value());
        AssertPublicKey(jwk.value(), key.get());
    }

    Y_UNIT_TEST(EcCertificate) {
        const auto key = GenerateKey(NID_X9_62_prime256v1);
        const auto cert = MakeCertificate(key.get(), "EC signing key");
        auto json = KeyParameters(key.get(), "P-256");
        SetChain(json, {cert.get()});
        json["alg"] = "ES256";
        auto jwk = ParseJwk(json);
        UNIT_ASSERT(jwk.has_value());
        AssertPublicKey(jwk.value(), key.get());
        const auto otherKey = GenerateKey(NID_X9_62_prime256v1);
        const auto otherParameters = KeyParameters(otherKey.get(), "P-256");
        json["x"] = otherParameters["x"];
        json["y"] = otherParameters["y"];
        AssertInvalidKey(json);
        json.EraseValue("crv");
        json.EraseValue("x");
        json.EraseValue("y");
        jwk = ParseJwk(json);
        UNIT_ASSERT(jwk.has_value());
        AssertPublicKey(jwk.value(), key.get());
        json["alg"] = "ES512";
        AssertInvalidKey(json);
    }

    Y_UNIT_TEST(CertificateCurveMustMatchParameters) {
        const auto key = GenerateKey(NID_X9_62_prime256v1);
        const auto otherKey = GenerateKey(NID_secp384r1);
        const auto cert = MakeCertificate(otherKey.get(), "P-384");
        auto json = KeyParameters(key.get(), "P-256");
        SetChain(json, {cert.get()});
        AssertInvalidKey(json, "parameters do not match x5c certificate index 0");
    }

    Y_UNIT_TEST(UnsupportedCertificateCurveRejected) {
        const auto key = GenerateKey(NID_secp256k1);
        const auto cert = MakeCertificate(key.get(), "secp256k1");
        auto json = ParseJson(R"({"kty": "EC"})");
        SetChain(json, {cert.get()});
        AssertInvalidKey(json, "Unsupported EC curve");
    }

    Y_UNIT_TEST(WeakRsaCertificateRejected) {
        const auto key = GenerateKey(NID_undef, 1024);
        const auto cert = MakeCertificate(key.get(), "RSA-1024");
        auto json = ParseJson(R"({"kty": "RSA"})");
        SetChain(json, {cert.get()});
        AssertInvalidKey(json, "RSA modulus below 2048 bits");
    }

    Y_UNIT_TEST(RsaPssCertificates) {
        const auto issuerKey = GenerateKey();
        const auto issuer = MakeCertificate(issuerKey.get(), "issuer", nullptr, nullptr,
            "critical,CA:TRUE", -3600, 3600, "keyCertSign");
        for (const auto& [alg, digest] : {std::pair{"PS256", EVP_sha256()},
                std::pair{"PS384", EVP_sha384()}, std::pair{"PS512", EVP_sha512()}}) {
            for (const bool restricted : {false, true}) {
                const auto key = restricted
                    ? GeneratePssKey(digest, digest, EVP_MD_size(digest)) : GeneratePssKey();
                const auto cert = MakeCertificate(key.get(), "PSS", issuer.get(), issuerKey.get());
                auto json = ParseJson(R"({"kty": "RSA"})");
                json["alg"] = alg;
                SetChain(json, {cert.get(), issuer.get()});
                auto jwk = ParseJwk(json);
                UNIT_ASSERT(jwk.has_value());
                AssertPublicKey(jwk.value(), key.get());
                const auto params = KeyParameters(key.get());
                json["n"] = params["n"];
                json["e"] = params["e"];
                jwk = ParseJwk(json);
                UNIT_ASSERT(jwk.has_value());
                AssertPublicKey(jwk.value(), key.get());
                json["n"] = KeyParameters(issuerKey.get())["n"];
                AssertInvalidKey(json);
            }
        }
    }

    Y_UNIT_TEST(IncompatibleRsaPssRestrictionsRejected) {
        const auto issuerKey = GenerateKey();
        const auto issuer = MakeCertificate(issuerKey.get(), "issuer", nullptr, nullptr,
            "critical,CA:TRUE", -3600, 3600, "keyCertSign");
        const auto check = [&](const EVP_MD* digest, const EVP_MD* mgf, int saltLength) {
            const auto key = GeneratePssKey(digest, mgf, saltLength);
            const auto cert = MakeCertificate(key.get(), "PSS", issuer.get(), issuerKey.get());
            auto json = ParseJson(R"({"kty": "RSA", "alg": "PS256"})");
            SetChain(json, {cert.get()});
            AssertInvalidKey(json);
        };
        check(EVP_sha384(), EVP_sha256(), 32);
        check(EVP_sha256(), EVP_sha384(), 32);
        check(EVP_sha256(), EVP_sha256(), 33);
        const auto key = GeneratePssKey(EVP_sha256(), EVP_sha256(), 20);
        const auto cert = MakeCertificate(key.get(), "PSS", issuer.get(), issuerKey.get());
        auto json = ParseJson(R"({"kty": "RSA", "alg": "PS256"})");
        SetChain(json, {cert.get()});
        const auto jwk = ParseJwk(json);
        UNIT_ASSERT(jwk.has_value());
        AssertPublicKey(jwk.value(), key.get()); // Encoded salt length is a minimum.
        for (const char* alg : {"RS256", "RSA-OAEP", "PS384", "PS512"}) {
            json["alg"] = alg;
            AssertInvalidKey(json);
        }
        json.EraseValue("alg");
        AssertInvalidKey(json);
    }

    Y_UNIT_TEST(MixedJwkSet) {
        const auto rsa = GenerateKey();
        const auto ec = GenerateKey(NID_X9_62_prime256v1);
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["keys"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        json["keys"].AppendValue(KeyParameters(rsa.get()));
        auto invalid = KeyParameters(rsa.get());
        invalid["n"] = "not-base64!";
        json["keys"].AppendValue(invalid);
        auto invalidOps = KeyParameters(rsa.get());
        invalidOps["key_ops"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        invalidOps["key_ops"].AppendValue("extension");
        json["keys"].AppendValue(invalidOps);
        json["keys"].AppendValue(KeyParameters(ec.get(), "P-256"));
        const auto jwks = ParseJwkSet(json);
        UNIT_ASSERT(jwks.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwks.value().Keys.size(), 2);
        AssertPublicKey(jwks.value().Keys[0], rsa.get());
        AssertPublicKey(jwks.value().Keys[1], ec.get());
    }
}

} // namespace NKikimr::NSecurity
