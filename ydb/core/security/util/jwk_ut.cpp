#include "jwk.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSecurity {

namespace {

NJson::TJsonValue ParseJson(const TString& json) {
    NJson::TJsonValue value;
    NJson::ReadJsonTree(json, &value, true);
    return value;
}

} // namespace

Y_UNIT_TEST_SUITE(TParseJWKTest) {

    // RFC 7517 Section 4.1 — "kty" (Key Type) Parameter
    // The "kty" parameter is REQUIRED.

    Y_UNIT_TEST(KtyRSA) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Type, TKeyType::RSA);
    }

    Y_UNIT_TEST(KtyEC) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "EC"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Type, TKeyType::EC);
    }

    Y_UNIT_TEST(KtyMissing) {
        const auto jwk = ParseJWK(ParseJson(R"({})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(KtyUnknown) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "oct"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(KtyNotString) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": 123})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    // RFC 7517 Section 4.2 — "use" (Public Key Use) Parameter

    Y_UNIT_TEST(UseSig) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "use": "sig"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->Usage.has_value());
        UNIT_ASSERT_EQUAL(jwk->Usage.value(), TUsage::SIG);
    }

    Y_UNIT_TEST(UseEnc) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "use": "enc"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->Usage.has_value());
        UNIT_ASSERT_EQUAL(jwk->Usage.value(), TUsage::ENC);
    }

    Y_UNIT_TEST(UseMissing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(!jwk->Usage.has_value());
    }

    Y_UNIT_TEST(UseUnknown) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "use": "unknown"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(!jwk->Usage.has_value());
    }

    // RFC 7517 Section 4.3 — "key_ops" (Key Operations) Parameter

    Y_UNIT_TEST(KeyOpsAllValues) {
        const auto jwk = ParseJWK(ParseJson(R"({
            "kty": "RSA",
            "key_ops": ["sign", "verify", "encrypt", "decrypt", "wrapKey", "unwrapKey", "deriveKey", "deriveBits"]
        })"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->KeyOperations.size(), 8);
        UNIT_ASSERT_EQUAL(jwk->KeyOperations[0], TKeyOps::SIGN);
        UNIT_ASSERT_EQUAL(jwk->KeyOperations[1], TKeyOps::VERIFY);
        UNIT_ASSERT_EQUAL(jwk->KeyOperations[2], TKeyOps::ENCRYPT);
        UNIT_ASSERT_EQUAL(jwk->KeyOperations[3], TKeyOps::DECRYPT);
        UNIT_ASSERT_EQUAL(jwk->KeyOperations[4], TKeyOps::WRAP_KEY);
        UNIT_ASSERT_EQUAL(jwk->KeyOperations[5], TKeyOps::UNWRAP_KEY);
        UNIT_ASSERT_EQUAL(jwk->KeyOperations[6], TKeyOps::DERIVE_KEY);
        UNIT_ASSERT_EQUAL(jwk->KeyOperations[7], TKeyOps::DERIVE_BITS);
    }

    Y_UNIT_TEST(KeyOpsMissing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->KeyOperations.empty());
    }

    Y_UNIT_TEST(KeyOpsEmpty) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "key_ops": []})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->KeyOperations.empty());
    }

    Y_UNIT_TEST(KeyOpsUnknownValuesSkipped) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "key_ops": ["sign", "unknown", "verify"]})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->KeyOperations.size(), 2);
        UNIT_ASSERT_EQUAL(jwk->KeyOperations[0], TKeyOps::SIGN);
        UNIT_ASSERT_EQUAL(jwk->KeyOperations[1], TKeyOps::VERIFY);
    }

    Y_UNIT_TEST(KeyOpsNonStringValuesSkipped) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "key_ops": ["sign", 123, "verify"]})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->KeyOperations.size(), 2);
        UNIT_ASSERT_EQUAL(jwk->KeyOperations[0], TKeyOps::SIGN);
        UNIT_ASSERT_EQUAL(jwk->KeyOperations[1], TKeyOps::VERIFY);
    }

    Y_UNIT_TEST(KeyOpsNotArray) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "key_ops": "sign"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->KeyOperations.empty());
    }

    // RFC 7517 Section 4.4 — "alg" (Algorithm) Parameter

    Y_UNIT_TEST(AlgNoneRejected) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": "none"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgHS256Rejected) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": "HS256"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgHS384Rejected) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": "HS384"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgHS512Rejected) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": "HS512"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgRS256) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": "RS256"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), TAlg::RS256);
    }

    Y_UNIT_TEST(AlgRS384) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": "RS384"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), TAlg::RS384);
    }

    Y_UNIT_TEST(AlgRS512) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": "RS512"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), TAlg::RS512);
    }

    Y_UNIT_TEST(AlgES256) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "EC", "alg": "ES256"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), TAlg::ES256);
    }

    Y_UNIT_TEST(AlgES384) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "EC", "alg": "ES384"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), TAlg::ES384);
    }

    Y_UNIT_TEST(AlgES512) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "EC", "alg": "ES512"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), TAlg::ES512);
    }

    Y_UNIT_TEST(AlgPS256) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": "PS256"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), TAlg::PS256);
    }

    Y_UNIT_TEST(AlgPS384) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": "PS384"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), TAlg::PS384);
    }

    Y_UNIT_TEST(AlgPS512) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": "PS512"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), TAlg::PS512);
    }

    Y_UNIT_TEST(AlgMissing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(!jwk->Algorithm.has_value());
    }

    Y_UNIT_TEST(AlgUnknownRejected) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": "UNKNOWN"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgNotStringRejected) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": 123})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgRSAWithECAlgorithmRejected) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "alg": "ES256"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgECWithRSAAlgorithmRejected) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "EC", "alg": "RS256"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(AlgECWithPSAlgorithmRejected) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "EC", "alg": "PS256"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    // RFC 7517 Section 4.5 — "kid" (Key ID) Parameter

    Y_UNIT_TEST(Kid) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "kid": "my-key-id"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->KeyId, "my-key-id");
    }

    Y_UNIT_TEST(KidMissing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->KeyId.empty());
    }

    // RFC 7517 Section 4.6 — "x5u" (X.509 URL) Parameter

    Y_UNIT_TEST(X5U) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "x5u": "https://example.com/cert"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Url, "https://example.com/cert");
    }

    Y_UNIT_TEST(X5UMissing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->X509Url.empty());
    }

    // RFC 7517 Section 4.7 — "x5c" (X.509 Certificate Chain) Parameter
    // Values are base64-encoded and decoded during parsing.

    Y_UNIT_TEST(X5C) {
        // "cert-data-1" -> base64 "Y2VydC1kYXRhLTE="
        // "cert-data-2" -> base64 "Y2VydC1kYXRhLTI="
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "x5c": ["Y2VydC1kYXRhLTE=", "Y2VydC1kYXRhLTI="]})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Chain.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Chain[0], "cert-data-1");
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Chain[1], "cert-data-2");
    }

    Y_UNIT_TEST(X5CMissing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->X509Chain.empty());
    }

    Y_UNIT_TEST(X5CEmpty) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "x5c": []})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->X509Chain.empty());
    }

    Y_UNIT_TEST(X5CNonStringElementFailsParsing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "x5c": ["Y2VydC1kYXRhLTE=", 123]})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(X5CNotArrayFailsParsing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "x5c": "Y2VydC1kYXRhLTE="})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(X5CInvalidBase64FailsParsing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "x5c": ["not-base64!"]})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    // RFC 7517 Section 4.8 — "x5t" (X.509 Certificate SHA-1 Thumbprint) Parameter
    // Value is base64url-encoded and decoded during parsing.

    Y_UNIT_TEST(X5T) {
        // "12345678901234567890" -> base64url "MTIzNDU2Nzg5MDEyMzQ1Njc4OTA"
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "x5t": "MTIzNDU2Nzg5MDEyMzQ1Njc4OTA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509CertificateSha1ThumbprintBytes, "12345678901234567890");
    }

    Y_UNIT_TEST(X5TMissing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->X509CertificateSha1ThumbprintBytes.empty());
    }

    Y_UNIT_TEST(X5TInvalidBase64FailsParsing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "x5t": "not-base64!"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(X5TWrongLengthFailsParsing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "x5t": "d3Jvbmc"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    // RFC 7517 Section 4.9 — "x5t#S256" (X.509 Certificate SHA-256 Thumbprint) Parameter
    // Value is base64url-encoded and decoded during parsing.

    Y_UNIT_TEST(X5TS256) {
        // "12345678901234567890123456789012" -> base64url "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI"
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "x5t#S256": "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509CertificateSha256ThumbprintBytes, "12345678901234567890123456789012");
    }

    Y_UNIT_TEST(X5TS256Missing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA"})"));
        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(jwk->X509CertificateSha256ThumbprintBytes.empty());
    }

    Y_UNIT_TEST(X5TS256InvalidBase64FailsParsing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "x5t#S256": "not-base64!"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    Y_UNIT_TEST(X5TS256WrongLengthFailsParsing) {
        const auto jwk = ParseJWK(ParseJson(R"({"kty": "RSA", "x5t#S256": "d3Jvbmc"})"));
        UNIT_ASSERT(!jwk.has_value());
    }

    // Full JWK with all parameters

    Y_UNIT_TEST(AllParameters) {
        // x5c: "cert-data" -> base64 "Y2VydC1kYXRh"
        // x5t: "12345678901234567890" -> base64url "MTIzNDU2Nzg5MDEyMzQ1Njc4OTA"
        // x5t#S256: "12345678901234567890123456789012" -> base64url "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI"
        const auto jwk = ParseJWK(ParseJson(R"({
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
        UNIT_ASSERT_EQUAL(jwk->Type, TKeyType::RSA);
        UNIT_ASSERT_EQUAL(jwk->Usage.value(), TUsage::SIG);
        UNIT_ASSERT_VALUES_EQUAL(jwk->KeyOperations.size(), 2);
        UNIT_ASSERT_EQUAL(jwk->Algorithm.value(), TAlg::RS256);
        UNIT_ASSERT_VALUES_EQUAL(jwk->KeyId, "my-key-id");
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Url, "https://example.com/cert");
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Chain.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509Chain[0], "cert-data");
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509CertificateSha1ThumbprintBytes, "12345678901234567890");
        UNIT_ASSERT_VALUES_EQUAL(jwk->X509CertificateSha256ThumbprintBytes, "12345678901234567890123456789012");
    }
}

Y_UNIT_TEST_SUITE(TParseJWKSetTest) {

    // RFC 7517 Section 5 — JWK Set Format
    // A JWK Set is a JSON object that contains a "keys" member whose value is an array of JWK objects.

    Y_UNIT_TEST(SingleKey) {
        const auto jwkSet = ParseJWKSet(ParseJson(R"({
            "keys": [{"kty": "RSA", "kid": "key1"}]
        })"));
        UNIT_ASSERT(jwkSet.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys.size(), 1);
        UNIT_ASSERT_EQUAL(jwkSet->Keys[0].Type, TKeyType::RSA);
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys[0].KeyId, "key1");
    }

    Y_UNIT_TEST(MultipleKeys) {
        const auto jwkSet = ParseJWKSet(ParseJson(R"({
            "keys": [
                {"kty": "RSA", "kid": "rsa-key"},
                {"kty": "EC", "kid": "ec-key"}
            ]
        })"));
        UNIT_ASSERT(jwkSet.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys.size(), 2);
        UNIT_ASSERT_EQUAL(jwkSet->Keys[0].Type, TKeyType::RSA);
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys[0].KeyId, "rsa-key");
        UNIT_ASSERT_EQUAL(jwkSet->Keys[1].Type, TKeyType::EC);
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys[1].KeyId, "ec-key");
    }

    Y_UNIT_TEST(EmptyKeysArray) {
        const auto jwkSet = ParseJWKSet(ParseJson(R"({"keys": []})"));
        UNIT_ASSERT(jwkSet.has_value());
        UNIT_ASSERT(jwkSet->Keys.empty());
    }

    Y_UNIT_TEST(KeysMissing) {
        const auto jwkSet = ParseJWKSet(ParseJson(R"({})"));
        UNIT_ASSERT(!jwkSet.has_value());
    }

    Y_UNIT_TEST(KeysNotArray) {
        const auto jwkSet = ParseJWKSet(ParseJson(R"({"keys": "not-an-array"})"));
        UNIT_ASSERT(!jwkSet.has_value());
    }

    Y_UNIT_TEST(InvalidKeyIgnored) {
        const auto jwkSet = ParseJWKSet(ParseJson(R"({
            "keys": [
                {"kty": "RSA", "kid": "valid"},
                {"kty": "unknown"},
                {"kty": "EC", "kid": "also-valid"}
            ]
        })"));
        UNIT_ASSERT(jwkSet.has_value());
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys.size(), 2);
        UNIT_ASSERT_EQUAL(jwkSet->Keys[0].Type, TKeyType::RSA);
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys[0].KeyId, "valid");
        UNIT_ASSERT_EQUAL(jwkSet->Keys[1].Type, TKeyType::EC);
        UNIT_ASSERT_VALUES_EQUAL(jwkSet->Keys[1].KeyId, "also-valid");
    }

}

Y_UNIT_TEST_SUITE(TPublicKeysTest) {

    Y_UNIT_TEST(CalculateCorrectPublicKeys) {
        const auto jwkSet = ParseJWKSet(ParseJson(R"({
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
        const auto firstPublicKey = jwkSet->Keys[0].CalculatePublicKey();
        UNIT_ASSERT(firstPublicKey.has_value());
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
        const auto secondPublicKey = jwkSet->Keys[1].CalculatePublicKey();
        UNIT_ASSERT(secondPublicKey.has_value());
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
        const auto jwk = ParseJWK(ParseJson(R"({
            "kty": "RSA",
            "alg": "RS256",
            "x5c": [
                "MIICozCCAYsCBgGeOZ43AjANBgkqhkiG9w0BAQsFADAVMRMwEQYDVQQDDApwcm9kdWN0aW9uMB4XDTI2MDUxODA1NDM1MFoXDTM2MDUxODA1NDUzMFowFTETMBEGA1UEAwwKcHJvZHVjdGlvbjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAPGH/axeTkeHg0sUq/M6ut+YoKG2V77o8F+Nq0PWQO+EQLzm8v/hGVJhizULQYHfVBhPIyzejLYDvcUtNWXwa5Mos2vVcA5SrtZWsikjKOJOhpNo0l3qYvq6xGltyLX+yB4slIT6SYSm1/rOzW2XjYP0GI8eJYGw+kVxZvB3I15Q29EaShULNCUnDltaOEPVI6gV8h7i0Okjhosc5G/rij2z29xwqpFYs+DnzWMiJHvdLValnuWy/8fDNraaBIopxE3sDOMTMkqBzM/wxPbDpRygIdv1FWfvBnMrnYKPumcYRTsRxBQBlcbSCEgvvkjJ3DTTxoVIAOOgbzsFNjZm3usCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAoeb91xEPgnBtSmOuVKUCGER0hXJlT1iRCMyxzb2uA/IoT//GGGOCUhlt2wKkOHR8NZS6QFzBO0/tB1t9/AXORjJyme8H1Xg6+1EwpzwBO2+Om6iJsNnga0eLL0xh9UvIciQzwVF6rSS6wSqxvVaozjMHD7b0CfI7Kezdz3sJKT1TmYA9xVcdVyTyxRU4JjGrLtvKGqjgywXlzCKKkPpcoUtMSKVzNIgN92U/v/47Y+cFAGVZ7k4mIuGbCRe4gxgK39tYuyAoAKNxDzp5qAQ3Pbs41pojRNEtetsOuR57sN/7GIlLkjhoOFzgpZ/ZZ6bdtYsqbPEKzS/sVz6JEhFsvA=="
            ]
        })"));

        UNIT_ASSERT(jwk.has_value());
        const auto publicKey = jwk->CalculatePublicKey();
        UNIT_ASSERT(publicKey.has_value());
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

    Y_UNIT_TEST(CalculatePublicKeyWithoutX5CReturnsNullopt) {
        const auto jwk = ParseJWK(ParseJson(R"({
            "kty": "RSA",
            "alg": "RS256",
            "n": "sXchDaQpG81NwH8YFkBM2fScS9bCqV8e3X5R2Ua3h2Y",
            "e": "AQAB"
        })"));

        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(!jwk->CalculatePublicKey().has_value());
    }

    Y_UNIT_TEST(CalculatePublicKeyWithInvalidX5CReturnsNullopt) {
        const auto jwk = ParseJWK(ParseJson(R"({
            "kty": "RSA",
            "alg": "RS256",
            "x5c": ["Y2VydC1kYXRh"]
        })"));

        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(!jwk->CalculatePublicKey().has_value());
    }

    Y_UNIT_TEST(CalculatePublicKeyWithWrongSha1ThumbprintReturnsNullopt) {
        const auto jwk = ParseJWK(ParseJson(R"({
            "kty": "RSA",
            "alg": "RS256",
            "x5c": [
                "MIICozCCAYsCBgGeOZ43AjANBgkqhkiG9w0BAQsFADAVMRMwEQYDVQQDDApwcm9kdWN0aW9uMB4XDTI2MDUxODA1NDM1MFoXDTM2MDUxODA1NDUzMFowFTETMBEGA1UEAwwKcHJvZHVjdGlvbjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAPGH/axeTkeHg0sUq/M6ut+YoKG2V77o8F+Nq0PWQO+EQLzm8v/hGVJhizULQYHfVBhPIyzejLYDvcUtNWXwa5Mos2vVcA5SrtZWsikjKOJOhpNo0l3qYvq6xGltyLX+yB4slIT6SYSm1/rOzW2XjYP0GI8eJYGw+kVxZvB3I15Q29EaShULNCUnDltaOEPVI6gV8h7i0Okjhosc5G/rij2z29xwqpFYs+DnzWMiJHvdLValnuWy/8fDNraaBIopxE3sDOMTMkqBzM/wxPbDpRygIdv1FWfvBnMrnYKPumcYRTsRxBQBlcbSCEgvvkjJ3DTTxoVIAOOgbzsFNjZm3usCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAoeb91xEPgnBtSmOuVKUCGER0hXJlT1iRCMyxzb2uA/IoT//GGGOCUhlt2wKkOHR8NZS6QFzBO0/tB1t9/AXORjJyme8H1Xg6+1EwpzwBO2+Om6iJsNnga0eLL0xh9UvIciQzwVF6rSS6wSqxvVaozjMHD7b0CfI7Kezdz3sJKT1TmYA9xVcdVyTyxRU4JjGrLtvKGqjgywXlzCKKkPpcoUtMSKVzNIgN92U/v/47Y+cFAGVZ7k4mIuGbCRe4gxgK39tYuyAoAKNxDzp5qAQ3Pbs41pojRNEtetsOuR57sN/7GIlLkjhoOFzgpZ/ZZ6bdtYsqbPEKzS/sVz6JEhFsvA=="
            ],
            "x5t": "MTIzNDU2Nzg5MDEyMzQ1Njc4OTA"
        })"));

        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(!jwk->CalculatePublicKey().has_value());
    }

    Y_UNIT_TEST(CalculatePublicKeyWithWrongSha256ThumbprintReturnsNullopt) {
        const auto jwk = ParseJWK(ParseJson(R"({
            "kty": "RSA",
            "alg": "RS256",
            "x5c": [
                "MIICozCCAYsCBgGeOZ43AjANBgkqhkiG9w0BAQsFADAVMRMwEQYDVQQDDApwcm9kdWN0aW9uMB4XDTI2MDUxODA1NDM1MFoXDTM2MDUxODA1NDUzMFowFTETMBEGA1UEAwwKcHJvZHVjdGlvbjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAPGH/axeTkeHg0sUq/M6ut+YoKG2V77o8F+Nq0PWQO+EQLzm8v/hGVJhizULQYHfVBhPIyzejLYDvcUtNWXwa5Mos2vVcA5SrtZWsikjKOJOhpNo0l3qYvq6xGltyLX+yB4slIT6SYSm1/rOzW2XjYP0GI8eJYGw+kVxZvB3I15Q29EaShULNCUnDltaOEPVI6gV8h7i0Okjhosc5G/rij2z29xwqpFYs+DnzWMiJHvdLValnuWy/8fDNraaBIopxE3sDOMTMkqBzM/wxPbDpRygIdv1FWfvBnMrnYKPumcYRTsRxBQBlcbSCEgvvkjJ3DTTxoVIAOOgbzsFNjZm3usCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAoeb91xEPgnBtSmOuVKUCGER0hXJlT1iRCMyxzb2uA/IoT//GGGOCUhlt2wKkOHR8NZS6QFzBO0/tB1t9/AXORjJyme8H1Xg6+1EwpzwBO2+Om6iJsNnga0eLL0xh9UvIciQzwVF6rSS6wSqxvVaozjMHD7b0CfI7Kezdz3sJKT1TmYA9xVcdVyTyxRU4JjGrLtvKGqjgywXlzCKKkPpcoUtMSKVzNIgN92U/v/47Y+cFAGVZ7k4mIuGbCRe4gxgK39tYuyAoAKNxDzp5qAQ3Pbs41pojRNEtetsOuR57sN/7GIlLkjhoOFzgpZ/ZZ6bdtYsqbPEKzS/sVz6JEhFsvA=="
            ],
            "x5t#S256": "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI"
        })"));

        UNIT_ASSERT(jwk.has_value());
        UNIT_ASSERT(!jwk->CalculatePublicKey().has_value());
    }

}

} // namespace NKikimr::NSecurity
