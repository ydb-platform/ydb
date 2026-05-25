#include "cert_auth_utils.h"

#include <ydb/core/security/certificate_check/test_utils/test_data.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/hex.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TCertificateAuthUtilsTest) {

    Y_UNIT_TEST(GetCertificateFingerprint_PEM_ValidCert) {
        std::string fingerprint = GetCertificateFingerprint(
            std::string(NCertTestUtils::TEST_CERT_PEM.begin(), NCertTestUtils::TEST_CERT_PEM.end()));
        UNIT_ASSERT_VALUES_EQUAL(fingerprint, NCertTestUtils::EXPECTED_FINGERPRINT);
    }

    Y_UNIT_TEST(GetCertificateFingerprint_DER_ValidCert) {
        TString derBinary = HexDecode(NCertTestUtils::TEST_CERT_DER_HEX);
        std::string fingerprint = GetCertificateFingerprint(derBinary, ECertificateFormat::DER);
        UNIT_ASSERT_VALUES_EQUAL(fingerprint, NCertTestUtils::EXPECTED_FINGERPRINT);
    }

    Y_UNIT_TEST(GetCertificateFingerprint_PEM_EmptyInput) {
        std::string fingerprint = GetCertificateFingerprint("");
        UNIT_ASSERT_VALUES_EQUAL(fingerprint, "certificate");
    }

    Y_UNIT_TEST(GetCertificateFingerprint_DER_EmptyInput) {
        std::string fingerprint = GetCertificateFingerprint("", ECertificateFormat::DER);
        UNIT_ASSERT_VALUES_EQUAL(fingerprint, "certificate");
    }

    Y_UNIT_TEST(GetCertificateFingerprint_PEM_InvalidData) {
        std::string fingerprint = GetCertificateFingerprint("not a certificate");
        UNIT_ASSERT_VALUES_EQUAL(fingerprint, "certificate");
    }

    Y_UNIT_TEST(GetCertificateFingerprint_DER_InvalidData) {
        std::string fingerprint = GetCertificateFingerprint("not a certificate", ECertificateFormat::DER);
        UNIT_ASSERT_VALUES_EQUAL(fingerprint, "certificate");
    }

    Y_UNIT_TEST(GetCertificateFingerprint_PEMAndDERProduceSameResult) {
        std::string pemFingerprint = GetCertificateFingerprint(
            std::string(NCertTestUtils::TEST_CERT_PEM.begin(), NCertTestUtils::TEST_CERT_PEM.end()));

        TString derBinary = HexDecode(NCertTestUtils::TEST_CERT_DER_HEX);
        std::string derFingerprint = GetCertificateFingerprint(derBinary, ECertificateFormat::DER);

        UNIT_ASSERT_VALUES_EQUAL(pemFingerprint, derFingerprint);
    }

    Y_UNIT_TEST(GetCertificatePublicKey_PEM_ValidCert) {
        std::string publicKey = GetCertificatePublicKey(
            std::string(NCertTestUtils::TEST_CERT_PEM.begin(), NCertTestUtils::TEST_CERT_PEM.end()));
        UNIT_ASSERT_VALUES_EQUAL(publicKey, NCertTestUtils::EXPECTED_PUBLIC_KEY);
    }

    Y_UNIT_TEST(GetCertificatePublicKey_DER_ValidCert) {
        TString derBinary = HexDecode(NCertTestUtils::TEST_CERT_DER_HEX);
        std::string publicKey = GetCertificatePublicKey(derBinary, ECertificateFormat::DER);
        UNIT_ASSERT_VALUES_EQUAL(publicKey, NCertTestUtils::EXPECTED_PUBLIC_KEY);
    }

    Y_UNIT_TEST(GetCertificatePublicKey_PEM_EmptyInput) {
        std::string publicKey = GetCertificatePublicKey("");
        UNIT_ASSERT_C(publicKey.empty(), "GetCertificatePublicKey should return empty string for empty input");
    }

    Y_UNIT_TEST(GetCertificatePublicKey_DER_EmptyInput) {
        std::string publicKey = GetCertificatePublicKey("", ECertificateFormat::DER);
        UNIT_ASSERT_C(publicKey.empty(), "GetCertificatePublicKey should return empty string for empty input");
    }

    Y_UNIT_TEST(GetCertificatePublicKey_PEM_InvalidData) {
        std::string publicKey = GetCertificatePublicKey("not a certificate");
        UNIT_ASSERT_C(publicKey.empty(), "GetCertificatePublicKey should return empty string for invalid PEM data");
    }

    Y_UNIT_TEST(GetCertificatePublicKey_DER_InvalidData) {
        std::string publicKey = GetCertificatePublicKey("not a certificate", ECertificateFormat::DER);
        UNIT_ASSERT_C(publicKey.empty(), "GetCertificatePublicKey should return empty string for invalid DER data");
    }

    Y_UNIT_TEST(GetCertificatePublicKey_PEMAndDERProduceSameResult) {
        std::string pemPublicKey = GetCertificatePublicKey(
            std::string(NCertTestUtils::TEST_CERT_PEM.begin(), NCertTestUtils::TEST_CERT_PEM.end()));

        TString derBinary = HexDecode(NCertTestUtils::TEST_CERT_DER_HEX);
        std::string derPublicKey = GetCertificatePublicKey(derBinary, ECertificateFormat::DER);

        UNIT_ASSERT_VALUES_EQUAL(pemPublicKey, derPublicKey);
    }

}

} // namespace NKikimr
