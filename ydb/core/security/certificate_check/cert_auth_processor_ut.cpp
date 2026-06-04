#include "cert_auth_processor.h"

#include <ydb/core/security/certificate_check/test_utils/test_data.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/hex.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TX509CertificateReaderTest) {

    Y_UNIT_TEST(ReadCertAsPEM_ValidCert) {
        auto x509 = X509CertificateReader::ReadCertAsPEM(NCertTestUtils::TEST_CERT_PEM);
        UNIT_ASSERT_C(x509 != nullptr, "ReadCertAsPEM should successfully parse a valid PEM certificate");

        auto subjectTerms = X509CertificateReader::ReadSubjectTerms(x509);
        bool foundCN = false;
        for (const auto& [key, value] : subjectTerms) {
            if (key == "CN") {
                UNIT_ASSERT_VALUES_EQUAL(value, "www.ssl.com");
                foundCN = true;
            }
        }
        UNIT_ASSERT_C(foundCN, "Subject should contain CN=www.ssl.com");
    }

    Y_UNIT_TEST(ReadCertAsPEM_EmptyInput) {
        auto x509 = X509CertificateReader::ReadCertAsPEM("");
        UNIT_ASSERT_C(x509 == nullptr, "ReadCertAsPEM should return nullptr for empty input");
    }

    Y_UNIT_TEST(ReadCertAsPEM_InvalidData) {
        auto x509 = X509CertificateReader::ReadCertAsPEM("not a certificate");
        UNIT_ASSERT_C(x509 == nullptr, "ReadCertAsPEM should return nullptr for invalid PEM data");
    }

    Y_UNIT_TEST(ReadCertAsPEM_MalformedPEM) {
        TString malformed =
            "-----BEGIN CERTIFICATE-----\n"
            "dGhpcyBpcyBub3QgYSB2YWxpZCBjZXJ0aWZpY2F0ZQ==\n"
            "-----END CERTIFICATE-----\n";
        auto x509 = X509CertificateReader::ReadCertAsPEM(malformed);
        UNIT_ASSERT_C(x509 == nullptr, "ReadCertAsPEM should return nullptr for malformed PEM content");
    }

    Y_UNIT_TEST(ReadCertAsDER_ValidCert) {
        TString derBinary = HexDecode(NCertTestUtils::TEST_CERT_DER_HEX);
        UNIT_ASSERT_C(!derBinary.empty(), "DER hex decode should produce non-empty result");

        auto x509 = X509CertificateReader::ReadCertAsDER(derBinary);
        UNIT_ASSERT_C(x509 != nullptr, "ReadCertAsDER should successfully parse a valid DER certificate");

        auto subjectTerms = X509CertificateReader::ReadSubjectTerms(x509);
        bool foundCN = false;
        for (const auto& [key, value] : subjectTerms) {
            if (key == "CN") {
                UNIT_ASSERT_VALUES_EQUAL(value, "www.ssl.com");
                foundCN = true;
            }
        }
        UNIT_ASSERT_C(foundCN, "Subject should contain CN=www.ssl.com");
    }

    Y_UNIT_TEST(ReadCertAsDER_EmptyInput) {
        auto x509 = X509CertificateReader::ReadCertAsDER("");
        UNIT_ASSERT_C(x509 == nullptr, "ReadCertAsDER should return nullptr for empty input");
    }

    Y_UNIT_TEST(ReadCertAsDER_InvalidData) {
        TString invalidDer = "this is not a DER certificate";
        auto x509 = X509CertificateReader::ReadCertAsDER(invalidDer);
        UNIT_ASSERT_C(x509 == nullptr, "ReadCertAsDER should return nullptr for invalid DER data");
    }

    Y_UNIT_TEST(ReadCertAsDER_TruncatedData) {
        TString derBinary = HexDecode(NCertTestUtils::TEST_CERT_DER_HEX);
        TString truncated = derBinary.substr(0, 10);
        auto x509 = X509CertificateReader::ReadCertAsDER(truncated);
        UNIT_ASSERT_C(x509 == nullptr, "ReadCertAsDER should return nullptr for truncated DER data");
    }

    Y_UNIT_TEST(ReadCertAsDER_TrailingGarbage) {
        TString derBinary = HexDecode(NCertTestUtils::TEST_CERT_DER_HEX);
        derBinary += "trailing garbage";
        auto x509 = X509CertificateReader::ReadCertAsDER(derBinary);
        UNIT_ASSERT_C(x509 == nullptr, "ReadCertAsDER should return nullptr for DER data with trailing garbage");
    }

    Y_UNIT_TEST(PEMAndDERProduceSameSubjectTerms) {
        auto pemX509 = X509CertificateReader::ReadCertAsPEM(NCertTestUtils::TEST_CERT_PEM);
        UNIT_ASSERT_C(pemX509 != nullptr, "ReadCertAsPEM should parse the test certificate");

        TString derBinary = HexDecode(NCertTestUtils::TEST_CERT_DER_HEX);
        auto derX509 = X509CertificateReader::ReadCertAsDER(derBinary);
        UNIT_ASSERT_C(derX509 != nullptr, "ReadCertAsDER should parse the test certificate");

        auto pemSubject = X509CertificateReader::ReadSubjectTerms(pemX509);
        auto derSubject = X509CertificateReader::ReadSubjectTerms(derX509);

        UNIT_ASSERT_VALUES_EQUAL(pemSubject.size(), derSubject.size());
        for (size_t i = 0; i < pemSubject.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(pemSubject[i].first, derSubject[i].first);
            UNIT_ASSERT_VALUES_EQUAL(pemSubject[i].second, derSubject[i].second);
        }
    }

    Y_UNIT_TEST(PEMAndDERProduceSameFingerprint) {
        auto pemX509 = X509CertificateReader::ReadCertAsPEM(NCertTestUtils::TEST_CERT_PEM);
        UNIT_ASSERT_C(pemX509 != nullptr, "ReadCertAsPEM should parse the test certificate");

        TString derBinary = HexDecode(NCertTestUtils::TEST_CERT_DER_HEX);
        auto derX509 = X509CertificateReader::ReadCertAsDER(derBinary);
        UNIT_ASSERT_C(derX509 != nullptr, "ReadCertAsDER should parse the test certificate");

        TString pemFingerprint = X509CertificateReader::GetFingerprint(pemX509);
        TString derFingerprint = X509CertificateReader::GetFingerprint(derX509);

        UNIT_ASSERT_C(!pemFingerprint.empty(), "PEM fingerprint should not be empty");
        UNIT_ASSERT_C(!derFingerprint.empty(), "DER fingerprint should not be empty");
        UNIT_ASSERT_VALUES_EQUAL(pemFingerprint, derFingerprint);
    }

    Y_UNIT_TEST(ReadCertAsPEM_SubjectDetails) {
        auto x509 = X509CertificateReader::ReadCertAsPEM(NCertTestUtils::TEST_CERT_PEM);
        UNIT_ASSERT_C(x509 != nullptr, "ReadCertAsPEM should parse the test certificate");

        auto subjectTerms = X509CertificateReader::ReadSubjectTerms(x509);

        // Build a map for easier lookup
        std::unordered_map<TString, TString> subjectMap;
        for (const auto& [key, value] : subjectTerms) {
            subjectMap[key] = value;
        }

        UNIT_ASSERT_VALUES_EQUAL(subjectMap["C"], "US");
        UNIT_ASSERT_VALUES_EQUAL(subjectMap["ST"], "Texas");
        UNIT_ASSERT_VALUES_EQUAL(subjectMap["L"], "Houston");
        UNIT_ASSERT_VALUES_EQUAL(subjectMap["O"], "SSL Corp");
        UNIT_ASSERT_VALUES_EQUAL(subjectMap["CN"], "www.ssl.com");
    }

    Y_UNIT_TEST(ReadCertAsDER_IssuerDetails) {
        TString derBinary = HexDecode(NCertTestUtils::TEST_CERT_DER_HEX);
        auto x509 = X509CertificateReader::ReadCertAsDER(derBinary);
        UNIT_ASSERT_C(x509 != nullptr, "ReadCertAsDER should parse the test certificate");

        auto issuerTerms = X509CertificateReader::ReadIssuerTerms(x509);

        std::unordered_map<TString, TString> issuerMap;
        for (const auto& [key, value] : issuerTerms) {
            issuerMap[key] = value;
        }

        UNIT_ASSERT_VALUES_EQUAL(issuerMap["C"], "US");
        UNIT_ASSERT_VALUES_EQUAL(issuerMap["ST"], "Texas");
        UNIT_ASSERT_VALUES_EQUAL(issuerMap["L"], "Houston");
        UNIT_ASSERT_VALUES_EQUAL(issuerMap["O"], "SSL Corp");
        UNIT_ASSERT_VALUES_EQUAL(issuerMap["CN"], "SSL.com EV SSL Intermediate CA RSA R3");
    }

}

} // namespace NKikimr
