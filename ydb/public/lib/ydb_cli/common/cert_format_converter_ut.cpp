#include "cert_format_converter.h"

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>

namespace NYdb::NConsoleClient {

namespace {

TString GetDataPath(const TString& path) {
    return ArcadiaFromCurrentLocation(__SOURCE_FILE__, "ut/test_data/" + path);
}

TString GetDataByPath(const TString& path) {
    if (path) {
        return TFileInput(GetDataPath(path)).ReadAll();
    }
    return {};
}

const TString PASSWORD = "p@ssw0rd";

} // anonymous

Y_UNIT_TEST_SUITE(CertFormatConverter) {
    void ValidateParse(const TString& certPath, const TString& keyPath, bool withPassword) {
        TString certContent = GetDataByPath(certPath);
        TString keyContent = GetDataByPath(keyPath);
        TString cert, key;
        if (withPassword) {
            // Does not parse without password
            // And also does not ask it, because we are in noninteractive mode
            UNIT_ASSERT_EXCEPTION_C(
                std::tie(cert, key) = ConvertCertToPEM(certContent, keyContent, "", false),
                yexception,
                "Cert: " << certPath << ". Key: " << keyPath);

            UNIT_ASSERT_NO_EXCEPTION_C(
                std::tie(cert, key) = ConvertCertToPEM(certContent, keyContent, PASSWORD, false),
                "Cert: " << certPath << ". Key: " << keyPath);
        } else {
            UNIT_ASSERT_NO_EXCEPTION_C(
                std::tie(cert, key) = ConvertCertToPEM(certContent, keyContent, "", false),
                "Cert: " << certPath << ". Key: " << keyPath);
        }

        UNIT_ASSERT_STRING_CONTAINS_C(cert, "-----BEGIN CERTIFICATE-----", "Cert: " << certPath << ". Key: " << keyPath);
        UNIT_ASSERT_STRING_CONTAINS_C(key, "-----BEGIN PRIVATE KEY-----", "Cert: " << certPath << ". Key: " << keyPath);

        // Check that returned cert/key can be parsed
        UNIT_ASSERT_NO_EXCEPTION_C(
            ConvertCertToPEM(cert, key, "", false),
            "Cert: " << certPath << ". Key: " << keyPath);
    }

    Y_UNIT_TEST(ParseFromPEM) {
        ValidateParse("ec-cert.pem", "ec-private-key.pem", false);
        ValidateParse("ec-cert.p12", "", false);
        ValidateParse("ec-all.pem", "ec-all.pem", false);
        ValidateParse("ec-all.pem", "", false);
        ValidateParse("rsa-cert.pem", "rsa-private-key.pem", false);
        ValidateParse("rsa-cert.p12", "", false);
        ValidateParse("rsa-all.pem", "rsa-all.pem", false);
        ValidateParse("rsa-all.pem", "", false);

        ValidateParse("ec-cert.pem", "ec-private-key-pwd.pem", true);
        ValidateParse("ec-cert-pwd.p12", "", true);
        ValidateParse("ec-all-pwd.pem", "ec-all-pwd.pem", true);
        ValidateParse("ec-all-pwd.pem", "", true);
        ValidateParse("rsa-cert.pem", "rsa-private-key-pwd.pem", true);
        ValidateParse("rsa-cert-pwd.p12", "", true);
        ValidateParse("rsa-all-pwd.pem", "rsa-all-pwd.pem", true);
        ValidateParse("rsa-all-pwd.pem", "", true);
    }

    Y_UNIT_TEST(InvalidKey) {
        TString certContent = GetDataByPath("ec-cert.pem");
        TString keyContent = "invalid";
        UNIT_ASSERT_EXCEPTION(
            ConvertCertToPEM(certContent, keyContent, "", false),
            yexception);
    }

    Y_UNIT_TEST(InvalidCert) {
        TString certContent = "invalid";
        TString keyContent = GetDataByPath("rsa-private-key.pem");
        UNIT_ASSERT_EXCEPTION(
            ConvertCertToPEM(certContent, keyContent, "", false),
            yexception);
    }

    Y_UNIT_TEST(InvalidKeyTakeFromCert) {
        TString certContent = GetDataByPath("ec-all.pem");
        TString keyContent = "invalid";

        UNIT_ASSERT_NO_EXCEPTION(ConvertCertToPEM(certContent, keyContent, "", false));

        certContent = GetDataByPath("ec-cert-pwd.p12");
        UNIT_ASSERT_NO_EXCEPTION(ConvertCertToPEM(certContent, keyContent, PASSWORD, false));
    }
}

} // namespace NYdb::NConsoleClient
