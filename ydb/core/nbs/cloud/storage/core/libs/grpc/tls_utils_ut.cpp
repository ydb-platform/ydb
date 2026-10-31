#include "tls_utils.h"

#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NYdb::NBS::NTlsUtils {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ReadCertResource(TStringBuf relativePath)
{
    return NResource::Find(
        TStringBuilder() << "grpc/ut/certs/" << relativePath);
}

void WriteTextFile(const TString& path, const TString& content)
{
    TFileOutput out(path);
    out.Write(content.data(), content.size());
}

TCertificateFiles CreateCertificatePair(
    const TString& dirPath,
    const TString& prefix,
    const TString& privateKeyContent,
    const TString& certChainContent)
{
    const TString privateKeyPath = TStringBuilder()
                                   << dirPath << "/" << prefix << ".key";
    const TString certChainPath = TStringBuilder()
                                  << dirPath << "/" << prefix << ".crt";

    WriteTextFile(privateKeyPath, privateKeyContent);
    WriteTextFile(certChainPath, certChainContent);

    return {
        .PrivateKeyPath = privateKeyPath,
        .CertChainPath = certChainPath,
    };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTlsUtilsTest)
{
    Y_UNIT_TEST(ShouldValidatePemCertificate)
    {
        const auto pem = ReadCertResource("server1.crt");
        const auto result = IsValidPemCertificate(pem);
        UNIT_ASSERT(!NYdb::NBS::HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRejectInvalidPemCertificate)
    {
        const auto result = IsValidPemCertificate("not a certificate");
        UNIT_ASSERT(NYdb::NBS::HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldMatchPrivateKeyAndCertificate)
    {
        const auto key = ReadCertResource("server1.key");
        const auto cert = ReadCertResource("server1.crt");
        const auto result = PrivateKeyAndCertificateMatch(key, cert);
        UNIT_ASSERT(!NYdb::NBS::HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldDetectMismatchedPrivateKeyAndCertificate)
    {
        const auto key = ReadCertResource("server1.key");
        const auto cert = ReadCertResource("server2.crt");
        const auto result = PrivateKeyAndCertificateMatch(key, cert);
        UNIT_ASSERT(NYdb::NBS::HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldExtractCertificateNotAfterTimestamp)
    {
        const auto cert = ReadCertResource("server1.crt");
        const auto result = GetCertificateNotAfterTimestampSec(cert);
        UNIT_ASSERT(!NYdb::NBS::HasError(result.GetError()));
        UNIT_ASSERT(result.GetResult() > 0);
    }

    Y_UNIT_TEST(ShouldReadAndValidateRootCertificate)
    {
        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
                                 << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));
        const auto result = ReadAndValidateRootCertificate(rootPath);
        UNIT_ASSERT(!NYdb::NBS::HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldReadAndValidateIdentityPair)
    {
        TTempDir tempDir;
        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "identity",
            ReadCertResource("server1.key"),
            ReadCertResource("server1.crt"));

        const auto result = ReadAndValidateIdentityPair(pair);
        UNIT_ASSERT(!NYdb::NBS::HasError(result.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1, result.GetResult().size());
    }

    Y_UNIT_TEST(ShouldRejectIdentityPairWithMismatchedFiles)
    {
        TTempDir tempDir;
        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "identity",
            ReadCertResource("server1.key"),
            ReadCertResource("server2.crt"));

        const auto result = ReadAndValidateIdentityPair(pair);
        UNIT_ASSERT(NYdb::NBS::HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRejectEmptyPemCertificate)
    {
        const auto result = IsValidPemCertificate("");
        UNIT_ASSERT(NYdb::NBS::HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRejectPemWithoutCertificate)
    {
        const auto key = ReadCertResource("server1.key");
        const auto result = IsValidPemCertificate(key);
        UNIT_ASSERT(NYdb::NBS::HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldFailExtractingNotAfterFromInvalidCertificate)
    {
        const auto result =
            GetCertificateNotAfterTimestampSec("not a certificate");
        UNIT_ASSERT(NYdb::NBS::HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldFailReadingMissingFile)
    {
        const auto result = TryReadFile("/nonexistent/certificate.pem");
        UNIT_ASSERT(NYdb::NBS::HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldFailValidatingMissingRootCertificate)
    {
        const auto result =
            ReadAndValidateRootCertificate("/nonexistent/ca.crt");
        UNIT_ASSERT(NYdb::NBS::HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRejectIdentityPairWithMissingFiles)
    {
        const TCertificateFiles files{
            .PrivateKeyPath = "/nonexistent/identity.key",
            .CertChainPath = "/nonexistent/identity.crt",
        };
        const auto result = ReadAndValidateIdentityPair(files);
        UNIT_ASSERT(NYdb::NBS::HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldUpdateAllCertificatesWhenValid)
    {
        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
                                 << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));

        const auto files = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            ReadCertResource("server1.crt"));

        TVector<TCertificatePair> certs{TCertificatePair{.Files = files}};
        TRootCaPair root{
            .RootCaPath = rootPath,
            .RootCa = ReadCertResource("ca.crt"),
        };
        TLog log;

        const auto result = UpdateCertificates(certs, root, log);

        UNIT_ASSERT(result.RootCa.Defined());
        UNIT_ASSERT_VALUES_EQUAL(ReadCertResource("ca.crt"), *result.RootCa);
        UNIT_ASSERT_VALUES_EQUAL(1, result.Certificates.size());
        UNIT_ASSERT(result.Certificates[0].Defined());
        UNIT_ASSERT(result.Certificates[0]->NotValidAfter != TInstant::Zero());
    }

    Y_UNIT_TEST(ShouldKeepPreviousRootWhenRootBecomesInvalid)
    {
        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
                                 << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, "not a certificate");

        const auto files = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            ReadCertResource("server1.crt"));

        const TString previousRoot = ReadCertResource("ca.crt");
        TVector<TCertificatePair> certs{TCertificatePair{.Files = files}};
        TRootCaPair root{.RootCaPath = rootPath, .RootCa = previousRoot};
        TLog log;

        const auto result = UpdateCertificates(certs, root, log);

        UNIT_ASSERT(result.RootCa.Defined());
        UNIT_ASSERT_VALUES_EQUAL(previousRoot, *result.RootCa);
    }

    Y_UNIT_TEST(ShouldFallBackToPreviousIdentityWhenFileInvalid)
    {
        TTempDir tempDir;
        const auto files = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            "broken certificate");

        TVector<TCertificatePair> certs{TCertificatePair{
            .Files = files,
            .PrivateKey = ReadCertResource("server1.key"),
            .CertChain = ReadCertResource("server1.crt"),
        }};
        TLog log;

        const auto result = UpdateCertificates(certs, TRootCaPair{}, log);

        UNIT_ASSERT(result.Certificates[0].Defined());
        UNIT_ASSERT_VALUES_EQUAL(
            ReadCertResource("server1.crt"),
            TString(result.Certificates[0]
                        ->CertificatesChain.front()
                        .cert_chain()));
    }

    Y_UNIT_TEST(ShouldLeaveIdentityUndefinedWhenInvalidAndNoPrevious)
    {
        TTempDir tempDir;
        const auto files = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            "broken certificate");

        TVector<TCertificatePair> certs{TCertificatePair{.Files = files}};
        TLog log;

        const auto result = UpdateCertificates(certs, TRootCaPair{}, log);

        UNIT_ASSERT_VALUES_EQUAL(1, result.Certificates.size());
        UNIT_ASSERT(!result.Certificates[0].Defined());
    }

    Y_UNIT_TEST(ShouldLoadCertificatePairsAndSkipEmpty)
    {
        TTempDir tempDir;
        const auto files = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            ReadCertResource("server1.crt"));

        TVector<TCertificateFiles> input{{}, files, {}};
        const auto pairs = LoadCertificatePairs(std::move(input));

        UNIT_ASSERT_VALUES_EQUAL(1, pairs.size());
        UNIT_ASSERT_VALUES_EQUAL(
            files.PrivateKeyPath,
            pairs[0].Files.PrivateKeyPath);
        UNIT_ASSERT_VALUES_EQUAL(
            files.CertChainPath,
            pairs[0].Files.CertChainPath);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadCertResource("server1.key"),
            pairs[0].PrivateKey);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadCertResource("server1.crt"),
            pairs[0].CertChain);
    }

    Y_UNIT_TEST(ShouldThrowOnIncompletePairs)
    {
        UNIT_ASSERT_EXCEPTION(
            LoadCertificatePairs({TCertificateFiles{.PrivateKeyPath = "/k"}}),
            yexception);
        UNIT_ASSERT_EXCEPTION(
            LoadCertificatePairs({TCertificateFiles{.CertChainPath = "/c"}}),
            yexception);
    }

    Y_UNIT_TEST(ShouldThrowOnUnreadablePairs)
    {
        UNIT_ASSERT_EXCEPTION(
            LoadCertificatePairs({TCertificateFiles{
                .PrivateKeyPath = "/nonexistent/k",
                .CertChainPath = "/nonexistent/c",
            }}),
            yexception);
    }

    Y_UNIT_TEST(ShouldLoadRootCaPair)
    {
        UNIT_ASSERT(!LoadRootCaPair({}).RootCaPath);

        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
                                 << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));

        const auto pair = LoadRootCaPair(rootPath);
        UNIT_ASSERT_VALUES_EQUAL(rootPath, pair.RootCaPath);
        UNIT_ASSERT_VALUES_EQUAL(ReadCertResource("ca.crt"), pair.RootCa);
    }

    Y_UNIT_TEST(ShouldThrowOnUnreadableRootCaPair)
    {
        UNIT_ASSERT_EXCEPTION(
            LoadRootCaPair("/nonexistent/ca.crt"),
            yexception);
    }
}

}   // namespace NYdb::NBS::NTlsUtils
