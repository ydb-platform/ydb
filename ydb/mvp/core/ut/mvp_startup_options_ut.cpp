#include <ydb/mvp/core/mvp_startup_options.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>
#include <util/stream/file.h>

using namespace NMVP;

namespace {
    template <size_t N>
    TMvpStartupOptions MakeOpts(const char* (&argv)[N]) {
        return TMvpStartupOptions::Build(N, argv);
    }

    template <size_t N>
    TMvpStartupOptions MakeOpts(const char* (&&argv)[N]) {
        return TMvpStartupOptions::Build(N, argv);
    }

    TTempFileHandle MakeTestFile(const TStringBuf content, const TString& name = "test", const TString& extension = "") {
        TTempFileHandle tmpFile = TTempFileHandle::InCurrentDir(name, extension);
        TUnbufferedFileOutput ofs(tmpFile.Name());
        ofs.Write(content);
        ofs.Finish();
        return tmpFile;
    }
}

Y_UNIT_TEST_SUITE(TMvpStartupOptions) {
    Y_UNIT_TEST(DefaultHttpPortWhenNoPorts) {
        TMvpStartupOptions opts = MakeOpts({"mvp_test"});
        UNIT_ASSERT_VALUES_EQUAL(opts.HttpPort, 8788);
        UNIT_ASSERT_VALUES_EQUAL(opts.HttpsPort, 0);
    }

    Y_UNIT_TEST(HttpsRequiresCert) {
        const char* argv[] = {"mvp_test", "--https-port", "8443"};
        UNIT_ASSERT_EXCEPTION_CONTAINS(MakeOpts(argv), yexception, "SSL certificate file must be provided for HTTPS");
    }

    Y_UNIT_TEST(CliOverridesYaml) {
        TTempFileHandle tmpFile = MakeTestFile(R"(
generic:
  server:
    http_port: 1234
    https_port: 0
)" , "mvp_startup_options_test", ".yaml");

        const char* argvNoCli[] = {"mvp_test", "--config", tmpFile.Name().c_str()};
        TMvpStartupOptions optsFromYaml = MakeOpts(argvNoCli);
        UNIT_ASSERT_VALUES_EQUAL(optsFromYaml.HttpPort, 1234);

        const char* argvWithCli[] = {"mvp_test", "--config", tmpFile.Name().c_str(), "--http-port", "4321"};
        TMvpStartupOptions optsWithCli = MakeOpts(argvWithCli);
        UNIT_ASSERT_VALUES_EQUAL(optsWithCli.HttpPort, 4321);
    }

    Y_UNIT_TEST(YamlHttpsWithoutCertThrows) {
        TTempFileHandle tmpFile = MakeTestFile(R"(
generic:
  server:
    https_port: 8443
)", "mvp_startup_options_test_no_cert", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpFile.Name().c_str()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(MakeOpts(argv), yexception, "SSL certificate file must be provided for HTTPS");
    }

    Y_UNIT_TEST(CliHttpsPortWithYamlCertSucceeds) {
        TTempFileHandle tmpCert = MakeTestFile("dummy-cert", "mvp_test_cert", ".pem");
        TString yamlWithCert = TStringBuilder() << R"(
generic:
  server:
    ssl_cert_file: )" << tmpCert.Name() << "\n";
        TTempFileHandle tmpYaml = MakeTestFile(yamlWithCert, "mvp_startup_options_test_with_cert", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str(), "--https-port", "8443"};
        TMvpStartupOptions opts = MakeOpts(argv);
        UNIT_ASSERT_VALUES_EQUAL(opts.HttpsPort, 8443);
        UNIT_ASSERT(!opts.SslCertificate.empty());
    }

    Y_UNIT_TEST(SslCertWithoutPortsDefaultsHttps) {
        TTempFileHandle tmpCert = MakeTestFile("dummy-cert", "mvp_test_cert2", ".pem");
        TString yamlWithCert2 = TStringBuilder() << R"(
generic:
  server:
    ssl_cert_file: )" << tmpCert.Name() << "\n";
        TTempFileHandle tmpYaml = MakeTestFile(yamlWithCert2, "mvp_startup_options_test_cert_noports", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        TMvpStartupOptions opts = MakeOpts(argv);
        UNIT_ASSERT_VALUES_EQUAL(opts.HttpPort, 0);
        UNIT_ASSERT_VALUES_EQUAL(opts.HttpsPort, 8789);
        UNIT_ASSERT(!opts.SslCertificate.empty());
    }
}
