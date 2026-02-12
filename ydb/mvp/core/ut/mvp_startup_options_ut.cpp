#include <ydb/mvp/core/mvp_startup_options.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>
#include <util/stream/file.h>

#include <fstream>
#include <cstdio>

using namespace NMVP;

Y_UNIT_TEST_SUITE(TMvpStartupOptions) {
    Y_UNIT_TEST(DefaultHttpPortWhenNoPorts) {
        const char* argv[] = {"mvp_test"};
        TMvpStartupOptions opts = TMvpStartupOptions::Build(1, argv);
        UNIT_ASSERT_VALUES_EQUAL(opts.HttpPort, 8788);
        UNIT_ASSERT_VALUES_EQUAL(opts.HttpsPort, 0);
    }

    Y_UNIT_TEST(HttpsRequiresCert) {
        const char* argv[] = {"mvp_test", "--https-port", "8443"};
        UNIT_ASSERT_EXCEPTION_CONTAINS(TMvpStartupOptions::Build(3, argv), yexception, "SSL certificate file must be provided for HTTPS");
    }

    Y_UNIT_TEST(CliOverridesYaml) {
        TTempFileHandle tmpFile = TTempFileHandle::InCurrentDir("mvp_startup_options_test", ".yaml");
        {
            TUnbufferedFileOutput ofs(tmpFile.Name());
            ofs.Write("generic:\n");
            ofs.Write("  server:\n");
            ofs.Write("    http_port: 1234\n");
            ofs.Write("    https_port: 0\n");
            ofs.Finish();
        }

        const char* argvNoCli[] = {"mvp_test", "--config", tmpFile.Name().data()};
        TMvpStartupOptions optsFromYaml = TMvpStartupOptions::Build(3, argvNoCli);
        UNIT_ASSERT_VALUES_EQUAL(optsFromYaml.HttpPort, 1234);

        const char* argvWithCli[] = {"mvp_test", "--config", tmpFile.Name().data(), "--http-port", "4321"};
        TMvpStartupOptions optsWithCli = TMvpStartupOptions::Build(5, argvWithCli);
        UNIT_ASSERT_VALUES_EQUAL(optsWithCli.HttpPort, 4321);
    }

    Y_UNIT_TEST(YamlHttpsWithoutCertThrows) {
        TTempFileHandle tmpFile = TTempFileHandle::InCurrentDir("mvp_startup_options_test_no_cert", ".yaml");
        {
            TUnbufferedFileOutput ofs(tmpFile.Name());
            ofs.Write("generic:\n");
            ofs.Write("  server:\n");
            ofs.Write("    https_port: 8443\n");
            ofs.Finish();
        }

        const char* argv[] = {"mvp_test", "--config", tmpFile.Name().data()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(TMvpStartupOptions::Build(3, argv), yexception, "SSL certificate file must be provided for HTTPS");
    }

    Y_UNIT_TEST(CliHttpsPortWithYamlCertSucceeds) {
        TTempFileHandle tmpYaml = TTempFileHandle::InCurrentDir("mvp_startup_options_test_with_cert", ".yaml");
        TTempFileHandle tmpCert = TTempFileHandle::InCurrentDir("mvp_test_cert", ".pem");
        {
            TUnbufferedFileOutput co(tmpCert.Name());
            co.Write("dummy-cert");
            co.Finish();

            TUnbufferedFileOutput oy(tmpYaml.Name());
            oy.Write("generic:\n");
            oy.Write("  server:\n");
            oy.Write("    ssl_cert_file: \"");
            oy.Write(tmpCert.Name());
            oy.Write("\"\n");
            oy.Finish();
        }

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().data(), "--https-port", "8443"};
        TMvpStartupOptions opts = TMvpStartupOptions::Build(5, argv);
        UNIT_ASSERT_VALUES_EQUAL(opts.HttpsPort, 8443);
        UNIT_ASSERT(!opts.SslCertificate.empty());
    }

    Y_UNIT_TEST(SslCertWithoutPortsDefaultsHttp) {
        TTempFileHandle tmpYaml = TTempFileHandle::InCurrentDir("mvp_startup_options_test_cert_noports", ".yaml");
        TTempFileHandle tmpCert = TTempFileHandle::InCurrentDir("mvp_test_cert2", ".pem");
        {
            TUnbufferedFileOutput co(tmpCert.Name());
            co.Write("dummy-cert");
            co.Finish();

            TUnbufferedFileOutput oy(tmpYaml.Name());
            oy.Write("generic:\n");
            oy.Write("  server:\n");
            oy.Write("    ssl_cert_file: \"");
            oy.Write(tmpCert.Name());
            oy.Write("\"\n");
            oy.Finish();
        }

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().data()};
        TMvpStartupOptions opts = TMvpStartupOptions::Build(3, argv);
        UNIT_ASSERT_VALUES_EQUAL(opts.HttpPort, 0);
        UNIT_ASSERT_VALUES_EQUAL(opts.HttpsPort, 8789);
        UNIT_ASSERT(!opts.SslCertificate.empty());
    }
}
