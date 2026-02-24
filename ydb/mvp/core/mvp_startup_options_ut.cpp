#include <ydb/mvp/core/mvp_startup_options.h>
#include <ydb/mvp/core/mvp_test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NMVP;

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

    Y_UNIT_TEST(TokensOverrideSectionAccepted) {
        TString yaml = TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    tokens:
      oauth2_exchange:
        - name: "nebiusJwt"
          token_endpoint: "grpcs://token.endpoint:443"
          subject_credentials:
            type: "FIXED"
            token: "service-account-id"
)";
        TTempFileHandle tmpYaml = MakeTestFile(yaml, "mvp_startup_tokens_override", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_NO_EXCEPTION(MakeOpts(argv));
    }

    Y_UNIT_TEST(Oauth2TokenExchangeWithTokenFileFromYaml) {
        TTempFileHandle tmpToken = MakeTestFile("MY_FIXED_TOKEN", "mvp_fixed_token", ".token");
        TString yaml = TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    tokens:
      oauth2_exchange:
        - name: "nebiusJwt"
          token_endpoint: "grpcs://token.endpoint:443"
          grant_type: "urn:ietf:params:oauth:grant-type:token-exchange"
          requested_token_type: "urn:ietf:params:oauth:token-type:access_token"
          subject_credentials:
            type: "FIXED"
            token: "service-account-id"
            token_type: "urn:nebius:params:oauth:token-type:subject_identifier"
          actor_credentials:
            type: "FIXED"
            token_file: )" << tmpToken.Name() << R"(
            token_type: "urn:ietf:params:oauth:token-type:jwt"
)";
        TTempFileHandle tmpYaml = MakeTestFile(yaml, "mvp_startup_oauth2_token_file", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        TMvpStartupOptions opts = MakeOpts(argv);

        UNIT_ASSERT(opts.Tokens.OAuth2ExchangeSize() > 0);
        const auto& tokenExchange = opts.Tokens.GetOAuth2Exchange(0);
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetTokenEndpoint(), "grpcs://token.endpoint:443");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetGrantType(), "urn:ietf:params:oauth:grant-type:token-exchange");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetRequestedTokenType(), "urn:ietf:params:oauth:token-type:access_token");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetToken(), "service-account-id");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetActorCredentials().GetTokenFile(), tmpToken.Name());
    }

    Y_UNIT_TEST(Oauth2TokenExchangeOverridesTokenFileByName) {
        TTempFileHandle tmpToken = MakeTestFile(R"pb(
AccessServiceType: nebius_v1
OAuth2Exchange {
  Name: "nebiusJwt"
  TokenEndpoint: "grpcs://token.from.file:443"
  GrantType: "urn:ietf:params:oauth:grant-type:token-exchange"
  RequestedTokenType: "urn:ietf:params:oauth:token-type:access_token"
  SubjectCredentials {
    Type: FIXED
    TokenType: "urn:nebius:params:oauth:token-type:subject_identifier"
    Token: "sa-from-file"
  }
  ActorCredentials {
    Type: FIXED
    TokenType: "urn:ietf:params:oauth:token-type:jwt"
    TokenFile: "/token/from/file"
  }
}
)pb", "mvp_oauth2_token_file", ".pb.txt");

        TString yaml = TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    token_file: )" << tmpToken.Name() << R"(
    tokens:
      oauth2_exchange:
        - name: "nebiusJwt"
          requested_token_type: "custom-requested-type"
          subject_credentials:
            token: "sa-from-yaml"
)";
        TTempFileHandle tmpYaml = MakeTestFile(yaml, "mvp_startup_oauth2_override", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        TMvpStartupOptions opts = MakeOpts(argv);

        UNIT_ASSERT(opts.Tokens.OAuth2ExchangeSize() > 0);
        const auto& tokenExchange = opts.Tokens.GetOAuth2Exchange(0);
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetName(), "nebiusJwt");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetTokenEndpoint(), "grpcs://token.from.file:443");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetRequestedTokenType(), "custom-requested-type");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetGrantType(), "urn:ietf:params:oauth:grant-type:token-exchange");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetToken(), "sa-from-yaml");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetActorCredentials().GetTokenFile(), "/token/from/file");
    }
}
