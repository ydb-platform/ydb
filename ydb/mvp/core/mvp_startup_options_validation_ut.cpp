#include <ydb/mvp/core/mvp_startup_options.h>
#include <ydb/mvp/core/mvp_test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NMVP;

namespace {

void AssertYamlThrows(const TString& yaml, TStringBuf errorPart) {
    TTempFileHandle tmpYaml = MakeTestFile(yaml, "mvp_startup_options_validation", ".yaml");
    const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
    UNIT_ASSERT_EXCEPTION_CONTAINS(MakeOpts(argv), yexception, errorPart);
}

void AssertYamlNoThrow(const TString& yaml) {
    TTempFileHandle tmpYaml = MakeTestFile(yaml, "mvp_startup_options_validation", ".yaml");
    const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
    UNIT_ASSERT_NO_EXCEPTION(MakeOpts(argv));
}

void AssertTokenFileConfigThrows(const TString& tokenFileProto,
                                 const TString& tokenFileName,
                                 TStringBuf errorPart)
{
    TTempFileHandle tmpToken = MakeTestFile(tokenFileProto, tokenFileName, ".pb.txt");
    TString yaml = TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    token_file: )" << tmpToken.Name() << "\n";
    AssertYamlThrows(yaml, errorPart);
}

TMvpStartupOptions MakeOptsFromTokenFileConfig(const TString& tokenFileProto,
                                               const TString& tokenFileName)
{
    TTempFileHandle tmpToken = MakeTestFile(tokenFileProto, tokenFileName, ".pb.txt");
    TTempFileHandle tmpYaml = MakeTestFile(TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    token_file: )" << tmpToken.Name() << "\n",
        "mvp_startup_options_validation",
        ".yaml");
    const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
    return MakeOpts(argv);
}

} // namespace

Y_UNIT_TEST_SUITE(TMvpStartupOptionsValidation) {
    Y_UNIT_TEST(MissingEndpointThrows) {
        AssertYamlThrows(TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    tokens:
      oauth2_exchange:
        - name: "nebiusJwt"
          subject_credentials:
            type: "FIXED"
            token: "service-account-id"
)",
            "'token_endpoint' is required in oauth2_exchange token config.");
    }

    Y_UNIT_TEST(MissingSubjectCredentialsThrows) {
        AssertYamlThrows(TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    tokens:
      oauth2_exchange:
        - name: "nebiusJwt"
          token_endpoint: "grpcs://token.endpoint:443"
)",
            "'subject_credentials' must be specified in oauth2_exchange token config");
    }

    Y_UNIT_TEST(MissingNameInAuthTokensThrows) {
        AssertYamlThrows(TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    tokens:
      oauth2_exchange:
        - token_endpoint: "grpcs://token.endpoint:443"
          subject_credentials:
            type: "FIXED"
            token: "service-account-id"
)",
            "'name' is required in 'auth.tokens.oauth2_exchange'.");
    }

    Y_UNIT_TEST(MissingNameInTokenFileThrows) {
        AssertTokenFileConfigThrows(R"pb(
AccessServiceType: nebius_v1
OAuth2Exchange {
  TokenEndpoint: "grpcs://token.from.file:443"
  SubjectCredentials {
    Type: FIXED
    Token: "sa-from-file"
  }
}
)pb",
            "mvp_validation_missing_name_token_file",
            "'name' is required in token file config.");
    }

    Y_UNIT_TEST(JwtWithoutTokenTypeThrows) {
        AssertYamlThrows(TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    tokens:
      oauth2_exchange:
        - name: "nebiusJwt"
          token_endpoint: "grpcs://token.endpoint:443"
          subject_credentials:
            type: "JWT"
            alg: "RS256"
            private_key: "private-key"
            iss: "service-account-id"
            sub: "service-account-id"
)",
            "token_type is required for JWT subject credentials");
    }

    Y_UNIT_TEST(HttpEndpointThrows) {
        AssertYamlThrows(TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    tokens:
      oauth2_exchange:
        - name: "nebiusJwt"
          token_endpoint: "http://token.endpoint/"
          subject_credentials:
            type: "FIXED"
            token: "service-account-id"
)",
            "'token_endpoint' must use grpc in 'auth.tokens.oauth2_exchange'.");
    }

    Y_UNIT_TEST(FixedCredentialsWithTokenAndTokenFileThrows) {
        AssertYamlThrows(TStringBuilder() << R"(
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
            token_file: "/var/run/secrets/tokens/jwt"
)",
            "must not set both token and token_file");
    }

    Y_UNIT_TEST(HostPortEndpointAccepted) {
        AssertYamlNoThrow(TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    tokens:
      oauth2_exchange:
        - name: "nebiusJwt"
          token_endpoint: "token.endpoint:443"
          subject_credentials:
            type: "FIXED"
            token: "service-account-id"
)");
    }

    Y_UNIT_TEST(AuthTokensAccessServiceTypeMismatchThrows) {
        AssertYamlThrows(TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    tokens:
      access_service_type: "yandex_v2"
)",
            "auth.tokens.access_service_type must match access_service_type");
    }

    Y_UNIT_TEST(SecretInfoWithSecretAndSecretFileThrows) {
        AssertTokenFileConfigThrows(R"pb(
AccessServiceType: nebius_v1
SecretInfo {
  Name: "secret-name"
  Secret: "inline-secret"
  SecretFile: "/tmp/secret.txt"
}
)pb",
            "mvp_validation_secret_and_secret_file",
            "must not set both secret and secret_file");
    }

    Y_UNIT_TEST(SecretInfoWithoutSecretAndSecretFileThrows) {
        AssertTokenFileConfigThrows(R"pb(
AccessServiceType: nebius_v1
SecretInfo {
  Name: "secret-name"
}
)pb",
            "mvp_validation_secret_missing",
            "requires either secret or secret_file");
    }
    Y_UNIT_TEST(SecretInfoWithUnreadableSecretFileThrows) {
        AssertTokenFileConfigThrows(R"pb(
AccessServiceType: nebius_v1
SecretInfo {
  Name: "secret-name"
  SecretFile: "/definitely/not/existing/secret.txt"
}
)pb",
            "mvp_validation_secret_unreadable",
            "unable to read secret_file for secret_info 'secret-name'");
    }

    Y_UNIT_TEST(SecretInfoWithWhitespaceSecretFileThrows) {
        TTempFileHandle secretFile = MakeTestFile("   \n\t  ", "mvp_validation_empty_secret", ".txt");
        AssertTokenFileConfigThrows(TStringBuilder()
                << "AccessServiceType: nebius_v1\n"
                << "SecretInfo {\n"
                << "  Name: \"secret-name\"\n"
                << "  SecretFile: \"" << secretFile.Name() << "\"\n"
                << "}\n",
            "mvp_validation_secret_empty_after_strip",
            "resolved to empty secret");
    }

    Y_UNIT_TEST(SecretInfoFromSecretFileIsNormalized) {
        TTempFileHandle secretFile = MakeTestFile("  normalized-secret \n", "mvp_validation_secret_normalize", ".txt");
        TMvpStartupOptions opts = MakeOptsFromTokenFileConfig(TStringBuilder()
                << "AccessServiceType: nebius_v1\n"
                << "SecretInfo {\n"
                << "  Name: \"secret-name\"\n"
                << "  SecretFile: \"" << secretFile.Name() << "\"\n"
                << "}\n",
            "mvp_validation_secret_normalize");

        UNIT_ASSERT_VALUES_EQUAL(opts.Tokens.SecretInfoSize(), 1);
        const auto& secretInfo = opts.Tokens.GetSecretInfo(0);
        UNIT_ASSERT_VALUES_EQUAL(secretInfo.GetName(), "secret-name");
        UNIT_ASSERT_VALUES_EQUAL(secretInfo.GetSecret(), "normalized-secret");
        UNIT_ASSERT(secretInfo.GetSecretFile().empty());
    }
}
