#include <ydb/mvp/oidc_proxy/mvp.h>
#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/mvp/core/utils.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/stream/file.h>

#include <yaml-cpp/yaml.h>

using namespace NMVP::NOIDC;

namespace {

NMvp::NOidcProxy::TOidcProxyAppConfig ParseConfig(const TString& yaml) {
    YAML::Node node = YAML::Load(yaml);
    NMvp::NOidcProxy::TOidcProxyAppConfig appConfig;
    NMVP::MergeYamlNodeToProto(node, appConfig);
    return appConfig;
}

} // namespace

Y_UNIT_TEST_SUITE(TMvpOidcProxyConfigValidation) {
    Y_UNIT_TEST(ExampleConfigsParse) {
        const TFsPath examplesPath = TFsPath(ArcadiaFromCurrentLocation(__SOURCE_FILE__, "examples"));
        UNIT_ASSERT_C(examplesPath.IsDirectory(), "Examples directory not found: " << examplesPath.GetPath());
        TVector<TString> fileNames;
        examplesPath.ListNames(fileNames);
        int parsedFiles = 0;

        for (const TString& fileName : fileNames) {
            if (!fileName.EndsWith(".yaml")) {
                continue;
            }

            const TFsPath filePath = examplesPath / fileName;
            UNIT_ASSERT_NO_EXCEPTION_C(
                ParseConfig(TFileInput(filePath.GetPath()).ReadAll()),
                "Failed to parse " << filePath.GetPath()
            );
            ++parsedFiles;
        }

        UNIT_ASSERT_C(parsedFiles > 0, "No .yaml example configs found in " << examplesPath.GetPath());
    }

    Y_UNIT_TEST(SessionServiceTokenNameRequired) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "nebius_v1"
oidc:
  client_id: "test-client"
  session_service_endpoint: "localhost:8655"
  authorization_server_address: "http://auth.test.net"
)", "mvp_oidc_proxy_missing_session_token_name", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(TMVP(3, argv), yexception, "SessionServiceTokenName must be specified");
    }

    Y_UNIT_TEST(SecretNameMustExistInTokenFileSecretInfo) {
        TTempFileHandle tmpTokenFile = MakeTestFile(R"pb(
OAuthInfo {
  Name: "service-account-jwt"
  Endpoint: "grpc://localhost:8655"
  Token: "oauth-token"
}
SecretInfo {
  Name: "another-secret"
  Secret: "secret-value"
}
)pb", "mvp_oidc_proxy_secret_info_missing", ".pb.txt");

        TString yaml = TStringBuilder() << R"(
generic:
  access_service_type: "yandex_v2"
  auth:
    token_file: )" << tmpTokenFile.Name() << R"(
oidc:
  client_id: "test-client"
  secret_name: "secret-name"
  session_service_endpoint: "localhost:8655"
  session_service_token_name: "service-account-jwt"
  authorization_server_address: "http://auth.test.net"
)";
        TTempFileHandle tmpYaml = MakeTestFile(yaml, "mvp_oidc_proxy_missing_secret_info", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(TMVP(3, argv), yexception, "was not found in auth token config secret_info");
    }

    Y_UNIT_TEST(SecretNameRequired) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "nebius_v1"
oidc:
  client_id: "test-client"
  session_service_endpoint: "localhost:8655"
  session_service_token_name: "service-account-jwt"
  authorization_server_address: "http://auth.test.net"
)", "mvp_oidc_proxy_missing_secret_name", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(TMVP(3, argv), yexception, "SecretName must be specified");
    }

    Y_UNIT_TEST(SecretInfoValueMustNotBeEmpty) {
        TTempFileHandle tmpTokenFile = MakeTestFile(R"pb(
OAuthInfo {
  Name: "service-account-jwt"
  Endpoint: "grpc://localhost:8655"
  Token: "oauth-token"
}
SecretInfo {
  Name: "secret-name"
  Secret: ""
}
)pb", "mvp_oidc_proxy_secret_info_empty", ".pb.txt");

        TString yaml = TStringBuilder() << R"(
generic:
  access_service_type: "yandex_v2"
  auth:
    token_file: )" << tmpTokenFile.Name() << R"(
oidc:
  client_id: "test-client"
  secret_name: "secret-name"
  session_service_endpoint: "localhost:8655"
  session_service_token_name: "service-account-jwt"
  authorization_server_address: "http://auth.test.net"
)";
        TTempFileHandle tmpYaml = MakeTestFile(yaml, "mvp_oidc_proxy_empty_secret_info", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(TMVP(3, argv), yexception, "requires either secret or secret_file");
    }

}
