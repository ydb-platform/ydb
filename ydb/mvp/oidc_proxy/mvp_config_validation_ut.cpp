#include <ydb/mvp/oidc_proxy/mvp.h>
#include <ydb/mvp/core/mvp_test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NMVP::NOIDC;

Y_UNIT_TEST_SUITE(TMvpOidcProxyConfigValidation) {
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
        UNIT_ASSERT_EXCEPTION_CONTAINS(TMVP(3, argv), yexception, "has empty value in auth token config secret_info");
    }
}
