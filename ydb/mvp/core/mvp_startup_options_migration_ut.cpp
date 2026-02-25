#include <ydb/mvp/core/mvp_startup_options.h>
#include <ydb/mvp/core/mvp_test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NMVP;

Y_UNIT_TEST_SUITE(TMvpStartupOptionsMigration) {
    Y_UNIT_TEST(NebiusTokenFileMigratesJwtInfoToOauth2Exchange) {
        TTempFileHandle tmpToken = MakeTestFile(R"pb(
AccessServiceType: nebius_v1
JwtInfo {
  Name: "legacy-jwt"
  Endpoint: "grpcs://token.endpoint:443"
  AccountId: "service-account-id"
  KeyId: "key-id"
  PrivateKey: "private-key"
  Audience: "legacy-audience"
}
)pb", "mvp_legacy_jwt", ".pb.txt");

        TString yaml = TStringBuilder() << R"(
generic:
  access_service_type: "nebius_v1"
  auth:
    token_file: )" << tmpToken.Name() << "\n";
        TTempFileHandle tmpYaml = MakeTestFile(yaml, "mvp_startup_nebius_jwt_migration", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        TMvpStartupOptions opts = MakeOpts(argv);

        UNIT_ASSERT_VALUES_EQUAL(opts.Tokens.JwtInfoSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(opts.Tokens.OAuth2ExchangeSize(), 1);
        const auto& tokenExchange = opts.Tokens.GetOAuth2Exchange(0);
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetName(), "legacy-jwt");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetTokenEndpoint(), "grpcs://token.endpoint:443");
        UNIT_ASSERT(tokenExchange.HasSubjectCredentials());
        UNIT_ASSERT(tokenExchange.GetSubjectCredentials().GetType() == NMvp::TOAuth2Exchange::TCredentials::JWT);
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetTokenType(), "urn:ietf:params:oauth:token-type:jwt");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetAlg(), "RS256");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetPrivateKey(), "private-key");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetKid(), "key-id");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetIss(), "service-account-id");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetSub(), "service-account-id");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().AudSize(), 0);
    }

    Y_UNIT_TEST(YandexTokenFileMigratesJwtInfoToOauth2Exchange) {
        TTempFileHandle tmpToken = MakeTestFile(R"pb(
AccessServiceType: yandex_v2
JwtInfo {
  Name: "legacy-jwt-yandex"
  Endpoint: "grpcs://token.endpoint:443"
  AccountId: "service-account-id"
  KeyId: "key-id"
  PrivateKey: "private-key"
  Audience: "legacy-audience"
}
)pb", "mvp_legacy_jwt_yandex", ".pb.txt");

        TString yaml = TStringBuilder() << R"(
generic:
  access_service_type: "yandex_v2"
  auth:
    token_file: )" << tmpToken.Name() << "\n";
        TTempFileHandle tmpYaml = MakeTestFile(yaml, "mvp_startup_yandex_jwt_migration", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        TMvpStartupOptions opts = MakeOpts(argv);

        UNIT_ASSERT_VALUES_EQUAL(opts.Tokens.JwtInfoSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(opts.Tokens.OAuth2ExchangeSize(), 1);
        const auto& tokenExchange = opts.Tokens.GetOAuth2Exchange(0);
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetName(), "legacy-jwt-yandex");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetTokenEndpoint(), "grpcs://token.endpoint:443");
        UNIT_ASSERT(tokenExchange.HasSubjectCredentials());
        UNIT_ASSERT(tokenExchange.GetSubjectCredentials().GetType() == NMvp::TOAuth2Exchange::TCredentials::JWT);
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetTokenType(), "urn:ietf:params:oauth:token-type:jwt");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetAlg(), "PS256");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetPrivateKey(), "private-key");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetKid(), "key-id");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetIss(), "service-account-id");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetSub(), "");
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().AudSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tokenExchange.GetSubjectCredentials().GetAud(0), "legacy-audience");
    }
}
