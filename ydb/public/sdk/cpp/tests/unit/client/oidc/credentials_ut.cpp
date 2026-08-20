#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>
#include <util/system/tempfile.h>

#include <vector>

namespace NYdb::inline Dev {

    namespace {

        class TTestConfigFile {
        public:
            explicit TTestConfigFile(TString content)
                : File_(MakeTempName(nullptr, "oidc_config"))
            {
                TUnbufferedFileOutput(File_.Name()).Write(content);
            }

            std::string Name() const {
                return File_.Name();
            }

        private:
            TTempFile File_;
        };

        TOidcConfig StaticConfig(std::string token = "static-token") {
            TOidcTokenSet tokens;
            tokens.AccessToken.Token = std::move(token);
            return TOidcConfig()
                .Issuer("https://idp.example")
                .Flow(TStaticOidcConfig().Tokens(tokens));
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TOidcCredentialsTest) {
        Y_UNIT_TEST(TokenUsability) {
            const TInstant now = TInstant::Now();
            const TOidcToken valid{.Token = "token"};
            const TOidcToken empty;
            const TOidcToken expired{.Token = "token", .ExpiresAt = now};
            UNIT_ASSERT(valid.IsUsable(now));
            UNIT_ASSERT(!empty.IsUsable(now));
            UNIT_ASSERT(!expired.IsUsable(now));
        }

        Y_UNIT_TEST(TokenUsabilityHonorsSkew) {
            const TInstant now = TInstant::Now();
            const TOidcToken token{
                .Token = "token",
                .ExpiresAt = now + TDuration::Seconds(30),
            };
            UNIT_ASSERT(token.IsUsable(now));
            UNIT_ASSERT(!token.IsUsable(now, TDuration::Seconds(30)));
        }

        Y_UNIT_TEST(TokenSetUsabilityIncludesRefreshToken) {
            const TInstant now = TInstant::Now();
            TOidcTokenSet tokens;
            tokens.RefreshToken = TOidcToken{.Token = "refresh-token"};
            UNIT_ASSERT(tokens.IsUsable(now));
        }

        Y_UNIT_TEST(ParseStaticConfigWithoutClientId) {
            const TTestConfigFile file(R"yaml(
issuer: https://idp.example
static_credentials:
  access_token: access-token
)yaml");
            const TOidcConfig config = TOidcConfig::Parse(file.Name());
            const auto& flow = std::get<TStaticOidcConfig>(config.Flow_);
            UNIT_ASSERT(flow.ClientId_.empty());
            UNIT_ASSERT_VALUES_EQUAL(flow.Tokens_.AccessToken.Token, "access-token");
        }

        Y_UNIT_TEST(ParseStaticRefreshTokenWithoutClientId) {
            const TTestConfigFile file(R"yaml(
issuer: https://idp.example
static_credentials:
  access_token: access-token
  refresh_token: refresh-token
)yaml");
            const TOidcConfig config = TOidcConfig::Parse(file.Name());
            const auto& flow = std::get<TStaticOidcConfig>(config.Flow_);
            UNIT_ASSERT(flow.ClientId_.empty());
            UNIT_ASSERT_VALUES_EQUAL(flow.Tokens_.RefreshToken->Token, "refresh-token");
        }

        Y_UNIT_TEST(ParseStaticRefreshConfig) {
            const TTestConfigFile file(R"yaml(
issuer: https://idp.example
static_credentials:
  client_id: ydb-cli
  access_token: access-token
  access_token_expires_at: 2200-01-01T00:00:00Z
  refresh_token: refresh-token
  refresh_token_expires_at: 2200-01-02T00:00:00Z
)yaml");
            const TOidcConfig config = TOidcConfig::Parse(file.Name());
            UNIT_ASSERT_VALUES_EQUAL(config.Issuer_, "https://idp.example");
            const auto& flow = std::get<TStaticOidcConfig>(config.Flow_);
            UNIT_ASSERT_VALUES_EQUAL(flow.ClientId_, "ydb-cli");
            UNIT_ASSERT_VALUES_EQUAL(flow.Tokens_.RefreshToken->Token, "refresh-token");
        }

        Y_UNIT_TEST(ParseClientConfig) {
            const TTestConfigFile file(R"yaml(
issuer: https://idp.example
client_credentials_grant:
  client_id: service
  client_secret: secret
  scope:
    - ydb
    - root
)yaml");
            const TOidcConfig config = TOidcConfig::Parse(file.Name());
            const auto& flow = std::get<TClientOidcConfig>(config.Flow_);
            UNIT_ASSERT_VALUES_EQUAL(flow.ClientId_, "service");
            UNIT_ASSERT_VALUES_EQUAL(flow.ClientSecret_, "secret");
            const std::vector<std::string> expectedScopes{"ydb", "root"};
            UNIT_ASSERT_VALUES_EQUAL(flow.Scope_, expectedScopes);
        }

        Y_UNIT_TEST(ParseDeviceConfig) {
            const TTestConfigFile file(R"yaml(
issuer: https://idp.example
device_authorization_grant:
  client_id: cli
)yaml");
            const TOidcConfig config = TOidcConfig::Parse(file.Name());
            const auto& flow = std::get<TDeviceOidcConfig>(config.Flow_);
            UNIT_ASSERT_VALUES_EQUAL(flow.ClientId_, "cli");
        }

        Y_UNIT_TEST(RejectMultipleFlows) {
            const TTestConfigFile file(R"yaml(
issuer: https://idp.example
static_credentials:
  access_token: token
client_credentials_grant:
  client_id: service
  client_secret: secret
)yaml");
            UNIT_ASSERT_EXCEPTION(TOidcConfig::Parse(file.Name()), std::invalid_argument);
        }

        Y_UNIT_TEST(StaticProviderReturnsBearerToken) {
            auto provider = CreateOidcCredentialsProviderFactory(StaticConfig())->CreateProvider();
            UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer static-token");
        }

        Y_UNIT_TEST(FactoryCreatesProviderOnlyOnceAcrossOverloads) {
            auto factory = CreateOidcCredentialsProviderFactory(StaticConfig());
            auto first = factory->CreateProvider();
            auto second = factory->CreateProvider(CreateSimpleCoreFacility());
            UNIT_ASSERT_VALUES_EQUAL(first.get(), second.get());
        }
    } // Y_UNIT_TEST_SUITE(TOidcCredentialsTest)

} // namespace NYdb::inline Dev
