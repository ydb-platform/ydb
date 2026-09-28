#include "oidc.h"
#include "oidc_options.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/stream/str.h>

#include <yaml-cpp/yaml.h>

namespace NYdb::NConsoleClient {
namespace {

TOidcCliOptions DeviceOptions();

TOidcCliOptions DeviceOptions() {
    TOidcCliOptions options;
    options.Issuer = "https://issuer.example";
    options.ClientId = "ydb-cli";
    return options;
}

} // namespace

Y_UNIT_TEST_SUITE(TOidcCliOptionsTest) {
    Y_UNIT_TEST(DefaultsToDeviceFlow) {
        const auto config = DeviceOptions().MakeConfig();
        UNIT_ASSERT_VALUES_EQUAL(config.Issuer, "https://issuer.example");
        UNIT_ASSERT_VALUES_EQUAL(std::get<NOidc::TDeviceOidcConfig>(config.FlowConfig).ClientId, "ydb-cli");
    }

    Y_UNIT_TEST(StaticTokenFileAndProfileContainOnlyPath) {
        TTempDir dir;
        TOidcCliOptions options;
        options.Issuer = "https://issuer.example";
        options.AccessTokenFile = (dir.Path() / "token").GetPath();
        for (const char* contents : {"private-token\n", "Bearer private-token\n"}) {
            TFileOutput(options.AccessTokenFile).Write(contents);
            UNIT_ASSERT_VALUES_EQUAL(CreateCliOidcCredentialsProviderFactory(options)->CreateProvider()->GetAuthInfo(), "Bearer private-token");
        }
        const auto auth = options.MakeProfileAuth();
        UNIT_ASSERT_VALUES_EQUAL(auth["data"]["access_token_file"].as<std::string>(), std::string(options.AccessTokenFile));
        UNIT_ASSERT(!auth["data"]["access_token"].IsDefined());
        UNIT_ASSERT(!TString(YAML::Dump(auth)).Contains("private-token"));
        TStringStream output;
        options.Print(output);
        UNIT_ASSERT(!output.Str().Contains("private-token"));
    }

    Y_UNIT_TEST(RejectsTokenFileForOtherFlows) {
        auto options = DeviceOptions();
        options.AccessTokenFile = "/nonexistent/token";
        for (const char* flow : {"client", "device"}) {
            options.Flow = flow;
            UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "requires static OIDC flow");
        }
        options.Flow = "static";
        UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "does not accept client ID");
    }

    Y_UNIT_TEST(ClientFlowAndScopes) {
        auto options = DeviceOptions();
        options.Flow = "client";
        options.ClientSecret = "secret";
        options.Scope = "openid  user-context\toffline_access";
        const auto config = options.MakeConfig();
        const auto& client = std::get<NOidc::TClientOidcConfig>(config.FlowConfig);
        UNIT_ASSERT_VALUES_EQUAL(client.ClientSecret, "secret");
        UNIT_ASSERT_VALUES_EQUAL(client.Scopes.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(client.Scopes[1], "user-context");
    }

    Y_UNIT_TEST(RequiresIssuerAndFlowCredentials) {
        TOidcCliOptions empty;
        UNIT_ASSERT_EXCEPTION_CONTAINS(empty.MakeConfig(), std::invalid_argument, "requires --oidc-issuer");
        auto options = DeviceOptions();
        options.ClientId.clear();
        UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "client_id is required");
        options.ClientId = "client";
        options.Flow = "client";
        UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "client_secret is required");
    }

    Y_UNIT_TEST(RejectsWrongFlowFields) {
        auto options = DeviceOptions();
        options.ClientSecret = "do-not-print-this";
        UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "Client secret requires client OIDC flow");
    }

    Y_UNIT_TEST(RejectsUnknownFlowAndInsecureIssuer) {
        auto options = DeviceOptions();
        options.Flow = "password";
        UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "must be static, client or device");
        options.Flow = "device";
        options.Issuer = "http://issuer.example";
        UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "requires HTTPS");
    }

    Y_UNIT_TEST(CreatesFileCacherForDirectOptions) {
        TTempDir dir;
        auto options = DeviceOptions();
        options.CachePath = (dir.Path() / "tokens.json").GetPath();
        const auto config = options.MakeConfig();
        UNIT_ASSERT(config.Cacher_ != nullptr);
        const NOidc::TTokenCache tokens{
            .AccessToken = {.Token = "cached-token", .ExpiresAt = TInstant::Seconds(2000000000)},
            .RefreshToken = std::nullopt,
        };
        config.Cacher_->Write(tokens);
        const auto restored = options.MakeConfig().Cacher_->Read();
        UNIT_ASSERT(restored.has_value());
        UNIT_ASSERT_VALUES_EQUAL(restored->AccessToken.Token, "cached-token");
    }

    Y_UNIT_TEST(MasksSecretsInConnectionInfo) {
        auto options = DeviceOptions();
        options.ClientSecret = "private-client-secret";
        TStringStream output;
        options.Print(output);
        UNIT_ASSERT_STRING_CONTAINS(output.Str(), "oidc-client-id: ydb-cli");
        UNIT_ASSERT_STRING_CONTAINS(output.Str(), "YDB_OIDC_CLIENT_SECRET: ***");
        UNIT_ASSERT(!output.Str().Contains("oidc-client-secret:"));
        UNIT_ASSERT(!output.Str().Contains("private-client-secret"));
    }

    Y_UNIT_TEST(SerializesProfileAuth) {
        auto options = DeviceOptions();
        options.Scope = "openid user-context";
        options.ClientSecretFile = "/tmp/secret";
        options.ClientSecret = "never-persist-this";
        const auto auth = options.MakeProfileAuth();
        UNIT_ASSERT_VALUES_EQUAL(auth["method"].as<std::string>(), "oidc");
        UNIT_ASSERT_VALUES_EQUAL(auth["data"]["client_id"].as<std::string>(), "ydb-cli");
        UNIT_ASSERT_VALUES_EQUAL(auth["data"]["scope"].as<std::string>(), "openid user-context");
        UNIT_ASSERT_VALUES_EQUAL(auth["data"]["client_secret_file"].as<std::string>(), "/tmp/secret");
        UNIT_ASSERT(!auth["data"]["client_secret"].IsDefined());
        UNIT_ASSERT(!TString(YAML::Dump(auth)).Contains("never-persist-this"));
    }
}

} // namespace NYdb::NConsoleClient
