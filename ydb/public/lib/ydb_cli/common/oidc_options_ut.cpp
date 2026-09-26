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
TOidcCliOptions StaticOptions();

TOidcCliOptions DeviceOptions() {
    TOidcCliOptions options;
    options.Issuer = "https://issuer.example";
    options.ClientId = "ydb-cli";
    return options;
}

TOidcCliOptions StaticOptions() {
    TOidcCliOptions options;
    options.Issuer = "https://issuer.example";
    options.Flow = "static";
    options.AccessToken = "opaque-token";
    return options;
}

} // namespace

Y_UNIT_TEST_SUITE(TOidcCliOptionsTest) {
    Y_UNIT_TEST(DefaultsToDeviceFlow) {
        const auto config = DeviceOptions().MakeConfig();
        UNIT_ASSERT_VALUES_EQUAL(config.Issuer, "https://issuer.example");
        UNIT_ASSERT_VALUES_EQUAL(std::get<NOidc::TDeviceOidcConfig>(config.FlowConfig).ClientId, "ydb-cli");
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

    Y_UNIT_TEST(StaticFlowUsesBearerToken) {
        auto options = StaticOptions();
        options.ExpiresAt = "2000000000";
        const auto config = options.MakeConfig();
        UNIT_ASSERT(std::get<NOidc::TStaticOidcConfig>(config.FlowConfig).ExpiresAt == TInstant::Seconds(2000000000));
        UNIT_ASSERT_VALUES_EQUAL(CreateCliOidcCredentialsProviderFactory(options)->CreateProvider()->GetAuthInfo(), "Bearer opaque-token");
    }

    Y_UNIT_TEST(LoadsFileConfiguration) {
        TTempDir dir;
        TOidcCliOptions options;
        options.ConfigFile = (dir.Path() / "oidc.yaml").GetPath();
        TFileOutput(options.ConfigFile).Write("issuer: https://issuer.example\nstatic_credentials:\n  access_token: file-token\n");
        UNIT_ASSERT_VALUES_EQUAL(CreateCliOidcCredentialsProviderFactory(options)->CreateProvider()->GetAuthInfo(), "Bearer file-token");
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
        options = StaticOptions();
        options.AccessToken.clear();
        UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "requires access_token");
    }

    Y_UNIT_TEST(RejectsWrongFlowFields) {
        auto options = DeviceOptions();
        options.ClientSecret = "do-not-print-this";
        UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "Client secret requires client OIDC flow");
        options = StaticOptions();
        options.ClientId = "client";
        UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "Static OIDC flow does not accept");
        options = DeviceOptions();
        options.AccessToken = "do-not-print-this";
        UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "require static OIDC flow");
    }

    Y_UNIT_TEST(RejectsInvalidExpiration) {
        auto options = StaticOptions();
        for (const char* value : {"-1", "not-a-number", "18446744073709551615"}) {
            options.ExpiresAt = value;
            UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "non-negative Unix seconds");
        }
    }

    Y_UNIT_TEST(RejectsFileAndDirectSettingsTogether) {
        auto options = DeviceOptions();
        options.ConfigFile = "oidc.yaml";
        UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "cannot be combined");
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
        options.AccessToken = "private-access-token";
        TStringStream output;
        options.Print(output);
        UNIT_ASSERT_STRING_CONTAINS(output.Str(), "oidc-client-id: ydb-cli");
        UNIT_ASSERT_STRING_CONTAINS(output.Str(), "oidc-client-secret: ***");
        UNIT_ASSERT_STRING_CONTAINS(output.Str(), "oidc-access-token: ***");
        UNIT_ASSERT(!output.Str().Contains("private-client-secret"));
        UNIT_ASSERT(!output.Str().Contains("private-access-token"));
    }

    Y_UNIT_TEST(SerializesProfileAuth) {
        auto options = DeviceOptions();
        options.Scope = "openid user-context";
        const auto auth = options.MakeProfileAuth();
        UNIT_ASSERT_VALUES_EQUAL(auth["method"].as<std::string>(), "oidc");
        UNIT_ASSERT_VALUES_EQUAL(auth["data"]["client_id"].as<std::string>(), "ydb-cli");
        UNIT_ASSERT_VALUES_EQUAL(auth["data"]["scope"].as<std::string>(), "openid user-context");
        options = {};
        options.ConfigFile = "/tmp/oidc.yaml";
        const auto fileAuth = options.MakeProfileAuth();
        UNIT_ASSERT_VALUES_EQUAL(fileAuth["method"].as<std::string>(), "oidc-config");
        UNIT_ASSERT_VALUES_EQUAL(fileAuth["data"].as<std::string>(), "/tmp/oidc.yaml");
    }
}

} // namespace NYdb::NConsoleClient
