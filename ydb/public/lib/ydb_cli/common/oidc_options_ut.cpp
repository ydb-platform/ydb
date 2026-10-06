#include "oidc.h"
#include "client_command_options.h"
#include "command.h"
#include "oidc_options.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/sdk/cpp/tests/unit/client/oauth2_token_exchange/helpers/test_token_exchange_server.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/common/scope.h>

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

    Y_UNIT_TEST(DefaultCredentialsGetterPrefersOAuthExchangeToOidc) {
        TTempDir dir;
        const auto path = (dir.Path() / "oauth.json").GetPath();
        TFileOutput(path).Write(R"({"subject-credentials":{"type":"fixed","token":"subject-token","token-type":"test-token-type"}})");
        TTestTokenExchangeServer server;
        server.Check.ExpectedInputParams = {
            {"grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"},
            {"requested_token_type", "urn:ietf:params:oauth:token-type:access_token"},
            {"subject_token", "subject-token"},
            {"subject_token_type", "test-token-type"},
        };
        server.Check.Response = R"({"access_token":"exchange-token","token_type":"bearer","expires_in":600})";
        char name[] = "ydb";
        char* args[] = {name};
        TClientCommand::TConfig config(1, args);
        config.UseOauth2TokenExchange = true;
        config.Oauth2KeyFile = path;
        config.IamEndpoint = server.GetEndpoint();
        config.Oidc.Issuer = "invalid-ignored-issuer";
        const auto factory = config.GetSingletonCredentialsProviderFactory();
        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer exchange-token");
        server.CheckExpectations();
    }

    Y_UNIT_TEST(DefaultCredentialsGetterAllowsAnonymousWithoutOAuthKey) {
        char name[] = "ydb";
        char* args[] = {name};
        TClientCommand::TConfig config(1, args);
        config.UseOauth2TokenExchange = true;
        UNIT_ASSERT(config.GetSingletonCredentialsProviderFactory()->CreateProvider()->GetAuthInfo().empty());
    }

    Y_UNIT_TEST(EmptyOptionsDoNotPrintOrSelectAuthentication) {
        TOidcCliOptions options;
        UNIT_ASSERT(!options.HasOptions());
        UNIT_ASSERT(!options.IsConfigured());
        TStringStream output;
        options.Print(output);
        UNIT_ASSERT(output.Str().empty());
        options.Scope = "read";
        UNIT_ASSERT(options.HasOptions());
        UNIT_ASSERT(!options.IsConfigured());
    }

    Y_UNIT_TEST(ConfigFileRejectsEveryDirectOptionBeforeOpeningFile) {
        for (const auto member : {&TOidcCliOptions::Issuer, &TOidcCliOptions::Flow,
                &TOidcCliOptions::ClientId, &TOidcCliOptions::ClientSecretFile,
                &TOidcCliOptions::AccessTokenFile, &TOidcCliOptions::Scope, &TOidcCliOptions::CachePath}) {
            TOidcCliOptions options;
            options.ConfigFile = "/nonexistent/oidc.yaml";
            options.*member = "value";
            UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "cannot be combined");
        }
    }

    Y_UNIT_TEST(NullCredentialsFactoryIsNotWrapped) {
        char name[] = "ydb";
        char* args[] = {name};
        TClientCommand::TConfig config(1, args);
        config.CredentialsGetter = [](const TClientCommand::TConfig&) -> TCredentialsProviderFactoryPtr {
            return nullptr;
        };
        UNIT_ASSERT(config.GetSingletonCredentialsProviderFactory() == nullptr);
    }

    Y_UNIT_TEST(DefaultsToDeviceFlow) {
        const auto config = DeviceOptions().MakeConfig();
        UNIT_ASSERT_VALUES_EQUAL(config.Issuer, "https://issuer.example");
        UNIT_ASSERT_VALUES_EQUAL(std::get<NOidc::TDeviceOidcConfig>(config.FlowConfig).ClientId, "ydb-cli");
    }

    Y_UNIT_TEST(DetectsDeviceFlowFromDirectOptions) {
        UNIT_ASSERT(!TOidcCliOptions().IsDeviceFlow());
        auto options = DeviceOptions();
        UNIT_ASSERT(options.IsDeviceFlow());
        options.Flow = "device";
        UNIT_ASSERT(options.IsDeviceFlow());
        const NTesting::TScopedEnvironment secret("YDB_OIDC_CLIENT_SECRET", "secret");
        options.Flow = "client";
        UNIT_ASSERT(!options.IsDeviceFlow());
    }

    Y_UNIT_TEST(DetectsDeviceFlowFromResolvedFileConfig) {
        TTempDir dir;
        const auto path = dir.Path() / "oidc.yaml";
        TOidcCliOptions options;
        options.ConfigFile = path.GetPath();
        for (const TString& flow : {
                TString("static_credentials:\n  access_token_file: token\n"),
                TString("client_credentials_grant:\n  client_id: client\n  client_secret_file: secret\n"),
                TString("device_authorization_grant:\n  client_id: cli\n")})
        {
            TFileOutput((dir.Path() / "token").GetPath()).Write("token");
            TFileOutput((dir.Path() / "secret").GetPath()).Write("secret");
            TFileOutput(path.GetPath()).Write("issuer: https://issuer.example\n" + flow);
            options.ResolvedConfig.reset();
            const bool device = flow.StartsWith("device_authorization_grant:");
            UNIT_ASSERT_VALUES_EQUAL(options.IsDeviceFlow(), device);
            options.ResolvedConfig = options.MakeConfig();
            path.DeleteIfExists();
            UNIT_ASSERT_VALUES_EQUAL(options.IsDeviceFlow(), device);
        }
    }

    Y_UNIT_TEST(DefaultCredentialsGetterSelectsOidcAndReusesProvider) {
        char name[] = "ydb";
        char* args[] = {name};
        TClientCommand::TConfig config(1, args);
        config.Oidc.Issuer = "https://issuer.example";
        config.Oidc.ResolvedConfig = NOidc::TOidcConfig{
            .Issuer = "https://issuer.example",
            .FlowConfig = NOidc::TStaticOidcConfig{.AccessToken = "oidc-token", .ExpiresAt = std::nullopt},
        };
        const auto factory = config.GetSingletonCredentialsProviderFactory();
        auto probeFacility = CreateSimpleCoreFacility();
        const auto provider = factory->CreateProvider(probeFacility);
        auto ready = provider->GetAuthInfoAsync();
        UNIT_ASSERT(ready.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(ready.GetValueSync(), "Bearer oidc-token");
        probeFacility.reset();

        const auto sqlFacility = CreateSimpleCoreFacility();
        UNIT_ASSERT(factory == config.GetSingletonCredentialsProviderFactory());
        UNIT_ASSERT(provider == factory->CreateProvider(sqlFacility));
        UNIT_ASSERT(provider == factory->CreateProvider());
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer oidc-token");
    }

    Y_UNIT_TEST(DefaultCredentialsGetterPrefersSecurityTokenToOidc) {
        char name[] = "ydb";
        char* args[] = {name};
        TClientCommand::TConfig config(1, args);
        config.SecurityToken = "legacy-token";
        config.Oidc.Issuer = "https://issuer.example";
        config.Oidc.ResolvedConfig = NOidc::TOidcConfig{
            .Issuer = "https://issuer.example",
            .FlowConfig = NOidc::TStaticOidcConfig{.AccessToken = "oidc-token", .ExpiresAt = std::nullopt},
        };
        const auto factory = config.GetSingletonCredentialsProviderFactory();
        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "legacy-token");
    }

    Y_UNIT_TEST(CustomCredentialsGetterControlsSelectionWithOidcConfigured) {
        char name[] = "ydb";
        char* args[] = {name};
        TClientCommand::TConfig config(1, args);
        config.Oidc.Issuer = "https://issuer.example";
        config.Oidc.ResolvedConfig = NOidc::TOidcConfig{
            .Issuer = "https://issuer.example",
            .FlowConfig = NOidc::TStaticOidcConfig{.AccessToken = "oidc-token", .ExpiresAt = std::nullopt},
        };
        size_t calls = 0;
        config.CredentialsGetter = [&calls](const TClientCommand::TConfig&) {
            ++calls;
            return CreateOAuthCredentialsProviderFactory("custom-token");
        };
        const auto factory = config.GetSingletonCredentialsProviderFactory();
        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "custom-token");
        UNIT_ASSERT(factory == config.GetSingletonCredentialsProviderFactory());
        UNIT_ASSERT(factory->CreateProvider() == factory->CreateProvider());
        UNIT_ASSERT_VALUES_EQUAL(calls, 1);
    }

    Y_UNIT_TEST(StaticTokenFileAndProfileContainOnlyPath) {
        TTempDir dir;
        TOidcCliOptions options;
        options.Issuer = "https://issuer.example";
        options.AccessTokenFile = (dir.Path() / "token").GetPath();
        for (const char* contents : {"private-token\n", "Bearer private-token\n"}) {
            TFileOutput(options.AccessTokenFile).Write(contents);
            UNIT_ASSERT(!options.IsDeviceFlow());
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

    Y_UNIT_TEST(PersistsInferredProfileFlowWithoutEnvironmentSecret) {
        TOidcCliOptions options;
        options.Issuer = "https://issuer.example";
        {
            const NTesting::TScopedEnvironment token("YDB_OIDC_ACCESS_TOKEN", "private-env-token");
            options.MakeConfig();
            const auto auth = options.MakeProfileAuth();
            UNIT_ASSERT_VALUES_EQUAL(auth["data"]["flow"].as<std::string>(), "static");
            UNIT_ASSERT(!TString(YAML::Dump(auth)).Contains("private-env-token"));
            options.Flow = auth["data"]["flow"].as<std::string>();
        }
        const NTesting::TScopedEnvironment noToken("YDB_OIDC_ACCESS_TOKEN", "");
        UNIT_ASSERT_EXCEPTION_CONTAINS(options.MakeConfig(), std::invalid_argument, "access_token");
        UNIT_ASSERT_VALUES_EQUAL(DeviceOptions().MakeProfileAuth()["data"]["flow"].as<std::string>(), "device");
    }

    Y_UNIT_TEST(StaticTokenRotationDoesNotUseFileCache) {
        TTempDir dir;
        TOidcCliOptions options;
        options.Issuer = "https://issuer.example";
        options.AccessTokenFile = (dir.Path() / "token").GetPath();
        options.CachePath = (dir.Path() / "cache.json").GetPath();
        for (const TString& token : {TString("first-token"), TString("rotated-token")}) {
            TFileOutput(options.AccessTokenFile).Write(token);
            UNIT_ASSERT(options.MakeConfig().Cacher_ == nullptr);
            UNIT_ASSERT_VALUES_EQUAL(CreateCliOidcCredentialsProviderFactory(options)->CreateProvider()->GetAuthInfo(), "Bearer " + token);
            UNIT_ASSERT(!TFsPath(options.CachePath).Exists());
        }
    }

    Y_UNIT_TEST(EnvironmentCredentialsAreLiteralValues) {
        TTempDir dir;
        const auto path = (dir.Path() / "secret").GetPath();
        TFileOutput(path).Write("must-not-read-this-file");
        const NTesting::TScopedEnvironment secret("YDB_OIDC_CLIENT_SECRET", path);
        const NTesting::TScopedEnvironment token("YDB_OIDC_ACCESS_TOKEN", path);
        TOidcCliOptions options;
        options.Issuer = "https://issuer.example";
        UNIT_ASSERT_VALUES_EQUAL(std::get<NOidc::TStaticOidcConfig>(options.MakeConfig().FlowConfig).AccessToken, path);
        options.Flow = "client";
        options.ClientId = "client";
        UNIT_ASSERT_VALUES_EQUAL(std::get<NOidc::TClientOidcConfig>(options.MakeConfig().FlowConfig).ClientSecret, path);
    }

    Y_UNIT_TEST(ResolvedConfigDoesNotReadTokenFileAgain) {
        TTempDir dir;
        const auto path = dir.Path() / "token";
        TFileOutput(path.GetPath()).Write("original-token");
        TOidcCliOptions values;
        TClientCommandOptions options;
        AddOidcOptions(options, values, false);
        const char* args[] = {"ydb", "--oidc-issuer", "https://issuer.example", "--oidc-access-token-file", path.GetPath().c_str()};
        TOptionsParseResult parsed(&options, std::size(args), args);
        UNIT_ASSERT(parsed.ParseFromProfilesAndEnv(nullptr, nullptr).empty());
        ResolveOidcOptions(values, parsed);
        path.DeleteIfExists();
        UNIT_ASSERT_VALUES_EQUAL(CreateCliOidcCredentialsProviderFactory(values)->CreateProvider()->GetAuthInfo(), "Bearer original-token");
    }

    Y_UNIT_TEST(ConfigFileIsLoadedOnceAndProfileStoresOnlyPath) {
        TTempDir dir;
        const auto path = dir.Path() / "oidc.yaml";
        const auto token = dir.Path() / "token";
        TFileOutput(token.GetPath()).Write("config-token");
        TFileOutput(path.GetPath()).Write("issuer: https://issuer.example\nstatic_credentials:\n  access_token_file: token\n");
        TOidcCliOptions values;
        TClientCommandOptions options;
        AddOidcOptions(options, values, false);
        const char* args[] = {"ydb", "--oidc-config", path.GetPath().c_str()};
        TOptionsParseResult parsed(&options, std::size(args), args);
        UNIT_ASSERT(parsed.ParseFromProfilesAndEnv(nullptr, nullptr).empty());
        ResolveOidcOptions(values, parsed);
        UNIT_ASSERT(values.IsConfigured());
        UNIT_ASSERT(values.HasOptions());
        const auto auth = values.MakeProfileAuth();
        UNIT_ASSERT_VALUES_EQUAL(auth["method"].as<std::string>(), "oidc-config");
        UNIT_ASSERT_VALUES_EQUAL(auth["data"].as<std::string>(), path.GetPath());
        UNIT_ASSERT(!TString(YAML::Dump(auth)).Contains("config-token"));
        path.DeleteIfExists();
        token.DeleteIfExists();
        UNIT_ASSERT_VALUES_EQUAL(CreateCliOidcCredentialsProviderFactory(values)->CreateProvider()->GetAuthInfo(), "Bearer config-token");
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
        const NTesting::TScopedEnvironment secret("YDB_OIDC_CLIENT_SECRET", "secret");
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
        options.ClientSecretFile = "/nonexistent/secret";
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
        options.Flow = "client";
        const NTesting::TScopedEnvironment secret("YDB_OIDC_CLIENT_SECRET", "private-client-secret");
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
        const NTesting::TScopedEnvironment secret("YDB_OIDC_CLIENT_SECRET", "never-persist-this");
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
