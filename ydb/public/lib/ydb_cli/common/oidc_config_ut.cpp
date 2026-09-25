#include <ydb/public/lib/ydb_cli/common/oidc_config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

#include <functional>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

using namespace NYdb::NOidc;

namespace NYdb::NConsoleClient {
namespace {

std::string WriteFile(const TFsPath& path, const std::string& contents);
std::string ExceptionMessage(const std::function<void()>& action);

std::string WriteFile(const TFsPath& path, const std::string& contents) {
    TFileOutput(path.GetPath()).Write(contents);
    return path.GetPath();
}

std::string ExceptionMessage(const std::function<void()>& action) {
    try {
        action();
    } catch (const std::exception& e) {
        return e.what();
    }
    UNIT_FAIL("Expected an exception");
    return {};
}

} // namespace

Y_UNIT_TEST_SUITE(TOidcConfigFile) {
    Y_UNIT_TEST(LoadsStaticCredentials) {
        TTempDir dir;
        const auto path = WriteFile(dir.Path() / "oidc.yaml", R"(
issuer: https://issuer.example
static_credentials:
  access_token: opaque-access
  expires_at: 2000000000
)");

        const auto config = LoadOidcConfig(path);
        UNIT_ASSERT_VALUES_EQUAL(config.Issuer, "https://issuer.example");
        UNIT_ASSERT(std::holds_alternative<TStaticOidcConfig>(config.FlowConfig));
        const auto& flow = std::get<TStaticOidcConfig>(config.FlowConfig);
        UNIT_ASSERT_VALUES_EQUAL(flow.AccessToken, "opaque-access");
        UNIT_ASSERT(flow.ExpiresAt.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*flow.ExpiresAt, TInstant::Seconds(2'000'000'000));
    }

    Y_UNIT_TEST(LoadsClientAndDeviceCredentials) {
        TTempDir dir;
        const auto clientPath = WriteFile(dir.Path() / "client.yaml", R"(
issuer: https://issuer.example
client_credentials_grant:
  client_id: client
  client_secret: secret
  scope: [openid, audience.read]
)");
        const auto devicePath = WriteFile(dir.Path() / "device.yaml", R"(
issuer: https://issuer.example
device_authorization_grant:
  client_id: cli
  scope:
    - openid
    - offline_access
)");

        const auto client = LoadOidcConfig(clientPath);
        const auto& clientFlow = std::get<TClientOidcConfig>(client.FlowConfig);
        UNIT_ASSERT_VALUES_EQUAL(clientFlow.ClientId, "client");
        UNIT_ASSERT_VALUES_EQUAL(clientFlow.ClientSecret, "secret");
        UNIT_ASSERT_VALUES_EQUAL(clientFlow.Scopes, (std::vector<std::string>{"openid", "audience.read"}));

        const auto device = LoadOidcConfig(devicePath);
        const auto& deviceFlow = std::get<TDeviceOidcConfig>(device.FlowConfig);
        UNIT_ASSERT_VALUES_EQUAL(deviceFlow.ClientId, "cli");
        UNIT_ASSERT_VALUES_EQUAL(deviceFlow.Scopes, (std::vector<std::string>{"openid", "offline_access"}));
    }

    Y_UNIT_TEST(RejectsUnsupportedLegacySettings) {
        TTempDir dir;
        for (const auto& field : {"socket_timeout", "connect_timeout", "allow_insecure_http", "token_endpoint_auth_method"}) {
            const auto path = WriteFile(dir.Path() / "unsupported.yaml",
                std::string("issuer: https://issuer.example\n") + field + ": value\nstatic_credentials:\n  access_token: token\n");
            UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(path), std::invalid_argument, field);
        }
        for (const auto& field : {"refresh_token", "refresh_expires_at", "client_id", "client_secret"}) {
            const auto path = WriteFile(dir.Path() / "unsupported-static.yaml",
                std::string("issuer: https://issuer.example\nstatic_credentials:\n  access_token: token\n  ") + field + ": value\n");
            UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(path), std::invalid_argument, field);
        }
    }

    Y_UNIT_TEST(RejectsInsecureIssuer) {
        TTempDir dir;
        const auto path = WriteFile(dir.Path() / "insecure.yaml",
            "issuer: http://issuer.example\nstatic_credentials:\n  access_token: token\n");
        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(path), std::invalid_argument, "HTTPS");
    }

    Y_UNIT_TEST(RequiresExactlyOneGrant) {
        TTempDir dir;
        const auto missingPath = WriteFile(dir.Path() / "missing.yaml", "issuer: https://issuer.example\n");
        const auto multiplePath = WriteFile(dir.Path() / "multiple.yaml", R"(
issuer: https://issuer.example
static_credentials:
  access_token: token
device_authorization_grant:
  client_id: cli
)");

        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(missingPath), std::invalid_argument, "exactly one");
        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(multiplePath), std::invalid_argument, "exactly one");
    }

    Y_UNIT_TEST(RejectsUnknownEmptyAndWrongTypeFields) {
        TTempDir dir;
        const auto unknownPath = WriteFile(dir.Path() / "unknown.yaml", R"(
issuer: https://issuer.example
client_credentials_grant:
  client_id: client
  client_secret: secret
  typo_scope: [openid]
)");
        const auto emptyPath = WriteFile(dir.Path() / "empty.yaml", R"(
issuer: ""
device_authorization_grant:
  client_id: cli
)");
        const auto typePath = WriteFile(dir.Path() / "type.yaml", R"(
issuer: https://issuer.example
device_authorization_grant:
  client_id: cli
  scope: openid
)");

        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(unknownPath), std::invalid_argument, "typo_scope");
        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(emptyPath), std::invalid_argument, "issuer");
        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(typePath), std::invalid_argument, "scope");
    }

    Y_UNIT_TEST(ErrorsNameSecretFieldWithoutDisclosingValue) {
        TTempDir dir;
        const auto path = WriteFile(dir.Path() / "secret.yaml", R"(
issuer: https://issuer.example
client_credentials_grant:
  client_id: client
  client_secret: [do-not-print-this-secret]
)");

        const auto message = ExceptionMessage([&] { LoadOidcConfig(path); });
        UNIT_ASSERT_STRING_CONTAINS(message, "client_secret");
        UNIT_ASSERT_VALUES_EQUAL(message.find("do-not-print-this-secret"), std::string::npos);
    }

    Y_UNIT_TEST(RejectsCoercedScalarTypes) {
        TTempDir dir;
        const auto numericClient = WriteFile(dir.Path() / "numeric-client.yaml", R"(
issuer: https://issuer.example
client_credentials_grant:
  client_id: 123
  client_secret: secret
)");
        const auto booleanSecret = WriteFile(dir.Path() / "boolean-secret.yaml", R"(
issuer: https://issuer.example
client_credentials_grant:
  client_id: client
  client_secret: true
)");
        const auto numericScope = WriteFile(dir.Path() / "numeric-scope.yaml", R"(
issuer: https://issuer.example
device_authorization_grant:
  client_id: cli
  scope: [openid, 123]
)");
        const auto quotedExpiry = WriteFile(dir.Path() / "quoted-expiry.yaml", R"(
issuer: https://issuer.example
static_credentials:
  access_token: access
  expires_at: "2000000000"
)");
        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(numericClient), std::invalid_argument, "client_id");
        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(booleanSecret), std::invalid_argument, "client_secret");
        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(numericScope), std::invalid_argument, "scope");
        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(quotedExpiry), std::invalid_argument, "expires_at");
    }

    Y_UNIT_TEST(RejectsExpiryThatCannotFitTInstant) {
        TTempDir dir;
        const auto path = WriteFile(dir.Path() / "overflow.yaml", R"(
issuer: https://issuer.example
static_credentials:
  access_token: access
  expires_at: 18446744073710
)");

        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadOidcConfig(path), std::invalid_argument, "expires_at");
    }

    Y_UNIT_TEST(ResolvesRelativeCachePathFromConfigDirectory) {
        TTempDir dir;
        const auto path = WriteFile(dir.Path() / "oidc.yaml", R"(
issuer: https://issuer.example
cache_path: state/tokens.json
static_credentials:
  access_token: opaque
)");
        (dir.Path() / "state").MkDir();

        auto config = LoadOidcConfig(path);
        UNIT_ASSERT(config.Cacher_ != nullptr);
        config.Cacher_->Write(TTokenCache{.AccessToken = {.Token = "saved-access"}});
        UNIT_ASSERT((dir.Path() / "state" / "tokens.json").Exists());
    }

    Y_UNIT_TEST(CreatesStaticProviderFromFile) {
        TTempDir dir;
        const auto path = WriteFile(dir.Path() / "static.yaml", R"(
issuer: https://issuer.example
static_credentials:
  access_token: opaque-access
)");

        const auto factory = CreateOidcFileCredentialsProviderFactory(path, nullptr);
        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer opaque-access");
    }

    Y_UNIT_TEST(CacheIdentitySurvivesReloadAndSeparatesCredentials) {
        TTempDir dir;
        const auto path = WriteFile(dir.Path() / "client.yaml", R"(
issuer: https://issuer.example
cache_path: tokens.json
client_credentials_grant:
  client_id: client
  client_secret: first-secret
)");
        const auto original = LoadOidcConfig(path);
        UNIT_ASSERT(original.Cacher_ != nullptr);
        original.Cacher_->Write(TTokenCache{.AccessToken = {.Token = "cached-access"}});

        const auto reloaded = LoadOidcConfig(path);
        const auto cached = reloaded.Cacher_->Read();
        UNIT_ASSERT(cached.has_value());
        UNIT_ASSERT_VALUES_EQUAL(cached->AccessToken.Token, "cached-access");

        WriteFile(path, R"(
issuer: https://issuer.example
cache_path: tokens.json
client_credentials_grant:
  client_id: client
  client_secret: second-secret
)");
        const auto changed = LoadOidcConfig(path);
        UNIT_ASSERT(!changed.Cacher_->Read().has_value());
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            changed.Cacher_->Write(TTokenCache{.AccessToken = {.Token = "another-access"}}),
            std::runtime_error, "identity");
        UNIT_ASSERT_VALUES_EQUAL(original.Cacher_->Read()->AccessToken.Token, "cached-access");
    }
} // Y_UNIT_TEST_SUITE(TOidcConfigFile)

} // namespace NYdb::NConsoleClient
