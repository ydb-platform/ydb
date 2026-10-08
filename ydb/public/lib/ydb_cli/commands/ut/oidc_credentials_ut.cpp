#include <ydb/public/lib/ydb_cli/commands/ydb_root_common.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>
#include <ydb/public/lib/ydb_cli/common/oidc.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NConsoleClient {
namespace {

class TTestRoot : public TClientCommandRootCommon {
public:
    explicit TTestRoot(const TClientSettings& settings);
    using TClientCommandRootCommon::SetCredentialsGetter;
};

class TTestWorkload : public TWorkloadCommand {
public:
    TTestWorkload();
    void Start(TConfig& config);
    TDriverConfig DriverConfig() const;
};

TClientSettings ClientSettings();
void ConfigureOidc(TClientCommand::TConfig& config);

TTestRoot::TTestRoot(const TClientSettings& settings)
    : TClientCommandRootCommon("ydb", settings)
{}

TTestWorkload::TTestWorkload()
    : TWorkloadCommand("test-workload", {}, "Test workload credentials")
{}

void TTestWorkload::Start(TConfig& config) {
    QueryExecuterType = "generic";
    PrepareForRun(config);
}

TDriverConfig TTestWorkload::DriverConfig() const {
    return Driver->Get().GetConfig();
}

TClientSettings ClientSettings() {
    TClientSettings settings;
    settings.EnableSsl = false;
    settings.UseAccessToken = true;
    settings.UseDefaultTokenFile = false;
    settings.UseIamAuth = false;
    settings.UseExportToYt = false;
    settings.UseStaticCredentials = false;
    settings.MentionUserAccount = false;
    settings.UseOauth2TokenExchange = false;
    settings.YdbDir = "ydb";
    return settings;
}

void ConfigureOidc(TClientCommand::TConfig& config) {
    config.Address = "localhost:1";
    config.Database = "/Root/test";
    config.SkipDiscovery = true;
    config.Oidc.Issuer = "https://issuer.example";
    config.Oidc.ResolvedConfig = NOidc::TOidcConfig{
        .Issuer = "https://issuer.example",
        .FlowConfig = NOidc::TStaticOidcConfig{.AccessToken = "oidc-token", .ExpiresAt = std::nullopt},
    };
}

} // namespace

Y_UNIT_TEST_SUITE(TOidcCommandCredentials) {
    Y_UNIT_TEST(CommonRootSelectsOidcAndPreservesTokenPriority) {
        TTestRoot root(ClientSettings());
        char name[] = "ydb";
        char* args[] = {name};
        TClientCommand::TConfig config(1, args);
        ConfigureOidc(config);
        root.SetCredentialsGetter(config);
        const auto factory = config.CredentialsGetter(config);
        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer oidc-token");
        config.SecurityToken = "legacy-token";
        UNIT_ASSERT_VALUES_EQUAL(config.CredentialsGetter(config)->CreateProvider()->GetAuthInfo(), "legacy-token");
    }

    Y_UNIT_TEST(WorkloadDriversReusePreparedCredentialsFactory) {
        char name[] = "ydb";
        char* args[] = {name};
        TClientCommand::TConfig config(1, args);
        ConfigureOidc(config);
        size_t factoryCreations = 0;
        config.CredentialsGetter = [&factoryCreations](const TClientCommand::TConfig& config) {
            ++factoryCreations;
            return CreateCliOidcCredentialsProviderFactory(config.Oidc);
        };
        const auto factory = config.GetSingletonCredentialsProviderFactory();
        const auto provider = factory->CreateProvider();
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfoAsync().GetValueSync(), "Bearer oidc-token");
        for (size_t i = 0; i < 2; ++i) {
            TTestWorkload workload;
            workload.Start(config);
            UNIT_ASSERT_VALUES_EQUAL(workload.DriverConfig().GetEndpoint(), config.Address);
            UNIT_ASSERT_VALUES_EQUAL(workload.DriverConfig().GetDatabase(), config.Database);
            UNIT_ASSERT_VALUES_EQUAL(factoryCreations, 1);
            UNIT_ASSERT(factory == config.GetSingletonCredentialsProviderFactory());
            UNIT_ASSERT(provider == factory->CreateProvider());
        }
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer oidc-token");
    }
}

} // namespace NYdb::NConsoleClient
