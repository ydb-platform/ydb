#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/list.h>
#include <util/system/env.h>
#include <util/system/shellcommand.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <memory>
#include <string>

namespace {

using namespace NYdb;
using namespace NYdb::NOidc;

const TDuration RequestTimeout = TDuration::Seconds(10);
const TDuration WaitTimeout = TDuration::Seconds(15);

enum class EFlow {
    Static,
    Client,
    Device,
};

struct TGrantCounts {
    unsigned long long Client;
    unsigned long long DeviceVerification;
    unsigned long long DeviceToken;
    unsigned long long Refresh;
};

class TDeviceAcceptor final : public IAuthAcceptor {
public:
    void Accept(const TDeviceAuthInfo& info) override;
    unsigned GetCount() const;

private:
    std::atomic<unsigned> Count = 0;
};

TString RunHelper(const TList<TString>& arguments);
TString ExpectedSid(EFlow flow);
TGrantCounts ReadGrantCounts();
TOidcConfig MakeOidcConfig(EFlow flow, const std::shared_ptr<TDeviceAcceptor>& acceptor);
TDriverConfig MakeDriverConfig(const TCredentialsProviderFactoryPtr& factory, EDiscoveryMode discovery);
void AssertAuthenticated(TDriver& driver, EFlow flow);
void AssertSingleGrant(EFlow flow, const TGrantCounts& before, const TDeviceAcceptor& acceptor);
void RunDriverLifecycle(EFlow flow, EDiscoveryMode discovery);
void RunStandaloneProvider(EFlow flow);
void RunInvalidStaticToken(EDiscoveryMode discovery);

TString RunHelper(const TList<TString>& arguments) {
    const auto binary = GetEnv("OIDC_HELPER_BINARY");
    UNIT_ASSERT_C(!binary.empty(), "OIDC_HELPER_BINARY is not set");
    TShellCommandOptions options;
    options.SetUseShell(false).SetAsync(true).SetLatency(10).SetCloseAllFdsOnExec(true);
    TShellCommand command(binary, arguments, options);
    command.Run();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (command.GetStatus() == TShellCommand::SHELL_RUNNING && std::chrono::steady_clock::now() < deadline) {
        Sleep(TDuration::MilliSeconds(10));
    }
    const bool timedOut = command.GetStatus() == TShellCommand::SHELL_RUNNING;
    if (timedOut) {
        command.Terminate(SIGKILL);
    }
    command.Wait();
    UNIT_ASSERT_C(!timedOut, "Keycloak helper timed out: " << command.GetError());
    UNIT_ASSERT_C(command.GetStatus() == TShellCommand::SHELL_FINISHED,
        "Keycloak helper failed: " << command.GetError() << command.GetInternalError());
    return command.GetOutput();
}

TString ExpectedSid(EFlow flow) {
    switch (flow) {
        case EFlow::Static:
            return GetEnv("OIDC_STATIC_SID");
        case EFlow::Client:
            return GetEnv("OIDC_CLIENT_SID");
        case EFlow::Device:
            return GetEnv("OIDC_DEVICE_SID");
    }
    UNIT_FAIL("Unknown OIDC flow");
    return {};
}

TGrantCounts ReadGrantCounts() {
    NJson::TJsonValue stats;
    UNIT_ASSERT(NJson::ReadJsonTree(RunHelper({"--stats"}), &stats));
    TGrantCounts result;
    UNIT_ASSERT(stats["client_credentials"].GetUInteger(&result.Client));
    UNIT_ASSERT(stats["device_verification"].GetUInteger(&result.DeviceVerification));
    UNIT_ASSERT(stats["device_token"].GetUInteger(&result.DeviceToken));
    UNIT_ASSERT(stats["refresh_token"].GetUInteger(&result.Refresh));
    return result;
}

void TDeviceAcceptor::Accept(const TDeviceAuthInfo& info) {
    UNIT_ASSERT(!info.UserCode.empty());
    UNIT_ASSERT(info.VerificationUrlComplete.has_value());
    const std::string issuer(GetEnv("OIDC_ISSUER"));
    UNIT_ASSERT(info.VerificationUrlComplete->starts_with(issuer + "/"));
    RunHelper({"--approve", TString(*info.VerificationUrlComplete)});
    Count.fetch_add(1);
}

unsigned TDeviceAcceptor::GetCount() const {
    return Count.load();
}

TOidcConfig MakeOidcConfig(EFlow flow, const std::shared_ptr<TDeviceAcceptor>& acceptor) {
    TOidcConfig config;
    config.Issuer = std::string(GetEnv("OIDC_ISSUER"));
    switch (flow) {
        case EFlow::Static:
            config.FlowConfig = TStaticOidcConfig{std::string(GetEnv("OIDC_ACCESS_TOKEN")), std::nullopt};
            break;
        case EFlow::Client:
            config.FlowConfig = TClientOidcConfig{
                std::string(GetEnv("OIDC_CLIENT_ID")), std::string(GetEnv("OIDC_CLIENT_SECRET")), {}};
            break;
        case EFlow::Device:
            config.FlowConfig = TDeviceOidcConfig{std::string(GetEnv("OIDC_DEVICE_CLIENT_ID")), {}};
            config.Acceptor(acceptor);
            break;
    }
    return config;
}

TDriverConfig MakeDriverConfig(const TCredentialsProviderFactoryPtr& factory, EDiscoveryMode discovery) {
    TDriverConfig config;
    config.SetEndpoint(std::string(GetEnv("YDB_ENDPOINT")));
    config.SetDatabase(std::string(GetEnv("YDB_DATABASE")));
    config.SetDiscoveryMode(discovery);
    config.SetCredentialsProviderFactory(factory);
    return config;
}

void AssertAuthenticated(TDriver& driver, EFlow flow) {
    NTable::TTableClient table(driver);
    auto session = table.CreateSession(NTable::TCreateSessionSettings().ClientTimeout(RequestTimeout));
    UNIT_ASSERT_C(session.Wait(WaitTimeout), "Authenticated table session creation timed out");
    const auto result = session.GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    NDiscovery::TDiscoveryClient discovery(driver);
    auto identity = discovery.WhoAmI(NDiscovery::TWhoAmISettings().ClientTimeout(RequestTimeout));
    UNIT_ASSERT_C(identity.Wait(WaitTimeout), "WhoAmI timed out");
    const auto who = identity.GetValueSync();
    UNIT_ASSERT_C(who.IsSuccess(), who.GetIssues().ToString());
    const auto expectedSid = ExpectedSid(flow);
    UNIT_ASSERT_C(!expectedSid.empty(), "Expected OIDC SID is not set");
    UNIT_ASSERT_VALUES_EQUAL(who.GetUserName(), expectedSid);
}

void AssertSingleGrant(EFlow flow, const TGrantCounts& before, const TDeviceAcceptor& acceptor) {
    const auto after = ReadGrantCounts();
    UNIT_ASSERT_VALUES_EQUAL(after.Client - before.Client, flow == EFlow::Client ? 1 : 0);
    UNIT_ASSERT_VALUES_EQUAL(after.DeviceVerification - before.DeviceVerification, flow == EFlow::Device ? 1 : 0);
    UNIT_ASSERT_VALUES_EQUAL(after.DeviceToken - before.DeviceToken, flow == EFlow::Device ? 1 : 0);
    UNIT_ASSERT_VALUES_EQUAL(after.Refresh, before.Refresh);
    UNIT_ASSERT_VALUES_EQUAL(acceptor.GetCount(), flow == EFlow::Device ? 1 : 0);
}

void RunDriverLifecycle(EFlow flow, EDiscoveryMode discovery) {
    const auto before = ReadGrantCounts();
    auto acceptor = std::make_shared<TDeviceAcceptor>();
    const auto factory = CreateOidcProviderFactory(MakeOidcConfig(flow, acceptor));
    {
        TDriver first(MakeDriverConfig(factory, discovery));
        AssertAuthenticated(first, flow);
        first.Stop(true);
    }
    // The factory retains authentication after the original driver's facility dies.
    {
        auto first = std::make_unique<TDriver>(MakeDriverConfig(factory, discovery));
        TDriver second(MakeDriverConfig(factory, discovery));
        AssertAuthenticated(*first, flow);
        AssertAuthenticated(second, flow);
        first->Stop(true);
        first.reset();
        // Stopping one driver must not cancel the other driver's provider adapter.
        AssertAuthenticated(second, flow);
        second.Stop(true);
    }
    {
        TDriver recreated(MakeDriverConfig(factory, discovery));
        AssertAuthenticated(recreated, flow);
        recreated.Stop(true);
    }
    AssertSingleGrant(flow, before, *acceptor);
}

void RunStandaloneProvider(EFlow flow) {
    const auto before = ReadGrantCounts();
    auto acceptor = std::make_shared<TDeviceAcceptor>();
    const auto factory = CreateOidcProviderFactory(MakeOidcConfig(flow, acceptor));
    const auto provider = factory->CreateProvider();
    UNIT_ASSERT(provider != nullptr);
    UNIT_ASSERT(provider == factory->CreateProvider());
    auto auth = provider->GetAuthInfoAsync();
    UNIT_ASSERT_C(auth.Wait(WaitTimeout), "Standalone authentication timed out");
    const auto ticket = auth.GetValueSync();
    UNIT_ASSERT(ticket.starts_with("Bearer "));
    UNIT_ASSERT(ticket.size() > std::string("Bearer ").size());
    UNIT_ASSERT(provider->IsValid());
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), ticket);
    // The no-argument provider and facility-bound driver share the same grant.
    TDriver driver(MakeDriverConfig(factory, EDiscoveryMode::Async));
    AssertAuthenticated(driver, flow);
    driver.Stop(true);
    AssertSingleGrant(flow, before, *acceptor);
}

void RunInvalidStaticToken(EDiscoveryMode discoveryMode) {
    auto acceptor = std::make_shared<TDeviceAcceptor>();
    auto config = MakeOidcConfig(EFlow::Static, acceptor);
    std::get<TStaticOidcConfig>(config.FlowConfig).AccessToken = std::string(GetEnv("OIDC_BAD_TOKEN"));
    TDriver driver(MakeDriverConfig(CreateOidcProviderFactory(config), discoveryMode));
    NDiscovery::TDiscoveryClient discovery(driver);
    auto identity = discovery.WhoAmI(NDiscovery::TWhoAmISettings().ClientTimeout(RequestTimeout));
    UNIT_ASSERT_C(identity.Wait(WaitTimeout), "Invalid JWT rejection timed out");
    const auto result = identity.GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::CLIENT_UNAUTHENTICATED, result.GetIssues().ToString());
    driver.Stop(true);
}

} // namespace

Y_UNIT_TEST_SUITE(OidcSdkCredentials) {
    Y_UNIT_TEST(StaticSyncDiscovery) {
        RunDriverLifecycle(EFlow::Static, EDiscoveryMode::Sync);
    }

    Y_UNIT_TEST(StaticAsyncDiscovery) {
        RunDriverLifecycle(EFlow::Static, EDiscoveryMode::Async);
    }

    Y_UNIT_TEST(ClientSyncDiscovery) {
        RunDriverLifecycle(EFlow::Client, EDiscoveryMode::Sync);
    }

    Y_UNIT_TEST(ClientAsyncDiscovery) {
        RunDriverLifecycle(EFlow::Client, EDiscoveryMode::Async);
    }

    Y_UNIT_TEST(DeviceSyncDiscovery) {
        RunDriverLifecycle(EFlow::Device, EDiscoveryMode::Sync);
    }

    Y_UNIT_TEST(DeviceAsyncDiscovery) {
        RunDriverLifecycle(EFlow::Device, EDiscoveryMode::Async);
    }

    Y_UNIT_TEST(StaticStandaloneProvider) {
        RunStandaloneProvider(EFlow::Static);
    }

    Y_UNIT_TEST(ClientStandaloneProvider) {
        RunStandaloneProvider(EFlow::Client);
    }

    Y_UNIT_TEST(DeviceStandaloneProvider) {
        RunStandaloneProvider(EFlow::Device);
    }

    Y_UNIT_TEST(InvalidStaticJwtSyncDiscovery) {
        RunInvalidStaticToken(EDiscoveryMode::Sync);
    }

    Y_UNIT_TEST(InvalidStaticJwtAsyncDiscovery) {
        RunInvalidStaticToken(EDiscoveryMode::Async);
    }

    Y_UNIT_TEST(WrongClientSecretIsRejected) {
        auto acceptor = std::make_shared<TDeviceAcceptor>();
        auto config = MakeOidcConfig(EFlow::Client, acceptor);
        std::get<TClientOidcConfig>(config.FlowConfig).ClientSecret = "wrong-secret";
        TDriver driver(MakeDriverConfig(CreateOidcProviderFactory(config), EDiscoveryMode::Async));
        NDiscovery::TDiscoveryClient discovery(driver);
        auto identity = discovery.WhoAmI(NDiscovery::TWhoAmISettings().ClientTimeout(RequestTimeout));
        UNIT_ASSERT_C(identity.Wait(WaitTimeout), "Invalid client secret rejection timed out");
        const auto result = identity.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::CLIENT_UNAUTHENTICATED, result.GetIssues().ToString());
        driver.Stop(true);
    }
}
