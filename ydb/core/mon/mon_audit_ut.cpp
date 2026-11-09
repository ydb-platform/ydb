#include <ydb/core/mon/mon.h>
#include <ydb/core/mon/ut_utils/ut_utils.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/audit_helpers/audit_helper.h>
#include <ydb/core/testlib/test_client.h>

#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/security/util.h>

namespace NMonitoring::NTests {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::Tests;

namespace {

const TString& ExpectedSanitizedToken() {
    static const TString token = NKikimr::SanitizeTicket(VALID_TOKEN);
    return token;
}

void AssertMonitoringAuditIdentity(const std::string& line, const TString& subject, const TString& sanitizedToken) {
    UNIT_ASSERT_STRING_CONTAINS(line, "component=monitoring");
    UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "subject=" << subject);
    UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "sanitized_token=" << sanitizedToken);
    UNIT_ASSERT_VALUES_EQUAL(line.find("subject="), line.rfind("subject="));
    UNIT_ASSERT_VALUES_EQUAL(line.find("sanitized_token="), line.rfind("sanitized_token="));
    UNIT_ASSERT(line.find(VALID_TOKEN) == std::string::npos);
}

class THttpMonAuditTestEnv {
public:
    explicit THttpMonAuditTestEnv(TVector<TString> ticketParserGroupSIDs = {"ydb.clusters.monitor@as"})
        : Port(PortManager.GetPort(2134))
        , GrpcPort(PortManager.GetPort(2135))
        , MonPort(PortManager.GetPort(8765))
        , Settings(Port)
        , TicketParserGroupSIDs(std::move(ticketParserGroupSIDs))
    {
        Settings.InitKikimrRunConfig()
            .SetNodeCount(1)
            .SetUseRealThreads(true)
            .SetDomainName("Root")
            .SetUseSectorMap(true)
            .SetMonitoringPortOffset(MonPort, true);

        auto& securityConfig = *Settings.AppConfig->MutableDomainsConfig()->MutableSecurityConfig();
        securityConfig.SetEnforceUserTokenCheckRequirement(true);
        securityConfig.MutableMonitoringAllowedSIDs()->Add("ydb.clusters.monitor@as");

        AuditLogLines = std::make_shared<std::vector<std::string>>();
        Settings.SetAuditLogBackendLines(AuditLogLines);

        auto* logClassConfig = Settings.AppConfig->MutableAuditConfig()->AddLogClassConfig();
        logClassConfig->SetLogClass(NKikimrConfig::TAuditConfig::TLogClassConfig::ClusterAdmin);
        logClassConfig->SetEnableLogging(true);
        logClassConfig->AddLogPhase(NKikimrConfig::TAuditConfig::TLogClassConfig::Received);
        logClassConfig->AddLogPhase(NKikimrConfig::TAuditConfig::TLogClassConfig::Completed);

        Settings.CreateTicketParser = [&](const TTicketParserSettings&) -> NActors::IActor* {
            return new TFakeTicketParserActor(TicketParserGroupSIDs);
        };

        Server = std::make_unique<TServer>(Settings);
        Server->EnableGRpc(GrpcPort);
        Client = std::make_unique<TClient>(Settings);
        Client->InitRootScheme();
        GrantConnect(*Client);

        auto* runtime = Server->GetRuntime();
        runtime->GetAppData().Mon->RegisterActorPage({
            .RelPath = "audit-no-auth",
            .ActorSystem = runtime->GetActorSystem(0),
            .ActorId = runtime->Register(new TTestActorPage()),
            .AuthMode = TMon::EAuthMode::Disabled,
        });

        HttpClient = std::make_unique<TKeepAliveHttpClient>("localhost", MonPort);
    }

    TKeepAliveHttpClient::THeaders MakeAuthHeaders(const TString& token = VALID_TOKEN) const {
        TKeepAliveHttpClient::THeaders headers;
        headers[AUTHORIZATION_HEADER] = token;
        return headers;
    }

    TKeepAliveHttpClient& GetHttpClient() const {
        return *HttpClient;
    }

    void AssertAuditIdentity(const TString& completedReason, const TString& subject, const TString& sanitizedToken,
            const TString& rawToken = VALID_TOKEN) const {
        for (const TString& reason : {TString("reason=Execute"), completedReason}) {
            std::string line;
            UNIT_ASSERT_C(WaitAndFindAuditLine(*AuditLogLines, reason.c_str(), line),
                "Audit log line did not appear: " << reason);
            AssertMonitoringAuditIdentity(line, subject, sanitizedToken);
            UNIT_ASSERT(line.find(rawToken.c_str()) == std::string::npos);
        }
    }

private:
    TPortManager PortManager;
    ui16 Port;
    ui16 GrpcPort;
    ui16 MonPort;
    TServerSettings Settings;
    std::unique_ptr<TServer> Server;
    std::unique_ptr<TClient> Client;
    std::unique_ptr<TKeepAliveHttpClient> HttpClient;
    TVector<TString> TicketParserGroupSIDs;
    std::shared_ptr<std::vector<std::string>> AuditLogLines;
};

} // namespace

Y_UNIT_TEST_SUITE(MonitoringAudit) {
    Y_UNIT_TEST(AuditLogContainsSubjectAndSanitizedToken) {
        THttpMonAuditTestEnv env;

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet("/trace", &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        env.AssertAuditIdentity("reason=200 Ok", "username", ExpectedSanitizedToken());
    }

    Y_UNIT_TEST(AuditLogContainsIdentityWhenAccessDenied) {
        THttpMonAuditTestEnv env({"not-allowed"});

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet("/trace", &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_FORBIDDEN);

        env.AssertAuditIdentity("reason=403 Forbidden", "username", ExpectedSanitizedToken());
    }

    Y_UNIT_TEST(AuditLogContainsEmptyIdentityWithoutCredentials) {
        THttpMonAuditTestEnv env;

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet("/trace", &responseStream);
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_UNAUTHORIZED);

        env.AssertAuditIdentity("reason=401 Unauthorized", "{none}", "{none}");
    }

    Y_UNIT_TEST(AuditLogContainsEmptyIdentityForInvalidCredentials) {
        THttpMonAuditTestEnv env;

        TStringStream responseStream;
        const TString invalidToken = "Bearer invalid";
        const auto status = env.GetHttpClient().DoGet("/trace", &responseStream, env.MakeAuthHeaders(invalidToken));
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_FORBIDDEN);

        env.AssertAuditIdentity("reason=403 Forbidden", "{none}", "{none}", invalidToken);
    }

    Y_UNIT_TEST(AuditLogContainsEmptyIdentityWithoutAuthorizer) {
        THttpMonAuditTestEnv env;

        TStringStream responseStream;
        const auto status = env.GetHttpClient().DoGet("/audit-no-auth", &responseStream, env.MakeAuthHeaders());
        UNIT_ASSERT_VALUES_EQUAL(status, HTTP_OK);

        env.AssertAuditIdentity("reason=200 Ok", "{none}", "{none}");
    }
}

} // namespace NMonitoring::NTests
