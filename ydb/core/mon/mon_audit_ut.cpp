#include <ydb/core/mon/mon.h>
#include <ydb/core/mon/ut_utils/ut_utils.h>
#include <ydb/core/testlib/audit_helpers/audit_helper.h>
#include <ydb/core/testlib/test_client.h>

#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <util/system/thread.h>

namespace NMonitoring::NTests {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::Tests;

namespace {

std::string WaitAndFindAuditLine(const std::vector<std::string>& auditLines, const std::string& substr) {
    for (size_t attempt = 0; attempt < 50; ++attempt) {
        auto found = std::find_if(auditLines.rbegin(), auditLines.rend(), [&](const auto& line) {
            return line.contains(substr);
        });
        if (found != auditLines.rend()) {
            Cerr << "AUDIT LOG checked line:" << Endl << "    " << *found << Endl;
            return *found;
        }
        Sleep(TDuration::MilliSeconds(100));
    }
    return FindAuditLine(auditLines, substr);
}

void AssertMonitoringAuditHasUser(const std::string& line) {
    UNIT_ASSERT_STRING_CONTAINS(line, "component=monitoring");
    UNIT_ASSERT_STRING_CONTAINS(line, "subject=username");
    UNIT_ASSERT_STRING_CONTAINS(line, "sanitized_token=");
    UNIT_ASSERT(line.find("sanitized_token={none}") == std::string::npos);
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
        logClassConfig->AddExcludeAccountType(NKikimrConfig::TAuditConfig::TLogClassConfig::Anonymous);
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

    const std::vector<std::string>& GetAuditLogLines() const {
        return *AuditLogLines;
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

        const auto& auditLines = env.GetAuditLogLines();
        AssertMonitoringAuditHasUser(WaitAndFindAuditLine(auditLines, "reason=Execute"));
        AssertMonitoringAuditHasUser(WaitAndFindAuditLine(auditLines, "reason=200 Ok"));
    }
}

} // namespace NMonitoring::NTests
