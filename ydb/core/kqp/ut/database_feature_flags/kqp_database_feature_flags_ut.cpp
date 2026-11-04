#include <ydb/core/audit/audit_log_service.h>
#include <ydb/core/base/feature_flags_service.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/feature_flags_configurator.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/yaml_config/yaml_config.h>

#include <util/system/thread.h>
#include <util/string/split.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NConsole;

constexpr TStringBuf Database = "/Root/Test";
constexpr TStringBuf Flag = "ENABLE_CLIENT_MODIFICATIONS";

TTestExtEnv::TEnvSettings EnvSettings() {
    TTestExtEnv::TEnvSettings settings;
    settings.FeatureFlags.SetEnableAlterDatabase(true);
    settings.FeatureFlags.SetEnableDatabaseAdmin(true);
    settings.FeatureFlags.SetDatabaseYamlConfigAllowed(true);
    return settings;
}

class TDatabaseFlagsEnv {
public:
    TPortManager Ports;
    TCapturedLog Audit{true};
    TTestExtEnv Env{EnvSettings()};
    TString TenantEndpoint;

    TDatabaseFlagsEnv() {
        Env.CreateDatabase("Test");
        const auto tenantPort = Ports.GetPort();
        Env.GetServer().EnableGRpc(tenantPort, 1, TString(Database));
        TenantEndpoint = TStringBuilder() << "localhost:" << tenantPort;
        auto& client = Env.GetClient();
        for (const auto* user : {"root@builtin", "owner@builtin", "other@builtin"}) {
            client.TestGrant("/", "Root", user, NACLib::EAccessRights::GenericFull);
        }
        client.TestModifyOwner("/Root", "Test", "owner@builtin");
        Tests::TClient::RefreshPathCache(Env.GetServer().GetRuntime(), TString(Database));

        auto& runtime = Runtime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).AdministrationAllowedSIDs = {"root@builtin"};
        }
        client.SetSecurityToken("root@builtin");

        auto request = MakeHolder<TEvConsole::TEvReplaceYamlConfigRequest>();
        request->Record.SetUserToken(NACLib::TUserToken("root@builtin", {}).SerializeAsString());
        request->Record.MutableRequest()->set_config(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  yaml_config_enabled: true
  feature_flags:
    enable_alter_database: true
    enable_database_admin: true
    database_yaml_config_allowed: true
    enable_client_modifications: true
    enable_background_compaction: false
)");
        const auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(MakeConsoleID(), sender, request.Release(), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto [response, error] = runtime.GrabEdgeEventsRethrow<TEvConsole::TEvReplaceYamlConfigResponse,
            TEvConsole::TEvGenericError>(handle, TDuration::Seconds(30));
        UNIT_ASSERT_C(response, (error ? error->Record.DebugString() : "No config replacement response"));

        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.RegisterService(MakeFeatureFlagsServiceID(),
                runtime.Register(CreateFeatureFlagsConfigurator(), node), node);
        }
        WaitFlag(true);
    }

    TTestActorRuntime& Runtime() {
        return *Env.GetServer().GetRuntime();
    }

    NQuery::TQueryClient QueryClient(const TString& user = "root@builtin") {
        return NQuery::TQueryClient(Env.GetDriver(), NQuery::TClientSettings().AuthToken(user));
    }

    void EnableAudit() {
        auto& runtime = Runtime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            NAudit::TAuditLogBackends backends;
            backends[NKikimrConfig::TAuditConfig::TXT].push_back(Audit.CreateBackend());
            auto writer = NAudit::CreateAuditWriter(std::move(backends));
            runtime.RegisterService(NAudit::MakeAuditServiceID(), runtime.Register(writer.release(), node), node);
        }
    }

    TString WaitAudit(const TString& requestId) {
        const TString needle = "request_id=" + requestId;
        const auto deadline = TInstant::Now() + TDuration::Seconds(10);
        do {
            const auto snapshot = Audit.Snapshot();
            for (const auto& part : StringSplitter(snapshot).Split('\n')) {
                const TStringBuf line = part.Token();
                if (line.Contains(needle) && line.Contains("operation=ALTER DATABASE FEATURE FLAGS")) {
                    return TString(line);
                }
            }
            Sleep(TDuration::MilliSeconds(10));
        } while (TInstant::Now() < deadline);
        UNIT_FAIL("No feature flag audit record for " << requestId << ": " << Audit.Snapshot());
        return {};
    }

    TString Config() {
        auto& runtime = Runtime();
        const auto sender = runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvConsole::TEvGetAllConfigsRequest>();
        request->Record.SetUserToken(NACLib::TUserToken("root@builtin", {}).SerializeAsString());
        request->Record.SetIngressDatabase(TString(Database));
        runtime.SendToPipe(MakeConsoleID(), sender, request.Release(), 0, GetPipeConfigWithRetries());
        auto response = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetAllConfigsResponse>(sender, TDuration::Seconds(30));
        UNIT_ASSERT(response);
        const auto& configs = response->Get()->Record.GetResponse();
        return configs.config_size() ? TString(configs.config(0)) : TString();
    }

    void WaitFlag(bool value) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        while (TInstant::Now() < deadline) {
            const auto& flags = Runtime().GetAppData(1).FeatureFlags;
            if (flags.GetEnableClientModifications() == value && !flags.GetEnableBackgroundCompaction()
                && !Runtime().GetAppData(0).FeatureFlags.GetEnableBackgroundCompaction())
            {
                return;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_FAIL("Database feature flags did not propagate");
    }
};

TString SetFlag(bool value) {
    return TStringBuilder() << "ALTER DATABASE `" << Database << "` SET (" << Flag
        << " = " << (value ? "true" : "false") << ");";
}

TString ResetFlag() {
    return TStringBuilder() << "ALTER DATABASE `" << Database << "` RESET (" << Flag << ");";
}

void CheckSuccess(const TStatus& result) {
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpDatabaseFeatureFlags) {
    Y_UNIT_TEST(SetResetAndPermissions) {
        TDatabaseFlagsEnv env;
        auto admin = env.QueryClient();
        auto owner = env.QueryClient("owner@builtin");
        auto other = env.QueryClient("other@builtin");

        CheckSuccess(owner.ExecuteQuery(SetFlag(false), NQuery::TTxControl::NoTx()).GetValueSync());
        env.WaitFlag(false);
        const TString disabledConfig = env.Config();
        UNIT_ASSERT_STRING_CONTAINS(disabledConfig, "enable_client_modifications: false");
        UNIT_ASSERT_STRING_CONTAINS(disabledConfig, "!inherit");
        UNIT_ASSERT(env.Runtime().GetAppData(0).FeatureFlags.GetEnableClientModifications());

        auto denied = other.ExecuteQuery(SetFlag(true), NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(denied.GetStatus(), EStatus::UNAUTHORIZED, denied.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(env.Config(), disabledConfig);

        // A repeated SET is a no-op, including the stored configuration revision.
        CheckSuccess(owner.ExecuteQuery(SetFlag(false), NQuery::TTxControl::NoTx()).GetValueSync());
        UNIT_ASSERT_VALUES_EQUAL(env.Config(), disabledConfig);

        CheckSuccess(admin.ExecuteQuery(SetFlag(true), NQuery::TTxControl::NoTx()).GetValueSync());
        env.WaitFlag(true);
        CheckSuccess(owner.ExecuteQuery(SetFlag(false), NQuery::TTxControl::NoTx()).GetValueSync());
        env.WaitFlag(false);
        CheckSuccess(owner.ExecuteQuery(ResetFlag(), NQuery::TTxControl::NoTx()).GetValueSync());
        env.WaitFlag(true);
        UNIT_ASSERT(!env.Config().Contains("enable_client_modifications"));
    }

    Y_UNIT_TEST(ReenableWritesFromDatabaseSession) {
        TDatabaseFlagsEnv env;
        TDriver driver(TDriverConfig().SetEndpoint(env.TenantEndpoint)
            .SetDatabase(TString(Database)).SetAuthToken("owner@builtin"));
        NQuery::TQueryClient client(driver, NQuery::TClientSettings().DiscoveryMode(EDiscoveryMode::Off));
        auto sessionResult = client.GetSession().GetValueSync();
        CheckSuccess(sessionResult);
        auto session = sessionResult.GetSession();

        CheckSuccess(session.ExecuteQuery("CREATE TABLE Writes (Key Uint64, PRIMARY KEY(Key))",
            NQuery::TTxControl::NoTx()).GetValueSync());
        CheckSuccess(session.ExecuteQuery(SetFlag(false), NQuery::TTxControl::NoTx()).GetValueSync());
        env.WaitFlag(false);

        auto write = session.ExecuteQuery("UPSERT INTO Writes (Key) VALUES (1ul)",
            NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(write.GetStatus(), EStatus::PRECONDITION_FAILED, write.GetIssues().ToString());

        CheckSuccess(session.ExecuteQuery(SetFlag(true), NQuery::TTxControl::NoTx()).GetValueSync());
        env.WaitFlag(true);
        CheckSuccess(session.ExecuteQuery("UPSERT INTO Writes (Key) VALUES (1ul)",
            NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync());
    }

    Y_UNIT_TEST(ExplainAndTransactionsDoNotModifyConfig) {
        TDatabaseFlagsEnv env;
        auto client = env.QueryClient();
        const TString before = env.Config();
        auto explain = client.ExecuteQuery(SetFlag(false), NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
        CheckSuccess(explain);
        UNIT_ASSERT_VALUES_EQUAL(env.Config(), before);
        auto transactional = client.ExecuteQuery(SetFlag(false), NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(!transactional.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(env.Config(), before);
    }

    Y_UNIT_TEST(SchemeQueryAndLegacyPermissions) {
        TDatabaseFlagsEnv env;
        NYdb::NTable::TTableClient tables(env.Env.GetDriver(), NYdb::NTable::TClientSettings().AuthToken("owner@builtin"));
        auto sessionResult = tables.CreateSession().GetValueSync();
        CheckSuccess(sessionResult);
        auto session = sessionResult.GetSession();
        CheckSuccess(session.ExecuteSchemeQuery(SetFlag(false)).GetValueSync());
        env.WaitFlag(false);
        CheckSuccess(session.ExecuteSchemeQuery(ResetFlag()).GetValueSync());
        env.WaitFlag(true);

        auto result = session.ExecuteSchemeQuery("ALTER DATABASE `/Root/Test` SET (MAX_SHARDS = 10)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAUTHORIZED, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DatabaseAdminGateAndRootTarget) {
        TDatabaseFlagsEnv env;
        auto owner = env.QueryClient("owner@builtin");
        auto admin = env.QueryClient();
        env.Runtime().GetAppData(0).FeatureFlags.SetEnableDatabaseAdmin(false);
        auto denied = owner.ExecuteQuery(SetFlag(false), NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(denied.GetStatus(), EStatus::UNAUTHORIZED, denied.GetIssues().ToString());
        UNIT_ASSERT(env.Config().empty());
        CheckSuccess(admin.ExecuteQuery(SetFlag(false), NQuery::TTxControl::NoTx()).GetValueSync());
        env.WaitFlag(false);

        auto root = admin.ExecuteQuery("ALTER DATABASE `/Root` SET (ENABLE_CLIENT_MODIFICATIONS = false)",
            NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!root.IsSuccess());
        UNIT_ASSERT(env.Runtime().GetAppData(0).FeatureFlags.GetEnableClientModifications());
    }

    Y_UNIT_TEST(AuditOutcomesAndRequestCorrelation) {
        TDatabaseFlagsEnv env;
        env.EnableAudit();
        auto owner = env.QueryClient("owner@builtin");
        auto other = env.QueryClient("other@builtin");

        CheckSuccess(owner.ExecuteQuery(SetFlag(false), NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().TraceId("flags-query-success")).GetValueSync());
        const auto success = env.WaitAudit("flags-query-success");
        UNIT_ASSERT_STRING_CONTAINS(success, "subject=owner@builtin");
        UNIT_ASSERT_STRING_CONTAINS(success, "database=/Root/Test");
        UNIT_ASSERT_STRING_CONTAINS(success, "status=SUCCESS");
        UNIT_ASSERT_STRING_CONTAINS(success, "old_overrides=ENABLE_CLIENT_MODIFICATIONS=INHERIT");
        UNIT_ASSERT_STRING_CONTAINS(success, "new_overrides=ENABLE_CLIENT_MODIFICATIONS=false");
        UNIT_ASSERT_STRING_CONTAINS(success, "config_version=1");
        UNIT_ASSERT_STRING_CONTAINS(success, "no_op=false");

        CheckSuccess(owner.ExecuteQuery(SetFlag(false), NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().TraceId("flags-query-noop")).GetValueSync());
        const auto noOp = env.WaitAudit("flags-query-noop");
        UNIT_ASSERT_STRING_CONTAINS(noOp, "config_version=1");
        UNIT_ASSERT_STRING_CONTAINS(noOp, "no_op=true");

        auto denied = other.ExecuteQuery(SetFlag(true), NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().TraceId("flags-query-denied")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(denied.GetStatus(), EStatus::UNAUTHORIZED);
        UNIT_ASSERT_STRING_CONTAINS(env.WaitAudit("flags-query-denied"), "detailed_status=UNAUTHORIZED");

        NYdb::NTable::TTableClient tables(env.Env.GetDriver(), NYdb::NTable::TClientSettings().AuthToken("owner@builtin"));
        auto sessionResult = tables.CreateSession().GetValueSync();
        CheckSuccess(sessionResult);
        auto session = sessionResult.GetSession();
        CheckSuccess(session.ExecuteSchemeQuery(ResetFlag(),
            NYdb::NTable::TExecSchemeQuerySettings().TraceId("flags-scheme-success")).GetValueSync());
        const auto schemeSuccess = env.WaitAudit("flags-scheme-success");
        UNIT_ASSERT_STRING_CONTAINS(schemeSuccess, "status=SUCCESS");
        UNIT_ASSERT_STRING_CONTAINS(schemeSuccess, "new_overrides=ENABLE_CLIENT_MODIFICATIONS=INHERIT");

        auto schemeFailure = session.ExecuteSchemeQuery(
            "ALTER DATABASE `/Root` SET (ENABLE_CLIENT_MODIFICATIONS = false)",
            NYdb::NTable::TExecSchemeQuerySettings().TraceId("flags-scheme-failed")).GetValueSync();
        UNIT_ASSERT(!schemeFailure.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(env.WaitAudit("flags-scheme-failed"), "status=ERROR");

        for (const auto* clause : {
            "SET (ENABLE_CLIENT_MODIFICATIONS = false, ENABLE_CLIENT_MODIFICATIONS = true)",
            "RESET (ENABLE_CLIENT_MODIFICATIONS, ENABLE_CLIENT_MODIFICATIONS)",
        }) {
            const TString requestId = TStringBuilder() << "flags-duplicate-" << clause[0];
            auto duplicate = owner.ExecuteQuery(TStringBuilder() << "ALTER DATABASE `/Root/Test` " << clause,
                NQuery::TTxControl::NoTx(), NQuery::TExecuteQuerySettings().TraceId(requestId)).GetValueSync();
            UNIT_ASSERT(!duplicate.IsSuccess());
            const auto audit = env.WaitAudit(requestId);
            UNIT_ASSERT_STRING_CONTAINS(audit, "phase=VALIDATION");
            UNIT_ASSERT_STRING_CONTAINS(audit, "subject=owner@builtin");
            UNIT_ASSERT_STRING_CONTAINS(audit, "status=ERROR");

            const TString schemeRequestId = TStringBuilder() << "flags-scheme-duplicate-" << clause[0];
            auto schemeDuplicate = session.ExecuteSchemeQuery(
                TStringBuilder() << "ALTER DATABASE `/Root/Test` " << clause,
                NYdb::NTable::TExecSchemeQuerySettings().TraceId(schemeRequestId)).GetValueSync();
            UNIT_ASSERT(!schemeDuplicate.IsSuccess());
            const auto schemeAudit = env.WaitAudit(schemeRequestId);
            UNIT_ASSERT_STRING_CONTAINS(schemeAudit, "subject=owner@builtin");
            UNIT_ASSERT_STRING_CONTAINS(schemeAudit, "status=ERROR");
        }

        auto transaction = owner.ExecuteQuery(SetFlag(false), NQuery::TTxControl::BeginTx().CommitTx(),
            NQuery::TExecuteQuerySettings().TraceId("flags-transaction-failed")).GetValueSync();
        UNIT_ASSERT(!transaction.IsSuccess());
        const auto transactionAudit = env.WaitAudit("flags-transaction-failed");
        UNIT_ASSERT_STRING_CONTAINS(transactionAudit, "phase=REQUEST");
        UNIT_ASSERT_STRING_CONTAINS(transactionAudit, "status=ERROR");
        UNIT_ASSERT(!env.Config().Contains("enable_client_modifications"));
    }

    Y_UNIT_TEST(NestedValidationFailuresAreAudited) {
        TDatabaseFlagsEnv env;
        env.EnableAudit();
        auto owner = env.QueryClient("owner@builtin");
        NYdb::NTable::TTableClient tables(env.Env.GetDriver(), NYdb::NTable::TClientSettings().AuthToken("owner@builtin"));
        auto sessionResult = tables.CreateSession().GetValueSync();
        CheckSuccess(sessionResult);
        auto session = sessionResult.GetSession();
        const TString before = env.Config();
        const TString query = R"(
            DO BEGIN
                ALTER DATABASE `/Root/Test`
                RESET (ENABLE_CLIENT_MODIFICATIONS, ENABLE_CLIENT_MODIFICATIONS);
            END DO;
        )";

        auto result = owner.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().TraceId("flags-nested-query-failed")).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        const auto audit = env.WaitAudit("flags-nested-query-failed");
        UNIT_ASSERT_STRING_CONTAINS(audit, "phase=VALIDATION");
        UNIT_ASSERT_STRING_CONTAINS(audit, "subject=owner@builtin");
        UNIT_ASSERT_STRING_CONTAINS(audit, "ENABLE_CLIENT_MODIFICATIONS=INHERIT");
        UNIT_ASSERT_STRING_CONTAINS(audit, "status=ERROR");

        auto schemeResult = session.ExecuteSchemeQuery(query,
            NYdb::NTable::TExecSchemeQuerySettings().TraceId("flags-nested-scheme-failed")).GetValueSync();
        UNIT_ASSERT(!schemeResult.IsSuccess());
        const auto schemeAudit = env.WaitAudit("flags-nested-scheme-failed");
        UNIT_ASSERT_STRING_CONTAINS(schemeAudit, "phase=REQUEST");
        UNIT_ASSERT_STRING_CONTAINS(schemeAudit, "subject=owner@builtin");
        UNIT_ASSERT_STRING_CONTAINS(schemeAudit, "ENABLE_CLIENT_MODIFICATIONS=INHERIT");
        UNIT_ASSERT_STRING_CONTAINS(schemeAudit, "status=ERROR");
        UNIT_ASSERT_VALUES_EQUAL(env.Config(), before);
    }

    Y_UNIT_TEST(Validation) {
        TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        settings.FeatureFlags.SetEnableAlterDatabase(true);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        for (const auto* clause : {
            "SET (ENABLE_CLIENT_MODIFICATIONS = 'false')",
            "SET (ENABLE_CLIENT_MODIFICATIONS = 0)",
            "SET (ENABLE_CLIENT_MODIFICATIONS = true, MAX_SHARDS = 10)",
            "SET (ENABLE_BACKGROUND_COMPACTION = false)",
            "SET (ENABLE_COMPUTE_SCHEDULER_MEMORY = false)",
            "RESET (MAX_SHARDS)",
            "RESET (UNKNOWN_FLAG)",
        }) {
            auto result = client.ExecuteQuery(TStringBuilder() << "ALTER DATABASE `/Root/Test` " << clause,
                NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), clause);
            UNIT_ASSERT_C(result.GetStatus() == EStatus::GENERIC_ERROR || result.GetStatus() == EStatus::BAD_REQUEST,
                result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterDatabaseGate) {
        TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        settings.FeatureFlags.SetEnableAlterDatabase(false);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        for (const auto& query : {SetFlag(false), ResetFlag()}) {
            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "ALTER DATABASE statement is not supported");
        }
    }
}

} // namespace NKikimr::NKqp
