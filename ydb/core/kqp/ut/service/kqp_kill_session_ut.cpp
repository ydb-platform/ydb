#include <atomic>

#include <ydb/core/kqp/common/buffer/events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/datashard/datashard_failpoints.h>
#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/services/workload_manager/events.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

TKikimrSettings KillSessionSettings() {
    auto settings = TKikimrSettings().SetWithSampleTables(false).SetAuthToken("root@builtin");
    settings.FeatureFlags.SetEnableKillSession(true);
    return settings;
}

TString KillSessionQuery(const std::string& sessionId) {
    return TStringBuilder() << "KILL SESSION `" << sessionId << "`;";
}

TSession CreateSession(TQueryClient& client) {
    auto result = client.GetSession().GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetSession();
}

void AssertSessionAlive(TSession& session) {
    auto result = session.ExecuteQuery("SELECT 1;", TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void GrantSessionConnect(const TKikimrRunner& kikimr, const TString& subject) {
    auto result = kikimr.GetSchemeClient().ModifyPermissions("/Root",
        NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(
            NYdb::NScheme::TPermissions(subject, {"ydb.database.connect"}))).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpKillSession) {

    Y_UNIT_TEST(DisabledByDefaultDoesNotKill) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false).SetAuthToken("root@builtin"));
        auto client = kikimr.GetQueryClient();
        auto victim = CreateSession(client);

        auto result = client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToString());
        AssertSessionAlive(victim);
    }

    Y_UNIT_TEST(QuerySessionLiteralAndRepeatedKill) {
        TKikimrRunner kikimr(KillSessionSettings());
        auto client = kikimr.GetQueryClient();
        auto victim = CreateSession(client);
        const auto query = KillSessionQuery(victim.GetId());

        auto result = client.ExecuteQuery(query, TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto repeated = client.ExecuteQuery(query, TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(repeated.GetStatus(), EStatus::PRECONDITION_FAILED, repeated.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(repeated.GetIssues().ToString(), "Session not found");

        auto victimResult = victim.ExecuteQuery("SELECT 1;", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(!victimResult.IsSuccess(), "Terminated session accepted another query");
    }

    Y_UNIT_TEST(TableSessionLiteral) {
        TKikimrRunner kikimr(KillSessionSettings());
        auto client = kikimr.GetQueryClient();
        auto tableClient = kikimr.GetTableClient();
        auto created = tableClient.CreateSession().GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto victim = created.GetSession();

        auto result = client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto victimResult = victim.ExecuteDataQuery("SELECT 1;",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(!victimResult.IsSuccess(), "Terminated Table session accepted another query");
    }

    Y_UNIT_TEST(LegacyExecuteSchemeQueryDoesNotKill) {
        TKikimrRunner kikimr(KillSessionSettings());
        auto client = kikimr.GetQueryClient();
        auto victim = CreateSession(client);
        auto tableClient = kikimr.GetTableClient();
        auto created = tableClient.CreateSession().GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto caller = created.GetSession();

        auto result = caller.ExecuteSchemeQuery(KillSessionQuery(victim.GetId())).GetValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "Legacy ExecuteSchemeQuery must not execute KILL SESSION");
        AssertSessionAlive(victim);
    }

    Y_UNIT_TEST_TWIN(Utf8Parameter, yqlSelect) {
        TKikimrRunner kikimr(KillSessionSettings());
        auto client = kikimr.GetQueryClient();
        auto victim = CreateSession(client);
        auto params = TParamsBuilder()
            .AddParam("$session_id").Utf8(victim.GetId()).Build()
            .Build();

        const TString query = TStringBuilder()
            << (yqlSelect ? "PRAGMA YqlSelect = 'force'; " : "")
            << "DECLARE $session_id AS Utf8; KILL SESSION $session_id;";
        auto result = client.ExecuteQuery(query,
            TTxControl::NoTx(), params, NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto repeated = client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(repeated.GetStatus(), EStatus::PRECONDITION_FAILED, repeated.GetIssues().ToString());
    }

    Y_UNIT_TEST(RepeatedParameterizedQueryUsesCurrentSessionId) {
        auto settings = KillSessionSettings();
        settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        auto first = CreateSession(client);
        auto second = CreateSession(client);
        const TString query = "DECLARE $session_id AS Utf8; KILL SESSION $session_id;";

        auto kill = [&](const std::string& sessionId) {
            auto params = TParamsBuilder().AddParam("$session_id").Utf8(sessionId).Build().Build();
            return client.ExecuteQuery(query, TTxControl::NoTx(), params, NoRetryExecuteQuerySettings()).GetValueSync();
        };
        auto firstResult = kill(first.GetId());
        UNIT_ASSERT_C(firstResult.IsSuccess(), firstResult.GetIssues().ToString());
        AssertSessionAlive(second);

        auto secondResult = kill(second.GetId());
        UNIT_ASSERT_C(secondResult.IsSuccess(), secondResult.GetIssues().ToString());
        auto repeated = kill(second.GetId());
        UNIT_ASSERT_VALUES_EQUAL_C(repeated.GetStatus(), EStatus::PRECONDITION_FAILED, repeated.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(MultipleKillStatements, perStatementExecution) {
        auto settings = KillSessionSettings();
        settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(perStatementExecution);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        auto first = CreateSession(client);
        auto second = CreateSession(client);

        const TString query = KillSessionQuery(first.GetId()) + "\n" + KillSessionQuery(second.GetId());
        auto result = client.ExecuteQuery(query, TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        for (const auto& sessionId : {first.GetId(), second.GetId()}) {
            auto repeated = client.ExecuteQuery(KillSessionQuery(sessionId), TTxControl::NoTx(),
                NoRetryExecuteQuerySettings()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(repeated.GetStatus(), EStatus::PRECONDITION_FAILED, repeated.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(KillBetweenSelectStatements) {
        auto settings = KillSessionSettings();
        settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        auto victim = CreateSession(client);
        const TString query = "SELECT 1;\n" + KillSessionQuery(victim.GetId()) + "\nSELECT 2;";
        auto result = client.ExecuteQuery(query, TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
        CompareYson(R"([[1]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[2]])", FormatResultSetYson(result.GetResultSet(1)));
        auto repeated = client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(repeated.GetStatus(), EStatus::PRECONDITION_FAILED, repeated.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(FailedKillStopsFollowingStatements, perStatementExecution) {
        auto settings = KillSessionSettings();
        settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(perStatementExecution);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        auto first = CreateSession(client);
        auto removed = CreateSession(client);
        auto last = CreateSession(client);
        auto killed = client.ExecuteQuery(KillSessionQuery(removed.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(killed.IsSuccess(), killed.GetIssues().ToString());

        const TString query = KillSessionQuery(first.GetId()) + "\n" + KillSessionQuery(removed.GetId())
            + "\n" + KillSessionQuery(last.GetId());
        auto result = client.ExecuteQuery(query, TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        auto repeated = client.ExecuteQuery(KillSessionQuery(first.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(repeated.GetStatus(), EStatus::PRECONDITION_FAILED, repeated.GetIssues().ToString());
        AssertSessionAlive(last);
    }

    Y_UNIT_TEST_TWIN(MixedDataWithoutPerStatementExecutionDoesNotKill, astCache) {
        auto settings = KillSessionSettings().SetWithSampleTables(true);
        // Both flags are required to split the query into individual statements.
        settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(astCache);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(!astCache);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        auto victim = CreateSession(client);
        const auto kill = KillSessionQuery(victim.GetId());
        const TString write = "UPSERT INTO `/Root/EightShard` (Key, Text) VALUES (100505u, \"not-written\");";

        for (const TString& query : {TString("SELECT 1;\n") + kill + "\nSELECT 2;",
                kill + "\nSELECT 1;", write + "\n" + kill, kill + "\n" + write})
        {
            auto result = client.ExecuteQuery(query, TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "KILL SESSION cannot be combined with data queries");
            AssertSessionAlive(victim);
        }

        auto stored = client.ExecuteQuery("SELECT Text FROM `/Root/EightShard` WHERE Key = 100505u;",
            TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(stored.IsSuccess(), stored.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(stored.GetResultSet(0).RowsCount(), 0);
    }

    Y_UNIT_TEST(ExplicitTransactionDoesNotKill) {
        TKikimrRunner kikimr(KillSessionSettings());
        auto client = kikimr.GetQueryClient();
        auto victim = CreateSession(client);

        auto result = client.ExecuteQuery(KillSessionQuery(victim.GetId()),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        AssertSessionAlive(victim);
    }

    Y_UNIT_TEST(ExplainDoesNotKill) {
        TKikimrRunner kikimr(KillSessionSettings());
        auto client = kikimr.GetQueryClient();
        auto victim = CreateSession(client);
        auto settings = NoRetryExecuteQuerySettings().ExecMode(EExecMode::Explain);

        auto result = client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        AssertSessionAlive(victim);
    }

    Y_UNIT_TEST(ExistingTransactionDoesNotKill) {
        TKikimrRunner kikimr(KillSessionSettings());
        auto client = kikimr.GetQueryClient();
        auto victim = CreateSession(client);
        auto caller = CreateSession(client);
        auto started = caller.BeginTransaction(TTxSettings::SerializableRW()).GetValueSync();
        UNIT_ASSERT_C(started.IsSuccess(), started.GetIssues().ToString());

        auto result = caller.ExecuteQuery(KillSessionQuery(victim.GetId()),
            TTxControl::Tx(started.GetTransaction())).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        AssertSessionAlive(victim);
    }

    Y_UNIT_TEST(SelfKillIsRejected) {
        TKikimrRunner kikimr(KillSessionSettings());
        auto client = kikimr.GetQueryClient();
        auto session = CreateSession(client);

        auto result = session.ExecuteQuery(KillSessionQuery(session.GetId()), TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        AssertSessionAlive(session);
    }

    Y_UNIT_TEST(InvalidSessionId) {
        TKikimrRunner kikimr(KillSessionSettings());
        auto client = kikimr.GetQueryClient();

        for (const auto& sessionId : {"not-a-session", "ydb://session/3?node_id=0&id=invalid", "ydb://session/3?node_id=1"}) {
            auto result = client.ExecuteQuery(KillSessionQuery(sessionId), TTxControl::NoTx(),
                NoRetryExecuteQuerySettings()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ParameterMustBeNonNullUtf8) {
        TKikimrRunner kikimr(KillSessionSettings());
        auto client = kikimr.GetQueryClient();
        auto victim = CreateSession(client);

        auto checkRejected = [&](const TString& type, const TParams& params) {
            auto result = client.ExecuteQuery(TStringBuilder()
                << "DECLARE $session_id AS " << type << "; KILL SESSION $session_id;",
                TTxControl::NoTx(), params, NoRetryExecuteQuerySettings()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), "Accepted session ID of type " << type);
            AssertSessionAlive(victim);
        };
        checkRejected("String", TParamsBuilder()
            .AddParam("$session_id").String(victim.GetId()).Build().Build());
        checkRejected("Uint64", TParamsBuilder()
            .AddParam("$session_id").Uint64(1).Build().Build());
        checkRejected("Utf8?", TParamsBuilder()
            .AddParam("$session_id").EmptyOptional(EPrimitiveType::Utf8).Build().Build());
    }

    Y_UNIT_TEST(OwnerCanKillOwnSessionButCannotProbeOthers) {
        TKikimrRunner kikimr(KillSessionSettings());
        GrantSessionConnect(kikimr, "owner@builtin");
        GrantSessionConnect(kikimr, "other@builtin");
        WaitForProxy(kikimr, "owner@builtin");
        WaitForProxy(kikimr, "other@builtin");

        auto owner = kikimr.GetQueryClient(TClientSettings().AuthToken("owner@builtin"));
        auto other = kikimr.GetQueryClient(TClientSettings().AuthToken("other@builtin"));
        auto victim = CreateSession(owner);
        const auto query = KillSessionQuery(victim.GetId());

        auto denied = other.ExecuteQuery(query, TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(denied.GetStatus(), EStatus::UNAUTHORIZED, denied.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(denied.GetIssues().ToString(), "Session not found or access denied");
        AssertSessionAlive(victim);

        auto killed = owner.ExecuteQuery(query, TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(killed.IsSuccess(), killed.GetIssues().ToString());

        auto absent = other.ExecuteQuery(query, TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(absent.GetStatus(), EStatus::UNAUTHORIZED, absent.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(absent.GetIssues().ToString(), "Session not found or access denied");
    }

    Y_UNIT_TEST(AdminCanKillAnotherUsersTableSession) {
        TKikimrRunner kikimr(KillSessionSettings());
        GrantSessionConnect(kikimr, "owner@builtin");
        WaitForProxy(kikimr, "owner@builtin");
        auto tableClient = kikimr.GetTableClient(NYdb::NTable::TClientSettings().AuthToken("owner@builtin"));
        auto created = tableClient.CreateSession().GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto victim = created.GetSession();
        auto admin = kikimr.GetQueryClient();

        auto result = admin.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(DatabaseOwnerCanKillForeignSession, groupOwner) {
        auto settings = KillSessionSettings();
        settings.FeatureFlags.SetEnableDatabaseAdmin(true);
        TKikimrRunner kikimr(settings);
        auto admin = kikimr.GetQueryClient();
        auto victim = CreateSession(admin);

        kikimr.GetTestClient().TestCreateUser("/Root", "databaseadmin", "secret_password", "root@builtin");
        if constexpr (groupOwner) {
            auto created = admin.ExecuteQuery("CREATE GROUP databaseadmins WITH USER databaseadmin;",
                TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        }
        GrantSessionConnect(kikimr, "databaseadmin");
        auto permissions = NYdb::NScheme::TModifyPermissionsSettings()
            .AddChangeOwner(groupOwner ? "databaseadmins" : "databaseadmin");
        auto changed = kikimr.GetSchemeClient().ModifyPermissions("/Root", permissions).GetValueSync();
        UNIT_ASSERT_C(changed.IsSuccess(), changed.GetIssues().ToString());
        Tests::TClient::RefreshPathCache(kikimr.GetTestServer().GetRuntime(), "/Root");

        auto owner = kikimr.GetQueryClient(TClientSettings().CredentialsProviderFactory(
            CreateLoginCredentialsProviderFactory({.User = "databaseadmin", .Password = "secret_password"})));
        auto result = owner.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto victimResult = victim.ExecuteQuery("SELECT 1;", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(!victimResult.IsSuccess(), "Database administrator did not terminate the session");
    }

    Y_UNIT_TEST(DatabaseOwnershipIsCheckedForEachExecution) {
        auto settings = KillSessionSettings();
        settings.FeatureFlags.SetEnableDatabaseAdmin(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        TKikimrRunner kikimr(settings);
        auto admin = kikimr.GetQueryClient();
        auto first = CreateSession(admin);
        auto second = CreateSession(admin);
        GrantSessionConnect(kikimr, "databaseadmin@builtin");

        auto changeOwner = [&](const std::string& newOwner) {
            auto result = kikimr.GetSchemeClient().ModifyPermissions("/Root",
                NYdb::NScheme::TModifyPermissionsSettings().AddChangeOwner(newOwner)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        changeOwner("databaseadmin@builtin");
        auto client = kikimr.GetQueryClient(TClientSettings().AuthToken("databaseadmin@builtin"));
        auto caller = CreateSession(client);
        const TString query = "DECLARE $session_id AS Utf8; KILL SESSION $session_id;";
        auto kill = [&](const std::string& sessionId) {
            auto params = TParamsBuilder().AddParam("$session_id").Utf8(sessionId).Build().Build();
            return caller.ExecuteQuery(query, TTxControl::NoTx(), params,
                NoRetryExecuteQuerySettings()).GetValueSync();
        };
        auto killed = kill(first.GetId());
        UNIT_ASSERT_C(killed.IsSuccess(), killed.GetIssues().ToString());

        // Connect remains granted; only the database-administrator privilege is revoked.
        changeOwner("root@builtin");
        auto denied = kill(second.GetId());
        UNIT_ASSERT_VALUES_EQUAL_C(denied.GetStatus(), EStatus::UNAUTHORIZED, denied.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(denied.GetIssues().ToString(), "Session not found or access denied");
        AssertSessionAlive(caller);
        AssertSessionAlive(second);
    }

    Y_UNIT_TEST_TWIN(UpdateRowCanKillForeignSession, tableService) {
        auto settings = KillSessionSettings();
        settings.FeatureFlags.SetEnableDatabaseAdmin(false);
        TKikimrRunner kikimr(settings);
        auto admin = kikimr.GetQueryClient();
        auto querySession = CreateSession(admin);
        auto tableClient = kikimr.GetTableClient();
        auto created = tableClient.CreateSession().GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto tableSession = created.GetSession();
        const auto sessionId = tableService ? tableSession.GetId() : querySession.GetId();
        auto granted = kikimr.GetSchemeClient().ModifyPermissions("/Root",
            NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(
                NYdb::NScheme::TPermissions("operator@builtin", {
                    "ydb.database.connect", "ydb.granular.update_row"}))).GetValueSync();
        UNIT_ASSERT_C(granted.IsSuccess(), granted.GetIssues().ToString());
        WaitForProxy(kikimr, "operator@builtin");
        auto client = kikimr.GetQueryClient(TClientSettings().AuthToken("operator@builtin"));

        auto killed = client.ExecuteQuery(KillSessionQuery(sessionId), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(killed.IsSuccess(), killed.GetIssues().ToString());
        auto repeated = client.ExecuteQuery(KillSessionQuery(sessionId), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(repeated.GetStatus(), EStatus::PRECONDITION_FAILED, repeated.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(repeated.GetIssues().ToString(), "Session not found");
    }

    Y_UNIT_TEST(UpdateRowGrantedThroughGroupCanKillForeignSession) {
        auto settings = KillSessionSettings();
        settings.FeatureFlags.SetEnableDatabaseAdmin(false);
        TKikimrRunner kikimr(settings);
        auto admin = kikimr.GetQueryClient();
        auto victim = CreateSession(admin);
        kikimr.GetTestClient().TestCreateUser("/Root", "operator", "secret_password", "root@builtin");
        auto created = admin.ExecuteQuery("CREATE GROUP operators WITH USER operator;",
            TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto granted = kikimr.GetSchemeClient().ModifyPermissions("/Root",
            NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(
                NYdb::NScheme::TPermissions("operators", {
                    "ydb.database.connect", "ydb.granular.update_row"}))).GetValueSync();
        UNIT_ASSERT_C(granted.IsSuccess(), granted.GetIssues().ToString());
        Tests::TClient::RefreshPathCache(kikimr.GetTestServer().GetRuntime(), "/Root");
        auto client = kikimr.GetQueryClient(TClientSettings().CredentialsProviderFactory(
            CreateLoginCredentialsProviderFactory({.User = "operator", .Password = "secret_password"})));

        auto killed = client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(killed.IsSuccess(), killed.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(GenericPermissionsCanKillForeignSession, genericUse) {
        auto settings = KillSessionSettings();
        settings.FeatureFlags.SetEnableDatabaseAdmin(false);
        TKikimrRunner kikimr(settings);
        auto admin = kikimr.GetQueryClient();
        auto victim = CreateSession(admin);
        auto granted = kikimr.GetSchemeClient().ModifyPermissions("/Root",
            NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(
                NYdb::NScheme::TPermissions("operator@builtin", {
                    genericUse ? "ydb.generic.use" : "ydb.generic.write", "ydb.database.connect"}))).GetValueSync();
        UNIT_ASSERT_C(granted.IsSuccess(), granted.GetIssues().ToString());
        WaitForProxy(kikimr, "operator@builtin");
        auto client = kikimr.GetQueryClient(TClientSettings().AuthToken("operator@builtin"));

        auto killed = client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(killed.IsSuccess(), killed.GetIssues().ToString());
    }

    Y_UNIT_TEST(UpdateRowIsCheckedForEachExecution) {
        auto settings = KillSessionSettings();
        settings.FeatureFlags.SetEnableDatabaseAdmin(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        TKikimrRunner kikimr(settings);
        auto admin = kikimr.GetQueryClient();
        auto first = CreateSession(admin);
        auto second = CreateSession(admin);
        GrantSessionConnect(kikimr, "operator@builtin");
        WaitForProxy(kikimr, "operator@builtin");
        auto client = kikimr.GetQueryClient(TClientSettings().AuthToken("operator@builtin"));
        auto caller = CreateSession(client);
        auto kill = [&](const std::string& sessionId) {
            auto params = TParamsBuilder().AddParam("$session_id").Utf8(sessionId).Build().Build();
            return caller.ExecuteQuery("DECLARE $session_id AS Utf8; KILL SESSION $session_id;",
                TTxControl::NoTx(), params, NoRetryExecuteQuerySettings()).GetValueSync();
        };
        auto denied = kill(first.GetId());
        UNIT_ASSERT_VALUES_EQUAL_C(denied.GetStatus(), EStatus::UNAUTHORIZED, denied.GetIssues().ToString());
        AssertSessionAlive(first);

        auto granted = kikimr.GetSchemeClient().ModifyPermissions("/Root",
            NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(
                NYdb::NScheme::TPermissions("operator@builtin", {"ydb.granular.update_row"}))).GetValueSync();
        UNIT_ASSERT_C(granted.IsSuccess(), granted.GetIssues().ToString());
        Tests::TClient::RefreshPathCache(kikimr.GetTestServer().GetRuntime(), "/Root");
        auto killed = kill(first.GetId());
        UNIT_ASSERT_C(killed.IsSuccess(), killed.GetIssues().ToString());

        auto revoked = kikimr.GetSchemeClient().ModifyPermissions("/Root",
            NYdb::NScheme::TModifyPermissionsSettings().AddRevokePermissions(
                NYdb::NScheme::TPermissions("operator@builtin", {"ydb.granular.update_row"}))).GetValueSync();
        UNIT_ASSERT_C(revoked.IsSuccess(), revoked.GetIssues().ToString());
        Tests::TClient::RefreshPathCache(kikimr.GetTestServer().GetRuntime(), "/Root");
        denied = kill(second.GetId());
        UNIT_ASSERT_VALUES_EQUAL_C(denied.GetStatus(), EStatus::UNAUTHORIZED, denied.GetIssues().ToString());
        AssertSessionAlive(caller);
        AssertSessionAlive(second);
    }

    Y_UNIT_TEST(UpdateRowOnSubdirectoryDoesNotAllowKillingForeignSession) {
        auto settings = KillSessionSettings();
        settings.FeatureFlags.SetEnableDatabaseAdmin(false);
        TKikimrRunner kikimr(settings);
        auto admin = kikimr.GetQueryClient();
        auto victim = CreateSession(admin);
        GrantSessionConnect(kikimr, "operator@builtin");
        auto created = kikimr.GetSchemeClient().MakeDirectory("/Root/Scope").GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto granted = kikimr.GetSchemeClient().ModifyPermissions("/Root/Scope",
            NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(
                NYdb::NScheme::TPermissions("operator@builtin", {"ydb.granular.update_row"}))).GetValueSync();
        UNIT_ASSERT_C(granted.IsSuccess(), granted.GetIssues().ToString());
        WaitForProxy(kikimr, "operator@builtin");
        auto client = kikimr.GetQueryClient(TClientSettings().AuthToken("operator@builtin"));

        auto denied = client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(denied.GetStatus(), EStatus::UNAUTHORIZED, denied.GetIssues().ToString());
        AssertSessionAlive(victim);
    }

    Y_UNIT_TEST(PermissionsWithoutUpdateRowDoNotAllowKillingForeignSession) {
        auto settings = KillSessionSettings();
        settings.FeatureFlags.SetEnableDatabaseAdmin(false);
        TKikimrRunner kikimr(settings);
        auto admin = kikimr.GetQueryClient();
        auto victim = CreateSession(admin);
        // Revoke matches complete ACL entries; it cannot subtract a bit from GenericUse.
        NACLib::TDiffACL permissions;
        permissions.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse & ~NACLib::UpdateRow, "regular@builtin");
        auto granted = kikimr.GetTestClient().ModifyACL("/", "Root", permissions.SerializeAsString(), "root@builtin");
        UNIT_ASSERT_VALUES_EQUAL(granted, NMsgBusProxy::MSTATUS_OK);
        Tests::TClient::RefreshPathCache(kikimr.GetTestServer().GetRuntime(), "/Root");
        WaitForProxy(kikimr, "regular@builtin");
        auto regular = kikimr.GetQueryClient(TClientSettings().AuthToken("regular@builtin"));

        auto denied = regular.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(denied.GetStatus(), EStatus::UNAUTHORIZED, denied.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(denied.GetIssues().ToString(), "Session not found or access denied");
        AssertSessionAlive(victim);
    }

    Y_UNIT_TEST_TWIN(WithoutConnectCannotProbeOrKillSessions, updateRow) {
        TKikimrRunner kikimr(KillSessionSettings());
        auto admin = kikimr.GetQueryClient();
        auto victim = CreateSession(admin);
        auto removed = CreateSession(admin);
        auto killed = admin.ExecuteQuery(KillSessionQuery(removed.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(killed.IsSuccess(), killed.GetIssues().ToString());
        if constexpr (updateRow) {
            auto granted = kikimr.GetSchemeClient().ModifyPermissions("/Root",
                NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(
                    NYdb::NScheme::TPermissions("no-connect@builtin", {"ydb.granular.update_row"}))).GetValueSync();
            UNIT_ASSERT_C(granted.IsSuccess(), granted.GetIssues().ToString());
            Tests::TClient::RefreshPathCache(kikimr.GetTestServer().GetRuntime(), "/Root");
        }
        auto caller = kikimr.GetQueryClient(TClientSettings().AuthToken("no-connect@builtin"));

        auto existing = caller.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        auto missing = caller.ExecuteQuery(KillSessionQuery(removed.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(existing.GetStatus(), EStatus::UNAUTHORIZED, existing.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(missing.GetStatus(), EStatus::UNAUTHORIZED, missing.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(existing.GetIssues().ToString(), missing.GetIssues().ToString());
        AssertSessionAlive(victim);
    }

    Y_UNIT_TEST(TableSessionOwnerIsKnownBeforeFirstQuery) {
        TKikimrRunner kikimr(KillSessionSettings());
        GrantSessionConnect(kikimr, "owner@builtin");
        WaitForProxy(kikimr, "owner@builtin");
        auto tableClient = kikimr.GetTableClient(NYdb::NTable::TClientSettings().AuthToken("owner@builtin"));
        auto created = tableClient.CreateSession().GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto victim = created.GetSession();
        auto owner = kikimr.GetQueryClient(TClientSettings().AuthToken("owner@builtin"));

        auto result = owner.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
            NoRetryExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ImplicitSessionCreatorIsRecorded) {
        TKikimrRunner kikimr(KillSessionSettings());
        auto client = kikimr.GetQueryClient();
        auto settings = NoRetryExecuteQuerySettings().TraceId("kill-session-implicit-owner");

        auto result = client.ExecuteQuery(R"(
            SELECT UserSID
            FROM `.sys/query_sessions`
            WHERE TraceId = "kill-session-implicit-owner";
        )", TTxControl::NoTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto parser = result.GetResultSetParser(0);
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("UserSID").GetOptionalUtf8().value_or(""), "root@builtin");
        UNIT_ASSERT(!parser.TryNextRow());
    }

    Y_UNIT_TEST(RemoteSessionPreservesCreator) {
        auto settings = KillSessionSettings().SetNodeCount(2).SetUseRealThreads(false);
        auto* balancing = settings.AppConfig.MutableTableServiceConfig()->MutableSessionBalancerSettings();
        balancing->SetSupportRemoteSessionCreation(true);
        balancing->SetBoardLookupIntervalMs(100);
        balancing->SetBoardPublishIntervalMs(100);
        TKikimrRunner kikimr(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        for (ui32 i = 0; i < runtime->GetNodeCount(); ++i) {
            runtime->GetAppData(i).AdministrationAllowedSIDs = {"root@builtin"};
            runtime->EnableScheduleForActor(runtime->GetLocalServiceId(MakeKqpProxyID(runtime->GetNodeId(i)), i));
        }
        kikimr.RunCall([&] {
            GrantSessionConnect(kikimr, "owner@builtin");
            WaitForProxy(kikimr, "owner@builtin");
        });
        auto tableClient = kikimr.RunCall([&] {
            return kikimr.GetTableClient(NYdb::NTable::TClientSettings().AuthToken("owner@builtin"));
        });
        auto owner = kikimr.RunCall([&] {
            return kikimr.GetQueryClient(TClientSettings().AuthToken("owner@builtin"));
        });
        auto admin = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        const auto entryProxy = MakeKqpProxyID(runtime->GetNodeId(0));
        const auto edge = runtime->AllocateEdgeActor(0);

        // Nodes have different DCs. Give only the entry proxy the remote DC, so
        // the real resource snapshots contain exactly one eligible destination.
        runtime->Send(GetNameserviceActorId(), edge, new TEvInterconnect::TEvGetNode(runtime->GetNodeId(1)));
        auto nodeInfo = runtime->GrabEdgeEvent<TEvInterconnect::TEvNodeInfo>(edge)->Release();
        UNIT_ASSERT(nodeInfo->Node);
        runtime->Send(entryProxy, edge, nodeInfo.Release());
        bool peersReady = false;
        const auto deadline = runtime->GetCurrentTime() + TDuration::Seconds(10);
        while (runtime->GetCurrentTime() < deadline) {
            runtime->Send(entryProxy, edge, new TEvKqp::TEvListProxyNodesRequest());
            auto peers = runtime->GrabEdgeEvent<TEvKqp::TEvListProxyNodesResponse>(edge);
            if (peers->Get()->ProxyNodes.size() == 2) {
                peersReady = true;
                break;
            }
            runtime->SimulateSleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(peersReady, "Entry proxy did not discover both resource-manager snapshots");
        runtime->SimulateSleep(TDuration::Seconds(3));

        bool forwarded = false;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvKqp::TEvCreateSessionRequest::EventType
                && ev->Sender.NodeId() != ev->GetRecipientRewrite().NodeId())
            {
                forwarded = true;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc); };

        const std::string remoteNode = std::string("node_id=") + ToString(runtime->GetNodeId(1)).c_str() + "&";
        auto created = kikimr.RunCall([&] { return tableClient.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto victim = created.GetSession();
        const auto remoteSessionId = victim.GetId();
        UNIT_ASSERT_C(forwarded && remoteSessionId.find(remoteNode) != std::string::npos,
            "Session was not created by the only eligible remote KQP proxy");

        auto identity = kikimr.RunCall([&] {
            auto params = TParamsBuilder().AddParam("$id").Utf8(remoteSessionId).Build().Build();
            return admin.ExecuteQuery(R"(
                DECLARE $id AS Utf8;
                SELECT UserSID FROM `.sys/query_sessions` WHERE SessionId = $id;
            )", TTxControl::NoTx(), params, NoRetryExecuteQuerySettings()).GetValueSync();
        });
        UNIT_ASSERT_C(identity.IsSuccess(), identity.GetIssues().ToString());
        auto parser = identity.GetResultSetParser(0);
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("UserSID").GetOptionalUtf8().value_or(""), "owner@builtin");
        UNIT_ASSERT(!parser.TryNextRow());

        auto result = kikimr.RunCall([&] {
            return owner.ExecuteQuery(KillSessionQuery(remoteSessionId), TTxControl::NoTx(),
                NoRetryExecuteQuerySettings()).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(BusyReadCancelledWithoutSdkRetry, tableService) {
        auto settings = KillSessionSettings().SetWithSampleTables(true).SetUseRealThreads(false);
        TKikimrRunner kikimr(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto tableClient = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        std::atomic<ui32> attempts = 0;
        auto sessionIdPromise = NThreading::NewPromise<std::string>();
        ui32 stateEvents = 0;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NYql::NDq::TEvDqCompute::TEvState::EventType) {
                ++stateEvents;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc); };
        NDataShard::gSkipReadIteratorResultFailPoint.Enable(-1);
        Y_DEFER { NDataShard::gSkipReadIteratorResultFailPoint.Disable(); };

        auto future = kikimr.RunInThreadPool([&]() -> TStatus {
            if constexpr (tableService) {
                return tableClient.RetryOperation([&](NYdb::NTable::TSession session) -> TAsyncStatus {
                    if (attempts.fetch_add(1) == 0) {
                        sessionIdPromise.SetValue(session.GetId());
                    }
                    return session.ExecuteDataQuery("SELECT * FROM `/Root/EightShard`;",
                        NYdb::NTable::TTxControl::BeginTx().CommitTx(),
                        NYdb::NTable::TExecDataQuerySettings().ClientTimeout(TDuration::Seconds(30)))
                        .Apply([](const NThreading::TFuture<NYdb::NTable::TDataQueryResult>& result) -> TStatus {
                            return result.GetValue();
                        });
                }, NYdb::NTable::TRetryOperationSettings().Idempotent(true).MaxRetries(3)).GetValueSync();
            } else {
                return client.RetryQuery([&](TSession session) -> TAsyncStatus {
                    if (attempts.fetch_add(1) == 0) {
                        sessionIdPromise.SetValue(session.GetId());
                    }
                    return session.ExecuteQuery("SELECT * FROM `/Root/EightShard`;",
                        TTxControl::BeginTx().CommitTx(),
                        TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30)))
                        .Apply([](const NThreading::TFuture<TExecuteQueryResult>& result) -> TStatus {
                            return result.GetValue();
                        });
                }, TRetryOperationSettings().Idempotent(true).MaxRetries(3)).GetValueSync();
            }
        });

        auto sessionId = runtime->WaitFuture(sessionIdPromise.GetFuture());
        TDispatchOptions started;
        started.FinalEvents.emplace_back([&](IEventHandle&) { return stateEvents > 0; });
        UNIT_ASSERT_C(runtime->DispatchEvents(started, TDuration::Seconds(10)), "Victim query did not start");
        UNIT_ASSERT_C(!future.HasValue(), "Victim query completed before KILL");

        auto killed = kikimr.RunCall([&] {
            return client.ExecuteQuery(KillSessionQuery(sessionId), TTxControl::NoTx(),
                NoRetryExecuteQuerySettings()).GetValueSync();
        });
        UNIT_ASSERT_C(killed.IsSuccess(), killed.GetIssues().ToString());

        auto result = runtime->WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::CANCELLED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "KILL SESSION");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "root@builtin");
        UNIT_ASSERT_VALUES_EQUAL(attempts.load(), 1);
    }

    Y_UNIT_TEST(KillWhileIndexCreationIsPending) {
        TKikimrRunner kikimr(KillSessionSettings().SetUseRealThreads(false));
        auto runtime = kikimr.GetTestServer().GetRuntime();
        auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto victim = kikimr.RunCall([&] { return CreateSession(client); });
        auto created = kikimr.RunCall([&] {
            return client.ExecuteQuery(
                "CREATE TABLE `/Root/KillDdl` (Key Uint64, Value Utf8, PRIMARY KEY (Key));",
                TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
        });
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        THolder<IEventHandle> indexResponse;
        auto indexObserver = runtime->AddObserver<NSchemeShard::TEvIndexBuilder::TEvCreateResponse>(
            [&](NSchemeShard::TEvIndexBuilder::TEvCreateResponse::TPtr& ev) {
                if (!indexResponse) {
                    indexResponse.Reset(ev.Release());
                }
            });
        auto ddlFuture = kikimr.RunInThreadPool([&] {
            return victim.ExecuteQuery("ALTER TABLE `/Root/KillDdl` ADD INDEX ByValue GLOBAL ON (Value);",
                TTxControl::NoTx(), NoRetryExecuteQuerySettings()).GetValueSync();
        });
        runtime->WaitFor("index creation response", [&] { return bool(indexResponse); }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(
            indexResponse->Get<NSchemeShard::TEvIndexBuilder::TEvCreateResponse>()->Record.GetStatus(),
            Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_C(!ddlFuture.HasValue(), "DDL completed before KILL");

        auto killed = kikimr.RunCall([&] {
            return client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
                NoRetryExecuteQuerySettings()).GetValueSync();
        });
        UNIT_ASSERT_C(killed.IsSuccess(), killed.GetIssues().ToString());
        auto result = runtime->WaitFuture(ddlFuture);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::CANCELLED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "KILL SESSION");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "root@builtin");

        // SchemeShard may continue an accepted DDL; a late reply must not revive the session.
        indexObserver.Remove();
        runtime->Send(indexResponse.Release());
        auto repeated = kikimr.RunCall([&] {
            return client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
                NoRetryExecuteQuerySettings()).GetValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(repeated.GetStatus(), EStatus::PRECONDITION_FAILED, repeated.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(KillIdleInteractiveTransactionWaitsForRollback, commit) {
        auto settings = KillSessionSettings().SetWithSampleTables(true).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableForceImmediateEffectsExecution(true);
        TKikimrRunner kikimr(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto victim = kikimr.RunCall([&] { return CreateSession(client); });
        auto started = kikimr.RunCall([&] { return victim.BeginTransaction(TTxSettings::SerializableRW()).GetValueSync(); });
        UNIT_ASSERT_C(started.IsSuccess(), started.GetIssues().ToString());
        auto tx = started.GetTransaction();
        auto written = kikimr.RunCall([&] {
            return victim.ExecuteQuery(
                "UPSERT INTO `/Root/EightShard` (Key, Text) VALUES (100504u, \"uncommitted\");",
                TTxControl::Tx(tx)).GetValueSync();
        });
        UNIT_ASSERT_C(written.IsSuccess(), written.GetIssues().ToString());

        // The query has finished, but the transaction and its writes are still open.
        THolder<IEventHandle> rollback;
        auto rollbackObserver = runtime->AddObserver<TEvKqpBuffer::TEvRollback>(
            [&](TEvKqpBuffer::TEvRollback::TPtr& ev) {
                if (!rollback) {
                    rollback.Reset(ev.Release());
                }
            });
        auto killFuture = kikimr.RunInThreadPool([&] {
            return client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
                NoRetryExecuteQuerySettings()).GetValueSync();
        });
        runtime->WaitFor("idle transaction rollback", [&] { return bool(rollback); }, TDuration::Seconds(10));
        UNIT_ASSERT_C(!killFuture.HasValue(), "KILL replied before transaction rollback");
        auto finishTx = [&]() -> TStatus {
            if constexpr (commit) {
                return tx.Commit().GetValueSync();
            } else {
                return tx.Rollback().GetValueSync();
            }
        };
        auto closing = kikimr.RunCall(finishTx);
        UNIT_ASSERT_VALUES_EQUAL_C(closing.GetStatus(), EStatus::CANCELLED, closing.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(closing.GetIssues().ToString(), "KILL SESSION");

        rollbackObserver.Remove();
        runtime->Send(rollback.Release());
        auto killed = runtime->WaitFuture(killFuture);
        UNIT_ASSERT_C(killed.IsSuccess(), killed.GetIssues().ToString());
        auto afterKill = kikimr.RunCall(finishTx);
        UNIT_ASSERT_C(!afterKill.IsSuccess(), "Terminated session accepted a transaction operation");
        auto stored = kikimr.RunCall([&] {
            return client.ExecuteQuery("SELECT Text FROM `/Root/EightShard` WHERE Key = 100504u;",
                TTxControl::NoTx()).GetValueSync();
        });
        UNIT_ASSERT_C(stored.IsSuccess(), stored.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(stored.GetResultSet(0).RowsCount(), 0);
        auto replacement = kikimr.RunCall([&] {
            return client.ExecuteQuery("UPSERT INTO `/Root/EightShard` (Key, Text) VALUES (100504u, \"after-kill\");",
                TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
        UNIT_ASSERT_C(replacement.IsSuccess(), replacement.GetIssues().ToString());
    }

    Y_UNIT_TEST(CompletedCommitIsNotReportedAsCancelled) {
        auto settings = KillSessionSettings().SetWithSampleTables(true).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableForceImmediateEffectsExecution(true);
        TKikimrRunner kikimr(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto victim = kikimr.RunCall([&] { return CreateSession(client); });
        THolder<IEventHandle> heldCommitResult;
        TActorId buffer;
        bool closeReceived = false;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvKqpBuffer::TEvCommit::EventType && !buffer) {
                buffer = ev->GetRecipientRewrite();
            } else if (ev->GetTypeRewrite() == TEvKqpBuffer::TEvResult::EventType && ev->Sender == buffer
                && !heldCommitResult)
            {
                heldCommitResult.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            } else if (ev->GetTypeRewrite() == TEvKqp::TEvCloseSessionRequest::EventType
                && ev->Get<TEvKqp::TEvCloseSessionRequest>()->Record.GetRequest().GetSessionId() == victim.GetId())
            {
                closeReceived = true;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc); };

        auto queryFuture = kikimr.RunInThreadPool([&] {
            return victim.ExecuteQuery(
                "UPSERT INTO `/Root/EightShard` (Key, Text) VALUES (100501u, \"committed-before-kill\");",
                TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
        TDispatchOptions committed;
        committed.FinalEvents.emplace_back([&](IEventHandle&) { return bool(heldCommitResult); });
        UNIT_ASSERT_C(runtime->DispatchEvents(committed, TDuration::Seconds(10)), "Commit did not complete");

        auto killFuture = kikimr.RunInThreadPool([&] {
            return client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
                NoRetryExecuteQuerySettings()).GetValueSync();
        });
        TDispatchOptions closing;
        closing.FinalEvents.emplace_back([&](IEventHandle&) { return closeReceived; });
        UNIT_ASSERT_C(runtime->DispatchEvents(closing, TDuration::Seconds(10)), "KILL did not reach the victim");
        runtime->SimulateSleep(TDuration::MilliSeconds(100));
        UNIT_ASSERT_C(!queryFuture.HasValue(), "Committed query was cancelled before receiving its commit result");
        UNIT_ASSERT_C(!killFuture.HasValue(), "KILL succeeded before the victim finished");

        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        runtime->Send(heldCommitResult.Release());
        auto queryResult = runtime->WaitFuture(queryFuture);
        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        auto killResult = runtime->WaitFuture(killFuture);
        UNIT_ASSERT_C(killResult.IsSuccess(), killResult.GetIssues().ToString());

        auto stored = kikimr.RunCall([&] {
            return client.ExecuteQuery("SELECT Text FROM `/Root/EightShard` WHERE Key = 100501u;",
                TTxControl::NoTx()).GetValueSync();
        });
        UNIT_ASSERT_C(stored.IsSuccess(), stored.GetIssues().ToString());
        CompareYson(R"([[["committed-before-kill"]]])", FormatResultSetYson(stored.GetResultSet(0)));
    }

    Y_UNIT_TEST_TWIN(NonCommittingWriteIsReportedAsCancelled, tableService) {
        auto settings = KillSessionSettings().SetWithSampleTables(true).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableForceImmediateEffectsExecution(true);
        TKikimrRunner kikimr(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto querySession = kikimr.RunCall([&] { return CreateSession(client); });
        auto tableClient = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto tableCreated = kikimr.RunCall([&] { return tableClient.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(tableCreated.IsSuccess(), tableCreated.GetIssues().ToString());
        auto tableSession = tableCreated.GetSession();
        const auto sessionId = tableService ? tableSession.GetId() : querySession.GetId();
        THolder<IEventHandle> heldFlushResult;
        TActorId buffer;
        bool closeReceived = false;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvKqpBuffer::TEvFlush::EventType && !buffer) {
                buffer = ev->GetRecipientRewrite();
            } else if (ev->GetTypeRewrite() == TEvKqpBuffer::TEvResult::EventType && ev->Sender == buffer
                && !heldFlushResult)
            {
                heldFlushResult.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            } else if (ev->GetTypeRewrite() == TEvKqp::TEvCloseSessionRequest::EventType
                && ev->Get<TEvKqp::TEvCloseSessionRequest>()->Record.GetRequest().GetSessionId() == sessionId)
            {
                closeReceived = true;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc); };

        auto queryFuture = kikimr.RunInThreadPool([&]() -> TStatus {
            const TString query =
                "UPSERT INTO `/Root/EightShard` (Key, Text) VALUES (100503u, \"must-be-rolled-back\");";
            if constexpr (tableService) {
                return tableSession.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx()).GetValueSync();
            } else {
                return querySession.ExecuteQuery(query, TTxControl::BeginTx()).GetValueSync();
            }
        });
        TDispatchOptions flushed;
        flushed.FinalEvents.emplace_back([&](IEventHandle&) { return bool(heldFlushResult); });
        UNIT_ASSERT_C(runtime->DispatchEvents(flushed, TDuration::Seconds(10)), "Non-committing write did not flush");

        auto killFuture = kikimr.RunInThreadPool([&] {
            return client.ExecuteQuery(KillSessionQuery(sessionId), TTxControl::NoTx(),
                NoRetryExecuteQuerySettings()).GetValueSync();
        });
        TDispatchOptions closing;
        closing.FinalEvents.emplace_back([&](IEventHandle&) { return closeReceived; });
        UNIT_ASSERT_C(runtime->DispatchEvents(closing, TDuration::Seconds(10)), "KILL did not reach the victim");
        runtime->SimulateSleep(TDuration::MilliSeconds(100));
        UNIT_ASSERT_C(!queryFuture.HasValue(), "Write replied before its pending flush completed");
        UNIT_ASSERT_C(!killFuture.HasValue(), "KILL succeeded before the victim finished");

        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        runtime->Send(heldFlushResult.Release());
        auto queryResult = runtime->WaitFuture(queryFuture);
        UNIT_ASSERT_VALUES_EQUAL_C(queryResult.GetStatus(), EStatus::CANCELLED, queryResult.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(queryResult.GetIssues().ToString(), "KILL SESSION");
        auto killResult = runtime->WaitFuture(killFuture);
        UNIT_ASSERT_C(killResult.IsSuccess(), killResult.GetIssues().ToString());

        auto stored = kikimr.RunCall([&] {
            return client.ExecuteQuery("SELECT Text FROM `/Root/EightShard` WHERE Key = 100503u;",
                TTxControl::NoTx()).GetValueSync();
        });
        UNIT_ASSERT_C(stored.IsSuccess(), stored.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(stored.GetResultSet(0).RowsCount(), 0);
    }

    Y_UNIT_TEST_TWIN(KillBeforeExecutionDoesNotResumeFromLateReply, workloadAdmission) {
        auto settings = KillSessionSettings().SetWithSampleTables(true).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableResourcePools(workloadAdmission);
        TKikimrRunner kikimr(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto victim = kikimr.RunCall([&] { return CreateSession(client); });
        if constexpr (workloadAdmission) {
            auto created = kikimr.RunCall([&] {
                return client.ExecuteQuery(
                    "CREATE RESOURCE POOL kill_test_pool WITH (concurrent_query_limit = 1);",
                    TTxControl::NoTx()).GetValueSync();
            });
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        }

        THolder<IEventHandle> heldReply;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            const ui32 expectedType = workloadAdmission
                ? static_cast<ui32>(NWorkloadManager::TEvContinueRequest::EventType)
                : static_cast<ui32>(TEvKqp::TEvCompileResponse::EventType);
            if (ev->GetTypeRewrite() == expectedType && !heldReply) {
                heldReply.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc); };

        auto queryFuture = kikimr.RunInThreadPool([&] {
            auto querySettings = TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30));
            if constexpr (workloadAdmission) {
                querySettings.ResourcePool("kill_test_pool");
            }
            return victim.ExecuteQuery(
                "UPSERT INTO `/Root/EightShard` (Key, Text) VALUES (100502u, \"must-not-execute\");",
                TTxControl::BeginTx().CommitTx(), querySettings).GetValueSync();
        });
        TDispatchOptions blocked;
        blocked.FinalEvents.emplace_back([&](IEventHandle&) { return bool(heldReply); });
        UNIT_ASSERT_C(runtime->DispatchEvents(blocked, TDuration::Seconds(10)), "Victim did not reach the blocked phase");

        auto killed = kikimr.RunCall([&] {
            return client.ExecuteQuery(KillSessionQuery(victim.GetId()), TTxControl::NoTx(),
                NoRetryExecuteQuerySettings()).GetValueSync();
        });
        UNIT_ASSERT_C(killed.IsSuccess(), killed.GetIssues().ToString());
        auto result = runtime->WaitFuture(queryFuture);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::CANCELLED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "KILL SESSION");

        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        runtime->Send(heldReply.Release());
        runtime->SimulateSleep(TDuration::MilliSeconds(100));
        auto stored = kikimr.RunCall([&] {
            return client.ExecuteQuery("SELECT Text FROM `/Root/EightShard` WHERE Key = 100502u;",
                TTxControl::NoTx()).GetValueSync();
        });
        UNIT_ASSERT_C(stored.IsSuccess(), stored.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(stored.GetResultSet(0).RowsCount(), 0);
    }
}

} // namespace NKikimr::NKqp
