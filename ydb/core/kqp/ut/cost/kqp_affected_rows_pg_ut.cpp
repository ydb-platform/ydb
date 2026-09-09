#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

NKikimrConfig::TAppConfig GetAppConfig() {
    auto app = NKikimrConfig::TAppConfig();
    app.MutableTableServiceConfig()->SetEnableReadCommittedIsolation(true);
    return app;
}

NYdb::NQuery::TExecuteQuerySettings GetQuerySettingsBasic() {
    NYdb::NQuery::TExecuteQuerySettings execSettings;
    execSettings.StatsMode(NYdb::NQuery::EStatsMode::Basic);
    execSettings.CollectAffectedRows(true);
    return execSettings;
}

NYdb::NQuery::TExecuteQuerySettings GetQuerySettingsFull() {
    NYdb::NQuery::TExecuteQuerySettings execSettings;
    execSettings.StatsMode(NYdb::NQuery::EStatsMode::Full);
    execSettings.CollectAffectedRows(true);
    return execSettings;
}

NYdb::NQuery::TExecuteQuerySettings GetQuerySettingsNone() {
    NYdb::NQuery::TExecuteQuerySettings execSettings;
    execSettings.StatsMode(NYdb::NQuery::EStatsMode::None);
    return execSettings;
}

NYdb::NQuery::TExecuteQuerySettings GetQuerySettingsBasicNoAffectedRows() {
    NYdb::NQuery::TExecuteQuerySettings execSettings;
    execSettings.StatsMode(NYdb::NQuery::EStatsMode::Basic);
    return execSettings;
}

NYdb::NQuery::TTxControl BeginReadCommittedRW() {
    return NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::ReadCommittedRW())
        .CommitTx();
}

NYdb::NQuery::TTxControl BeginSerializableRW() {
    return NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW())
        .CommitTx();
}

void CreateTestTable(TSession session) {
    auto result = session.ExecuteQuery(R"(
        CREATE TABLE `/Root/TestTable` (
            Group Uint32,
            Name String,
            Amount Uint32,
            Comment String,
            PRIMARY KEY (Group, Name)
        );
    )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

uint64_t GetAffectedRowsForTable(const NYdb::NQuery::TExecuteQueryResult& result, const TString& tableName) {
    auto stats = result.GetStats();
    if (!stats) {
        return 0;
    }
    const auto& proto = NYdb::TProtoAccessor::GetProto(*stats);
    uint64_t total = 0;
    for (const auto& phase : proto.query_phases()) {
        for (const auto& tableAccess : phase.table_access()) {
            if (tableAccess.name() == tableName) {
                total += tableAccess.affected_rows();
            }
        }
    }
    return total;
}

bool HasAnyAffectedRows(const NYdb::NQuery::TExecuteQueryResult& result) {
    auto stats = result.GetStats();
    if (!stats) {
        return false;
    }
    const auto& proto = NYdb::TProtoAccessor::GetProto(*stats);
    for (const auto& phase : proto.query_phases()) {
        for (const auto& tableAccess : phase.table_access()) {
            if (tableAccess.affected_rows() > 0) {
                return true;
            }
        }
    }
    return false;
}

bool HasAnyAffectedRowsField(const NYdb::NQuery::TExecuteQueryResult& result) {
    auto stats = result.GetStats();
    if (!stats) {
        return false;
    }
    const auto& proto = NYdb::TProtoAccessor::GetProto(*stats);
    for (const auto& phase : proto.query_phases()) {
        for (const auto& tableAccess : phase.table_access()) {
            if (tableAccess.has_affected_rows()) {
                return true;
            }
        }
    }
    return false;
}

}

Y_UNIT_TEST_SUITE(KqpAffectedRowsPg) {
    Y_UNIT_TEST(ReadCommittedRW_InsertSingleRow) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto result = session.ExecuteQuery(Q_(R"(
            INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
            VALUES (1u, "Anna", 3500u, "None");
        )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
        UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);
    }

    Y_UNIT_TEST(ReadCommittedRW_InsertMultipleRows) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto result = session.ExecuteQuery(Q_(R"(
            INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
            VALUES (1u, "Anna", 3500u, "None"), (2u, "Bob", 4000u, "None"), (3u, "Carl", 5000u, "None");
        )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
        UNIT_ASSERT_VALUES_EQUAL(affectedRows, 3u);
    }

    Y_UNIT_TEST(ReadCommittedRW_Upsert) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 4000u, "Updated");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_Replace) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                REPLACE INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 5000u, "Replaced");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_Update) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                UPDATE `/Root/TestTable` ON SELECT 1u AS Group, "Anna" AS Name, 4000u AS Amount, "None" AS Comment;
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_UpdateNoMatch) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                UPDATE `/Root/TestTable` ON SELECT 999u AS Group, "Nobody" AS Name, 4000u AS Amount, "None" AS Comment;
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 0u);
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_Delete) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                DELETE FROM `/Root/TestTable` ON SELECT 1u AS Group, "Anna" AS Name;
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_DeleteNoMatch) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                DELETE FROM `/Root/TestTable` ON SELECT 999u AS Group, "Nobody" AS Name;
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 0u);
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_ReadOnlyQuery) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/TestTable`;
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 0u);
            UNIT_ASSERT_C(HasAnyAffectedRowsField(result),
                "affected_rows field should be present (with 0) for read-only query when collect_affected_rows is enabled");
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_MultiStatement) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto result = session.ExecuteQuery(Q_(R"(
            INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
            INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (2u, "Bob", 4000u, "None");
            INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (3u, "Carl", 5000u, "None");
        )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
        UNIT_ASSERT_VALUES_EQUAL(affectedRows, 3u);
    }

    Y_UNIT_TEST(ReadCommittedRW_FullStatsMode) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto result = session.ExecuteQuery(Q_(R"(
            INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
            VALUES (1u, "Anna", 3500u, "None");
        )"), BeginReadCommittedRW(), GetQuerySettingsFull()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
        UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);
    }

    Y_UNIT_TEST(ReadCommittedRW_FullStatsMode_MultiRowInsertAndDelete) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None"), (2u, "Bob", 4000u, "None"), (3u, "Carl", 5000u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsFull()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 3u);
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                DELETE FROM `/Root/TestTable` WHERE Group >= 2u;
            )"), BeginReadCommittedRW(), GetQuerySettingsFull()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 2u);
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_NoneStatsMode) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto result = session.ExecuteQuery(Q_(R"(
            INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
            VALUES (1u, "Anna", 3500u, "None");
        )"), BeginReadCommittedRW(), GetQuerySettingsNone()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_C(!HasAnyAffectedRows(result), "affected_rows field should NOT be present with None stats mode");
    }

    Y_UNIT_TEST(SerializableRW_AffectedRows) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto result = session.ExecuteQuery(Q_(R"(
            INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
            VALUES (1u, "Anna", 3500u, "None");
        )"), BeginSerializableRW(), GetQuerySettingsBasic()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
        UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);
    }

    Y_UNIT_TEST(ReadCommittedRW_InsertFromSelect) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None"), (2u, "Bob", 4000u, "None"), (3u, "Carl", 5000u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                SELECT Group + 100u, Name, Amount, Comment FROM `/Root/TestTable`;
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 3u);
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_UpdateWithWhere) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None"), (2u, "Bob", 4000u, "None"), (3u, "Carl", 5000u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                UPDATE `/Root/TestTable` SET Amount = 9999u WHERE Group = 1u;
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_DeleteWithWhere) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None"), (2u, "Bob", 4000u, "None"), (3u, "Carl", 5000u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                DELETE FROM `/Root/TestTable` WHERE Group >= 2u;
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 2u);
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_ReturningDoesNotChangeCount) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None"), (2u, "Bob", 4000u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                UPDATE `/Root/TestTable` SET Amount = Amount + 1u RETURNING Group, Name;
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 2u);
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_FailedQuery) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

            // Failed queries may still report table_access stats with affected_rows
            // from the attempted write.
            UNIT_ASSERT_C(!HasAnyAffectedRows(result), "affected_rows field should NOT be present with failed single insert");
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_WithSecondaryIndex) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteQuery(R"(
                CREATE TABLE `/Root/TestTable` (
                    Group Uint32,
                    Name String,
                    Amount Uint32,
                    Comment String,
                    PRIMARY KEY (Group, Name),
                    INDEX IdxAmount GLOBAL ON (Amount)
                );
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None"), (2u, "Bob", 4000u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 2u);
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                UPDATE `/Root/TestTable` SET Amount = 5000u WHERE Group = 1u;
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);
        }

        {
            // UPDATE sets the same value (no-op at data level, but still counts as affected)
            auto result = session.ExecuteQuery(Q_(R"(
                UPDATE `/Root/TestTable` SET Amount = 5000u WHERE Group = 1u;
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_NoOpUpdate) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None");
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                UPDATE `/Root/TestTable` SET Amount = Amount;
            )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);
        }
    }

    Y_UNIT_TEST_TWIN(ReadCommittedRW_CollectAffectedRows, AffectedRows) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (1u, "Anna", 3500u, "None");
            )"),
                BeginReadCommittedRW(),
                AffectedRows
                ? GetQuerySettingsBasic()
                : GetQuerySettingsBasicNoAffectedRows()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, static_cast<ui64>(AffectedRows));
            UNIT_ASSERT_VALUES_EQUAL(HasAnyAffectedRowsField(result), static_cast<bool>(AffectedRows));
        }

        {
            auto result = session.ExecuteQuery(Q_(R"(
                DELETE FROM `/Root/TestTable` WHERE Group = 1u;
            )"), 
                BeginReadCommittedRW(),
                AffectedRows
                ? GetQuerySettingsBasic()
                : GetQuerySettingsBasicNoAffectedRows()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto affectedRows = GetAffectedRowsForTable(result, "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(affectedRows, static_cast<ui64>(AffectedRows));
            UNIT_ASSERT_VALUES_EQUAL(HasAnyAffectedRowsField(result), static_cast<bool>(AffectedRows));
        }
    }

    Y_UNIT_TEST(ReadCommittedRW_InteractiveTx_InsertThenDeleteSameKey) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        auto create = session.ExecuteQuery(Q_(R"(
            CREATE TABLE `/Root/repro_affected_empty` (
                id Int32 NOT NULL,
                PRIMARY KEY (id)
            );
        )"), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

        auto insert = session.ExecuteQuery(Q_(R"(
            INSERT INTO `/Root/repro_affected_empty` (id) VALUES (1);
        )"), NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::ReadCommittedRW()), GetQuerySettingsBasic()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insert.GetStatus(), EStatus::SUCCESS, insert.GetIssues().ToString());

        auto affectedRows = GetAffectedRowsForTable(insert, "/Root/repro_affected_empty");
        UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);

        auto tx = insert.GetTransaction();
        UNIT_ASSERT_C(tx.has_value(), "INSERT did not return a transaction handle");

        auto del = session.ExecuteQuery(Q_(R"(
            DELETE FROM `/Root/repro_affected_empty` WHERE id = 1;
        )"), NYdb::NQuery::TTxControl::Tx(*tx), GetQuerySettingsBasic()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(del.GetStatus(), EStatus::SUCCESS, del.GetIssues().ToString());

        affectedRows = GetAffectedRowsForTable(del, "/Root/repro_affected_empty");
        UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);

        auto commit = tx->Commit().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());

        auto leftover = session.ExecuteQuery(Q_(R"(
            SELECT id FROM `/Root/repro_affected_empty`;
        )"), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(leftover.GetStatus(), EStatus::SUCCESS, leftover.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(leftover.GetResultSet(0).RowsCount(), 0u);
    }

    Y_UNIT_TEST(ReadCommittedRW_InteractiveTx_InsertThenDeletePriorAndNewKeys) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        auto create = session.ExecuteQuery(Q_(R"(
            CREATE TABLE `/Root/repro_affected_prior` (
                id Int32 NOT NULL,
                PRIMARY KEY (id)
            );
        )"), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

        auto seed = session.ExecuteQuery(Q_(R"(
            INSERT INTO `/Root/repro_affected_prior` (id) VALUES (1), (2);
        )"), BeginReadCommittedRW(), GetQuerySettingsBasic()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(seed.GetStatus(), EStatus::SUCCESS, seed.GetIssues().ToString());

        auto affectedRows = GetAffectedRowsForTable(seed, "/Root/repro_affected_prior");
        UNIT_ASSERT_VALUES_EQUAL(affectedRows, 2u);

        auto insert = session.ExecuteQuery(Q_(R"(
            INSERT INTO `/Root/repro_affected_prior` (id) VALUES (3);
        )"), NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::ReadCommittedRW()), GetQuerySettingsBasic()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insert.GetStatus(), EStatus::SUCCESS, insert.GetIssues().ToString());

        affectedRows = GetAffectedRowsForTable(insert, "/Root/repro_affected_prior");
        UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);

        auto tx = insert.GetTransaction();
        UNIT_ASSERT_C(tx.has_value(), "INSERT did not return a transaction handle");

        auto delNew = session.ExecuteQuery(Q_(R"(
            DELETE FROM `/Root/repro_affected_prior` WHERE id = 3;
        )"), NYdb::NQuery::TTxControl::Tx(*tx), GetQuerySettingsBasic()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(delNew.GetStatus(), EStatus::SUCCESS, delNew.GetIssues().ToString());

        affectedRows = GetAffectedRowsForTable(delNew, "/Root/repro_affected_prior");
        UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);

        auto delPrior = session.ExecuteQuery(Q_(R"(
            DELETE FROM `/Root/repro_affected_prior` WHERE id = 1;
        )"), NYdb::NQuery::TTxControl::Tx(*tx), GetQuerySettingsBasic()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(delPrior.GetStatus(), EStatus::SUCCESS, delPrior.GetIssues().ToString());

        affectedRows = GetAffectedRowsForTable(delPrior, "/Root/repro_affected_prior");
        UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);

        auto commit = tx->Commit().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());

        auto leftover = session.ExecuteQuery(Q_(R"(
            SELECT id FROM `/Root/repro_affected_prior` ORDER BY id;
        )"), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(leftover.GetStatus(), EStatus::SUCCESS, leftover.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(leftover.GetResultSet(0).RowsCount(), 1u);
    }

    Y_UNIT_TEST(ReadCommittedRW_InteractiveTx_FdwStyleInsertWithoutAffectedThenDelete) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        auto create = session.ExecuteQuery(Q_(R"(
            CREATE TABLE `/Root/repro_affected_fdw` (
                id Int32 NOT NULL,
                PRIMARY KEY (id)
            );
        )"), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

        auto insert = session.ExecuteQuery(Q_(R"(
            INSERT INTO `/Root/repro_affected_fdw` (id) VALUES (1);
        )"), NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::ReadCommittedRW()), GetQuerySettingsBasicNoAffectedRows()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insert.GetStatus(), EStatus::SUCCESS, insert.GetIssues().ToString());
        UNIT_ASSERT_C(!HasAnyAffectedRowsField(insert), "no affected_rows expected without CollectAffectedRows");

        auto tx = insert.GetTransaction();
        UNIT_ASSERT_C(tx.has_value(), "INSERT did not return a transaction handle");

        auto del = session.ExecuteQuery(Q_(R"(
            DELETE FROM `/Root/repro_affected_fdw` WHERE id = 1;
        )"), NYdb::NQuery::TTxControl::Tx(*tx), GetQuerySettingsBasic()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(del.GetStatus(), EStatus::SUCCESS, del.GetIssues().ToString());

        auto affectedRows = GetAffectedRowsForTable(del, "/Root/repro_affected_fdw");
        UNIT_ASSERT_VALUES_EQUAL(affectedRows, 1u);

        auto commit = tx->Commit().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());

        auto leftover = session.ExecuteQuery(Q_(R"(
            SELECT id FROM `/Root/repro_affected_fdw`;
        )"), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(leftover.GetStatus(), EStatus::SUCCESS, leftover.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(leftover.GetResultSet(0).RowsCount(), 0u);
    }
}

} // namespace NKqp
} // namespace NKikimr
