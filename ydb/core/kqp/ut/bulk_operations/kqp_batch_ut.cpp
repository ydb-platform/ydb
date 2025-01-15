#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <format>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

static NKikimrConfig::TAppConfig GetAppConfig() {
    auto app = NKikimrConfig::TAppConfig();
    app.MutableTableServiceConfig()->SetEnableOlapSink(true);
    app.MutableTableServiceConfig()->SetEnableOltpSink(true);
    return app;
}

static NYdb::NQuery::TExecuteQuerySettings GetQuerySettings() {
    NYdb::NQuery::TExecuteQuerySettings execSettings;
    execSettings.StatsMode(NYdb::NQuery::EStatsMode::Basic);
    return execSettings;
}

static void CreateTestTable(auto session, const TString& name = "TestTable") {
    UNIT_ASSERT(session.ExecuteQuery(TStringBuilder() << R"(
        CREATE TABLE `)" << name << R"(` (
            Group Uint32,
            Name String,
            Age Uint64,
            Amount Uint64,
            Comment String,
            PRIMARY KEY (Group, Name)
        );)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync().IsSuccess());

    auto result = session.ExecuteQuery(TStringBuilder() << R"(
        UPSERT INTO `/Root/)" << name << R"(` (Group, Name, Age, Amount, Comment) VALUES
                (1u, "Anna", 23ul, 3500ul, "None"),
                (1u, "Paul", 36, 300ul, "None"),
                (2u, "Tony", 81, 7200ul, "None");
    )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

Y_UNIT_TEST_SUITE(KqpBatch) {
    Y_UNIT_TEST(Update) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            BATCH UPDATE TestTable SET Amount = 1000 WHERE Group = 1;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST(UpdateNotIdempotent_1) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            BATCH UPDATE TestTable SET Age = Age * 10 WHERE Group = 1;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(UpdateNotIdempotent_2) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            BATCH UPDATE TestTable SET Age = Group * Age WHERE Group = 1;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(UpdateNotIdempotent_3) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            BATCH UPDATE TestTable SET Age = Amount, Amount = Age WHERE Group = 1;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(UpdateMultiTable_1) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            BATCH UPDATE TestTable SET Amount = 1000 WHERE Age IN (SELECT Age FROM TestTable WHERE Group = 1);
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(UpdateMultiTable_2) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, "TestFirst");
        CreateTestTable(session, "TestSecond");

        auto query = Q_(R"(
            BATCH UPDATE TestFirst SET Amount = 1000 WHERE Age IN (SELECT Age FROM TestSecond WHERE Group = 1);
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(UpdateMultiTable_3) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, "TestFirst");
        CreateTestTable(session, "TestSecond");

        auto query = Q_(R"(
            BATCH UPDATE TestFirst SET Amount = 1000 WHERE Age = 10;
            BATCH UPDATE TestSecond SET Amount = 1000 WHERE Age = 10;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(UpdateMultiStatement_1) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            BATCH UPDATE TestTable SET Amount = 1000 WHERE Age = 10;
            SELECT 42;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(UpdateMultiStatement_2) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, "TestTable");

        auto query = Q_(R"(
            BATCH UPDATE TestTable SET Amount = 1000 WHERE Age = 10;
            UPSERT INTO `/Root/TestTable` (Group, Name, Age, Amount, Comment)
                VALUES (7u, "Mark", 74ul, 200ul, "None");
            SELECT * FROM TestTable;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(Delete) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            BATCH DELETE FROM TestTable WHERE Group = 2;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST(DeleteMultiTable_1) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            BATCH DELETE FROM TestTable WHERE Age IN (SELECT Age FROM TestTable WHERE Group = 2);
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(DeleteMultiTable_2) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, "TestFirst");
        CreateTestTable(session, "TestSecond");

        auto query = Q_(R"(
            BATCH DELETE FROM TestFirst WHERE Age IN (SELECT Age FROM TestSecond WHERE Group = 2);
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(DeleteMultiTable_3) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, "TestFirst");
        CreateTestTable(session, "TestSecond");

        auto query = Q_(R"(
            BATCH DELETE FROM TestFirst WHERE Age = 10;
            BATCH DELETE FROM TestSecond WHERE Age = 10;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(DeleteMultiStatement_1) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            BATCH DELETE FROM TestTable WHERE Group = 2;
            SELECT 42;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(DeleteMultiStatement_2) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            BATCH DELETE FROM TestTable WHERE Group = 2;
            UPSERT INTO `/Root/TestTable` (Group, Name, Age, Amount, Comment)
                VALUES (7u, "Mark", 74ul, 200ul, "None");
            SELECT * FROM TestTable;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }
}

} // namespace NKqp
} // namespace NKikimr
