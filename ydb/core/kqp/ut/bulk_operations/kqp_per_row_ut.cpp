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

Y_UNIT_TEST_SUITE(KqpPerRow) {
    Y_UNIT_TEST(UpdatePerRow) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            UPDATE PER ROW TestTable SET Amount = 1000 WHERE Group = 1;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST(UpdatePerRowNotIdempotent_1) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            UPDATE PER ROW TestTable SET Age = Age * 10 WHERE Group = 1;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(UpdatePerRowNotIdempotent_2) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            UPDATE PER ROW TestTable SET Age = Group * Age WHERE Group = 1;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(UpdatePerRowNotIdempotent_3) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            UPDATE PER ROW TestTable SET Age = Amount, Amount = Age WHERE Group = 1;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(UpdatePerRowMultiTable_1) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            UPDATE PER ROW TestTable SET Amount = 1000 WHERE Age IN (SELECT Age FROM TestTable WHERE Group = 1)
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(UpdatePerRowMultiTable_2) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, "TestFirst");
        CreateTestTable(session, "TestSecond");

        auto query = Q_(R"(
            UPDATE PER ROW TestFirst SET Amount = 1000 WHERE Age = 10;
            UPDATE PER ROW TestSecond SET Amount = 1000 WHERE Age = 10;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(UpdatePerRowMultiStatement) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, "TestFirst");
        CreateTestTable(session, "TestSecond");

        auto query = Q_(R"(
            UPDATE PER ROW TestFirst SET Amount = 1000 WHERE Age = 10;
            SELECT 42;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(DeletePerRow) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            DELETE PER ROW FROM TestTable WHERE Group = 2;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST(DeletePerRowMultiTable_1) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto query = Q_(R"(
            DELETE PER ROW FROM TestTable WHERE Age IN (SELECT Age FROM TestTable WHERE Group = 2)
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(DeletePerRowMultiTable_2) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, "TestFirst");
        CreateTestTable(session, "TestSecond");

        auto query = Q_(R"(
            DELETE PER ROW FROM TestFirst WHERE Age = 10;
            DELETE PER ROW FROM TestSecond WHERE Age = 10;
        )");

        auto txControl = NYdb::NQuery::TTxControl::NoTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }
}

} // namespace NKqp
} // namespace NKikimr
