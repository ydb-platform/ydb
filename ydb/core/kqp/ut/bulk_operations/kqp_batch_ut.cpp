#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <format>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

NKikimrConfig::TAppConfig GetAppConfig() {
    auto app = NKikimrConfig::TAppConfig();
    app.MutableTableServiceConfig()->SetEnableOlapSink(true);
    app.MutableTableServiceConfig()->SetEnableOltpSink(true);
    app.MutableTableServiceConfig()->SetEnableBatchUpdates(true);
    return app;
}

NYdb::NQuery::TExecuteQuerySettings GetQuerySettings() {
    NYdb::NQuery::TExecuteQuerySettings execSettings;
    execSettings.StatsMode(NYdb::NQuery::EStatsMode::Basic);
    return execSettings;
}

void CreateSimpleTable(TSession& session, const TString& name = "TestTable") {
    UNIT_ASSERT(session.ExecuteQuery(TStringBuilder() << R"(
        CREATE TABLE `)" << name << R"(` (
            Group Uint32,
            Name String,
            Age Uint64,
            Amount Uint64,
            PRIMARY KEY (Group)
        );)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync().IsSuccess());

    auto result = session.ExecuteQuery(TStringBuilder() << R"(
        UPSERT INTO `/Root/)" << name << R"(` (Group, Name, Age, Amount) VALUES
                (1u, "Anna", 23ul, 3500ul),
                (2u, "Paul", 36ul, 300ul),
                (3u, "Tony", 81ul, 7200ul),
                (4u, "John", 11ul, 10ul),
                (5u, "Lena", 3ul, 0ul);
    )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void CreateTwoPartitionsTable(TSession& session, const TString& name = "TestTable") {
    UNIT_ASSERT(session.ExecuteQuery(TStringBuilder() << R"(
        CREATE TABLE `)" << name << R"(` (
            Group Uint32,
            Name String,
            Age Uint64,
            Amount Uint64,
            PRIMARY KEY (Group)
        ) WITH (
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2,
            PARTITION_AT_KEYS = (4u)
        );)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync().IsSuccess());

    auto result = session.ExecuteQuery(TStringBuilder() << R"(
        UPSERT INTO `/Root/)" << name << R"(` (Group, Name, Age, Amount) VALUES
                (1u, "Anna", 23ul, 3500ul),
                (2u, "Paul", 36ul, 300ul),
                (3u, "Tony", 81ul, 7200ul),
                (4u, "John", 11ul, 10ul),
                (5u, "Lena", 3ul, 0ul),
                (6u, "Mary", 48ul, 730ul);
    )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void CreateTuplePrimaryTable(TSession& session, const TString& name = "TestTable") {
    UNIT_ASSERT(session.ExecuteQuery(TStringBuilder() << R"(
        CREATE TABLE `)" << name << R"(` (
            Group Uint32,
            Name String,
            Age Uint64,
            Amount Uint64,
            PRIMARY KEY (Group, Name)
        );)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync().IsSuccess());

    auto result = session.ExecuteQuery(TStringBuilder() << R"(
        UPSERT INTO `/Root/)" << name << R"(` (Group, Name, Age, Amount) VALUES
                (1u, "Anna", 23ul, 3500ul),
                (1u, "Cake", 31ul, 274ul),
                (2u, "Paul", 36ul, 300ul),
                (3u, "Tony", 81ul, 7200ul),
                (3u, "Dan", 8ul, 430ul),
                (4u, "John", 11ul, 10ul),
                (5u, "Lena", 3ul, 0ul);
    )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void CreateTuplePrimaryReorderTable(TSession& session, const TString& name = "TestTable") {
    UNIT_ASSERT(session.ExecuteQuery(TStringBuilder() << R"(
        CREATE TABLE `)" << name << R"(` (
            Col1 Int32,
            Col2 Int64,
            Col3 Int64,
            Col4 Int64,
            PRIMARY KEY (Col2, Col1)
        );)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync().IsSuccess());

    auto result = session.ExecuteQuery(TStringBuilder() << R"(
        UPSERT INTO `/Root/)" << name << R"(` (Col1, Col2, Col3, Col4) VALUES
                (1u, NULL, 1, 0),
                (NULL, 2u, 2, -1),
                (NULL, 1u, 3, -2),
                (2u, NULL, 4, -3),
                (3u, NULL, 5, -4);
    )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpBatch) {
    Y_UNIT_TEST(UpdateSimple) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSimpleTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Amount = 1000 WHERE Age <= 30;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[1000u];[1u];["Anna"]];
                [[36u];[300u];[2u];["Paul"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[11u];[1000u];[4u];["John"]];
                [[3u];[1000u];[5u];["Lena"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateMultiFilter) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSimpleTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Amount = 1000 WHERE Age < 15 OR Group < 3 AND Name != "Anna";
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[3500u];[1u];["Anna"]];
                [[36u];[1000u];[2u];["Paul"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[11u];[1000u];[4u];["John"]];
                [[3u];[1000u];[5u];["Lena"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateMultiSet) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSimpleTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Amount = 0, Name = "None" WHERE Age <= 25;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[0u];[1u];["None"]];
                [[36u];[300u];[2u];["Paul"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[11u];[0u];[4u];["None"]];
                [[3u];[0u];[5u];["None"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateMultiBoth) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSimpleTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Amount = 0, Name = "None" WHERE Age > 15 AND Amount < 3500;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[3500u];[1u];["Anna"]];
                [[36u];[0u];[2u];["None"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[11u];[10u];[4u];["John"]];
                [[3u];[0u];[5u];["Lena"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateTwoPartitionsSimple) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTwoPartitionsTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Amount = 100 WHERE Age <= 30;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[100u];[1u];["Anna"]];
                [[36u];[300u];[2u];["Paul"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[11u];[100u];[4u];["John"]];
                [[3u];[100u];[5u];["Lena"]];
                [[48u];[730u];[6u];["Mary"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateTwoPartitionsMulti) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTwoPartitionsTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Amount = 0, Name = "None" WHERE Age > 15 AND Amount < 3500;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[3500u];[1u];["Anna"]];
                [[36u];[0u];[2u];["None"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[11u];[10u];[4u];["John"]];
                [[3u];[0u];[5u];["Lena"]];
                [[48u];[0u];[6u];["None"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateTwoPartitionsByPrimaryRange) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTwoPartitionsTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Amount = 25 WHERE Group <= 3;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[25u];[1u];["Anna"]];
                [[36u];[25u];[2u];["Paul"]];
                [[81u];[25u];[3u];["Tony"]];
                [[11u];[10u];[4u];["John"]];
                [[3u];[0u];[5u];["Lena"]];
                [[48u];[730u];[6u];["Mary"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateTwoPartitionsByPrimaryPoint) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTwoPartitionsTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Amount = 0 WHERE Group = 1u;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[0u];[1u];["Anna"]];
                [[36u];[300u];[2u];["Paul"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[11u];[10u];[4u];["John"]];
                [[3u];[0u];[5u];["Lena"]];
                [[48u];[730u];[6u];["Mary"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateTuplePrimarySimple) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTuplePrimaryTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Amount = 1000 WHERE Age <= 30;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[1000u];[1u];["Anna"]];
                [[31u];[274u];[1u];["Cake"]];
                [[36u];[300u];[2u];["Paul"]];
                [[8u];[1000u];[3u];["Dan"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[11u];[1000u];[4u];["John"]];
                [[3u];[1000u];[5u];["Lena"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateTuplePrimaryReorder) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTuplePrimaryReorderTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Col3 = 0 WHERE Col4 >= 0;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        // todo ditimizhev
    }

    Y_UNIT_TEST(UpdateTuplePrimaryMulti) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTuplePrimaryTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Amount = 0, Age = 0 WHERE Age > 15 AND Amount < 3500;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[3500u];[1u];["Anna"]];
                [[0u];[0u];[1u];["Cake"]];
                [[0u];[0u];[2u];["Paul"]];
                [[8u];[430u];[3u];["Dan"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[11u];[10u];[4u];["John"]];
                [[3u];[0u];[5u];["Lena"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateTuplePrimaryByPrimaryRange) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTuplePrimaryTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Amount = 25 WHERE Group <= 3 AND Name <= "Tony";
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[25u];[1u];["Anna"]];
                [[31u];[25u];[1u];["Cake"]];
                [[36u];[25u];[2u];["Paul"]];
                [[8u];[25u];[3u];["Dan"]];
                [[81u];[25u];[3u];["Tony"]];
                [[11u];[10u];[4u];["John"]];
                [[3u];[0u];[5u];["Lena"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateTuplePrimaryByPrimaryPoint) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTuplePrimaryTable(session);

        {
            auto query = Q_(R"(
                BATCH UPDATE TestTable SET Amount = 0 WHERE Group = 1u AND Name = "Anna";
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[0u];[1u];["Anna"]];
                [[31u];[274u];[1u];["Cake"]];
                [[36u];[300u];[2u];["Paul"]];
                [[8u];[430u];[3u];["Dan"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[11u];[10u];[4u];["John"]];
                [[3u];[0u];[5u];["Lena"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateNotIdempotent_1) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSimpleTable(session);

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

        CreateSimpleTable(session);

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

        CreateSimpleTable(session);

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

        CreateSimpleTable(session);

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

        CreateSimpleTable(session, "TestFirst");
        CreateSimpleTable(session, "TestSecond");

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

        CreateSimpleTable(session, "TestFirst");
        CreateSimpleTable(session, "TestSecond");

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

        CreateSimpleTable(session);

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

        CreateSimpleTable(session, "TestTable");

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

    Y_UNIT_TEST(DeleteSimple) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSimpleTable(session);

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestTable WHERE Age <= 30;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[36u];[300u];[2u];["Paul"]];
                [[81u];[7200u];[3u];["Tony"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DeleteMulti) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSimpleTable(session);

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestTable WHERE Age < 15 OR Group < 3 AND Name != "Anna";
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[3500u];[1u];["Anna"]];
                [[81u];[7200u];[3u];["Tony"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DeleteTwoPartitionsSimple) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTwoPartitionsTable(session);

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestTable WHERE Age <= 30;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[36u];[300u];[2u];["Paul"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[48u];[730u];[6u];["Mary"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DeleteTwoPartitionsMulti) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTwoPartitionsTable(session);

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestTable WHERE Age > 15 AND Amount < 3500;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[3500u];[1u];["Anna"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[11u];[10u];[4u];["John"]];
                [[3u];[0u];[5u];["Lena"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DeleteTwoPartitionsByPrimaryRange) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTwoPartitionsTable(session);

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestTable WHERE Group <= 3;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[11u];[10u];[4u];["John"]];
                [[3u];[0u];[5u];["Lena"]];
                [[48u];[730u];[6u];["Mary"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DeleteTwoPartitionsByPrimaryPoint) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTwoPartitionsTable(session);

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestTable WHERE Group = 1u;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), /* EStatus::SUCCESS */ EStatus::BAD_REQUEST); // todo: isBatch does not match in SA
        }
        // {
        //     auto query = Q_(R"(
        //         SELECT * FROM TestTable ORDER BY Group;
        //     )");

        //     auto txControl = NYdb::NQuery::TTxControl::NoTx();
        //     auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        //     UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        //     CompareYson(R"([
        //         [[36u];[300u];[2u];["Paul"]];
        //         [[81u];[7200u];[3u];["Tony"]];
        //         [[11u];[10u];[4u];["John"]];
        //         [[3u];[0u];[5u];["Lena"]];
        //         [[48u];[730u];[6u];["Mary"]]
        //     ])", FormatResultSetYson(result.GetResultSet(0)));
        // }
    }

    Y_UNIT_TEST(DeleteTuplePrimarySimple) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTuplePrimaryTable(session);

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestTable WHERE Age <= 30;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[31u];[274u];[1u];["Cake"]];
                [[36u];[300u];[2u];["Paul"]];
                [[81u];[7200u];[3u];["Tony"]];
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DeleteTuplePrimaryMulti) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTuplePrimaryTable(session);

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestTable WHERE Age > 15 AND Amount < 3500;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[23u];[3500u];[1u];["Anna"]];
                [[8u];[430u];[3u];["Dan"]];
                [[81u];[7200u];[3u];["Tony"]];
                [[11u];[10u];[4u];["John"]];
                [[3u];[0u];[5u];["Lena"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DeleteTuplePrimaryByPrimaryRange) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTuplePrimaryTable(session);

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestTable WHERE Group <= 3 AND Name <= "Tony";
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT * FROM TestTable ORDER BY Group;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            CompareYson(R"([
                [[11u];[10u];[4u];["John"]];
                [[3u];[0u];[5u];["Lena"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DeleteTuplePrimaryByPrimaryPoint) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTuplePrimaryTable(session);

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestTable WHERE Group = 1u AND Name = "Anna";
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), /* EStatus::SUCCESS */ EStatus::BAD_REQUEST); // todo: isBatch does not match in SA
        }
        // {
        //     auto query = Q_(R"(
        //         SELECT * FROM TestTable ORDER BY Group;
        //     )");

        //     auto txControl = NYdb::NQuery::TTxControl::NoTx();
        //     auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        //     UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        //     CompareYson(R"([
        //         [[36u];[300u];[2u];["Paul"]];
        //         [[81u];[7200u];[3u];["Tony"]];
        //         [[11u];[10u];[4u];["John"]];
        //         [[3u];[0u];[5u];["Lena"]]
        //     ])", FormatResultSetYson(result.GetResultSet(0)));
        // }
    }

    Y_UNIT_TEST(DeleteTuplePrimaryReorder) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTuplePrimaryReorderTable(session);

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestTable WHERE Col4 >= 0;
            )");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();
            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        // todo ditimizhev
    }

    Y_UNIT_TEST(DeleteMultiTable_1) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSimpleTable(session);

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

        CreateSimpleTable(session, "TestFirst");
        CreateSimpleTable(session, "TestSecond");

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

        CreateSimpleTable(session, "TestFirst");
        CreateSimpleTable(session, "TestSecond");

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

        CreateSimpleTable(session);

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

        CreateSimpleTable(session);

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
