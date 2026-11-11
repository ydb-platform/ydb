#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_query_helpers.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYqlTests;

Y_UNIT_TEST_SUITE(KqpRboYql) {
    Y_UNIT_TEST(PGInsertUpdate) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackOnDML(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE src (
                _q_000_f_000_type String,
                _q_000_f_000_rtref String,
                _q_000_f_000_rrref String,
                _q_000_f_001_type String,
                _q_000_f_001_rtref String,
                _q_000_f_001_rrref String,
                _q_000_f_002 String,
                _ydb_pk String NOT NULL,
                PRIMARY KEY (_ydb_pk)
            );

            CREATE TABLE dst (
                _q_000_f_000_type String,
                _q_000_f_000_rtref String,
                _q_000_f_000_rrref String,
                _q_000_f_001_type String,
                _q_000_f_001_rtref String,
                _q_000_f_001_rrref String,
                _q_000_f_002 Decimal(35,4),
                _ydb_pk Utf8 NOT NULL,
                PRIMARY KEY (_ydb_pk)
            );
        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        auto dbSession2 = client.GetSession().GetValueSync().GetSession();

        auto insertSelectRes = dbSession2.ExecuteQuery(R"(
            INSERT INTO dst (`_q_000_f_000_type`, `_q_000_f_000_rtref`, `_q_000_f_000_rrref`, `_q_000_f_001_type`, `_q_000_f_001_rtref`, `_q_000_f_001_rrref`, `_q_000_f_002`, `_ydb_pk`) 
SELECT `_q_000_f_000_type` AS `_q_000_f_000_type`, `_q_000_f_000_rtref` AS `_q_000_f_000_rtref`, `_q_000_f_000_rrref` AS `_q_000_f_000_rrref`, `_q_000_f_001_type` AS `_q_000_f_001_type`, `_q_000_f_001_rtref` AS `_q_000_f_001_rtref`, `_q_000_f_001_rrref` AS `_q_000_f_001_rrref`, CAST(`_q_000_f_002` AS Decimal(35,4)) AS `_q_000_f_002`, CAST(RandomUuid(`_ydb_pk`) AS Utf8) AS `_ydb_pk` 
FROM (
    SELECT `t3`.`_q_000_f_000_type`, `t3`.`_q_000_f_000_rtref`, `t3`.`_q_000_f_000_rrref`, `t3`.`_q_000_f_001_type`, `t3`.`_q_000_f_001_rtref`, `t3`.`_q_000_f_001_rrref`, `t3`.`_q_000_f_002`, `t3`.`_ydb_pk`
    FROM src AS `t3`
) AS `direct_base_source`
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT(insertSelectRes.IsSuccess());

    }

    Y_UNIT_TEST_TWIN(InsertSelectColumnOrder, Distinct) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackOnDML(false);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        for (const TString table : {"src", "dst"}) {
            auto result = tableSession.ExecuteSchemeQuery(TStringBuilder() << "CREATE TABLE " << table << R"( (
                _q_001_f_001_type String,
                _q_001_f_001_rrref String,
                _ydb_pk Utf8,
                PRIMARY KEY (_ydb_pk)
            );)").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto rows = NYdb::TValueBuilder()
            .BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("_q_001_f_001_type").String("TYPE")
                    .AddMember("_q_001_f_001_rrref").String("REF")
                    .AddMember("_ydb_pk").Utf8("source")
                .EndStruct()
            .EndList().Build();
        auto seed = kikimr.GetTableClient().BulkUpsert("/Root/src", std::move(rows)).GetValueSync();
        UNIT_ASSERT_C(seed.IsSuccess(), seed.GetIssues().ToString());

        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        const TString select = Distinct ? "SELECT DISTINCT " : "SELECT ";
        const TVector<TString> queries = {
            TStringBuilder() << R"(
                INSERT INTO dst (_q_001_f_001_type, _q_001_f_001_rrref, _ydb_pk)
            )" << select << R"(
                _q_001_f_001_type, _q_001_f_001_rrref, CAST("target" AS Utf8)
                FROM src;
            )",
            TStringBuilder() << R"(
                INSERT INTO dst (_ydb_pk, _q_001_f_001_type, _q_001_f_001_rrref)
            )" << select << R"(
                CAST("aliased" AS Utf8) AS z, _q_001_f_001_type AS y, _q_001_f_001_rrref AS a
                FROM src;
            )",
            TStringBuilder() << R"(
                INSERT INTO dst (_q_001_f_001_type, _q_001_f_001_rrref, _ydb_pk)
            )" << select << R"(
                _q_001_f_001_type, _q_001_f_001_rrref, CAST("limited" AS Utf8)
                FROM src ORDER BY _q_001_f_001_type LIMIT 1;
            )",
            TStringBuilder() << R"(
                INSERT INTO dst (_ydb_pk, _q_001_f_001_type, _q_001_f_001_rrref)
            )" << select << R"(
                Unwrap(CAST("union" AS Utf8)) AS _ydb_pk, _q_001_f_001_type, _q_001_f_001_rrref
                FROM src
                UNION
            )" << select << R"(
                CAST("union" AS Utf8) AS _ydb_pk, _q_001_f_001_type, _q_001_f_001_rrref
                FROM src ORDER BY _ydb_pk LIMIT 1;
            )",
        };
        const auto before = GetNewRBOCompileCounters(kikimr);
        for (const auto& query : queries) {
            auto insert = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
        }
        const auto after = GetNewRBOCompileCounters(kikimr);
        UNIT_ASSERT_C(after.first > before.first, "INSERT must use the new RBO");
        UNIT_ASSERT_VALUES_EQUAL(after.second, before.second);

        auto result = session.ExecuteQuery(R"(
            SELECT _q_001_f_001_type, _q_001_f_001_rrref, _ydb_pk FROM dst ORDER BY _ydb_pk;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)),
            R"([[["TYPE"];["REF"];["aliased"]];[["TYPE"];["REF"];["limited"]];[["TYPE"];["REF"];["target"]];[["TYPE"];["REF"];["union"]]])");
    }

    Y_UNIT_TEST(InsertUpdate) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackOnDML(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE src (
                id Uint64 NOT NULL,
                v Int64,
                PRIMARY KEY (id)
            );

            CREATE TABLE dst (
                id Uint64 NOT NULL,
                v Int64,
                PRIMARY KEY (id)
            );

            ALTER TABLE `/Root/dst` ADD INDEX Index1 GLOBAL ON (id, v);
            ALTER TABLE `/Root/dst` ADD INDEX Index12 GLOBAL ON (v);

        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        auto dbSession2 = client.GetSession().GetValueSync().GetSession();

        NYdb::TValueBuilder rows;
        rows.BeginList();
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Uint64(1)
            .AddMember("v").Int64(10)
            .EndStruct();
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Uint64(2)
            .AddMember("v").Int64(20)
            .EndStruct();
        rows.EndList();

        auto result = db.BulkUpsert("/Root/src", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto insertSelectRes = dbSession2.ExecuteQuery(R"(
            --PRAGMA YqlSelect = "disable";
            INSERT INTO dst (id, v)
            SELECT id, v
            FROM src;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT(insertSelectRes.IsSuccess());

        auto selectRes = dbSession2.ExecuteQuery(R"(
            SELECT id, v
            FROM dst;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(selectRes.GetResultSet(0)), R"([[1u;[10]];[2u;[20]]])");

        auto updateSelectRes = dbSession2.ExecuteQuery(R"(
            --PRAGMA YqlSelect = "disable";
            UPDATE dst ON
            SELECT id, v
            FROM src;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT(updateSelectRes.IsSuccess());

        selectRes = dbSession2.ExecuteQuery(R"(
            SELECT id, v
            FROM dst;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(selectRes.GetResultSet(0)), R"([[1u;[10]];[2u;[20]]])");

        auto deleteRes = dbSession2.ExecuteQuery(R"(
            --PRAGMA YqlSelect = "disable";
            DELETE FROM dst ON
            SELECT id, v
            FROM src;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT(deleteRes.IsSuccess());

        selectRes = dbSession2.ExecuteQuery(R"(
            SELECT id, v
            FROM dst;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(selectRes.GetResultSet(0)), R"([])");

    }
}

} // namespace NKikimr::NKqp
