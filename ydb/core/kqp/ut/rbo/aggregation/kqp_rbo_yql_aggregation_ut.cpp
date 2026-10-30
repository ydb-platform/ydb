#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/kqp/ut/olap/helpers/test_case.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/simple/settings.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_plan_conversion_utils.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>
#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_trace_output.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <yql/essentials/core/pg_settings/guc_settings.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/string/split.h>

#include <algorithm>
#include <array>
#include <ctime>
#include <optional>
#include <regex>
#include <fstream>
#include <utility>

// These headers import NKikimr::NKqp into the global namespace, so include them last
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_aggregation_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_join_builder.h>


namespace NKikimr::NKqp {
namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYql::NNodes;
using namespace NStat;

void TestAggregation(bool columnStore) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableNewRBOPhysicalStagePeephole(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    TString withColumnstore = R"(WITH (Store = COLUMN);)";
    TString t1 = R"(CREATE TABLE `/Root/t1` (
            a Int64	NOT NULL,
	            b Int64,
            c Int64,
            d Int64,
            primary key(a)
        ))";
    TString t2 = R"(CREATE TABLE `/Root/t2` (
            a Int64 NOT NULL,
	            b Int64,
            c Int64,
            d Decimal(14, 3),
            e Decimal(12, 2) NOT NULL,
            primary key(a)
        ))";
    if (columnStore) {
        t1 += withColumnstore;
        t2 += withColumnstore;
    } else {
        t1 += ";";
        t2 += ";";
    }

    Y_ENSURE(session.ExecuteSchemeQuery(t1).GetValueSync().IsSuccess());
    Y_ENSURE(session.ExecuteSchemeQuery(t2).GetValueSync().IsSuccess());

    db = kikimr.GetTableClient();
    auto session2 = db.CreateSession().GetValueSync().GetSession();

    const std::vector<std::string> queriesOnEmptyColumns = {
        R"(
            select count(*) from `/Root/t1` as t1;
        )",
        // non optional, optional coumn
        R"(
            select count(t1.a), count(t1.b) from `/Root/t1` as t1;
        )",
        R"(
            select sum(t1.a), sum(t1.b) from `/Root/t1` as t1;
        )",
        R"(
            select min(t1.a), min(t1.b) from `/Root/t1` as t1;
        )",
        R"(
            select avg(t1.a), avg(t1.b) from `/Root/t1` as t1;
        )",
        R"(
            SELECT stddev_samp(t1.a), stddev_samp(t1.b) from `/Root/t1` as t1;
        )",
        R"(
            select some(t1.a), some(t1.b) from `/Root/t1` as t1;
        )",
        R"(
            select sum(distinct t1.a), max(distinct t1.b) from `/Root/t1` as t1;
        )",
        R"(
            select count(distinct t1.a), count(distinct t1.b) from `/Root/t1` as t1;
        )",
        R"(
            select avg(distinct t1.a), avg(distinct t1.b) from `/Root/t1` as t1;
        )",
        R"(
            select avg(distinct t2.e), avg(distinct t2.d) from `/Root/t2` as t2;
        )"
    };

    const std::vector<std::string> resultsEmptyColumns = {
        R"([[0u]])",
        R"([[0u;0u]])",
        R"([[#;#]])",
        R"([[#;#]])",
        R"([[#;#]])",
        R"([[#;#]])",
        R"([[#;#]])",
        R"([[#;#]])",
        R"([[0u;0u]])",
        R"([[#;#]])",
        R"([[#;#]])"
    };

    for (ui32 i = 0; i < queriesOnEmptyColumns.size(); ++i) {
        const auto& query = queriesOnEmptyColumns[i];
        auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), resultsEmptyColumns[i]);
    }

    NYdb::TValueBuilder rowsTableT1;
    rowsTableT1.BeginList();
    for (size_t i = 0; i < 5; ++i) {
        rowsTableT1.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i & 1 ? 1 : 2)
            .AddMember("c").Int64(2)
            .AddMember("d").Int64(i % 3)
            .EndStruct();
    }
    rowsTableT1.EndList();

    auto resultUpsert = db.BulkUpsert("/Root/t1", rowsTableT1.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    NYdb::TValueBuilder rowsTableT2;
    rowsTableT2.BeginList();
    for (size_t i = 0; i < 5; ++i) {
        rowsTableT2.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i & 1 ? 1 : 2)
            .AddMember("c").Int64(2)
            .AddMember("d").Decimal(TDecimalValue(ToString(i + 0.1), 14, 3))
            .AddMember("e").Decimal(TDecimalValue(ToString(i + 0.2), 12, 2))
            .EndStruct();
    }
    rowsTableT2.EndList();

    resultUpsert = db.BulkUpsert("/Root/t2", rowsTableT2.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    std::vector<std::string> queries = {
        R"(
            select t2.b, sum(t2.d), sum(t2.e) from `/Root/t2` as t2 group by t2.b order by t2.b;
        )",
        R"(
            select t2.b, min(t2.d), max(t2.e) from `/Root/t2` as t2 group by t2.b order by t2.b;
        )",
        R"(
            select t2.b, count(t2.d), count(t2.e) from `/Root/t2` as t2 group by t2.b order by t2.b;
        )",
        R"(
            select t2.b, avg(t2.d), avg(t2.e) from `/Root/t2` as t2 group by t2.b order by t2.b;
        )",
        R"(
            select t1.b, sum(t1.c) from `/Root/t1` as t1 group by t1.b order by t1.b;
        )",
        R"(
            select t1.b, sum(t1.c) from `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a group by t1.b order by t1.b;
        )",
        R"(
            select t1.b, min(t1.a) from `/Root/t1` as t1 group by t1.b order by t1.b;
        )",
        R"(
            select t1.b, max(t1.a) from `/Root/t1` as t1 group by t1.b order by t1.b;
        )",
        R"(
            select t1.b, count(t1.a) from `/Root/t1` as t1 group by t1.b order by t1.b;
        )",
        R"(
            select max(t1.b) as maxb, min(t1.a) from `/Root/t1` as t1 order by maxb;
        )",
        R"(
            select sum(t1.a) as suma from `/Root/t1` as t1 group by t1.b, t1.c order by suma;
        )",
        R"(
            select sum(t1.c), t1.b from `/Root/t1` as t1 group by t1.b order by t1.b;
        )",
        R"(
            select max(t1.a) as maxa, min(t1.a), min(t1.b) as min_b from `/Root/t1` as t1 order by maxa;
        )",
        R"(
            select sum(t1.a + 1 + t1.c) as sumExpr0, sum(t1.c + 2) as sumExpr1 from `/Root/t1` as t1 group by t1.b order by sumExpr0;
        )",
        R"(
            select sum(distinct t1.b) as sum, t1.a from `/Root/t1` as t1 group by t1.a order by sum, t1.a;
        )",
        R"(
            select sum(t1.a) + 1, t1.b from `/Root/t1` as t1 group by t1.b order by t1.b;
        )",
        R"(
            select count(distinct t1.a), t1.b from `/Root/t1` as t1 group by t1.b, t1.c order by t1.b;
        )",
        R"(
            select avg(t1.b) from `/Root/t1` as t1;
        )",
        R"(
            select avg(t1.a) as avgA, avg(t1.c) as avgC from `/Root/t1` as t1 group by t1.b;
        )",
        R"(
            select sum(t1.b) as sumb from `/Root/t1` as t1 group by t1.b order by sumb;
        )",
        R"(
            select count(*), sum(t1.a) as result from `/Root/t1` as t1 order by result;
        )",
        R"(
            select count(*) as result from `/Root/t1` as t1 order by result;
        )",
        R"(
            select t1.b, count(*) from `/Root/t1` as t1 group by t1.b order by t1.b;
        )",
        R"(
            select count(*) from `/Root/t1` as t1 group by t1.b order by t1.b;
        )",
        R"(
            select
                   sum(case when t1.b > 0
                        then 1
                        else 0 end) as count1,
                   sum(case when t1.b < 0
                        then 1
                        else 0 end) count2 from `/Root/t1` as t1 group by t1.b order by count1, count2;
        )",
        R"(
             select max(t1.a), min(t1.a) from `/Root/t1` as t1;
        )",
        R"(
             select max(t1.a), min(t1.a) from `/Root/t1` as t1 group by t1.b order by t1.b;
        )",
        R"(
             select max(t1.a), min(t1.a) from `/Root/t1` as t1 group by t1.a order by t1.a;
        )",
        R"(
             select max(t1.a) from `/Root/t1` as t1 group by t1.b, t1.a order by t1.a, t1.b;
        )",
        R"(
            PRAGMA YqlSelectAllowUnnamedGroupByExpr;
            select count(*) from `/Root/t1` as t1 group by t1.b + 1 order by t1.b + 1;
        )",
        R"(
            PRAGMA YqlSelectAllowUnnamedGroupByExpr;
            select sum(t1.c) as sum0, sum(t1.a + 3) as sum1 from `/Root/t1` as t1 group by t1.b + 1 order by sum0;
        )",
        R"(
            PRAGMA YqlSelectAllowUnnamedGroupByExpr;
            select sum(t1.c + 2) as sum0 from `/Root/t1` as t1 group by t1.b + t1.a order by sum0;
        )",
        R"(
            PRAGMA YqlSelectAllowUnnamedGroupByExpr;
            select
                   sum(case when t1.a > 0
                        then 1
                        else 0 end) +
                   sum(case when t1.a < 0
                        then 1
                        else 0 end) + 1, sum(t1.a) as r, t1.b + 2 as group_key from `/Root/t1` as t1 group by t1.b + 2 order by r;
        )",
        R"(
            PRAGMA YqlSelectAllowUnnamedGroupByExpr;
            select sum(t1.c) as sum0, t1.b + 1, t1.c + 2 from `/Root/t1` as t1 group by t1.b + 1, t1.c + 2 order by sum0;
        )",
        R"(
            PRAGMA YqlSelectAllowUnnamedGroupByExpr;
            select sum(t1.c) as sum0, t1.b, t1.c from `/Root/t1` as t1 group by t1.b, t1.c order by sum0;
        )",
        R"(
            pragma YqlSelect = "force";
            select distinct sum(t1.c) as sum_c, sum(t1.a) as sum_b, t1.b from `/Root/t1` as t1 group by t1.b order by sum_c;
        )",
        R"(
            pragma YqlSelect = "force";
            select distinct min(t1.a) as min_a, max(t1.a) as max_a, t1.b from `/Root/t1` as t1 group by t1.b order by min_a;
        )",
        R"(
            pragma YqlSelect = "force";
            select distinct (t1.a + t1.b) as res from `/Root/t1` as t1 order by res;
        )",
        R"(
            pragma YqlSelect = "force";
            select distinct (t1.b + 1) as res from `/Root/t1` as t1 order by res;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select distinct t1.a, t1.b from `/Root/t1` as t1 order by t1.a, t1.b;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select max(distinct t1.a), min(distinct t1.b) from `/Root/t1` as t1;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select max(t1.a), min(distinct t1.b) from `/Root/t1` as t1;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select max(distinct t1.a), min(t1.b) from `/Root/t1` as t1;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select sum(distinct t1.a), sum(t1.a) from `/Root/t1` as t1;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select sum(distinct t1.b), sum(t1.b) from `/Root/t1` as t1;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select sum(distinct t1.a) as r0, sum(t1.c) as r1 from `/Root/t1` as t1 group by t1.b order by r0, r1;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select sum(distinct t1.c) as r0, sum(t1.a) as r1 from `/Root/t1` as t1 group by t1.b order by r0, r1;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select sum(distinct t1.c) as r0, sum(t1.a) as r1 from `/Root/t1` as t1 group by t1.b order by r0, r1;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select count(distinct t1.a) as r0, count(t1.a) as r1 from `/Root/t1` as t1 group by t1.b order by r0, r1;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select count(distinct t1.a) as r0, count(distinct t1.c) as r1 from `/Root/t1` as t1 group by t1.b order by r0, r1;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select min(distinct t1.a) as r0, max(distinct t1.c) as r1 from `/Root/t1` as t1 group by t1.b order by r0, r1;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select avg(distinct t1.a) as r0, avg(distinct t1.c) as r1 from `/Root/t1` as t1 group by t1.b order by t1.b;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select distinct coalesce(t1.a, 0) as a, coalesce(t1.b, 1) as b, unwrap(t1.c) as c from `/Root/t1` as t1 order by a, b, c;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select count(distinct t1.a) as r0, count(t1.b) as r1, count(t1.c) as r2 from `/Root/t1` as t1 order by r0, r1, r2;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            select count(distinct t1.a) as r0, count(distinct t1.c) as r1, count(t1.d) as r2 from `/Root/t1` as t1 group by t1.b order by r0, r1, r2;
        )",
        // GROUP BY without aggregation functions must still emit one row per distinct key.
        R"(
            PRAGMA YqlSelect = 'force';
            select t1.b from `/Root/t1` as t1 group by t1.b order by t1.b;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;
            select t1.b, t2.c from `/Root/t1` as t1, `/Root/t2` as t2 where t1.b = t2.b group by t1.b, t2.c order by t1.b limit 100;
        )",
        R"(
            PRAGMA YqlSelectAllowUnnamedGroupByExpr;
            select t1.d + 1 as k from `/Root/t1` as t1 group by t1.d + 1 order by k;
        )",
    };

    std::vector<std::string> results = {
                                        R"([[[1];["4.2"];"4.4"];[[2];["6.3"];"6.6"]])",
                                        R"([[[1];["1.1"];"3.2"];[[2];["0.1"];"4.2"]])",
                                        R"([[[1];2u;2u];[[2];3u;3u]])",
                                        R"([[[1];["2.1"];"2.2"];[[2];["2.1"];"2.2"]])",
                                        R"([[[1];[4]];[[2];[6]]])",
                                        R"([[[1];[4]];[[2];[6]]])",
                                        R"([[[1];1];[[2];0]])",
                                        R"([[[1];3];[[2];4]])",
                                        R"([[[1];2u];[[2];3u]])",
                                        R"([[[2];[0]]])",
                                        R"([[4];[6]])",
                                        R"([[[4];[1]];[[6];[2]]])",
                                        R"([[[4];[0];[1]]])",
                                        R"([[[10];[8]];[[15];[12]]])",
                                        R"([[[1];1];[[1];3];[[2];0];[[2];2];[[2];4]])",
                                        R"([[5;[1]];[7;[2]]])",
                                        R"([[2u;[1]];[3u;[2]]])",
                                        R"([[[1.6]]])",
                                        R"([[2.;[2.]];[2.;[2.]]])",
                                        R"([[[2]];[[6]]])",
                                        R"([[5u;[10]]])",
                                        R"([[5u]])",
                                        R"([[[1];2u];[[2];3u]])",
                                        R"([[2u];[3u]])",
                                        R"([[2;0];[3;0]])",
                                        R"([[[4];[0]]])",
                                        R"([[3;1];[4;0]])",
                                        R"([[0;0];[1;1];[2;2];[3;3];[4;4]])",
                                        R"([[0];[1];[2];[3];[4]])",
                                        R"([[2u];[3u]])",
                                        R"([[[4];10];[[6];15]])",
                                        R"([[[4]];[[8]];[[8]]])",
                                        R"([[3;4;[3]];[3;6;[4]]])",
                                        R"([[[4];[2];[4]];[[6];[3];[4]]])",
                                        R"([[[4];[1];[2]];[[6];[2];[2]]])",
                                        R"([[[4];4;[1]];[[6];6;[2]]])",
                                        R"([[0;4;[2]];[1;3;[1]]])",
                                        R"([[[2]];[[4]];[[6]]])",
                                        R"([[[2]];[[3]]])",
                                        R"([[0;[2]];[1;[1]];[2;[2]];[3;[1]];[4;[2]]])",
                                        R"([[[4];[1]]])",
                                        R"([[[4];[1]]])",
                                        R"([[[4];[1]]])",
                                        R"([[[10];[10]]])",
                                        R"([[[3];[8]]])",
                                        R"([[4;[4]];[6;[6]]])",
                                        R"([[[2];4];[[2];6]])",
                                        R"([[[2];4];[[2];6]])",
                                        R"([[2u;2u];[3u;3u]])",
                                        R"([[2u;1u];[3u;1u]])",
                                        R"([[0;[2]];[1;[2]]])",
                                        R"([[2.;[2.]];[2.;[2.]]])",
                                        R"([[0;2;2];[1;1;2];[2;2;2];[3;1;2];[4;2;2]])",
                                        R"([[5u;5u;5u]])",
                                        R"([[2u;1u;2u];[3u;1u;3u]])",
                                        R"([[[1]];[[2]]])",
                                        R"([[[1];[2]];[[2];[2]]])",
                                        R"([[[1]];[[2]];[[3]]])"
                                    };

    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto &query = queries[i];
        //Cout << query << Endl;
        auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
    }

    NYdb::TValueBuilder rowsTableT1More;
    rowsTableT1More.BeginList();
    for (size_t i = 5; i < 20; ++i) {
        rowsTableT1More.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i & 1 ? 1 : 2)
            .AddMember("c").Int64(2)
            .AddMember("d").Int64(3)
            .EndStruct();
    }
    rowsTableT1More.EndList();

    auto resultUpsertMore = db.BulkUpsert("/Root/t1", rowsTableT1More.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsertMore.IsSuccess(), resultUpsertMore.GetIssues().ToString());

    queries = {
        R"(
            PRAGMA YqlSelect = 'force';
            SELECT stddev_samp(t1.a) as res0, stddev_samp(t1.b) as res1 from `/Root/t1` as t1 order by res0, res1;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            SELECT stddev_samp(t1.a) as res, t1.b from `/Root/t1` as t1 group by t1.b order by t1.b;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            SELECT stddev_samp(t1.b) as res, t1.c from `/Root/t1` as t1 group by t1.c order by res;
        )",
    };

    results = {
        R"([[[5.916079783];[0.512989176]]])",
        R"([[6.055300708;[1]];[6.055300708;[2]]])",
        R"([[[0.512989176];[2]]])"
    };

    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto &query = queries[i];
        //Cout << query << Endl;
        auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
   }
}

void RunWindowFunctionsTest(const bool newRbo, const bool columnStore, const bool aggregates, TVector<TString>& names,
                            TVector<TString>& results, TVector<TString>& issues) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const TString schema = TStringBuilder() << R"(
        CREATE TABLE `/Root/t1` (
            a Int64 NOT NULL,
            b Int64,
            c Int64,
            d Int64,
            e Int64,
            f Decimal(22,9),
            g Utf8,
            h Double,
            PRIMARY KEY (a)
        )
    )" << (columnStore ? " WITH (Store = Column);" : ";");
    auto schemeResult = tableSession.ExecuteSchemeQuery(schema).GetValueSync();
    UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

    {
        // a, b (partition), c (order), d (second partition), e (measure); nullopt is NULL.
        using TCell = std::optional<i64>;
        const TVector<std::tuple<i64, TCell, TCell, TCell, TCell>> rowData = {
            { 1, 1,       10,       100,      5},           // ordinary partition
            { 2, 1,       20,       100,      3},
            { 3, 1,       20,       200,      7},           // tie on the order key c
            { 4, 1,       30,       200,      std::nullopt}, // NULL measure inside a partition
            { 5, 1,       40,       100,      5},           // tie on the measure e
            { 6, 2,       10,       100,      -2},          // negative measure
            { 7, 2,       20,       100,      0},           // zero measure
            { 8, 2,       30,       200,      8},
            { 9, 3,       10,       100,      std::nullopt}, // partition where every measure is NULL
            {10, 3,       20,       100,      std::nullopt},
            {11, 4,       10,       100,      42},          // single row partition
            {12, std::nullopt, 10,  100,      1},           // NULL partition key ...
            {13, std::nullopt, 20,  100,      2},           // ... both rows form one partition
            {14, 5,       std::nullopt, 100,  3},           // NULL order key
            {15, 5,       10,       100,      4},
            {16, 5,       10,       200,      6},           // tie on c, split by the second key d
            {17, 6,       10,       std::nullopt, 9},       // NULL secondary partition key
            {18, 6,       20,       std::nullopt, 11},
            {19, 6,       30,       100,      13},
            {20, 7,       10,       100,      1000000000},  // large measure
        };

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (const auto& [a, b, c, d, e] : rowData) {
            auto addCell = [](NYdb::TValueBuilder& builder, const TString& name, const TCell& cell) {
                builder.AddMember(name);
                if (cell) {
                    builder.BeginOptional().Int64(*cell).EndOptional();
                } else {
                    builder.EmptyOptional(NYdb::EPrimitiveType::Int64);
                }
            };
            rows.AddListItem().BeginStruct();
            rows.AddMember("a").Int64(a);
            addCell(rows, "b", b);
            addCell(rows, "c", c);
            addCell(rows, "d", d);
            addCell(rows, "e", e);
            rows.AddMember("f");
            if (e) {
                rows.BeginOptional().Decimal(NYdb::TDecimalValue(TStringBuilder() << *e << ".5", 22, 9)).EndOptional();
            } else {
                rows.EmptyOptional(NYdb::TTypeBuilder().Decimal(NYdb::TDecimalType(22, 9)).Build());
            }
            const THashMap<i64, TString> names = {{10, "ddd"}, {20, "aaa"}, {30, "ccc"}, {40, "bbb"}};
            rows.AddMember("g");
            if (c) {
                rows.BeginOptional().Utf8(names.at(*c)).EndOptional();
            } else {
                rows.EmptyOptional(NYdb::EPrimitiveType::Utf8);
            }
            rows.AddMember("h");
            if (c) {
                rows.BeginOptional().Double(*c / 8.0).EndOptional();
            } else {
                rows.EmptyOptional(NYdb::EPrimitiveType::Double);
            }
            rows.EndStruct();
        }
        rows.EndList();

        auto seedResult = kikimr.GetTableClient().BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(seedResult.IsSuccess(), seedResult.GetIssues().ToString());
    }

    const TVector<std::pair<TString, TString>> queries = {
        {"partitioned rank", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c, Sum(e) AS sales,
                Rank() OVER (PARTITION BY b ORDER BY Sum(e) DESC) AS rank_in_group
            FROM `/Root/t1`
            GROUP BY b, c
            ORDER BY b, c;
        )"},
        {"partitioned sum", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c, Sum(e) AS sales,
                Sum(Sum(e)) OVER (PARTITION BY b) AS total_sales
            FROM `/Root/t1`
            GROUP BY b, c
            ORDER BY b, c;
        )"},
        {"partitioned average", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c, Sum(e) AS sales,
                Avg(Sum(e)) OVER (PARTITION BY b) AS average_sales
            FROM `/Root/t1`
            GROUP BY b, c
            ORDER BY b, c;
        )"},
       {"global rank", R"(
            PRAGMA YqlSelect = "force";

            SELECT c, Sum(e) AS sales,
                Rank() OVER (ORDER BY Sum(e) ASC) AS global_rank
            FROM `/Root/t1`
            GROUP BY c
            ORDER BY c;
        )"},
        {"rank with rollup partition expression", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c, Sum(e) AS sales,
                Rank() OVER (
                    PARTITION BY
                        Grouping(b) + Grouping(c),
                        CASE WHEN Grouping(c) == 0 THEN b ELSE NULL END
                    ORDER BY Sum(e) ASC, c DESC
                ) AS rank_within_parent
            FROM `/Root/t1`
            GROUP BY ROLLUP(b, c)
            ORDER BY b, c, sales;
        )"},
        {"cumulative sum", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c, Sum(e) AS sales,
                Sum(Sum(e)) OVER (
                    PARTITION BY b
                    ORDER BY c
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_sales
            FROM `/Root/t1`
            GROUP BY b, c
            ORDER BY b, c;
        )"},
        {"cumulative maximum of a value", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Max(e) OVER (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_max
            FROM `/Root/t1`
            ORDER BY a;
        )"},
        {"sliding frame ending at the current row", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS sliding_sum,
                Max(e) OVER w AS sliding_max
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            )
            ORDER BY a;
        )"},
        {"centred frame", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS centred_sum,
                Max(e) OVER w AS centred_max
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            )
            ORDER BY a;
        )"},
        {"forward looking frame", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS ahead_sum,
                Min(e) OVER w AS ahead_min
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )
            ORDER BY a;
        )"},
        {"trailing frame", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS trailing_sum,
                Max(e) OVER w AS trailing_max
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
            ORDER BY a;
        )"},
        {"suffix frame", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS suffix_sum,
                Max(e) OVER w AS suffix_max
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
            ORDER BY a;
        )"},
        {"explicit whole partition frame", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS partition_sum,
                Max(e) OVER w AS partition_max
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
            ORDER BY a;
        )"},
        {"range whole partition frame", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS partition_sum,
                Max(e) OVER w AS partition_max
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
                RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
            ORDER BY a;
        )"},
        {"range default frame written out", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS range_sum,
                Min(e) OVER w AS range_min
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
                RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            ORDER BY a;
        )"},
        {"range default frame implied", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS range_sum,
                Min(e) OVER w AS range_min
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
            )
            ORDER BY a;
        )"},
        {"range frame with ties", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS range_sum,
                Count(e) OVER w AS range_count
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
                RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            ORDER BY a;
        )"},
        {"running decimal aggregates", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, f,
                Sum(f) OVER w AS running_sum,
                Avg(f) OVER w AS running_avg,
                Min(f) OVER w AS running_min,
                Max(f) OVER w AS running_max
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            ORDER BY a;
        )"},
        {"whole partition decimal aggregates", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, f,
                Sum(f) OVER w AS partition_sum,
                Avg(f) OVER w AS partition_avg,
                Count(f) OVER w AS partition_count
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
            ORDER BY a;
        )"},
        {"range decimal aggregates", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, f,
                Sum(f) OVER w AS range_sum,
                Avg(f) OVER w AS range_avg
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
            )
            ORDER BY a;
        )"},
        {"range frame over a decimal order key", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, f, e,
                Sum(e) OVER w AS range_sum,
                Count(e) OVER w AS range_count
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY f DESC
            )
            ORDER BY a;
        )"},
        {"forward frame count and average", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Count(e) OVER w AS ahead_count,
                Avg(e) OVER w AS ahead_avg
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING
            )
            ORDER BY a;
        )"},
        {"ranking with a centred frame", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                RowNumber() OVER w AS row_number_in_group,
                Rank() OVER w AS rank_in_group,
                Sum(e) OVER w AS centred_sum
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            )
            ORDER BY a;
        )"},
        {"global sliding frame", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, e,
                Sum(e) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sliding_sum
            FROM `/Root/t1`
            ORDER BY a;
        )"},
        {"centred decimal average", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, f,
                Avg(f) OVER w AS centred_avg,
                Max(f) OVER w AS centred_max
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            )
            ORDER BY a;
        )"},
        {"running frame reaching ahead", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS ahead_sum,
                Count(e) OVER w AS ahead_count
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING
            )
            ORDER BY a;
        )"},
        {"trailing frame count with ranking", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Rank() OVER w AS rank_in_group,
                Count(e) OVER w AS trailing_count,
                Avg(e) OVER w AS trailing_avg
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING
            )
            ORDER BY a;
        )"},
        {"suffix aggregates with ranking", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e, f,
                RowNumber() OVER w AS row_number_in_group,
                Count(e) OVER w AS suffix_count,
                Min(e) OVER w AS suffix_min,
                Avg(f) OVER w AS suffix_avg
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
            ORDER BY a;
        )"},
        {"global whole partition frame", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, e,
                Sum(e) OVER () AS total,
                Max(e) OVER () AS maximum
            FROM `/Root/t1`
            ORDER BY a;
        )"},
        {"global suffix frame", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, e,
                Sum(e) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS suffix_sum
            FROM `/Root/t1`
            ORDER BY a;
        )"},
        {"range frame around the current value", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS around_sum,
                Count(e) OVER w AS around_count
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
                RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING
            )
            ORDER BY a;
        )"},
        {"range frame ending at the current value", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS recent_sum,
                Max(e) OVER w AS recent_max
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
                RANGE BETWEEN 10 PRECEDING AND CURRENT ROW
            )
            ORDER BY a;
        )"},
        {"range frame ending before the current value", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Count(e) OVER w AS earlier_count,
                Avg(e) OVER w AS earlier_avg
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
                RANGE BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING
            )
            ORDER BY a;
        )"},
        {"range frame reaching ahead", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS ahead_sum,
                Rank() OVER w AS rank_in_group
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
                RANGE BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING
            )
            ORDER BY a;
        )"},
        {"range frame after the current value", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Count(e) OVER w AS later_count,
                Min(e) OVER w AS later_min
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
                RANGE BETWEEN 5 FOLLOWING AND 15 FOLLOWING
            )
            ORDER BY a;
        )"},
        {"range suffix frame", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS suffix_sum,
                DenseRank() OVER w AS dense_rank_in_group
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
                RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
            ORDER BY a;
        )"},
        {"descending range frame with offsets", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Sum(e) OVER w AS recent_sum
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c DESC
                RANGE BETWEEN 10 PRECEDING AND CURRENT ROW
            )
            ORDER BY a;
        )"},
        {"global range frame with offsets", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, e,
                Sum(e) OVER (ORDER BY a RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS nearby_sum
            FROM `/Root/t1`
            ORDER BY a;
        )"},
        {"ranking over an order expression", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, d,
                Rank() OVER w AS rank_in_group,
                RowNumber() OVER w AS row_number_in_group
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY Abs(c - d), a
            )
            ORDER BY a;
        )"},
        {"range frame over an order expression", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, d, e,
                Sum(e) OVER w AS range_sum
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY Abs(c - d)
            )
            ORDER BY a;
        )"},
        {"rows frame over an order expression", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, d, e,
                Sum(e) OVER w AS centred_sum
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY Abs(c - d), a
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            )
            ORDER BY a;
        )"},
        {"range offsets over an order expression", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, d, e,
                Sum(e) OVER w AS nearby_sum
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY Abs(c - d)
                RANGE BETWEEN 50 PRECEDING AND CURRENT ROW
            )
            ORDER BY a;
        )"},
        {"range offsets over a double order key", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, h, e,
                Sum(e) OVER w AS nearby_sum,
                Count(e) OVER w AS nearby_count
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY h
                RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
            )
            ORDER BY a;
        )"},
        {"range offsets ending before a double order key", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, h, e,
                Sum(e) OVER w AS earlier_sum
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY h
                RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
            ORDER BY a;
        )"},
        {"count star over different frames", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c,
                Count(*) OVER (PARTITION BY b) AS partition_rows,
                Count(*) OVER (PARTITION BY b ORDER BY c) AS rows_so_far,
                Count(*) OVER (PARTITION BY b ORDER BY c, a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS nearby_rows,
                Count(*) OVER (PARTITION BY b ORDER BY c RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS recent_rows
            FROM `/Root/t1`
            ORDER BY a;
        )"},
        {"range frame over a string order key", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, g, e,
                Sum(e) OVER w AS range_sum,
                Count(e) OVER w AS range_count
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY g
            )
            ORDER BY a;
        )"},
        {"range frame over a descending string order key", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, g, e,
                Sum(e) OVER w AS range_sum,
                Min(e) OVER w AS range_min
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY g DESC
            )
            ORDER BY a;
        )"},
        {"range frame over two order keys", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, d, e, c,
                Sum(c) OVER w AS range_sum,
                Count(c) OVER w AS range_count
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY d, e
            )
            ORDER BY a;
        )"},
        {"ranking and range aggregates over two order keys", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, d, e,
                Rank() OVER w AS rank_in_group,
                DenseRank() OVER w AS dense_rank_in_group,
                Sum(c) OVER w AS range_sum
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY d DESC, e
            )
            ORDER BY a;
        )"},
        {"global range frame over a string order key", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, g, e,
                Sum(e) OVER (ORDER BY g) AS range_sum
            FROM `/Root/t1`
            ORDER BY a;
        )"},
        {"ranking over a string order key", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, g,
                Rank() OVER w AS rank_in_group,
                DenseRank() OVER w AS dense_rank_in_group
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY g
            )
            ORDER BY a;
        )"},
        {"running sum over a string order key", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, g, e,
                Sum(e) OVER w AS running_sum
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY g, a
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            ORDER BY a;
        )"},
        {"multi column partition", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, d, Sum(e) AS sales,
                Avg(Sum(e)) OVER (PARTITION BY b, d) AS avg_sales
            FROM `/Root/t1`
            GROUP BY b, d, c
            ORDER BY b, d, sales;
        )"},
        {"multi column order", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c, d, Sum(e) AS sales,
                Rank() OVER (
                    PARTITION BY b
                    ORDER BY d ASC, c DESC
                ) AS rank_in_group
            FROM `/Root/t1`
            GROUP BY b, c, d
            ORDER BY b, c, d;
        )"},
        {"two windows sharing a specification", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Max(e) OVER (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_max,
                Min(e) OVER (
                    PARTITION BY b
                    ORDER BY c, a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_min
            FROM `/Root/t1`
            ORDER BY a;
        )"},
        {"named window shared by several functions", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c, e,
                Rank() OVER w AS rank_in_group,
                Max(e) OVER w AS running_max,
                Sum(e) OVER w AS running_sum
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c, a
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            ORDER BY a;
        )"},
        {"named window shared by several functions over aggregates", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c, Sum(e) AS sales,
                Rank() OVER w AS rank_in_group,
                Sum(Sum(e)) OVER w AS running_sales,
                Max(Sum(e)) OVER w AS running_max
            FROM `/Root/t1`
            GROUP BY b, c
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
            )
            ORDER BY b, c;
        )"},
        {"named window without an order", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c, Sum(e) AS sales,
                Avg(Sum(e)) OVER w AS avg_sales,
                Sum(Sum(e)) OVER w AS total_sales
            FROM `/Root/t1`
            GROUP BY b, c
            WINDOW w AS (PARTITION BY b)
            ORDER BY b, c;
        )"},
        {"two windows with different specifications", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c, Sum(e) AS sales,
                Avg(Sum(e)) OVER (PARTITION BY b) AS avg_sales,
                Rank() OVER (PARTITION BY b ORDER BY c) AS rank_in_group
            FROM `/Root/t1`
            GROUP BY b, c
            ORDER BY b, c;
        )"},
        {"window result inside an expression", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c,
                Sum(e) * 100 / Sum(Sum(e)) OVER (PARTITION BY b) AS revenue_ratio
            FROM `/Root/t1`
            GROUP BY b, c
            ORDER BY b, c;
        )"},
        {"window over a windowed subquery", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c, cumulative_sales,
                Max(cumulative_sales) OVER (
                    PARTITION BY b
                    ORDER BY c
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS peak_cumulative_sales
            FROM (
                SELECT b, c,
                    Sum(Sum(e)) OVER (
                        PARTITION BY b
                        ORDER BY c
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS cumulative_sales
                FROM `/Root/t1`
                GROUP BY b, c
            )
            ORDER BY b, c;
        )"},
        {"filter on a window result", R"(
            PRAGMA YqlSelect = "force";
            PRAGMA OrderedColumns;

            SELECT * FROM (
                SELECT b, c, Sum(e) AS sales,
                    Rank() OVER (PARTITION BY b ORDER BY Sum(e) DESC) AS rank_in_group
                FROM `/Root/t1`
                GROUP BY b, c
            ) WHERE rank_in_group <= 10
            ORDER BY b, c;
        )"},
        {"sort and limit above a window", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c, sales, rank_in_group FROM (
                SELECT b, c, Sum(e) AS sales,
                    Rank() OVER (PARTITION BY b ORDER BY Sum(e) DESC) AS rank_in_group
                FROM `/Root/t1`
                GROUP BY b, c
            ) ORDER BY rank_in_group, sales, b, c LIMIT 100;
        )"},
        {"join on a window result", R"(
            PRAGMA YqlSelect = "force";

            SELECT x.b AS b, x.rank_in_group AS asc_rank, y.rank_in_group AS desc_rank
            FROM (
                SELECT b, c, Rank() OVER (PARTITION BY b ORDER BY c ASC) AS rank_in_group
                FROM `/Root/t1`
            ) AS x
            JOIN (
                SELECT b, c, Rank() OVER (PARTITION BY b ORDER BY c DESC) AS rank_in_group
                FROM `/Root/t1`
            ) AS y
            ON x.b == y.b AND x.rank_in_group == y.rank_in_group
            ORDER BY b, asc_rank;
        )"},
        {"input names matching physical window temporaries", R"(
            PRAGMA YqlSelect = "force";

            SELECT `__kqp_win_acc_0_`, `__kqp_win_pos_1_`, `__kqp_win_peer_0_`,
                Sum(`__kqp_win_acc_0_`) OVER w AS total,
                Rank() OVER w AS rank
            FROM (
                SELECT b + 2 AS `__kqp_win_acc_0_`, a + 1 AS `__kqp_win_pos_1_`,
                    c + 1 AS `__kqp_win_peer_0_`
                FROM `/Root/t1`
            )
            WINDOW w AS (ORDER BY `__kqp_win_peer_0_`)
            ORDER BY `__kqp_win_pos_1_`;
        )"},
        {"two global windows in one select", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c,
                Rank() OVER (ORDER BY c ASC) AS asc_rank,
                Rank() OVER (ORDER BY c DESC) AS desc_rank
            FROM `/Root/t1`
            ORDER BY a;
        )"},
        {"rank over group by without aggregates", R"(
            PRAGMA YqlSelect = "force";

            SELECT b, c, Rank() OVER (PARTITION BY b ORDER BY c) AS rnk
            FROM `/Root/t1`
            GROUP BY b, c
            ORDER BY b, c;
        )"},
    };

    const TVector<std::pair<TString, TString>> aggregateQueries = {
        {"whole partition aggregates", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b,
                Sum(e) OVER w AS total,
                Min(e) OVER w AS min_e,
                Max(e) OVER w AS max_e,
                Avg(e) OVER w AS avg_e,
                Count(e) OVER w AS cnt
            FROM `/Root/t1`
            WINDOW w AS (PARTITION BY b)
            ORDER BY a;
        )"},
        {"whole partition aggregates over a not null measure", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b,
                Sum(a) OVER w AS total,
                Min(a) OVER w AS min_a,
                Max(a) OVER w AS max_a,
                Avg(a) OVER w AS avg_a,
                Count(a) OVER w AS cnt
            FROM `/Root/t1`
            WINDOW w AS (PARTITION BY b)
            ORDER BY a;
        )"},
        {"whole partition aggregates over a not null measure without a partition", R"(
            PRAGMA YqlSelect = "force";

            SELECT a,
                Sum(a) OVER () AS total,
                Max(a) OVER () AS max_a,
                Avg(a) OVER () AS avg_a
            FROM `/Root/t1`
            ORDER BY a;
        )"},
        {"interval sum without a partition", R"(
            PRAGMA YqlSelect = "force";

            SELECT a,
                Sum(Interval("PT1S")) OVER () AS total
            FROM `/Root/t1`
            ORDER BY a;
        )"},
        {"running aggregates over a not null measure", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b,
                Sum(a) OVER w AS total,
                Min(a) OVER w AS min_a,
                Avg(a) OVER w AS avg_a
            FROM `/Root/t1`
            WINDOW w AS (PARTITION BY b ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            ORDER BY a;
        )"},
        {"range running aggregates over a not null measure", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c,
                Sum(a) OVER w AS total,
                Max(a) OVER w AS max_a
            FROM `/Root/t1`
            WINDOW w AS (PARTITION BY b ORDER BY c)
            ORDER BY a;
        )"},
        {"sliding frame over a not null measure", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b,
                Sum(a) OVER w AS total,
                Max(a) OVER w AS max_a
            FROM `/Root/t1`
            WINDOW w AS (PARTITION BY b ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
            ORDER BY a;
        )"},
        {"whole partition aggregates without a partition", R"(
            PRAGMA YqlSelect = "force";

            SELECT a,
                Sum(e) OVER () AS total,
                Max(c) OVER () AS max_c,
                Count(*) OVER () AS cnt
            FROM `/Root/t1`
            ORDER BY a;
        )"},
        {"whole partition range frame over two partition keys", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, d, e,
                Sum(e) OVER w AS total,
                Avg(f) OVER w AS avg_f
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b, d
                ORDER BY c
                RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
            ORDER BY a;
        )"},
        {"ranking and whole partition aggregates in one window", R"(
            PRAGMA YqlSelect = "force";

            SELECT a, b, c,
                Rank() OVER w AS rank_in_group,
                Sum(e) OVER w AS total,
                Count(e) OVER w AS cnt
            FROM `/Root/t1`
            WINDOW w AS (
                PARTITION BY b
                ORDER BY c
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
            ORDER BY a;
        )"},
    };

    for (const auto& [name, query] : aggregates ? aggregateQueries : queries) {
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        names.push_back(name);
        if (result.IsSuccess()) {
            results.push_back(TString{FormatResultSetYson(result.GetResultSet(0))});
        } else {
            results.push_back({});
            issues.push_back(TStringBuilder() << name << ": " << result.GetIssues().ToString());
        }
    }
}

const THashSet<TString> WindowQueriesNotLoweredYet{
};

void CompareWindowFunctionsWithOldOptimizer(const bool columnStore, const bool aggregates) {
    TVector<TString> oldNames, oldResults, oldIssues;
    RunWindowFunctionsTest(/*newRbo=*/false, columnStore, aggregates, oldNames, oldResults, oldIssues);
    UNIT_ASSERT_VALUES_EQUAL_C(oldIssues.size(), 0, "The old optimizer must run every window query: "
                                                        << JoinSeq("; ", oldIssues));

    TVector<TString> newNames, newResults, newIssues;
    RunWindowFunctionsTest(/*newRbo=*/true, columnStore, aggregates, newNames, newResults, newIssues);
    UNIT_ASSERT_VALUES_EQUAL(oldNames.size(), newNames.size());

    const TString table = columnStore ? "column" : "row";
    for (ui32 i = 0; i < oldNames.size(); ++i) {
        const auto& name = oldNames[i];
        const bool lowered = !newResults[i].empty();
        if (WindowQueriesNotLoweredYet.contains(name)) {
            UNIT_ASSERT_C(!lowered, "'" << name << "' now runs with the New RBO on a " << table
                                        << " table, remove it from WindowQueriesNotLoweredYet");
            continue;
        }
        UNIT_ASSERT_C(lowered, "The New RBO must run '" << name << "' on a " << table << " table: "
                                                        << JoinSeq("; ", newIssues));
        UNIT_ASSERT_VALUES_EQUAL_C(newResults[i], oldResults[i],
                                   "New RBO returned different rows for '" << name << "' on a " << table << " table");
    }
}

void InsertIntoSchema1(NYdb::NTable::TTableClient& db, std::string tableName, ui32 numRows) {
    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < numRows; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i + 1)
            .AddMember("c").Int64(i + 2)
            .AddMember("d").Int64(i + 3)
            .AddMember("e").Int64(i + 4)
            .EndStruct();
    }
    rows.EndList();
    auto resultUpsert = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());
}

} // namespace

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpRboYql) {

    // A scalar aggregate yields one row even when none of its results is used.
    Y_UNIT_TEST(ScalarAggregatesWithoutUsedResults) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (a Int64 NOT NULL, primary key(a));
            CREATE TABLE `/Root/t2` (a Int64 NOT NULL, b Int64, primary key(a));
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder t1;
        t1.BeginList();
        for (const i64 a : {1, 3}) {
            t1.AddListItem().BeginStruct().AddMember("a").Int64(a).EndStruct();
        }
        t1.EndList();
        auto upsertResult = db.BulkUpsert("/Root/t1", t1.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder t2;
        t2.BeginList();
        for (const i64 a : {1, 2}) {
            t2.AddListItem().BeginStruct().AddMember("a").Int64(a).AddMember("b").OptionalInt64(a * 10).EndStruct();
        }
        t2.EndList();
        upsertResult = db.BulkUpsert("/Root/t2", t2.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        const std::vector<std::pair<TString, TString>> cases = {
            {"SELECT COUNT(*) AS c FROM (SELECT MAX(a) AS m FROM `/Root/t1` UNION ALL SELECT MAX(b) AS m FROM `/Root/t2`);",
             "[[2u]]"},
            {"SELECT t1.a FROM `/Root/t1` AS t1 WHERE EXISTS (SELECT MAX(t2.b) FROM `/Root/t2` AS t2) ORDER BY t1.a;",
             "[[1];[3]]"},
            {"SELECT t1.a FROM `/Root/t1` AS t1 WHERE EXISTS (SELECT MAX(t2.b) FROM `/Root/t2` AS t2 WHERE t2.a = t1.a) ORDER BY t1.a;",
             "[[1];[3]]"},
            {"SELECT t1.a FROM `/Root/t1` AS t1 WHERE NOT EXISTS (SELECT MAX(t2.b) FROM `/Root/t2` AS t2 WHERE t2.a = t1.a) ORDER BY t1.a;",
             "[]"},
            {"SELECT 1 AS one FROM (SELECT COUNT(*) AS c FROM `/Root/t1`);",
             "[[1]]"},
        };
        for (const auto& [query, expected] : cases) {
            auto result = session.ExecuteDataQuery(TString("PRAGMA YqlSelect = 'force';\n") + query,
                TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, query);
        }
    }

    Y_UNIT_TEST_TWIN(Aggregation, ColumnStore) {
        TestAggregation(ColumnStore);
    }

    Y_UNIT_TEST_TWIN(WindowFunctions, ColumnStore) {
        CompareWindowFunctionsWithOldOptimizer(ColumnStore, /*aggregates=*/false);
    }

    Y_UNIT_TEST_TWIN(WindowAggregates, ColumnStore) {
        CompareWindowFunctionsWithOldOptimizer(ColumnStore, /*aggregates=*/true);
    }

    Y_UNIT_TEST_TWIN(WindowSortWithoutBlocksUnderWindowFunctionsV2, WindowFunctionsV2) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableWindowFunctionsV2(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                c Int64,
                PRIMARY KEY (a)
            ) WITH (Store = Column);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        const TString query = TStringBuilder() << R"(
            PRAGMA YqlSelect = "force";
            PRAGMA ydb.WindowFunctionsV2 = ")" << (WindowFunctionsV2 ? "true" : "false") << R"(";

            SELECT a, Sum(c) OVER (PARTITION BY b ORDER BY c) AS s
            FROM `/Root/t1`;
        )";

        auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
        auto result = kikimr.GetQueryClient().ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetAst(), "AST is not available");
        const TString ast(*result.GetStats()->GetAst());

        UNIT_ASSERT_C(ast.Contains("WideChopper"), ast);
        if (WindowFunctionsV2) {
            UNIT_ASSERT_C(!ast.Contains("WideSortBlocks"), ast);
            UNIT_ASSERT_C(ast.Contains("(WideSort "), ast);
        } else {
            UNIT_ASSERT_C(ast.Contains("WideSortBlocks"), ast);
        }
    }

    Y_UNIT_TEST_TWIN(WholePartitionWindowAsAggregateJoin, WindowFunctionsV2) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                c Int64,
                PRIMARY KEY (a)
            ) WITH (Store = Column);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        {
            using TCell = std::optional<i64>;
            const TVector<std::tuple<i64, TCell, TCell>> rowData = {
                {1, 1, 10},
                {2, 1, 20},
                {3, 2, 5},
                {4, std::nullopt, 7},
                {5, std::nullopt, std::nullopt},
            };

            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [a, b, c] : rowData) {
                rows.AddListItem().BeginStruct();
                rows.AddMember("a").Int64(a);
                for (const auto& [name, cell] : {std::pair{"b", b}, std::pair{"c", c}}) {
                    rows.AddMember(name);
                    if (cell) {
                        rows.BeginOptional().Int64(*cell).EndOptional();
                    } else {
                        rows.EmptyOptional(NYdb::EPrimitiveType::Int64);
                    }
                }
                rows.EndStruct();
            }
            rows.EndList();

            auto seedResult = kikimr.GetTableClient().BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(seedResult.IsSuccess(), seedResult.GetIssues().ToString());
        }

        auto client = kikimr.GetQueryClient();
        const TString query = TStringBuilder() << R"(
            PRAGMA YqlSelect = "force";
            PRAGMA ydb.WindowFunctionsV2 = ")" << (WindowFunctionsV2 ? "true" : "false") << R"(";

            SELECT a,
                Sum(c) OVER (PARTITION BY b) AS s,
                Count(a) OVER (PARTITION BY b) AS cnt,
                Max(a) OVER (PARTITION BY b) AS max_a
            FROM `/Root/t1`
            ORDER BY a;
        )";

        auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
        auto explain = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(explain.GetStatus(), EStatus::SUCCESS, explain.GetIssues().ToString());
        UNIT_ASSERT_C(explain.GetStats() && explain.GetStats()->GetAst(), "AST is not available");
        const TString ast(*explain.GetStats()->GetAst());

        if (WindowFunctionsV2) {
            UNIT_ASSERT_C(!ast.Contains("WideChopper"), ast);
            UNIT_ASSERT_C(!ast.Contains("Fold1"), ast);
        } else {
            UNIT_ASSERT_C(ast.Contains("WideChopper"), ast);
            UNIT_ASSERT_C(ast.Contains("Fold1"), ast);
        }

        auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [1;[30];2u;2];
            [2;[30];2u;2];
            [3;[5];1u;3];
            [4;[7];2u;5];
            [5;[7];2u;5]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(Having) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                c Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        NYdb::TValueBuilder rowsTableT1;
        rowsTableT1.BeginList();
        for (size_t i = 0; i < 10; ++i) {
            rowsTableT1.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i & 1 ? 1 : 2)
                .AddMember("c").Int64(i + 1)
                .EndStruct();
        }
        rowsTableT1.EndList();

        auto resultUpsert = db.BulkUpsert("/Root/t1", rowsTableT1.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.c), t1.b FROM `/Root/t1` as t1 group by t1.b having sum(t1.c) > 0 order by t1.b;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.c), t1.b FROM `/Root/t1` as t1 group by t1.b having sum(t1.c) < 10 order by t1.b;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.c), t1.b FROM `/Root/t1` as t1 group by t1.b having sum(t1.a) >= 1 and sum(t1.c) <= 10 order by t1.b;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.c), t1.a FROM `/Root/t1` as t1 group by t1.a having sum(t1.c) > 1 and sum(t1.c) < 3 order by t1.a;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a), t1.c FROM `/Root/t1` as t1 group by t1.c having sum(t1.a + 1) >= 1 order by t1.c;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a), t1.c FROM `/Root/t1` as t1 group by t1.c having sum(t1.a) + 2 >= 2 order by t1.c;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a), t1.c FROM `/Root/t1` as t1 group by t1.c having sum(t1.a + 3) + 2 >= 5 order by t1.c;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a), t1.c FROM `/Root/t1` as t1 group by t1.c having sum(t1.a + 1) + sum(t1.a + 2) >= 5 order by t1.c;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a + 1) + 11, t1.c FROM `/Root/t1` as t1 group by t1.c having sum(t1.a + 1) + sum(t1.a + 2) >= 5 order by t1.c;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a) as a_sum FROM `/Root/t1` as t1 having sum(t1.a) >= 5 order by a_sum;
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a) FROM `/Root/t1` as t1 having sum(t1.b) >= 5 order by sum(t1.a)
            )",
            R"(
                PRAGMA YqlSelect = 'force';
                SELECT sum(t1.a) FROM `/Root/t1` as t1 group by t1.c having sum(t1.b) >= 5 order by t1.c;
            )",
        };

        std::vector<std::string> results = {
            R"([[[30];[1]];[[25];[2]]])",
            R"([])",
            R"([])",
            R"([[[2];1]])",
            R"([[0;[1]];[1;[2]];[2;[3]];[3;[4]];[4;[5]];[5;[6]];[6;[7]];[7;[8]];[8;[9]];[9;[10]]])",
            R"([[0;[1]];[1;[2]];[2;[3]];[3;[4]];[4;[5]];[5;[6]];[6;[7]];[7;[8]];[8;[9]];[9;[10]]])",
            R"([[0;[1]];[1;[2]];[2;[3]];[3;[4]];[4;[5]];[5;[6]];[6;[7]];[7;[8]];[8;[9]];[9;[10]]])",
            R"([[1;[2]];[2;[3]];[3;[4]];[4;[5]];[5;[6]];[6;[7]];[7;[8]];[8;[9]];[9;[10]]])",
            R"([[13;[2]];[14;[3]];[15;[4]];[16;[5]];[17;[6]];[18;[7]];[19;[8]];[20;[9]];[21;[10]]])",
            R"([[[45]]])",
            R"([[[45]]])",
            R"([])",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

    Y_UNIT_TEST(Rollup) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                c Int64,
                d Int64,
                e Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b Int64,
                c Int64,
                d Int64,
                e Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 4}, {"/Root/t2", 3}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema1(db, table, rowsNum);
        }

        const std::vector<std::string> queries = {
            R"(
                SELECT count(t1.a), t1.b as b FROM `/Root/t1` as t1
                group by rollup(t1.b)
                order by b;
            )",
            R"(
                SELECT count(t1.b), t1.a as a FROM `/Root/t1` as t1
                group by rollup(t1.a)
                order by a;
            )",
            R"(
                SELECT count(t1.a), t1.b as b, t1.c as c FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c)
                order by b, c;
            )",
            R"(
                SELECT count(t1.a), t1.b as b, t1.c as c, t1.d as d FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c, t1.d)
                order by b, c, d;
            )",
            R"(
                SELECT count(t1.a), t1.b as b, t1.c as c, t1.d as d, t1.e as e FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c, t1.d, t1.e)
                order by b, c, d, e;
            )",
            R"(
                SELECT sum(t1.b), min(t1.b), max(t1.b), avg(t1.b), t1.a as a, t1.c as c FROM `/Root/t1` as t1
                group by rollup(t1.a, t1.c)
                order by a, c;
            )",
            R"(
                SELECT count(t1.a + 1) + 2, t1.b as b FROM `/Root/t1` as t1
                group by rollup(t1.b)
                order by b;
            )",
            R"(
                SELECT count(distinct t1.c), t1.b as b FROM `/Root/t1` as t1
                group by rollup(t1.b)
                order by b;
            )",
            R"(
                SELECT t1.b as b, t1.c as c FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c)
                order by b, c;
            )",
        };

        const std::vector<std::string> results = {
            R"([[4u;#];[1u;[1]];[1u;[2]];[1u;[3]];[1u;[4]]])",
            R"([[4u;#];[1u;[0]];[1u;[1]];[1u;[2]];[1u;[3]]])",
            R"([[4u;#;#];[1u;[1];#];[1u;[1];[2]];[1u;[2];#];[1u;[2];[3]];[1u;[3];#];[1u;[3];[4]];[1u;[4];#];[1u;[4];[5]]])",
            R"([[4u;#;#;#];[1u;[1];#;#];[1u;[1];[2];#];[1u;[1];[2];[3]];[1u;[2];#;#];[1u;[2];[3];#];[1u;[2];[3];[4]];[1u;[3];#;#];[1u;[3];[4];#];[1u;[3];[4];[5]];[1u;[4];#;#];[1u;[4];[5];#];[1u;[4];[5];[6]]])",
            R"([[4u;#;#;#;#];[1u;[1];#;#;#];[1u;[1];[2];#;#];[1u;[1];[2];[3];#];[1u;[1];[2];[3];[4]];[1u;[2];#;#;#];[1u;[2];[3];#;#];[1u;[2];[3];[4];#];[1u;[2];[3];[4];[5]];[1u;[3];#;#;#];[1u;[3];[4];#;#];[1u;[3];[4];[5];#];[1u;[3];[4];[5];[6]];[1u;[4];#;#;#];[1u;[4];[5];#;#];[1u;[4];[5];[6];#];[1u;[4];[5];[6];[7]]])",
            R"([[[10];[1];[4];[2.5];#;#];[[1];[1];[1];[1.];[0];#];[[1];[1];[1];[1.];[0];[2]];[[2];[2];[2];[2.];[1];#];[[2];[2];[2];[2.];[1];[3]];[[3];[3];[3];[3.];[2];#];[[3];[3];[3];[3.];[2];[4]];[[4];[4];[4];[4.];[3];#];[[4];[4];[4];[4.];[3];[5]]])",
            R"([[6u;#];[3u;[1]];[3u;[2]];[3u;[3]];[3u;[4]]])",
            R"([[4u;#];[1u;[1]];[1u;[2]];[1u;[3]];[1u;[4]]])",
            R"([[#;#];[[1];#];[[1];[2]];[[2];#];[[2];[3]];[[3];#];[[3];[4]];[[4];#];[[4];[5]]])",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),  NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
        }
    }

    Y_UNIT_TEST(RollupGrouping) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int64,
                c Int64,
                d Int64,
                e Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();

        db = kikimr.GetTableClient();
        InsertIntoSchema1(db, "/Root/t1", 4);

        const std::vector<std::string> queries = {
            R"(
                SELECT count(t1.a), t1.b as b, grouping(t1.b) as g FROM `/Root/t1` as t1
                group by rollup(t1.b)
                order by b;
            )",
            R"(
                SELECT count(t1.a), t1.b as b, t1.c as c, grouping(t1.b) as gb, grouping(t1.c) as gc,
                       grouping(t1.b, t1.c) as gbc
                FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c)
                order by b, c;
            )",
            R"(
                SELECT count(t1.a), t1.b as b, grouping(t1.b) as g FROM `/Root/t1` as t1
                group by t1.b
                order by b;
            )",
            R"(
                SELECT sum(t1.a), t1.b as b, t1.c as c, grouping(t1.b) + grouping(t1.c) as loch
                FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c)
                order by loch desc, b, c;
            )",
            R"(
                SELECT t1.b as b, t1.c as c, grouping(t1.c) as gc,
                       rank() over (partition by grouping(t1.c) order by sum(t1.a) desc) as rnk
                FROM `/Root/t1` as t1
                group by rollup(t1.b, t1.c)
                order by gc, rnk;
            )",
            R"(
                SELECT t1.b as b, grouping(t1.b) as g FROM `/Root/t1` as t1
                group by rollup(t1.b)
                order by b;
            )",
        };

        const std::vector<std::string> results = {
            R"([[4u;#;1u];[1u;[1];0u];[1u;[2];0u];[1u;[3];0u];[1u;[4];0u]])",
            R"([[4u;#;#;1u;1u;3u];[1u;[1];#;0u;1u;1u];[1u;[1];[2];0u;0u;0u];[1u;[2];#;0u;1u;1u];[1u;[2];[3];0u;0u;0u];)"
            R"([1u;[3];#;0u;1u;1u];[1u;[3];[4];0u;0u;0u];[1u;[4];#;0u;1u;1u];[1u;[4];[5];0u;0u;0u]])",
            R"([[1u;[1];0u];[1u;[2];0u];[1u;[3];0u];[1u;[4];0u]])",
            R"([[[6];#;#;2u];[[0];[1];#;1u];[[1];[2];#;1u];[[2];[3];#;1u];[[3];[4];#;1u];)"
            R"([[0];[1];[2];0u];[[1];[2];[3];0u];[[2];[3];[4];0u];[[3];[4];[5];0u]])",
            R"([[[4];[5];0u;1u];[[3];[4];0u;2u];[[2];[3];0u;3u];[[1];[2];0u;4u];)"
            R"([#;#;1u;1u];[[4];#;1u;2u];[[3];#;1u;3u];[[2];#;1u;4u];[[1];#;1u;5u]])",
            R"([[#;1u];[[1];0u];[[2];0u];[[3];0u];[[4];0u]])",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "Query " << i << ": " << result.GetIssues().ToString());

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "Query " << i << ": " << result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), results[i], "Query " << i);
        }
    }

}

} // namespace NKikimr::NKqp
