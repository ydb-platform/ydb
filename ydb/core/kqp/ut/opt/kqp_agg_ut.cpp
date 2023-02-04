#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpAgg) {
    Y_UNIT_TEST(AggWithLookup) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            $dict =
                SELECT ToDict(AGGREGATE_LIST(AsTuple(Value2, AsStruct(Key as Lookup))))
                FROM TwoShard
                WHERE Key < 10;

            SELECT
                Text,
                SUM(DictLookup($dict, Data).Lookup) AS SumLookup
            FROM EightShard
            GROUP BY Text
            ORDER BY SumLookup, Text;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["Value3"];[6u]];
            [["Value1"];[9u]];
            [["Value2"];[9u]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(AggWithSelfLookup) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            $dict =
                SELECT ToDict(AGGREGATE_LIST(AsTuple(Key - 100, AsStruct(Data as Lookup))))
                FROM EightShard;

            SELECT
                Text,
                SUM(DictLookup($dict, Data).Lookup) AS SumLookup
            FROM EightShard
            GROUP BY Text
            ORDER BY SumLookup, Text;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["Value2"];[15]];
            [["Value1"];[16]];
            [["Value3"];[17]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(AggWithSelfLookup2) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            $dict =
                SELECT ToDict(AGGREGATE_LIST(AsTuple(Key - 100, AsStruct(Data as Lookup))))
                FROM EightShard;

            SELECT Text, SUM(Lookup) AS SumLookup
            FROM (
                SELECT Text, DictLookup($dict, MIN(Data)).Lookup AS Lookup
                FROM EightShard
                GROUP BY Text
            )
            GROUP BY Text
            ORDER BY SumLookup, Text;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["Value1"];[1]];
            [["Value2"];[1]];
            [["Value3"];[1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

} // namespace NKikimr::NKqp
