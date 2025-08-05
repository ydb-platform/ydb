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

    Y_UNIT_TEST(AggWithHop) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            SELECT
                Text, 
                CAST(COUNT(*) as Int32) as Count,
                SUM(Data)
            FROM EightShard
            GROUP BY HOP(CAST(Key AS Timestamp?), "PT1M", "PT1M", "PT1M"), Text
            ORDER BY Text;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["Value1"];[8];[15]];
            [["Value2"];[8];[16]];
            [["Value3"];[8];[17]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(GroupByLimit) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            --!syntax_v1

            CREATE TABLE `TestTable` (
                a Uint64,
                b Uint64,
                c Uint64,
                d Uint64,
                e Uint64,
                PRIMARY KEY (a, b, c)
            );
        )").GetValueSync());

        AssertSuccessResult(session.ExecuteDataQuery(R"(
            REPLACE INTO `TestTable` (a, b, c, d, e) VALUES
                (1, 11, 21, 31, 41),
                (2, 12, 22, 32, 42),
                (3, 13, 23, 33, 43);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync());


        {  // query with 36 groups and limit 32
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                PRAGMA GroupByLimit = '32';

                SELECT a, b, c, d, SUM(e) Data FROM TestTable
                    GROUP BY ROLLUP(a, b, c, d, a * b AS ab, b * c AS bc, c * d AS cd, a + b AS sum)
                    ORDER BY a, b, c, d;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }

        {  // query with 36 groups (without explicit limit)
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                SELECT a, b, c, d, SUM(e) Data FROM TestTable
                    GROUP BY ROLLUP(a, b, c, d, a * b AS ab, b * c AS bc, c * d AS cd, a + b AS sum)
                    ORDER BY a, b, c, d;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [#;#;#;#;[126u]];
                [[1u];#;#;#;[41u]];
                [[1u];[11u];#;#;[41u]];
                [[1u];[11u];[21u];#;[41u]];
                [[1u];[11u];[21u];[31u];[41u]];
                [[1u];[11u];[21u];[31u];[41u]];
                [[1u];[11u];[21u];[31u];[41u]];
                [[1u];[11u];[21u];[31u];[41u]];
                [[1u];[11u];[21u];[31u];[41u]];
                [[2u];#;#;#;[42u]];
                [[2u];[12u];#;#;[42u]];
                [[2u];[12u];[22u];#;[42u]];
                [[2u];[12u];[22u];[32u];[42u]];
                [[2u];[12u];[22u];[32u];[42u]];
                [[2u];[12u];[22u];[32u];[42u]];
                [[2u];[12u];[22u];[32u];[42u]];
                [[2u];[12u];[22u];[32u];[42u]];
                [[3u];#;#;#;[43u]];
                [[3u];[13u];#;#;[43u]];
                [[3u];[13u];[23u];#;[43u]];
                [[3u];[13u];[23u];[33u];[43u]];
                [[3u];[13u];[23u];[33u];[43u]];
                [[3u];[13u];[23u];[33u];[43u]];
                [[3u];[13u];[23u];[33u];[43u]];
                [[3u];[13u];[23u];[33u];[43u]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST_TWIN(AggHashShuffle, UseSink) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);

        TKikimrRunner kikimr(settings);
        {
            const TString query = R"(
                CREATE TABLE Source (
                    Key UUID,
                    Value Uint64,
                    Value2 String,
                    PRIMARY KEY (Value)
                );
            )";

            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                $data = RandomUuid(0);
                INSERT INTO Source (Key, Value, Value2) VALUES ($data, 1u, "testtesttest");
            )";

            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                SELECT 
                    Key,
                    COUNT(*) AS Cnt
                FROM Source
                GROUP BY Key
            )";

            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;

            //UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), "[[2u]]");
        }
       
    }

    Y_UNIT_TEST(AggWithSqlIn) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto queryClient = kikimr.GetQueryClient();
        auto result = queryClient.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(result);
        auto session2 = result.GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64	NOT NULL,
                b Int32,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

       res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b Int32,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t3` (
                a Int64	NOT NULL,
                b Int32,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        auto insertRes = session2.ExecuteQuery(R"(
            INSERT INTO `/Root/t1` (a, b) VALUES (1, 1);
            INSERT INTO `/Root/t2` (a, b) VALUES (1, 1);
            INSERT INTO `/Root/t3` (a, b) VALUES (1, 1);
            INSERT INTO `/Root/t1` (a, b) VALUES (2, 1);
            INSERT INTO `/Root/t2` (a, b) VALUES (2, 1);
            INSERT INTO `/Root/t3` (a, b) VALUES (2, 1);
            INSERT INTO `/Root/t1` (a, b) VALUES (3, 1);
            INSERT INTO `/Root/t2` (a, b) VALUES (3, 1);
            INSERT INTO `/Root/t3` (a, b) VALUES (3, 1);
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(insertRes.IsSuccess());

        std::vector<TString> queries = {
            R"(
                SELECT sum(CASE WHEN (t1.a IN (SELECT t2.a FROM t2)) THEN 1 ELSE 0 END),
                       sum(CASE WHEN (t1.a IN (SELECT t3.a FROM t3)) THEN 1 ELSE 0 END)
                FROM t1;
            )",
           R"(
                SELECT sum(CASE WHEN (t1.a IN (SELECT t2.a FROM t2)) THEN 1 ELSE 0 END),
                       sum(CASE WHEN (t1.a IN (SELECT t3.a FROM t3)) THEN 1 ELSE 0 END)
                FROM t1 left outer join t2 on t1.a = t2.a
                        left outer join t3 on t1.a = t3.a;
            )",
        };

        std::vector<TString> results = {
            R"([[[3];[3]]])",
            R"([[[3];[3]]])",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto query = queries[i];
            auto result =
                session2
                    .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            result = session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            TString output = FormatResultSetYson(result.GetResultSet(0));
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }
}

} // namespace NKikimr::NKqp
