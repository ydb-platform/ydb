#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpSort) {
    Y_UNIT_TEST(ReverseOptimized) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            SELECT Group, Name, Amount, Comment
            FROM `/Root/Test`
            ORDER BY Group DESC, Name DESC;
        )";

        auto result = session.ExplainDataQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        UNIT_ASSERT_C(result.GetAst().Contains("('\"Reverse\")"), result.GetAst());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        Cout << plan;
        auto node = FindPlanNodeByKv(plan, "Node Type", "TableFullScan"); // without `Sort`
        UNIT_ASSERT(node.IsDefined());
        auto read = FindPlanNodeByKv(node, "Name", "TableFullScan");
        UNIT_ASSERT(read.IsDefined());
        UNIT_ASSERT(read.GetMapSafe().contains("Reverse"));
        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([
                [[2u];["Tony"];[7200u];["None"]];
                [[1u];["Paul"];[300u];["None"]];
                [[1u];["Anna"];[3500u];["None"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ReverseOptimizedWithPredicate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            SELECT Group, Name, Amount, Comment
            FROM `/Root/Test`
            WHERE Group < 2u
            ORDER BY Group DESC, Name DESC;
        )";

        auto result = session.ExplainDataQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        UNIT_ASSERT_C(result.GetAst().Contains("('\"Reverse\")"), result.GetAst());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        Cout << plan;
        auto node = FindPlanNodeByKv(plan, "Node Type", "TableRangeScan"); // without `Sort`
        UNIT_ASSERT_C(node.IsDefined(), result.GetPlan());
        auto read = FindPlanNodeByKv(node, "Name", "TableRangeScan");
        UNIT_ASSERT(read.IsDefined());
        UNIT_ASSERT(read.GetMapSafe().contains("Reverse"));
        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([
                [[1u];["Paul"];[300u];["None"]];
                [[1u];["Anna"];[3500u];["None"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ReverseFirstKeyOptimized) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            SELECT Group, Name, Amount, Comment
            FROM `/Root/Test`
            ORDER BY Group DESC;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            UNIT_ASSERT_C(result.GetAst().Contains("('\"Reverse\")"), result.GetAst());

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << plan;
            auto node = FindPlanNodeByKv(plan, "Node Type", "TableFullScan"); // without `Sort`
            UNIT_ASSERT_C(node.IsDefined(), result.GetPlan());
            auto read = FindPlanNodeByKv(node, "Name", "TableFullScan");
            UNIT_ASSERT(read.IsDefined());
            UNIT_ASSERT(read.GetMapSafe().contains("Reverse"));
        }

        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([
                [[2u];["Tony"];[7200u];["None"]];
                [[1u];["Paul"];[300u];["None"]];
                [[1u];["Anna"];[3500u];["None"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ReverseMixedOrderNotOptimized) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            SELECT Group, Name, Amount, Comment
            FROM `/Root/Test`
            ORDER BY Group DESC, Name ASC;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            UNIT_ASSERT_C(!result.GetAst().Contains("('\"Reverse\")"), result.GetAst());

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << plan;
            auto node = FindPlanNodeByKv(plan, "Node Type", "TableFullScan");
            UNIT_ASSERT_C(node.IsDefined(), result.GetPlan());
            UNIT_ASSERT(!node.GetMapSafe().contains("Reverse"));
        }

        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([
                [[2u];["Tony"];[7200u];["None"]];
                [[1u];["Anna"];[3500u];["None"]];
                [[1u];["Paul"];[300u];["None"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ReverseRangeOptimized) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            SELECT Group, Name, Amount, Comment
            FROM `/Root/Test`
            WHERE Group < 2
            ORDER BY Group DESC, Name DESC;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            UNIT_ASSERT_C(result.GetAst().Contains("('\"Reverse\")"), result.GetAst());

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << plan;
            auto node = FindPlanNodeByKv(plan, "Node Type", "TableRangeScan");
            UNIT_ASSERT_C(node.IsDefined(), result.GetPlan());
            auto read = FindPlanNodeByKv(node, "Name", "TableRangeScan");
            UNIT_ASSERT(read.IsDefined());
            UNIT_ASSERT(read.GetMapSafe().contains("Reverse"));
        }

        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([
                [[1u];["Paul"];[300u];["None"]];
                [[1u];["Anna"];[3500u];["None"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ReverseLimitOptimized) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            SELECT Group, Name, Amount, Comment
            FROM `/Root/Test`
            ORDER BY Group DESC, Name DESC
            LIMIT 1;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            UNIT_ASSERT_C(result.GetAst().Contains("'\"ItemsLimit\""), result.GetAst());

            UNIT_ASSERT_C(result.GetAst().Contains("('\"Reverse\")"), result.GetAst());

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << plan;
            auto node = FindPlanNodeByKv(plan, "Node Type", "TableFullScan");
            UNIT_ASSERT_C(node.IsDefined(), result.GetPlan());
            auto read = FindPlanNodeByKv(node, "Name", "TableFullScan");
            UNIT_ASSERT(read.IsDefined());
            UNIT_ASSERT(read.GetMapSafe().contains("Reverse"));
            auto limit = FindPlanNodeByKv(plan, "Name", "Limit");
            UNIT_ASSERT(limit.IsDefined());
            UNIT_ASSERT(limit.GetMapSafe().contains("Limit"));
        }

        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([
                [[2u];["Tony"];[7200u];["None"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ReverseRangeLimitOptimized) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            SELECT Group, Name, Amount, Comment
            FROM `/Root/Test`
            WHERE Group < 2
            ORDER BY Group DESC, Name DESC
            LIMIT 1;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            UNIT_ASSERT_C(result.GetAst().Contains("('\"Reverse\")"), result.GetAst());

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << plan;
            auto node = FindPlanNodeByKv(plan, "Node Type", "TableRangeScan");

            UNIT_ASSERT_C(node.IsDefined(), result.GetPlan());
            auto read = FindPlanNodeByKv(node, "Name", "TableRangeScan");
            UNIT_ASSERT(read.IsDefined());
            UNIT_ASSERT(read.GetMapSafe().contains("Reverse"));

            UNIT_ASSERT_C(result.GetAst().Contains("'\"ItemsLimit\""), result.GetAst());
        }

        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([
                [[1u];["Paul"];[300u];["None"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ReverseEightShardOptimized) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            SELECT Key, Text, Data
            FROM `/Root/EightShard`
            ORDER BY Key DESC
            LIMIT 8;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            UNIT_ASSERT_C(result.GetAst().Contains("'\"ItemsLimit\""), result.GetAst());

            UNIT_ASSERT_C(result.GetAst().Contains("('\"Reverse\")"), result.GetAst());

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << plan;
            auto node = FindPlanNodeByKv(plan, "Node Type", "TableFullScan");
            UNIT_ASSERT_C(node.IsDefined(), result.GetPlan());
            auto read = FindPlanNodeByKv(node, "Name", "TableFullScan");
            UNIT_ASSERT(read.IsDefined());
            UNIT_ASSERT(read.GetMapSafe().contains("Reverse"));
        }

        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([
                [[803u];["Value3"];[3]];
                [[802u];["Value2"];[1]];
                [[801u];["Value1"];[2]];
                [[703u];["Value3"];[2]];
                [[702u];["Value2"];[3]];
                [[701u];["Value1"];[1]];
                [[603u];["Value3"];[1]];
                [[602u];["Value2"];[2]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(TopSortParameter) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            DECLARE $limit AS Uint32;
            DECLARE $offset AS Uint32;
            DECLARE $minKey AS Uint64;

            SELECT *
            FROM `/Root/EightShard`
            WHERE Key >= $minKey
            ORDER BY Data, Key DESC
            LIMIT $limit OFFSET $offset;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << plan;
            auto node = FindPlanNodeByKv(plan, "Node Type", "TableRangeScan");
            UNIT_ASSERT_C(node.IsDefined(), result.GetPlan());
        }

        {
            auto params = db.GetParamsBuilder()
                .AddParam("$limit")
                    .Uint32(5)
                    .Build()
                .AddParam("$offset")
                    .Uint32(3)
                    .Build()
                .AddParam("$minKey")
                    .Uint64(300)
                    .Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());

            CompareYson(R"([
                [[1];[502u];["Value2"]];
                [[1];[401u];["Value1"]];
                [[1];[303u];["Value3"]];
                [[2];[801u];["Value1"]];
                [[2];[703u];["Value3"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(TopSortExpr) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            DECLARE $limit AS Uint32;
            DECLARE $offset AS Uint32;
            DECLARE $minKey AS Uint64;

            SELECT *
            FROM `/Root/EightShard`
            WHERE Key >= $minKey
            ORDER BY Data, Key DESC
            LIMIT $limit + 1 OFFSET $offset - 1;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << plan;
            auto node = FindPlanNodeByKv(plan, "Node Type", "TableRangeScan");
            UNIT_ASSERT_C(node.IsDefined(), result.GetPlan());
        }

        {
            auto params = db.GetParamsBuilder()
                .AddParam("$limit")
                    .Uint32(4)
                    .Build()
                .AddParam("$offset")
                    .Uint32(4)
                    .Build()
                .AddParam("$minKey")
                    .Uint64(300)
                    .Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());

            CompareYson(R"([
                [[1];[502u];["Value2"]];
                [[1];[401u];["Value1"]];
                [[1];[303u];["Value3"]];
                [[2];[801u];["Value1"]];
                [[2];[703u];["Value3"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(TopSortExprPk) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            DECLARE $limit AS Uint32;
            DECLARE $offset AS Uint32;
            DECLARE $minKey AS Uint64;

            SELECT *
            FROM `/Root/EightShard`
            WHERE Key >= $minKey
            ORDER BY Key
            LIMIT $limit + 1 OFFSET $offset - 1;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_C(result.GetAst().Contains("ItemsLimit"), result.GetAst());
        }

        {
            auto params = db.GetParamsBuilder()
                .AddParam("$limit")
                    .Uint32(4)
                    .Build()
                .AddParam("$offset")
                    .Uint32(4)
                    .Build()
                .AddParam("$minKey")
                    .Uint64(300)
                    .Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());

            CompareYson(R"([
                [[1];[401u];["Value1"]];
                [[3];[402u];["Value2"]];
                [[2];[403u];["Value3"]];
                [[2];[501u];["Value1"]];
                [[1];[502u];["Value2"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ComplexPkExclusiveSecondOptionalPredicate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            TString query = R"(
                DECLARE $x AS Uint32;
                DECLARE $y AS String?;

                SELECT *
                FROM `/Root/Join2`
                WHERE Key1 = $x AND Key2 > $y
                ORDER BY Key1, Key2
                LIMIT 10;
            )";

            {
                auto params = db.GetParamsBuilder()
                    .AddParam("$x")
                        .Uint32(105)
                        .Build()
                    .AddParam("$y")
                        .OptionalString("One")
                        .Build()

                    .Build();

                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
                UNIT_ASSERT(result.IsSuccess());

                CompareYson(R"([
                    [[105u];["Two"];["Name4"];["Value28"]]
                ])", FormatResultSetYson(result.GetResultSet(0)));
            }

            {
                auto params = db.GetParamsBuilder()
                    .AddParam("$x")
                        .Uint32(105)
                        .Build()
                    .AddParam("$y")
                        .EmptyOptional(EPrimitiveType::String)
                        .Build()

                    .Build();

                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
                UNIT_ASSERT(result.IsSuccess());

                CompareYson(R"([
                ])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }

        {
            TString query = R"(
                DECLARE $x AS Uint32;
                DECLARE $y AS String?;

                SELECT *
                FROM `/Root/Join2`
                WHERE Key1 = $x AND Key2 < $y
                ORDER BY Key1, Key2
                LIMIT 10;
            )";

            {
                auto params = db.GetParamsBuilder()
                    .AddParam("$x")
                        .Uint32(105)
                        .Build()
                    .AddParam("$y")
                        .OptionalString("Two")
                        .Build()

                    .Build();

                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
                UNIT_ASSERT(result.IsSuccess());

                CompareYson(R"([
                    [[105u];["One"];["Name2"];["Value27"]]
                ])", FormatResultSetYson(result.GetResultSet(0)));
            }

            {
                auto params = db.GetParamsBuilder()
                    .AddParam("$x")
                        .Uint32(105)
                        .Build()
                    .AddParam("$y")
                        .EmptyOptional(EPrimitiveType::String)
                        .Build()

                    .Build();

                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
                UNIT_ASSERT(result.IsSuccess());

                CompareYson(R"([
                ])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }
    }

    Y_UNIT_TEST(ComplexPkInclusiveSecondOptionalPredicate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            TString query = R"(
                DECLARE $x AS Uint32;
                DECLARE $y AS String?;

                SELECT *
                FROM `/Root/Join2`
                WHERE Key1 = $x AND Key2 >= $y
                ORDER BY Key1, Key2
                LIMIT 10;
            )";

            {
                auto params = db.GetParamsBuilder()
                    .AddParam("$x")
                        .Uint32(105)
                        .Build()
                    .AddParam("$y")
                        .OptionalString("One")
                        .Build()

                    .Build();

                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
                UNIT_ASSERT(result.IsSuccess());

                CompareYson(R"([
                    [[105u];["One"];["Name2"];["Value27"]];[[105u];["Two"];["Name4"];["Value28"]]
                ])", FormatResultSetYson(result.GetResultSet(0)));
            }

            {
                auto params = db.GetParamsBuilder()
                    .AddParam("$x")
                        .Uint32(105)
                        .Build()
                    .AddParam("$y")
                        .EmptyOptional(EPrimitiveType::String)
                        .Build()

                    .Build();

                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
                UNIT_ASSERT(result.IsSuccess());

                CompareYson(R"([
                ])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }

        {
            TString query = R"(
                DECLARE $x AS Uint32;
                DECLARE $y AS String?;

                SELECT *
                FROM `/Root/Join2`
                WHERE Key1 = $x AND Key2 <= $y
                ORDER BY Key1, Key2
                LIMIT 10;
            )";

            {
                auto params = db.GetParamsBuilder()
                    .AddParam("$x")
                        .Uint32(105)
                        .Build()
                    .AddParam("$y")
                        .OptionalString("Two")
                        .Build()

                    .Build();

                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
                UNIT_ASSERT(result.IsSuccess());

                CompareYson(R"([
                    [[105u];["One"];["Name2"];["Value27"]];[[105u];["Two"];["Name4"];["Value28"]]
                ])", FormatResultSetYson(result.GetResultSet(0)));
            }

            {
                auto params = db.GetParamsBuilder()
                    .AddParam("$x")
                        .Uint32(105)
                        .Build()
                    .AddParam("$y")
                        .EmptyOptional(EPrimitiveType::String)
                        .Build()

                    .Build();

                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
                UNIT_ASSERT(result.IsSuccess());

                CompareYson(R"([
                ])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }
    }

    Y_UNIT_TEST(TopSortTableExpr) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            DECLARE $key AS Uint32;

            $fetch = (
                SELECT Value2 + 1 AS ComputedLimit FROM `/Root/TwoShard`
                WHERE Key = $key
            );

            SELECT *
            FROM `/Root/EightShard`
            ORDER BY Data DESC, Key
            LIMIT CAST($fetch AS Uint64) ?? 0;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_C(!result.GetAst().Contains("KiPartialTake"), result.GetAst());
        }

        {
            auto params = db.GetParamsBuilder()
                .AddParam("$key")
                    .Uint32(3)
                    .Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());

            CompareYson(R"([
                [[3];[102u];["Value2"]];
                [[3];[203u];["Value3"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(TopSortTableExprOffset) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            DECLARE $key AS Uint32;

            $fetch = (
                SELECT Value2 + 1 AS Take FROM `/Root/TwoShard`
                WHERE Key = $key
            );

            SELECT *
            FROM `/Root/EightShard`
            ORDER BY Data DESC, Key
            LIMIT 2 OFFSET CAST($fetch AS Uint64) ?? 0;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_C(!result.GetAst().Contains("KiPartialTake"), result.GetAst());
        }

        {
            auto params = db.GetParamsBuilder()
                .AddParam("$key")
                    .Uint32(3)
                    .Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());

            CompareYson(R"([
                [[3];[301u];["Value1"]];
                [[3];[402u];["Value2"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(TopSortResults) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto table = db.GetTableBuilder()
            .AddNullableColumn("Key", EPrimitiveType::Uint64)
            .AddNullableColumn("Value1", EPrimitiveType::Int32)
            .AddNullableColumn("Value2", EPrimitiveType::String)
            .AddNullableColumn("Value3", EPrimitiveType::Uint32)
            .SetPrimaryKeyColumns(TVector<TString>{"Key"})
            .Build();

        auto createSettings = TCreateTableSettings()
            .PartitioningPolicy(TPartitioningPolicy().UniformPartitions(10));

        auto tableResult = session.CreateTable("/Root/TopSortTest", std::move(table),
            createSettings).ExtractValueSync();
        tableResult.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(tableResult.IsSuccess());

        const ui32 BatchSize = 200;
        const ui32 BatchCount = 5;
        for (ui32 i = 0; i < BatchCount; ++i) {
            auto paramsBuilder = session.GetParamsBuilder();
            auto& rowsParam = paramsBuilder.AddParam("$rows");

            rowsParam.BeginList();
            for (ui32 j = 0; j < BatchSize; ++j) {
                rowsParam.AddListItem()
                    .BeginStruct()
                    .AddMember("Key").Uint64(RandomNumber<ui64>())
                    .AddMember("Value1").Int32(i)
                    .AddMember("Value2").String(CreateGuidAsString())
                    .AddMember("Value3").Uint32(RandomNumber<ui32>())
                    .EndStruct();
            }
            rowsParam.EndList();
            rowsParam.Build();

            auto result = session.ExecuteDataQuery(R"(
                DECLARE $rows AS
                    List<Struct<
                        Key: Uint64,
                        Value1: Int32,
                        Value2: String,
                        Value3: Uint32>>;

                REPLACE INTO `/Root/TopSortTest`
                SELECT * FROM AS_TABLE($rows);
            )", TTxControl::BeginTx().CommitTx(), paramsBuilder.Build(),
                TExecDataQuerySettings().KeepInQueryCache(true)).ExtractValueSync();

            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT(result.IsSuccess());
        }

        auto queryTemplate = R"(
            PRAGMA kikimr.OptDisableTopSort = '%s';

            DECLARE $filter AS Int32;
            DECLARE $limit AS Uint64;
            DECLARE $offset AS Uint64;

            SELECT * FROM `/Root/TopSortTest`
            WHERE Value1 != $filter
            ORDER BY Value2 DESC, Value3, Key DESC
            LIMIT $limit OFFSET $offset;
        )";

        auto query = Sprintf(queryTemplate, "False");
        auto queryDisabled = Sprintf(queryTemplate, "True");

        const ui32 QueriesCount = 20;
        for (ui32 i = 0; i < QueriesCount; ++i) {
            auto params = db.GetParamsBuilder()
                .AddParam("$filter")
                    .Int32(RandomNumber<ui32>(BatchCount))
                    .Build()
                .AddParam("$limit")
                    .Uint64(RandomNumber<ui64>(BatchSize * BatchCount))
                    .Build()
                .AddParam("$offset")
                    .Uint64(RandomNumber<ui64>(BatchSize * BatchCount))
                    .Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params,
                TExecDataQuerySettings().KeepInQueryCache(true)).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT(result.IsSuccess());

            auto resultDisabled = session.ExecuteDataQuery(queryDisabled, TTxControl::BeginTx().CommitTx(), params,
                TExecDataQuerySettings().KeepInQueryCache(true)).ExtractValueSync();
            resultDisabled.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT(resultDisabled.IsSuccess());

            CompareYson(FormatResultSetYson(result.GetResultSet(0)),
                FormatResultSetYson(resultDisabled.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(TopParameter) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = R"(
            DECLARE $limit AS Uint64;

            SELECT *
            FROM `/Root/TwoShard`
            ORDER BY Key
            LIMIT $limit;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            result.GetIssues().PrintTo(Cerr);

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << plan;
            auto node = FindPlanNodeByKv(plan, "Node Type", "TableFullScan");
            UNIT_ASSERT_C(node.IsDefined(), result.GetPlan());
        }

        {
            auto params = db.GetParamsBuilder()
                .AddParam("$limit")
                    .Uint64(2)
                    .Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];["One"];[-1]];
                [[2u];["Two"];[0]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(TopParameterFilter) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = R"(
            DECLARE $limit AS Uint64;
            DECLARE $value AS Int32;

            SELECT *
            FROM `/Root/TwoShard`
            WHERE Value2 != $value
            LIMIT $limit;
        )";

        {
            auto result = session.ExplainDataQuery(query).GetValueSync();
            result.GetIssues().PrintTo(Cerr);

            Cerr << result.GetAst() << Endl;

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << plan;
            auto node = FindPlanNodeByKv(plan, "Node Type", "TableFullScan");
            UNIT_ASSERT_C(node.IsDefined(), result.GetPlan());
            node = FindPlanNodeByKv(plan, "Node Type", "Limit-Filter");
            auto limit = FindPlanNodeByKv(node, "Limit", "Min(1001,$limit)");
            UNIT_ASSERT(limit.IsDefined());
        }

        {
            auto params = db.GetParamsBuilder()
                .AddParam("$limit")
                    .Uint64(2)
                    .Build()
                .AddParam("$value")
                    .Int32(0)
                    .Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto actual = ReformatYson(FormatResultSetYson(result.GetResultSet(0)));

            if (ReformatYson(R"([
                    [[1u];["One"];[-1]];
                    [[3u];["Three"];[1]]
                ])") == actual)
            {
                return;
            }

            auto expected = R"([
                [[4000000001u];["BigOne"];[-1]];
                [[4000000003u];["BigThree"];[1]]
            ])";

            UNIT_ASSERT_NO_DIFF(ReformatYson(expected), actual);
        }
    }

    // KIKIMR-11523
    Y_UNIT_TEST(PassLimit) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto status = session.ExecuteSchemeQuery(R"(
                create table `/Root/index` (
                    id Utf8,
                    id2 Utf8,
                    op_id Utf8,
                    payload Utf8,
                    primary key (id, id2, op_id)
                );

                create table `/Root/ops` (
                    id Utf8,
                    payload Utf8,
                    primary key (id)
                );
            )").GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        auto result = session.ExplainDataQuery(R"(
            declare $idx_id as Utf8;
            declare $limit as Uint64;

            $q = (
                SELECT id, id2, op_id, payload
                FROM `/Root/index`
                WHERE id = "1" AND (id2 > "a" OR (id2 = "a" AND op_id > ""U))
                ORDER BY id, id2, op_id
                LIMIT $limit
            );

            SELECT ops.id,
                   idx.id, idx.id2, idx.op_id
            FROM $q AS idx
            LEFT JOIN `/Root/ops` AS ops ON idx.op_id = ops.id
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        Cerr << result.GetPlan() << Endl << Endl;
        Cerr << result.GetAst() << Endl;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        Cout << plan;

        auto tableRangeRead = FindPlanNodeByKv(plan, "Node Type", "TableRangeScan");
        UNIT_ASSERT(tableRangeRead.IsDefined());

        auto& rangeReadOp = tableRangeRead.GetMapSafe().at("Operators").GetArraySafe().at(0).GetMapSafe();
        UNIT_ASSERT_VALUES_EQUAL("TableRangeScan", rangeReadOp.at("Name").GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL("index", rangeReadOp.at("Table").GetStringSafe());
    }

    Y_UNIT_TEST(Offset) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            $data = SELECT * FROM EightShard WHERE Text = "Value1" LIMIT 7;

            SELECT * FROM $data LIMIT 3 OFFSET 0;
            SELECT * FROM $data LIMIT 3 OFFSET 3;
            SELECT * FROM $data LIMIT 3 OFFSET 6;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(1).RowsCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(2).RowsCount(), 1);
    }

    Y_UNIT_TEST(OffsetPk) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            $data = SELECT * FROM EightShard WHERE Text = "Value1" ORDER BY Key LIMIT 7;

            SELECT * FROM $data ORDER BY Key LIMIT 3 OFFSET 0;
            SELECT * FROM $data ORDER BY Key LIMIT 3 OFFSET 3;
            SELECT * FROM $data ORDER BY Key LIMIT 3 OFFSET 6;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1];[101u];["Value1"]];
            [[2];[201u];["Value1"]];
            [[3];[301u];["Value1"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        CompareYson(R"([
            [[1];[401u];["Value1"]];
            [[2];[501u];["Value1"]];
            [[3];[601u];["Value1"]]
        ])", FormatResultSetYson(result.GetResultSet(1)));

        CompareYson(R"([
            [[1];[701u];["Value1"]]
        ])", FormatResultSetYson(result.GetResultSet(2)));
    }

    Y_UNIT_TEST(OffsetTopSort) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            $data = SELECT * FROM EightShard WHERE Text = "Value1" ORDER BY Data, Key LIMIT 7;

            SELECT * FROM $data ORDER BY Data, Key LIMIT 3 OFFSET 0;
            SELECT * FROM $data ORDER BY Data, Key LIMIT 3 OFFSET 3;
            SELECT * FROM $data ORDER BY Data, Key LIMIT 3 OFFSET 6;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1];[101u];["Value1"]];
            [[1];[401u];["Value1"]];
            [[1];[701u];["Value1"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        CompareYson(R"([
            [[2];[201u];["Value1"]];
            [[2];[501u];["Value1"]];
            [[2];[801u];["Value1"]]
        ])", FormatResultSetYson(result.GetResultSet(1)));

        CompareYson(R"([
            [[3];[301u];["Value1"]]
        ])", FormatResultSetYson(result.GetResultSet(2)));
    }

    Y_UNIT_TEST(UnionAllSortLimit) {
        NKikimrConfig::TAppConfig appConfig;
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig);

        TKikimrRunner kikimr{serverSettings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            SELECT * FROM Logs WHERE App = "nginx" AND Ts >= 2

            UNION ALL

            SELECT * FROM Logs WHERE App >= "kikimr-db"

            ORDER BY App, Ts, Host
            LIMIT 3;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);

        for (auto& read : plan["tables"][0]["reads"].GetArraySafe()) {
            UNIT_ASSERT(read.Has("limit"));
            UNIT_ASSERT_VALUES_EQUAL(read["limit"], "3");
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
