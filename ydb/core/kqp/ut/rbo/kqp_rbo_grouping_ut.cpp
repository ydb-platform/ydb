#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_transformer.h>

#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/public/langver/yql_langver.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpRboGrouping) {
    Y_UNIT_TEST(MasksDependOnGroupingSetNotNullValues) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetEnableNewRBO(true);
        config.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        config.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        config.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false));
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto scheme = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/GroupingKeys` (
                id Int64 NOT NULL, a String, b String, PRIMARY KEY (id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
        NYdb::TValueBuilder rows;
        rows.BeginList();
        const auto addRow = [&](i64 id, std::optional<std::string> a, std::optional<std::string> b) {
            rows.AddListItem().BeginStruct()
                .AddMember("id").Int64(id)
                .AddMember("a").OptionalString(a)
                .AddMember("b").OptionalString(b)
                .EndStruct();
        };
        addRow(1, std::nullopt, std::nullopt);
        addRow(2, "x", std::nullopt);
        addRow(3, "x", "y");
        rows.EndList();
        const auto upsert = kikimr.GetTableClient().BulkUpsert("/Root/GroupingKeys", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        const auto check = [&](const TString& suffix, const TString& expected) {
            const auto result = session.ExecuteQuery(TString(R"(
                PRAGMA YqlSelect = 'force';
                SELECT a, b, GROUPING(a, b) AS mask, GROUPING(b, a) AS reverse_mask,
                       GROUPING(a) + GROUPING(b) AS depth, COUNT(*) AS n
                FROM `/Root/GroupingKeys` AS g
            )") + suffix, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), expected);
            NYdb::TTypeParser maskType(result.GetResultSet(0).GetColumnsMeta()[2].Type);
            UNIT_ASSERT_VALUES_EQUAL(maskType.GetPrimitive(), NYdb::EPrimitiveType::Uint64);
        };

        // Ordinary NULL groups and rolled-up NULLs coexist; reversing arguments
        // moves the subtotal bit. ORDER BY also uses an unprojected GROUPING call.
        check("GROUP BY ROLLUP(a, b) ORDER BY GROUPING(g.a, g.b), a, b;",
            R"([[#;#;0u;0u;0u;1u];[["x"];#;0u;0u;0u;1u];[["x"];["y"];0u;0u;0u;1u];[#;#;1u;2u;1u;1u];[["x"];#;1u;2u;1u;2u];[#;#;3u;3u;2u;3u]])");
        check("GROUP BY a, b ORDER BY a, b;",
            R"([[#;#;0u;0u;0u;1u];[["x"];#;0u;0u;0u;1u];[["x"];["y"];0u;0u;0u;1u]])");
        // HAVING must be specialized independently for each grouping set.
        check("GROUP BY ROLLUP(a, b) HAVING GROUPING(a, b) = 1ul ORDER BY a, b;",
            R"([[#;#;1u;2u;1u;1u];[["x"];#;1u;2u;1u;2u]])");
    }

    Y_UNIT_TEST(IndependentCompilerRootsKeepDistinctResults) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetEnableNewRBO(true);
        config.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        config.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        config.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false));
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto scheme = tableSession.ExecuteSchemeQuery(
            "CREATE TABLE `/Root/RootInput` (id Int64 NOT NULL, PRIMARY KEY (id));").GetValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
        auto row = NYdb::TValueBuilder().BeginList().AddListItem().BeginStruct()
            .AddMember("id").Int64(1).EndStruct().AddListItem().BeginStruct()
            .AddMember("id").Int64(2).EndStruct().EndList().Build();
        const auto upsert = kikimr.GetTableClient().BulkUpsert("/Root/RootInput", std::move(row)).GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        const auto result = session.ExecuteQuery(R"(
            PRAGMA YqlSelect = 'force';
            SELECT 11 AS n FROM `/Root/RootInput` LIMIT 1;
            SELECT 22 AS n FROM `/Root/RootInput` LIMIT 1;
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), "[[11]]");
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(1)), "[[22]]");
    }

    Y_UNIT_TEST(ResultMergeRebasesMaterializationAndPublicBindings) {
        using namespace NYql;
        using namespace NYql::NNodes;
        TExprContext ctx;
        TTypeAnnotationContext types;
        TKqpRBOCleanupTransformer cleanup(types);
        const auto pos = ctx.AppendPosition(TPosition());
        const auto empty = ctx.NewList(pos, {});
        const auto type = ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, "Uint64")});
        const auto binding = [&](ui32 tx) {
            return ctx.NewCallable(pos, "KqpTxResultBinding", {
                type, ctx.NewAtom(pos, ToString(tx)), ctx.NewAtom(pos, "0")});
        };
        const auto localParameter = ctx.NewList(pos, {ctx.NewAtom(pos, "materialized"), binding(0)});
        const auto root = [&](TStringBuf value) {
            // The first tx materializes one value; the second consumes it.
            const auto payload = ctx.NewCallable(pos, "Uint64", {ctx.NewAtom(pos, value)});
            const auto txResults = ctx.NewList(pos, {payload});
            const auto materialize = ctx.NewCallable(pos, "KqpPhysicalTx", {empty, txResults, empty, empty});
            const auto main = ctx.NewCallable(pos, "KqpPhysicalTx", {
                empty, txResults, ctx.NewList(pos, {localParameter}), empty});
            return ctx.NewCallable(pos, "KqpPhysicalQuery", {
                ctx.NewList(pos, {materialize, main}), ctx.NewList(pos, {binding(1)}), empty});
        };
        const auto first = root("11");
        const auto second = root("22");
        const auto bundle = [&](TExprNode::TPtr right) {
            return ctx.NewList(pos, {ctx.NewList(pos, {ctx.NewList(pos, {
                ctx.NewList(pos, {first, empty}), ctx.NewList(pos, {right, empty})}), empty})});
        };
        TExprNode::TPtr output;
        UNIT_ASSERT(cleanup.DoTransform(bundle(second), output, ctx) == IGraphTransformer::TStatus::Ok);
        const auto merged = TKqpPhysicalQuery(output);
        UNIT_ASSERT_VALUES_EQUAL(merged.Transactions().Size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(merged.Results().Item(0).Cast<TKqpTxResultBinding>().TxIndex().Value(), "1");
        UNIT_ASSERT_VALUES_EQUAL(merged.Results().Item(1).Cast<TKqpTxResultBinding>().TxIndex().Value(), "3");
        const auto parameter = merged.Transactions().Item(3).ParamBindings().Item(0).Binding().Cast().Cast<TKqpTxResultBinding>();
        UNIT_ASSERT_VALUES_EQUAL(parameter.TxIndex().Value(), "2");
        UNIT_ASSERT_VALUES_EQUAL(parameter.ResultIndex().Value(), "0");
        UNIT_ASSERT_VALUES_EQUAL(TKqpTxResultBinding(localParameter->ChildPtr(1)).TxIndex().Value(), "0");
        // Shared public streams cannot be assigned two independent result slots.
        UNIT_ASSERT(cleanup.DoTransform(bundle(first), output, ctx) == IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "shared result roots");
        const auto shared = ctx.NewCallable(pos, "KqpPhysicalQuery", {first->ChildPtr(0), second->ChildPtr(1), empty});
        UNIT_ASSERT(cleanup.DoTransform(bundle(shared), output, ctx) == IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "share a relational plan");
        const auto bad = ctx.ReplaceNode(TExprNode::TPtr(second), *localParameter->Child(1), binding(2));
        UNIT_ASSERT(cleanup.DoTransform(bundle(bad), output, ctx) == IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "outside its root's transaction list");
    }
}

} // namespace NKikimr::NKqp
