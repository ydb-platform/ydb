#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpHashCombineReplacement) {
    Y_UNIT_TEST_QUAD(DqHashCombineTest, UseDqHashCombine, UseDqHashAggregate) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);

        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        // Enable manual override of _KqpYqlCombinerMemoryLimit
        settings.AppConfig.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlHeavyProgramMemoryLimit(0);
        NKikimrKqp::TKqpSetting combinerMemLimit;
        combinerMemLimit.SetName("_KqpYqlCombinerMemoryLimit");
        combinerMemLimit.SetValue("1000000");
        settings.KqpSettings.emplace_back(combinerMemLimit);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/aggregatable` (
                        id Int64 NOT NULL,
                        group_key Int64 NOT NULL,
                        data Int64 NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/aggregatable` (id, group_key, data) VALUES
                        (1, 0, 100),
                        (2, 0, 200),
                        (3, 1, 300),
                        (4, 1, 400)
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix = "/Root";
            )";
            TString dqHashCombinePragma = Sprintf("PRAGMA ydb.UseDqHashCombine = \"%s\";\n\n", UseDqHashCombine ? "true" : "false");
            TString dqHashAggregatePragma = Sprintf("PRAGMA ydb.UseDqHashAggregate = \"%s\";\n\n", UseDqHashAggregate ? "true" : "false");
            TString select = R"(
                PRAGMA ydb.OptUseFinalizeByKey = "true";
                PRAGMA ydb.OptEnableOlapPushdown = "false"; -- need this to force intermediate/final combiner pair over the sample table

                SELECT T.group_key, SUM(T.data)
                FROM `aggregatable` AS T
                GROUP BY group_key
            )";

            TString groupQuery = TStringBuilder() << hints << dqHashCombinePragma << dqHashAggregatePragma << select;

            auto explainResult = queryClient.ExecuteQuery(
                groupQuery,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            Cerr << TString(*astOpt) << Endl;
            TString ast = TString(*astOpt);
            Cout << "AST (HashCombine=" << (UseDqHashCombine ? "true" : "false") << ", HashAggregate=" << (UseDqHashAggregate ? "true" : "false") << "): " << ast << Endl;

            int hashCombines = 0;
            size_t pos = 0;
            const std::string_view combinerName {"DqPhyHashCombine"};
            while ((pos = ast.find(combinerName, pos)) != std::string::npos) {
                ++hashCombines;
                ++pos;
            }

            int hashCombinesExpected = (UseDqHashCombine ? 1 : 0) + (UseDqHashAggregate ? 1 : 0);
            UNIT_ASSERT_C(hashCombinesExpected == hashCombines,
                TStringBuilder() << "AST should contain " << hashCombinesExpected << " DqPhyHashCombine instances; actual AST: " << groupQuery << Endl << ast);

            auto status = queryClient.ExecuteQuery(groupQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
