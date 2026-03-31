#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpConstantFolding) {
    Y_UNIT_TEST(SkipFoldingUdf) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableFoldUdfs(true);
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
                b Utf8 NOT NULL,
                c Utf8 NOT NULL,
                primary key(a)
            )
            PARTITION BY HASH(a)
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        TVector<TString> queries = {
            R"(
                $to_date = ($dt) -> (
                    DateTime::Parse("%Y-%m-%d %H:%M")($dt)
                );

                $in = SELECT $to_date(b) as b, c FROM `/Root/t1`;
                SELECT * FROM $in where c in ("abcdef");
            )",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto query = queries[i];
            auto result = session2
                              .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                                            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                              .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            Y_ENSURE(ast.find("Udf") != TString::npos);
        }
    }

    Y_UNIT_TEST(ApplyFoldingUdf) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableFoldUdfs(true);
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false);

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
                b String NOT NULL,
                c Date NOT NULL,
                primary key(a)
            )
            PARTITION BY HASH(a)
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        TVector<TString> queries = {
            R"(
                SELECT *
                FROM `/Root/t1` as t1
                WHERE t1.b = String::HexDecode("54");
            )",
            R"(
                SELECT *
                FROM `/Root/t1` as t1
                WHERE t1.c = DateTime::MakeDate(DateTime::Parse('%Y-%m-%d')("2023-10-20"));
            )",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto query = queries[i];
            auto result = session2
                              .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                                            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                              .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            Y_ENSURE(ast.find("Udf") == TString::npos);
        }
    }
}
} // namespace NKikimr::NKqp
