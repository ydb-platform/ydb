#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpUnion) {
    Y_UNIT_TEST(HashShuffleConnections) {
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

        auto insertRes = session2.ExecuteQuery(R"(
            INSERT INTO `/Root/t1` (a, b) VALUES (1, 1);
            INSERT INTO `/Root/t2` (a, b) VALUES (1, 1);
            INSERT INTO `/Root/t1` (a, b) VALUES (2, 1);
            INSERT INTO `/Root/t2` (a, b) VALUES (2, 1);
            INSERT INTO `/Root/t1` (a, b) VALUES (3, 1);
            INSERT INTO `/Root/t2` (a, b) VALUES (3, 1);
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(insertRes.IsSuccess());

        std::vector<TString> queries = {
            R"(
                PRAGMA kikimr.OptEnableHashShuffleConnectionsForExtend = "true";
                SELECT * FROM `/Root/t1`
                UNION ALL
                SELECT * FROM `/Root/t2`;
            )",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto query = queries[i];
            auto result =
                session2
                    .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_C(ast.find("DqCnHashShuffle") != std::string::npos, TStringBuilder() << "HashShuffle connections are not applied for UNION ALL. Query: " << query);

            result = session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }
}

} // namespace NKikimr::NKqp
