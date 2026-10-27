#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>

#include <library/cpp/threading/local_executor/local_executor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpQueryService) {
    // Attempt to trigger simultanous server side close and return session
    // From sdk perspective check no dataraces
    Y_UNIT_TEST(ReturnAndCloseSameTime) {
        auto kikimr = DefaultKikimrRunner();
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        size_t iterations = 999;
        auto db = kikimr.GetQueryClient();

        NPar::LocalExecutor().RunAdditionalThreads(2);
        while (iterations--) {
            auto lim = iterations % 33;
            TVector<NYdb::NQuery::TQueryClient::TSession> sessions;
            TVector<TString> sids;
            sessions.reserve(lim);
            sids.reserve(lim);
            for (size_t i = 0; i < lim; ++i) {
                auto sessionResult = db.GetSession().GetValueSync();
                UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());

                sessions.push_back(sessionResult.GetSession());
                sids.push_back(TString{sessions.back().GetId()});
            }

            if (iterations & 1) {
                auto rng = std::default_random_engine {};
                std::ranges::shuffle(sids, rng);
            }

            NPar::LocalExecutor().ExecRange([sessions{std::move(sessions)}, sids{std::move(sids)}, clientConfig](int id) mutable{
                if (id == 0) {
                    for (size_t i = 0; i < sessions.size(); i++) {
                        auto s = std::move(sessions[i]);
                        auto execResult = s.ExecuteQuery("SELECT 1;",
                            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                        switch (execResult.GetStatus()) {
                            case EStatus::SUCCESS:
                            case EStatus::BAD_SESSION:
                            case EStatus::CANCELLED:
                                break;
                            default:
                                UNIT_ASSERT_C(false, "unexpected status: " << execResult.GetStatus());
                        }
                    }
                } else if (id == 1) {
                    for (size_t i = 0; i < sids.size(); i++) {
                        bool allDoneOk = true;
                        NTestHelpers::CheckDelete(clientConfig, sids[i], Ydb::StatusIds::SUCCESS, allDoneOk);
                        UNIT_ASSERT(allDoneOk);
                    }
                } else {
                    Y_ABORT_UNLESS(false, "unexpected thread cxount");
                }
            }, 0, 2, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
        }

        WaitForZeroSessions(counters);
    }

    Y_UNIT_TEST(TableSink_ReplaceFromSelectLargeOlap) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

        TTestHelper testHelper(settings);

        TKikimrRunner& kikimr = testHelper.GetKikimr();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("Col1").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("Col2").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable1;
        testTable1.SetName("/Root/ColumnShard1").SetPrimaryKey({ "Col1" }).SetSharding({ "Col1" }).SetSchema(schema).SetMinPartitionsCount(1000);
        testHelper.CreateTable(testTable1);

        TTestHelper::TColumnTable testTable2;
        testTable2.SetName("/Root/ColumnShard2").SetPrimaryKey({ "Col1" }).SetSharding({ "Col1" }).SetSchema(schema).SetMinPartitionsCount(1000);
        testHelper.CreateTable(testTable2);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable1.GetArrowSchema(schema));
            for (size_t index = 0; index < 10000; ++index) {
                tableInserter.AddRow().Add(index).Add(index * 10);
            }
            testHelper.BulkUpsert(testTable1, tableInserter);
        }

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto client = kikimr.GetQueryClient();

        {
            const TString sql = R"(
                REPLACE INTO `/Root/ColumnShard2`
                SELECT * FROM `/Root/ColumnShard1`
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());

            auto it = client.StreamExecuteQuery(R"(
                SELECT COUNT(*) FROM `/Root/ColumnShard2`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[10000u]])");
        }
    }

}

} // namespace NKqp
} // namespace NKikimr
