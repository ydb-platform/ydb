#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>

#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

#include <library/cpp/json/json_reader.h>

#include <util/string/printf.h>


namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScripting;

using NYql::TExprContext;
using NYql::TExprNode;

Y_UNIT_TEST_SUITE(KqpLocksTricky) {

    Y_UNIT_TEST_TWIN(TestNoLocksIssue, withSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(withSink);

        auto setting = NKikimrKqp::TKqpSetting();
        TKikimrSettings settings;
        settings.SetAppConfig(appConfig);
        settings.SetUseRealThreads(false);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto writeSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        
        kikimr.RunCall([&]{ CreateSampleTablesWithIndex(session, false /* no need in table data */); return true; });

        {
            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            // running the query that touches the main table and the index.
            const TString query(Q1_(R"(
                SELECT COUNT(b.Key) FROM `/Root/SecondaryKeys` VIEW Index as b
                UNION ALL
                SELECT COUNT(a.Key) FROM `/Root/SecondaryKeys` as a;
            )"));

            std::vector<std::unique_ptr<IEventHandle>> executerResponses;
            bool blockResponses = true;

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (blockResponses && ev->GetTypeRewrite() == TEvKqpExecuter::TEvTxResponse::EventType) {
                    auto* msg = ev->Get<TEvKqpExecuter::TEvTxResponse>();
                    UNIT_ASSERT_C(msg->Snapshot.IsValid(), "unexpected tx response reply without the snapshot");
                    executerResponses.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            };

            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&executerResponses](IEventHandle&) {
                return executerResponses.size() > 0;
            });

            runtime.SetObserverFunc(grab);

            auto future = kikimr.RunInThreadPool([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                return session.ExecuteDataQuery(query, txc, execSettings).ExtractValueSync();
            });


            // basically the query runs using two executors and two stages.
            // the first stage runs the specified select over the table and index,
            // and the second stage runs the operation with locks.
            // since we want to emulate the spurious transaction locks invalidated issue,
            // we are pausing the first stage by the capturing the EvKqpExecuter::TEvTxResponse event.
            runtime.DispatchEvents(opts);
            Y_VERIFY_S(executerResponses.size() > 0, "empty executer responses???");

            blockResponses = false;

            // the write that should be conflicting with the select above.
            // this write should breaks locks, but it's not the issue because the select above was
            // executed with the mvcc snapshot.
            const TString writeQuery(Q1_(R"(
                REPLACE INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES
                    (1,    1,    "Payload1"),
                    (2,    2,    "Payload2"),
                    (5,    5,    "Payload5"),
                    (NULL, 6,    "Payload6"),
                    (7,    NULL, "Payload7"),
                    (NULL, NULL, "Payload8");
            )"));

            auto writeResult = kikimr.RunCall([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                return writeSession.ExecuteDataQuery(writeQuery, txc, execSettings).ExtractValueSync();
            });

            UNIT_ASSERT(writeResult.IsSuccess());

            // releasing executor response from the paused select query.
            for(auto& ev: executerResponses) {
                runtime.Send(ev.release());
            }

            auto result = runtime.WaitFuture(future);
            // select must be successful. no transaction locks invalidated issues.
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
        }
    }

    Y_UNIT_TEST_TWIN(TestNoLocksIssueInteractiveTx, withSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(withSink);

        auto setting = NKikimrKqp::TKqpSetting();
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto writeSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        
        kikimr.RunCall([&]{ CreateSampleTablesWithIndex(session, false /* no need in table data */); return true; });

        {
            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            // open tx
            const TString queryFirst(Q1_(R"(
                SELECT COUNT(a.Key) FROM `/Root/SecondaryKeys` as a;
            )"));

            std::vector<std::unique_ptr<IEventHandle>> executerResponses;
            bool blockResponses = false;
            bool introspectExecutorResponses = true;
            IKqpGateway::TKqpSnapshot txSnaphsot;

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (ev->GetTypeRewrite() == TEvKqpExecuter::TEvTxResponse::EventType) {
                    if (introspectExecutorResponses) {
                        auto* msg = ev->Get<TEvKqpExecuter::TEvTxResponse>();
                        UNIT_ASSERT_C(msg->Snapshot.IsValid(), "unexpected tx response reply without the snapshot");
                        if (txSnaphsot.IsValid()) {
                            UNIT_ASSERT(txSnaphsot == msg->Snapshot);
                        }

                        txSnaphsot = msg->Snapshot;
                    }

                    if (blockResponses) {
                        executerResponses.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }

                }
        
                return TTestActorRuntime::EEventAction::PROCESS;
            };

            runtime.SetObserverFunc(grab);

            std::optional<TTransaction> tx;

            auto result = kikimr.RunCall([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW());
                return session.ExecuteDataQuery(queryFirst, txc, execSettings).ExtractValueSync();
            });

            UNIT_ASSERT(txSnaphsot.IsValid());
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetTransaction().Defined());
            tx.emplace(*result.GetTransaction());

            // running the query that touches the main table and the index.
            const TString query(Q1_(R"(
                SELECT COUNT(b.Key) FROM `/Root/SecondaryKeys` VIEW Index as b
                UNION ALL
                SELECT COUNT(a.Key) FROM `/Root/SecondaryKeys` as a;
            )"));

            blockResponses = true;

            auto future = kikimr.RunInThreadPool([&]{
                auto txc = TTxControl::Tx(*tx).CommitTx();
                return session.ExecuteDataQuery(query, txc, execSettings).ExtractValueSync();
            });

            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&executerResponses](IEventHandle&) {
                return executerResponses.size() > 0;
            });

            runtime.DispatchEvents(opts);
            Y_VERIFY_S(executerResponses.size() > 0, "empty executer responses???");

            blockResponses = false;
            introspectExecutorResponses = false;

            const TString writeQuery(Q1_(R"(
                REPLACE INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES
                    (1,    1,    "Payload1"),
                    (2,    2,    "Payload2"),
                    (5,    5,    "Payload5"),
                    (NULL, 6,    "Payload6"),
                    (7,    NULL, "Payload7"),
                    (NULL, NULL, "Payload8");
            )"));

            auto writeResult = kikimr.RunCall([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                return writeSession.ExecuteDataQuery(writeQuery, txc, execSettings).ExtractValueSync();
            });

            UNIT_ASSERT(writeResult.IsSuccess());

            // releasing executor response from the paused select query.
            for(auto& ev: executerResponses) {
                runtime.Send(ev.release());
            }

            auto resultSecond = runtime.WaitFuture(future);
            // select must be successful. no transaction locks invalidated issues.
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }
}
}
