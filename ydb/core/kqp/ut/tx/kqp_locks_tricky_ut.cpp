#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>

#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

#include <library/cpp/json/json_reader.h>

#include <util/string/printf.h>
#include <util/generic/scope.h>


namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScripting;

using NYql::TExprContext;
using NYql::TExprNode;

Y_UNIT_TEST_SUITE(KqpLocksTricky) {

    Y_UNIT_TEST_TWIN(TestNoLocksIssue, withSink) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(withSink);

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
                if (ev->GetTypeRewrite() == TEvKqpExecuter::TEvTxResponse::EventType && blockResponses) {
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

            auto saveObserver = runtime.SetObserverFunc(grab);
            Y_DEFER {
                runtime.SetObserverFunc(saveObserver);
            };

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
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(withSink);

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

            auto saveObserver = runtime.SetObserverFunc(grab);
            Y_DEFER {
                runtime.SetObserverFunc(saveObserver);
            };

            std::optional<NYdb::NTable::TTransaction> tx;

            auto result = kikimr.RunCall([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW());
                return session.ExecuteDataQuery(queryFirst, txc, execSettings).ExtractValueSync();
            });

            UNIT_ASSERT(txSnaphsot.IsValid());
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetTransaction().has_value());
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

    Y_UNIT_TEST(TestNoWrite) {
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto deleteSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        
        kikimr.RunCall([&]{ CreateSampleTablesWithIndex(session, false /* no need in table data */); return true; });

        {
            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            const TString query(Q1_(R"(
                SELECT * FROM `/Root/KeyValue` WHERE Key = 3u;

                UPDATE `/Root/KeyValue` SET Value = "Test" WHERE Key = 3u AND Value = "Not exists";
            )"));

            std::vector<std::unique_ptr<IEventHandle>> writes;
            bool blockWrites = true;

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWrite::EventType && blockWrites) {
                    auto* evWrite = ev->Get<NKikimr::NEvents::TDataEvents::TEvWrite>();
                    UNIT_ASSERT(evWrite->Record.OperationsSize() == 0);
                    UNIT_ASSERT(evWrite->Record.GetLocks().GetLocks().size() != 0);
                    UNIT_ASSERT(evWrite->Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Rollback);
                    UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetStep() == 0);
                    UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetTxId() == 0);
                    writes.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            };

            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&writes](IEventHandle&) {
                return writes.size() > 0;
            });

            auto saveObserver = runtime.SetObserverFunc(grab);
            Y_DEFER {
                runtime.SetObserverFunc(saveObserver);
            };

            auto future = kikimr.RunInThreadPool([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                return session.ExecuteDataQuery(query, txc, execSettings).ExtractValueSync();
            });

            runtime.DispatchEvents(opts);
            UNIT_ASSERT(writes.size() > 0);

            blockWrites = false;

            const TString deleteQuery(Q1_(R"(
                DELETE FROM `/Root/KeyValue` WHERE Key = 3u;
            )"));

            auto deleteResult = kikimr.RunCall([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                return deleteSession.ExecuteDataQuery(deleteQuery, txc, execSettings).ExtractValueSync();
            });

            UNIT_ASSERT(deleteResult.IsSuccess());

            for(auto& ev: writes) {
                runtime.Send(ev.release());
            }

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());        
        }
    }

    Y_UNIT_TEST(TestSnapshotIfInsertRead) {
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto upsertSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);
        
        {
            const TString query(Q1_(R"(
                SELECT Ensure("ok", false, "error") FROM `/Root/KeyValue2` WHERE Key = "10u";

                INSERT INTO `/Root/KeyValue` (Key, Value) VALUES (10u, "test");
            )"));

            std::vector<std::unique_ptr<IEventHandle>> writes;

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (writes.empty() && ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWrite::EventType) {
                    auto* evWrite = ev->Get<NKikimr::NEvents::TDataEvents::TEvWrite>();
                    UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetStep() != 0);
                    UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetTxId() != 0);
                    writes.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            };

            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) {
                return writes.size() > 0;
            });

            runtime.SetObserverFunc(grab);

            auto future = kikimr.RunInThreadPool([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                return session.ExecuteDataQuery(query, txc, execSettings).ExtractValueSync();
            });

            runtime.DispatchEvents(opts);
            UNIT_ASSERT(writes.size() > 0);

            {
                const TString upsertQuery(Q1_(R"(
                    INSERT INTO `/Root/KeyValue` (Key, Value) VALUES (10u, "other");
                    INSERT INTO `/Root/KeyValue2` (Key, Value) VALUES ("10u", "other");
                )"));

                auto upsertResult = kikimr.RunCall([&]{
                    auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                    return upsertSession.ExecuteDataQuery(upsertQuery, txc, execSettings).ExtractValueSync();
                });

                UNIT_ASSERT_VALUES_EQUAL_C(upsertResult.GetStatus(), EStatus::SUCCESS, upsertResult.GetIssues().ToString());    
            }


            for(auto& ev: writes) {
                runtime.Send(ev.release());
            }

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());        
        }
    }

    Y_UNIT_TEST_TWIN(TestSecondaryIndexWithoutSnapshot, StreamIndex) {
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(StreamIndex);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto upsertSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        kikimr.RunCall([&]{ CreateSampleTablesWithIndex(session, false /* no need in table data */); return true; });
        
        {
            const TString query(Q1_(R"(
                INSERT INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES (10, 10, "test");
            )"));

            bool hasWrite = false;

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWrite::EventType) {
                    auto* evWrite = ev->Get<NKikimr::NEvents::TDataEvents::TEvWrite>();
                    UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetStep() == 0);
                    UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetTxId() == 0);
                    hasWrite = true;
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            };

            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) {
                return hasWrite;
            });

            runtime.SetObserverFunc(grab);

            auto future = kikimr.RunInThreadPool([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                return session.ExecuteDataQuery(query, txc, execSettings).ExtractValueSync();
            });

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());    

            
            runtime.DispatchEvents(opts);
            UNIT_ASSERT(hasWrite);    
        }
    }

    Y_UNIT_TEST_TWIN(TestSnapshotWithDependentReads, UseSink) {
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto upsertSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        {
                const TString upsertQuery(Q1_(R"(
                    UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (1u, "One");
                    UPSERT INTO `/Root/KeyValue2` (Key, Value) VALUES ("One", "expected");
                )"));

                auto upsertResult = kikimr.RunCall([&]{
                    auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                    return upsertSession.ExecuteDataQuery(upsertQuery, txc, execSettings).ExtractValueSync();
                });

                UNIT_ASSERT_VALUES_EQUAL_C(upsertResult.GetStatus(), EStatus::SUCCESS, upsertResult.GetIssues().ToString());    
        }
        
        {
            const TString query(Q1_(R"(
                $cnt1 = SELECT Value FROM `/Root/KeyValue` WHERE Key = 1u;
                SELECT Ensure("ok", $cnt1="One", "first error");

                $cnt2 = SELECT Value FROM `/Root/KeyValue2` WHERE Key = $cnt1;
                SELECT Ensure("ok", $cnt2="expected", "second error");

                UPSERT INTO KeyValueLargePartition (Key, Value) VALUES
                    (1000u, "test");
            )"));

            std::vector<std::unique_ptr<IEventHandle>> reads;
            bool hasRead = false;
            bool allowAllReads = false;
            bool hasResult = false;

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvRead::EventType) {
                    auto* evRead = ev->Get<NKikimr::TEvDataShard::TEvRead>();
                    UNIT_ASSERT(evRead->Record.GetSnapshot().GetStep() != 0);
                    UNIT_ASSERT(evRead->Record.GetSnapshot().GetTxId() != 0);
                }
                if (!allowAllReads && ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvRead::EventType) {
                    // Block second read
                    if (hasRead) {
                        reads.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    } else {
                        hasRead = true;
                    }
                } else if (!allowAllReads && ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvReadResult::EventType) {
                    hasResult = true;
                    return TTestActorRuntime::EEventAction::PROCESS;
                }


                return TTestActorRuntime::EEventAction::PROCESS;
            };

            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) {
                return reads.size() > 0 && hasResult;
            });

            runtime.SetObserverFunc(grab);

            auto future = kikimr.RunInThreadPool([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                return session.ExecuteDataQuery(query, txc, execSettings).ExtractValueSync();
            });

            runtime.DispatchEvents(opts);
            UNIT_ASSERT(reads.size() > 0 && hasResult);

            {
                const TString upsertQuery(Q1_(R"(
                    UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (1u, "not expected");
                    UPSERT INTO `/Root/KeyValue2` (Key, Value) VALUES ("One", "not expected");
                )"));

                auto upsertResult = kikimr.RunCall([&]{
                    auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                    return upsertSession.ExecuteDataQuery(upsertQuery, txc, execSettings).ExtractValueSync();
                });

                UNIT_ASSERT_VALUES_EQUAL_C(upsertResult.GetStatus(), EStatus::SUCCESS, upsertResult.GetIssues().ToString());    
            }

            allowAllReads = true;

            for(auto& ev: reads) {
                runtime.Send(ev.release());
            }

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());        
        }
    }

    Y_UNIT_TEST_TWIN(TestSnapshotWithOnlineRO, AllowInconsistentReads) {
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto upsertSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        {
            const TString query(Q1_(R"(
                SELECT Value FROM `/Root/KeyValue` WHERE Key = 1u;
                SELECT Value FROM `/Root/KeyValue2` WHERE Key = "One";
            )"));

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvRead::EventType) {
                    auto* evRead = ev->Get<NKikimr::TEvDataShard::TEvRead>();
                    UNIT_ASSERT((evRead->Record.GetSnapshot().GetStep() == 0) == AllowInconsistentReads);
                    UNIT_ASSERT((evRead->Record.GetSnapshot().GetTxId() == 0) == AllowInconsistentReads);
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            };


            runtime.SetObserverFunc(grab);

            auto future = kikimr.RunInThreadPool([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::OnlineRO(
                    TTxOnlineSettings().AllowInconsistentReads(AllowInconsistentReads))).CommitTx();
                return session.ExecuteDataQuery(query, txc, execSettings).ExtractValueSync();
            });

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_TWIN(TestSnapshotWithOnlineROOneShard, AllowInconsistentReads) {
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto upsertSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        {
            const TString query(Q1_(R"(
                SELECT Value FROM `/Root/KeyValue` WHERE Key = 1u;
                SELECT Value FROM `/Root/KeyValue` WHERE Key = 1u;
            )"));

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvRead::EventType) {
                    auto* evRead = ev->Get<NKikimr::TEvDataShard::TEvRead>();
                    UNIT_ASSERT((evRead->Record.GetSnapshot().GetStep() == 0));
                    UNIT_ASSERT((evRead->Record.GetSnapshot().GetTxId() == 0));
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            };

            runtime.SetObserverFunc(grab);

            auto future = kikimr.RunInThreadPool([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::OnlineRO(
                    TTxOnlineSettings().AllowInconsistentReads(AllowInconsistentReads))).CommitTx();
                return session.ExecuteDataQuery(query, txc, execSettings).ExtractValueSync();
            });

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }
}
}
