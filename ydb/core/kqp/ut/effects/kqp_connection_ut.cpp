#include <ydb/core/kqp/common/buffer/events.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <util/generic/scope.h>


namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpFail) {

    Y_UNIT_TEST(Immediate) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });
        
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        Y_UNUSED(runtime);

        auto edgeActor = runtime.AllocateEdgeActor();

        const auto& shards = GetTableShards(
            &kikimr.GetTestServer(),
            edgeActor,
            "/Root/KeyValue");

        {
            const TString query =
                R"(
                    SELECT * FROM `/Root/KeyValue`;
                    UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (2, 'value');
                )";

            std::vector<std::unique_ptr<IEventHandle>> requests;
            int stage = 0;

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (stage == 0 && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWrite::EventType) {
                    auto* msg = ev->Get<NEvents::TDataEvents::TEvWrite>();
                    UNIT_ASSERT(msg->Record.GetLocks().GetLocks().size() == 1);
                    UNIT_ASSERT(msg->Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Commit);
                    ++stage;
                    requests.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                } else if (stage == 1 && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWrite::EventType) {
                    auto* msg = ev->Get<NEvents::TDataEvents::TEvWrite>();
                    UNIT_ASSERT(msg->Record.GetLocks().GetLocks().size() == 1);
                    UNIT_ASSERT(msg->Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Rollback);
                    ++stage;
                    return TTestActorRuntime::EEventAction::PROCESS;
                }

                UNIT_ASSERT(ev->GetTypeRewrite() != TEvKqpBuffer::TEvError::EventType);
                UNIT_ASSERT(ev->GetTypeRewrite() != TEvKqpBuffer::TEvTerminate::EventType);

                return TTestActorRuntime::EEventAction::PROCESS;
            };


            runtime.SetObserverFunc(grab);

            auto future = kikimr.RunInThreadPool([&]{
                return session.ExecuteQuery(
                    query,
                    TTxControl::BeginTx().CommitTx(),
                    TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return stage == 1;
                });
                runtime.DispatchEvents(opts);
            }

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED, result.GetIssues().ToString());

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return stage == 2;
                });
                runtime.DispatchEvents(opts);
            }

            UNIT_ASSERT(stage == 2);
        }
    }

    Y_UNIT_TEST(OnPrepare) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });
        
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        Y_UNUSED(runtime);

        auto edgeActor = runtime.AllocateEdgeActor();

        const auto& shards = GetTableShards(
            &kikimr.GetTestServer(),
            edgeActor,
            "/Root/TwoShard");

        {
            const TString query =
                R"(
                    SELECT * FROM `/Root/TwoShard`;
                    UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES (2, 'value');
                )";

            std::vector<std::unique_ptr<IEventHandle>> requests;
            int stage = 0;

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (stage < 2 && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWrite::EventType) {
                    auto* msg = ev->Get<NEvents::TDataEvents::TEvWrite>();
                    UNIT_ASSERT(msg->Record.GetLocks().GetLocks().size() == 1);
                    UNIT_ASSERT(msg->Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Commit);
                    ++stage;
                    requests.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                } else if (ev->GetTypeRewrite() == TEvTxProxy::TEvProposeTransaction::EventType) {
                    UNIT_ASSERT(false);
                } else if (stage < 4 && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWrite::EventType) {
                    auto* msg = ev->Get<NEvents::TDataEvents::TEvWrite>();
                    UNIT_ASSERT(msg->Record.GetLocks().GetLocks().size() == 1);
                    UNIT_ASSERT(msg->Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Rollback);
                    ++stage;
                    return TTestActorRuntime::EEventAction::PROCESS;
                }

                UNIT_ASSERT(ev->GetTypeRewrite() != TEvKqpBuffer::TEvError::EventType);
                UNIT_ASSERT(ev->GetTypeRewrite() != TEvKqpBuffer::TEvTerminate::EventType);

                return TTestActorRuntime::EEventAction::PROCESS;
            };


            runtime.SetObserverFunc(grab);

            auto future = kikimr.RunInThreadPool([&]{
                return session.ExecuteQuery(
                    query,
                    TTxControl::BeginTx().CommitTx(),
                    TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return stage == 2;
                });
                runtime.DispatchEvents(opts);
            }

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED, result.GetIssues().ToString());

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return stage == 4;
                });
                runtime.DispatchEvents(opts);
            }

            UNIT_ASSERT(stage == 4);
        }
    }

    Y_UNIT_TEST(OnCommit) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });
        
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        Y_UNUSED(runtime);

        auto edgeActor = runtime.AllocateEdgeActor();

        const auto& shards = GetTableShards(
            &kikimr.GetTestServer(),
            edgeActor,
            "/Root/TwoShard");

        {
            const TString query =
                R"(
                    SELECT * FROM `/Root/TwoShard`;
                    UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES (2, 'value');
                )";

            std::vector<std::unique_ptr<IEventHandle>> requests;
            int stage = 0;

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (stage < 2 && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWrite::EventType) {
                    auto* msg = ev->Get<NEvents::TDataEvents::TEvWrite>();
                    UNIT_ASSERT(msg->Record.GetLocks().GetLocks().size() == 1);
                    UNIT_ASSERT(msg->Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Commit);
                    ++stage;
                    return TTestActorRuntime::EEventAction::PROCESS;
                } else if (ev->GetTypeRewrite() == TEvTxProxy::TEvProposeTransaction::EventType) {
                    AFL_ENSURE(stage == 2);
                    ++stage;
                    requests.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                } else if (stage < 5 && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWrite::EventType) {
                    auto* msg = ev->Get<NEvents::TDataEvents::TEvWrite>();
                    UNIT_ASSERT(msg->Record.GetLocks().GetLocks().size() == 1);
                    UNIT_ASSERT(msg->Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Rollback);
                    ++stage;
                    return TTestActorRuntime::EEventAction::PROCESS;
                }

                UNIT_ASSERT(ev->GetTypeRewrite() != TEvKqpBuffer::TEvError::EventType);
                UNIT_ASSERT(ev->GetTypeRewrite() != TEvKqpBuffer::TEvTerminate::EventType);

                return TTestActorRuntime::EEventAction::PROCESS;
            };


            runtime.SetObserverFunc(grab);

            auto future = kikimr.RunInThreadPool([&]{
                return session.ExecuteQuery(
                    query,
                    TTxControl::BeginTx().CommitTx(),
                    TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
            });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return stage == 3;
                });
                runtime.DispatchEvents(opts);
            }

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED, result.GetIssues().ToString());

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return stage == 5;
                });
                runtime.DispatchEvents(opts);
            }

            UNIT_ASSERT(stage == 5);
        }
    }
}
}
}
