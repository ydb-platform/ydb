#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <util/generic/scope.h>

#include <atomic>


namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpReattach) {

    Y_UNIT_TEST(ReattachDeliveryProblem) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false).SetEnableDataShardVolatileTransactions(false);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });
        
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        Y_UNUSED(runtime);

        {
            const TString query(Q1_(R"(
                UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES (1u, 'value'), (4000000001u, 'value');
            )"));

            std::vector<std::unique_ptr<IEventHandle>> requests;

            TActorId writeActor;
            ui64 tablet = 0;
            ui64 txId = 0;
            
            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (tablet == 0 && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType) {
                    auto* msg = ev->Get<NEvents::TDataEvents::TEvWriteResult>();

                    writeActor = ev->Recipient;
                    tablet = msg->Record.GetOrigin();
                    txId = msg->Record.GetTxId();

                    return TTestActorRuntime::EEventAction::PROCESS;
                } else if (ev->GetTypeRewrite() == TEvTxProxy::TEvProposeTransaction::EventType) {
                    auto restartTx = std::make_unique<TEvDataShard::TEvProposeTransactionRestart>(
                        tablet,
                        txId);

                    runtime.Send(writeActor, ev->Sender, restartTx.release());

                    auto deliveryProblem = std::make_unique<TEvPipeCache::TEvDeliveryProblem>(
                        tablet,
                        false);

                    runtime.Send(writeActor, ev->Sender, deliveryProblem.release());

                    requests.emplace_back(ev.Release());

                    return TTestActorRuntime::EEventAction::DROP;
                } else if (ev->GetTypeRewrite() == TEvDataShard::TEvProposeTransactionAttach::EventType) {
                    UNIT_ASSERT(ev->Cookie == 1);

                    auto deliveryProblem = std::make_unique<TEvPipeCache::TEvDeliveryProblem>(
                        tablet,
                        true);

                    runtime.Send(writeActor, ev->Sender, deliveryProblem.release());

                    requests.emplace_back(ev.Release());

                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            };


            runtime.SetObserverFunc(grab);

            auto future = kikimr.RunInThreadPool([&]{
                return session.ExecuteQuery(
                    query,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            });

            const size_t requestsExpected = 2;

            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) {
                return requests.size() >= requestsExpected;
            });
            runtime.DispatchEvents(opts);
            UNIT_ASSERT(requests.size() == requestsExpected);

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNDETERMINED, result.GetIssues().ToString());
        }
    }

    // YDBBUGS-1232: a ColumnShard pipe break during a write commit must trigger a
    // reattach attempt, not an immediate UNDETERMINED. Unlike DataShard, ColumnShard
    // implements neither TEvProposeTransactionRestart nor TEvProposeTransactionAttach,
    // so TKqpTransactionManager::ShouldReattach() never permits reattach: its gate
    // requires a restart notification that ColumnShard never sends. This repro injects
    // a delivery problem for the participating columnshard during the distributed
    // commit (with no restart notification, faithful to ColumnShard) and asserts that
    // KQP attempts to reattach. It fails until ColumnShard recovery is implemented.
    Y_UNIT_TEST(ColumnShardDeliveryProblemMustReattach) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false).SetEnableDataShardVolatileTransactions(false);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        kikimr.RunCall([&] {
            auto result = session.ExecuteQuery(Q_(R"(
                CREATE TABLE `/Root/ColumnReattach` (
                    Key Uint64 NOT NULL,
                    Value Utf8,
                    PRIMARY KEY (Key)
                ) WITH (STORE = COLUMN);
            )"), TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            return result;
        });

        std::atomic<bool> reattachAttempted{false};
        ui64 tablet = 0;

        auto grab = [&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
            if (ev->GetTypeRewrite() != TEvPipeCache::EvForward) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }
            auto* fwd = ev->Get<TEvPipeCache::TEvForward>();
            if (!fwd->Ev) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            if (fwd->Ev->Type() == TEvTxProxy::TEvProposeTransaction::EventType && tablet == 0) {
                // The buffer write actor is already in StateCommit and is proposing the
                // distributed commit to the coordinator. Simulate a columnshard pipe
                // break: deliver a delivery problem for the participating columnshard,
                // with no restart notification (ColumnShard never sends one).
                const auto& proposal = static_cast<const TEvTxProxy::TEvProposeTransaction&>(*fwd->Ev).Record;
                const auto& affected = proposal.GetTransaction().GetAffectedSet();
                UNIT_ASSERT_C(affected.size() > 0, "expected a participating columnshard");
                tablet = affected.Get(0).GetTabletId();

                const TActorId bufferActor = ev->Sender;
                auto deliveryProblem = std::make_unique<TEvPipeCache::TEvDeliveryProblem>(
                    tablet, /* notDelivered */ false);
                runtime.Send(bufferActor, ev->Recipient, deliveryProblem.release());

                return TTestActorRuntime::EEventAction::DROP;
            }

            if (fwd->Ev->Type() == TEvDataShard::TEvProposeTransactionAttach::EventType) {
                reattachAttempted.store(true);
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime.SetObserverFunc(grab);

        auto future = kikimr.RunInThreadPool([&] {
            return session.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/ColumnReattach` (Key, Value) VALUES (1u, 'value');
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        });

        auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
        UNIT_ASSERT_C(reattachAttempted.load(),
            "KQP did not attempt to reattach to the columnshard after a mid-commit delivery problem; "
            "query status: " + result.GetIssues().ToString());
    }
}
}
}
