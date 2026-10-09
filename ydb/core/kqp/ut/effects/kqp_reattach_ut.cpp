#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <util/generic/scope.h>


namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpReattach) {

    Y_UNIT_TEST_TWIN(DistributedCommitWithoutCookie, Volatile) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false).SetEnableDataShardVolatileTransactions(Volatile);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        THashSet<ui64> preparedTxIds;
        size_t repliesWithoutCookie = 0;
        {
            auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType) {
                    const auto& record = ev->Get<NEvents::TDataEvents::TEvWriteResult>()->Record;
                    if (record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED) {
                        preparedTxIds.insert(record.GetTxId());
                    } else if (record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED
                            && preparedTxIds.contains(record.GetTxId()) && ev->Cookie != 0) {
                        // Older DataShards may send distributed commit replies without
                        // echoing the cookie of the prepare request.
                        ev = new IEventHandle(ev->GetRecipientRewrite(), ev->Sender,
                            ev->Release<NEvents::TDataEvents::TEvWriteResult>().Release(),
                            ev->Flags, 0, nullptr, std::move(ev->TraceId));
                        ++repliesWithoutCookie;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });
            Y_DEFER { runtime.SetObserverFunc(observer); };

            auto result = kikimr.RunCall([&] {
                return session.ExecuteQuery(Q1_(R"(
                    UPSERT INTO `/Root/TwoShard` (Key, Value1)
                    VALUES (1u, 'value'), (4000000001u, 'value');
                )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_C(repliesWithoutCookie >= 2, "Expected commit replies from both shards");
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto result = kikimr.RunCall([&] {
            return session.ExecuteQuery(Q1_(R"(
                SELECT Value1 FROM `/Root/TwoShard`
                WHERE Key IN (1u, 4000000001u) ORDER BY Key;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["value"]];[["value"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

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
}
}
}
