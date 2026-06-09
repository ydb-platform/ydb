#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <util/generic/scope.h>


namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpOverload) {

    Y_UNIT_TEST(OltpOverloaded) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });
        
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        Y_UNUSED(runtime);

        {
            const TString query(Q1_(R"(
                UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (1, 'value');
                SELECT * FROM `/Root/KeyValue`;
            )"));

            std::vector<std::unique_ptr<IEventHandle>> requests;
            std::vector<std::unique_ptr<IEventHandle>> responses;
            bool blockResults = true;

            size_t overloadSeqNo = 0;

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (blockResults && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType) {
                    auto* msg = ev->Get<NEvents::TDataEvents::TEvWriteResult>();

                    auto overloadedResult = NEvents::TDataEvents::TEvWriteResult::BuildError(
                        msg->Record.GetOrigin(),
                        msg->Record.GetTxId(),
                        NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED,
                        "");

                    UNIT_ASSERT(overloadSeqNo > 0);
                    overloadedResult->Record.SetOverloadSubscribed(overloadSeqNo);

                    runtime.Send(ev->Recipient, ev->Sender, overloadedResult.release());

                    auto overloadedReady = std::make_unique<TEvDataShard::TEvOverloadReady>(msg->Record.GetOrigin(), overloadSeqNo);

                    runtime.Send(ev->Recipient, ev->Sender, overloadedReady.release());

                    responses.emplace_back(ev.Release());

                    blockResults = false;

                    return TTestActorRuntime::EEventAction::DROP;
                } else if (!blockResults && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWrite::EventType) {
                    for(auto& ev : responses) {
                        runtime.Send(ev.release());
                    }
                    responses.clear();
                    requests.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                } else if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWrite::EventType) {
                    auto* msg = ev->Get<NEvents::TDataEvents::TEvWrite>();
                    overloadSeqNo = msg->Record.GetOverloadSubscribe();
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            };


            runtime.SetObserverFunc(grab);

            auto future = kikimr.RunInThreadPool([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW());
                return session.ExecuteQuery(query, txc).ExtractValueSync();
            });

            size_t requestsExpected = 1;

            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) {
                return requests.size() >= requestsExpected;
            });
            runtime.DispatchEvents(opts);
            AFL_ENSURE(requests.size() == requestsExpected);

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto tx = result.GetTransaction();
            UNIT_ASSERT(tx);


            blockResults = true;
            ++requestsExpected;

            auto commitFuture = kikimr.RunInThreadPool([&]{
                return tx->Commit().GetValueSync();
            });

            runtime.DispatchEvents(opts);
            AFL_ENSURE(requests.size() == requestsExpected);

            auto commitResult = runtime.WaitFuture(commitFuture);
            UNIT_ASSERT_C(commitResult.IsSuccess(), commitResult.GetIssues().ToString());
        }
    }
}
}
}
