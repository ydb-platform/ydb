#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

namespace NKikimr::NKqp::NScheduler {

Y_UNIT_TEST_SUITE(TKqpSchedulerService) {

    /* Scenario:
        - Start query execution and receive TEvTxRequest.
        - When sending TEvAddQuery from executer to scheduler, immediately receive TEvAbortExecution.
        - Imitate receiving TEvQueryResponse before receiving self TEvPoison by executer.
        - Do not crash or get undefined behavior.
     */
    Y_UNIT_TEST(TestSuddenAbortAfterReady) {
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->MutableComputeSchedulerSettings()->SetAccountDefaultPool(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); } );
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );

        auto prepareResult = kikimr.RunCall([&] { return session.PrepareDataQuery(Q_(R"(
                SELECT COUNT(*) FROM `/Root/TwoShard`;
            )")).GetValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(prepareResult.GetStatus(), EStatus::SUCCESS, prepareResult.GetIssues().ToString());
        auto dataQuery = prepareResult.GetQuery();

        TActorId executerId, targetId;
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            {
                TStringStream ss;
                ss << "Got " << ev->GetTypeName() << " " << ev->Recipient << " " << ev->Sender << Endl;
                Cerr << ss.Str();
            }

            if (ev->GetTypeRewrite() == TEvKqpExecuter::TEvTxRequest::EventType) {
                targetId = ActorIdFromProto(ev->Get<TEvKqpExecuter::TEvTxRequest>()->Record.GetTarget());
            }

            if (ev->GetTypeRewrite() == NScheduler::TEvAddQuery::EventType) {
                executerId = ev->Sender;
                auto* abortExecution = new TEvKqp::TEvAbortExecution(NYql::NDqProto::StatusIds::UNSPECIFIED, NYql::TIssues());
                runtime.Send(new IEventHandle(ev->Sender, targetId, abortExecution));
            }

            if (ev->GetTypeRewrite() == NActors::TEvents::TEvPoison::EventType && ev->Sender == executerId && ev->Recipient == executerId) {
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto future = kikimr.RunInThreadPool([&] { return dataQuery.Execute(TTxControl::BeginTx().CommitTx(), TExecDataQuerySettings()).GetValueSync(); });

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&](IEventHandle& ev) {
            return ev.GetTypeRewrite() == TEvKqpExecuter::TEvTxResponse::EventType;
        });
        runtime.DispatchEvents(opts);

        auto result = runtime.WaitFuture(future);
        UNIT_ASSERT(!result.IsSuccess());
    }
}

} // namespace NKikimr
