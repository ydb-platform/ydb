#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpExecuter) {

    /* Scenario:
        - Start query execution and receive TEvTxRequest.
        - When sending TEvAddQuery from executer to scheduler, immediately receive TEvAbortExecution.
        - Imitate receiving TEvQueryResponse before receiving self TEvPoison by executer.
        - Check that scheduler got TEvRemoveQuery.
        - Do not crash or get undefined behavior.
     */
    Y_UNIT_TEST(TestSuddenAbortAfterReady) {
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->MutableComputeSchedulerSettings()->SetAccountDefaultPool(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); } );
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );

        TActorId executerId, targetId;
        ui8 queries = 0;
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            Cerr << (TStringBuilder() << "Got " << ev->GetTypeName() << " " << ev->Recipient << " " << ev->Sender << Endl);

            if (ev->GetTypeRewrite() == TEvKqpExecuter::TEvTxRequest::EventType) {
                targetId = ActorIdFromProto(ev->Get<TEvKqpExecuter::TEvTxRequest>()->Record.GetTarget());
            }

            if (ev->GetTypeRewrite() == NScheduler::TEvAddQuery::EventType) {
                ++queries;
                executerId = ev->Sender;
                auto* abortExecution = new TEvKqp::TEvAbortExecution(NYql::NDqProto::StatusIds::UNSPECIFIED, NYql::TIssues());
                runtime.Send(new IEventHandle(ev->Sender, targetId, abortExecution));
            }

            if (ev->GetTypeRewrite() == NActors::TEvents::TEvPoison::EventType && ev->Sender == executerId && ev->Recipient == executerId) {
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto future = kikimr.RunInThreadPool([&] {
            return session.ExecuteDataQuery("SELECT COUNT(*) FROM `/Root/TwoShard`;", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&](IEventHandle& ev) {
            if (ev.GetTypeRewrite() == NScheduler::TEvRemoveQuery::EventType) {
                --queries;
            }
            return (ev.GetTypeRewrite() == TEvKqpExecuter::TEvTxResponse::EventType || ev.GetTypeRewrite() == NScheduler::TEvRemoveQuery::EventType) && !queries;
        });
        runtime.DispatchEvents(opts);

        auto result = runtime.WaitFuture(future);
        UNIT_ASSERT(!result.IsSuccess());
    }

    // TODO: Test shard write shuffle.
    /*
    Y_UNIT_TEST(BlindWriteDistributed) {
        TKikimrRunner kikimr;
        auto gateway = MakeIcGateway(kikimr);

        TExprContext ctx;
        auto tx = BuildTxPlan(R"(
            DECLARE $items AS 'List<Struct<Key:Uint64?, Text:String?>>';

            $itemsSource = (
                SELECT Item.Key AS Key, Item.Text AS Text
                FROM (SELECT $items AS List) FLATTEN BY List AS Item
            );

            UPSERT INTO [Root/EightShard]
            SELECT * FROM $itemsSource;
        )", gateway, ctx, kikimr.GetTestServer().GetRuntime()->GetAnyNodeActorSystem());

        LogTxPlan(kikimr, tx);

        auto db = kikimr.GetTableClient();
        auto params = db.GetParamsBuilder()
            .AddParam("$items")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(205)
                    .AddMember("Text")
                        .OptionalString("New")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(505)
                    .AddMember("Text")
                        .OptionalString("New")
                    .EndStruct()
                .EndList()
                .Build()
            .Build();

        auto paramsMap = GetParamsMap(std::move(params));

        IKqpGateway::TExecPhysicalRequest request;
        request.Transactions.emplace_back(tx.Ref(), GetParamRefsMap(paramsMap));

        auto txResult = gateway->ExecutePhysical(std::move(request)).GetValueSync();
        UNIT_ASSERT(txResult.Success());

        UNIT_ASSERT_VALUES_EQUAL(txResult.ExecuterResult.GetStats().GetAffectedShards(), 2);

        auto session = db.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM [Root/EightShard] WHERE Text = "New" ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"(
            [
                [#;[205u];["New"]];
                [#;[505u];["New"]]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    */
}

} // namespace NKikimr::NKqp
