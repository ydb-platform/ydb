#pragma once

#include "mlp.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

namespace NKikimr::NPQ::NMLP {

using namespace NPersQueue;

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

inline auto CreateSetup() {
    auto setup = std::make_shared<TTopicSdkTestSetup>("TODO");
    setup->GetServer().EnableLogs({
            NKikimrServices::PQ_MLP_READER,
            NKikimrServices::PQ_MLP_COMMITTER,
            NKikimrServices::PQ_MLP_UNLOCKER,
            NKikimrServices::PQ_MLP_DEADLINER,
            NKikimrServices::PQ_MLP_CONSUMER,
            NKikimrServices::PQ_MLP_ENRICHER,
            NKikimrServices::PERSQUEUE,
            NKikimrServices::PERSQUEUE_READ_BALANCER,
        },
        NActors::NLog::PRI_DEBUG
    );
    return setup;
}

inline void ExecuteDDL(TTopicSdkTestSetup& setup, const TString& query) {
    TDriver driver(setup.MakeDriverConfig());
    TQueryClient client(driver);
    auto session = client.GetSession().GetValueSync().GetSession();

    Cerr << "DDL: " << query << Endl << Flush;
    auto res = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

    driver.Stop(true);
}

inline void CreateTopic(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName, const TString& consumerName) {
    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    const auto settings = NYdb::NTopic::TCreateTopicSettings()
            .BeginAddConsumer()
                .ConsumerName(consumerName)
                .AddAttribute("_mlp", "1")
            .EndAddConsumer();
    client.CreateTopic(topicName, settings);

    setup->GetServer().WaitInit(GetTopicPath(topicName));
}

inline TActorId CreateReaderActor(NActors::TTestActorRuntime& runtime, TReaderSettings&& settings) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto readerId = runtime.Register(CreateReader(edgeId, std::move(settings)));
    runtime.EnableScheduleForActor(readerId);
    runtime.DispatchEvents();

    return readerId;
}

inline TActorId CreateCommitterActor(NActors::TTestActorRuntime& runtime, TCommitterSettings&& settings) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto readerId = runtime.Register(CreateCommitter(edgeId, std::move(settings)));
    runtime.EnableScheduleForActor(readerId);
    runtime.DispatchEvents();

    return readerId;
}

inline TActorId CreateMessageDeadlineChangerActor(NActors::TTestActorRuntime& runtime, TMessageDeadlineChangerSettings&& settings) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto readerId = runtime.Register(CreateMessageDeadlineChanger(edgeId, std::move(settings)));
    runtime.EnableScheduleForActor(readerId);
    runtime.DispatchEvents();

    return readerId;
}

inline THolder<TEvPQ::TEvMLPReadResponse> WaitResult(NActors::TTestActorRuntime& runtime) {
    return runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>();
}

inline THolder<TEvReadResponse> GetReadResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5)) {
    return runtime.GrabEdgeEvent<TEvReadResponse>(timeout);
}

inline THolder<TEvChangeResponse> GetChangeResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5)) {
    return runtime.GrabEdgeEvent<TEvChangeResponse>(timeout);
}

inline void AssertReadError(NActors::TTestActorRuntime& runtime, Ydb::StatusIds::StatusCode errorCode, const TString& message, TDuration timeout = TDuration::Seconds(5)) {
    auto response = GetReadResponse(runtime, timeout);
    if (!response) {
        UNIT_FAIL("Timeout");
    }

    UNIT_ASSERT_VALUES_EQUAL_C(Ydb::StatusIds::StatusCode_Name(response->Status),
        Ydb::StatusIds::StatusCode_Name(errorCode), response->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(response->ErrorDescription, message);
}

}
