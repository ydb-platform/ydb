#pragma once

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/mlp/mlp.h>

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

inline void CreateTopic(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName,
    NYdb::NTopic::TCreateTopicSettings& settings) {
    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic(topicName, settings);

    setup->GetServer().WaitInit(GetTopicPath(topicName));
}

inline void CreateTopic(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName, const TString& consumerName) {
    return CreateTopic(setup, topicName, NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer(consumerName)
                .KeepMessagesOrder(false)
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(10)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()
        );
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

inline TActorId CreateDescriberActor(NActors::TTestActorRuntime& runtime,const TString& databasePath, const TString& topicPath) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto readerId = runtime.Register(NDescriber::CreateDescriberActor(edgeId, databasePath, {topicPath}));
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

inline THolder<NDescriber::TEvDescribeTopicsResponse> GetDescriberResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5)) {
    return runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(timeout);
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

inline void WriteMany(std::shared_ptr<TTopicSdkTestSetup> setup, const std::string& topic, ui32 partitionId, size_t messageSize, size_t messageCount) {
    TTopicClient client(setup->MakeDriver());

    TWriteSessionSettings settings;
    settings.Path(topic);
    settings.PartitionId(partitionId);
    settings.Codec(NYdb::NTopic::ECodec::RAW);
    settings.DeduplicationEnabled(true);
    settings.ProducerId("test-producer")
        .MessageGroupId("test-producer");
    auto session = client.CreateSimpleBlockingWriteSession(settings);

    for(; messageCount; --messageCount) {
        auto message = NUnitTest::RandomString(messageSize);
        UNIT_ASSERT(session->Write(message));
    }

    session->Close();

}

}
