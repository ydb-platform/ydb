#include "actors.h"

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/testlib/grpc_request/grpc_request.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

using namespace NYdb::NTopic::NTests;
using namespace NKikimr::Tests::NGrpc;

std::shared_ptr<TTopicSdkTestSetup> CreateSetup() {
    auto setup = std::make_shared<TTopicSdkTestSetup>("PQv1");
    setup->GetServer().EnableLogs({
            NKikimrServices::PQ_MLP_READER,
            NKikimrServices::PQ_MLP_WRITER,
            NKikimrServices::PQ_MLP_COMMITTER,
            NKikimrServices::PQ_MLP_UNLOCKER,
            NKikimrServices::PQ_MLP_DEADLINER,
            NKikimrServices::PQ_MLP_PURGER,
            NKikimrServices::PQ_MLP_CONSUMER,
            NKikimrServices::PQ_MLP_ENRICHER,
            NKikimrServices::PQ_MLP_DLQ_MOVER,
            NKikimrServices::PQ_MLP_DESCRIBER,
        },
        NActors::NLog::PRI_DEBUG
    );
    setup->GetServer().EnableLogs({
            NKikimrServices::PERSQUEUE,
            NKikimrServices::PERSQUEUE_READ_BALANCER,
            NKikimrServices::PQ_WRITE_PROXY
        },
        NActors::NLog::PRI_INFO
    );

    return setup;
}

template<typename TRequest, typename TResponse>
std::shared_ptr<TResultHolder<TResponse>> DoRequest(NActors::TTestActorRuntime& runtime, const TRequest& request) {
    auto result = std::make_shared<TResultHolder<TResponse>>();

    auto ctx = new TRequestCtx<TRequest, TResponse>(
        request,
        "/Root/test_db/topic1",
        "/Root/test_db",
        result
    );
    runtime.Register(CreateCreateTopicActor(ctx));

    for (int i = 0; i < 50; ++i) {
        if (result->ResultStatus) {
            break;
        }

        Sleep(TDuration::MilliSeconds(50));
    }

    UNIT_ASSERT_C(result->ResultStatus, "The operation is still in progress");
    return result;
}

    

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(CreateTopic) {

Y_UNIT_TEST(SharedConsumer) {

    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);


    Ydb::PersQueue::V1::CreateTopicRequest request;
    request.set_path("/Root/test_db/topic1");

    auto& settings = *request.mutable_settings();
    settings.set_partitions_count(1);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Days(1).MilliSeconds());

    settings.mutable_attributes()->insert({"_federation_account", "account1"});

    auto& readRule = *settings.add_read_rules();

    readRule.set_consumer_name("test_consumer");
    readRule.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    readRule.set_version(1);
    //readRule.set_service_type("test_service_type");
    readRule.set_starting_message_timestamp_ms(1000);

    auto& type = *readRule.mutable_shared_consumer_type();
    type.set_keep_messages_order(true);
    type.mutable_default_processing_timeout()->set_seconds(3);
    type.mutable_receive_message_wait_time()->set_seconds(5);
    type.mutable_receive_message_delay()->set_seconds(7);
    type.mutable_dead_letter_policy()->set_enabled(true);
    type.mutable_dead_letter_policy()->mutable_condition()->set_max_processing_attempts(11);
    type.mutable_dead_letter_policy()->mutable_move_action()->set_dead_letter_queue("test_dead_letter_queue");

    auto result = DoRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(runtime, request);

    auto status = result->ResultStatus;
    UNIT_ASSERT(status);
    UNIT_ASSERT_VALUES_EQUAL_C(*status, Ydb::StatusIds::SUCCESS, result->Issues.ToString());

    runtime.Register(NPQ::NDescriber::CreateDescriberActor(runtime.AllocateEdgeActor(), "/Root/test_db", {"/Root/test_db/topic1"}));
    auto response = runtime.GrabEdgeEvent<NPQ::NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));

    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1);
    auto topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NPQ::NDescriber::EStatus::SUCCESS);

    auto config = topic.Info->Description.GetPQTabletConfig();
    const auto* consumer = NPQ::GetConsumer(config, "test_consumer");
    UNIT_ASSERT(consumer);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetImportant(), false);
    UNIT_ASSERT_VALUES_EQUAL(NKikimrPQ::TPQTabletConfig::EConsumerType_Name(consumer->GetType()),
        ::NKikimrPQ::TPQTabletConfig::EConsumerType_Name(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP));
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetKeepMessageOrder(), true);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetDefaultProcessingTimeoutSeconds(), 3);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetDefaultReceiveMessageWaitTimeMs(), 5000);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetDefaultDelayMessageTimeMs(), 7000);
    UNIT_ASSERT_VALUES_EQUAL(NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(consumer->GetDeadLetterPolicy()),
        NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE));
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetMaxProcessingAttempts(), 11);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetDeadLetterQueue(), "test_dead_letter_queue");
}

};

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
