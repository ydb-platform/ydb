#include "mlp.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>


namespace NKikimr::NPQ::NMLP {

using namespace NPersQueue;

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

Y_UNIT_TEST_SUITE(TMLPReaderTests) {

    void ExecuteDDL(TTopicSdkTestSetup& setup, const TString& query) {
        TDriver driver(setup.MakeDriverConfig());
        TQueryClient client(driver);
        auto session = client.GetSession().GetValueSync().GetSession();

        Cerr << "DDL: " << query << Endl << Flush;
        auto res = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        driver.Stop(true);
    }

    void CreateTopic(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName, const TString& consumerName) {
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

    void CreateActor(NActors::TTestActorRuntime& runtime, TReaderSetting&& settings) {
        auto edgeId = runtime.AllocateEdgeActor();
        auto describerId = runtime.Register(CreateReader(edgeId, std::move(settings)));
        runtime.EnableScheduleForActor(describerId);
        runtime.DispatchEvents();
    }

    THolder<TEvPersQueue::TEvMLPReadResponse> WaitResult(NActors::TTestActorRuntime& runtime) {
        return runtime.GrabEdgeEvent<TEvPersQueue::TEvMLPReadResponse>();
    }

    void AssertError(NActors::TTestActorRuntime& runtime, ::NPersQueue::NErrorCode::EErrorCode errorCode, const TString& message, TDuration timeout = TDuration::Seconds(5)) {
        auto ev = runtime.GrabEdgeEvent<TEvPersQueue::TEvMLPErrorResponse>(timeout);
        UNIT_ASSERT_VALUES_EQUAL_C(::NPersQueue::NErrorCode::EErrorCode_Name(ev->GetErrorCode()),
            ::NPersQueue::NErrorCode::EErrorCode_Name(errorCode), ev->GetErrorMessage());
        UNIT_ASSERT_VALUES_EQUAL(ev->GetErrorMessage(), message);
    }

    Y_UNIT_TEST(TopicNotExistsConsumer) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::PQ_MLP_READER, NKikimrServices::PQ_MLP_CONSUMER },
                NActors::NLog::PRI_DEBUG
        );
        
        auto& runtime = setup->GetRuntime();
        CreateActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic_not_exists",
            .Consumer = "consumer_not_exists"
        });

        AssertError(runtime, ::NPersQueue::NErrorCode::EErrorCode::SCHEMA_ERROR,
            "You do not have access or the '/Root/topic_not_exists' does not exist");
    }

    Y_UNIT_TEST(TopicWithoutConsumer) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::PQ_MLP_READER, NKikimrServices::PQ_MLP_CONSUMER },
                NActors::NLog::PRI_DEBUG
        );
        
        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        CreateActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "consumer_not_exists"
        });

        AssertError(runtime, ::NPersQueue::NErrorCode::EErrorCode::SCHEMA_ERROR,
            "Consumer 'consumer_not_exists' does not exist");
    }

    Y_UNIT_TEST(EmptyTopic) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::PQ_MLP_READER, NKikimrServices::PQ_MLP_CONSUMER },
                NActors::NLog::PRI_DEBUG
        );

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        auto& runtime = setup->GetRuntime();
        CreateActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer"
        });

        AssertError(runtime, ::NPersQueue::NErrorCode::EErrorCode::SCHEMA_ERROR,
            "Consumer 'consumer_not_exists' not found");
    }
}

} // namespace NKikimr::NPQ::NMLP
