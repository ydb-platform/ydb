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

    auto CreateSetup() {
        auto setup = std::make_shared<TTopicSdkTestSetup>("TODO");
        setup->GetServer().EnableLogs({
                NKikimrServices::PQ_MLP_READER,
                NKikimrServices::PQ_MLP_CONSUMER,
                NKikimrServices::PERSQUEUE,
                NKikimrServices::PERSQUEUE_READ_BALANCER,
            },
            NActors::NLog::PRI_DEBUG
        );
        return setup;
    }

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
        auto readerId = runtime.Register(CreateReader(edgeId, std::move(settings)));
        runtime.EnableScheduleForActor(readerId);
        runtime.DispatchEvents();
    }

    THolder<TEvPersQueue::TEvMLPReadResponse> WaitResult(NActors::TTestActorRuntime& runtime) {
        return runtime.GrabEdgeEvent<TEvPersQueue::TEvMLPReadResponse>();
    }

    void AssertError(NActors::TTestActorRuntime& runtime, ::NPersQueue::NErrorCode::EErrorCode errorCode, const TString& message, TDuration timeout = TDuration::Seconds(5)) {
        TAutoPtr<IEventHandle> handle;
        auto [error, read] = runtime.GrabEdgeEvents<TEvPersQueue::TEvMLPErrorResponse, TEvPersQueue::TEvMLPReadResponse>(handle,timeout);

        if (error) {
            UNIT_ASSERT_VALUES_EQUAL_C(::NPersQueue::NErrorCode::EErrorCode_Name(error->GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(errorCode), error->GetErrorMessage());
            UNIT_ASSERT_VALUES_EQUAL(error->GetErrorMessage(), message);
        } else if (read) {
            UNIT_FAIL("Unexpected read result");
        } else {
            UNIT_FAIL("Timeout");
        }
    }

    NKikimrPQ::TEvMLPReadResponse GetReadResonse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5)) {
        TAutoPtr<IEventHandle> handle;
        auto [error, read] = runtime.GrabEdgeEvents<TEvPersQueue::TEvMLPErrorResponse, TEvPersQueue::TEvMLPReadResponse>(handle,timeout);

        if (error) {
            UNIT_FAIL("Unexpected error: " << ::NPersQueue::NErrorCode::EErrorCode_Name(error->GetErrorCode())
                << " " << error->GetErrorMessage());
        } else if (read) {
            return read->Record;
        } else {
            UNIT_FAIL("Timeout");
        }

        return {};
    }

    Y_UNIT_TEST(TopicNotExistsConsumer) {
        auto setup = CreateSetup();
        
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
        auto setup = CreateSetup();
        
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
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        auto& runtime = setup->GetRuntime();
        CreateActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer"
        });

        auto response = GetReadResonse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response.GetMessage().size(), 0);
    }
}

} // namespace NKikimr::NPQ::NMLP
