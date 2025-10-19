#include <ydb/core/persqueue/public/mlp/ut/common.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPReaderTests) {

    Y_UNIT_TEST(TopicNotExists) {
        auto setup = CreateSetup();
        
        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic_not_exists",
            .Consumer = "consumer_not_exists"
        });

        AssertReadError(runtime, Ydb::StatusIds::SCHEME_ERROR,
            "You do not have access or the '/Root/topic_not_exists' does not exist");
    }

    Y_UNIT_TEST(TopicWithoutConsumer) {
        auto setup = CreateSetup();
        
        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "consumer_not_exists"
        });

        AssertReadError(runtime, Ydb::StatusIds::SCHEME_ERROR,
            "Consumer 'consumer_not_exists' does not exist");
    }

    Y_UNIT_TEST(EmptyTopic) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer"
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
    }

    Y_UNIT_TEST(TopicWithData) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");
        setup->Write("/Root/topic1", "msg-1", 0);

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(3)
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
    }

    Y_UNIT_TEST(TopicWithManyIterationsData) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");
        setup->Write("/Root/topic1", "msg-1", 0);
        setup->Write("/Root/topic1", "msg-2", 0);
        setup->Write("/Root/topic1", "msg-3", 0);

        auto& runtime = setup->GetRuntime();

        Sleep(TDuration::Seconds(2));

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(1),
                .VisibilityTimeout = TDuration::Seconds(5),
                .MaxNumberOfMessage = 2
            });

            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[1].Data, "msg-2");
        }

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(1),
                .VisibilityTimeout = TDuration::Seconds(5),
                .MaxNumberOfMessage = 2
            });

            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-3");
        }

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(0),
                .VisibilityTimeout = TDuration::Seconds(2),
                .MaxNumberOfMessage = 2
            });

            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
        }

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(5),
                .VisibilityTimeout = TDuration::Seconds(2),
                .MaxNumberOfMessage = 2
            });

            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[1].Data, "msg-2");
        }

    }
}

} // namespace NKikimr::NPQ::NMLP
