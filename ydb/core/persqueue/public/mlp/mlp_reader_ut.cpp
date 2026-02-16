#include <ydb/core/persqueue/public/mlp/ut/common/common.h>

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
            .WaitTime = TDuration::Seconds(3),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
            .UncompressMessages = true
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
                .ProcessingTimeout = TDuration::Seconds(2),
                .MaxNumberOfMessage = 2,
                .UncompressMessages = true
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
                .WaitTime = TDuration::Seconds(0),
                .ProcessingTimeout = TDuration::Seconds(5),
                .MaxNumberOfMessage = 10,
                .UncompressMessages = true
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
                .ProcessingTimeout = TDuration::Seconds(2),
                .MaxNumberOfMessage = 2,
                .UncompressMessages = true
            });

            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
        }

        Sleep(TDuration::Seconds(2));

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(5),
                .ProcessingTimeout = TDuration::Seconds(2),
                .MaxNumberOfMessage = 2,
                .UncompressMessages = true
            });

            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[1].Data, "msg-2");
        }

    }

    Y_UNIT_TEST(TopicWithBigMessage) {
        auto setup = CreateSetup();

        auto bigMessage = NUnitTest::RandomString(1_MB);

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");
        setup->Write("/Root/topic1", bigMessage, 0);

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(3),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
            .UncompressMessages = true
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, bigMessage);
    }


    Y_UNIT_TEST(TopicWithKeepMessageOrder) {
        auto setup = CreateSetup();
        auto& runtime = setup->GetRuntime();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1, true);

        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 0,
                    .MessageBody = "message_body_1",
                    .MessageGroupId = "message_group_id_1",
                    .MessageDeduplicationId = "deduplication-id-1"
                },
                {
                    .Index = 1,
                    .MessageBody = "message_body_2",
                    .MessageGroupId = "message_group_id_1",
                    .MessageDeduplicationId = "deduplication-id-2"
                },
                {
                    .Index = 2,
                    .MessageBody = "message_body_3",
                    .MessageGroupId = "message_group_id_2",
                    .MessageDeduplicationId = "deduplication-id-3"
                }
            }
        });

        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 3);
        }

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(3),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
            .UncompressMessages = true
        });

        {
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "message_body_1");
        }

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(3),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
            .UncompressMessages = true
        });

        {
            // message with offset 1 has been skipped because his message group equals message groups of the first message
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 2);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "message_body_3");
        }
    }

}

} // namespace NKikimr::NPQ::NMLP
