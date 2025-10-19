#include <ydb/core/persqueue/public/mlp/ut/common.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPChangerTests) {

Y_UNIT_TEST(TopicNotExists) {
    auto setup = CreateSetup();
    
    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic_not_exists",
        .Consumer = "consumer_not_exists",
        .Messages = { TMessageId(0, 0) }
    });

    auto result = GetChangeResponse(runtime);

    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SCHEME_ERROR);
}


Y_UNIT_TEST(ConsumerNotExists) {
    auto setup = CreateSetup();

    ExecuteDDL(*setup, "CREATE TOPIC topic1");
    
    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer_not_exists",
        .Messages = { TMessageId(0, 0) }
    });

    auto result = GetChangeResponse(runtime);

    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SCHEME_ERROR);
}

Y_UNIT_TEST(PartitionNotExists) {
    auto setup = CreateSetup();

    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    
    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(13, 17) }
    });

    auto result = GetChangeResponse(runtime);

    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 13);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 17);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Success, false);
}

Y_UNIT_TEST(CommitTest) {
    auto setup = CreateSetup();

    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    
    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 17) }
    });

    auto result = GetChangeResponse(runtime);

    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 17);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Success, true);
}

Y_UNIT_TEST(ReadAndReleaseTest) {
    auto setup = CreateSetup();

    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);
    setup->Write("/Root/topic1", "msg-2", 0);
    setup->Write("/Root/topic1", "msg-3", 0);

    Sleep(TDuration::Seconds(2));

    auto& runtime = setup->GetRuntime();

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .VisibilityTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 2
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].MessageId.Offset, 1);
    }

    {
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 1) }
        });

        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Success, true);
    }

    {
        CreateMessageDeadlineChangerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) },
            .Deadline = TInstant::Now() - TDuration::Seconds(1)
        });

        auto result = GetChangeResponse(runtime);

        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Success, true);
    }

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

        // You should receive two messages. With offset 0 because his VisibilityDeadline was changed,
        // which expired, and with offset 2, which has not yet been processed.
        // The message from offset 1 has been deleted.
        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].MessageId.Offset, 2);
    }


}

}

} // namespace NKikimr::NPQ::NMLP
