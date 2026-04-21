#include <ydb/core/persqueue/public/mlp/ut/common/common.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPDescriberTests) {

Y_UNIT_TEST(TopicNotExists) {
    auto setup = CreateSetup();

    auto& runtime = setup->GetRuntime();
    CreateDescriberActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic_not_exists",
        .Consumer = "consumer_not_exists"
    });

    auto result = GetDescribeResponse(runtime);

    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SCHEME_ERROR);
}


Y_UNIT_TEST(ConsumerNotExists) {
    auto setup = CreateSetup();

    ExecuteDDL(*setup, "CREATE TOPIC topic1");

    auto& runtime = setup->GetRuntime();
    CreateDescriberActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer_not_exists"
    });

    auto result = GetDescribeResponse(runtime);

    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SCHEME_ERROR);
}

Y_UNIT_TEST(DescribeTest) {
    auto setup = CreateSetup();

    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    CreateWriterActor(setup->GetRuntime(), {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Messages = {
            {
                .Index = 0,
                .MessageBody = "msg-1",
            },
            {
                .Index = 1,
                .MessageBody = "msg-2",
            },
            {
                .Index = 2,
                .MessageBody = "msg-3",
                .Delay = TDuration::Seconds(10)
            },
        }
    });

    CreateReaderActor(setup->GetRuntime(), {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .WaitTime = TDuration::Seconds(1),
        .ProcessingTimeout = TDuration::Seconds(30),
        .MaxNumberOfMessage = 1
    });

    for (size_t i = 0; i <= 10; ++i) {
        CreateDescriberActor(setup->GetRuntime(), {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer"
        });
    
        auto result = GetDescribeResponse(setup->GetRuntime());

        if (i < 10 &&
                (result->ApproximateMessageCount != 3
                || result->ApproximateDelayedMessageCount != 1
                || result->ApproximateLockedMessageCount != 1)) {
            Sleep(TDuration::Seconds(1));
            continue;
        }

        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);

        UNIT_ASSERT_VALUES_EQUAL(result->ApproximateMessageCount, 3);
        UNIT_ASSERT_VALUES_EQUAL(result->ApproximateDelayedMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->ApproximateLockedMessageCount, 1);
    }

    CreateCommitterActor(setup->GetRuntime(), {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0) }
    });

    for (size_t i = 0; i <= 10; ++i) {
        CreateDescriberActor(setup->GetRuntime(), {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer"
        });

        auto result = GetDescribeResponse(setup->GetRuntime());

        if (i < 10 &&
                (result->ApproximateMessageCount != 2
                || result->ApproximateDelayedMessageCount != 1
                || result->ApproximateLockedMessageCount != 0)) {
            Sleep(TDuration::Seconds(1));
            continue;
        }

        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);

        UNIT_ASSERT_VALUES_EQUAL(result->ApproximateMessageCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(result->ApproximateDelayedMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->ApproximateLockedMessageCount, 0);
    }
}

}

} // namespace NKikimr::NPQ::NMLP
