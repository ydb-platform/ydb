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

}

} // namespace NKikimr::NPQ::NMLP
