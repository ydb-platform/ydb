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

}

} // namespace NKikimr::NPQ::NMLP
