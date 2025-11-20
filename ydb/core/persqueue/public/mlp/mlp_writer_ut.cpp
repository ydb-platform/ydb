#include <ydb/core/persqueue/public/mlp/ut/common/common.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPWriterTests) {

    Y_UNIT_TEST(TopicNotExists) {
        auto setup = CreateSetup();
        
        auto& runtime = setup->GetRuntime();
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic_not_exists",
            .Messages = {
                {
                    .MessageBody = "message_body",
                    .MessageId = "message_id",
                    .BatchId = "batch_id"
                }
            }
        });

        AssertWriteError(runtime, Ydb::StatusIds::SCHEME_ERROR,
            "You do not have access or the '/Root/topic_not_exists' does not exist");
    }

    Y_UNIT_TEST(EmptyWrite) {
        auto setup = CreateSetup();
        
        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        auto& runtime = setup->GetRuntime();
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {}
        });

        auto response = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(Ydb::StatusIds::StatusCode_Name(response->Status),
            Ydb::StatusIds::StatusCode_Name(Ydb::StatusIds::SUCCESS), response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
    }

    Y_UNIT_TEST(WriteOneMessage) {
        auto setup = CreateSetup();
        
        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        auto& runtime = setup->GetRuntime();
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .MessageBody = "message_body",
                    .MessageId = "message_id",
                    .BatchId = "batch_id"
                }
            }
        });

        auto response = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(Ydb::StatusIds::StatusCode_Name(response->Status),
            Ydb::StatusIds::StatusCode_Name(Ydb::StatusIds::SUCCESS), response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        auto& msg = response->Messages[0];
        UNIT_ASSERT_VALUES_EQUAL(msg.MessageId, "message_id");
        UNIT_ASSERT_VALUES_EQUAL(msg.BatchId, "batch_id");
        UNIT_ASSERT_VALUES_EQUAL(msg.Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(msg.Offset, 0);
    }
}

} // namespace NKikimr::NPQ::NMLP
