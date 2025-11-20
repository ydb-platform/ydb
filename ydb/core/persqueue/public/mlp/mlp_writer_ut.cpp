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
                    .Index = 0,
                    .MessageBody = "message_body",
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
                    .Index = 3,
                    .MessageBody = "message_body",
                    .MessageGroupId = "message_group_id",
                    .MessageDeduplicationId = "message_deduplication_id",
                    .SerializedMessageAttributes = "message_attributes",
                }
            }
        });

        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL_C(Ydb::StatusIds::StatusCode_Name(response->Status),
                Ydb::StatusIds::StatusCode_Name(Ydb::StatusIds::SUCCESS), response->ErrorDescription);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            auto& msg = response->Messages[0];
            UNIT_ASSERT_VALUES_EQUAL(msg.Index, 3);
            UNIT_ASSERT(msg.MessageId.has_value());
            UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->PartitionId, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->Offset, 0);
        }

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .VisibilityTimeout = TDuration::Seconds(5),
            .MaxNumberOfMessage = 10,
            .UncompressMessages = true
        });

        {
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "message_body");
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageGroupId, "message_group_id");
        }
    }

    Y_UNIT_TEST(WriteTwoMessage_OnePartition) {
        auto setup = CreateSetup();
        
        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1);

        auto& runtime = setup->GetRuntime();
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 3,
                    .MessageBody = "message_body_1",
                    .MessageGroupId = "message_group_id_1",
                },
                {
                    .Index = 7,
                    .MessageBody = "message_body_2",
                    .MessageGroupId = "message_group_id_2",
                }
            }
        });

        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL_C(Ydb::StatusIds::StatusCode_Name(response->Status),
                Ydb::StatusIds::StatusCode_Name(Ydb::StatusIds::SUCCESS), response->ErrorDescription);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 2);
            {
                auto& msg = response->Messages[0];
                UNIT_ASSERT_VALUES_EQUAL(msg.Index, 3);
                UNIT_ASSERT(msg.MessageId.has_value());
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->PartitionId, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->Offset, 0);
            }
            {
                auto& msg = response->Messages[1];
                UNIT_ASSERT_VALUES_EQUAL(msg.Index, 7);
                UNIT_ASSERT(msg.MessageId.has_value());
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->PartitionId, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->Offset, 1);
            }
        }
    }

    Y_UNIT_TEST(WriteTwoMessage_TwoPartition) {
        auto setup = CreateSetup();
        
        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 2);

        auto& runtime = setup->GetRuntime();
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 0,
                    .MessageBody = "message_body_1",
                    .MessageGroupId = "message_group_id_1",
                },
                {
                    .Index = 1,
                    .MessageBody = "message_body_2",
                    .MessageGroupId = "message_group_id_2",
                }
            }
        });

        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL_C(Ydb::StatusIds::StatusCode_Name(response->Status),
                Ydb::StatusIds::StatusCode_Name(Ydb::StatusIds::SUCCESS), response->ErrorDescription);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 2);
            {
                auto& msg = response->Messages[0];
                UNIT_ASSERT_VALUES_EQUAL(msg.Index, 0);
                UNIT_ASSERT(msg.MessageId.has_value());
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->PartitionId, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->Offset, 0);
            }
            {
                auto& msg = response->Messages[1];
                UNIT_ASSERT_VALUES_EQUAL(msg.Index, 1);
                UNIT_ASSERT(msg.MessageId.has_value());
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->PartitionId, 1);
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->Offset, 0);
            }
        }
    }
}

} // namespace NKikimr::NPQ::NMLP
