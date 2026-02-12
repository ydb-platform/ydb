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

        auto response = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::NOT_FOUND);
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
        UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::SUCCESS);
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

    Y_UNIT_TEST(WriteTwoMessage_Deduplicated) {
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
                    .MessageDeduplicationId = "deduplication-id"
                },
                {
                    .Index = 7,
                    .MessageBody = "message_body_2",
                    .MessageGroupId = "message_group_id_1",
                    .MessageDeduplicationId = "deduplication-id"
                },
                {
                    .Index = 11,
                    .MessageBody = "message_body_2",
                    .MessageGroupId = "message_group_id_1",
                    .MessageDeduplicationId = "other-deduplication-id"
                }
            }
        });

        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 3);
            {
                auto& msg = response->Messages[0];
                UNIT_ASSERT_VALUES_EQUAL(msg.Index, 3);
                UNIT_ASSERT_VALUES_EQUAL(msg.Status, Ydb::StatusIds::SUCCESS);
                UNIT_ASSERT(msg.MessageId.has_value());
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->PartitionId, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->Offset, 0);
            }
            {
                auto& msg = response->Messages[1];
                UNIT_ASSERT_VALUES_EQUAL(msg.Index, 7);
                UNIT_ASSERT_VALUES_EQUAL(msg.Status, Ydb::StatusIds::ALREADY_EXISTS);
                UNIT_ASSERT(msg.MessageId.has_value());
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->PartitionId, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->Offset, 0);
            }
            {
                auto& msg = response->Messages[2];
                UNIT_ASSERT_VALUES_EQUAL(msg.Index, 11);
                UNIT_ASSERT_VALUES_EQUAL(msg.Status, Ydb::StatusIds::SUCCESS);
                UNIT_ASSERT(msg.MessageId.has_value());
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->PartitionId, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->Offset, 1);
            }
        }
    }

    Y_UNIT_TEST(Deduplicated_Reboot) {
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
                    .MessageDeduplicationId = "deduplication-id"
                }
            }
        });

        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            {
                auto& msg = response->Messages[0];
                UNIT_ASSERT_VALUES_EQUAL(msg.Index, 3);
                UNIT_ASSERT_VALUES_EQUAL(msg.Status, Ydb::StatusIds::SUCCESS);
                UNIT_ASSERT(msg.MessageId.has_value());
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->PartitionId, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->Offset, 0);
            }
        }

        ReloadPQTablet(setup, "/Root", "/Root/topic1", 0);

        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 7,
                    .MessageBody = "message_body_2",
                    .MessageGroupId = "message_group_id_1",
                    .MessageDeduplicationId = "deduplication-id"
                }
            }
        });

        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            {
                auto& msg = response->Messages[0];
                UNIT_ASSERT_VALUES_EQUAL(msg.Index, 7);
                UNIT_ASSERT_VALUES_EQUAL(msg.Status, Ydb::StatusIds::ALREADY_EXISTS);
                UNIT_ASSERT(msg.MessageId.has_value());
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->PartitionId, 0);
                UNIT_ASSERT_VALUES_EQUAL(msg.MessageId->Offset, 0);
            }
        }
    }

    Y_UNIT_TEST(WriteToAutopartitioningTopic) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1, false, true);

        auto& runtime = setup->GetRuntime();

        auto end = TInstant::Now() + TDuration::Seconds(5);
        while (TInstant::Now() < end) {
            CreateWriterActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Messages = {
                    {
                        .Index = 3,
                        .MessageBody = TString(100_KB, 'a'),
                        .MessageGroupId = TStringBuilder() << "message_group_id-" << RandomNumber<ui64>(100000)
                    }
                }
            });
        }

        {
            auto client = setup->MakeClient();
            auto describe = client.DescribeTopic(GetTopicPath("/Root/topic1")).GetValueSync();
            UNIT_ASSERT_GE_C(describe.GetTopicDescription().GetPartitions().size(), 3, "Split must be done");
        }
    }


}

} // namespace NKikimr::NPQ::NMLP
