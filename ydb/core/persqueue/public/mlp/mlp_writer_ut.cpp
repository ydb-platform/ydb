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
            .ProcessingTimeout = TDuration::Seconds(5),
            .MaxNumberOfMessage = 10,
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

    Y_UNIT_TEST(WriteWithoutMessageGroupId) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 2);

        auto& runtime = setup->GetRuntime();
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 0,
                    .MessageBody = "message_body",
                }
            }
        });

        auto response = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(response->Messages[0].MessageId.has_value());
        UNIT_ASSERT_LT(response->Messages[0].MessageId->PartitionId, 2u);
    }

    Y_UNIT_TEST(WriteWithDelay) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        auto& runtime = setup->GetRuntime();
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 0,
                    .MessageBody = "delayed_message",
                    .Delay = TDuration::Seconds(3),
                }
            }
        });

        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Status, Ydb::StatusIds::SUCCESS);
        }

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(0),
                .ProcessingTimeout = TDuration::Seconds(5),
                .MaxNumberOfMessage = 1,
            });
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
        }

        Sleep(TDuration::Seconds(4));

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(1),
                .ProcessingTimeout = TDuration::Seconds(5),
                .MaxNumberOfMessage = 1,
            });
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "delayed_message");
        }
    }

    Y_UNIT_TEST(WriteAttributesRoundTrip) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        auto& runtime = setup->GetRuntime();
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 0,
                    .MessageBody = "message_with_attrs",
                    .MessageGroupId = "group-1",
                    .MessageDeduplicationId = "dedup-1",
                    .Attributes = {
                        {"attr-a", "value-a"},
                        {"attr-b", "value-b"},
                    },
                }
            }
        });

        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Status, Ydb::StatusIds::SUCCESS);
        }

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(5),
            .MaxNumberOfMessage = 1,
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "message_with_attrs");
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageGroupId, "group-1");
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageDeduplicationId, "dedup-1");
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Attributes.count("attr-a"), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Attributes.find("attr-a")->second, "value-a");
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Attributes.count("attr-b"), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Attributes.find("attr-b")->second, "value-b");
    }

    Y_UNIT_TEST(WriteShouldBeCharged) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        auto& runtime = setup->GetRuntime();
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 0,
                    .MessageBody = "charged_message",
                }
            },
            .ShouldBeCharged = true,
        });

        auto response = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(response->Messages[0].MessageId.has_value());
    }

    Y_UNIT_TEST(UnauthorizedWriter) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, "user1@staff");
        ModifyTopicAcl(*setup, "topic1", acl);

        auto& runtime = setup->GetRuntime();
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 0,
                    .MessageBody = "message_body",
                }
            },
            .UserToken = MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{}),
        });

        auto response = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::UNAUTHORIZED);
    }

    Y_UNIT_TEST(WriteToAutopartitioningTopic) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1, false, true);

        auto& runtime = setup->GetRuntime();

        size_t messagesCount = 0;
        size_t writeErrors = 0;

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

            auto r = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(r->Messages.size(), 1);
            if (r->Messages[0].Status == Ydb::StatusIds::SUCCESS) {
                ++messagesCount;
            } else {
                ++writeErrors;
            }

            Sleep(TDuration::MilliSeconds(10));
        }

        Cerr << (TStringBuilder() << ">>>>> written messagesCount: " << messagesCount << Endl);
        Cerr << (TStringBuilder() << ">>>>> write errors: " << writeErrors << Endl);

        {
            auto client = setup->MakeClient();
            auto describe = client.DescribeTopic(GetTopicPath("/Root/topic1")).GetValueSync();
            UNIT_ASSERT_GE_C(describe.GetTopicDescription().GetPartitions().size(), 3, "Split must be done");
            Cerr << (TStringBuilder() << ">>>>> partitions count: " << describe.GetTopicDescription().GetPartitions().size() << Endl);
        }

        size_t emptyResults = 0;

        end = TInstant::Now() + TDuration::Seconds(15);
        size_t readMessages = 0;
        while (readMessages < messagesCount && TInstant::Now() < end) {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(0),
                .ProcessingTimeout = TDuration::Seconds(5),
                .MaxNumberOfMessage = 1,
            });
            auto r = GetReadResponse(runtime);

            if (r->Messages.empty()) {
                ++emptyResults;
                continue;
            }

            ++readMessages;

            CreateCommitterActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .Messages = { r->Messages[0].MessageId }
            });
        }

        Cerr << (TStringBuilder() << ">>>>> empty results: " << emptyResults << Endl);
        UNIT_ASSERT_VALUES_EQUAL(messagesCount, readMessages);

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(5),
            .MaxNumberOfMessage = 1,
        });
        auto r = GetReadResponse(runtime);
        UNIT_ASSERT_C(r->Messages.empty(), "all messages have already been read successfully");
    }


}

} // namespace NKikimr::NPQ::NMLP
