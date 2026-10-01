#include <ydb/core/persqueue/public/mlp/ut/common/common.h>

namespace NKikimr::NPQ::NMLP {

namespace {

void AssertCommittedOffset(
    std::shared_ptr<TTopicSdkTestSetup>& setup,
    const std::string& topic,
    const std::string& consumer,
    ui32 partitionId,
    ui64 expected,
    TDuration timeout = TDuration::Seconds(10))
{
    ui64 actual = 0;
    const auto deadline = TInstant::Now() + timeout;

    do {
        auto describe = setup->DescribeConsumer(topic, consumer);
        actual = describe.GetPartitions()[partitionId].GetPartitionConsumerStats()->GetCommittedOffset();
        if (actual == expected) {
            return;
        }
        Sleep(TDuration::MilliSeconds(100));
    } while (TInstant::Now() < deadline);

    UNIT_ASSERT_VALUES_EQUAL_C(
        actual,
        expected,
        TStringBuilder() << "Committed offset did not reach expected value"
            << ", topic=" << topic
            << ", consumer=" << consumer
            << ", partitionId=" << partitionId);
}

} // namespace

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
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Failed);
}

Y_UNIT_TEST(CommitTest) {
    auto setup = CreateSetup();

    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);
    setup->Write("/Root/topic1", "msg-2", 0);

    Sleep(TDuration::Seconds(2));

    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0) }
    });

    auto result = GetChangeResponse(runtime);

    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Success);

    AssertCommittedOffset(setup, "/Root/topic1", "mlp-consumer", 0, 1);
}

Y_UNIT_TEST(DoubleCommitTest) {
    auto setup = CreateSetup();

    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);

    Sleep(TDuration::Seconds(2));

    auto& runtime = setup->GetRuntime();
    for (size_t attempt = 0; attempt < 2; ++attempt) {
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) }
        });

        auto result = GetChangeResponse(runtime);

        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT(result->Messages[0].Status == (attempt == 0 ? EOperationResult::Success : EOperationResult::NotFound));
    }

    AssertCommittedOffset(setup, "/Root/topic1", "mlp-consumer", 0, 1);
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
            .ProcessingTimeout = TDuration::Seconds(30),
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
        UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Success);
    }

    {
        CreateMessageDeadlineChangerActor(runtime, TMessageDeadlineChangerSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) },
            .Deadlines = {TInstant::Now() - TDuration::Seconds(1), },
        });

        auto result = GetChangeResponse(runtime);

        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Success);
    }

    Sleep(TDuration::Seconds(2));

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(5),
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

Y_UNIT_TEST(EmptyCommit) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = {}
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 0);
}

Y_UNIT_TEST(CommitUnknownOffset) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);

    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 99) }
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::NotFound);
}

Y_UNIT_TEST(CommitMultiPartition) {
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
        UNIT_ASSERT(response->Messages[0].MessageId.has_value());
        UNIT_ASSERT(response->Messages[1].MessageId.has_value());
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId->PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[1].MessageId->PartitionId, 1);

        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = {
                *response->Messages[0].MessageId,
                *response->Messages[1].MessageId,
            }
        });
    }

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 2);
    for (const auto& msg : result->Messages) {
        UNIT_ASSERT(msg.Status == EOperationResult::Success);
    }

    AssertCommittedOffset(setup, "/Root/topic1", "mlp-consumer", 0, 1);
    AssertCommittedOffset(setup, "/Root/topic1", "mlp-consumer", 1, 1);
}

Y_UNIT_TEST(UnlockAfterRead) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);

    auto& runtime = setup->GetRuntime();
    TMessageId messageId;

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].ApproximateReceiveCount, 1);
        messageId = response->Messages[0].MessageId;
    }

    {
        CreateUnlockerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { messageId }
        });
        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Success);
    }

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, messageId.PartitionId);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, messageId.Offset);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].ApproximateReceiveCount, 2);
    }
}

Y_UNIT_TEST(UnlockNotInFlight) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);

    Sleep(TDuration::Seconds(1));

    auto& runtime = setup->GetRuntime();
    CreateUnlockerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0) }
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::NotInFlight);
}

Y_UNIT_TEST(UnlockNotFound) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);

    Sleep(TDuration::Seconds(1));

    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0) }
    });
    {
        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Success);
    }

    CreateUnlockerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0) }
    });
    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::NotFound);
}

Y_UNIT_TEST(UnlockUnknownOffset) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    auto& runtime = setup->GetRuntime();
    CreateUnlockerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 17) }
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::NotFound);
}

Y_UNIT_TEST(EmptyUnlock) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    auto& runtime = setup->GetRuntime();
    CreateUnlockerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = {}
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 0);
}

Y_UNIT_TEST(DeadlineChangerExtendAndExpire) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);

    auto& runtime = setup->GetRuntime();
    TMessageId messageId;

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
        messageId = response->Messages[0].MessageId;
    }

    {
        CreateMessageDeadlineChangerActor(runtime, TMessageDeadlineChangerSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { messageId },
            .Deadlines = { TInstant::Now() + TDuration::Minutes(10) },
        });
        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Success);
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

    {
        CreateMessageDeadlineChangerActor(runtime, TMessageDeadlineChangerSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { messageId },
            .Deadlines = { TInstant::Now() - TDuration::Seconds(1) },
        });
        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Success);
    }

    Sleep(TDuration::Seconds(1));

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
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, messageId.Offset);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
    }
}

Y_UNIT_TEST(DeadlineChangerNotInFlight) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);

    Sleep(TDuration::Seconds(1));

    auto& runtime = setup->GetRuntime();
    CreateMessageDeadlineChangerActor(runtime, TMessageDeadlineChangerSettings{
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0) },
        .Deadlines = { TInstant::Now() + TDuration::Seconds(30) },
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::NotInFlight);
}

Y_UNIT_TEST(DeadlineChangerNotFound) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    auto& runtime = setup->GetRuntime();
    CreateMessageDeadlineChangerActor(runtime, TMessageDeadlineChangerSettings{
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 42) },
        .Deadlines = { TInstant::Now() + TDuration::Seconds(30) },
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::NotFound);
}

Y_UNIT_TEST(EmptyDeadlineChange) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    auto& runtime = setup->GetRuntime();
    CreateMessageDeadlineChangerActor(runtime, TMessageDeadlineChangerSettings{
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = {},
        .Deadlines = {},
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 0);
}

Y_UNIT_TEST(MixedCommitResults) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);

    Sleep(TDuration::Seconds(1));

    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0), TMessageId(0, 99) }
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 2);

    THashMap<ui64, EOperationResult> byOffset;
    for (const auto& msg : result->Messages) {
        byOffset[msg.MessageId.Offset] = msg.Status;
    }
    UNIT_ASSERT(byOffset[0] == EOperationResult::Success);
    UNIT_ASSERT(byOffset[99] == EOperationResult::NotFound);
}

Y_UNIT_TEST(CommitAfterPQReboot) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);

    auto& runtime = setup->GetRuntime();
    TMessageId messageId;

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        messageId = response->Messages[0].MessageId;
    }

    ReloadPQTablet(setup, "/Root", "/Root/topic1", 0);

    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { messageId }
    });
    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Success);

    AssertCommittedOffset(setup, "/Root/topic1", "mlp-consumer", 0, 1);
}

Y_UNIT_TEST(UnauthorizedCommitter) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    NACLib::TDiffACL acl;
    acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@staff");
    ModifyTopicAcl(*setup, "topic1", acl);

    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0) },
        .UserToken = MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{}),
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT(!result->ErrorDescription.empty());
}

Y_UNIT_TEST(UnauthorizedUnlocker) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    NACLib::TDiffACL acl;
    acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@staff");
    ModifyTopicAcl(*setup, "topic1", acl);

    auto& runtime = setup->GetRuntime();
    CreateUnlockerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0) },
        .UserToken = MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{}),
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT(!result->ErrorDescription.empty());
}

Y_UNIT_TEST(UnauthorizedDeadlineChanger) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    NACLib::TDiffACL acl;
    acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@staff");
    ModifyTopicAcl(*setup, "topic1", acl);

    auto& runtime = setup->GetRuntime();
    CreateMessageDeadlineChangerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0) },
        .Deadlines = { TInstant::Now() + TDuration::Seconds(30) },
        .UserToken = MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{}),
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT(!result->ErrorDescription.empty());
}

Y_UNIT_TEST(DeadlineChangerMessagesDeadlinesSizeMismatch) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);

    auto& runtime = setup->GetRuntime();
    CreateMessageDeadlineChangerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0), TMessageId(0, 1) },
        .Deadlines = { TInstant::Now() + TDuration::Seconds(30) },
    });

    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT(result->ErrorDescription.Contains("size mismatch"));
}

Y_UNIT_TEST(DeadlineChangerMultiPartition) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer", 2);

    auto& runtime = setup->GetRuntime();
    std::vector<TMessageId> messageIds;
    {
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {.Index = 0, .MessageBody = "p0", .MessageGroupId = "g0"},
                {.Index = 1, .MessageBody = "p1", .MessageGroupId = "g1"},
            }
        });
        auto write = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(write->Messages.size(), 2);
        messageIds = {*write->Messages[0].MessageId, *write->Messages[1].MessageId};
        UNIT_ASSERT_VALUES_UNEQUAL(messageIds[0].PartitionId, messageIds[1].PartitionId);
    }

    std::vector<TMessageId> lockedIds;
    for (size_t i = 0; i < 2; ++i) {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(2),
            .ProcessingTimeout = TDuration::Seconds(5),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        lockedIds.push_back(response->Messages[0].MessageId);
    }
    UNIT_ASSERT_VALUES_UNEQUAL(lockedIds[0].PartitionId, lockedIds[1].PartitionId);

    const auto far = TInstant::Now() + TDuration::Minutes(10);
    CreateMessageDeadlineChangerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = lockedIds,
        .Deadlines = { far, far },
    });
    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 2);
    for (const auto& msg : result->Messages) {
        UNIT_ASSERT(msg.Status == EOperationResult::Success);
    }

    // Extended visibility — immediate re-read must be empty.
    CreateReaderActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .WaitTime = TDuration::Seconds(0),
        .ProcessingTimeout = TDuration::Seconds(5),
        .MaxNumberOfMessage = 2,
    });
    {
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
    }
}

Y_UNIT_TEST(DeadlineChangerMultiPartitionDifferentDeadlines) {
    // Different deadlines on different partitions must be applied to the matching offsets
    // (not the full Deadlines vector to every partition request).
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer", 2);
    auto& runtime = setup->GetRuntime();

    {
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {.Index = 0, .MessageBody = "short-deadline", .MessageGroupId = "g0"},
                {.Index = 1, .MessageBody = "long-deadline", .MessageGroupId = "g1"},
            }
        });
        auto write = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(write->Messages.size(), 2);
        UNIT_ASSERT_VALUES_UNEQUAL(write->Messages[0].MessageId->PartitionId,
            write->Messages[1].MessageId->PartitionId);
    }

    std::vector<TMessageId> lockedIds;
    THashMap<TString, TString> dataById;
    auto messageKey = [](const TMessageId& id) -> TString {
        return TStringBuilder() << id.PartitionId << ":" << id.Offset;
    };
    for (size_t i = 0; i < 2; ++i) {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(2),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        lockedIds.push_back(response->Messages[0].MessageId);
        dataById[messageKey(response->Messages[0].MessageId)] = response->Messages[0].Data;
    }
    UNIT_ASSERT_VALUES_UNEQUAL(lockedIds[0].PartitionId, lockedIds[1].PartitionId);

    // Put short deadline on the first locked message, long on the second — regardless of partition order.
    // Use a few seconds of slack: deadlines are stored with second precision.
    const auto shortDeadline = TInstant::Now() + TDuration::Seconds(3);
    const auto longDeadline = TInstant::Now() + TDuration::Minutes(10);
    CreateMessageDeadlineChangerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = lockedIds,
        .Deadlines = { shortDeadline, longDeadline },
    });
    {
        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 2);
        for (const auto& msg : result->Messages) {
            UNIT_ASSERT(msg.Status == EOperationResult::Success);
        }
    }

    // Deadlines are stored with second precision.
    const auto expectedShort = TInstant::Seconds(shortDeadline.Seconds());
    const auto expectedLong = TInstant::Seconds(longDeadline.Seconds());

    auto assertDeadline = [&](const TMessageId& id, TInstant expected) {
        auto state = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer", id.PartitionId);
        UNIT_ASSERT_VALUES_EQUAL(state->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(state->Messages[0].Offset, id.Offset);
        UNIT_ASSERT_VALUES_EQUAL_C(state->Messages[0].ProcessingDeadline, expected,
            TStringBuilder() << "partition=" << id.PartitionId << " offset=" << id.Offset);
    };
    assertDeadline(lockedIds[0], expectedShort);
    assertDeadline(lockedIds[1], expectedLong);

    // After short deadline expires only that message becomes readable again.
    Sleep(TDuration::Seconds(5));
    TMessageId expiredId;
    bool gotExpired = false;
    for (size_t i = 0; i < 5 && !gotExpired; ++i) {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(2),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 2,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        if (response->Messages.size() == 1) {
            expiredId = response->Messages[0].MessageId;
            UNIT_ASSERT_VALUES_EQUAL(expiredId.PartitionId, lockedIds[0].PartitionId);
            UNIT_ASSERT_VALUES_EQUAL(expiredId.Offset, lockedIds[0].Offset);
            UNIT_ASSERT_VALUES_EQUAL(dataById[messageKey(lockedIds[0])], response->Messages[0].Data);
            gotExpired = true;
        }
    }
    UNIT_ASSERT_C(gotExpired, "short-deadline message did not become readable");

    // Long-deadline message still has its deadline and is not returned by the read above.
    auto longState = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer",
        lockedIds[1].PartitionId);
    UNIT_ASSERT_VALUES_EQUAL(longState->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(longState->Messages[0].Offset, lockedIds[1].Offset);
    UNIT_ASSERT_VALUES_EQUAL(longState->Messages[0].ProcessingDeadline, expectedLong);
}

Y_UNIT_TEST(UnlockMultiPartition) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer", 2);

    auto& runtime = setup->GetRuntime();
    {
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {.Index = 0, .MessageBody = "u0", .MessageGroupId = "g0"},
                {.Index = 1, .MessageBody = "u1", .MessageGroupId = "g1"},
            }
        });
        auto write = GetWriteResponse(runtime);
        UNIT_ASSERT(write);
        UNIT_ASSERT_VALUES_EQUAL(write->Messages.size(), 2);
    }

    std::vector<TMessageId> lockedIds;
    for (size_t i = 0; i < 2; ++i) {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(2),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        lockedIds.push_back(response->Messages[0].MessageId);
    }

    CreateUnlockerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = lockedIds,
    });
    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 2);
    for (const auto& msg : result->Messages) {
        UNIT_ASSERT(msg.Status == EOperationResult::Success);
    }
}

Y_UNIT_TEST(MixedUnlockResults) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);

    auto& runtime = setup->GetRuntime();
    TMessageId lockedId;
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        lockedId = response->Messages[0].MessageId;
    }

    CreateUnlockerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { lockedId, TMessageId(0, 99) },
    });
    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 2);

    THashMap<ui64, EOperationResult> byOffset;
    for (const auto& msg : result->Messages) {
        byOffset[msg.MessageId.Offset] = msg.Status;
    }
    UNIT_ASSERT(byOffset[lockedId.Offset] == EOperationResult::Success);
    UNIT_ASSERT(byOffset[99] == EOperationResult::NotFound);
}

Y_UNIT_TEST(MixedDeadlineResults) {
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);

    auto& runtime = setup->GetRuntime();
    TMessageId lockedId;
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        lockedId = response->Messages[0].MessageId;
    }

    const auto deadline = TInstant::Now() + TDuration::Seconds(30);
    CreateMessageDeadlineChangerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { lockedId, TMessageId(0, 99) },
        .Deadlines = { deadline, deadline },
    });
    auto result = GetChangeResponse(runtime);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 2);

    THashMap<ui64, EOperationResult> byOffset;
    for (const auto& msg : result->Messages) {
        byOffset[msg.MessageId.Offset] = msg.Status;
    }
    UNIT_ASSERT(byOffset[lockedId.Offset] == EOperationResult::Success);
    UNIT_ASSERT(byOffset[99] == EOperationResult::NotFound);
}

Y_UNIT_TEST(CapacitySmokeKeepOrder) {
    // Bounded replacement for the disabled CapacityTest load loop.
    auto setup = CreateSetup();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1, true);
    auto& runtime = setup->GetRuntime();

    CreateWriterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Messages = {
            {.Index = 0, .MessageBody = "a", .MessageGroupId = "g", .MessageDeduplicationId = "d1"},
            {.Index = 1, .MessageBody = "b", .MessageGroupId = "g", .MessageDeduplicationId = "d2"},
        }
    });
    {
        auto write = GetWriteResponse(runtime);
        UNIT_ASSERT(write);
        UNIT_ASSERT_VALUES_EQUAL(write->Messages.size(), 2);
    }

    TMessageId firstId;
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "a");
        firstId = response->Messages[0].MessageId;
    }

    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { firstId },
    });
    {
        auto commit = GetChangeResponse(runtime);
        UNIT_ASSERT(commit);
        UNIT_ASSERT(commit->Messages[0].Status == EOperationResult::Success);
    }

    CreateReaderActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .WaitTime = TDuration::Seconds(1),
        .ProcessingTimeout = TDuration::Seconds(30),
        .MaxNumberOfMessage = 1,
    });
    auto response = GetReadResponse(runtime);
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "b");
}

Y_UNIT_TEST(CapacityTest) {
    // Heavy load benchmark — intentionally disabled in CI. See CapacitySmokeKeepOrder.
    return;

    auto setup = CreateSetup();

    CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1, true);

    Cerr << (TStringBuilder() << ">>>>> TOPIC WAS CREATED" << Endl);

    struct State {
        size_t ReadSuccess = 0;
        size_t ReadFailed = 0;
        size_t CommitSuccess = 0;
        size_t CommitFailed = 0;
        size_t WriteSuccess = 0;
        size_t WriteAlreadyExists = 0;
        size_t WriteFailed = 0;
    };

    State state;

    struct TestActor : public TActorBootstrapped<TestActor> {

        TestActor(State& state)
            : State(state) {}

        void Bootstrap() {
            Become(&TestActor::StateWork);
            Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
            Next();
        }

        void Next() {
            while (InflightWrite < 250) {
                Register(CreateWriter(SelfId(), TWriterSettings{
                    .DatabasePath = "/Root",
                    .TopicName = "/Root/topic1",
                    .Messages = {{
                        .Index = 0,
                        .MessageBody = Body,
                        .MessageGroupId = TStringBuilder() << "message-group-" << RandomNumber<ui64>(100000),
                        .MessageDeduplicationId = TStringBuilder() << "deduplication-id-" << RandomNumber<ui64>(5000000)
                    }}
                }));

                ++InflightWrite;
            }

            while (Inflight < 300) {
                Register(CreateReader(SelfId(), TReaderSettings{
                    .DatabasePath = "/Root",
                    .TopicName = "/Root/topic1",
                    .Consumer = "mlp-consumer",
                    .WaitTime = TDuration::Seconds(1),
                    .ProcessingTimeout = TDuration::Seconds(5),
                    .MaxNumberOfMessage = 1
                }));

                ++Inflight;
            }
        }

        void Handle(NMLP::TEvWriteResponse::TPtr& ev) {
            --InflightWrite;

            auto& messages = ev->Get()->Messages;
            if (messages.size() != 1 || messages[0].Status == Ydb::StatusIds::INTERNAL_ERROR) {
                State.WriteFailed++;
            } else if (messages[0].Status == Ydb::StatusIds::ALREADY_EXISTS) {
                State.WriteAlreadyExists++;
            } else {
                State.WriteSuccess++;
            }

            Next();
        }

        void Handle(NMLP::TEvReadResponse::TPtr& ev) {
            --Inflight;

            if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
                ++State.ReadSuccess;

                if (!ev->Get()->Messages.empty() && RandomNumber<size_t>(10) > 0) {
                    Register(CreateCommitter(SelfId(), TCommitterSettings{
                        .DatabasePath = "/Root",
                        .TopicName = "/Root/topic1",
                        .Consumer = "mlp-consumer",
                        .Messages = { ev->Get()->Messages[0].MessageId }
                    }));

                    ++Inflight;
                }
            } else {
                ++State.ReadFailed;
            }

            Next();
        }

        void Handle(NMLP::TEvChangeResponse::TPtr& ev) {
            --Inflight;

            if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
                ++State.CommitSuccess;
            } else {
                ++State.CommitFailed;
            }

            Next();
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NMLP::TEvWriteResponse, Handle);
                hFunc(NMLP::TEvReadResponse, Handle);
                hFunc(NMLP::TEvChangeResponse, Handle);
                sFunc(TEvents::TEvPoison, PassAway);
                sFunc(TEvents::TEvWakeup, PassAway);
            }
        }

        size_t Inflight = 0;
        size_t InflightWrite = 0;

        TString Body = NUnitTest::RandomString(10_KB);

        State& State;
    };

    auto& runtime = setup->GetRuntime();
    runtime.Register(new TestActor(state));


    Sleep(TDuration::Seconds(65));

    Cerr << "Total:\n  Read success: " << state.ReadSuccess
        << "\n  Read fail: " << state.ReadFailed
        << "\n  Commit success: " << state.CommitSuccess
        << "\n  Commit fail: " << state.CommitFailed
        << "\n  Write success: " << state.WriteSuccess
        << "\n  Write fail: " << state.WriteFailed
        << "\n  Write deduplicated: " << state.WriteAlreadyExists
        << "\n  RPS: " << (state.ReadSuccess + state.CommitSuccess + state.WriteSuccess + state.WriteAlreadyExists) / 60
        << Endl;
}

}

} // namespace NKikimr::NPQ::NMLP
