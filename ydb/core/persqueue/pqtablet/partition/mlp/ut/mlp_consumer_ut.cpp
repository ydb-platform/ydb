#include "mlp_storage.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/mon.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPConsumerTests) {

Y_UNIT_TEST(ReloadPQTablet) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    CreateTopic(setup, "/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(1)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer());

    // Write many messages because small snapshot do not write wal
    WriteMany(setup, "/Root/topic1", 0, 16, 113);

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    Cerr << ">>>>> BEGIN COMMIT" << Endl;

    {
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) }
        });

        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    Cerr << ">>>>> BEGIN REBOOT " << Endl;
    ReloadPQTablet(setup, "/Root", "/Root/topic1", 0);

    for (size_t i = 0; i < 10; ++i) {
        Sleep(TDuration::Seconds(1));

        auto result = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        if (i < 9 && result->Messages.size() != 2) {
            continue;
        }

        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Offset, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Status, static_cast<ui32>(TStorage::EMessageStatus::Locked));
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].Offset, 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].Status, static_cast<ui32>(TStorage::EMessageStatus::Unprocessed));

        break;
    }
}

Y_UNIT_TEST(AlterConsumer) {
    auto setup = CreateSetup();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionPeriod(TDuration::Seconds(3))
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .DefaultProcessingTimeout(TDuration::Seconds(13))
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(17)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    {
        auto result = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");

        UNIT_ASSERT_VALUES_EQUAL(result->RetentionPeriod.value(), TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(result->Config.GetDefaultProcessingTimeoutSeconds(), 13);
        UNIT_ASSERT_VALUES_EQUAL(result->Config.GetMaxProcessingAttempts(), 17);
        UNIT_ASSERT_VALUES_EQUAL(::NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(result->Config.GetDeadLetterPolicy()),
            ::NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE));
    }

    client.CreateTopic("/Root/dlq-queue", NYdb::NTopic::TCreateTopicSettings()).GetValueSync();

    client.AlterTopic("/Root/topic1", NYdb::NTopic::TAlterTopicSettings()
            .SetRetentionPeriod(TDuration::Seconds(103))
            .BeginAlterConsumer("mlp-consumer")
                .DefaultProcessingTimeout(TDuration::Seconds(113))
                .BeginAlterDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(117)
                    .EndCondition()
                    .SetMoveAction("dlq-queue")
                .EndAlterDeadLetterPolicy()
            .EndAlterConsumer()).GetValueSync();

    {
        auto result = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");

        UNIT_ASSERT_VALUES_EQUAL(result->RetentionPeriod.value(), TDuration::Seconds(103));
        UNIT_ASSERT_VALUES_EQUAL(result->Config.GetDefaultProcessingTimeoutSeconds(), 113);
        UNIT_ASSERT_VALUES_EQUAL(result->Config.GetMaxProcessingAttempts(), 117);
        UNIT_ASSERT_VALUES_EQUAL(::NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(result->Config.GetDeadLetterPolicy()),
            ::NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE));
    }
}

Y_UNIT_TEST(RecreateConsumer) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionPeriod(TDuration::Seconds(3))
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .DefaultProcessingTimeout(TDuration::Seconds(13))
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(1000)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    Cerr << ">>>>> Write many messages for creating WAL (if message count is small every will create the snapshot)" << Endl;
    for (size_t i = 0; i < 50; ++i) {
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 0,
                    .MessageBody = "message_body",
                    .MessageGroupId = TStringBuilder() << "message_group_id_" << i
                },
            }
        });

        auto response = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    }

    Cerr << ">>>>> many iteration for creating many WAL records" << Endl;
    for (size_t i = 0; i < 50; ++i) {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });
        GetReadResponse(runtime);

        CreateUnlockerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) }
        });
        GetChangeResponse(runtime);
    }

    Cerr << ">>>>> Commit message" << Endl;
    {
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) }
        });

        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    Cerr << ">>>>> drop consumer" << Endl;
    auto result = client.AlterTopic("/Root/topic1", NYdb::NTopic::TAlterTopicSettings()
            .SetRetentionPeriod(TDuration::Seconds(103))
            .AppendDropConsumers("mlp-consumer")
        ).GetValueSync();

    Cerr << ">>>>> add consumer" << Endl;
    client.AlterTopic("/Root/topic1", NYdb::NTopic::TAlterTopicSettings()
            .SetRetentionPeriod(TDuration::Seconds(103))
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .DefaultProcessingTimeout(TDuration::Seconds(13))
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(17)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()
        ).GetValueSync();

    Cerr << ">>>>> read message (write snapshot)" << Endl;
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
    }

    Cerr << ">>>>> read message (write WAL)" << Endl;
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 1);
    }

    Cerr << ">>>>> reload pq tablet" << Endl;
    ReloadPQTablet(setup, "/Root", "/Root/topic1", 0);

    Cerr << ">>>>> read message after reload" << Endl;
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 2);
    }
}

Y_UNIT_TEST(ReloadPQTabletAfterAlterConsumer) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionPeriod(TDuration::Seconds(3))
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .DefaultProcessingTimeout(TDuration::Seconds(13))
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(17)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    WriteMany(setup, "/Root/topic1", 0, 16, 113);

    Sleep(TDuration::Seconds(1));

    Cerr << ">>>>> BEGIN COMMIT" << Endl;
    {
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) }
        });

        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    client.CreateTopic("/Root/dlq-queue", NYdb::NTopic::TCreateTopicSettings()).GetValueSync();

    client.AlterTopic("/Root/topic1", NYdb::NTopic::TAlterTopicSettings()
        .SetRetentionPeriod(TDuration::Seconds(103))
        .BeginAlterConsumer("mlp-consumer")
            .DefaultProcessingTimeout(TDuration::Seconds(113))
            .BeginAlterDeadLetterPolicy()
                .Enable()
                .BeginCondition()
                    .MaxProcessingAttempts(117)
                .EndCondition()
                .SetMoveAction("dlq-queue")
            .EndAlterDeadLetterPolicy()
        .EndAlterConsumer()).GetValueSync();

    Cerr << ">>>>> BEGIN REBOOT " << Endl;
    ReloadPQTablet(setup, "/Root", "/Root/topic1", 0);

    Sleep(TDuration::Seconds(1));

    // Checking that alter consumer do not change consumer generation and snapshot and wal read successfully
    for (size_t i = 0; i < 10; ++i) {
        Sleep(TDuration::Seconds(1));

        auto result = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        if (i < 9 && result->Messages.size() != 16) {
            continue;
        }

        // Message with offset 0 was committed and deleted
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Offset, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Status, static_cast<ui32>(TStorage::EMessageStatus::Unprocessed));

        break;
    }
}

Y_UNIT_TEST(CommitNonExistentMessage) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionPeriod(TDuration::Seconds(3))
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .DefaultProcessingTimeout(TDuration::Seconds(13))
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(17)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    Cerr << ">>>>> BEGIN COMMIT" << Endl;
    {
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) }
        });

        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }
}

Y_UNIT_TEST(UnlockNonExistentMessage) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionPeriod(TDuration::Seconds(3))
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .DefaultProcessingTimeout(TDuration::Seconds(13))
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(17)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    Cerr << ">>>>> BEGIN UNLOCK" << Endl;
    {
        CreateUnlockerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) }
        });

        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }
}

Y_UNIT_TEST(ChangeMessageDeadlineNonExistentMessage) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionPeriod(TDuration::Seconds(3))
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .DefaultProcessingTimeout(TDuration::Seconds(13))
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(17)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    Cerr << ">>>>> BEGIN CHANGE MESSAGE DEADLINE" << Endl;
    {
        CreateMessageDeadlineChangerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) },
            .Deadlines = { TInstant::Seconds(1000) }
        });

        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }
}

void HtmlApp(std::string_view consumer, size_t partitionId, std::string_view expected) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
            .EndAddConsumer()).GetValueSync();

    Sleep(TDuration::Seconds(1));

    auto tabletId = GetTabletId(setup, "/Root", "/Root/topic1", 0);
    auto url = TStringBuilder() << "/app?TabletID=" << tabletId
        << "&consumer=" << consumer
        << "&partitionId=" << partitionId;
    runtime.SendToPipe(tabletId, runtime.AllocateEdgeActor(),
        new NMon::TEvRemoteHttpInfo(url, HTTP_METHOD_GET));

    auto response = runtime.GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>();
    UNIT_ASSERT(response);

    Cerr << (TStringBuilder() <<">>>>> " << response->Html << Endl);
    UNIT_ASSERT(response->Html.find(expected) != TString::npos);
}

Y_UNIT_TEST(HtmlApp_Success) {
    HtmlApp("mlp-consumer", 0, "Total metrics");
}

Y_UNIT_TEST(HtmlApp_BadConsumer) {
    HtmlApp("mlp-consumer-not-exists", 0, "MLP consumer 'mlp-consumer-not-exists' not found");
}

Y_UNIT_TEST(HtmlApp_BadPartition) {
    HtmlApp("mlp-consumer", 13, "Tablet info");
}

Y_UNIT_TEST(RetentionExpiresMessages) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);
    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionPeriod(TDuration::Seconds(3))
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
            .EndAddConsumer()).GetValueSync();

    setup->Write("/Root/topic1", "expire-me", 0);
    Sleep(TDuration::Seconds(1));

    {
        auto state = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT(!state->Messages.empty());
    }

    // Past retention: consumer wakeups compact expired messages away.
    for (size_t i = 0; i < 15; ++i) {
        Sleep(TDuration::Seconds(1));
        // Nudge the consumer so ProccessDeadlines / Compact run.
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(0),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        GetReadResponse(runtime);

        auto state = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        if (state->Messages.empty()) {
            auto describe = setup->DescribeConsumer("/Root/topic1", "mlp-consumer");
            UNIT_ASSERT_VALUES_EQUAL(describe.GetPartitions()[0].GetPartitionConsumerStats()->GetCommittedOffset(), 1);
            return;
        }
    }
    UNIT_FAIL("Message was not removed by retention");
}

Y_UNIT_TEST(DLQ_DeleteActionAfterMaxAttempts) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);
    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(1)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    setup->Write("/Root/topic1", "delete-me", 0);
    Sleep(TDuration::Seconds(1));

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
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
    }

    {
        CreateUnlockerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = {TMessageId(0, 0)},
        });
        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    for (size_t i = 0; i < 10; ++i) {
        Sleep(TDuration::MilliSeconds(500));
        auto state = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        if (!state->Messages.empty()) {
            continue;
        }
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
        return;
    }
    UNIT_FAIL("Message was not deleted by DLQ DeleteAction");
}

Y_UNIT_TEST(ZeroVisibilityTimeoutUnlocksImmediately) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);
    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(1)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    setup->Write("/Root/topic1", "zero-vis", 0);
    Sleep(TDuration::Seconds(1));

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Zero(),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "zero-vis");
    }

    // DeadlineDelta==0 expires on the next consumer cycle → unlock → delete (max attempts=1).
    for (size_t i = 0; i < 15; ++i) {
        Sleep(TDuration::Seconds(1));
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(0),
            .ProcessingTimeout = TDuration::Seconds(5),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        auto state = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        if (response->Messages.empty() && state->Messages.empty()) {
            return;
        }
    }
    UNIT_FAIL("Zero-visibility message was not unlocked/deleted");
}

Y_UNIT_TEST(LongPollEmptyThenDataArrives) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    CreateReaderActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .WaitTime = TDuration::Seconds(10),
        .ProcessingTimeout = TDuration::Seconds(30),
        .MaxNumberOfMessage = 1,
    });

    Sleep(TDuration::MilliSeconds(500));
    setup->Write("/Root/topic1", "late-arrival", 0);

    auto response = GetReadResponse(runtime, TDuration::Seconds(15));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "late-arrival");
}

Y_UNIT_TEST(FetchAfterEndOffsetChanged) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    // Let the consumer initialize while the partition is empty.
    Sleep(TDuration::Seconds(2));
    {
        auto state = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT(state->Messages.empty());
    }

    setup->Write("/Root/topic1", "after-idle", 0);

    for (size_t i = 0; i < 10; ++i) {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        if (response->Messages.empty()) {
            Sleep(TDuration::MilliSeconds(500));
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "after-idle");
        return;
    }
    UNIT_FAIL("Consumer did not fetch message after EndOffsetChanged");
}

Y_UNIT_TEST(PurgeClearsInflightAndUnprocessed) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    setup->Write("/Root/topic1", "msg0", 0);
    setup->Write("/Root/topic1", "msg1", 0);
    Sleep(TDuration::Seconds(1));

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
    }

    CreatePurgerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
    });
    AssertPurgeOK(runtime);

    {
        auto state = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT(state->Messages.empty());
    }
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(0),
            .ProcessingTimeout = TDuration::Seconds(5),
            .MaxNumberOfMessage = 10,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
    }
}

Y_UNIT_TEST(VisibilityTimeoutRedelivery) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "redeliver-me", 0);
    Sleep(TDuration::Seconds(1));

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(2),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].ApproximateReceiveCount, 1);
    }

    Sleep(TDuration::Seconds(3));

    {
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
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "redeliver-me");
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].ApproximateReceiveCount, 2);
    }

    auto state = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
    UNIT_ASSERT_VALUES_EQUAL(state->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(state->Messages[0].Status, static_cast<ui32>(TStorage::EMessageStatus::Locked));
    UNIT_ASSERT_VALUES_EQUAL(state->Messages[0].ProcessingCount, 2);
}

Y_UNIT_TEST(ChangeDeadlineExtendsVisibility) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "extend-me", 0);
    Sleep(TDuration::Seconds(1));

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(2),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    }

    {
        CreateMessageDeadlineChangerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = {TMessageId(0, 0)},
            .Deadlines = {TInstant::Now() + TDuration::Seconds(30)},
        });
        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    Sleep(TDuration::Seconds(3)); // past original 2s visibility

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

    auto state = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
    UNIT_ASSERT_VALUES_EQUAL(state->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(state->Messages[0].Status, static_cast<ui32>(TStorage::EMessageStatus::Locked));
}

Y_UNIT_TEST(DelayedMessageNotReadableUntilDeadline) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    CreateWriterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Messages = {{
            .Index = 0,
            .MessageBody = "delayed",
            .Delay = TDuration::Seconds(3),
        }},
    });
    {
        auto write = GetWriteResponse(runtime);
        UNIT_ASSERT(write);
        UNIT_ASSERT_VALUES_EQUAL(write->Messages.size(), 1);
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
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
    }

    {
        auto state = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_VALUES_EQUAL(state->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(state->Messages[0].Status, static_cast<ui32>(TStorage::EMessageStatus::Delayed));
    }

    Sleep(TDuration::Seconds(4));

    {
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
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "delayed");
    }
}

Y_UNIT_TEST(FetchThrottledAtMinMessages) {
    // Consumer skips fetch once InflightMessageCount >= MinMessages (100).
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    WriteMany(setup, "/Root/topic1", 0, /*messageSize=*/64, /*messageCount=*/150);

    for (size_t i = 0; i < 20; ++i) {
        Sleep(TDuration::MilliSeconds(500));
        auto state = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        // Fetch stops once Inflight >= MinMessages (100); last batch may overshoot slightly.
        if (state->Messages.size() >= 100) {
            UNIT_ASSERT(state->Messages.size() < 150);
            CreatePurgerActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
            });
            AssertPurgeOK(runtime);
            return;
        }
    }
    UNIT_FAIL("Consumer did not stop fetching around MinMessages=100");
}

Y_UNIT_TEST(ReloadWhileMessageLocked) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "locked-across-reload", 0);
    Sleep(TDuration::Seconds(1));

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(60),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    }

    ReloadPQTablet(setup, "/Root", "/Root/topic1", 0);

    for (size_t i = 0; i < 10; ++i) {
        Sleep(TDuration::Seconds(1));
        auto state = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        if (state->Messages.size() != 1) {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL(state->Messages[0].Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(state->Messages[0].Status, static_cast<ui32>(TStorage::EMessageStatus::Locked));
        return;
    }
    UNIT_FAIL("Locked message was not restored after reload");
}

Y_UNIT_TEST(EmptyReadImmediateOnEmptyTopic) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    CreateReaderActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .WaitTime = TDuration::Seconds(0),
        .ProcessingTimeout = TDuration::Seconds(5),
        .MaxNumberOfMessage = 1,
    });
    auto response = GetReadResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
}

Y_UNIT_TEST(CommitAfterUnlockSucceeds) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    setup->Write("/Root/topic1", "msg", 0);
    Sleep(TDuration::Seconds(1));

    CreateReaderActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .WaitTime = TDuration::Seconds(1),
        .ProcessingTimeout = TDuration::Seconds(30),
        .MaxNumberOfMessage = 1,
    });
    {
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    }

    CreateUnlockerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = {TMessageId(0, 0)},
    });
    {
        auto unlock = GetChangeResponse(runtime);
        UNIT_ASSERT(unlock);
        UNIT_ASSERT_VALUES_EQUAL(unlock->Status, Ydb::StatusIds::SUCCESS);
    }

    // Unlocked → Unprocessed; commit of Unprocessed is allowed.
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = {TMessageId(0, 0)},
    });
    auto commit = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(commit->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(commit->Messages.size(), 1);
    UNIT_ASSERT(commit->Messages[0].Status == EOperationResult::Success);
}

Y_UNIT_TEST(KeepMessagesOrderBasicFifo) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1, true);

    CreateWriterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Messages = {
            {.Index = 0, .MessageBody = "first", .MessageGroupId = "g", .MessageDeduplicationId = "d1"},
            {.Index = 1, .MessageBody = "second", .MessageGroupId = "g", .MessageDeduplicationId = "d2"},
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
            .MaxNumberOfMessage = 10,
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "first");
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
        .MaxNumberOfMessage = 10,
    });
    auto response = GetReadResponse(runtime);
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "second");
}

Y_UNIT_TEST(DLQ_MoveFailsThenSucceedsAfterDlqCreated) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    // Create DLQ first so create/alter ACL checks pass, then drop it so the mover fails.
    client.CreateTopic("/Root/topic1-dlq", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
            .EndAddConsumer()).GetValueSync();

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(1)
                    .EndCondition()
                    .MoveAction("/Root/topic1-dlq")
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    client.DropTopic("/Root/topic1-dlq").GetValueSync();

    const auto msg = "dlq-retry-me";
    setup->Write("/Root/topic1", msg, 0);
    Sleep(TDuration::Seconds(1));

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
    }
    {
        CreateUnlockerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) },
        });
        auto unlock = GetChangeResponse(runtime);
        UNIT_ASSERT(unlock);
        UNIT_ASSERT_VALUES_EQUAL(unlock->Status, Ydb::StatusIds::SUCCESS);
    }

    // After failed move the message must become readable again.
    for (size_t i = 0; i < 15; ++i) {
        Sleep(TDuration::Seconds(1));
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(0),
            .ProcessingTimeout = TDuration::Seconds(5),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        if (response->Messages.size() == 1) {
            break;
        }
        UNIT_ASSERT_C(i < 14, "message did not return after DLQ move failure");
    }

    client.CreateTopic("/Root/topic1-dlq", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
            .EndAddConsumer()).GetValueSync();

    {
        CreateUnlockerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) },
        });
        auto unlock = GetChangeResponse(runtime);
        UNIT_ASSERT(unlock);
        UNIT_ASSERT_VALUES_EQUAL(unlock->Status, Ydb::StatusIds::SUCCESS);
    }

    for (size_t i = 0; i < 15; ++i) {
        Sleep(TDuration::Seconds(1));
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1-dlq",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(0),
            .ProcessingTimeout = TDuration::Seconds(5),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime);
        if (i < 14 && response->Messages.empty()) {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, msg);
        return;
    }
}

Y_UNIT_TEST(LongPollDuringPQTabletReload) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    CreateReaderActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .WaitTime = TDuration::Seconds(30),
        .ProcessingTimeout = TDuration::Seconds(5),
        .MaxNumberOfMessage = 1,
    });

    Sleep(TDuration::MilliSeconds(500));
    ReloadPQTablet(setup, "/Root", "/Root/topic1", 0);
    setup->Write("/Root/topic1", "after-reload", 0);

    auto inFlight = GetReadResponse(runtime, TDuration::Seconds(60));
    UNIT_ASSERT(inFlight);
    // Consumer PassAway replies UNAVAILABLE "Actor destroyed"; or the read may still succeed.
    UNIT_ASSERT(inFlight->Status == Ydb::StatusIds::UNAVAILABLE
        || inFlight->Status == Ydb::StatusIds::SUCCESS);
    if (inFlight->Status == Ydb::StatusIds::SUCCESS && !inFlight->Messages.empty()) {
        UNIT_ASSERT_VALUES_EQUAL(inFlight->Messages[0].Data, "after-reload");
        return;
    }

    for (size_t i = 0; i < 10; ++i) {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(2),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime, TDuration::Seconds(60));
        UNIT_ASSERT(response);
        if (response->Status == Ydb::StatusIds::SUCCESS && response->Messages.size() == 1) {
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "after-reload");
            return;
        }
        Sleep(TDuration::Seconds(1));
    }
    UNIT_FAIL("message not readable after tablet reload during long-poll");
}

}

} // namespace NKikimr::NPQ::NMLP
