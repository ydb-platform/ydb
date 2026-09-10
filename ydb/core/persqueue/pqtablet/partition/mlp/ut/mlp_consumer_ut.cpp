#include "mlp.h"
#include "mlp_storage.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/mon.h>

#include <atomic>

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

// ---------------------------------------------------------------------------
// Tests for PQConfig.MLPUnlockedGroupsRatio: FIFO (KeepMessageOrder)
// read-ahead behavior in TConsumerActor::RequiredToFetchMessageCount().
//
// Scenario: 100000 messages spread over 10 groups + 5 messages in 5 new unique
// groups (15 groups total). With read-ahead disabled (== 0, legacy behavior) the
// consumer fetches in small MinMessages-sized chunks, so a single read cannot see
// the tail groups. With read-ahead enabled (> 0) the consumer fetches up to
// MaxMessages, pulling in the heads of all groups.
// ---------------------------------------------------------------------------

static constexpr size_t kReadAheadBaseGroups = 10;
static constexpr size_t kReadAheadBaseMessages = 100000;
static constexpr size_t kReadAheadUniqueGroups = 5;

static void WriteReadAheadDataset(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topic) {
    // 100000 messages over 10 groups (round-robin).
    WriteManyGroups(setup, topic, /*messageSize=*/1, kReadAheadBaseMessages, kReadAheadBaseGroups);

    // 5 messages, each in its own brand-new unique group (groups 100..104).
    auto& runtime = setup->GetRuntime();
    std::vector<TWriterSettings::TMessage> messages;
    for (size_t i = 0; i < kReadAheadUniqueGroups; ++i) {
        messages.push_back({
            .Index = i,
            .MessageBody = NUnitTest::RandomString(1),
            .MessageGroupId = TStringBuilder() << "unique_message_group_id_" << (100 + i),
        });
    }
    CreateWriterActor(runtime, TWriterSettings{
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Messages = std::move(messages),
    });
    auto response = GetWriteResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), kReadAheadUniqueGroups);
}

// ---------------------------------------------------------------------------
// Test type 1 (deterministic, single-threaded actor harness): boot a consumer
// with a pre-built snapshot of 100000 messages over 10 groups and inspect the
// Count in the CmdRead request it emits to the PQ tablet.
//
// The TTopicSdkTestSetup runtime uses real threads, where the consumer -> tablet
// local Send() is not intercepted by runtime observers. Here we register the
// consumer directly against edge actors and grab the read request.
// ---------------------------------------------------------------------------

namespace {

constexpr ui64 kReadAheadTabletId = 100;
constexpr const char* kReadAheadConsumer = "mlp-consumer";

NKikimrPQ::TPQTabletConfig MakeReadAheadTopicConfig() {
    NKikimrPQ::TPQTabletConfig config;
    config.SetTopicName("topic");
    config.SetTopicPath("/Root/topic");

    auto* partition = config.AddAllPartitions();
    partition->SetPartitionId(0);
    partition->SetTabletId(kReadAheadTabletId);
    partition->SetStatus(NKikimrPQ::ETopicPartitionStatus::Active);

    auto* consumer = config.AddConsumers();
    consumer->SetName(kReadAheadConsumer);
    consumer->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    consumer->SetKeepMessageOrder(true);
    consumer->SetGeneration(1);
    return config;
}

NKikimrPQ::TPQTabletConfig::TConsumer MakeReadAheadConsumerConfig() {
    NKikimrPQ::TPQTabletConfig::TConsumer consumer;
    consumer.SetName(kReadAheadConsumer);
    consumer.SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    consumer.SetKeepMessageOrder(true);
    consumer.SetGeneration(1);
    return consumer;
}

// Builds a serialized snapshot with `messageCount` messages spread round-robin
// across `groupCount` groups, locking the heads of `lockedGroups` distinct groups.
TString BuildSnapshotBytes(size_t messageCount, size_t groupCount, size_t lockedGroups) {
    auto timeProvider = CreateDefaultTimeProvider();
    TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});
    const TInstant now = timeProvider->Now();
    for (size_t offset = 0; offset < messageCount; ++offset) {
        storage.AddMessage(offset, /*hasMessagegroup=*/true, /*messageGroupIdHash=*/offset % groupCount, now);
    }
    TStorage::TPosition position;
    for (size_t i = 0; i < lockedGroups; ++i) {
        auto locked = storage.Next(now + TDuration::Hours(1), position);
        UNIT_ASSERT(locked.has_value());
    }
    auto batch = storage.ExtractBatch();
    Y_UNUSED(batch);

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    auto* configuration = snapshot.MutableConfiguration();
    configuration->SetConsumerName(kReadAheadConsumer);
    configuration->SetGeneration(1);
    storage.SerializeTo(snapshot);
    return snapshot.SerializeAsString();
}

THolder<TEvKeyValue::TEvResponse> MakeSnapshotKvResponse(ui64 cookie, const TString& snapshotBytes) {
    auto response = MakeHolder<TEvKeyValue::TEvResponse>();
    response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    response->Record.SetCookie(cookie);
    auto* readResult = response->Record.AddReadResult();
    readResult->SetStatus(NKikimrProto::OK);
    readResult->SetValue(snapshotBytes);
    response->Record.AddReadRangeResult()->SetStatus(NKikimrProto::NODATA);
    return response;
}

class TIgnorePipeCacheActor : public TActorBootstrapped<TIgnorePipeCacheActor> {
public:
    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STRICT_STFUNC(StateWork,
        IgnoreFunc(TEvPipeCache::TEvForward);
        IgnoreFunc(TEvPipeCache::TEvUnlink);
    )
};

// Boots a consumer with the given read-ahead flag and a pre-built snapshot of
// messageCount messages over groupCount groups with lockedGroups heads locked,
// then returns the Count of the first CmdRead it emits, or nullopt if the fully
// buffered consumer decides not to fetch at all.
std::optional<ui64> GrabFirstFetchCount(float readAhead, size_t messageCount, size_t groupCount, size_t lockedGroups) {
    TTestBasicRuntime runtime(1, false);
    runtime.Initialize(TAppPrepare().Unwrap());
    runtime.SetScheduledLimit(10000);
    runtime.GetAppData().PQConfig.SetMLPUnlockedGroupsRatio(readAhead);

    auto pipeCache = runtime.Register(new TIgnorePipeCacheActor());
    runtime.EnableScheduleForActor(pipeCache);
    runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCache);

    auto tablet = runtime.AllocateEdgeActor();
    auto partition = runtime.AllocateEdgeActor();

    ::NMonitoring::TDynamicCounterPtr counters(new ::NMonitoring::TDynamicCounters());
    auto consumer = runtime.Register(CreateConsumerActor(
        "/Root",
        kReadAheadTabletId,
        tablet,
        /*partitionId=*/0,
        partition,
        /*partitionGeneration=*/1,
        MakeReadAheadTopicConfig(),
        MakeReadAheadConsumerConfig(),
        TDuration::Hours(1),
        /*partitionEndOffset=*/messageCount + 1000,
        counters));
    runtime.EnableScheduleForActor(consumer);

    const TString snapshotBytes = BuildSnapshotBytes(messageCount, groupCount, lockedGroups);

    auto kvReq = runtime.GrabEdgeEvent<TEvKeyValue::TEvRequest>(TDuration::Seconds(10));
    UNIT_ASSERT(kvReq);
    runtime.Send(new IEventHandle(consumer, tablet,
        MakeSnapshotKvResponse(kvReq->Record.GetCookie(), snapshotBytes).Release()));

    auto readReq = runtime.GrabEdgeEvent<TEvPersQueue::TEvRequest>(TDuration::Seconds(3));
    if (!readReq) {
        return std::nullopt;
    }
    UNIT_ASSERT(readReq->Record.HasPartitionRequest());
    UNIT_ASSERT(readReq->Record.GetPartitionRequest().HasCmdRead());
    const auto& read = readReq->Record.GetPartitionRequest().GetCmdRead();
    UNIT_ASSERT(read.HasCount());
    return static_cast<ui64>(read.GetCount());
}

} // namespace

// EstimateFetchCountForNewGroups: exercised directly as a pure function.
Y_UNIT_TEST(FifoReadAheadEstimateFetchCountForNewGroups) {
    UNIT_ASSERT_VALUES_EQUAL(EstimateFetchCountForNewGroups(100000, 10, 1), 10000);
    UNIT_ASSERT_VALUES_EQUAL(EstimateFetchCountForNewGroups(100000, 10, 3), 30000);
    UNIT_ASSERT_VALUES_EQUAL(EstimateFetchCountForNewGroups(0, 0, 5), 5);
    UNIT_ASSERT_VALUES_EQUAL(EstimateFetchCountForNewGroups(100000, 10, 0), 0);
    UNIT_ASSERT_VALUES_EQUAL(EstimateFetchCountForNewGroups(5, 10, 2), 2);
    UNIT_ASSERT_VALUES_EQUAL(EstimateFetchCountForNewGroups(95, 10, 1), 10);
}

// With read-ahead disabled the fully buffered FIFO consumer does not fetch ahead
// at all, even with several groups locked.
Y_UNIT_TEST(FifoReadAheadDisabledDoesNotFetch) {
    const auto count = GrabFirstFetchCount(/*readAhead=*/0.0f, kReadAheadBaseMessages, kReadAheadBaseGroups, /*lockedGroups=*/6);
    UNIT_ASSERT_C(!count, TStringBuilder() << "expected no fetch with read-ahead disabled, got " << *count);
}

// ratio 0.5: 10 groups, 6 locked -> readable 4 < target ceil(5)=5 -> 1 missing
// group; density is 100000/10 = 10000 messages per group.
Y_UNIT_TEST(FifoReadAheadRatioFetchesEstimatedBatch) {
    const auto count = GrabFirstFetchCount(/*readAhead=*/0.5f, kReadAheadBaseMessages, kReadAheadBaseGroups, /*lockedGroups=*/6);
    UNIT_ASSERT(count);
    Cerr << ">>>>> first CmdRead count (ratio 0.5): " << *count << Endl;
    UNIT_ASSERT_VALUES_EQUAL(*count, 10000);
}

// ratio 1.0: readable 4 < target 10 -> 6 missing groups -> estimate 60000, capped
// by the free in-flight capacity (MaxMessages - 100000 = 20000).
Y_UNIT_TEST(FifoReadAheadEnabledFetchesMaxBatches) {
    const auto count = GrabFirstFetchCount(/*readAhead=*/1.0f, kReadAheadBaseMessages, kReadAheadBaseGroups, /*lockedGroups=*/6);
    UNIT_ASSERT(count);
    Cerr << ">>>>> first CmdRead count (enabled): " << *count << Endl;
    UNIT_ASSERT_VALUES_EQUAL(*count, 20000);
}

// Test type 2: with read-ahead enabled the consumer keeps fetching past the
// 100000 head messages until it reaches the 5 unique tail groups, so all 15 group
// heads become readable in a single read.
size_t ReadDistinctGroupHeads(std::shared_ptr<TTopicSdkTestSetup>& setup, size_t attempts) {
    auto& runtime = setup->GetRuntime();
    const size_t expectedGroups = kReadAheadBaseGroups + kReadAheadUniqueGroups; // 15
    size_t best = 0;
    for (size_t i = 0; i < attempts; ++i) {
        Sleep(TDuration::Seconds(1));
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(2),
            .ProcessingTimeout = TDuration::Seconds(1),
            .MaxNumberOfMessage = static_cast<ui32>(expectedGroups),
        });
        auto response = GetReadResponse(runtime, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        best = std::max(best, response->Messages.size());
        if (best == expectedGroups) {
            break;
        }
        Cerr << ">>>>> attempt " << i << ": read " << response->Messages.size() << " groups" << Endl;
    }
    return best;
}

void FifoReadAheadReadAllGroupsImpl(float readAhead) {
    auto setup = CreateSetup();
    setup->GetRuntime().GetAppData().PQConfig.SetMLPUnlockedGroupsRatio(readAhead);
    CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1, /*keepMessagesOrder=*/true);
    WriteReadAheadDataset(setup, "/Root/topic1");

    const size_t expectedGroups = kReadAheadBaseGroups + kReadAheadUniqueGroups;
    UNIT_ASSERT_VALUES_EQUAL(ReadDistinctGroupHeads(setup, 30), expectedGroups);
}

Y_UNIT_TEST(FifoReadAheadEnabledReadsAllGroups) {
    FifoReadAheadReadAllGroupsImpl(1.0f);
}

Y_UNIT_TEST(FifoReadAheadRatioReadsAllGroups) {
    FifoReadAheadReadAllGroupsImpl(0.5f);
}

// Negative: with read-ahead disabled the FIFO consumer keeps only a minimal
// buffer and never fetches deep enough to surface the 5 unique tail groups, so
// only the 10 head groups are ever readable.
Y_UNIT_TEST(FifoReadAheadDisabledDoesNotReachTailGroups) {
    auto setup = CreateSetup();
    setup->GetRuntime().GetAppData().PQConfig.SetMLPUnlockedGroupsRatio(0.0f);
    CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1, /*keepMessagesOrder=*/true);
    WriteReadAheadDataset(setup, "/Root/topic1");

    UNIT_ASSERT_VALUES_EQUAL(ReadDistinctGroupHeads(setup, 8), kReadAheadBaseGroups);
}

}

} // namespace NKikimr::NPQ::NMLP
