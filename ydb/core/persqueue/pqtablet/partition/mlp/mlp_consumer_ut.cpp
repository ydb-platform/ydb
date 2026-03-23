#include "mlp_storage.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/iterator/iterate_values.h>

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

static size_t WaitForPartitionCount(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName, size_t expectedCount, size_t maxRetries = 30) {
    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    size_t partitionCount = 0;
    for (size_t i = 0; i < maxRetries; ++i) {
        Sleep(TDuration::Seconds(1));
        auto describeResult = client.DescribeTopic(topicName).GetValueSync();
        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

        partitionCount = describeResult.GetTopicDescription().GetPartitions().size();
        Cerr << ">>>>> Partition count: " << partitionCount << " (expected " << expectedCount << ")" << Endl;

        if (partitionCount >= expectedCount) {
            break;
        }
    }

    driver.Stop(true);
    return partitionCount;
}

static void DumpStorageState(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName, const TString& consumerName, const TSet<ui32>& partitions) {
    TStringStream output;
    Y_DEFER {
        output << "\n";
        Cerr << output.Str();
    };
    for (const ui32 partition : partitions) {
        auto result = GetConsumerState(setup, "/Root", topicName, consumerName, partition);
        if (!result) {
            output << LabeledOutput(partition) << "; null " << Endl;
            continue;
        }
        output << LabeledOutput(partition) << "; messages: " << result->Messages.size() << Endl;
        for (size_t i = 0; i < result->Messages.size(); ++i) {
            output << "    message " << i << ": offset=" << result->Messages[i].Offset << " status=" << result->Messages[i].Status << Endl;
        }
    }
}

struct TReadCommitCount {
    size_t ReadCount = 0;
    bool CommitLast = true;
    // If CommitLast is true, all ReadCount messages are committed.
    // If CommitLast is false, the last message is left uncommitted (locked).
    // Effective CommitCount == ReadCount - !CommitLast
};

struct TGroupDescription {
    size_t SizeBeforeSplit = 0;
    size_t SizeAfterSplit = 0;
    TReadCommitCount ReadBeforeSplit;
};

static TString JoinMessageIds(std::span<const TMessageId> messages) {
    TStringBuilder ss;
    for (const auto& msg : messages) {
        if (ss.size() > 0) {
            ss << ", ";
        }
        ss << "{p=" << msg.PartitionId << ",o=" << msg.Offset << "}";
    }
    return "[" + ss + "]";
}

static auto Commit(NActors::TTestActorRuntime& runtime, const TString& topicName, const TString& consumer, const std::vector<TMessageId>& messagesToCommit) {
    Cerr << "Commiting messages: " << JoinMessageIds(messagesToCommit) << "\n";
    CreateCommitterActor(runtime, {
                                      .DatabasePath = "/Root",
                                      .TopicName = topicName,
                                      .Consumer = consumer,
                                      .Messages = messagesToCommit,
                                  });
    auto commitResult = GetChangeResponse(runtime);
    return commitResult;
}

static auto Unlock(NActors::TTestActorRuntime& runtime, const TString& topicName, const TString& consumer, const std::vector<TMessageId>& messagesToUnlock) {
    Cerr << "Unlocking messages: " << JoinMessageIds(messagesToUnlock) << "\n";
    CreateUnlockerActor(runtime, {
                                      .DatabasePath = "/Root",
                                      .TopicName = topicName,
                                      .Consumer = consumer,
                                      .Messages = messagesToUnlock,
                                  });
    auto commitResult = GetChangeResponse(runtime);
    return commitResult;
}

TMap<TString, TMessageId> ReadAndCommitFIFOExceptLast(
    NActors::TTestActorRuntime& runtime,
    const TString& topicName,
    const TString& consumer,
    TMap<TString, size_t>& remainingReads,
    TStringBuf phase,
    int retries
) {

    TMap<TString, TMessageId> lastMessageOfTheGroup;
    for (int readIteration = 1; ; ++readIteration) {
        {
            TStringBuilder sb;
            sb << phase << " ReadAndCommitFIFOExceptLast iteration=" << readIteration << ", retries left=" << retries << ":\n";
            for (auto& [g, n] : remainingReads) {
                sb << "  " << g << ": read=" << n << Endl;
            }
            if (lastMessageOfTheGroup.size() > 0) {
                sb << "Last messages (" << lastMessageOfTheGroup.size() << "):\n";
                for (auto& [g, m] : lastMessageOfTheGroup) {
                    sb << "  group=" << g
                       << " partition=" << m.PartitionId
                       << " offset=" << m.Offset << Endl;
                }
            }
            sb << "\n";
            Cerr << sb << Endl;
        }
        const size_t toRead = [&]() {
            size_t r = 0;
            for (auto& [g, n] : remainingReads) {
                if (!lastMessageOfTheGroup.contains(g)) {
                    r += n;
                }
            }
            return r;
        }();
        if (toRead == 0) {
            break;
        }
        CreateReaderActor(runtime, {.DatabasePath = "/Root",
                                    .TopicName = topicName,
                                    .Consumer = consumer,
                                    .WaitTime = TDuration::Seconds(3),
                                    .ProcessingTimeout = TDuration::Seconds(3000),
                                    .MaxNumberOfMessage = 10});
        auto readResult = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(readResult->Status, Ydb::StatusIds::SUCCESS, phase);
        if (readResult->Messages.empty()) {
            if (retries-- <= 0) {
                break;
            } else {
                continue;
            }
        }
        std::vector<TMessageId> messagesToCommit;
        for (auto& msg : readResult->Messages) {
            const TString& groupId = msg.MessageGroupId;
            Cerr << ">>>>> " << phase << " ReadAndCommitFIFOExceptLast read: " << msg.Data
                 << " group=" << groupId
                 << " partition=" << msg.MessageId.PartitionId
                 << " offset=" << msg.MessageId.Offset << Endl;
            UNIT_ASSERT_C(!lastMessageOfTheGroup.contains(groupId), LabeledOutput(groupId));

            if (remainingReads.Value(groupId, 0) > 1) {
                messagesToCommit.push_back(msg.MessageId);
                remainingReads[groupId]--;
            } else {
                lastMessageOfTheGroup[groupId] = msg.MessageId;
            }
        }
        if (!messagesToCommit.empty()) {
            auto commitResult = Commit(runtime, topicName, consumer, messagesToCommit);
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult->Status, Ydb::StatusIds::SUCCESS, phase);
        }
    }

    // Handle the last message for each group: commit or leave uncommitted
    std::vector<TMessageId> messagesToCommit;
    std::vector<TMessageId> messagesToUnlock;
    TMap<TString, TMessageId> uncommitedMessages;
    for (const auto& [groupId, message] : lastMessageOfTheGroup) {
        if (remainingReads.Value(groupId, 0) > 1) {
            messagesToCommit.push_back(message);
            remainingReads[groupId]--;
        } else if (remainingReads.Value(groupId, 0) == 1) {
            remainingReads[groupId]--;
            uncommitedMessages[groupId] = message;
        } else {
            Y_ASSERT(remainingReads.Value(groupId, 0) == 0);
            messagesToUnlock.push_back(message);
        }
    }
    if (!messagesToCommit.empty()) {
        auto commitResult = Commit(runtime, topicName, consumer, messagesToCommit);
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult->Status, Ydb::StatusIds::SUCCESS, phase);
    }
    if (!messagesToUnlock.empty()) {
        auto unlockResult = Unlock(runtime, topicName, consumer, messagesToUnlock);
        UNIT_ASSERT_VALUES_EQUAL_C(unlockResult->Status, Ydb::StatusIds::SUCCESS, phase);
    }
    return uncommitedMessages;
}

TMap<TString, TMessageId> ReadAndCommitFIFO(
    NActors::TTestActorRuntime& runtime,
    const TString& topicName,
    const TString& consumer,
    const TMap<TString, TReadCommitCount>& operations,
    TStringBuf phase,
    int retries = 0
) {
    TMap<TString, size_t> remainingReads;
    for (const auto& [groupId, op] : operations) {
        remainingReads[groupId] = op.ReadCount;
    }
    TMap<TString, TMessageId> last = ReadAndCommitFIFOExceptLast(runtime, topicName, consumer, remainingReads, phase, retries);
    for (const auto& [groupId, cnt] : remainingReads) {
        UNIT_ASSERT_VALUES_EQUAL_C(cnt, 0, phase << ": Not enough messages for group " << groupId << "; " << cnt << " more required");
    }
    TMap<TString, TMessageId> uncommited;
    std::vector<TMessageId> messagesToCommit;
    for (const auto& [groupId, message] : last) {
        const auto& op = operations.at(groupId);
        if (op.CommitLast) {
            messagesToCommit.push_back(message);
        } else {
            uncommited[groupId] = message;
        }
    }
    if (!messagesToCommit.empty()) {
        auto commitResult = Commit(runtime, topicName, consumer, messagesToCommit);
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult->Status, Ydb::StatusIds::SUCCESS, phase);
    }
    return uncommited;
}

void PartitionSplitWithMessageGroupOrdering(const TMap<TString, TGroupDescription>& groups) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    Cerr << ">>>>> Phase 1: Create topic with autoscaling and shared consumer with KeepMessageOrder(true)" << Endl;

    Cerr << ">>>>> groups:\n";
    for (const auto& [g, desc] : groups) {
        Cerr << "  " << g << ": "
             << LabeledOutput(desc.SizeBeforeSplit, desc.SizeAfterSplit,
                              desc.ReadBeforeSplit.ReadCount, desc.ReadBeforeSplit.CommitLast) << "\n";
    }
    Cerr << Endl;

    auto status = CreateTopic(setup, "/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
        .BeginConfigurePartitioningSettings()
            .MinActivePartitions(1)
            .MaxActivePartitions(10)
            .BeginConfigureAutoPartitioningSettings()
                .Strategy(EAutoPartitioningStrategy::ScaleUp)
                .UpUtilizationPercent(80)
                .DownUtilizationPercent(20)
                .StabilizationWindow(TDuration::Seconds(2))
            .EndConfigureAutoPartitioningSettings()
        .EndConfigurePartitioningSettings()
        .BeginAddSharedConsumer("mlp-consumer")
            .KeepMessagesOrder(true)
            .DefaultProcessingTimeout(TDuration::Seconds(30))
            .BeginDeadLetterPolicy()
                .Enable()
                .BeginCondition()
                    .MaxProcessingAttempts(10)
                .EndCondition()
                .DeleteAction()
            .EndDeadLetterPolicy()
        .EndAddConsumer());
    UNIT_ASSERT_VALUES_EQUAL_C(status.IsSuccess(), true, status.GetIssues().ToString());

    Cerr << ">>>>> Phase 2: Write messages to parent partition (partition 0)" << Endl;

    for (const auto& [groupId, desc] : groups) {
        if (desc.SizeBeforeSplit == 0) {
            continue;
        }

        std::vector<TWriterSettings::TMessage> messages;
        for (size_t i = 0; i < desc.SizeBeforeSplit; ++i) {
            messages.push_back({
                .Index = i,
                .MessageBody = TStringBuilder() << "msg-" << groupId << "-" << (i + 1),
                .MessageGroupId = groupId,
            });
        }

        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = std::move(messages),
        });
        auto writeResp = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(writeResp->Messages.size(), desc.SizeBeforeSplit,
            "phase 2: write before split for group " << groupId);
        for (auto& msg : writeResp->Messages) {
            UNIT_ASSERT_VALUES_EQUAL_C(msg.Status, Ydb::StatusIds::SUCCESS,
                "phase 2: write before split for group " << groupId);
        }
    }
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0});

    Cerr << ">>>>> Phase 3: Read and commit/unlock messages before split" << Endl;

    TMap<TString, TReadCommitCount> readBeforeSplit;
    for (const auto& [groupId, desc] : groups) {
        if (desc.ReadBeforeSplit.ReadCount > 0) {
            readBeforeSplit[groupId] = desc.ReadBeforeSplit;
        }
    }
    auto inflyMessages = ReadAndCommitFIFO(runtime, "/Root/topic1", "mlp-consumer", readBeforeSplit, "phase 3");
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0});

    Cerr << ">>>>> Phase 4: Force partition split" << Endl;

    ui64 txId = 1006;
    NKikimr::NPQ::NTest::SplitPartition(runtime, txId, "/Root", "topic1", 0, "\x80");

    Cerr << ">>>>> Phase 4b: Wait for child partitions to be created and verify partition count" << Endl;

    auto partitionCount = WaitForPartitionCount(setup, "/Root/topic1", 3);
    UNIT_ASSERT_VALUES_EQUAL_C(partitionCount, 3, "Expected 3 partitions after split (1 parent + 2 children)");

    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2});

    Cerr << ">>>>> Phase 5: Write messages to child partitions" << Endl;

    for (const auto& [groupId, desc] : groups) {
        if (desc.SizeAfterSplit == 0) {
            continue;
        }

        std::vector<TWriterSettings::TMessage> messages;
        for (size_t i = 0; i < desc.SizeAfterSplit; ++i) {
            messages.push_back({
                .Index = i,
                .MessageBody = TStringBuilder() << "msg-" << groupId << "-child-" << (i + 1),
                .MessageGroupId = groupId,
            });
        }

        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = std::move(messages),
        });
        auto writeResp = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(writeResp->Messages.size(), desc.SizeAfterSplit,
            "phase 5: write after split for group " << groupId);
        for (auto& msg : writeResp->Messages) {
            UNIT_ASSERT_VALUES_EQUAL_C(msg.Status, Ydb::StatusIds::SUCCESS,
                "phase 5: write after split for group " << groupId);
        }
    }

    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2});

    Cerr << ">>>>> Phase 6: Verify that we cannot read any messages written to the child partitions, if not all messages from the parent partition was commited" << Endl;

    // Determine which groups have all parent messages committed.
    // A group is "fully committed in parent" if effective CommitCount >= SizeBeforeSplit.
    // Groups that are not fully committed have child messages blocked by FIFO ordering.
    THashSet<TString> groupsFullyCommittedInParent;
    THashSet<TString> groupsWithUncommittedParent;
    for (const auto& [groupId, desc] : groups) {
        if (desc.SizeBeforeSplit == 0) {
            continue; // No parent messages for this group
        }
        const size_t effectiveCommitCount = desc.ReadBeforeSplit.ReadCount
            - (desc.ReadBeforeSplit.CommitLast ? 0 : 1);
        if (effectiveCommitCount >= desc.SizeBeforeSplit) {
            groupsFullyCommittedInParent.insert(groupId);
        } else {
            groupsWithUncommittedParent.insert(groupId);
        }
    }

    Cerr << ">>>>> Phase 6: groupsFullyCommittedInParent: " << JoinSeq(", ", groupsFullyCommittedInParent) << "\n";
    Cerr << ">>>>> Phase 6: groupsWithUncommittedParent: " << JoinSeq(", ", groupsWithUncommittedParent) << "\n";

    // Probe read: attempt to read messages and verify FIFO invariant.
    // We do a single read with MaxNumberOfMessage=10 to see what's available.
    // Due to FIFO: at most 1 message per group, and groups with inflight (locked)
    // messages from Phase 3 won't return new messages.
    //
    // Expected readable messages:
    // - For groups with all parent committed and no inflight: child messages (from child partitions)
    // - For groups with uncommitted parent and no inflight: next parent message (from partition 0)
    // - For groups with inflight messages: nothing (blocked by FIFO)
    // - For groups with SizeBeforeSplit == 0: child messages (no parent messages exist)
    {
        CreateReaderActor(runtime, {.DatabasePath = "/Root",
                                    .TopicName = "/Root/topic1",
                                    .Consumer = "mlp-consumer",
                                    .WaitTime = TDuration::Seconds(3),
                                    .ProcessingTimeout = TDuration::Seconds(30),
                                    .MaxNumberOfMessage = 10});
        auto readResult = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(readResult->Status, Ydb::StatusIds::SUCCESS, "phase 6");

        // Validate FIFO invariant: at most 1 message per group in a single read response
        THashSet<TString> seenGroups;
        for (const auto& msg : readResult->Messages) {
            UNIT_ASSERT_C(!seenGroups.contains(msg.MessageGroupId),
                "FIFO violation in Phase 6: duplicate group " << msg.MessageGroupId
                << " in single read response");
            seenGroups.insert(msg.MessageGroupId);

            Cerr << ">>>>> Phase 6 read: " << msg.Data
                 << " group=" << msg.MessageGroupId
                 << " partition=" << msg.MessageId.PartitionId
                 << " offset=" << msg.MessageId.Offset << Endl;

            // Verify that groups with uncommitted parent messages only return parent messages
            if (groupsWithUncommittedParent.contains(msg.MessageGroupId)) {
                UNIT_ASSERT_C(msg.MessageId.PartitionId == 0,
                    "FIFO ordering violation: group " << msg.MessageGroupId
                    << " has uncommitted parent messages but returned child message from partition "
                    << msg.MessageId.PartitionId);
            }
        }

        // Groups with inflight messages from Phase 3 should NOT appear in the read
        for (const auto& [groupId, messageId] : inflyMessages) {
            UNIT_ASSERT_C(!seenGroups.contains(groupId),
                "FIFO violation: group " << groupId
                << " has an inflight (locked) message but appeared in read response");
        }

        // Unlock all messages we just read (we're only probing, not consuming)
        if (!readResult->Messages.empty()) {
            std::vector<TMessageId> toUnlock;
            for (const auto& msg : readResult->Messages) {
                toUnlock.push_back(msg.MessageId);
            }
            auto unlockResult = Unlock(runtime, "/Root/topic1", "mlp-consumer", toUnlock);
            UNIT_ASSERT_VALUES_EQUAL_C(unlockResult->Status, Ydb::StatusIds::SUCCESS, "phase 6");
        }
    }

    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2});

    Cerr << ">>>>> Phase 7: Commit all remaining messages from the parent partition" << Endl;

    // First, commit any inflight messages from Phase 3
    if (!inflyMessages.empty()) {
        std::vector<TMessageId> messagesToCommit;
        for (const auto& [groupId, messageId] : inflyMessages) {
            messagesToCommit.push_back(messageId);
            Cerr << ">>>>> Phase 7: Committing inflight message from Phase 3: group=" << groupId
                 << " partition=" << messageId.PartitionId
                 << " offset=" << messageId.Offset << Endl;
        }
        auto commitResult = Commit(runtime, "/Root/topic1", "mlp-consumer", messagesToCommit);
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult->Status, Ydb::StatusIds::SUCCESS, "phase 7");
    }

    // Read and commit all remaining parent messages.
    // After Phase 3, ReadBeforeSplit.ReadCount messages were read per group.
    // The inflight messages (at most 1 per group when !CommitLast) were just committed above.
    // So we need to read (SizeBeforeSplit - ReadCount) more messages per group.
    {
        TMap<TString, TReadCommitCount> remainingParentOps;
        for (const auto& [groupId, desc] : groups) {
            size_t alreadyRead = desc.ReadBeforeSplit.ReadCount;
            size_t remaining = desc.SizeBeforeSplit - alreadyRead;
            if (remaining > 0) {
                remainingParentOps[groupId] = {.ReadCount = remaining, .CommitLast = true};
            }
        }

        if (!remainingParentOps.empty()) {
            auto phase7Uncommitted = ReadAndCommitFIFO(runtime, "/Root/topic1", "mlp-consumer", remainingParentOps, "phase 7");
            UNIT_ASSERT_C(phase7Uncommitted.empty(),
                "Phase 7: Expected all remaining parent messages to be committed, but "
                << phase7Uncommitted.size() << " groups have uncommitted messages");
        }
    }

    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2});

    Cerr << ">>>>> Phase 8: Verify that we can read all remaining messages from child partitions" << Endl;

    // Now all parent messages are committed. All child messages should be readable.
    {
        TMap<TString, TReadCommitCount> childOps;
        for (const auto& [groupId, desc] : groups) {
            if (desc.SizeAfterSplit > 0) {
                childOps[groupId] = {.ReadCount = desc.SizeAfterSplit, .CommitLast = true};
            }
        }

        if (!childOps.empty()) {
            auto phase8Uncommitted = ReadAndCommitFIFO(runtime, "/Root/topic1", "mlp-consumer", childOps, "phase 8", 1);
            UNIT_ASSERT_C(phase8Uncommitted.empty(),
                "Phase 8: Expected all child messages to be committed, but "
                << phase8Uncommitted.size() << " groups have uncommitted messages");
        }
    }

    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2});

    // Final verification: try to read more messages - should get nothing
    {
        CreateReaderActor(runtime, {.DatabasePath = "/Root",
                                    .TopicName = "/Root/topic1",
                                    .Consumer = "mlp-consumer",
                                    .WaitTime = TDuration::Seconds(3),
                                    .ProcessingTimeout = TDuration::Seconds(30),
                                    .MaxNumberOfMessage = 10});
        auto finalRead = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(finalRead->Status, Ydb::StatusIds::SUCCESS, "phase 8 final");
        UNIT_ASSERT_C(finalRead->Messages.empty(),
            "Expected no more messages, but got " << finalRead->Messages.size());
    }

    Cerr << ">>>>> All phases completed successfully" << Endl;
}

// Case 1: All written messages committed before split.
// Expected: child messages for all groups (A, B, C) are immediately available.
Y_UNIT_TEST(PartitionSplitWithMessageGroupOrdering_AllCommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 3,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    });
}

// Case 2: None of the written messages committed before split.
// Expected: child messages for all groups are blocked until parent messages are committed.
Y_UNIT_TEST(PartitionSplitWithMessageGroupOrdering_NoneCommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 0,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 0,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 0,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    });
}

// Case 3: All messages of group-A and group-B committed before split.
//         None of group-C committed before split.
// Expected: child messages for A and B available; C blocked until parent committed.
Y_UNIT_TEST(PartitionSplitWithMessageGroupOrdering_GroupAB_CommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 3,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 0,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    });
}

// Case 4: One message of each group committed before split.
// Expected: child messages for all groups are available (partial commit unblocks).
Y_UNIT_TEST(PartitionSplitWithMessageGroupOrdering_OnePerGroupCommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    });
}

// Case 5: All messages of group-A and group-B committed before split.
//         One message of group-C committed before split.
// Expected: child messages for all groups are available.
Y_UNIT_TEST(PartitionSplitWithMessageGroupOrdering_GroupAB_OneCCommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 3,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    });
}

Y_UNIT_TEST(PartitionSplitWithMessageGroupOrdering_GroupBC_OneACommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    });
}

// Sanity check: minimal test with two groups, one message each before and after split.
Y_UNIT_TEST(PartitionSplitWithMessageGroupOrdering_TwoGroups_Minimal) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 1, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-B", {.SizeBeforeSplit = 1, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
    });
}

// Sanity check: two groups before split, one new group only after split.
Y_UNIT_TEST(PartitionSplitWithMessageGroupOrdering_NewGroupAfterSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 1, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-B", {.SizeBeforeSplit = 1, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-C", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    });
}

// Sanity check: single group, uncommitted before split.
Y_UNIT_TEST(PartitionSplitWithMessageGroupOrdering_SingleGroup_Uncommitted) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 1, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1, .CommitLast = false}}},
    });
}

// Sanity check: single group, committed before split.
Y_UNIT_TEST(PartitionSplitWithMessageGroupOrdering_SingleGroup_Committed) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 1, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1, .CommitLast = true}}},
    });
}

// Sanity check: single group before split, new group after split, committed before split.
Y_UNIT_TEST(PartitionSplitWithMessageGroupOrdering_UnrelatedGroup_Committed) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 1, .SizeAfterSplit = 0, .ReadBeforeSplit = {.ReadCount = 1, .CommitLast = true}}},
        {"group-B", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    });
}


}

} // namespace NKikimr::NPQ::NMLP
