#include "mlp_storage.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/iterator/iterate_values.h>
#include <util/string/split.h>
#include <span>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPConsumerFIFOWithSplit) {

static void CreateSetupFIFOTopic(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName) {
    auto status = CreateTopic(setup, topicName, NYdb::NTopic::TCreateTopicSettings()
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
}

static size_t WaitForPartitionCount(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName, size_t expectedCount, size_t maxRetries = 300) {
    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    size_t partitionCount = 0;
    for (size_t i = 0; i < maxRetries; ++i) {
        Sleep(TDuration::MilliSeconds(100));
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
                                    .ProcessingTimeout = TDuration::Seconds(300),
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

void PartitionSplitWithMessageGroupOrdering(const TMap<TString, TGroupDescription>& groups, const std::span<const ui32> partitionsToRestart = {}) {
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
    CreateSetupFIFOTopic(setup, "/Root/topic1");

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
    NKikimr::NPQ::NTest::SplitPartition(runtime, ++txId, "/Root", "topic1", 0, "\x80");

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

    for (ui32 partitionId : partitionsToRestart) {
        Cerr << ">>>>> Restart partition " << partitionId << Endl;
        ReloadPQTablet(setup, "/Root", "/Root/topic1", partitionId);
    }

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
            auto phase7Uncommitted = ReadAndCommitFIFO(runtime, "/Root/topic1", "mlp-consumer", remainingParentOps, "phase 7", 3);
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
            auto phase8Uncommitted = ReadAndCommitFIFO(runtime, "/Root/topic1", "mlp-consumer", childOps, "phase 8", 3);
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
Y_UNIT_TEST(Order_AllCommittedBeforeSplit) {
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
Y_UNIT_TEST(Order_NoneCommittedBeforeSplit) {
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
Y_UNIT_TEST(Order_GroupAB_CommittedBeforeSplit) {
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
Y_UNIT_TEST(Order_OnePerGroupCommittedBeforeSplit) {
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
Y_UNIT_TEST(Order_GroupAB_OneCCommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 3,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    });
}

Y_UNIT_TEST(Order_GroupBC_OneACommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    });
}

Y_UNIT_TEST(Order_AllCommittedBeforeSplit_RestartParentPartition) {
    static constexpr ui32 restartPartitions[] = {0};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 3,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_AllCommittedBeforeSplit_RestartChildPartition) {
    static constexpr ui32 restartPartitions[] = {1};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 3,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_NoneCommittedBeforeSplit_RestartParentPartition) {
    static constexpr ui32 restartPartitions[] = {0};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 0,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 0,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 0,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_NoneCommittedBeforeSplit_RestartChildPartition) {
    static constexpr ui32 restartPartitions[] = {1};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 0,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 0,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 0,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_GroupAB_CommittedBeforeSplit_RestartParentPartition) {
    static constexpr ui32 restartPartitions[] = {0};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 3,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 0,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_GroupAB_CommittedBeforeSplit_RestartChildPartition) {
    static constexpr ui32 restartPartitions[] = {1};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 3,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 0,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_GroupBC_OneACommittedBeforeSplit_RestartParentPartition) {
    static constexpr ui32 restartPartitions[] = {0};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_GroupBC_OneACommittedBeforeSplit_RestartChildPartition) {
    static constexpr ui32 restartPartitions[] = {1};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 3, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-B", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-C", {.SizeBeforeSplit = 2, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 2,}}},
        {"group-D", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
        {"group-E", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    }, restartPartitions);
}

// Sanity check: minimal test with two groups, one message each before and after split.
Y_UNIT_TEST(Order_TwoGroups_Minimal) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 1, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-B", {.SizeBeforeSplit = 1, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
    });
}

// Sanity check: two groups before split, one new group only after split.
Y_UNIT_TEST(Order_NewGroupAfterSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 1, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-B", {.SizeBeforeSplit = 1, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1,}}},
        {"group-C", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    });
}

// Sanity check: single group, uncommitted before split.
Y_UNIT_TEST(Order_SingleGroup_Uncommitted) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 1, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1, .CommitLast = false}}},
    });
}

// Sanity check: single group, committed before split.
Y_UNIT_TEST(Order_SingleGroup_Committed) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 1, .SizeAfterSplit = 1, .ReadBeforeSplit = {.ReadCount = 1, .CommitLast = true}}},
    });
}

// Sanity check: single group before split, new group after split, committed before split.
Y_UNIT_TEST(Order_UnrelatedGroup_Committed) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.SizeBeforeSplit = 1, .SizeAfterSplit = 0, .ReadBeforeSplit = {.ReadCount = 1, .CommitLast = true}}},
        {"group-B", {.SizeBeforeSplit = 0, .SizeAfterSplit = 1}},
    });
}

// Helper: write groupCount groups x messagesPerGroup messages, starting from group index groupOffset.
// Group names: "group-{0}", "group-{1}", ...
// Message bodies: "{phaseIndex}-{messageIndex}"
// Updates expectedCounts map with the number of messages written per group.
static void WriteGroups(
    NActors::TTestActorRuntime& runtime,
    const TString& topicName,
    size_t groupCount,
    size_t messagesPerGroup,
    size_t phaseIndex,
    TMap<TString, size_t>& expectedCounts
) {
    Cerr << "Phase " << phaseIndex << " Write " << groupCount << " groups x " << messagesPerGroup << "\n";
    for (size_t g = 0; g < groupCount; ++g) {
        TString groupId = TStringBuilder() << "group-" << g;
        std::vector<TWriterSettings::TMessage> messages;
        for (size_t m = 0; m < messagesPerGroup; ++m) {
            messages.push_back({
                .Index = m,
                .MessageBody = TStringBuilder() << LeftPad(phaseIndex, 10, '0') << "-" << LeftPad(m, 10, '0'),
                .MessageGroupId = groupId,
            });
        }
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = topicName,
            .Messages = std::move(messages),
        });
        auto writeResp = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(writeResp->Messages.size(), messagesPerGroup,
            "WriteGroups: phase=" << phaseIndex << " group=" << groupId);
        for (auto& msg : writeResp->Messages) {
            UNIT_ASSERT_VALUES_EQUAL_C(msg.Status, Ydb::StatusIds::SUCCESS,
                "WriteGroups: phase=" << phaseIndex << " group=" << groupId);
        }
        expectedCounts[groupId] += messagesPerGroup;
    }
}

static size_t TotalExpectedMessages(const TMap<TString, size_t>& expectedCounts) {
    return Accumulate(IterateValues(expectedCounts), size_t(0));
}

struct TCollectedMessage {
    TString GroupId;
    TMessageId MessageId;
    TString Data;
};

// Read all available messages in a loop, committing each batch.
// Returns all collected messages in read order.
// Verifies FIFO invariant: at most 1 message per group per read batch.
static std::vector<TCollectedMessage> ReadAllAndCommit(
    NActors::TTestActorRuntime& runtime,
    const TString& topicName,
    const TString& consumer,
    TMap<TString, size_t> remainingReads,
    TStringBuf phase,
    int retries = 100
) {
    std::vector<TCollectedMessage> allMessages;
    size_t batchIndex = 0;

    for (int readIteration = 1; ; ++readIteration) {
        {
            TStringBuilder sb;
            sb << phase << " ReadAllAndCommit iteration=" << readIteration << ", retries left=" << retries << ":\n";
            for (auto& [g, n] : remainingReads) {
                sb << "  " << g << ": remaining=" << n << Endl;
            }
            Cerr << sb << Endl;
        }
        if (TotalExpectedMessages(remainingReads) == 0) {
            break;
        }

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = topicName,
            .Consumer = consumer,
            .WaitTime = TDuration::MilliSeconds(100),
            .ProcessingTimeout = TDuration::Seconds(300),
            .MaxNumberOfMessage = 20,
        });
        auto readResult = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(readResult->Status, Ydb::StatusIds::SUCCESS, phase);

        if (readResult->Messages.empty()) {
            if (--retries < 0) {
                break;
            }
            continue;
        }

        THashSet<TString> seenGroupsInBatch;
        std::vector<TMessageId> messagesToCommit;
        for (auto& msg : readResult->Messages) {
            Cerr << ">>>>> " << phase << " batch=" << batchIndex
                 << " read: " << msg.Data
                 << " group=" << msg.MessageGroupId
                 << " partition=" << msg.MessageId.PartitionId
                 << " offset=" << msg.MessageId.Offset << Endl;

            UNIT_ASSERT_C(!seenGroupsInBatch.contains(msg.MessageGroupId),
                phase << ": FIFO violation - duplicate group " << msg.MessageGroupId
                << " in batch " << batchIndex);
            seenGroupsInBatch.insert(msg.MessageGroupId);

            allMessages.push_back({
                .GroupId = msg.MessageGroupId,
                .MessageId = msg.MessageId,
                .Data = msg.Data,
            });
            messagesToCommit.push_back(msg.MessageId);

            if (remainingReads.contains(msg.MessageGroupId) && remainingReads[msg.MessageGroupId] > 0) {
                remainingReads[msg.MessageGroupId]--;
            }
        }

        auto commitResult = Commit(runtime, topicName, consumer, messagesToCommit);
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult->Status, Ydb::StatusIds::SUCCESS, phase);

        ++batchIndex;
    }

    return allMessages;
}

// Read once (single Read call), commit all returned messages, return collected messages.
static std::vector<TCollectedMessage> ReadOnceAndCommit(
    NActors::TTestActorRuntime& runtime,
    const TString& topicName,
    const TString& consumer,
    TStringBuf phase
) {
    std::vector<TCollectedMessage> collected;

    CreateReaderActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topicName,
        .Consumer = consumer,
        .WaitTime = TDuration::Seconds(3),
        .ProcessingTimeout = TDuration::Seconds(300),
        .MaxNumberOfMessage = 20,
    });
    auto readResult = GetReadResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL_C(readResult->Status, Ydb::StatusIds::SUCCESS, phase);

    if (readResult->Messages.empty()) {
        return collected;
    }

    THashSet<TString> seenGroupsInBatch;
    std::vector<TMessageId> messagesToCommit;

    for (auto& msg : readResult->Messages) {
        Cerr << ">>>>> " << phase << " ReadOnce: " << msg.Data
             << " group=" << msg.MessageGroupId
             << " partition=" << msg.MessageId.PartitionId
             << " offset=" << msg.MessageId.Offset << Endl;

        UNIT_ASSERT_C(!seenGroupsInBatch.contains(msg.MessageGroupId),
            phase << ": FIFO violation - duplicate group " << msg.MessageGroupId
            << " in ReadOnce batch");
        seenGroupsInBatch.insert(msg.MessageGroupId);

        collected.push_back({
            .GroupId = msg.MessageGroupId,
            .MessageId = msg.MessageId,
            .Data = msg.Data,
        });
        messagesToCommit.push_back(msg.MessageId);
    }

    auto commitResult = Commit(runtime, topicName, consumer, messagesToCommit);
    UNIT_ASSERT_VALUES_EQUAL_C(commitResult->Status, Ydb::StatusIds::SUCCESS, phase);

    return collected;
}

// Verify FIFO ordering invariants on collected messages:
// 1. Within each group, messages appear in write order (phase1 < phase2 < phase3, and within phase, index order)
// 2. Each group has the expected total message count
// 3. Across batches, group messages are strictly ordered (batch indices are monotonically increasing per group)
static void VerifyFIFOOrdering(
    const std::vector<TCollectedMessage>& messages,
    const TMap<TString, size_t>& expectedCountPerGroup,
    TStringBuf testName
) {
    TMap<TString, TVector<TString>> dataByGroup;
    TMap<TString, size_t> groupSize;
    for (const auto& msg : messages) {
        dataByGroup[msg.GroupId].push_back(msg.Data);
        groupSize[msg.GroupId] += 1;
    }
    for (const auto& [groupId, expectedCount] : expectedCountPerGroup) {
        UNIT_ASSERT_VALUES_EQUAL_C(groupSize.Value(groupId, 0), expectedCount, testName << ": group " << groupId);
    }
    UNIT_ASSERT_VALUES_EQUAL_C(groupSize.size(), expectedCountPerGroup.size(), testName << ": unexpected group count");

    for (const auto& [groupId, msgs] : dataByGroup) {
        for (size_t i = 1; i < msgs.size(); ++i) {
            const bool ordered = msgs[i - 1] < msgs[i - 0];
            UNIT_ASSERT_C(ordered, testName << ": group " << groupId << " " << LabeledOutput(i, msgs[i - 1], msgs[i - 0]));
        }
    }
}

// Test 1: All writes happen first, then read+commit at the end.
// Verifies all messages are read with correct FIFO ordering across two levels of splits.
Y_UNIT_TEST(DoubleSplit_ReadCommitAtEnd) {
    auto setup = CreateSetup();
    CreateSetupFIFOTopic(setup, "/Root/topic1");
    auto& runtime = setup->GetRuntime();

    TMap<TString, size_t> expectedCounts;
        Cerr << ">>>>> DoubleSplit Phase 1" << Endl;
    WriteGroups(runtime, "/Root/topic1", 5, 2, 1, expectedCounts);
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0});

    Cerr << ">>>>> DoubleSplit Phase 2: Split partition 0 at 0x80" << Endl;
    ui64 txId = 1006;
    NKikimr::NPQ::NTest::SplitPartition(runtime, ++txId, "/Root", "topic1", 0, "\x80");

    auto partitionCount = WaitForPartitionCount(setup, "/Root/topic1", 3);
    UNIT_ASSERT_VALUES_EQUAL_C(partitionCount, 3, "Expected 3 partitions after first split");

    Cerr << ">>>>> DoubleSplit Phase 2" << Endl;
    WriteGroups(runtime, "/Root/topic1", 10, 2, 2, expectedCounts);
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2});

    Cerr << ">>>>> DoubleSplit Phase 3: Split partitions" << Endl;
    NKikimr::NPQ::NTest::SplitPartitions(runtime, ++txId, "/Root", "topic1", {{1, "\x40"}, {2, "\xC0"}});
    partitionCount = WaitForPartitionCount(setup, "/Root/topic1", 7);
    UNIT_ASSERT_VALUES_EQUAL_C(partitionCount, 7, "Expected 7 partitions after double split");

    Cerr << ">>>>> DoubleSplit Phase 4" << Endl;
    WriteGroups(runtime, "/Root/topic1", 15, 2, 3, expectedCounts);
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2, 3, 4, 5, 6});

    Cerr << ">>>>> DoubleSplit_ReadCommitAtEnd: Reading and committing all messages" << Endl;
    auto allMessages = ReadAllAndCommit(runtime, "/Root/topic1", "mlp-consumer", expectedCounts, "DoubleSplit_ReadCommitAtEnd");

    Cerr << ">>>>> DoubleSplit_ReadCommitAtEnd: Verifying FIFO ordering" << Endl;
    size_t expectedTotal = TotalExpectedMessages(expectedCounts);
    UNIT_ASSERT_VALUES_EQUAL_C(allMessages.size(), expectedTotal,
        "DoubleSplit_ReadCommitAtEnd: expected " << expectedTotal << " messages but got " << allMessages.size());

    VerifyFIFOOrdering(allMessages, expectedCounts, "DoubleSplit_ReadCommitAtEnd");

}

// Test 2: Same as Test 1, but one read+commit cycle after the first write (before first split).
Y_UNIT_TEST(DoubleSplit_ReadAfterFirstWrite) {
    auto setup = CreateSetup();
    CreateSetupFIFOTopic(setup, "/Root/topic1");
    auto& runtime = setup->GetRuntime();

    TMap<TString, size_t> expectedCounts;

    Cerr << ">>>>> DoubleSplit_ReadAfterFirstWrite Phase 1: Write 5 groups x 2 messages to partition 0" << Endl;
    WriteGroups(runtime, "/Root/topic1", 5, 2, 1, expectedCounts);
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0});

    Cerr << ">>>>> DoubleSplit_ReadAfterFirstWrite: Early read + commit after first write" << Endl;
    auto earlyMessages = ReadOnceAndCommit(runtime, "/Root/topic1", "mlp-consumer",
        "DoubleSplit_ReadAfterFirstWrite_early");
    Cerr << ">>>>> DoubleSplit_ReadAfterFirstWrite: Early read got " << earlyMessages.size() << " messages" << Endl;
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0});

    Cerr << ">>>>> DoubleSplit_ReadAfterFirstWrite Phase 2: Split partition 0 at 0x80" << Endl;
    ui64 txId = 1006;
    NKikimr::NPQ::NTest::SplitPartition(runtime, ++txId, "/Root", "topic1", 0, "\x80");
    auto partitionCount = WaitForPartitionCount(setup, "/Root/topic1", 3);
    UNIT_ASSERT_VALUES_EQUAL_C(partitionCount, 3, "Expected 3 partitions after first split");

    Cerr << ">>>>> DoubleSplit_ReadAfterFirstWrite Phase 2: Write 10 groups x 3 messages" << Endl;
    WriteGroups(runtime, "/Root/topic1", 10, 3, 2, expectedCounts);
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2});

    Cerr << ">>>>> DoubleSplit_ReadAfterFirstWrite Phase 3: Split partitions" << Endl;
    NKikimr::NPQ::NTest::SplitPartitions(runtime, ++txId, "/Root", "topic1", {{1, "\x40"}, {2, "\xC0"}});
    partitionCount = WaitForPartitionCount(setup, "/Root/topic1", 7);
    UNIT_ASSERT_VALUES_EQUAL_C(partitionCount, 7, "Expected 7 partitions after double split");

    Cerr << ">>>>> DoubleSplit_ReadAfterFirstWrite Phase 3: Write 15 groups x 3 messages" << Endl;
    WriteGroups(runtime, "/Root/topic1", 15, 3, 3, expectedCounts);
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2, 3, 4, 5, 6});

    Cerr << ">>>>> DoubleSplit_ReadAfterFirstWrite: Reading and committing remaining messages" << Endl;
    TMap<TString, size_t> remainingReads = expectedCounts;
    for (const auto& msg : earlyMessages) {
        remainingReads[msg.GroupId]--;
    }
    auto remainingMessages = ReadAllAndCommit(runtime, "/Root/topic1", "mlp-consumer",
        remainingReads, "DoubleSplit_ReadAfterFirstWrite_remaining");
    std::vector<TCollectedMessage> allMessages;
    allMessages.insert(allMessages.end(), earlyMessages.begin(), earlyMessages.end());
    allMessages.insert(allMessages.end(), remainingMessages.begin(), remainingMessages.end());

    Cerr << ">>>>> DoubleSplit_ReadAfterFirstWrite: Verifying FIFO ordering ("
         << earlyMessages.size() << " early + " << remainingMessages.size() << " remaining = "
         << allMessages.size() << " total)" << Endl;

    size_t expectedTotal = TotalExpectedMessages(expectedCounts);
    UNIT_ASSERT_VALUES_EQUAL_C(allMessages.size(), expectedTotal,
        "DoubleSplit_ReadAfterFirstWrite: expected " << expectedTotal << " messages but got " << allMessages.size());

    VerifyFIFOOrdering(allMessages, expectedCounts, "DoubleSplit_ReadAfterFirstWrite");

}

}

} // namespace NKikimr::NPQ::NMLP
