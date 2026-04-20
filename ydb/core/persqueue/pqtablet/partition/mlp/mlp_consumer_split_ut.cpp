#include "mlp_storage.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/iterator/iterate_values.h>
#include <util/string/split.h>
#include <atomic>
#include <span>

namespace NKikimr::NPQ::NMLP {

namespace {

    struct TLeapTimeProvider: public ITimeProvider {
        TInstant Now() override {
            TInstant r = TInstant::Now();
            if (i64 offset = Offset.load(); offset >= 0) {
                return r + TDuration::MicroSeconds(offset);
            } else {
                return r - TDuration::MicroSeconds(-offset);
            }
        }

        void Add(TDuration duration) {
            Offset.fetch_add(duration.MicroSeconds());
        }

        std::atomic<i64> Offset = 0;
    };

    class TBufferedCerr: TNonCopyable {
    public:
        TBufferedCerr()
            : Out(Buffer)
        {
        }

        ~TBufferedCerr() noexcept(false) {
            Cerr << Buffer;
        }

        TString Buffer;
        TStringOutput Out;
    };
    template <class T>
    TBufferedCerr& operator<<(TBufferedCerr& os Y_LIFETIME_BOUND, const T& t) {
        os.Out << t;
        return os;
    }
    template <class T>
    TBufferedCerr&& operator<<(TBufferedCerr&& os Y_LIFETIME_BOUND, const T& t) {
        os.Out << t;
        return std::move(os);
    }
#define ACerr (TBufferedCerr{})
} // namespace

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
        ACerr << ">>>>> Partition count: " << partitionCount << " (expected " << expectedCount << ")" << Endl;

        if (partitionCount >= expectedCount) {
            break;
        }
    }

    driver.Stop(true);
    return partitionCount;
}

struct TLeafPartition {
    ui32 Id;
    unsigned char Lo;
    unsigned char Hi;
};

class TPartitionSplitter {
public:
    TPartitionSplitter()
        : Leaves_({{0, 0x00, 0xFF}})
        , TotalPartitions_(1)
    {}

    size_t TotalPartitions() const { return TotalPartitions_; }
    const std::vector<TLeafPartition>& Leaves() const { return Leaves_; }

    void SplitAllPartitions(NActors::TTestActorRuntime& runtime, ui64& txId, const TString& dir, const TString& topic) {
        std::map<ui32, TString> boundaries;
        for (const auto& leaf : Leaves_) {
            unsigned char mid = (unsigned char)(((unsigned)leaf.Lo + (unsigned)leaf.Hi) / 2);
            boundaries[leaf.Id] = TString(1, (char)mid);
            ACerr << ">>>>> split partition " << leaf.Id << " at 0x" << Hex((ui32)mid) << Endl;
        }
        NKikimr::NPQ::NTest::SplitPartitions(runtime, txId, dir, topic, boundaries);
        AdvanceLeaves();
    }

    void SplitLeaf(NActors::TTestActorRuntime& runtime, ui64& txId, const TString& dir, const TString& topic, ui32 leafId, TString boundary) {
        NKikimr::NPQ::NTest::SplitPartition(runtime, txId, dir, topic, leafId, std::move(boundary));
        AdvanceLeaves();
    }

private:
    void AdvanceLeaves() {
        std::vector<TLeafPartition> newLeaves;
        for (size_t i = 0; i < Leaves_.size(); ++i) {
            const auto& leaf = Leaves_[i];
            unsigned char mid = (unsigned char)(((unsigned)leaf.Lo + (unsigned)leaf.Hi) / 2);
            ui32 leftId = (ui32)(TotalPartitions_ + i * 2);
            ui32 rightId = (ui32)(TotalPartitions_ + i * 2 + 1);
            newLeaves.push_back({leftId, leaf.Lo, mid});
            newLeaves.push_back({rightId, (unsigned char)(mid + 1), leaf.Hi});
        }
        TotalPartitions_ += Leaves_.size() * 2;
        Leaves_ = std::move(newLeaves);
    }

    std::vector<TLeafPartition> Leaves_;
    size_t TotalPartitions_;
};

Y_UNIT_TEST_SUITE(TMLPConsumerFIFOWithSplit) {

static void DumpStorageState(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName, const TString& consumerName, const TSet<ui32>& partitions) {
    TStringStream output;
    Y_DEFER {
        ACerr << output.Str() << "\n";
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
    ACerr << "Commiting messages: " << JoinMessageIds(messagesToCommit) << "\n";
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
    ACerr << "Unlocking messages: " << JoinMessageIds(messagesToUnlock) << "\n";
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
            ACerr << sb << Endl;
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
            ACerr << ">>>>> " << phase << " ReadAndCommitFIFOExceptLast read: " << msg.Data
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

    ACerr << ">>>>> Phase 1: Create topic with autoscaling and shared consumer with KeepMessageOrder(true)" << Endl;

    ACerr << ">>>>> groups:\n";
    for (const auto& [g, desc] : groups) {
        ACerr << "  " << g << ": "
             << LabeledOutput(desc.SizeBeforeSplit, desc.SizeAfterSplit,
                              desc.ReadBeforeSplit.ReadCount, desc.ReadBeforeSplit.CommitLast) << "\n";
    }
    ACerr << Endl;
    CreateSetupFIFOTopic(setup, "/Root/topic1");

    ACerr << ">>>>> Phase 2: Write messages to parent partition (partition 0)" << Endl;

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

    ACerr << ">>>>> Phase 3: Read and commit/unlock messages before split" << Endl;

    TMap<TString, TReadCommitCount> readBeforeSplit;
    for (const auto& [groupId, desc] : groups) {
        if (desc.ReadBeforeSplit.ReadCount > 0) {
            readBeforeSplit[groupId] = desc.ReadBeforeSplit;
        }
    }
    auto inflyMessages = ReadAndCommitFIFO(runtime, "/Root/topic1", "mlp-consumer", readBeforeSplit, "phase 3");
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0});

    ACerr << ">>>>> Phase 4: Force partition split" << Endl;

    TPartitionSplitter splitter;
    ui64 txId = 1006;
    splitter.SplitAllPartitions(runtime, ++txId, "/Root", "topic1");

    ACerr << ">>>>> Phase 4b: Wait for child partitions to be created and verify partition count" << Endl;

    auto partitionCount = WaitForPartitionCount(setup, "/Root/topic1", splitter.TotalPartitions());
    UNIT_ASSERT_VALUES_EQUAL_C(partitionCount, splitter.TotalPartitions(), "Expected " << splitter.TotalPartitions() << " partitions after split");

    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2});

    ACerr << ">>>>> Phase 5: Write messages to child partitions" << Endl;

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
        ACerr << ">>>>> Restart partition " << partitionId << Endl;
        ReloadPQTablet(setup, "/Root", "/Root/topic1", partitionId);
    }

    ACerr << ">>>>> Phase 6: Verify that we cannot read any messages written to the child partitions, if not all messages from the parent partition was commited" << Endl;

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

    ACerr << ">>>>> Phase 6: groupsFullyCommittedInParent: " << JoinSeq(", ", groupsFullyCommittedInParent) << "\n";
    ACerr << ">>>>> Phase 6: groupsWithUncommittedParent: " << JoinSeq(", ", groupsWithUncommittedParent) << "\n";

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

            ACerr << ">>>>> Phase 6 read: " << msg.Data
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

    ACerr << ">>>>> Phase 7: Commit all remaining messages from the parent partition" << Endl;

    // First, commit any inflight messages from Phase 3
    if (!inflyMessages.empty()) {
        std::vector<TMessageId> messagesToCommit;
        for (const auto& [groupId, messageId] : inflyMessages) {
            messagesToCommit.push_back(messageId);
            ACerr << ">>>>> Phase 7: Committing inflight message from Phase 3: group=" << groupId
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

    ACerr << ">>>>> Phase 8: Verify that we can read all remaining messages from child partitions" << Endl;

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

    ACerr << ">>>>> All phases completed successfully" << Endl;
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
    ACerr << "Phase " << phaseIndex << " Write " << groupCount << " groups x " << messagesPerGroup << "\n";
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
            ACerr << sb << Endl;
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
            ACerr << ">>>>> " << phase << " batch=" << batchIndex
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
        ACerr << ">>>>> " << phase << " ReadOnce: " << msg.Data
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

    TPartitionSplitter splitter;
    TMap<TString, size_t> expectedCounts;
    ui64 txId = 1006;

    ACerr << ">>>>> DoubleSplit Phase 1" << Endl;
    WriteGroups(runtime, "/Root/topic1", 5, 2, 1, expectedCounts);
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0});

    ACerr << ">>>>> DoubleSplit Phase 2: Split all leaves" << Endl;
    splitter.SplitAllPartitions(runtime, ++txId, "/Root", "topic1");
    auto partitionCount = WaitForPartitionCount(setup, "/Root/topic1", splitter.TotalPartitions());
    UNIT_ASSERT_VALUES_EQUAL_C(partitionCount, splitter.TotalPartitions(), "Expected " << splitter.TotalPartitions() << " partitions after first split");

    ACerr << ">>>>> DoubleSplit Phase 2" << Endl;
    WriteGroups(runtime, "/Root/topic1", 10, 2, 2, expectedCounts);
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2});

    ACerr << ">>>>> DoubleSplit Phase 3: Split all leaves" << Endl;
    splitter.SplitAllPartitions(runtime, ++txId, "/Root", "topic1");
    partitionCount = WaitForPartitionCount(setup, "/Root/topic1", splitter.TotalPartitions());
    UNIT_ASSERT_VALUES_EQUAL_C(partitionCount, splitter.TotalPartitions(), "Expected " << splitter.TotalPartitions() << " partitions after double split");

    ACerr << ">>>>> DoubleSplit Phase 4" << Endl;
    WriteGroups(runtime, "/Root/topic1", 15, 2, 3, expectedCounts);
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2, 3, 4, 5, 6});

    ACerr << ">>>>> DoubleSplit_ReadCommitAtEnd: Reading and committing all messages" << Endl;
    auto allMessages = ReadAllAndCommit(runtime, "/Root/topic1", "mlp-consumer", expectedCounts, "DoubleSplit_ReadCommitAtEnd");

    ACerr << ">>>>> DoubleSplit_ReadCommitAtEnd: Verifying FIFO ordering" << Endl;
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

    TPartitionSplitter splitter;
    TMap<TString, size_t> expectedCounts;
    ui64 txId = 1006;

    ACerr << ">>>>> DoubleSplit_ReadAfterFirstWrite Phase 1: Write 5 groups x 2 messages to partition 0" << Endl;
    WriteGroups(runtime, "/Root/topic1", 5, 2, 1, expectedCounts);
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0});

    ACerr << ">>>>> DoubleSplit_ReadAfterFirstWrite: Early read + commit after first write" << Endl;
    auto earlyMessages = ReadOnceAndCommit(runtime, "/Root/topic1", "mlp-consumer",
        "DoubleSplit_ReadAfterFirstWrite_early");
    ACerr << ">>>>> DoubleSplit_ReadAfterFirstWrite: Early read got " << earlyMessages.size() << " messages" << Endl;
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0});

    ACerr << ">>>>> DoubleSplit_ReadAfterFirstWrite Phase 2: Split all leaves" << Endl;
    splitter.SplitAllPartitions(runtime, ++txId, "/Root", "topic1");
    auto partitionCount = WaitForPartitionCount(setup, "/Root/topic1", splitter.TotalPartitions());
    UNIT_ASSERT_VALUES_EQUAL_C(partitionCount, splitter.TotalPartitions(), "Expected " << splitter.TotalPartitions() << " partitions after first split");

    ACerr << ">>>>> DoubleSplit_ReadAfterFirstWrite Phase 2: Write 10 groups x 3 messages" << Endl;
    WriteGroups(runtime, "/Root/topic1", 10, 3, 2, expectedCounts);
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2});

    ACerr << ">>>>> DoubleSplit_ReadAfterFirstWrite Phase 3: Split all leaves" << Endl;
    splitter.SplitAllPartitions(runtime, ++txId, "/Root", "topic1");
    partitionCount = WaitForPartitionCount(setup, "/Root/topic1", splitter.TotalPartitions());
    UNIT_ASSERT_VALUES_EQUAL_C(partitionCount, splitter.TotalPartitions(), "Expected " << splitter.TotalPartitions() << " partitions after double split");

    ACerr << ">>>>> DoubleSplit_ReadAfterFirstWrite Phase 3: Write 15 groups x 3 messages" << Endl;
    WriteGroups(runtime, "/Root/topic1", 15, 3, 3, expectedCounts);
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", {0, 1, 2, 3, 4, 5, 6});

    ACerr << ">>>>> DoubleSplit_ReadAfterFirstWrite: Reading and committing remaining messages" << Endl;
    TMap<TString, size_t> remainingReads = expectedCounts;
    for (const auto& msg : earlyMessages) {
        remainingReads[msg.GroupId]--;
    }
    auto remainingMessages = ReadAllAndCommit(runtime, "/Root/topic1", "mlp-consumer",
        remainingReads, "DoubleSplit_ReadAfterFirstWrite_remaining");
    std::vector<TCollectedMessage> allMessages;
    allMessages.insert(allMessages.end(), earlyMessages.begin(), earlyMessages.end());
    allMessages.insert(allMessages.end(), remainingMessages.begin(), remainingMessages.end());

    ACerr << ">>>>> DoubleSplit_ReadAfterFirstWrite: Verifying FIFO ordering ("
         << earlyMessages.size() << " early + " << remainingMessages.size() << " remaining = "
         << allMessages.size() << " total)" << Endl;

    size_t expectedTotal = TotalExpectedMessages(expectedCounts);
    UNIT_ASSERT_VALUES_EQUAL_C(allMessages.size(), expectedTotal,
        "DoubleSplit_ReadAfterFirstWrite: expected " << expectedTotal << " messages but got " << allMessages.size());

    VerifyFIFOOrdering(allMessages, expectedCounts, "DoubleSplit_ReadAfterFirstWrite");

}

}

struct TDedupWriteSpec {
    TString Body;
    std::optional<TString> DedupId;
    std::optional<TString> GroupId;
};

struct TWriteResult {
    Ydb::StatusIds::StatusCode Status;
    TMessageId MessageId;
    size_t StageIdx = 0;
    TDuration WriteTimestampOffset;
};

Y_UNIT_TEST_SUITE(TMLPConsumerFIFOWithSplitDedup) {

static constexpr TDuration DEDUP_WINDOW = TDuration::Minutes(5);

struct TDedupStage {
    std::vector<TDedupWriteSpec> Write;
};

static TMap<TString, TString> ComputeExpectedMessages(const std::vector<TDedupStage>& stages) {
    TMap<TString, TString> expected;
    TSet<TString> seenDedupIds;
    for (const auto& stage : stages) {
        for (const auto& msg : stage.Write) {
            if (msg.DedupId) {
                if (seenDedupIds.insert(*msg.DedupId).second) {
                    expected[msg.Body] = *msg.DedupId;
                }
            } else {
                expected[msg.Body] = "";
            }
        }
    }
    return expected;
}

static TMap<TString, TWriteResult> WriteDedupStage(
    NActors::TTestActorRuntime& runtime,
    const TString& topicName,
    std::vector<TDedupWriteSpec> messages,
    TStringBuf phase,
    size_t stage,
    TDuration elapsed
) {
    TMap<TString, TWriteResult> bodyStatuses;
    for (int writeIteration = 0; !messages.empty(); ++writeIteration) {
        TSet<TString> usedGroups;
        TSet<TString> usedDedupIds;
        std::vector<TDedupWriteSpec> messagesNext;
        std::vector<TWriterSettings::TMessage> writerMessages;

        for (auto& m : messages) {
            bool use = true; // write messages in non-overlapping groups
            use = use && (!m.DedupId.has_value() || usedDedupIds.insert(m.DedupId.value()).second);
            use = use && (!m.GroupId.has_value() || usedGroups.insert(m.GroupId.value()).second);
            if (use) {
                writerMessages.push_back({
                    .Index = writerMessages.size(),
                    .MessageBody = m.Body,
                    .MessageGroupId = m.GroupId,
                    .MessageDeduplicationId = m.DedupId,
                });
            } else {
                messagesNext.push_back(std::move(m));
            }
        }
        ACerr << "Write iteration " << writeIteration << ": " << writerMessages.size() << " messages:" << Endl;
        for (const auto& m : writerMessages) {
            ACerr << "  " << LabeledOutput(m.Index, m.MessageBody, m.MessageGroupId, m.MessageDeduplicationId) << Endl;
        }

        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = topicName,
            .Messages = writerMessages,
        });
        auto writeResp = GetWriteResponse(runtime);
        UNIT_ASSERT_C(writeResp, phase << ": write response timeout");
        ACerr << "Write iteration " << writeIteration << " response: " << writeResp->Messages.size() << " messages:" << Endl;
        for (size_t i = 0; i < writeResp->Messages.size(); ++i) {
            auto toString = [](const std::optional<TMessageId>& id) -> TString {
                if (id.has_value()) {
                    return TStringBuilder() << LabeledOutput(id->PartitionId, id->Offset);
                }
                return "<none>";
            };
            ACerr << "  " << LabeledOutput(i, writeResp->Messages[i].Index, writeResp->Messages[i].Status) << " " << toString(writeResp->Messages[i].MessageId) << Endl;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(writeResp->Messages.size(), writerMessages.size(), phase);

        for (size_t i = 0; i < writeResp->Messages.size(); ++i) {
            const auto& body = writerMessages[i].MessageBody;
            const auto& resp = writeResp->Messages[i];
            UNIT_ASSERT_C(
                resp.Status == Ydb::StatusIds::SUCCESS || resp.Status == Ydb::StatusIds::ALREADY_EXISTS,
                phase << ": unexpected write status " << Ydb::StatusIds::StatusCode_Name(resp.Status)
                      << " for body=" << body);
            UNIT_ASSERT_C(!bodyStatuses.contains(body), phase << ": duplicate body in stage: " << body);
            UNIT_ASSERT_C(resp.MessageId.has_value(), phase << ": no message id for body=" << body);
            bodyStatuses[body] = {.Status = resp.Status, .MessageId = resp.MessageId.value(), .StageIdx = stage, .WriteTimestampOffset = elapsed};
        }

        messagesNext.swap(messages);
    }

    return bodyStatuses;
}

static TMap<TString, TString> ReadAllDedupAndCommit(
    NActors::TTestActorRuntime& runtime,
    const TString& topicName,
    const TString& consumer,
    int retries = 100
) {
    TMap<TString, TString> readMessages;

    for (;;) {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = topicName,
            .Consumer = consumer,
            .WaitTime = TDuration::MilliSeconds(100),
            .ProcessingTimeout = TDuration::Seconds(300),
            .MaxNumberOfMessage = 20,
        });
        auto readResult = GetReadResponse(runtime);
        UNIT_ASSERT_C(readResult, "ReadAllDedupAndCommit: read response timeout");
        UNIT_ASSERT_VALUES_EQUAL(readResult->Status, Ydb::StatusIds::SUCCESS);

        if (readResult->Messages.empty()) {
            if (--retries < 0) {
                break;
            }
            continue;
        }

        std::vector<TMessageId> toCommit;
        for (const auto& msg : readResult->Messages) {
            ACerr << ">>>>> ReadAllDedupAndCommit: data=" << msg.Data
                 << " dedupId=" << msg.MessageDeduplicationId
                 << " group=" << msg.MessageGroupId
                 << " partition=" << msg.MessageId.PartitionId
                 << " offset=" << msg.MessageId.Offset << Endl;
            readMessages[msg.Data] =  msg.MessageDeduplicationId;
            toCommit.push_back(msg.MessageId);
        }

        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = topicName,
            .Consumer = consumer,
            .Messages = toCommit,
        });
        auto commitResult = GetChangeResponse(runtime);
        UNIT_ASSERT_C(commitResult, "ReadAllDedupAndCommit: commit response timeout");
        UNIT_ASSERT_VALUES_EQUAL(commitResult->Status, Ydb::StatusIds::SUCCESS);
    }

    return readMessages;
}

static void VerifyDedupResult(
    const TMap<TString, TString>& readMessages,
    const TMap<TString, TString>& expectedMessages,
    TStringBuf testName
) {
    for (const auto& [body, dedupId] : readMessages) {
        const auto* expectedDedupId = expectedMessages.FindPtr(body);
        UNIT_ASSERT_C(expectedDedupId,
            testName << ": unexpected body=" << body << " in read result");
        UNIT_ASSERT_VALUES_EQUAL_C(
            dedupId, *expectedDedupId,
            testName << ": dedupId mismatch for body=" << body);
    }
    UNIT_ASSERT_VALUES_EQUAL_C(readMessages.size(), expectedMessages.size(), testName << ": total message count mismatch");

}

static void VerifyWriteStatuses(
    const TMap<TString, TWriteResult>& bodyStatuses,
    const std::vector<TDedupWriteSpec>& messages,
    TMap<TString, TVector<TWriteResult>>& historyByDedupId,  // in/out: first successful MessageId per dedupId
    TStringBuf phase
) {
    for (const auto& spec : messages) {
        const auto* result = bodyStatuses.FindPtr(spec.Body);
        UNIT_ASSERT_C(result, phase << ": no write status recorded for body=" << spec.Body);
        if (spec.DedupId) {
            TBufferedCerr log;
            log << "spec=" << spec << "\n";
            log << "this=" << *result << "\n";
            auto& history = historyByDedupId[*spec.DedupId];
            log << "history=[" << JoinSeq(", ", history) << "]\n";
            auto lastSuccessIt = std::ranges::find(history.rbegin(), history.rend(), Ydb::StatusIds::SUCCESS, &TWriteResult::Status);
            bool firstWrite = (lastSuccessIt == history.rend());
            TMaybe<TDuration> sinceLastSuccess = lastSuccessIt == history.rend() ? Nothing() : MakeMaybe(result->WriteTimestampOffset - lastSuccessIt->WriteTimestampOffset);
            TMaybe<TDuration> sinceLastAttempt = history.empty() ? Nothing() : MakeMaybe(result->WriteTimestampOffset - history.back().WriteTimestampOffset);
            bool lastSuccessExpired = sinceLastSuccess.Defined() && (*sinceLastSuccess > DEDUP_WINDOW);
            bool lastAttemptRecent = sinceLastAttempt.Defined() && (*sinceLastAttempt <= DEDUP_WINDOW);
            int indexFromLast = lastSuccessIt - history.rbegin();
            // When last SUCCESS expired but there were recent ALREADY_EXISTS attempts, the outcome is ambiguous — skip assertion.
            bool ambiguous = lastSuccessExpired && lastAttemptRecent;
            log << LabeledOutput(firstWrite, lastSuccessExpired, lastAttemptRecent, ambiguous, indexFromLast) << "\n";
            log << LabeledOutput(sinceLastSuccess, sinceLastAttempt) << "\n";
            if (!ambiguous) {
                bool expectedWrite = firstWrite || lastSuccessExpired;
                UNIT_ASSERT_VALUES_EQUAL_C(result->Status, (expectedWrite ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::ALREADY_EXISTS), phase << ": unexpected status for body=" << spec.Body << " dedupId=" << *spec.DedupId << "\n" << log.Buffer);
            }
            history.push_back(*result);
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, phase << ": expected SUCCESS for body=" << spec.Body << " dedupId=" << spec.DedupId.value_or("<none>"));
        }
    }
}

static void RunDedupTest(const TString& testName, const std::vector<TDedupStage>& stages, TDuration writeDuration = TDuration::Zero()) {
    UNIT_ASSERT_C(!stages.empty(), "RunDedupTest: no stages");
    const auto leapTimeProvider = MakeIntrusive<TLeapTimeProvider>();
    auto setup = CreateSetup();
    setup->GetServer().EnableLogs({
            NKikimrServices::PERSQUEUE,
        },
        NActors::NLog::PRI_DEBUG
    );
    auto& runtime = setup->GetRuntime();
    for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
        runtime.GetAppData(i).TimeProvider = leapTimeProvider;
    }
    CreateSetupFIFOTopic(setup, "/Root/topic1");
    TPartitionSplitter splitter;
    TMap<TString, TVector<TWriteResult>> messagesByDedupId;
    TDuration elapsed;
    for (size_t stageIdx = 0; stageIdx < stages.size(); ++stageIdx) {
        TStringBuilder phase;
        phase << testName << " stage=" << stageIdx;

        ACerr << ">>>>> " << phase << ": writing " << stages[stageIdx].Write.size() << " messages (elapsed=" << elapsed << ")" << Endl;
        auto bodyStatuses = WriteDedupStage(runtime, "/Root/topic1", stages[stageIdx].Write, phase, stageIdx, elapsed);
        VerifyWriteStatuses(bodyStatuses, stages[stageIdx].Write, messagesByDedupId, phase);

        leapTimeProvider->Add(writeDuration);
        elapsed += writeDuration;

        if (stageIdx + 1 < stages.size()) {
            ui64 txId = 1006 + stageIdx;
            splitter.SplitAllPartitions(runtime, txId, "/Root", "topic1");
            auto partitionCount = WaitForPartitionCount(setup, "/Root/topic1", splitter.TotalPartitions());
            UNIT_ASSERT_VALUES_EQUAL_C(partitionCount, splitter.TotalPartitions(), phase << ": expected " << splitter.TotalPartitions() << " partitions after split");
        }
    }

    ACerr << ">>>>> " << testName << ": reading all messages" << Endl;
    auto readMessages = ReadAllDedupAndCommit(runtime, "/Root/topic1", "mlp-consumer");
    if (writeDuration == TDuration::Zero()) {
        const auto expectedMessages = ComputeExpectedMessages(stages);
        VerifyDedupResult(readMessages, expectedMessages, testName);
    }
    ACerr << ">>>>> " << testName << ": done" << Endl;
}

Y_UNIT_TEST(Dedup_NoSplit_WithGroup) {
    RunDedupTest("Dedup_NoSplit_WithGroup", {
        {.Write = {
            {.Body = "first-body", .DedupId = "dedupA", .GroupId = "group-X"},
            {.Body = "second-body", .DedupId = "dedupA", .GroupId = "group-X"},
        }},
    });
}

Y_UNIT_TEST(Dedup_NoSplit_NoGroup) {
    RunDedupTest("Dedup_NoSplit_NoGroup", {
        {.Write = {
            {.Body = "first-body", .DedupId = "dedupA"},
            {.Body = "second-body", .DedupId = "dedupA"},
        }},
    });
}

Y_UNIT_TEST(Dedup_WithGroup_AllNew) {
    RunDedupTest("Dedup_WithGroup_AllNew", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-X"},
            {.Body = "s0-dedupB", .DedupId = "dedupB", .GroupId = "group-X"},
        }},
        {.Write = {
            {.Body = "s1-dedupC", .DedupId = "dedupC", .GroupId = "group-X"},
            {.Body = "s1-dedupD", .DedupId = "dedupD", .GroupId = "group-X"},
        }},
    });
}

Y_UNIT_TEST(Dedup_WithGroup_SameIds1) {
    RunDedupTest("Dedup_WithGroup_SameIds", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-X"},
        }},
        {.Write = {
            {.Body = "s1-dedupA", .DedupId = "dedupA", .GroupId = "group-X"},
        }},
    });
}

Y_UNIT_TEST(Dedup_WithGroup_SameIds) {
    RunDedupTest("Dedup_WithGroup_SameIds", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-X"},
            {.Body = "s0-dedupB", .DedupId = "dedupB", .GroupId = "group-X"},
        }},
        {.Write = {
            {.Body = "s1-dedupA", .DedupId = "dedupA", .GroupId = "group-X"},
            {.Body = "s1-dedupB", .DedupId = "dedupB", .GroupId = "group-X"},
        }},
    });
}

Y_UNIT_TEST(Dedup_WithGroup_PartialOverlap) {
    RunDedupTest("Dedup_WithGroup_PartialOverlap", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-X"},
            {.Body = "s0-dedupB", .DedupId = "dedupB", .GroupId = "group-X"},
        }},
        {.Write = {
            {.Body = "s1-dedupC", .DedupId = "dedupC", .GroupId = "group-X"},
            {.Body = "s1-dedupA", .DedupId = "dedupA", .GroupId = "group-X"},
        }},
    });
}

Y_UNIT_TEST(Dedup_WithGroup_PartialOverlapWithNonDedup) {
    std::vector<TDedupStage> stages;
    auto& s0 = stages.emplace_back();
    for (size_t i = 0; i < 5; ++i) {
        s0.Write.push_back({.Body = TStringBuilder() << "s0-nodedup-" << i, .GroupId = "group-X"});
    }
    s0.Write.push_back({.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-X"});
    s0.Write.push_back({.Body = "s0-dedupB", .DedupId = "dedupB", .GroupId = "group-X"});

    auto& s1 = stages.emplace_back();
    for (size_t i = 0; i < 5; ++i) {
        s1.Write.push_back({.Body = TStringBuilder() << "s1-nodedup-" << i, .GroupId = "group-X"});
    }
    s1.Write.push_back({.Body = "s1-dedupC", .DedupId = "dedupC", .GroupId = "group-X"});
    s1.Write.push_back({.Body = "s1-dedupA", .DedupId = "dedupA", .GroupId = "group-X"});

    RunDedupTest("Dedup_WithGroup_PartialOverlapWithNonDedup", stages);
}

Y_UNIT_TEST(Dedup_NoGroup_AllNew) {
    RunDedupTest("Dedup_NoGroup_AllNew", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA"},
            {.Body = "s0-dedupB", .DedupId = "dedupB"},
        }},
        {.Write = {
            {.Body = "s1-dedupC", .DedupId = "dedupC"},
            {.Body = "s1-dedupD", .DedupId = "dedupD"},
        }},
    });
}

Y_UNIT_TEST(Dedup_NoGroup_SameIds) {
    RunDedupTest("Dedup_NoGroup_SameIds", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA"},
            {.Body = "s0-dedupB", .DedupId = "dedupB"},
        }},
        {.Write = {
            {.Body = "s1-dedupA", .DedupId = "dedupA"},
            {.Body = "s1-dedupB", .DedupId = "dedupB"},
        }},
    });
}

Y_UNIT_TEST(Dedup_NoGroup_PartialOverlap) {
    RunDedupTest("Dedup_NoGroup_PartialOverlap", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA"},
            {.Body = "s0-dedupB", .DedupId = "dedupB"},
        }},
        {.Write = {
            {.Body = "s1-dedupC", .DedupId = "dedupC"},
            {.Body = "s1-dedupA", .DedupId = "dedupA"},
        }},
    });
}

Y_UNIT_TEST(Dedup_NoGroup_PartialOverlapWithNonDedup) {
    std::vector<TDedupStage> stages;
    auto& s0 = stages.emplace_back();
    for (size_t i = 0; i < 5; ++i) {
        s0.Write.push_back({.Body = TStringBuilder() << "s0-nodedup-" << i});
    }
    s0.Write.push_back({.Body = "s0-dedupA", .DedupId = "dedupA"});
    s0.Write.push_back({.Body = "s0-dedupB", .DedupId = "dedupB"});

    auto& s1 = stages.emplace_back();
    for (size_t i = 0; i < 5; ++i) {
        s1.Write.push_back({.Body = TStringBuilder() << "s1-nodedup-" << i});
    }
    s1.Write.push_back({.Body = "s1-dedupC", .DedupId = "dedupC"});
    s1.Write.push_back({.Body = "s1-dedupA", .DedupId = "dedupA"});

    RunDedupTest("Dedup_NoGroup_PartialOverlapWithNonDedup", stages);
}

Y_UNIT_TEST(Dedup_MixedGroup_PartialOverlap) {
    RunDedupTest("Dedup_MixedGroup_PartialOverlap", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-X"},
            {.Body = "s0-dedupB", .DedupId = "dedupB"},
        }},
        {.Write = {
            {.Body = "s1-dedupC", .DedupId = "dedupC", .GroupId = "group-X"},
            {.Body = "s1-dedupA", .DedupId = "dedupA"},
        }},
    });
}

Y_UNIT_TEST(Dedup_DoubleSplit) {
    RunDedupTest("Dedup_DoubleSplit", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s0-dedupB", .DedupId = "dedupB", .GroupId = "group-B"},
            {.Body = "s0-dedupC", .DedupId = "dedupC", .GroupId = "group-C"},
        }},
        {.Write = {
            {.Body = "s1-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s1-dedupB", .DedupId = "dedupB", .GroupId = "group-B"},
            {.Body = "s1-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
        }},
        {.Write = {
            {.Body = "s2-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s2-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
            {.Body = "s2-dedupF", .DedupId = "dedupF", .GroupId = "group-F"},
        }},
    });
}

Y_UNIT_TEST(Dedup_DoubleSplit_3min) {
    RunDedupTest("Dedup_DoubleSplit_3min", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s0-dedupB", .DedupId = "dedupB", .GroupId = "group-B"},
            {.Body = "s0-dedupC", .DedupId = "dedupC", .GroupId = "group-C"},
        }},
        {.Write = {
            {.Body = "s1-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s1-dedupB", .DedupId = "dedupB", .GroupId = "group-B"},
            {.Body = "s1-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
        }},
        {.Write = {
            {.Body = "s2-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s2-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
            {.Body = "s2-dedupF", .DedupId = "dedupF", .GroupId = "group-F"},
        }},
    }, TDuration::Minutes(3));
}

Y_UNIT_TEST(Dedup_DoubleSplit_6min) {
    RunDedupTest("Dedup_DoubleSplit_6min", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s0-dedupB", .DedupId = "dedupB", .GroupId = "group-B"},
            {.Body = "s0-dedupC", .DedupId = "dedupC", .GroupId = "group-C"},
        }},
        {.Write = {
            {.Body = "s1-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s1-dedupB", .DedupId = "dedupB", .GroupId = "group-B"},
            {.Body = "s1-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
        }},
        {.Write = {
            {.Body = "s2-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s2-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
            {.Body = "s2-dedupF", .DedupId = "dedupF", .GroupId = "group-F"},
        }},
    }, TDuration::Minutes(6));
}

Y_UNIT_TEST(Dedup_WithGroup_SameIds_3min) {
    RunDedupTest("Dedup_WithGroup_SameIds_3min", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-X"},
            {.Body = "s0-dedupB", .DedupId = "dedupB", .GroupId = "group-X"},
        }},
        {.Write = {
            {.Body = "s1-dedupA", .DedupId = "dedupA", .GroupId = "group-X"},
            {.Body = "s1-dedupB", .DedupId = "dedupB", .GroupId = "group-X"},
        }},
    }, TDuration::Minutes(3));
}

Y_UNIT_TEST(Dedup_WithGroup_SameIds_6min) {
    RunDedupTest("Dedup_WithGroup_SameIds_6min", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-X"},
            {.Body = "s0-dedupB", .DedupId = "dedupB", .GroupId = "group-X"},
        }},
        {.Write = {
            {.Body = "s1-dedupA", .DedupId = "dedupA", .GroupId = "group-X"},
            {.Body = "s1-dedupB", .DedupId = "dedupB", .GroupId = "group-X"},
        }},
    }, TDuration::Minutes(6));
}

Y_UNIT_TEST(Dedup_TripleSplit) {
    RunDedupTest("Dedup_TripleSplit", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s0-dedupB", .DedupId = "dedupB", .GroupId = "group-B"},
            {.Body = "s0-dedupC", .DedupId = "dedupC", .GroupId = "group-C"},
            {.Body = "s0-dedupI", .DedupId = "dedupI", .GroupId = "group-I"},
        }},
        {.Write = {
            {.Body = "s1-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s1-dedupB", .DedupId = "dedupB", .GroupId = "group-B"},
            {.Body = "s1-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
            {.Body = "s0-dedupG", .DedupId = "dedupG", .GroupId = "group-G"},
        }},
        {.Write = {
            {.Body = "s2-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s2-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
            {.Body = "s2-dedupF", .DedupId = "dedupF", .GroupId = "group-F"},
            {.Body = "s0-dedupC", .DedupId = "dedupC", .GroupId = "group-C"},
        }},
        {.Write = {
            {.Body = "s3-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s3-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
            {.Body = "s3-dedupF", .DedupId = "dedupF", .GroupId = "group-F"},
            {.Body = "s3-dedupG", .DedupId = "dedupG", .GroupId = "group-G"},
            {.Body = "s3-dedupH", .DedupId = "dedupH", .GroupId = "group-H"},
        }},
    });
}

Y_UNIT_TEST(Dedup_TripleSplit_3min) {
    RunDedupTest("Dedup_TripleSplit_3min", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s0-dedupB", .DedupId = "dedupB", .GroupId = "group-B"},
            {.Body = "s0-dedupC", .DedupId = "dedupC", .GroupId = "group-C"},
            {.Body = "s0-dedupI", .DedupId = "dedupI", .GroupId = "group-I"},
        }},
        {.Write = {
            {.Body = "s1-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s1-dedupB", .DedupId = "dedupB", .GroupId = "group-B"},
            {.Body = "s1-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
            {.Body = "s0-dedupG", .DedupId = "dedupG", .GroupId = "group-G"},
        }},
        {.Write = {
            {.Body = "s2-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s2-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
            {.Body = "s2-dedupF", .DedupId = "dedupF", .GroupId = "group-F"},
            {.Body = "s0-dedupC", .DedupId = "dedupC", .GroupId = "group-C"},
        }},
        {.Write = {
            {.Body = "s3-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s3-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
            {.Body = "s3-dedupF", .DedupId = "dedupF", .GroupId = "group-F"},
            {.Body = "s3-dedupG", .DedupId = "dedupG", .GroupId = "group-G"},
            {.Body = "s3-dedupH", .DedupId = "dedupH", .GroupId = "group-H"},
        }},
    }, TDuration::Minutes(3));
}

Y_UNIT_TEST(Dedup_TripleSplit_6min) {
    RunDedupTest("Dedup_TripleSplit_6min", {
        {.Write = {
            {.Body = "s0-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s0-dedupB", .DedupId = "dedupB", .GroupId = "group-B"},
            {.Body = "s0-dedupC", .DedupId = "dedupC", .GroupId = "group-C"},
            {.Body = "s0-dedupI", .DedupId = "dedupI", .GroupId = "group-I"},
        }},
        {.Write = {
            {.Body = "s1-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s1-dedupB", .DedupId = "dedupB", .GroupId = "group-B"},
            {.Body = "s1-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
            {.Body = "s0-dedupG", .DedupId = "dedupG", .GroupId = "group-G"},
        }},
        {.Write = {
            {.Body = "s2-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s2-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
            {.Body = "s2-dedupF", .DedupId = "dedupF", .GroupId = "group-F"},
            {.Body = "s0-dedupC", .DedupId = "dedupC", .GroupId = "group-C"},
        }},
        {.Write = {
            {.Body = "s3-dedupA", .DedupId = "dedupA", .GroupId = "group-A"},
            {.Body = "s3-dedupE", .DedupId = "dedupE", .GroupId = "group-E"},
            {.Body = "s3-dedupF", .DedupId = "dedupF", .GroupId = "group-F"},
            {.Body = "s3-dedupG", .DedupId = "dedupG", .GroupId = "group-G"},
            {.Body = "s3-dedupH", .DedupId = "dedupH", .GroupId = "group-H"},
        }},
    }, TDuration::Minutes(6));
}

struct TSplitSpec {
    size_t Count = 1;
    TDuration PauseBetween = TDuration::Zero();
};

static void RunRepeatedDedupWithSplits(TStringBuf testName, TDuration writePause, size_t numIterations, TSplitSpec splitSpec = {}) {
    const TString groupId = "group-X";
    const TString dedupId = "dedupA";

    const auto leapTimeProvider = MakeIntrusive<TLeapTimeProvider>();
    auto setup = CreateSetup();
    setup->GetServer().EnableLogs({NKikimrServices::PERSQUEUE}, NActors::NLog::PRI_DEBUG);
    auto& runtime = setup->GetRuntime();
    for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
        runtime.GetAppData(i).TimeProvider = leapTimeProvider;
    }
    CreateSetupFIFOTopic(setup, "/Root/topic1");

    TPartitionSplitter splitter;
    ui64 txId = 1006;
    size_t bodyCounter = 0;
    TDuration elapsed;

    for (size_t iter = 0; iter < numIterations; ++iter) {
        TStringBuilder phase;
        phase << testName << " iteration=" << iter;
        ACerr << ">>>>> " << phase << " start (elapsed=" << elapsed << ")" << Endl;

        int gotSuccess = 0;
        int gotAlreadyExists = 0;
        while (!(gotSuccess == 1 && gotAlreadyExists >= 1)) {
            TString body = TStringBuilder() << "body-" << bodyCounter++;
            ACerr << ">>>>> " << phase << " write body=" << body << " elapsed=" << elapsed << Endl;

            CreateWriterActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Messages = {{.Index = 0, .MessageBody = body, .MessageGroupId = groupId, .MessageDeduplicationId = dedupId}},
            });
            auto writeResp = GetWriteResponse(runtime);
            UNIT_ASSERT_C(writeResp, phase << ": write response timeout");
            UNIT_ASSERT_VALUES_EQUAL_C(writeResp->Messages.size(), 1u, phase);
            auto status = writeResp->Messages[0].Status;
            UNIT_ASSERT_C(status == Ydb::StatusIds::SUCCESS || status == Ydb::StatusIds::ALREADY_EXISTS, phase << ": unexpected status " << Ydb::StatusIds::StatusCode_Name(status));
            ACerr << ">>>>> " << phase << " body=" << body << " status=" << Ydb::StatusIds::StatusCode_Name(status) << Endl;

            gotSuccess += (status == Ydb::StatusIds::SUCCESS);
            gotAlreadyExists += (status == Ydb::StatusIds::ALREADY_EXISTS);
            UNIT_ASSERT_LE_C(gotSuccess, 1, phase);
            leapTimeProvider->Add(writePause);
            elapsed += writePause;
        }

        if (iter + 1 < numIterations) {
            for (size_t s = 0; s < splitSpec.Count; ++s) {
                if (s > 0) {
                    leapTimeProvider->Add(splitSpec.PauseBetween);
                    elapsed += splitSpec.PauseBetween;
                }
                ACerr << ">>>>> " << phase << " split " << s << " (elapsed=" << elapsed << ")" << Endl;
                splitter.SplitAllPartitions(runtime, ++txId, "/Root", "topic1");
                auto partitionCount = WaitForPartitionCount(setup, "/Root/topic1", splitter.TotalPartitions());
                UNIT_ASSERT_VALUES_EQUAL_C(partitionCount, splitter.TotalPartitions(), phase << ": partition count mismatch after split " << s);
            }
        }
    }

    ACerr << ">>>>> " << testName << ": reading all messages" << Endl;
    auto readMessages = ReadAllDedupAndCommit(runtime, "/Root/topic1", "mlp-consumer");
    UNIT_ASSERT_C(!readMessages.empty(), testName << ": expected at least one message");
    for (const auto& [body, readDedupId] : readMessages) {
        UNIT_ASSERT_VALUES_EQUAL_C(readDedupId, dedupId, testName << ": dedupId mismatch for body=" << body);
    }
    ACerr << ">>>>> " << testName << ": done, read " << readMessages.size() << " messages" << Endl;
    UNIT_ASSERT_VALUES_EQUAL(readMessages.size(), numIterations);
}

Y_UNIT_TEST(Dedup_RepeatedWriteUntilWindowCloses) {
    RunRepeatedDedupWithSplits("Dedup_RepeatedWriteUntilWindowCloses", TDuration::Seconds(47), 4);
}

Y_UNIT_TEST(Dedup_RepeatedWriteUntilWindowCloses_DoubleSplit) {
    RunRepeatedDedupWithSplits("Dedup_RepeatedWriteUntilWindowCloses_DoubleSplit", TDuration::Seconds(47), 4, {.Count = 2, .PauseBetween = TDuration::Seconds(80)});
}

}

} // namespace NKikimr::NPQ::NMLP


template <>
void Out<NKikimr::NPQ::NMLP::TWriteResult>(IOutputStream& os, const NKikimr::NPQ::NMLP::TWriteResult& w) {
    os << "{";
    os << "Status=" << w.Status << ", ";
    os << "MessageId=" << w.MessageId.PartitionId << "-" << w.MessageId.Offset << ", ";
    os << "StageIdx=" << w.StageIdx << ", ";
    os << "WriteTimestampOffset=" << w.WriteTimestampOffset;
    os << "}";
}

template <>
void Out<NKikimr::NPQ::NMLP::TDedupWriteSpec>(IOutputStream& os, const NKikimr::NPQ::NMLP::TDedupWriteSpec& w) {
    os << "{";
    os << "Body=" << w.Body << ", ";
    os << "DedupId=" << w.DedupId << ", ";
    os << "GroupId=" << w.GroupId;
    os << "}";
}
