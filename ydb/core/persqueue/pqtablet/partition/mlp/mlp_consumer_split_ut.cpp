#include "mlp_storage.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/iterator/iterate_keys.h>
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
    {
        PartitionsAtLevel_[0] = {0};
    }

    size_t TotalPartitions() const { return TotalPartitions_; }
    const std::vector<TLeafPartition>& Leaves() const { return Leaves_; }
    TSet<ui32> PartitionsAtLevel(size_t level) const {
        return PartitionsAtLevel_.at(level);
    }

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

private:
    void AdvanceLeaves() {
        size_t level = PartitionsAtLevel_.size();
        std::vector<TLeafPartition> newLeaves;
        for (size_t i = 0; i < Leaves_.size(); ++i) {
            const auto& leaf = Leaves_[i];
            unsigned char mid = (unsigned char)(((unsigned)leaf.Lo + (unsigned)leaf.Hi) / 2);
            ui32 leftId = (ui32)(TotalPartitions_ + i * 2);
            ui32 rightId = (ui32)(TotalPartitions_ + i * 2 + 1);
            newLeaves.push_back({leftId, leaf.Lo, mid});
            newLeaves.push_back({rightId, (unsigned char)(mid + 1), leaf.Hi});
            PartitionsAtLevel_[level].insert(leftId);
            PartitionsAtLevel_[level].insert(rightId);
        }
        TotalPartitions_ += Leaves_.size() * 2;
        Leaves_ = std::move(newLeaves);
    }

    std::vector<TLeafPartition> Leaves_;
    TMap<size_t, TSet<ui32>> PartitionsAtLevel_;
    size_t TotalPartitions_;
};

struct TCounter {
    int Write = 0;
    int Read = 0;
    int Commit = 0;
    TString LastReadBody = {};
    TVector<int> WriteByStage;
    TVector<int> ReadByStage;
    TVector<int> CommitByStage;


    void IncWrite(int stage) {
        Adjust(Write, WriteByStage, 1, stage);
    }

    void IncRead(int stage) {
        Adjust(Read, ReadByStage, 1, stage);
    }

    void IncCommit(int stage) {
        Adjust(Commit, CommitByStage, 1, stage);
    }

    void DecRead(int stage) {
        Adjust(Read, ReadByStage, -1, stage);
    }

    void Adjust(int& value, TVector<int>& vec, int diff, int stage) {

        while (std::cmp_less_equal(vec.size(), stage)) {
            vec.push_back(0);
        }
        value += diff;
        vec[stage] += diff;
    }
};

using TGroupCounter = TMap<TString, TCounter>;

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
    size_t Count = 0;
    bool CommitLast = true;
    // If CommitLast is true, all ReadCount messages are committed.
    // If CommitLast is false, the last message is left uncommitted (locked).
    // Effective CommitCount == ReadCount - !CommitLast
    //
    // For non-first stages, at the beginning of read processing:
    //   If ReadCount > 0: commit previous stage's uncommitted message (if any), then read.
    //   If ReadCount == 0 && CommitLast == true: commit previous stage's uncommitted message (if any).
    //   If ReadCount == 0 && CommitLast == false: leave previous uncommitted message as-is.
};

struct TStage {
    size_t WriteCount = 0;
    TReadCommitCount Read;
};

struct TGroupDescription {
    TStage Root;
    std::optional<TStage> IntermediateSplit;
    TStage LastSplit;
};


static bool HasIntermediateSplit(const TMap<TString, TGroupDescription>& groups) {
    for (const auto& [_, desc] : groups) {
        if (desc.IntermediateSplit.has_value()) {
            return true;
        }
    }
    return false;
}

static TSet<ui32> AllPartitionIds(size_t totalPartitions) {
    TSet<ui32> ids;
    for (ui32 i = 0; i < totalPartitions; ++i) {
        ids.insert(i);
    }
    return ids;
}

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

static TSet<TString> JoinKeys(const auto& m1, const auto& m2) {
    TSet<TString> result;
    for (const auto& [k, _] : m1) {
        result.insert(k);
    }
    for (const auto& [k, _] : m2) {
        result.insert(k);
    }
    return result;
}


TMap<TString, TMessageId> ReadAndCommitFIFOExceptLast(
    NActors::TTestActorRuntime& runtime,
    const TString& topicName,
    const TString& consumer,
    TMap<TString, size_t>& remainingReads,
    TGroupCounter& groupCounter,
    int stageIdx,
    TStringBuf phase,
    int retries
) {

    TMap<TString, TMessageId> lastMessageOfTheGroup;
    for (int readIteration = 1; ; ++readIteration) {
        {
            TBufferedCerr sb;
            sb << phase << " ReadAndCommitFIFOExceptLast iteration=" << readIteration << ", retries left=" << retries << ":\n";
            for (const auto& g : JoinKeys(remainingReads, groupCounter)) {
                size_t n = remainingReads.Value(g, 0);
                sb << "  " << g << ": read remaining=" << n << " ";
                auto* lm = lastMessageOfTheGroup.FindPtr(g);
                if (lm) {
                    sb << "- 1";
                } else {
                    sb << "- 0";
                }
                sb << "\t";
                sb << groupCounter.Value(g, TCounter{});
                sb << "\t";
                if (lm) {
                    sb << "Last message: ";
                    sb << " partition=" << lm->PartitionId;
                    sb << " offset=" << lm->Offset << Endl;
                }
                sb << "\n";
            }
            sb << "\n";
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
                                    .MaxNumberOfMessage = 50});
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
            groupCounter[groupId].IncRead(stageIdx);

            ACerr << ">>>>> " << phase << " ReadAndCommitFIFOExceptLast read: " << msg.Data
                 << " group=" << groupId
                 << " partition=" << msg.MessageId.PartitionId
                 << " offset=" << msg.MessageId.Offset << Endl;
            UNIT_ASSERT_C(!lastMessageOfTheGroup.contains(groupId), LabeledOutput(groupId));
            UNIT_ASSERT_LE_C(groupCounter[groupId].LastReadBody, msg.Data, LabeledOutput(groupId, groupCounter[groupId].LastReadBody, msg.Data));
            groupCounter[groupId].LastReadBody = msg.Data;
            if (remainingReads.Value(groupId, 0) > 1) {
                messagesToCommit.push_back(msg.MessageId);
                remainingReads[groupId]--;
                groupCounter[groupId].IncCommit(stageIdx);
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
            groupCounter[groupId].IncCommit(stageIdx);
        } else if (remainingReads.Value(groupId, 0) == 1) {
            remainingReads[groupId]--;
            uncommitedMessages[groupId] = message;
        } else {
            Y_ASSERT(remainingReads.Value(groupId, 0) == 0);
            messagesToUnlock.push_back(message);
            groupCounter[groupId].DecRead(stageIdx);
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
    TGroupCounter& groupCounter,
    int stageIdx,
    TStringBuf phase,
    bool allowPartialRead,
    int retries
) {
    TMap<TString, size_t> remainingReads;
    for (const auto& [groupId, op] : operations) {
        remainingReads[groupId] = op.Count;
    }
    TMap<TString, TMessageId> last = ReadAndCommitFIFOExceptLast(runtime, topicName, consumer, remainingReads, groupCounter, stageIdx, phase, retries);
    for (const auto& [groupId, cnt] : remainingReads) {
        if (!allowPartialRead) {
            UNIT_ASSERT_VALUES_EQUAL_C(cnt, 0, phase << ": Not enough messages for group " << groupId << "; " << cnt << " more required (" << operations.at(groupId).Count << " total)");
        }
        if (cnt != 0) {
            ACerr << phase << ": Not enough messages for group " << groupId << "; " << cnt << " more required (" << operations.at(groupId).Count << " total)" << Endl;
        }
    }
    TMap<TString, TMessageId> uncommited;
    std::vector<TMessageId> messagesToCommit;
    for (const auto& [groupId, message] : last) {
        const auto& op = operations.at(groupId);
        if (op.CommitLast) {
            messagesToCommit.push_back(message);
            groupCounter[groupId].IncCommit(stageIdx);
        } else {
            uncommited[groupId] = message;
        }
    }
    if (!messagesToCommit.empty()) {
        auto commitResult = Commit(runtime, topicName, consumer, messagesToCommit);
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult->Status, Ydb::StatusIds::SUCCESS, phase);
    }
    {
        TBufferedCerr sb;
        sb << phase << "ReadAndCommitFIFO result\n";
        for (const auto& g : JoinKeys(remainingReads, groupCounter)) {
            size_t n = remainingReads.Value(g, 0);
            sb << "  " << g << ": read remaining=" << n << " ";
            sb << "\t";
            sb << groupCounter.Value(g, TCounter{});
            sb << "\t";
            const TReadCommitCount op = operations.Value(g, TReadCommitCount{});
            sb << LabeledOutput(op.Count, op.CommitLast);
            sb << "\n";
        }
        sb << "\n";
    }

    return uncommited;
}

// return unexpected errornous groups
TMap<TString, TVector<TMessageId>> ReadAndCommitFIFOAll(
    NActors::TTestActorRuntime& runtime,
    const TString& topicName,
    const TString& consumer,
    TMap<TString, size_t>& remainingReads,
    TGroupCounter& groupCounter,
    int stageIdx,
    TStringBuf phase,
    int retries
) {
    TMap<TString, TVector<TMessageId>> unexpected;
    for (int readIteration = 1; ; ++readIteration) {
        {
            TBufferedCerr sb;
            sb << phase << " ReadAndCommitFIFOExceptLast iteration=" << readIteration << ", retries left=" << retries << ":\n";
            for (const auto& g : JoinKeys(remainingReads, groupCounter)) {
                size_t n = remainingReads.Value(g, 0);
                sb << "  " << g << ": read remaining=" << n << " ";
                sb << "\t";
                sb << groupCounter.Value(g, TCounter{});
                sb << "\t";
                sb << "\n";
            }
            sb << "\n";
        }
        const size_t toRead = [&]() {
            size_t r = 0;
            for (auto& [g, n] : remainingReads) {
                    r += n;
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
                                    .MaxNumberOfMessage = 50});
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
            groupCounter[groupId].IncRead(stageIdx);

            ACerr << ">>>>> " << phase << " ReadAndCommitFIFOExceptLast read: " << msg.Data
                 << " group=" << groupId
                 << " partition=" << msg.MessageId.PartitionId
                 << " offset=" << msg.MessageId.Offset << Endl;
            UNIT_ASSERT_LE_C(groupCounter[groupId].LastReadBody, msg.Data, LabeledOutput(groupId, groupCounter[groupId].LastReadBody, msg.Data));
            groupCounter[groupId].LastReadBody = msg.Data;
            if (remainingReads.Value(groupId, 0) > 0) {
                remainingReads[groupId]--;
            } else {
                unexpected[groupId].push_back(msg.MessageId);
            }
            messagesToCommit.push_back(msg.MessageId);
            groupCounter[groupId].IncCommit(stageIdx);
        }
        if (!messagesToCommit.empty()) {
            auto commitResult = Commit(runtime, topicName, consumer, messagesToCommit);
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult->Status, Ydb::StatusIds::SUCCESS, phase);
        }
    }
    return unexpected;
}



void PartitionSplitWithMessageGroupOrdering(const TMap<TString, TGroupDescription>& groups, const std::span<const ui32> partitionsToRestart = {}) {
    const bool doIntermediateSplit = HasIntermediateSplit(groups);
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    ACerr << ">>>>> Phase 1: Create topic with autoscaling and shared consumer with KeepMessageOrder(true)\n";
    ACerr << ">>>>> groups:\n";
    for (const auto& [g, desc] : groups) {
        auto printStage = [&](TStringBuf name, const TStage& s) {
            ACerr << "    " << name << ": WriteCount=" << s.WriteCount << " Read={ReadCount=" << s.Read.Count << " CommitLast=" << s.Read.CommitLast << "}\n";
        };
        ACerr << "  " << g << ":\n";
        printStage("Root", desc.Root);
        if (desc.IntermediateSplit.has_value()) {
            printStage("IntermediateSplit", *desc.IntermediateSplit);
        }
        printStage("LastSplit", desc.LastSplit);
    }
    ACerr << Endl;
    CreateSetupFIFOTopic(setup, "/Root/topic1");

    // inflyMessages: per-group uncommitted message carried over from the previous stage's Read (CommitLast=false)
    TMap<TString, TMessageId> inflyMessages;
    TGroupCounter groupCounter;
    TPartitionSplitter splitter;
    ui64 txId = 1006;

    auto dumpGroupStatistics = [&]() {
        TBufferedCerr log;
        log << "Group statistics:\n";
        for (const auto& [groupId, desc] : groups) {
            const TCounter c = groupCounter.Value(groupId, TCounter{});
            log << "    " << groupId << ": write=" << c.Write << " read=" << c.Read << " commit=" << c.Commit << "\n";
       }
    };

    auto executeStage = [&](size_t stageIdx, auto getStage, TStringBuf phaseName) {
        ACerr << ">>>>> Stage " << stageIdx << " (" << phaseName << "): write\n";
        for (const auto& [groupId, desc] : groups) {
            const TStage& stage = getStage(desc);
            if (stage.WriteCount == 0) {
                continue;
            }
            std::vector<TWriterSettings::TMessage> messages;
            for (size_t i = 0; i < stage.WriteCount; ++i) {
                messages.push_back({
                    .Index = i,
                    .MessageBody = TStringBuilder() << "msg-" << groupId << "-s" << LeftPad(stageIdx, 3, '0') << "-" << LeftPad(i + 1, 8, '0'),
                    .MessageGroupId = groupId,
                });
                groupCounter[groupId].IncWrite(stageIdx);
            }
            ACerr << "    write for group " << groupId << ": " << messages.size() << " messages\n";
            CreateWriterActor(runtime, {.DatabasePath = "/Root", .TopicName = "/Root/topic1", .Messages = std::move(messages)});
            auto writeResp = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL_C(writeResp->Messages.size(), stage.WriteCount, phaseName << ": write for group " << groupId);
            for (auto& msg : writeResp->Messages) {
                UNIT_ASSERT_VALUES_EQUAL_C(msg.Status, Ydb::StatusIds::SUCCESS, phaseName << ": write for group " << groupId);
            }
        }
        DumpStorageState(setup, "/Root/topic1", "mlp-consumer", AllPartitionIds(splitter.TotalPartitions()));

        ACerr << ">>>>> Stage " << stageIdx << " (" << phaseName << "): read\n";
        if (stageIdx > 0) {
            std::vector<TMessageId> toCommit;
            {
                TBufferedCerr log;
                for (const auto& [groupId, desc] : groups) {
                    const TStage& stage = getStage(desc);
                    auto* inflyMsg = inflyMessages.FindPtr(groupId);
                    if (!inflyMsg) {
                        continue;
                    }
                    if (stage.Read.Count > 0 || stage.Read.CommitLast) {
                        toCommit.push_back(*inflyMsg);
                        inflyMessages.erase(groupId);
                        groupCounter[groupId].IncCommit(stageIdx - 1);
                        log << "    commit inflight message for group " << groupId << "\n";
                    } else {
                        log << "    keep inflight message for group " << groupId << "\n";
                    }
                }
            }
            if (!toCommit.empty()) {
                auto commitResult = Commit(runtime, "/Root/topic1", "mlp-consumer", toCommit);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult->Status, Ydb::StatusIds::SUCCESS, phaseName << " (commit prev inflight)");
            }
        }
        TMap<TString, TReadCommitCount> readOps;
        {
            TBufferedCerr log;
            for (const auto& [groupId, desc] : groups) {
                const TStage& stage = getStage(desc);
                if (stage.Read.Count > 0) {
                    readOps[groupId] = stage.Read;
                    log << "    read from group " << groupId << ": " << stage.Read.Count << " messages\n";
                }
            }
        }
        if (!readOps.empty()) {
            bool allowPartialRead = stageIdx == 1;
            auto newInfly = ReadAndCommitFIFO(runtime, "/Root/topic1", "mlp-consumer", readOps, groupCounter, stageIdx, phaseName, allowPartialRead, 1);
            for (auto& [groupId, msgId] : newInfly) {
                inflyMessages[groupId] = msgId;
            }
        }
        DumpStorageState(setup, "/Root/topic1", "mlp-consumer", AllPartitionIds(splitter.TotalPartitions()));
        dumpGroupStatistics();
    };

    executeStage(0, [](const TGroupDescription& d) -> const TStage& { return d.Root; }, "root");

    ACerr << ">>>>> First split\n";
    splitter.SplitAllPartitions(runtime, ++txId, "/Root", "topic1");
    {
        auto pc = WaitForPartitionCount(setup, "/Root/topic1", splitter.TotalPartitions());
        UNIT_ASSERT_VALUES_EQUAL_C(pc, splitter.TotalPartitions(), "Expected " << splitter.TotalPartitions() << " partitions after first split");
    }

    if (doIntermediateSplit) {
        executeStage(1, [](const TGroupDescription& d) -> const TStage& { return d.IntermediateSplit.has_value() ? *d.IntermediateSplit : Default<TStage>(); }, "intermediate-split");

        ACerr << ">>>>> Second split\n";
        splitter.SplitAllPartitions(runtime, ++txId, "/Root", "topic1");
        {
            auto pc = WaitForPartitionCount(setup, "/Root/topic1", splitter.TotalPartitions());
            UNIT_ASSERT_VALUES_EQUAL_C(pc, splitter.TotalPartitions(), "Expected " << splitter.TotalPartitions() << " partitions after first split");
        }

        DumpStorageState(setup, "/Root/topic1", "mlp-consumer", AllPartitionIds(splitter.TotalPartitions()));
    }

    executeStage(2, [](const TGroupDescription& d) -> const TStage& { return d.LastSplit; }, "last-split");

    for (ui32 partitionId : partitionsToRestart) {
        ACerr << ">>>>> Restart partition " << partitionId << Endl;
        ReloadPQTablet(setup, "/Root", "/Root/topic1", partitionId);
    }

    for (const auto& [groupId, counter] : groupCounter) {
        int inflyCnt = counter.Read - counter.Commit;
        UNIT_ASSERT_C(EqualToOneOf(inflyCnt, 0, 1), LabeledOutput(inflyCnt));
        UNIT_ASSERT_VALUES_EQUAL(inflyCnt, inflyMessages.contains(groupId));
    }

    {
        // Probe read: attempt to read messages and verify FIFO invariant.
        // We do a single read with to see what's available.
        // Due to FIFO: at most 1 message per group, and groups with inflight (locked)
        // messages from Phase 3 won't return new messages.
        TSet<TString> expectedAvailableGroups;
        for (const auto& [groupId, counter] : groupCounter) {
            if (!(counter.Write > counter.Read && counter.Read == counter.Commit)) {
                continue;
            }
            expectedAvailableGroups.insert(groupId);
        }
        ACerr << ">>>>> Validation: expectedAvailableGroups: " << JoinSeq(", ", expectedAvailableGroups) << "\n";
        CreateReaderActor(runtime, {.DatabasePath = "/Root", .TopicName = "/Root/topic1", .Consumer = "mlp-consumer",
                                    .WaitTime = TDuration::Seconds(3),
                                    .ProcessingTimeout = TDuration::Seconds(30),
                                    .MaxNumberOfMessage = 50});
        auto readResult = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(readResult->Status, Ydb::StatusIds::SUCCESS, "validation probe read");
        THashSet<TString> seenGroups;
        for (const auto& msg : readResult->Messages) {
            UNIT_ASSERT_C(!seenGroups.contains(msg.MessageGroupId), "FIFO violation in validation: duplicate group " << msg.MessageGroupId << " in single read response");
            groupCounter[msg.MessageGroupId].IncRead(4);
            seenGroups.insert(msg.MessageGroupId);
            ACerr << ">>>>> Validation read: " << msg.Data << " group=" << msg.MessageGroupId << " partition=" << msg.MessageId.PartitionId << " offset=" << msg.MessageId.Offset << Endl;
            UNIT_ASSERT_C(expectedAvailableGroups.contains(msg.MessageGroupId), "Unexpected group " << msg.MessageGroupId << " in read response");
            UNIT_ASSERT_LE_C(groupCounter[msg.MessageGroupId].LastReadBody, msg.Data, LabeledOutput(msg.MessageGroupId, groupCounter[msg.MessageGroupId].LastReadBody, msg.Data));
            groupCounter[msg.MessageGroupId].LastReadBody = msg.Data;
        }
        for (const auto& [groupId, messageId] : inflyMessages) {
            UNIT_ASSERT_C(!seenGroups.contains(groupId),
                          "FIFO violation: group " << groupId << " has an inflight (locked) message but appeared in read response");
        }
        if (!readResult->Messages.empty()) {
            std::vector<TMessageId> toUnlock;
            for (const auto& msg : readResult->Messages) {
                toUnlock.push_back(msg.MessageId);
                groupCounter[msg.MessageGroupId].DecRead(4);
            }
            auto unlockResult = Unlock(runtime, "/Root/topic1", "mlp-consumer", toUnlock);
            UNIT_ASSERT_VALUES_EQUAL_C(unlockResult->Status, Ydb::StatusIds::SUCCESS, "validation probe unlock");
        }
    }

    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", AllPartitionIds(splitter.TotalPartitions()));

    // Finalization: commit all inflight, then read remaining messages from each stage
    if (!inflyMessages.empty()) {
        std::vector<TMessageId> toCommit;
        for (const auto& [groupId, messageId] : inflyMessages) {
            toCommit.push_back(messageId);
            ACerr << ">>>>> Finalization: committing inflight group=" << groupId
                  << " partition=" << messageId.PartitionId << " offset=" << messageId.Offset << Endl;
            groupCounter[groupId].IncCommit(4);
        }
        auto commitResult = Commit(runtime, "/Root/topic1", "mlp-consumer", toCommit);
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult->Status, Ydb::StatusIds::SUCCESS, "finalization commit inflight");
        inflyMessages.clear();
    }
    dumpGroupStatistics();
    // Read remaining messages
    {
        TMap<TString, size_t> remainingReads;
        for (const auto& [groupId, counter] : groupCounter) {
            UNIT_ASSERT_VALUES_EQUAL_C(counter.Read, counter.Commit, groupId);
            if (size_t remaining = counter.Write - counter.Commit; remaining > 0) {
                remainingReads[groupId] = remaining;
            }
        }
        if (!remainingReads.empty()) {
            TMap<TString, TVector<TMessageId>> unexpected = ReadAndCommitFIFOAll(runtime, "/Root/topic1", "mlp-consumer", remainingReads, groupCounter, 4, "finalization", 10);
            {
                TBufferedCerr log;
                if (unexpected.empty()) {
                    log << "finalization: no unexpected messages\n";
                } else {
                    log << "finalization: unexpected messages:\n";
                    for (const auto& [groupId, messages] : unexpected) {
                        log << "  group=" << groupId << " messages=" << JoinMessageIds(messages) << "\n";
                    }
                }
                for (const auto& groupId : JoinKeys(remainingReads, groupCounter)) {
                    log << "  group=" << groupId << " remaining=" << remainingReads.Value(groupId, 0) << "\t" << groupCounter.Value(groupId, TCounter{}) << "\n";
                }
            }
            UNIT_ASSERT_C(unexpected.empty(), "finalization: expected all remaining messages committed");
            for (const auto& [groupId, remaining] : remainingReads) {
                UNIT_ASSERT_VALUES_EQUAL_C(remaining, 0, "finalization: expected all remaining messages committed in group " << groupId);
            }
        }
    }

    dumpGroupStatistics();
    DumpStorageState(setup, "/Root/topic1", "mlp-consumer", AllPartitionIds(splitter.TotalPartitions()));

    // Final verification: try to read more messages - should get nothing
    {
        CreateReaderActor(runtime, {.DatabasePath = "/Root", .TopicName = "/Root/topic1", .Consumer = "mlp-consumer",
                                    .WaitTime = TDuration::Seconds(3),
                                    .ProcessingTimeout = TDuration::Seconds(30),
                                    .MaxNumberOfMessage = 10});
        auto finalRead = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(finalRead->Status, Ydb::StatusIds::SUCCESS, "finalization final read");
        UNIT_ASSERT_C(finalRead->Messages.empty(), "Expected no more messages, but got " << finalRead->Messages.size());
    }

    ACerr << ">>>>> All phases completed successfully" << Endl;
}

// Case 1: All written messages committed before split.
// Expected: child messages for all groups (A, B, C) are immediately available.
Y_UNIT_TEST(Order_AllCommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3, .Read = {.Count = 3}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    });
}

// Case 2: None of the written messages committed before split.
// Expected: child messages for all groups are blocked until parent messages are committed.
Y_UNIT_TEST(Order_NoneCommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    });
}

// Case 3: All messages of group-A and group-B committed before split.
//         None of group-C committed before split.
// Expected: child messages for A and B available; C blocked until parent committed.
Y_UNIT_TEST(Order_GroupAB_CommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3, .Read = {.Count = 3}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    });
}

// Case 4: One message of each group committed before split.
// Expected: child messages for all groups are available (partial commit unblocks).
Y_UNIT_TEST(Order_OnePerGroupCommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3, .Read = {.Count = 1}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2, .Read = {.Count = 1}}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2, .Read = {.Count = 1}}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    });
}

// Case 5: All messages of group-A and group-B committed before split.
//         One message of group-C committed before split.
// Expected: child messages for all groups are available.
Y_UNIT_TEST(Order_GroupAB_OneCCommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3, .Read = {.Count = 3}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2, .Read = {.Count = 1}}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    });
}

Y_UNIT_TEST(Order_GroupBC_OneACommittedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3, .Read = {.Count = 1}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    });
}

Y_UNIT_TEST(Order_AllCommittedBeforeSplit_RestartParentPartition) {
    static constexpr ui32 restartPartitions[] = {0};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3, .Read = {.Count = 3}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_AllCommittedBeforeSplit_RestartChildPartition) {
    static constexpr ui32 restartPartitions[] = {1};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3, .Read = {.Count = 3}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_NoneCommittedBeforeSplit_RestartParentPartition) {
    static constexpr ui32 restartPartitions[] = {0};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_NoneCommittedBeforeSplit_RestartChildPartition) {
    static constexpr ui32 restartPartitions[] = {1};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_GroupAB_CommittedBeforeSplit_RestartParentPartition) {
    static constexpr ui32 restartPartitions[] = {0};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3, .Read = {.Count = 3}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_GroupAB_CommittedBeforeSplit_RestartChildPartition) {
    static constexpr ui32 restartPartitions[] = {1};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3, .Read = {.Count = 3}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_GroupBC_OneACommittedBeforeSplit_RestartParentPartition) {
    static constexpr ui32 restartPartitions[] = {0};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3, .Read = {.Count = 1}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    }, restartPartitions);
}

Y_UNIT_TEST(Order_GroupBC_OneACommittedBeforeSplit_RestartChildPartition) {
    static constexpr ui32 restartPartitions[] = {1};
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 3, .Read = {.Count = 1}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = {.WriteCount = 1}}},
        {"group-D", {.LastSplit = {.WriteCount = 1}}},
        {"group-E", {.LastSplit = {.WriteCount = 1}}},
    }, restartPartitions);
}

// Sanity check: minimal test with two groups, one message each before and after split.
Y_UNIT_TEST(Order_TwoGroups_Minimal) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 1, .Read = {.Count = 1}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 1, .Read = {.Count = 1}}, .LastSplit = {.WriteCount = 1}}},
    });
}

// Sanity check: two groups before split, one new group only after split.
Y_UNIT_TEST(Order_NewGroupAfterSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 1, .Read = {.Count = 1}}, .LastSplit = {.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 1, .Read = {.Count = 1}}, .LastSplit = {.WriteCount = 1}}},
        {"group-C", {.LastSplit = {.WriteCount = 1}}},
    });
}

// Sanity check: single group, uncommitted before split.
Y_UNIT_TEST(Order_SingleGroup_Uncommitted) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 1, .Read = {.Count = 1, .CommitLast = false}}, .LastSplit = {.WriteCount = 1, .Read = {.CommitLast = false}}}},
    });
}

// Sanity check: single group, committed before split.
Y_UNIT_TEST(Order_SingleGroup_Committed) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 1, .Read = {.Count = 1}}, .LastSplit = {.WriteCount = 1}}},
    });
}

// Sanity check: single group before split, new group after split, committed before split.
Y_UNIT_TEST(Order_UnrelatedGroup_Committed) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 1, .Read = {.Count = 1}}}},
        {"group-B", {.LastSplit = {.WriteCount = 1}}},
    });
}

// Double split: single group, locked before first split, write after second split.
// Steps: write 1 msg → read+lock → split → split again → write 1 msg → verify blocked → commit → read all.
Y_UNIT_TEST(Order_DoubleSplit_SingleGroup_LockedBeforeSplit) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 1, .Read = {.Count = 1, .CommitLast = false}}, .IntermediateSplit = TStage{.Read = {.CommitLast = false}}, .LastSplit = TStage{.WriteCount = 1}}},
    });
}

Y_UNIT_TEST(Order_DoubleSplit_MultipleGroups_R1) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-00", {.Root = {.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 3}}},
    });
}

Y_UNIT_TEST(Order_DoubleSplit_MultipleGroups_R2) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-00", {.Root = {.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 3}}},
        {"group-01", {.Root = {.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 2, .Read = {.Count = 0, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 3}}},
    });
}

Y_UNIT_TEST(Order_DoubleSplit_MultipleGroups_R7) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-00", {.Root = {.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 3}}},
        {"group-01", {.Root = {.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 2, .Read = {.Count = 0, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 3}}},
        {"group-02", {.Root = {.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 1, .Read = {.Count = 2, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 4}}},
        {"group-03", {.Root = {.WriteCount = 1, .Read = {.Count = 1, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 2}}},
        {"group-04", {.Root = {.WriteCount = 5, .Read = {.Count = 3, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 1, .Read = {.Count = 2, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 5}}},
        {"group-05", {.Root = {.WriteCount = 2, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 3, .Read = {.Count = 1, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 5}}},
        {"group-06", {.Root = {.WriteCount = 3, .Read = {.Count = 2, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 2, .Read = {.Count = 0, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 3}}},
    });
}

Y_UNIT_TEST(Order_DoubleSplit_MultipleGroups_R8) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-00", {.Root = {.WriteCount = 4, .Read = {.Count = 1, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 3, .Read = {.Count = 5, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 4}}},
        {"group-01", {.Root = {.WriteCount = 3, .Read = {.Count = 1, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 5, .Read = {.Count = 5, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 0}}},
        {"group-02", {.Root = {.WriteCount = 2, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 4, .Read = {.Count = 6, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 4}}},
        {"group-03", {.Root = {.WriteCount = 5, .Read = {.Count = 5, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 0}}},
        {"group-04", {.Root = {.WriteCount = 3, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 4, .Read = {.Count = 0, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 5}}},
        {"group-05", {.Root = {.WriteCount = 3, .Read = {.Count = 2, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 4, .Read = {.Count = 0, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 1}}},
        {"group-06", {.Root = {.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 3, .Read = {.Count = 3, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 5}}},
        {"group-07", {.Root = {.WriteCount = 2, .Read = {.Count = 1, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 5, .Read = {.Count = 1, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 3}}},
    });
}

Y_UNIT_TEST(Order_DoubleSplit_MultipleGroups_R32) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-00", {.Root = {.WriteCount = 4, .Read = {.Count = 1, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 3, .Read = {.Count = 5, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 4}}},
        {"group-01", {.Root = {.WriteCount = 3, .Read = {.Count = 1, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 5, .Read = {.Count = 5, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 0}}},
        {"group-02", {.Root = {.WriteCount = 2, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 4, .Read = {.Count = 6, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 4}}},
        {"group-03", {.Root = {.WriteCount = 5, .Read = {.Count = 5, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 0}}},
        {"group-04", {.Root = {.WriteCount = 3, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 4, .Read = {.Count = 0, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 5}}},
        {"group-05", {.Root = {.WriteCount = 3, .Read = {.Count = 2, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 4, .Read = {.Count = 0, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 1}}},
        {"group-06", {.Root = {.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 3, .Read = {.Count = 3, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 5}}},
        {"group-07", {.Root = {.WriteCount = 2, .Read = {.Count = 1, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 5, .Read = {.Count = 1, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 3}}},
        {"group-08", {.Root = {.WriteCount = 4, .Read = {.Count = 2, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 4, .Read = {.Count = 3, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 2}}},
        {"group-09", {.Root = {.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 4, .Read = {.Count = 2, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 1}}},
        {"group-10", {.Root = {.WriteCount = 3, .Read = {.Count = 1, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 5, .Read = {.Count = 5, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 5}}},
        {"group-11", {.Root = {.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 5, .Read = {.Count = 5, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 2}}},
        {"group-12", {.Root = {.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 3, .Read = {.Count = 1, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 2}}},
        {"group-13", {.Root = {.WriteCount = 5, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 0, .Read = {.Count = 4, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 2}}},
        {"group-14", {.Root = {.WriteCount = 1, .Read = {.Count = 1, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 4}}},
        {"group-15", {.Root = {.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 0}}},
        {"group-16", {.Root = {.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 5, .Read = {.Count = 2, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 0}}},
        {"group-17", {.Root = {.WriteCount = 2, .Read = {.Count = 1, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 4, .Read = {.Count = 3, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 4}}},
        {"group-18", {.Root = {.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 4, .Read = {.Count = 3, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 1}}},
        {"group-19", {.Root = {.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 2}}},
        {"group-20", {.Root = {.WriteCount = 2, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 3, .Read = {.Count = 3, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 2}}},
        {"group-21", {.Root = {.WriteCount = 2, .Read = {.Count = 0, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 2}}},
        {"group-22", {.Root = {.WriteCount = 5, .Read = {.Count = 5, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 5, .Read = {.Count = 5, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 3}}},
        {"group-23", {.Root = {.WriteCount = 3, .Read = {.Count = 2, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 1, .Read = {.Count = 1, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 4}}},
        {"group-24", {.Root = {.WriteCount = 1, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 2, .Read = {.Count = 1, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 0}}},
        {"group-25", {.Root = {.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 1, .Read = {.Count = 1, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 1}}},
        {"group-26", {.Root = {.WriteCount = 4, .Read = {.Count = 3, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 5, .Read = {.Count = 2, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 5}}},
        {"group-27", {.Root = {.WriteCount = 3, .Read = {.Count = 1, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 4, .Read = {.Count = 3, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 2}}},
        {"group-28", {.Root = {.WriteCount = 3, .Read = {.Count = 3, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 0, .Read = {.Count = 0, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 1}}},
        {"group-29", {.Root = {.WriteCount = 3, .Read = {.Count = 0, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 1, .Read = {.Count = 4, .CommitLast = 1}}, .LastSplit = TStage{.WriteCount = 5}}},
        {"group-30", {.Root = {.WriteCount = 4, .Read = {.Count = 0, .CommitLast = 1}}, .IntermediateSplit = TStage{.WriteCount = 3, .Read = {.Count = 6, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 2}}},
        {"group-31", {.Root = {.WriteCount = 4, .Read = {.Count = 2, .CommitLast = 0}}, .IntermediateSplit = TStage{.WriteCount = 4, .Read = {.Count = 0, .CommitLast = 0}}, .LastSplit = TStage{.WriteCount = 0}}},
    });
}

// Double split: multiple groups, mixed commit states before first split.
Y_UNIT_TEST(Order_DoubleSplit_MixedGroups) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 1, .Read = {.Count = 1, .CommitLast = false}}, .IntermediateSplit = TStage{.Read = {.CommitLast = false}}, .LastSplit = TStage{.WriteCount = 1}}},
        {"group-B", {.Root = {.WriteCount = 2, .Read = {.Count = 2}}, .LastSplit = TStage{.WriteCount = 1}}},
        {"group-C", {.LastSplit = TStage{.WriteCount = 1}}},
    });
}

// Double split: writes between splits and after second split.
Y_UNIT_TEST(Order_DoubleSplit_WritesBetweenSplits) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 1, .Read = {.Count = 1, .CommitLast = false}}, .IntermediateSplit = TStage{.WriteCount = 1, .Read = {.CommitLast = false}}, .LastSplit = TStage{.WriteCount = 1}}},
    });
}

// Double split: read between splits.
Y_UNIT_TEST(Order_DoubleSplit_ReadBetweenSplits) {
    PartitionSplitWithMessageGroupOrdering({
        {"group-A", {.Root = {.WriteCount = 2, .Read = {.Count = 1}}, .IntermediateSplit = TStage{.WriteCount = 1, .Read = {.Count = 1}}, .LastSplit = TStage{.WriteCount = 1}}},
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
                sb << "  " << g << ": read remaining=" << n << Endl;
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
                 << " dedupId=" << msg.MessageMetaAttributes.Value(NPQ::MESSAGE_ATTRIBUTE_DEDUPLICATION_ID, "")
                 << " group=" << msg.MessageGroupId
                 << " partition=" << msg.MessageId.PartitionId
                 << " offset=" << msg.MessageId.Offset << Endl;
            readMessages[msg.Data] =  msg.MessageMetaAttributes.Value(NPQ::MESSAGE_ATTRIBUTE_DEDUPLICATION_ID, "");
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

template<>
void Out<NKikimr::NPQ::NMLP::TCounter>(IOutputStream& os, const NKikimr::NPQ::NMLP::TCounter& c) {
    os << "{";
    os << "w:" << c.Write << " [" << JoinSeq(",", c.WriteByStage) << "]";
    os << ", ";
    os << "r:" << c.Read << " [" << JoinSeq(",", c.ReadByStage) << "]";
    os << ", ";
    os << "c:" << c.Commit << " [" << JoinSeq(",", c.CommitByStage) << "]";
    os << ", ";
    os << "last: " << c.LastReadBody;
    os << "}";
}
