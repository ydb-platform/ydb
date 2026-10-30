#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>
#include <ydb/core/persqueue/ut/common/raw_read_session.h>

#include <ydb/library/testlib/helpers.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/generic/mapfindptr.h>
#include <util/string/builder.h>

#include <functional>

namespace NKikimr {

    using namespace NYdb::NTopic;
    using namespace NYdb::NTopic::NTests;
    using namespace NKikimr::NPQ::NTest;

    namespace {

        constexpr ui32 ParentPartition = 0;
        constexpr ui32 PartitionAfterFirstSplit = 2;
        constexpr ui32 PartitionAfterSecondSplit = 4;
        const TString Producer = "producer-1";

        using TOffsets = TMap<ui64, ui64>;

        bool WaitFor(TDuration timeout, const std::function<bool()>& condition) {
            const TInstant deadline = TInstant::Now() + timeout;
            while (!condition()) {
                if (TInstant::Now() > deadline) {
                    return false;
                }
                Sleep(TDuration::MilliSeconds(100));
            }
            return true;
        }

        TString DumpOffsets(const TOffsets& offsets) {
            TStringBuilder out;
            for (const auto& [partitionId, offset] : offsets) {
                out << partitionId << ":" << offset << " ";
            }
            return out;
        }

        TString DumpMessages(const std::vector<MsgInfo>& messages) {
            TStringBuilder out;
            for (const MsgInfo& message : messages) {
                out << message.PartitionId << ":" << message.Offset << ":" << message.Data << " ";
            }
            return out;
        }

        TTopicSdkTestSetup CreateSplitSetup() {
            TTopicSdkTestSetup setup = CreateSetup();
            setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);
            return setup;
        }

        // Close waits for the write acknowledgement, so a following split cannot overtake the message.
        void WriteMessage(TTopicSdkTestSetup& setup, const TString& data, ui64 seqNo) {
            TTopicClient client = setup.MakeClient();
            std::shared_ptr<ISimpleBlockingWriteSession> session = CreateWriteSession(client, Producer, std::nullopt, TString{TEST_TOPIC}, false);
            UNIT_ASSERT(session->Write(Msg(data, seqNo)));
            UNIT_ASSERT(session->Close(TDuration::Seconds(10)));
        }

        // Partitions 0 and 2 get one message each before the returned read_from, partition 4 gets "child-4" after it.
        TInstant WriteThroughTwoSplits(TTopicSdkTestSetup& setup) {
            ui64 txId = 1006;
            WriteMessage(setup, "parent-0", 1);
            SplitPartition(setup, ++txId, ParentPartition, "a");
            WriteMessage(setup, "parent-2", 2);
            SplitPartition(setup, ++txId, PartitionAfterFirstSplit, "d");
            const TInstant readFrom = TInstant::Now();
            WriteMessage(setup, "child-4", 3);
            return readFrom;
        }

        TOffsets ConsumerCommittedOffsets(TTopicClient& client) {
            TDescribeConsumerResult describe = client.DescribeConsumer(TEST_TOPIC, TEST_CONSUMER, TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
            UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
            TOffsets offsets;
            for (const TPartitionInfo& partition : describe.GetConsumerDescription().GetPartitions()) {
                const std::optional<TPartitionConsumerStats>& stats = partition.GetPartitionConsumerStats();
                offsets[partition.GetPartitionId()] = stats.has_value() ? stats->GetCommittedOffset() : 0;
            }
            return offsets;
        }

        void WaitConsumerCommittedOffsets(TTopicSdkTestSetup& setup, const TOffsets& expected) {
            TTopicClient client = setup.MakeClient();
            TOffsets actual;
            const bool reached = WaitFor(TDuration::Seconds(10), [&]() {
                actual = ConsumerCommittedOffsets(client);
                return AllOf(expected, [&](const auto& item) { const ui64* offset = MapFindPtr(actual, item.first); return offset && *offset == item.second; });
            });
            UNIT_ASSERT_C(reached, "consumer committed offsets " << DumpOffsets(actual) << ", expected " << DumpOffsets(expected));
        }

        // The last match: the old SDK may deliver a message again in a new partition session after a release.
        const MsgInfo* FindLastMessage(const std::vector<MsgInfo>& messages, ui64 partitionId) {
            const auto it = FindIf(messages.rbegin(), messages.rend(), [&](const MsgInfo& message) { return message.PartitionId == partitionId; });
            return it == messages.rend() ? nullptr : &*it;
        }

        MsgInfo WaitForMessage(ITestReadSession& session, ui64 partitionId) {
            std::vector<MsgInfo> received;
            WaitFor(TDuration::Seconds(30), [&]() { received = session.GetReceivedMessages(); return FindLastMessage(received, partitionId) != nullptr; });
            const MsgInfo* message = FindLastMessage(received, partitionId);
            UNIT_ASSERT_C(message, "no message from partition " << partitionId << ", received [" << DumpMessages(received) << "]");
            return *message;
        }

        ui64 AcknowledgedOffset(ITestReadSession& session, ui64 partitionId) {
            const std::unordered_map<ui64, ui64> acks = session.GetCommittedOffsets();
            const ui64* ack = MapFindPtr(acks, partitionId);
            return ack ? *ack : 0;
        }

        void WaitCommitAcknowledgement(ITestReadSession& session, ui64 partitionId, ui64 offset) {
            UNIT_ASSERT_C(WaitFor(TDuration::Seconds(15), [&]() { return AcknowledgedOffset(session, partitionId) >= offset; }), "no CommitAcknowledgement for partition " << partitionId << " up to " << offset << ", acknowledged " << AcknowledgedOffset(session, partitionId));
        }

        std::shared_ptr<ITestReadSession> StartReadSession(TTopicSdkTestSetup& setup, bool autoPartitioningSupport, bool autoCommit, std::optional<TInstant> readFrom) {
            std::shared_ptr<ITestReadSession> session = CreateTestReadSession({
                .Name = "reader",
                .Setup = setup,
                .Sdk = SdkVersion::Topic,
                .AutoCommit = autoCommit,
                .AutoPartitioningSupport = autoPartitioningSupport,
                .ReadFromTimestamp = readFrom,
            });
            session->Run();
            return session;
        }

        TRawReadSessionSettings RawSettings(bool autoPartitioningSupport, TInstant readFrom) {
            return {
                .Topics = {TString{TEST_TOPIC}},
                .AutoPartitioningSupport = autoPartitioningSupport,
                .ReadFrom = readFrom,
            };
        }

        void CommitChildAndCheckSkippedParents(TTopicSdkTestSetup& setup, TRawReadSession& reader) {
            const TRawReadSession::TMessage child = reader.ReadMessage(TEST_TOPIC, PartitionAfterSecondSplit);
            UNIT_ASSERT_VALUES_EQUAL(child.Data, "child-4");
            UNIT_ASSERT_VALUES_EQUAL_C(reader.GetMessages().size(), 1, "read_from must skip every parent message");
            reader.Commit(child);
            reader.WaitCommitted(TEST_TOPIC, PartitionAfterSecondSplit, child.Offset + 1);
            WaitConsumerCommittedOffsets(setup, {{ParentPartition, 1}, {PartitionAfterFirstSplit, 1}, {PartitionAfterSecondSplit, child.Offset + 1}});
        }

    } // namespace

    Y_UNIT_TEST_SUITE(SplitReadFromTimestamp) {

        // read_from skips every parent message, so the client never commits them. CommitAcknowledgement for the child must still arrive.
        Y_UNIT_TEST_TWIN(CommitAcknowledgementAfterReadFrom, AutoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSplitSetup();
            const TInstant readFrom = WriteThroughTwoSplits(setup);

            std::shared_ptr<ITestReadSession> reader = StartReadSession(setup, AutoPartitioningSupport, true, readFrom);
            const MsgInfo child = WaitForMessage(*reader, PartitionAfterSecondSplit);
            UNIT_ASSERT_VALUES_EQUAL(child.Data, "child-4");
            WaitCommitAcknowledgement(*reader, PartitionAfterSecondSplit, child.Offset + 1);
            UNIT_ASSERT_VALUES_EQUAL_C(reader->GetReceivedMessages().size(), 1, DumpMessages(reader->GetReceivedMessages()));
            WaitConsumerCommittedOffsets(setup, {{ParentPartition, 1}, {PartitionAfterFirstSplit, 1}, {PartitionAfterSecondSplit, child.Offset + 1}});
            reader->Close();

            // Parents come before the child, so a parent message read again would be received before the new child message.
            WriteMessage(setup, "child-4-next", 4);
            std::shared_ptr<ITestReadSession> nextReader = StartReadSession(setup, AutoPartitioningSupport, false, std::nullopt);
            const MsgInfo next = WaitForMessage(*nextReader, PartitionAfterSecondSplit);
            UNIT_ASSERT_VALUES_EQUAL(next.Data, "child-4-next");
            UNIT_ASSERT_VALUES_EQUAL_C(nextReader->GetReceivedMessages().size(), 1, DumpMessages(nextReader->GetReceivedMessages()));
            nextReader->Close();
        }

        // The parent message was delivered. Its commit is what releases the child commit.
        Y_UNIT_TEST_TWIN(ChildCommitWaitsForDeliveredParent, AutoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSplitSetup();
            ui64 txId = 1006;
            WriteMessage(setup, "parent-0", 1);
            SplitPartition(setup, ++txId, ParentPartition, "a");
            WriteMessage(setup, "child-2", 2);

            std::shared_ptr<ITestReadSession> reader = StartReadSession(setup, AutoPartitioningSupport, false, std::nullopt);
            WaitForMessage(*reader, ParentPartition);
            if (AutoPartitioningSupport) {
                WaitForMessage(*reader, PartitionAfterFirstSplit).Msg->Commit();
                UNIT_ASSERT_C(!WaitFor(TDuration::Seconds(3), [&]() { return AcknowledgedOffset(*reader, PartitionAfterFirstSplit) > 0; }), "child CommitAcknowledgement arrived before the delivered parent was committed");
            } else {
                UNIT_ASSERT_C(!WaitFor(TDuration::Seconds(3), [&]() { return FindLastMessage(reader->GetReceivedMessages(), PartitionAfterFirstSplit) != nullptr; }), "child was read before the delivered parent was committed");
            }

            WaitForMessage(*reader, ParentPartition).Msg->Commit();
            const MsgInfo child = WaitForMessage(*reader, PartitionAfterFirstSplit);
            if (!AutoPartitioningSupport) {
                child.Msg->Commit();
            }
            WaitCommitAcknowledgement(*reader, PartitionAfterFirstSplit, child.Offset + 1);
            reader->Close();
        }

    } // Y_UNIT_TEST_SUITE(SplitReadFromTimestamp)

    Y_UNIT_TEST_SUITE(SplitReadFromTimestampRawStream) {

        Y_UNIT_TEST_TWIN(ChildCommitCommitsSkippedParents, AutoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSplitSetup();
            const TInstant readFrom = WriteThroughTwoSplits(setup);

            TRawReadSession reader(setup, RawSettings(AutoPartitioningSupport, readFrom));
            CommitChildAndCheckSkippedParents(setup, reader);
        }

        // The skipped parents are not remembered anywhere between sessions: the next session finds them again.
        Y_UNIT_TEST_TWIN(ChildCommitAfterReconnect, AutoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSplitSetup();
            const TInstant readFrom = WriteThroughTwoSplits(setup);

            {
                TRawReadSession reader(setup, RawSettings(AutoPartitioningSupport, readFrom));
                reader.ReadMessage(TEST_TOPIC, PartitionAfterSecondSplit);
            }
            TRawReadSession reader(setup, RawSettings(AutoPartitioningSupport, readFrom));
            CommitChildAndCheckSkippedParents(setup, reader);
        }

    } // Y_UNIT_TEST_SUITE(SplitReadFromTimestampRawStream)

} // namespace NKikimr
