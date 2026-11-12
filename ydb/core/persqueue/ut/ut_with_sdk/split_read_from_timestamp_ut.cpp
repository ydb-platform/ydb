#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

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

        void WriteToPartition(TTopicSdkTestSetup& setup, const TString& data, ui32 partitionId) {
            TTopicClient client = setup.MakeClient();
            std::shared_ptr<ISimpleBlockingWriteSession> session = CreateWriteSession(client, TStringBuilder() << Producer << "-" << partitionId, partitionId, TString{TEST_TOPIC}, false);
            UNIT_ASSERT(session->Write(Msg(data, 1)));
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

        // StartPartitionSession inserts the partition; EndPartitionSession keeps it in the ended events after the session drops it.
        bool SessionSawPartition(ITestReadSession& session, ui64 partitionId) {
            if (session.GetPartitions().contains(partitionId)) {
                return true;
            }
            const std::vector<EvEndMsg> ended = session.GetEndedPartitionEvents();
            return FindIfPtr(ended, [&](const EvEndMsg& event) { return event.PartitionId == partitionId; }) != nullptr;
        }

        bool WaitForPartitionSession(ITestReadSession& session, ui64 partitionId) {
            return WaitFor(TDuration::Seconds(30), [&]() { return SessionSawPartition(session, partitionId); });
        }

        // The reader asks the active parent for data before the split, but read_from filters out its only message.
        void CheckChildCommitAfterReaderStartedBeforeSplit(bool autoPartitioningSupport, bool directRead) {
            TTopicSdkTestSetup setup = CreateSplitSetup();
            WriteMessage(setup, "parent-0", 1);
            // Blob write timestamps have second precision.
            Sleep(TDuration::Seconds(1));
            std::shared_ptr<ITestReadSession> reader = CreateTestReadSession({
                .Name = "reader",
                .Setup = setup,
                .Sdk = SdkVersion::Topic,
                .AutoCommit = true,
                .AutoPartitioningSupport = autoPartitioningSupport,
                .ReadFromTimestamp = TInstant::Now(),
                .DirectRead = directRead,
            });
            reader->Run();
            UNIT_ASSERT_C(WaitForPartitionSession(*reader, ParentPartition), "parent partition session did not start");

            ui64 txId = 1006;
            SplitPartition(setup, ++txId, ParentPartition, "a");
            WriteToPartition(setup, "child", PartitionAfterFirstSplit);

            const MsgInfo child = WaitForMessage(*reader, PartitionAfterFirstSplit);
            UNIT_ASSERT_VALUES_EQUAL(child.Data, "child");
            UNIT_ASSERT_VALUES_EQUAL_C(reader->GetReceivedMessages().size(), 1, "read_from must skip the parent message, received [" << DumpMessages(reader->GetReceivedMessages()) << "]");
            WaitCommitAcknowledgement(*reader, PartitionAfterFirstSplit, child.Offset + 1);
            WaitConsumerCommittedOffsets(setup, {{ParentPartition, 1}, {PartitionAfterFirstSplit, child.Offset + 1}});
            reader->Close();
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

        // The skipped parents are not remembered anywhere between sessions: the next session finds them again.
        Y_UNIT_TEST_TWIN(ChildCommitAfterReconnect, AutoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSplitSetup();
            const TInstant readFrom = WriteThroughTwoSplits(setup);

            {
                std::shared_ptr<ITestReadSession> reader = StartReadSession(setup, AutoPartitioningSupport, false, readFrom);
                const MsgInfo child = WaitForMessage(*reader, PartitionAfterSecondSplit);
                UNIT_ASSERT_VALUES_EQUAL(child.Data, "child-4");
                reader->Close();
            }

            std::shared_ptr<ITestReadSession> reader = StartReadSession(setup, AutoPartitioningSupport, true, readFrom);
            const MsgInfo child = WaitForMessage(*reader, PartitionAfterSecondSplit);
            UNIT_ASSERT_VALUES_EQUAL(child.Data, "child-4");
            UNIT_ASSERT_VALUES_EQUAL_C(reader->GetReceivedMessages().size(), 1, DumpMessages(reader->GetReceivedMessages()));
            WaitCommitAcknowledgement(*reader, PartitionAfterSecondSplit, child.Offset + 1);
            WaitConsumerCommittedOffsets(setup, {{ParentPartition, 1}, {PartitionAfterFirstSplit, 1}, {PartitionAfterSecondSplit, child.Offset + 1}});
            reader->Close();
        }

        Y_UNIT_TEST_TWIN(ChildCommitAfterReaderStartedBeforeSplit, AutoPartitioningSupport) {
            CheckChildCommitAfterReaderStartedBeforeSplit(AutoPartitioningSupport, false);
        }

        // The session lists the parent and the child, so the child is locked while the parent message is still uncommitted.
        // read_from then skips that message. The child CommitAcknowledgement must still arrive and commit the parent.
        Y_UNIT_TEST_TWIN(ExplicitChildLockedBeforeParentSkipped, AutoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSplitSetup();
            WriteMessage(setup, "parent-0", 1);
            // Blob write timestamps have second precision.
            Sleep(TDuration::Seconds(1));
            const TInstant readFrom = TInstant::Now();
            ui64 txId = 1006;
            SplitPartition(setup, ++txId, ParentPartition, "a");
            Sleep(TDuration::Seconds(1));
            WriteToPartition(setup, "child", PartitionAfterFirstSplit);

            std::shared_ptr<ITestReadSession> reader = CreateTestReadSession({
                .Name = "reader",
                .Setup = setup,
                .Sdk = SdkVersion::Topic,
                .AutoCommit = false,
                .Partitions = {ParentPartition, PartitionAfterFirstSplit},
                .AutoPartitioningSupport = AutoPartitioningSupport,
                .ReadFromTimestamp = readFrom,
            });
            reader->Run();

            const MsgInfo child = WaitForMessage(*reader, PartitionAfterFirstSplit);
            UNIT_ASSERT_VALUES_EQUAL(child.Data, "child");
            UNIT_ASSERT_VALUES_EQUAL_C(reader->GetReceivedMessages().size(), 1, "read_from must skip the parent message, received [" << DumpMessages(reader->GetReceivedMessages()) << "]");
            child.Msg->Commit();
            WaitCommitAcknowledgement(*reader, PartitionAfterFirstSplit, child.Offset + 1);
            WaitConsumerCommittedOffsets(setup, {{ParentPartition, 1}, {PartitionAfterFirstSplit, child.Offset + 1}});
            reader->Close();
        }

        Y_UNIT_TEST_TWIN(ChildCommitAfterReaderStartedBeforeSplit_DirectRead, AutoPartitioningSupport) {
            CheckChildCommitAfterReaderStartedBeforeSplit(AutoPartitioningSupport, true);
        }

        // Partition 2 is empty, so only partition 0 delivered data. The child of 2 must wait for that grandparent commit.
        Y_UNIT_TEST(ChildCommitWaitsForDeliveredGrandparent) {
            TTopicSdkTestSetup setup = CreateSplitSetup();
            ui64 txId = 1006;
            WriteMessage(setup, "parent-0", 1);
            SplitPartition(setup, ++txId, ParentPartition, "a");
            SplitPartition(setup, ++txId, PartitionAfterFirstSplit, "d");
            WriteMessage(setup, "child-4", 2);

            std::shared_ptr<ITestReadSession> reader = StartReadSession(setup, true, false, std::nullopt);
            const MsgInfo parent = WaitForMessage(*reader, ParentPartition);
            UNIT_ASSERT_VALUES_EQUAL(parent.Data, "parent-0");
            const MsgInfo child = WaitForMessage(*reader, PartitionAfterSecondSplit);
            UNIT_ASSERT_VALUES_EQUAL(child.Data, "child-4");
            child.Msg->Commit();
            UNIT_ASSERT_C(!WaitFor(TDuration::Seconds(3), [&]() { return AcknowledgedOffset(*reader, PartitionAfterSecondSplit) > 0; }), "child CommitAcknowledgement arrived before the delivered grandparent was committed");
            parent.Msg->Commit();
            WaitCommitAcknowledgement(*reader, PartitionAfterSecondSplit, child.Offset + 1);
            reader->Close();
        }

        // Reader A requests the parent and the child, but another session keeps the parent. A's child commit must not wait for that parent.
        Y_UNIT_TEST_TWIN(ExplicitChildParentOnAnotherReader, AutoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSplitSetup();
            WriteMessage(setup, "parent-0", 1);
            ui64 txId = 1006;
            SplitPartition(setup, ++txId, ParentPartition, "a");
            WriteToPartition(setup, "child", PartitionAfterFirstSplit);

            std::shared_ptr<ITestReadSession> parentReader = CreateTestReadSession({
                .Name = "parent-reader",
                .Setup = setup,
                .Sdk = SdkVersion::Topic,
                .AutoCommit = false,
                .Partitions = {ParentPartition},
                .AutoPartitioningSupport = AutoPartitioningSupport,
            });
            parentReader->Run();
            const MsgInfo parent = WaitForMessage(*parentReader, ParentPartition);
            parent.Msg->Commit();
            WaitCommitAcknowledgement(*parentReader, ParentPartition, parent.Offset + 1);

            std::shared_ptr<ITestReadSession> childReader = CreateTestReadSession({
                .Name = "child-reader",
                .Setup = setup,
                .Sdk = SdkVersion::Topic,
                .AutoCommit = false,
                .Partitions = {ParentPartition, PartitionAfterFirstSplit},
                .AutoPartitioningSupport = AutoPartitioningSupport,
            });
            childReader->Run();
            const MsgInfo child = WaitForMessage(*childReader, PartitionAfterFirstSplit);
            UNIT_ASSERT_VALUES_EQUAL(child.Data, "child");
            UNIT_ASSERT_C(!SessionSawPartition(*childReader, ParentPartition), "child session was given the parent partition");
            child.Msg->Commit();
            WaitCommitAcknowledgement(*childReader, PartitionAfterFirstSplit, child.Offset + 1);
            childReader->Close();
            parentReader->Close();
        }

        // Without auto-partitioning support the children of a skipped parent are balanced on their own,
        // so the reader of a child may be a session that has never seen the parent.
        Y_UNIT_TEST(ChildCommitOnAnotherReader_NoAutoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSplitSetup();
            WriteMessage(setup, "parent-0", 1);
            ui64 txId = 1006;
            SplitPartition(setup, ++txId, ParentPartition, "a");
            const TInstant readFrom = TInstant::Now();
            WriteToPartition(setup, "child-1", 1);
            WriteToPartition(setup, "child-2", 2);

            // The explicit partition pins the parent to its own session, the children go to the other one.
            std::shared_ptr<ITestReadSession> parentReader = CreateTestReadSession({
                .Name = "parent-reader",
                .Setup = setup,
                .Sdk = SdkVersion::Topic,
                .AutoCommit = false,
                .Partitions = {ParentPartition},
                .AutoPartitioningSupport = false,
                .ReadFromTimestamp = readFrom,
            });
            parentReader->Run();
            UNIT_ASSERT_C(WaitForPartitionSession(*parentReader, ParentPartition), "parent partition session did not start");

            std::shared_ptr<ITestReadSession> childReader = StartReadSession(setup, false, false, readFrom);
            std::vector<MsgInfo> received;
            const bool gotChild = WaitFor(TDuration::Seconds(30), [&]() { received = childReader->GetReceivedMessages(); return !received.empty(); });
            UNIT_ASSERT_C(gotChild, "no child message, received [" << DumpMessages(received) << "]");
            UNIT_ASSERT_C(!SessionSawPartition(*childReader, ParentPartition), "child session was given the parent partition");
            const MsgInfo child = received.front();
            child.Msg->Commit();
            WaitCommitAcknowledgement(*childReader, child.PartitionId, child.Offset + 1);
            WaitConsumerCommittedOffsets(setup, {{ParentPartition, 1}, {child.PartitionId, child.Offset + 1}});
            childReader->Close();
            parentReader->Close();
        }

    } // Y_UNIT_TEST_SUITE(SplitReadFromTimestamp)

} // namespace NKikimr
