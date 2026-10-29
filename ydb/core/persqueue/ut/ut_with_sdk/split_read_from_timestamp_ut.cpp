#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/mapfindptr.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>

#include <google/protobuf/util/time_util.h>

#include <grpcpp/create_channel.h>

#include <chrono>

namespace NKikimr {

    using namespace NYdb::NTopic;
    using namespace NYdb::NTopic::NTests;
    using namespace NKikimr::NPQ::NTest;

    namespace {

        constexpr TDuration RetentionPeriod = TDuration::Seconds(15);
        constexpr ui32 ParentPartition = 0;
        constexpr ui32 PartitionAfterFirstSplit = 2;
        constexpr ui32 PartitionAfterSecondSplit = 4;
        const TString Producer = "producer-1";

        struct TPartitionOffset {
            ui64 Start;
            ui64 End;
        };

        TString DumpMessages(const std::vector<MsgInfo>& messages) {
            TStringBuilder out;
            for (const MsgInfo& message : messages) {
                out << message.PartitionId << ":" << message.SeqNo << ":" << message.Data << " ";
            }
            return out;
        }

        TString DumpConsumerOffsets(TTopicClient& client) {
            TDescribeConsumerResult describe = client.DescribeConsumer(TEST_TOPIC, TEST_CONSUMER, TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
            if (!describe.IsSuccess()) {
                return describe.GetIssues().ToString();
            }
            TStringBuilder out;
            for (const TPartitionInfo& partition : describe.GetConsumerDescription().GetPartitions()) {
                const std::optional<TPartitionConsumerStats>& stats = partition.GetPartitionConsumerStats();
                out << partition.GetPartitionId() << ":";
                if (stats.has_value()) {
                    out << stats->GetCommittedOffset();
                } else {
                    out << "-";
                }
                out << " ";
            }
            return out;
        }

        TString DumpOffsets(const THashMap<ui64, TPartitionOffset>& offsets) {
            TVector<ui64> ids;
            for (const auto& [id, offset] : offsets) {
                Y_UNUSED(offset);
                ids.push_back(id);
            }
            Sort(ids);
            TStringBuilder out;
            for (ui64 id : ids) {
                const TPartitionOffset* offset = MapFindPtr(offsets, id);
                out << id << ":" << offset->Start << "-" << offset->End << " ";
            }
            return out;
        }

        THashMap<ui64, TPartitionOffset> ReadOffsets(TTopicClient& client) {
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
            while (true) {
                TDescribeTopicResult describe = client.DescribeTopic(TEST_TOPIC, TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
                UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
                THashMap<ui64, TPartitionOffset> offsets;
                bool complete = true;
                for (const TPartitionInfo& partition : describe.GetTopicDescription().GetPartitions()) {
                    if (!partition.GetPartitionStats().has_value()) {
                        complete = false;
                        break;
                    }
                    const TPartitionStats& stats = partition.GetPartitionStats().value();
                    offsets[partition.GetPartitionId()] = TPartitionOffset{
                        .Start = stats.GetStartOffset(),
                        .End = stats.GetEndOffset(),
                    };
                }
                if (complete && !offsets.empty()) {
                    return offsets;
                }
                UNIT_ASSERT_C(TInstant::Now() < deadline, "partition stats are not available");
                Sleep(TDuration::MilliSeconds(200));
            }
        }

        ui64 ConsumerCommittedOffset(TTopicClient& client, ui64 partitionId) {
            TDescribeConsumerResult describe = client.DescribeConsumer(TEST_TOPIC, TEST_CONSUMER, TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
            UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
            for (const TPartitionInfo& partition : describe.GetConsumerDescription().GetPartitions()) {
                if (partition.GetPartitionId() != partitionId) {
                    continue;
                }
                const std::optional<TPartitionConsumerStats>& stats = partition.GetPartitionConsumerStats();
                return stats.has_value() ? stats->GetCommittedOffset() : 0;
            }
            UNIT_FAIL("partition " << partitionId << " is missing in consumer description");
            return 0;
        }

        void AssertConsumerOffset(TTopicClient& client, ui64 partitionId, ui64 expected) {
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
            ui64 actual = Max<ui64>();
            while (TInstant::Now() < deadline) {
                actual = ConsumerCommittedOffset(client, partitionId);
                if (actual == expected) {
                    return;
                }
                Sleep(TDuration::MilliSeconds(200));
            }
            UNIT_FAIL("partition " << partitionId << " committed offset " << actual << ", expected " << expected << ", consumer " << DumpConsumerOffsets(client));
        }

        void AssertEndOffset(TTopicClient& client, ui32 partitionId, ui64 endOffset) {
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
            ui64 actual = Max<ui64>();
            while (TInstant::Now() < deadline) {
                const THashMap<ui64, TPartitionOffset> offsets = ReadOffsets(client);
                const TPartitionOffset* offset = MapFindPtr(offsets, ui64(partitionId));
                UNIT_ASSERT_C(offset, "partition " << partitionId << " is missing, offsets " << DumpOffsets(offsets));
                actual = offset->End;
                if (actual == endOffset) {
                    return;
                }
                Sleep(TDuration::MilliSeconds(200));
            }
            UNIT_FAIL("partition " << partitionId << " end offset " << actual << ", expected " << endOffset);
        }

        // Parents keep messages older than read_from. The client does not receive them and does not commit them.
        // CommitAcknowledgement for the child must still arrive.
        void CommitAcknowledgementAfterReadFrom(bool autoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSetup();
            setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100, RetentionPeriod);

            TTopicClient client = setup.MakeClient();
            ui64 seqNo = 1;
            std::shared_ptr<ISimpleBlockingWriteSession> session = CreateWriteSession(client, Producer, std::nullopt, TString{TEST_TOPIC}, false);

            UNIT_ASSERT(session->Write(Msg("parent-0", seqNo++)));
            AssertEndOffset(client, ParentPartition, 1);

            ui64 txId = 1006;
            SplitPartition(setup, ++txId, ParentPartition, "a");

            UNIT_ASSERT(session->Write(Msg("parent-2", seqNo++)));
            AssertEndOffset(client, PartitionAfterFirstSplit, 1);

            SplitPartition(setup, ++txId, PartitionAfterFirstSplit, "d");

            const TInstant readFrom = TInstant::Now();
            UNIT_ASSERT(session->Write(Msg("child-4", seqNo++)));
            AssertEndOffset(client, PartitionAfterSecondSplit, 1);

            std::shared_ptr<ITestReadSession> readSession = CreateTestReadSession({
                .Name = autoPartitioningSupport ? "commit-ack-aware" : "commit-ack",
                .Setup = setup,
                .Sdk = SdkVersion::Topic,
                .ExpectedMessagesCount = 1,
                .AutoCommit = true,
                .AutoPartitioningSupport = autoPartitioningSupport,
                .ReadFromTimestamp = readFrom,
            });
            readSession->Run();

            std::vector<MsgInfo> received;
            const TInstant readDeadline = TInstant::Now() + TDuration::Seconds(30);
            while (TInstant::Now() < readDeadline && received.empty()) {
                received = readSession->GetReceivedMessages();
                Sleep(TDuration::MilliSeconds(200));
            }

            const ui64 expectedCommitted = received.empty() ? 0 : received[0].Offset + 1;
            ui64 committed = 0;
            const TInstant commitDeadline = TInstant::Now() + TDuration::Seconds(15);
            while (TInstant::Now() < commitDeadline) {
                const std::unordered_map<ui64, ui64> acks = readSession->GetCommittedOffsets();
                const ui64* ack = MapFindPtr(acks, static_cast<ui64>(PartitionAfterSecondSplit));
                committed = ack ? *ack : 0;
                Cerr << ">>>>> child committed " << committed << " expected " << expectedCommitted
                     << " consumer " << DumpConsumerOffsets(client) << Endl;
                if (committed >= expectedCommitted && expectedCommitted > 0) {
                    break;
                }
                Sleep(TDuration::MilliSeconds(200));
            }

            received = readSession->GetReceivedMessages();
            UNIT_ASSERT_C(received.size() == 1, "received [" << DumpMessages(received) << "], consumer " << DumpConsumerOffsets(client));
            UNIT_ASSERT_VALUES_EQUAL_C(received[0].PartitionId, PartitionAfterSecondSplit, DumpMessages(received));
            UNIT_ASSERT_VALUES_EQUAL(received[0].Data, "child-4");
            UNIT_ASSERT_C(committed >= received[0].Offset + 1,
                "no CommitAcknowledgement for partition " << PartitionAfterSecondSplit
                << ", committed " << committed << ", expected " << received[0].Offset + 1
                << ", consumer " << DumpConsumerOffsets(client)
                << ", topic " << DumpOffsets(ReadOffsets(client)));
            // read_from skipped every parent message, so the first child commit also commits both ancestors to their end.
            AssertConsumerOffset(client, ParentPartition, 1);
            AssertConsumerOffset(client, PartitionAfterFirstSplit, 1);
            AssertConsumerOffset(client, PartitionAfterSecondSplit, received[0].Offset + 1);
            readSession->Close();

            // A later session without read_from does not get the skipped parent messages back.
            UNIT_ASSERT(session->Write(Msg("child-4-next", seqNo++)));
            std::shared_ptr<ITestReadSession> nextSession = CreateTestReadSession({
                .Name = autoPartitioningSupport ? "next-aware" : "next",
                .Setup = setup,
                .Sdk = SdkVersion::Topic,
                .ExpectedMessagesCount = 1,
                .AutoCommit = false,
                .AutoPartitioningSupport = autoPartitioningSupport,
            });
            nextSession->Run();
            nextSession->WaitAllMessages();
            received = nextSession->GetReceivedMessages();
            UNIT_ASSERT_C(received.size() == 1, "received [" << DumpMessages(received) << "]");
            UNIT_ASSERT_VALUES_EQUAL(received[0].Data, "child-4-next");
            nextSession->Close();

            UNIT_ASSERT(session->Close(TDuration::Seconds(5)));
        }

        const MsgInfo* FindMessage(const std::vector<MsgInfo>& messages, ui64 partitionId) {
            const MsgInfo* found = nullptr;
            for (const MsgInfo& message : messages) {
                if (message.PartitionId == partitionId) {
                    found = &message;
                }
            }
            return found;
        }

        void CommitMessage(const MsgInfo& message) {
            UNIT_ASSERT(message.Msg);
            message.Msg->Commit();
        }

        ui64 CommittedOffset(ITestReadSession& session, ui64 partitionId) {
            const std::unordered_map<ui64, ui64> acks = session.GetCommittedOffsets();
            const ui64* ack = MapFindPtr(acks, partitionId);
            return ack ? *ack : 0;
        }

        std::vector<MsgInfo> WaitForMessage(ITestReadSession& session, ui64 partitionId, TDuration timeout) {
            const TInstant deadline = TInstant::Now() + timeout;
            std::vector<MsgInfo> received;
            while (TInstant::Now() < deadline) {
                received = session.GetReceivedMessages();
                if (FindMessage(received, partitionId)) {
                    return received;
                }
                Sleep(TDuration::MilliSeconds(200));
            }
            return received;
        }

        // The parent message was delivered. Its commit is what releases the child commit.
        void ChildCommitWaitsForDeliveredParent(bool autoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSetup();
            setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100, RetentionPeriod);

            TTopicClient client = setup.MakeClient();
            ui64 seqNo = 1;
            std::shared_ptr<ISimpleBlockingWriteSession> session = CreateWriteSession(client, Producer, std::nullopt, TString{TEST_TOPIC}, false);

            UNIT_ASSERT(session->Write(Msg("parent-0", seqNo++)));
            AssertEndOffset(client, ParentPartition, 1);

            ui64 txId = 1006;
            SplitPartition(setup, ++txId, ParentPartition, "a");

            UNIT_ASSERT(session->Write(Msg("child-2", seqNo++)));
            AssertEndOffset(client, PartitionAfterFirstSplit, 1);

            std::shared_ptr<ITestReadSession> readSession = CreateTestReadSession({
                .Name = autoPartitioningSupport ? "parent-commit-aware" : "parent-commit",
                .Setup = setup,
                .Sdk = SdkVersion::Topic,
                .ExpectedMessagesCount = 2,
                .AutoCommit = false,
                .AutoPartitioningSupport = autoPartitioningSupport,
            });
            readSession->Run();

            std::vector<MsgInfo> received = WaitForMessage(*readSession, ParentPartition, TDuration::Seconds(30));
            const MsgInfo* parent = FindMessage(received, ParentPartition);
            UNIT_ASSERT_C(parent, "parent message was not read, received [" << DumpMessages(received) << "]");

            if (autoPartitioningSupport) {
                received = WaitForMessage(*readSession, PartitionAfterFirstSplit, TDuration::Seconds(30));
                const MsgInfo* child = FindMessage(received, PartitionAfterFirstSplit);
                UNIT_ASSERT_C(child, "child message was not read, received [" << DumpMessages(received) << "]");
                CommitMessage(*child);

                const TInstant blockedUntil = TInstant::Now() + TDuration::Seconds(3);
                ui64 childCommitted = 0;
                while (TInstant::Now() < blockedUntil) {
                    childCommitted = CommittedOffset(*readSession, PartitionAfterFirstSplit);
                    if (childCommitted > 0) {
                        break;
                    }
                    Sleep(TDuration::MilliSeconds(200));
                }
                UNIT_ASSERT_VALUES_EQUAL_C(childCommitted, 0,
                    "child CommitAcknowledgement arrived before the delivered parent was committed, consumer " << DumpConsumerOffsets(client));
            } else {
                const TInstant blockedUntil = TInstant::Now() + TDuration::Seconds(3);
                while (TInstant::Now() < blockedUntil) {
                    received = readSession->GetReceivedMessages();
                    UNIT_ASSERT_C(!FindMessage(received, PartitionAfterFirstSplit),
                        "child was read before the delivered parent was committed, received [" << DumpMessages(received) << "]");
                    Sleep(TDuration::MilliSeconds(200));
                }
            }

            received = readSession->GetReceivedMessages();
            parent = FindMessage(received, ParentPartition);
            UNIT_ASSERT(parent);
            CommitMessage(*parent);

            received = WaitForMessage(*readSession, PartitionAfterFirstSplit, TDuration::Seconds(30));
            const MsgInfo* child = FindMessage(received, PartitionAfterFirstSplit);
            UNIT_ASSERT_C(child, "child message was not read after the parent commit, received [" << DumpMessages(received) << "]");
            if (!autoPartitioningSupport) {
                CommitMessage(*child);
            }

            const ui64 expectedChildCommitted = child->Offset + 1;
            ui64 childCommitted = 0;
            const TInstant commitDeadline = TInstant::Now() + TDuration::Seconds(15);
            while (TInstant::Now() < commitDeadline) {
                childCommitted = CommittedOffset(*readSession, PartitionAfterFirstSplit);
                if (childCommitted >= expectedChildCommitted) {
                    break;
                }
                Sleep(TDuration::MilliSeconds(200));
            }
            UNIT_ASSERT_C(childCommitted >= expectedChildCommitted,
                "no CommitAcknowledgement for partition " << PartitionAfterFirstSplit
                << ", committed " << childCommitted << ", expected " << expectedChildCommitted
                << ", consumer " << DumpConsumerOffsets(client));

            UNIT_ASSERT(session->Close(TDuration::Seconds(5)));
            readSession->Close();
        }

        // Partitions 0 and 2 get one message each before read_from, partition 4 gets "child-4" after it.
        TInstant WriteThroughTwoSplits(TTopicSdkTestSetup& setup) {
            TTopicClient client = setup.MakeClient();
            std::shared_ptr<ISimpleBlockingWriteSession> session = CreateWriteSession(client, Producer, std::nullopt, TString{TEST_TOPIC}, false);
            ui64 txId = 1006;
            UNIT_ASSERT(session->Write(Msg("parent-0", 1)));
            AssertEndOffset(client, ParentPartition, 1);
            SplitPartition(setup, ++txId, ParentPartition, "a");
            UNIT_ASSERT(session->Write(Msg("parent-2", 2)));
            AssertEndOffset(client, PartitionAfterFirstSplit, 1);
            SplitPartition(setup, ++txId, PartitionAfterFirstSplit, "d");
            const TInstant readFrom = TInstant::Now();
            UNIT_ASSERT(session->Write(Msg("child-4", 3)));
            AssertEndOffset(client, PartitionAfterSecondSplit, 1);
            UNIT_ASSERT(session->Close(TDuration::Seconds(5)));
            return readFrom;
        }

        struct TRawMessage {
            ui64 PartitionId;
            ui64 Offset;
            TString Data;
        };

        // Topic API read stream without the SDK: it confirms partition sessions as the SDK does, but commits only on request.
        class TRawReadSession {
            using TFromClient = Ydb::Topic::StreamReadMessage::FromClient;
            using TFromServer = Ydb::Topic::StreamReadMessage::FromServer;

            struct TPartitionSession {
                ui64 PartitionId;
                ui64 CommittedOffset;
            };

        public:
            TRawReadSession(TTopicSdkTestSetup& setup, bool autoPartitioningSupport, TInstant readFrom)
                : Stub(Ydb::Topic::V1::TopicService::NewStub(grpc::CreateChannel(TStringBuilder() << "localhost:" << setup.GetServer().GrpcPort, grpc::InsecureChannelCredentials())))
            {
                Context.AddMetadata(NYdb::YDB_DATABASE_HEADER, TString{setup.GetDatabase()});
                Context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(60));
                Stream = Stub->StreamRead(&Context);

                TFromClient init;
                init.mutable_init_request()->set_consumer(TEST_CONSUMER);
                init.mutable_init_request()->set_auto_partitioning_support(autoPartitioningSupport);
                Ydb::Topic::StreamReadMessage::InitRequest::TopicReadSettings* topic = init.mutable_init_request()->add_topics_read_settings();
                topic->set_path(TEST_TOPIC);
                *topic->mutable_read_from() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(readFrom.MilliSeconds());
                Send(init);
                const TFromServer response = Receive("init response");
                UNIT_ASSERT_C(response.has_init_response(), response);

                TFromClient read;
                read.mutable_read_request()->set_bytes_size(1_MB);
                Send(read);
            }

            ~TRawReadSession() {
                Context.TryCancel();
            }

            const std::vector<TRawMessage>& GetMessages() const {
                return Messages;
            }

            TRawMessage ReadMessage(ui64 partitionId) {
                const auto received = [&]() { return FindIfPtr(Messages, [&](const TRawMessage& message) { return message.PartitionId == partitionId; }); };
                while (!received()) {
                    Process(Receive(TStringBuilder() << "a message from partition " << partitionId));
                }
                return *received();
            }

            void Commit(const TRawMessage& message) {
                const auto it = FindIf(PartitionSessions, [&](const auto& session) { return session.second.PartitionId == message.PartitionId; });
                UNIT_ASSERT_C(it != PartitionSessions.end(), "no partition session for partition " << message.PartitionId);
                TFromClient commit;
                Ydb::Topic::StreamReadMessage::CommitOffsetRequest::PartitionCommitOffset* offsets = commit.mutable_commit_offset_request()->add_commit_offsets();
                offsets->set_partition_session_id(it->first);
                Ydb::Topic::OffsetsRange* range = offsets->add_offsets();
                range->set_start(it->second.CommittedOffset);
                range->set_end(message.Offset + 1);
                Send(commit);
            }

            void WaitCommitted(ui64 partitionId, ui64 offset) {
                while (Committed.Value(partitionId, 0) < offset) {
                    Process(Receive(TStringBuilder() << "CommitOffsetResponse for partition " << partitionId << " up to " << offset));
                }
            }

        private:
            void Send(const TFromClient& message) {
                UNIT_ASSERT_C(Stream->Write(message), "write to the read stream failed: " << message);
            }

            TFromServer Receive(const TString& expected) {
                TFromServer message;
                UNIT_ASSERT_C(Stream->Read(&message), "read stream closed while waiting for " << expected);
                UNIT_ASSERT_VALUES_EQUAL_C(message.status(), Ydb::StatusIds::SUCCESS, message);
                return message;
            }

            void Process(const TFromServer& message) {
                switch (message.server_message_case()) {
                    case TFromServer::kStartPartitionSessionRequest: {
                        const Ydb::Topic::StreamReadMessage::StartPartitionSessionRequest& request = message.start_partition_session_request();
                        const i64 id = request.partition_session().partition_session_id();
                        PartitionSessions[id] = TPartitionSession{
                            .PartitionId = static_cast<ui64>(request.partition_session().partition_id()),
                            .CommittedOffset = static_cast<ui64>(request.committed_offset()),
                        };
                        TFromClient response;
                        response.mutable_start_partition_session_response()->set_partition_session_id(id);
                        Send(response);
                        break;
                    }
                    case TFromServer::kStopPartitionSessionRequest: {
                        const i64 id = message.stop_partition_session_request().partition_session_id();
                        if (message.stop_partition_session_request().graceful()) {
                            TFromClient response;
                            response.mutable_stop_partition_session_response()->set_partition_session_id(id);
                            Send(response);
                        } else {
                            PartitionSessions.erase(id);
                        }
                        break;
                    }
                    case TFromServer::kReadResponse: {
                        for (const Ydb::Topic::StreamReadMessage::ReadResponse::PartitionData& data : message.read_response().partition_data()) {
                            const TPartitionSession* session = MapFindPtr(PartitionSessions, data.partition_session_id());
                            UNIT_ASSERT_C(session, "data for unknown partition session " << data.partition_session_id());
                            for (const Ydb::Topic::StreamReadMessage::ReadResponse::Batch& batch : data.batches()) {
                                for (const Ydb::Topic::StreamReadMessage::ReadResponse::MessageData& messageData : batch.message_data()) {
                                    Messages.push_back(TRawMessage{
                                        .PartitionId = session->PartitionId,
                                        .Offset = static_cast<ui64>(messageData.offset()),
                                        .Data = messageData.data(),
                                    });
                                }
                            }
                        }
                        TFromClient read;
                        read.mutable_read_request()->set_bytes_size(message.read_response().bytes_size());
                        Send(read);
                        break;
                    }
                    case TFromServer::kCommitOffsetResponse: {
                        for (const Ydb::Topic::StreamReadMessage::CommitOffsetResponse::PartitionCommittedOffset& committed : message.commit_offset_response().partitions_committed_offsets()) {
                            const TPartitionSession* session = MapFindPtr(PartitionSessions, committed.partition_session_id());
                            UNIT_ASSERT_C(session, "commit for unknown partition session " << committed.partition_session_id());
                            Committed[session->PartitionId] = committed.committed_offset();
                        }
                        break;
                    }
                    default:
                        break;
                }
            }

            std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> Stub;
            grpc::ClientContext Context;
            std::unique_ptr<grpc::ClientReaderWriter<TFromClient, TFromServer>> Stream;
            THashMap<i64, TPartitionSession> PartitionSessions;
            THashMap<ui64, ui64> Committed;
            std::vector<TRawMessage> Messages;
        };

        void CommitChildAndCheckSkippedParents(TTopicSdkTestSetup& setup, TRawReadSession& reader) {
            const TRawMessage child = reader.ReadMessage(PartitionAfterSecondSplit);
            UNIT_ASSERT_VALUES_EQUAL(child.Data, "child-4");
            UNIT_ASSERT_VALUES_EQUAL_C(reader.GetMessages().size(), 1, "read_from must skip every parent message");
            reader.Commit(child);
            reader.WaitCommitted(PartitionAfterSecondSplit, child.Offset + 1);

            TTopicClient client = setup.MakeClient();
            AssertConsumerOffset(client, ParentPartition, 1);
            AssertConsumerOffset(client, PartitionAfterFirstSplit, 1);
            AssertConsumerOffset(client, PartitionAfterSecondSplit, child.Offset + 1);
        }

        void RawChildCommitCommitsSkippedParents(bool autoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSetup();
            setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100, RetentionPeriod);
            const TInstant readFrom = WriteThroughTwoSplits(setup);

            TRawReadSession reader(setup, autoPartitioningSupport, readFrom);
            CommitChildAndCheckSkippedParents(setup, reader);
        }

        // The skipped parents are not remembered anywhere between sessions: the next session finds them again.
        void RawChildCommitAfterReconnect(bool autoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSetup();
            setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100, RetentionPeriod);
            const TInstant readFrom = WriteThroughTwoSplits(setup);

            {
                TRawReadSession reader(setup, autoPartitioningSupport, readFrom);
                reader.ReadMessage(PartitionAfterSecondSplit);
            }
            TRawReadSession reader(setup, autoPartitioningSupport, readFrom);
            CommitChildAndCheckSkippedParents(setup, reader);
        }

    } // namespace

    Y_UNIT_TEST_SUITE(SplitReadFromTimestamp) {

        Y_UNIT_TEST(CommitAcknowledgementAfterReadFrom) {
            CommitAcknowledgementAfterReadFrom(true);
        }

        Y_UNIT_TEST(CommitAcknowledgementAfterReadFrom_NoAutoPartitioningSupport) {
            CommitAcknowledgementAfterReadFrom(false);
        }

        Y_UNIT_TEST(ChildCommitWaitsForDeliveredParent) {
            ChildCommitWaitsForDeliveredParent(true);
        }

        Y_UNIT_TEST(ChildCommitWaitsForDeliveredParent_NoAutoPartitioningSupport) {
            ChildCommitWaitsForDeliveredParent(false);
        }

    } // Y_UNIT_TEST_SUITE(SplitReadFromTimestamp)

    Y_UNIT_TEST_SUITE(SplitReadFromTimestampRawStream) {

        Y_UNIT_TEST(ChildCommitCommitsSkippedParents) {
            RawChildCommitCommitsSkippedParents(true);
        }

        Y_UNIT_TEST(ChildCommitCommitsSkippedParents_NoAutoPartitioningSupport) {
            RawChildCommitCommitsSkippedParents(false);
        }

        Y_UNIT_TEST(ChildCommitAfterReconnect) {
            RawChildCommitAfterReconnect(true);
        }

        Y_UNIT_TEST(ChildCommitAfterReconnect_NoAutoPartitioningSupport) {
            RawChildCommitAfterReconnect(false);
        }

    } // Y_UNIT_TEST_SUITE(SplitReadFromTimestampRawStream)

} // namespace NKikimr
