#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/mapfindptr.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>

#include <google/protobuf/util/time_util.h>

#include <grpcpp/create_channel.h>

#include <chrono>
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

            TRawReadSession reader(setup, AutoPartitioningSupport, readFrom);
            CommitChildAndCheckSkippedParents(setup, reader);
        }

        // The skipped parents are not remembered anywhere between sessions: the next session finds them again.
        Y_UNIT_TEST_TWIN(ChildCommitAfterReconnect, AutoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateSplitSetup();
            const TInstant readFrom = WriteThroughTwoSplits(setup);

            {
                TRawReadSession reader(setup, AutoPartitioningSupport, readFrom);
                reader.ReadMessage(PartitionAfterSecondSplit);
            }
            TRawReadSession reader(setup, AutoPartitioningSupport, readFrom);
            CommitChildAndCheckSkippedParents(setup, reader);
        }

    } // Y_UNIT_TEST_SUITE(SplitReadFromTimestampRawStream)

} // namespace NKikimr
