#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/mapfindptr.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#include <grpcpp/create_channel.h>

#include <chrono>

namespace NKikimr::NPersQueueTests {

    using namespace NYdb::NTopic;
    using namespace NYdb::NTopic::NTests;
    using namespace NKikimr::NPQ::NTest;

    namespace {

        using TFromClient = Ydb::Topic::StreamReadMessage::FromClient;
        using TFromServer = Ydb::Topic::StreamReadMessage::FromServer;

        // Partition ids are local to a topic: both topics have partition 0.
        const TString TopicA = "topic-a";
        const TString TopicB = "topic-b";

        TTopicSdkTestSetup CreateTwoTopicsSetup() {
            TTopicSdkTestSetup setup = CreateSetup();
            setup.CreateTopic(TopicA, TEST_CONSUMER, 1, 100);
            setup.CreateTopic(TopicB, TEST_CONSUMER, 1, 100);
            return setup;
        }

        void Write(TTopicSdkTestSetup& setup, const TString& topic, ui32 partition, const TString& data) {
            TTopicClient client = setup.MakeClient();
            std::shared_ptr<ISimpleBlockingWriteSession> session = CreateWriteSession(client, TStringBuilder() << "producer-" << partition, partition, topic, false);
            UNIT_ASSERT(session->Write(Msg(data, 1)));
            UNIT_ASSERT(session->Close(TDuration::Seconds(10)));
        }

        // A Topic API read stream of topics A and B. The test sends every client message itself.
        // Next() describes server messages as "start topic-a/0", "end topic-a/0", "data topic-a/1 <message>", "committed topic-a/1 <offset>".
        class TReader {
        public:
            TReader(TTopicSdkTestSetup& setup, bool autoPartitioningSupport, const TVector<i64>& partitionIds)
                : Stub(Ydb::Topic::V1::TopicService::NewStub(grpc::CreateChannel(TStringBuilder() << "localhost:" << setup.GetServer().GrpcPort, grpc::InsecureChannelCredentials())))
            {
                Context.AddMetadata(NYdb::YDB_DATABASE_HEADER, TString{setup.GetDatabase()});
                Context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
                Stream = Stub->StreamRead(&Context);

                TFromClient init;
                init.mutable_init_request()->set_consumer(TEST_CONSUMER);
                init.mutable_init_request()->set_auto_partitioning_support(autoPartitioningSupport);
                for (const TString& topic : {TopicA, TopicB}) {
                    Ydb::Topic::StreamReadMessage::InitRequest::TopicReadSettings* settings = init.mutable_init_request()->add_topics_read_settings();
                    settings->set_path(topic);
                    for (const i64 partitionId : partitionIds) {
                        settings->add_partition_ids(partitionId);
                    }
                }
                Send(init);
                UNIT_ASSERT_VALUES_EQUAL(Next(1), "init");
            }

            ~TReader() {
                Context.TryCancel();
            }

            // Reads server messages until there are `count` events and returns them sorted: the order of arrival does not matter.
            TString Next(size_t count) {
                TVector<TString> events;
                while (events.size() < count) {
                    TFromServer message;
                    if (!Stream->Read(&message)) {
                        events.push_back("nothing more");
                        break;
                    }
                    Describe(message, events);
                }
                Sort(events);
                return JoinSeq("; ", events);
            }

            void Confirm(TStringBuf partition) {
                TFromClient response;
                response.mutable_start_partition_session_response()->set_partition_session_id(Find(partition).Id);
                Send(response);
            }

            void Read() {
                TFromClient request;
                request.mutable_read_request()->set_bytes_size(1_MB);
                Send(request);
            }

            // Commits all messages before `end` that are not committed yet.
            void Commit(TStringBuf partition, i64 end) {
                const TPartitionSession& session = Find(partition);
                TFromClient request;
                Ydb::Topic::StreamReadMessage::CommitOffsetRequest::PartitionCommitOffset* commit = request.mutable_commit_offset_request()->add_commit_offsets();
                commit->set_partition_session_id(session.Id);
                Ydb::Topic::OffsetsRange* range = commit->add_offsets();
                range->set_start(session.CommittedOffset);
                range->set_end(end);
                Send(request);
            }

        private:
            struct TPartitionSession {
                i64 Id;
                i64 CommittedOffset;
            };

            // A failed write shows up as a server error or as "nothing more" in Next().
            void Send(const TFromClient& message) {
                Stream->Write(message);
            }

            TString Name(i64 partitionSessionId) const {
                return Names.Value(partitionSessionId, TString{"unknown"});
            }

            const TPartitionSession& Find(TStringBuf partition) const {
                const TPartitionSession* session = MapFindPtr(Sessions, partition);
                UNIT_ASSERT_C(session, "no StartPartitionSession for " << partition);
                return *session;
            }

            void Describe(const TFromServer& message, TVector<TString>& events) {
                if (message.status() != Ydb::StatusIds::SUCCESS) {
                    events.push_back("error " + message.ShortDebugString());
                    return;
                }
                switch (message.server_message_case()) {
                    case TFromServer::kInitResponse:
                        events.push_back("init");
                        break;
                    case TFromServer::kStartPartitionSessionRequest: {
                        const Ydb::Topic::StreamReadMessage::PartitionSession& session = message.start_partition_session_request().partition_session();
                        const TString name = TStringBuilder() << TStringBuf(session.path()).RAfter('/') << "/" << session.partition_id();
                        Sessions[name] = TPartitionSession{
                            .Id = session.partition_session_id(),
                            .CommittedOffset = message.start_partition_session_request().committed_offset(),
                        };
                        Names[session.partition_session_id()] = name;
                        events.push_back("start " + name);
                        break;
                    }
                    case TFromServer::kEndPartitionSession:
                        events.push_back("end " + Name(message.end_partition_session().partition_session_id()));
                        break;
                    case TFromServer::kReadResponse:
                        for (const Ydb::Topic::StreamReadMessage::ReadResponse::PartitionData& data : message.read_response().partition_data()) {
                            for (const Ydb::Topic::StreamReadMessage::ReadResponse::Batch& batch : data.batches()) {
                                for (const Ydb::Topic::StreamReadMessage::ReadResponse::MessageData& messageData : batch.message_data()) {
                                    events.push_back(TStringBuilder() << "data " << Name(data.partition_session_id()) << " " << messageData.data());
                                }
                            }
                        }
                        break;
                    case TFromServer::kCommitOffsetResponse:
                        for (const Ydb::Topic::StreamReadMessage::CommitOffsetResponse::PartitionCommittedOffset& committed : message.commit_offset_response().partitions_committed_offsets()) {
                            events.push_back(TStringBuilder() << "committed " << Name(committed.partition_session_id()) << " " << committed.committed_offset());
                        }
                        break;
                    default:
                        events.push_back(message.ShortDebugString());
                        break;
                }
            }

            std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> Stub;
            grpc::ClientContext Context;
            std::unique_ptr<grpc::ClientReaderWriter<TFromClient, TFromServer>> Stream;
            THashMap<TString, TPartitionSession> Sessions;
            THashMap<i64, TString> Names;
        };

    } // namespace

    Y_UNIT_TEST_SUITE(ReadSessionMultiTopic) {

        Y_UNIT_TEST(EachSplitPartitionEndsItsOwnSession) {
            TTopicSdkTestSetup setup = CreateTwoTopicsSetup();
            ui64 txId = 1006;
            SplitPartition(setup.GetRuntime(), ++txId, TopicA, 0, "a");
            SplitPartition(setup.GetRuntime(), ++txId, TopicB, 0, "a");

            // The explicit partition keeps the children off the session, so the session holds the same partitions while both parents end.
            TReader reader(setup, /*autoPartitioningSupport=*/true, /*partitionIds=*/{0});
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(2), "start topic-a/0; start topic-b/0");
            reader.Confirm("topic-a/0");
            reader.Confirm("topic-b/0");
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(2), "end topic-a/0; end topic-b/0");
        }

        // Partition 0 of topic B is still being read. It is not a parent of topic-a/1 and must not hold its commit.
        Y_UNIT_TEST(ChildCommitDoesNotWaitForOtherTopic) {
            TTopicSdkTestSetup setup = CreateTwoTopicsSetup();
            Write(setup, TopicA, 0, "a-parent");
            ui64 txId = 1006;
            SplitPartition(setup.GetRuntime(), ++txId, TopicA, 0, "a");
            Write(setup, TopicA, 1, "a-child");

            TReader reader(setup, /*autoPartitioningSupport=*/false, /*partitionIds=*/{});
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(2), "start topic-a/0; start topic-b/0");
            reader.Confirm("topic-a/0");
            reader.Confirm("topic-b/0");
            reader.Read();
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(1), "data topic-a/0 a-parent");
            reader.Commit("topic-a/0", 1);
            // Without auto-partitioning support the children are given out after the parent is committed.
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(3), "committed topic-a/0 1; start topic-a/1; start topic-a/2");
            reader.Confirm("topic-a/1");
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(1), "data topic-a/1 a-child");
            reader.Commit("topic-a/1", 1);
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(1), "committed topic-a/1 1");
        }

        // The commit of topic-b/1 waits for its parent topic-b/0, not for partition 0 of topic A.
        Y_UNIT_TEST(ChildCommitWaitsForItsOwnParent) {
            TTopicSdkTestSetup setup = CreateTwoTopicsSetup();
            Write(setup, TopicA, 0, "a-parent");
            Write(setup, TopicB, 0, "b-parent");
            ui64 txId = 1006;
            SplitPartition(setup.GetRuntime(), ++txId, TopicA, 0, "a");
            SplitPartition(setup.GetRuntime(), ++txId, TopicB, 0, "a");
            Write(setup, TopicA, 1, "a-child");
            Write(setup, TopicB, 1, "b-child");

            TReader reader(setup, /*autoPartitioningSupport=*/true, /*partitionIds=*/{});
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(2), "start topic-a/0; start topic-b/0");
            reader.Confirm("topic-b/0");
            reader.Read();
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(4), "data topic-b/0 b-parent; end topic-b/0; start topic-b/1; start topic-b/2");
            reader.Confirm("topic-b/1");
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(1), "data topic-b/1 b-child");
            // Not acknowledged until topic-b/0 is committed.
            reader.Commit("topic-b/1", 1);

            reader.Confirm("topic-a/0");
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(4), "data topic-a/0 a-parent; end topic-a/0; start topic-a/1; start topic-a/2");
            reader.Commit("topic-a/0", 1);
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(1), "committed topic-a/0 1");
            reader.Confirm("topic-a/1");
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(1), "data topic-a/1 a-child");
            reader.Commit("topic-a/1", 1);
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(1), "committed topic-a/1 1");

            reader.Commit("topic-b/0", 1);
            UNIT_ASSERT_VALUES_EQUAL(reader.Next(2), "committed topic-b/0 1; committed topic-b/1 1");
        }

    } // Y_UNIT_TEST_SUITE(ReadSessionMultiTopic)

} // namespace NKikimr::NPersQueueTests
