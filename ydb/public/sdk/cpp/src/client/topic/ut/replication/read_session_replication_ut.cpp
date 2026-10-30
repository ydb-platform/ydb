#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <chrono>
#include <memory>
#include <string>

namespace NYdb::inline Dev::NTopic::NTests {
    namespace {

        constexpr ui64 FirstOffset = 1000;
        constexpr ui64 EndOffset = FirstOffset + 2;
        const std::string ControlPayload = "replication-offsets";

        class TReplicationReadService: public Ydb::Topic::V1::TopicService::Service {
            using TClientMessage = Ydb::Topic::StreamReadMessage::FromClient;
            using TServerMessage = Ydb::Topic::StreamReadMessage::FromServer;

        public:
            grpc::Status StreamRead(grpc::ServerContext*, grpc::ServerReaderWriter<TServerMessage, TClientMessage>* stream) override {
                TDisjointIntervalTree<ui64> commits;
                TClientMessage request;
                while (stream->Read(&request)) {
                    if (request.has_init_request()) {
                        TServerMessage response;
                        response.set_status(Ydb::StatusIds::SUCCESS);
                        response.mutable_init_response()->set_session_id("replication-test");
                        if (!stream->Write(response)) {
                            break;
                        }
                        response.Clear();
                        response.set_status(Ydb::StatusIds::SUCCESS);
                        auto* start = response.mutable_start_partition_session_request();
                        start->mutable_partition_session()->set_partition_session_id(1);
                        start->mutable_partition_session()->set_path("topic");
                        start->mutable_partition_offsets()->set_start(FirstOffset);
                        start->mutable_partition_offsets()->set_end(EndOffset);
                        if (!stream->Write(response)) {
                            break;
                        }
                    } else if (request.has_start_partition_session_response()) {
                        // Send only after Confirm, so Confirm cannot reset the SDK's
                        // expected offset after it has processed the control message.
                        if (!SendMessage(stream, 0, ControlPayload, true) || !SendMessage(stream, FirstOffset, "first", false) || !SendMessage(stream, 0, ControlPayload, true) || !SendMessage(stream, FirstOffset + 1, std::string(512 * 1024 + 1, 'x'), false)) {
                            break;
                        }
                    } else if (request.has_commit_offset_request()) {
                        for (const auto& partition : request.commit_offset_request().commit_offsets()) {
                            for (const auto& range : partition.offsets()) {
                                commits.InsertInterval(range.start(), range.end());
                            }
                        }
                        if (!commits.Empty() && commits.Min() == 0 && commits.begin()->second == EndOffset) {
                            TServerMessage response;
                            response.set_status(Ydb::StatusIds::SUCCESS);
                            auto* committed = response.mutable_commit_offset_response()->add_partitions_committed_offsets();
                            committed->set_partition_session_id(1);
                            committed->set_committed_offset(EndOffset);
                            if (!stream->Write(response)) {
                                break;
                            }
                        }
                    }
                }
                return grpc::Status::OK;
            }

        private:
            static bool SendMessage(grpc::ServerReaderWriter<TServerMessage, TClientMessage>* stream,
                                    ui64 offset, const std::string& payload, bool service) {
                TServerMessage response;
                response.set_status(Ydb::StatusIds::SUCCESS);
                response.mutable_read_response()->set_bytes_size(payload.size());
                auto* partition = response.mutable_read_response()->add_partition_data();
                partition->set_partition_session_id(1);
                auto* batch = partition->add_batches();
                batch->set_codec(Ydb::Topic::CODEC_RAW);
                auto* message = batch->add_message_data();
                message->set_offset(offset);
                message->set_data(payload);
                message->set_uncompressed_size(payload.size());
                if (service) {
                    auto* metadata = message->add_metadata_items();
                    metadata->set_key("_ydb_service_replication_message");
                    metadata->set_value("true");
                }
                return stream->Write(response);
            }
        };

        class TTestServer {
        public:
            explicit TTestServer(TReplicationReadService& service)
                : Address("localhost:" + std::to_string(static_cast<ui16>(Port)))
            {
                Server = grpc::ServerBuilder()
                             .AddListeningPort(TString(Address), grpc::InsecureServerCredentials())
                             .RegisterService(&service)
                             .BuildAndStart();
                UNIT_ASSERT(Server);
            }

            ~TTestServer() {
                Server->Shutdown(std::chrono::system_clock::now());
                Server->Wait();
            }

            NTesting::TPortHolder Port = NTesting::GetFreePort();
            const std::string Address;
            std::unique_ptr<grpc::Server> Server;
        };

        void TestReplicationMessageCommits(bool decompress) {
            TReplicationReadService service;
            TTestServer server(service);
            TDriver driver(TDriverConfig().SetEndpoint(server.Address).SetDatabase("/Root").SetDiscoveryMode(EDiscoveryMode::Off));
            TTopicClient client(driver);
            auto reader = client.CreateReadSession(TReadSessionSettings()
                                                       .ConsumerName("replicator")
                                                       .AppendTopics(TTopicReadSettings("topic"))
                                                       .Decompress(decompress)
                                                       .RetryPolicy(IRetryPolicy::GetNoRetryPolicy()));

            ui64 committed = 0;
            size_t controlCount = 0;
            size_t messageCount = 0;
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            while (committed != EndOffset && TInstant::Now() < deadline) {
                if (!reader->WaitEvent().Wait(deadline - TInstant::Now())) {
                    break;
                }
                auto event = reader->GetEvent(false);
                UNIT_ASSERT(event);
                if (auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                    start->Confirm(0, 0);
                } else if (auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                    TDeferredCommit commit;
                    auto process = [&](const auto& message) {
                        if (message.GetOffset() == 0) {
                            UNIT_ASSERT_VALUES_EQUAL(message.GetData(), ControlPayload);
                            const auto& fields = message.GetMessageMeta()->Fields;
                            UNIT_ASSERT_VALUES_EQUAL(fields.size(), 1);
                            UNIT_ASSERT_VALUES_EQUAL(fields.front().first, "_ydb_service_replication_message");
                            UNIT_ASSERT_VALUES_EQUAL(fields.front().second, "true");
                            ++controlCount;
                            return;
                        }
                        UNIT_ASSERT_VALUES_EQUAL(message.GetOffset(), FirstOffset + messageCount);
                        UNIT_ASSERT_VALUES_EQUAL(message.GetData(), messageCount == 0 ? "first" : std::string(512 * 1024 + 1, 'x'));
                        commit.Add(message.GetPartitionSession(), message.GetOffset(), message.GetOffset() + 1);
                        ++messageCount;
                    };
                    if (decompress) {
                        for (const auto& message : data->GetMessages()) {
                            process(message);
                        }
                    } else {
                        for (const auto& message : data->GetCompressedMessages()) {
                            process(message);
                        }
                    }
                    commit.Commit();
                } else if (auto* ack = std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&*event)) {
                    committed = ack->GetCommittedOffset();
                } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                    UNIT_FAIL(closed->DebugString());
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(committed, EndOffset);
            UNIT_ASSERT_VALUES_EQUAL(controlCount, 2);
            UNIT_ASSERT_VALUES_EQUAL(messageCount, 2);
            UNIT_ASSERT(reader->Close(TDuration::Seconds(5)));
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TReadSessionReplication) {
        Y_UNIT_TEST(ControlMessagesDoNotBlockCompressedMessageCommits) {
            TestReplicationMessageCommits(false);
        }

        Y_UNIT_TEST(ControlMessagesDoNotBlockDecompressedMessageCommits) {
            TestReplicationMessageCommits(true);
        }
    } // Y_UNIT_TEST_SUITE(TReadSessionReplication)

} // namespace NYdb::inline Dev::NTopic::NTests
