#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <algorithm>
#include <mutex>
#include <vector>

namespace NYdb::inline Dev::NTopic::NTests {
    namespace {

        // Run compression in reverse order so all blocks are ready when the first
        // block becomes sendable. This deterministically exercises request batching.
        class TManualExecutor: public IExecutor {
        public:
            bool IsAsync() const override {
                return true;
            }
            void Stop() override {
            }

            void Post(TFunction&& f) override {
                std::lock_guard guard(Mutex);
                Tasks.push_back(std::move(f));
            }

            void RunAll() {
                while (true) {
                    std::vector<TFunction> tasks;
                    {
                        std::lock_guard guard(Mutex);
                        tasks.swap(Tasks);
                    }
                    if (tasks.empty()) {
                        return;
                    }
                    for (auto it = tasks.rbegin(); it != tasks.rend(); ++it) {
                        (*it)();
                    }
                }
            }

        private:
            void DoStart() override {
            }
            std::mutex Mutex;
            std::vector<TFunction> Tasks;
        };

        struct TTopicApi {
            using TClient = TTopicClient;
            using TSettings = TWriteSessionSettings;
            using TEvents = TWriteSessionEvent;
            using TService = Ydb::Topic::V1::TopicService::Service;
            using TRequest = Ydb::Topic::StreamWriteMessage::FromClient;
            using TResponse = Ydb::Topic::StreamWriteMessage::FromServer;

            static void Configure(TSettings& settings) {
                settings.DirectWriteToPartition(false);
            }

            static void Ack(const TRequest& request, TResponse& response) {
                for (const auto& message : request.write_request().messages()) {
                    auto* ack = response.mutable_write_response()->add_acks();
                    ack->set_seq_no(message.seq_no());
                    ack->mutable_written()->set_offset(message.seq_no() - 1);
                }
            }
        };

        struct TPersQueueApi {
            using TClient = NPersQueue::TPersQueueClient;
            using TSettings = NPersQueue::TWriteSessionSettings;
            using TEvents = NPersQueue::TWriteSessionEvent;
            using TService = Ydb::PersQueue::V1::PersQueueService::Service;
            using TRequest = Ydb::PersQueue::V1::StreamingWriteClientMessage;
            using TResponse = Ydb::PersQueue::V1::StreamingWriteServerMessage;

            static void Configure(TSettings& settings) {
                settings.ClusterDiscoveryMode(NPersQueue::EClusterDiscoveryMode::Off);
            }

            static void Ack(const TRequest& request, TResponse& response) {
                auto* ack = response.mutable_batch_write_response();
                for (const auto seqNo : request.write_request().sequence_numbers()) {
                    ack->add_sequence_numbers(seqNo);
                    ack->add_offsets(seqNo - 1);
                    ack->add_already_written(false);
                }
            }
        };

        template <typename TApi>
        class TWriteService: public TApi::TService {
        public:
            using TStream = grpc::ServerReaderWriter<typename TApi::TResponse, typename TApi::TRequest>;

            // Each specialization overrides the RPC belonging to its service.
            grpc::Status StreamWrite(grpc::ServerContext*, TStream* stream) {
                return Write(stream);
            }
            grpc::Status StreamingWrite(grpc::ServerContext*, TStream* stream) {
                return Write(stream);
            }

            std::vector<size_t> GetRequestSizes() const {
                std::lock_guard guard(Mutex);
                return RequestSizes;
            }

        private:
            grpc::Status Write(TStream* stream) {
                typename TApi::TRequest request;
                while (stream->Read(&request)) {
                    typename TApi::TResponse response;
                    response.set_status(Ydb::StatusIds::SUCCESS);
                    if (request.has_init_request()) {
                        response.mutable_init_response()->set_session_id("test-session");
                    } else if (request.has_write_request()) {
                        {
                            std::lock_guard guard(Mutex);
                            RequestSizes.push_back(request.ByteSizeLong());
                        }
                        TApi::Ack(request, response);
                    } else {
                        response.mutable_update_token_response();
                    }
                    if (!stream->Write(response)) {
                        break;
                    }
                }
                return grpc::Status::OK;
            }

            mutable std::mutex Mutex;
            std::vector<size_t> RequestSizes;
        };

        template <typename TApi, typename TSession>
        auto WaitForToken(TSession& session) {
            while (true) {
                UNIT_ASSERT_C(session.WaitEvent().Wait(TDuration::Seconds(10)), "Timed out waiting for a write token");
                auto event = session.GetEvent(false);
                UNIT_ASSERT(event);
                if (auto* ready = std::get_if<typename TApi::TEvents::TReadyToAcceptEvent>(&*event)) {
                    return std::move(ready->ContinuationToken);
                }
                if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                    UNIT_FAIL(closed->DebugString());
                }
            }
        }

        std::string MakePayload(size_t size) {
            // Incompressible, deterministic input: batching must use the wire size.
            std::string result(size, '\0');
            uint32_t state = 1;
            for (auto& c : result) {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                c = static_cast<char>(state);
            }
            return result;
        }

        template <typename TApi>
        void CheckWrites(TDriverConfig config, size_t limit, bool oversized = false, bool exactFit = false) {
            TWriteService<TApi> service;
            grpc::ServerBuilder builder;
            int port = 0;
            builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
            builder.RegisterService(&service);
            auto server = builder.BuildAndStart();
            UNIT_ASSERT(server);

            config.SetEndpoint("127.0.0.1:" + std::to_string(port)).SetDiscoveryMode(EDiscoveryMode::Off);
            TDriver driver(config);
            typename TApi::TClient client(driver);
            auto executor = std::make_shared<TManualExecutor>();
            typename TApi::TSettings settings;
            settings.Path("topic")
                .MessageGroupId("producer")
                .Codec(exactFit ? ECodec::RAW : ECodec::GZIP)
                .BatchFlushInterval(TDuration::Zero())
                .BatchFlushSizeBytes(0)
                .CompressionExecutor(executor)
                .RetryPolicy(IRetryPolicy::GetNoRetryPolicy());
            TApi::Configure(settings);
            auto session = client.CreateWriteSession(settings);

            const size_t messageCount = oversized || exactFit ? 1 : 10;
            const auto payload = MakePayload(oversized ? limit : 600);
            for (size_t i = 0; i < messageCount; ++i) {
                session->Write(WaitForToken<TApi>(*session), payload, i + 1, TInstant::Zero());
            }
            executor->RunAll();

            size_t ackCount = 0;
            bool closed = false;
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            while (ackCount < messageCount && !closed && TInstant::Now() < deadline) {
                UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(10)), "Timed out waiting for write result");
                for (auto& event : session->GetEvents()) {
                    if (auto* acks = std::get_if<typename TApi::TEvents::TAcksEvent>(&event)) {
                        ackCount += acks->Acks.size();
                    } else if (auto* error = std::get_if<TSessionClosedEvent>(&event)) {
                        UNIT_ASSERT_C(oversized, error->DebugString());
                        UNIT_ASSERT_VALUES_EQUAL(error->GetStatus(), EStatus::BAD_REQUEST);
                        UNIT_ASSERT_STRING_CONTAINS(error->GetIssues().ToString(), "maximum outbound gRPC request size");
                        closed = true;
                    }
                }
            }

            const auto sizes = service.GetRequestSizes();
            if (oversized) {
                UNIT_ASSERT(closed);
                UNIT_ASSERT(sizes.empty());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(ackCount, messageCount);
                if (exactFit) {
                    UNIT_ASSERT_VALUES_EQUAL(sizes.size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(sizes.front(), limit);
                } else {
                    UNIT_ASSERT(sizes.size() > 1);
                }
                for (const auto size : sizes) {
                    UNIT_ASSERT_LE(size, limit);
                }
                if (limit > 1024) {
                    // A larger outbound override must take precedence over the common
                    // limit, rather than unnecessarily fragmenting into 1 KiB requests.
                    UNIT_ASSERT(*std::max_element(sizes.begin(), sizes.end()) > 1024);
                }
                UNIT_ASSERT(session->Close(TDuration::Seconds(10)));
            }
            session.reset();
            driver.Stop(true);
            server->Shutdown();
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TopicWriteDriverLimit) {
        Y_UNIT_TEST(EffectiveLimit) {
            const auto check = [](const TDriverConfig& config, uint64_t expected) {
                TDriver driver(config);
                auto connections = CreateInternalInterface(driver);
                UNIT_ASSERT_VALUES_EQUAL(connections->GetMaxOutboundMessageSize(), expected);
                UNIT_ASSERT_VALUES_EQUAL(NWriteSessionGrpc::GetMaxGrpcMessageSize(*connections), std::min<uint64_t>(120_MB, expected));
                driver.Stop(true);
            };
            check(TDriverConfig(), 64000000);
            check(TDriverConfig().SetMaxInboundMessageSize(1024), 64000000);
            check(TDriverConfig().SetMaxMessageSize(4096), 4096);
            check(TDriverConfig().SetMaxMessageSize(4096).SetMaxOutboundMessageSize(1024), 1024);
            check(TDriverConfig().SetMaxMessageSize(1024).SetMaxOutboundMessageSize(4096), 4096);
            check(TDriverConfig().SetMaxMessageSize(1024).SetMaxOutboundMessageSize(0), 1024);
            check(TDriverConfig().SetMaxOutboundMessageSize(256_MB), 256_MB);
        }

        Y_UNIT_TEST(ExactlyFittingBlockTopic) {
            TTopicApi::TRequest request;
            auto* write = request.mutable_write_request();
            write->set_codec(Ydb::Topic::CODEC_RAW);
            auto* message = write->add_messages();
            message->set_seq_no(1);
            message->mutable_created_at();
            message->set_uncompressed_size(600);
            message->set_data(MakePayload(600));
            const auto limit = request.ByteSizeLong();
            CheckWrites<TTopicApi>(TDriverConfig().SetMaxOutboundMessageSize(limit), limit, false, true);
        }

        Y_UNIT_TEST(CommonLimitTopic) {
            CheckWrites<TTopicApi>(TDriverConfig().SetMaxMessageSize(1024), 1024);
        }

        Y_UNIT_TEST(CommonLimitPersQueue) {
            CheckWrites<TPersQueueApi>(TDriverConfig().SetMaxMessageSize(1024), 1024);
        }

        Y_UNIT_TEST(SmallerOutboundOverrideTopic) {
            CheckWrites<TTopicApi>(TDriverConfig().SetMaxMessageSize(4096).SetMaxOutboundMessageSize(1024), 1024);
        }

        Y_UNIT_TEST(SmallerOutboundOverridePersQueue) {
            CheckWrites<TPersQueueApi>(TDriverConfig().SetMaxMessageSize(4096).SetMaxOutboundMessageSize(1024), 1024);
        }

        Y_UNIT_TEST(LargerOutboundOverrideTopic) {
            CheckWrites<TTopicApi>(TDriverConfig().SetMaxMessageSize(1024).SetMaxOutboundMessageSize(2048), 2048);
        }

        Y_UNIT_TEST(LargerOutboundOverridePersQueue) {
            CheckWrites<TPersQueueApi>(TDriverConfig().SetMaxMessageSize(1024).SetMaxOutboundMessageSize(2048), 2048);
        }

        Y_UNIT_TEST(InboundLimitDoesNotRestrictWritesTopic) {
            CheckWrites<TTopicApi>(TDriverConfig().SetMaxMessageSize(2048).SetMaxInboundMessageSize(256), 2048);
        }

        Y_UNIT_TEST(InboundLimitDoesNotRestrictWritesPersQueue) {
            CheckWrites<TPersQueueApi>(TDriverConfig().SetMaxMessageSize(2048).SetMaxInboundMessageSize(256), 2048);
        }

        Y_UNIT_TEST(OversizedBlockTopic) {
            CheckWrites<TTopicApi>(TDriverConfig().SetMaxOutboundMessageSize(1024), 1024, true);
        }

        Y_UNIT_TEST(OversizedBlockPersQueue) {
            CheckWrites<TPersQueueApi>(TDriverConfig().SetMaxOutboundMessageSize(1024), 1024, true);
        }
    } // Y_UNIT_TEST_SUITE(TopicWriteDriverLimit)

} // namespace NYdb::inline Dev::NTopic::NTests
