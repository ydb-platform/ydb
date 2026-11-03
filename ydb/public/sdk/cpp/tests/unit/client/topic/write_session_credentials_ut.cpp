#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <chrono>
#include <mutex>

namespace NYdb::inline Dev {
    namespace {

        class TAsyncCredentials final: public ICredentialsProvider {
        public:
            std::string GetAuthInfo() const override {
                throw TAuthenticationError("Synchronous credentials access");
            }

            NThreading::TFuture<std::string> GetAuthInfoAsync() const override {
                NThreading::TFuture<std::string> future;
                {
                    std::lock_guard guard(Lock);
                    future = Token.GetFuture();
                }
                if (!Requested.exchange(true)) {
                    FirstRequest.SetValue();
                }
                return future;
            }

            bool IsValid() const override {
                return true;
            }

            void SetToken(const std::string& token) {
                NThreading::TPromise<std::string> promise;
                {
                    std::lock_guard guard(Lock);
                    if (Token.HasValue()) {
                        Token = NThreading::NewPromise<std::string>();
                    }
                    promise = Token;
                }
                promise.SetValue(token);
            }

            NThreading::TPromise<std::string> DeferToken() {
                std::lock_guard guard(Lock);
                Token = NThreading::NewPromise<std::string>();
                return Token;
            }

            mutable NThreading::TPromise<void> FirstRequest = NThreading::NewPromise();

        private:
            mutable std::atomic<bool> Requested = false;
            mutable std::mutex Lock;
            NThreading::TPromise<std::string> Token = NThreading::NewPromise<std::string>();
        };

        class TAsyncCredentialsFactory final: public ICredentialsProviderFactory {
        public:
            explicit TAsyncCredentialsFactory(std::shared_ptr<TAsyncCredentials> provider)
                : Provider(std::move(provider))
            {
            }

            TCredentialsProviderPtr CreateProvider() const override {
                return Provider;
            }

        private:
            const std::shared_ptr<TAsyncCredentials> Provider;
        };

        using TTopicRequest = Ydb::Topic::StreamWriteMessage::FromClient;
        using TTopicResponse = Ydb::Topic::StreamWriteMessage::FromServer;
        using TPqRequest = Ydb::PersQueue::V1::StreamingWriteClientMessage;
        using TPqResponse = Ydb::PersQueue::V1::StreamingWriteServerMessage;

        void Ack(const TTopicRequest& request, TTopicResponse& response) {
            for (const auto& message : request.write_request().messages()) {
                auto* ack = response.mutable_write_response()->add_acks();
                ack->set_seq_no(message.seq_no());
                ack->mutable_written()->set_offset(message.seq_no());
            }
        }

        void Ack(const TPqRequest& request, TPqResponse& response) {
            for (auto seqNo : request.write_request().sequence_numbers()) {
                auto* ack = response.mutable_batch_write_response();
                ack->add_sequence_numbers(seqNo);
                ack->add_offsets(seqNo);
                ack->add_already_written(false);
            }
        }

        class TWriteService final: public Ydb::Topic::V1::TopicService::Service,
                                   public Ydb::PersQueue::V1::PersQueueService::Service {
        public:
            explicit TWriteService(std::shared_ptr<TAsyncCredentials> credentials, bool reconnect)
                : Credentials(std::move(credentials))
                , Reconnect(reconnect)
            {
            }

            grpc::Status StreamWrite(grpc::ServerContext* context,
                                     grpc::ServerReaderWriter<TTopicResponse, TTopicRequest>* stream) override {
                return Handle(context, stream);
            }

            grpc::Status StreamingWrite(grpc::ServerContext* context,
                                        grpc::ServerReaderWriter<TPqResponse, TPqRequest>* stream) override {
                return Handle(context, stream);
            }

            std::atomic<size_t> Connections = 0;
            std::atomic<size_t> Refreshes = 0;
            NThreading::TPromise<void> FirstWrite = NThreading::NewPromise();
            NThreading::TPromise<void> SecondWrite = NThreading::NewPromise();

        private:
            template <class TResponse, class TRequest>
            grpc::Status Handle(grpc::ServerContext* context, grpc::ServerReaderWriter<TResponse, TRequest>* stream) {
                const size_t connection = ++Connections;
                const auto token = context->client_metadata().find("x-ydb-auth-ticket");
                if (token == context->client_metadata().end() ||
                    std::string(token->second.data(), token->second.size()) != (connection == 1 ? "abc" : "reconnected")) {
                    return {grpc::StatusCode::UNAUTHENTICATED, "wrong initial token"};
                }
                TRequest request;
                while (stream->Read(&request)) {
                    TResponse response;
                    response.set_status(Ydb::StatusIds::SUCCESS);
                    if (request.has_init_request()) {
                        if (Reconnect && connection == 1) {
                            // Leave a refresh pending when this stream disconnects.
                            PendingRefresh = Credentials->DeferToken();
                        }
                        response.mutable_init_response()->set_session_id("session");
                    } else if (request.has_update_token_request()) {
                        ++Refreshes;
                        if (request.update_token_request().token() != "rotated") {
                            // A server with auth disabled accepts initial metadata but
                            // rejects an unnecessary refresh of its dummy token.
                            return {grpc::StatusCode::UNAUTHENTICATED, "refreshed token is invalid"};
                        }
                        response.mutable_update_token_response();
                    } else if (request.has_write_request()) {
                        if (Reconnect && connection == 1) {
                            Credentials->DeferToken().SetValue("reconnected");
                            return {grpc::StatusCode::UNAVAILABLE, "reconnect with a new token"};
                        }
                        Ack(request, response);
                        if (++Writes == 1) {
                            if (Reconnect) {
                                // The old refresh must not replace the new stream's token.
                                PendingRefresh.SetValue("abc");
                            }
                            FirstWrite.SetValue();
                        } else if (Writes == 2) {
                            SecondWrite.SetValue();
                        }
                    }
                    if (!stream->Write(response)) {
                        break;
                    }
                }
                return grpc::Status::OK;
            }

            const std::shared_ptr<TAsyncCredentials> Credentials;
            const bool Reconnect;
            NThreading::TPromise<std::string> PendingRefresh;
            std::atomic<size_t> Writes = 0;
        };

        template <bool Topic>
        void CheckCredentials(bool reconnect) {
            auto credentials = std::make_shared<TAsyncCredentials>();
            TWriteService service(credentials, reconnect);
            grpc::ServerBuilder builder;
            int port = 0;
            builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
            builder.RegisterService(static_cast<Ydb::Topic::V1::TopicService::Service*>(&service));
            builder.RegisterService(static_cast<Ydb::PersQueue::V1::PersQueueService::Service*>(&service));
            auto server = builder.BuildAndStart();
            UNIT_ASSERT(server);
            struct TShutdown {
                grpc::Server& Server;
                ~TShutdown() {
                    Server.Shutdown(std::chrono::system_clock::now());
                }
            } shutdown{*server};

            TDriver driver(TDriverConfig()
                               .SetEndpoint("127.0.0.1:" + std::to_string(port))
                               .SetDatabase("/Root")
                               .SetDiscoveryMode(EDiscoveryMode::Off)
                               .SetCredentialsProviderFactory(std::make_shared<TAsyncCredentialsFactory>(credentials)));

            auto run = [&](auto& client, auto settings) {
                auto session = client.CreateSimpleBlockingWriteSession(settings);
                // Session creation must not wait for the pending credentials future.
                UNIT_ASSERT(credentials->FirstRequest.GetFuture().Wait(TDuration::Seconds(10)));
                credentials->SetToken("abc");
                UNIT_ASSERT(session->Write("first", {}, {}, TDuration::Seconds(10)));
                UNIT_ASSERT_C(service.FirstWrite.GetFuture().Wait(TDuration::Seconds(10)), "first write failed");
                UNIT_ASSERT_VALUES_EQUAL(service.Refreshes.load(), 0);

                credentials->SetToken("rotated");
                UNIT_ASSERT(session->Write("second", {}, {}, TDuration::Seconds(10)));
                UNIT_ASSERT_C(service.SecondWrite.GetFuture().Wait(TDuration::Seconds(10)), "write after rotation failed");
                UNIT_ASSERT(session->Close(TDuration::Seconds(10)));
                UNIT_ASSERT_VALUES_EQUAL(service.Refreshes.load(), 1);
                UNIT_ASSERT_VALUES_EQUAL(service.Connections.load(), reconnect ? 2 : 1);
            };

            if constexpr (Topic) {
                NTopic::TTopicClient client(driver);
                run(client, NTopic::TWriteSessionSettings()
                                .Path("topic")
                                .MessageGroupId("producer")
                                .DirectWriteToPartition(false)
                                .Codec(NTopic::ECodec::RAW)
                                .BatchFlushInterval(TDuration::Zero())
                                .BatchFlushMessageCount(1));
            } else {
                NPersQueue::TPersQueueClient client(driver);
                run(client, NPersQueue::TWriteSessionSettings()
                                .Path("topic")
                                .MessageGroupId("producer")
                                .ClusterDiscoveryMode(NPersQueue::EClusterDiscoveryMode::Off)
                                .Codec(NPersQueue::ECodec::RAW)
                                .BatchFlushInterval(TDuration::Zero()));
            }
        }

    } // namespace

    Y_UNIT_TEST_SUITE(WriteSessionCredentials) {
        Y_UNIT_TEST(TopicKeepsInitialAsyncTokenAndRefreshesChanges) {
            CheckCredentials<true>(false);
        }
        Y_UNIT_TEST(PersQueueKeepsInitialAsyncTokenAndRefreshesChanges) {
            CheckCredentials<false>(false);
        }
        Y_UNIT_TEST(TopicRemembersTokenAfterReconnect) {
            CheckCredentials<true>(true);
        }
        Y_UNIT_TEST(PersQueueRemembersTokenAfterReconnect) {
            CheckCredentials<false>(true);
        }
    } // Y_UNIT_TEST_SUITE(WriteSessionCredentials)

} // namespace NYdb::inline Dev
