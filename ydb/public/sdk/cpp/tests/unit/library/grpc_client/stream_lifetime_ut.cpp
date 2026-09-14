#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

using namespace NYdbGrpc;

namespace {

    using TQueryResponse = Ydb::Query::ExecuteQueryResponsePart;
    using TTopicRequest = Ydb::Topic::StreamWriteMessage::FromClient;
    using TTopicResponse = Ydb::Topic::StreamWriteMessage::FromServer;
    using TMetadata = std::unordered_multimap<std::string, std::string>;

    class TQueryService final: public Ydb::Query::V1::QueryService::Service {
        grpc::Status ExecuteQuery(grpc::ServerContext* context,
                                  const Ydb::Query::ExecuteQueryRequest*, grpc::ServerWriter<TQueryResponse>* stream) override {
            context->AddInitialMetadata("x-runtime", "query");
            TQueryResponse response;
            response.set_status(Ydb::StatusIds::SUCCESS);
            stream->Write(response);
            return grpc::Status::OK;
        }
    };

    class TTopicService final: public Ydb::Topic::V1::TopicService::Service {
        grpc::Status StreamWrite(grpc::ServerContext* context,
                                 grpc::ServerReaderWriter<TTopicResponse, TTopicRequest>* stream) override {
            context->AddInitialMetadata("x-runtime", "topic");
            stream->SendInitialMetadata();
            TTopicRequest request;
            if (!stream->Read(&request)) {
                return grpc::Status(grpc::StatusCode::CANCELLED, "No request");
            }
            TTopicResponse response;
            response.set_status(Ydb::StatusIds::SUCCESS);
            stream->Write(response);
            return grpc::Status::OK;
        }
    };

    class TServer {
    public:
        TServer() {
            grpc::ServerBuilder builder;
            int port = 0;
            builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
            builder.RegisterService(&Query_);
            builder.RegisterService(&Topic_);
            Server_ = builder.BuildAndStart();
            UNIT_ASSERT(Server_);
            Endpoint = "127.0.0.1:" + std::to_string(port);
        }

        ~TServer() {
            Server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(5));
            Server_->Wait();
        }

        std::string Endpoint;

    private:
        TQueryService Query_;
        TTopicService Topic_;
        std::unique_ptr<grpc::Server> Server_;
    };

    template <class T>
    T Await(std::future<T> result) {
        UNIT_ASSERT(result.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        return result.get();
    }

    template <class TStart>
    TGrpcStatus AwaitStatus(TStart&& start, std::shared_ptr<void> owner = {}) {
        auto done = std::make_shared<std::promise<TGrpcStatus>>();
        start([done, owner = std::move(owner)](TGrpcStatus&& status) {
            // Keep asynchronous output buffers alive even when the wait times out.
            Y_UNUSED(owner);
            done->set_value(std::move(status));
        });
        return Await(done->get_future());
    }

    template <class TProcessor, class TStart>
    typename TProcessor::TPtr Connect(TStart&& start) {
        using TResult = std::pair<TGrpcStatus, typename TProcessor::TPtr>;
        auto done = std::make_shared<std::promise<TResult>>();
        start([done](TGrpcStatus&& status, typename TProcessor::TPtr processor) {
            done->set_value({std::move(status), std::move(processor)});
        });
        auto [status, processor] = Await(done->get_future());
        UNIT_ASSERT(status.Ok());
        UNIT_ASSERT(processor);
        return processor;
    }

    TCallMeta CallMeta() {
        TCallMeta meta;
        meta.Timeout = NYdb::TDeadline::AfterDuration(TDuration::Seconds(5));
        return meta;
    }

} // namespace

Y_UNIT_TEST_SUITE(StreamLifetimeTests) {
    Y_UNIT_TEST(QueryStreamOutlivesClientHandleAndCompletesLateCallbacks) {
        TServer server;
        for (const bool finishBeforeEof : {false, true}) {
            std::unique_ptr<TServiceConnection<Ydb::Query::V1::QueryService>> connection;
            {
                TGRpcClientLow client(1);
                const TTcpKeepAliveSettings keepAlive = {false, 0, 0, 0};
                connection = client.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(
                    TGRpcClientConfig(server.Endpoint), keepAlive, false);
                client.Stop(true);
            }
            auto processor = Connect<IStreamRequestReadProcessor<TQueryResponse>>([&](auto callback) {
                connection->DoStreamRequest<Ydb::Query::ExecuteQueryRequest, TQueryResponse>(
                    Ydb::Query::ExecuteQueryRequest{}, std::move(callback),
                    &Ydb::Query::V1::QueryService::Stub::AsyncExecuteQuery, CallMeta());
            });
            Y_SCOPE_EXIT(processor) {
                processor->Cancel();
            };
            auto metadata = std::make_shared<TMetadata>();
            UNIT_ASSERT(AwaitStatus([&](auto callback) {
                            processor->ReadInitialMetadata(metadata.get(), std::move(callback));
                        }, metadata)
                            .Ok());
            UNIT_ASSERT(metadata->contains("x-runtime"));
            UNIT_ASSERT_VALUES_EQUAL(metadata->find("x-runtime")->second, "query");
            auto response = std::make_shared<TQueryResponse>();
            UNIT_ASSERT(AwaitStatus([&](auto callback) {
                            processor->Read(response.get(), std::move(callback));
                        }, response)
                            .Ok());
            UNIT_ASSERT(response->status() == Ydb::StatusIds::SUCCESS);
            if (finishBeforeEof) {
                auto finished = std::make_shared<std::promise<TGrpcStatus>>();
                processor->AddFinishedCallback([finished](TGrpcStatus&& status) {
                    finished->set_value(std::move(status));
                });
                UNIT_ASSERT(AwaitStatus([&](auto callback) {
                                processor->Finish(std::move(callback));
                            }).Ok());
                UNIT_ASSERT(Await(finished->get_future()).Ok());
            }
            for (unsigned read = 0; read < 2; ++read) {
                const auto status = AwaitStatus([&](auto callback) {
                    processor->Read(response.get(), std::move(callback));
                }, response);
                UNIT_ASSERT(status.GRpcStatusCode == grpc::StatusCode::OUT_OF_RANGE);
            }
            UNIT_ASSERT(AwaitStatus([&](auto callback) {
                            processor->Finish(std::move(callback));
                        }).Ok());
            metadata->clear();
            UNIT_ASSERT(AwaitStatus([&](auto callback) {
                            processor->ReadInitialMetadata(metadata.get(), std::move(callback));
                        }, metadata)
                            .Ok());
            UNIT_ASSERT(metadata->contains("x-runtime"));
            UNIT_ASSERT_VALUES_EQUAL(metadata->find("x-runtime")->second, "query");
            UNIT_ASSERT(AwaitStatus([&](auto callback) {
                            processor->AddFinishedCallback(std::move(callback));
                        }).Ok());
        }
    }

    Y_UNIT_TEST(CancelledQueryReportsFailedStartWithoutAProcessor) {
        TGRpcClientLow client(1);
        auto context = client.CreateContext();
        context->Cancel();
        auto connection = client.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(
            TGRpcClientConfig("127.0.0.1:0"));
        using TProcessor = IStreamRequestReadProcessor<TQueryResponse>;
        auto completed = std::make_shared<std::promise<std::pair<TGrpcStatus, TProcessor::TPtr>>>();
        connection->DoStreamRequest<Ydb::Query::ExecuteQueryRequest, TQueryResponse>(
            Ydb::Query::ExecuteQueryRequest{},
            [completed](TGrpcStatus&& status, TProcessor::TPtr processor) {
                completed->set_value({std::move(status), std::move(processor)});
            }, &Ydb::Query::V1::QueryService::Stub::AsyncExecuteQuery, CallMeta(), context.get());
        auto [status, processor] = Await(completed->get_future());
        UNIT_ASSERT(!status.Ok());
        UNIT_ASSERT(!status.InternalError);
        UNIT_ASSERT(!processor);
    }

    Y_UNIT_TEST(TopicStreamOutlivesClientHandleAndRejectsWritesAfterCompletion) {
        TServer server;
        std::unique_ptr<TServiceConnection<Ydb::Topic::V1::TopicService>> connection;
        {
            TGRpcClientLow client(1);
            connection = client.CreateGRpcServiceConnection<Ydb::Topic::V1::TopicService>(
                TGRpcClientConfig(server.Endpoint));
        }
        auto processor = Connect<IStreamRequestReadWriteProcessor<TTopicRequest, TTopicResponse>>([&](auto callback) {
            connection->DoStreamRequest<TTopicRequest, TTopicResponse>(
                std::move(callback), &Ydb::Topic::V1::TopicService::Stub::AsyncStreamWrite, CallMeta());
        });
        Y_SCOPE_EXIT(processor) {
            processor->Cancel();
        };
        auto metadata = std::make_shared<TMetadata>();
        UNIT_ASSERT(AwaitStatus([&](auto callback) {
                        processor->ReadInitialMetadata(metadata.get(), std::move(callback));
                    }, metadata)
                        .Ok());
        UNIT_ASSERT(metadata->contains("x-runtime"));
        UNIT_ASSERT_VALUES_EQUAL(metadata->find("x-runtime")->second, "topic");
        UNIT_ASSERT(AwaitStatus([&](auto callback) {
                        processor->Write(TTopicRequest{}, std::move(callback));
                    }).Ok());
        auto response = std::make_shared<TTopicResponse>();
        UNIT_ASSERT(AwaitStatus([&](auto callback) {
                        processor->Read(response.get(), std::move(callback));
                    }, response)
                        .Ok());
        UNIT_ASSERT(response->status() == Ydb::StatusIds::SUCCESS);
        for (unsigned read = 0; read < 2; ++read) {
            const auto status = AwaitStatus([&](auto callback) {
                processor->Read(response.get(), std::move(callback));
            }, response);
            UNIT_ASSERT(status.GRpcStatusCode == grpc::StatusCode::OUT_OF_RANGE);
        }
        UNIT_ASSERT(AwaitStatus([&](auto callback) {
            processor->Write(TTopicRequest{}, std::move(callback));
        }).GRpcStatusCode == grpc::StatusCode::CANCELLED);
        UNIT_ASSERT(AwaitStatus([&](auto callback) {
                        processor->Finish(std::move(callback));
                    }).Ok());
        metadata->clear();
        UNIT_ASSERT(AwaitStatus([&](auto callback) {
                        processor->ReadInitialMetadata(metadata.get(), std::move(callback));
                    }, metadata)
                        .Ok());
        UNIT_ASSERT(metadata->contains("x-runtime"));
        UNIT_ASSERT_VALUES_EQUAL(metadata->find("x-runtime")->second, "topic");
        UNIT_ASSERT(AwaitStatus([&](auto callback) {
                        processor->AddFinishedCallback(std::move(callback));
                    }).Ok());
    }
} // Y_UNIT_TEST_SUITE(StreamLifetimeTests)
