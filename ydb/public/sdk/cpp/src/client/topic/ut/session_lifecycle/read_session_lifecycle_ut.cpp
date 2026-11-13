#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/executor.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/include/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session_impl.ipp>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/generic/scope.h>

#include <functional>
#include <memory>
#include <thread>
#include <utility>

namespace NYdb::inline Dev {
    namespace {

        template <typename TRequest, typename TResponse>
        struct TTestStream: NYdbGrpc::IStreamRequestReadWriteProcessor<TRequest, TResponse> {
            using TBase = NYdbGrpc::IStreamRequestReadWriteProcessor<TRequest, TResponse>;
            using TReadCallback = typename TBase::TReadCallback;
            TResponse* Response = nullptr;
            TReadCallback ReadCallback;
            void Read(TResponse* response, TReadCallback callback) override {
                Response = response;
                ReadCallback = std::move(callback);
            }
            void Cancel() override {
                ReadCallback = {};
            }
            void Write(TRequest&&, typename TBase::TWriteCallback) override {
            }
            void ReadInitialMetadata(std::unordered_multimap<std::string, std::string>*, TReadCallback) override {
            }
            void Finish(TReadCallback) override {
            }
            void AddFinishedCallback(TReadCallback) override {
            }
        };

        template <typename TRequest, typename TResponse>
        struct TTestFactory: NTopic::ISessionConnectionProcessorFactory<TRequest, TResponse> {
            using TBase = NTopic::ISessionConnectionProcessorFactory<TRequest, TResponse>;
            using TContext = NYdbGrpc::IQueueClientContextPtr;
            typename TBase::TConnectedCallback Connect;
            std::weak_ptr<NYdbGrpc::IQueueClientContext> ConnectContext, TimeoutContext, DelayContext;
            void CreateProcessor(typename TBase::TConnectedCallback callback, const TRpcRequestSettings&,
                                 TContext connect, TDuration, TContext timeout, typename TBase::TConnectTimeoutCallback,
                                 TDuration, TContext delay) override {
                Connect = std::move(callback);
                ConnectContext = connect;
                TimeoutContext = timeout;
                DelayContext = delay;
            }
        };

        template <typename TClient, typename TReadSettings>
        void CheckClosedReadSessions(const TReadSettings& baseSettings) {
            for (const auto& [terminalError, closeTimeout] : {
                     std::pair{false, TDuration::Zero()},
                     std::pair{true, TDuration::Zero()},
                     std::pair{true, TDuration::Seconds(5)}}) {
                TDriver driver(TDriverConfig().SetEndpoint("localhost:1").SetDatabase("/Root").SetNetworkThreadsNum(2));
                auto client = std::make_unique<TClient>(driver);
                auto settings = baseSettings;
                auto closed = NThreading::NewPromise<EStatus>();
                if (terminalError) {
                    settings.RetryPolicy(NTopic::IRetryPolicy::GetNoRetryPolicy()).ClientTimeout(TDuration::MilliSeconds(100));
                    settings.EventHandlers_.SessionClosedHandler([closed](const NTopic::TSessionClosedEvent& event) mutable {
                        closed.SetValue(event.GetStatus());
                    });
                }
                auto session = client->CreateReadSession(settings);
                if (terminalError) {
                    UNIT_ASSERT_C(closed.GetFuture().Wait(TDuration::Seconds(10)), "Read session did not report the terminal error");
                    UNIT_ASSERT(closed.GetFuture().GetValueSync() != EStatus::SUCCESS);
                }
                session->Close(closeTimeout);
                session->Close(closeTimeout);

                auto stopped = NThreading::NewPromise();
                std::thread stopper([&driver, stopped]() mutable {
                    driver.Stop(true);
                    stopped.SetValue();
                });
                const bool stoppedWithClosedSession = stopped.GetFuture().Wait(TDuration::Seconds(5));
                session.reset();
                client.reset();
                stopper.join();
                UNIT_ASSERT_C(stoppedWithClosedSession,
                              "Closed session blocks driver.Stop(true): terminal error=" << terminalError << ", close timeout=" << closeTimeout);
            }
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TReadSessionLifecycle) {
        Y_UNIT_TEST(TopicClosedReadSessionDoesNotBlockDriverStop) {
            CheckClosedReadSessions<NTopic::TTopicClient>(NTopic::TReadSessionSettings()
                                                              .ConsumerName("c")
                                                              .AppendTopics(NTopic::TTopicReadSettings("/Root/topic")));
        }

        Y_UNIT_TEST(PersQueueClosedReadSessionDoesNotBlockDriverStop) {
            CheckClosedReadSessions<NPersQueue::TPersQueueClient>(NPersQueue::TReadSessionSettings()
                                                                      .DisableClusterDiscovery(true)
                                                                      .ConsumerName("c")
                                                                      .AppendTopics(NPersQueue::TTopicReadSettings("/Root/topic")));
        }

        Y_UNIT_TEST(ReconnectAfterAbortDoesNotAccessContextFactory) {
            using namespace NTopic;
            TSingleClusterReadSessionImpl<false> topic(TReadSessionSettings{}, "", "", "", TLog{}, nullptr, nullptr, nullptr, 1, 1);
            TSingleClusterReadSessionImpl<true> persqueue(NPersQueue::TReadSessionSettings{}, "", "", "", TLog{}, nullptr, nullptr, nullptr, 1, 1);
            topic.Abort();
            persqueue.Abort();
            UNIT_ASSERT(!topic.Reconnect(TPlainStatus{}));
            UNIT_ASSERT(!persqueue.Reconnect(TPlainStatus{}));
        }

        Y_UNIT_TEST(TerminalAbortReleasesContexts) {
            using namespace NTopic;
            for (bool directRead : {false, true}) {
                NYdbGrpc::TGRpcClientLow grpc(1);
                auto root = grpc.CreateContext();
                std::weak_ptr<NYdbGrpc::IQueueClientContext> weakRoot = root;
                auto counters = MakeIntrusive<TReaderCounters>();
                MakeCountersNotNull(*counters);
                TReadSessionSettings settings;
                settings.Counters(counters).DirectRead(directRead).RetryPolicy(IRetryPolicy::GetFixedIntervalPolicy());
                settings.DecompressionExecutor(CreateSyncExecutor());
                settings.EventHandlers_.HandlersExecutor(CreateSyncExecutor());
                auto factory = std::make_shared<TTestFactory<TClientMessage<false>, TServerMessage<false>>>();
                auto events = std::make_shared<TReadSessionEventsQueue<false>>(settings);
                auto context = MakeWithCallbackContext<TSingleClusterReadSessionImpl<false>>(
                    settings, "", "session", "", TLog{}, factory, events, std::move(root), 1, 1);
                Y_DEFER {
                    context->TryGet()->Abort();
                    context->Cancel();
                };
                auto session = context->TryGet();
                if (directRead) {
                    session->Start();
                } else {
                    UNIT_ASSERT(session->Reconnect(TPlainStatus(EStatus::UNAVAILABLE, "Retry")));
                    UNIT_ASSERT(!factory->DelayContext.expired());
                }
                UNIT_ASSERT(!factory->ConnectContext.expired() && !factory->TimeoutContext.expired());
                if (directRead) {
                    auto stream = MakeIntrusive<TTestStream<TClientMessage<false>, TServerMessage<false>>>();
                    std::exchange(factory->Connect, {})(TPlainStatus{}, stream);
                    UNIT_ASSERT(stream->Response && stream->ReadCallback);
                    stream->Response->set_status(Ydb::StatusIds::SUCCESS);
                    stream->Response->mutable_init_response()->set_session_id("session");
                    std::exchange(stream->ReadCallback, {})(NYdbGrpc::TGrpcStatus{});
                } else {
                    factory->Connect = {};
                }
                UNIT_ASSERT(!weakRoot.expired());
                session->AbortSession(EStatus::ABORTED, "Terminal error");
                UNIT_ASSERT(weakRoot.expired());
                UNIT_ASSERT(factory->ConnectContext.expired() && factory->TimeoutContext.expired() && factory->DelayContext.expired());
                UNIT_ASSERT(!session->Reconnect(TPlainStatus{}));
                session->Abort();
                session->Abort();
                grpc.Stop(true);
            }
        }
    } // Y_UNIT_TEST_SUITE(TReadSessionLifecycle)

    namespace NTopic::NTests {

        class TDirectReadSessionImplTestSetup {
        public:
            enum class EFailure { Connect,
                                  Read,
                                  Start };

            static void CheckTerminalCallback(EFailure failure) {
                struct TControlCallbacks: IDirectReadSessionControlCallbacks {
                    std::function<void(EStatus)> OnAbort;
                    void AbortSession(TSessionClosedEvent&& event) override {
                        OnAbort(event.GetStatus());
                    }
                };
                NYdbGrpc::TGRpcClientLow grpcClient(1);
                auto counters = MakeIntrusive<TReaderCounters>();
                MakeCountersNotNull(*counters);
                TReadSessionSettings settings;
                settings.Counters(counters).RetryPolicy(IRetryPolicy::GetNoRetryPolicy());
                auto callbacks = std::make_shared<TControlCallbacks>();
                auto factory = std::make_shared<TTestFactory<TDirectReadClientMessage, TDirectReadServerMessage>>();
                auto stream = MakeIntrusive<TTestStream<TDirectReadClientMessage, TDirectReadServerMessage>>();
                auto context = MakeWithCallbackContext<TDirectReadSession>(0, "", settings, callbacks, grpcClient.CreateContext(), factory, TLog{});
                auto session = context->TryGet();
                size_t aborts = 0;
                bool closed = false, contextReleased = false, errorDelivered = false, wasDeferred = true;
                std::thread canceller;
                callbacks->OnAbort = [&](EStatus status) {
                    if (++aborts != 1) {
                        return;
                    }
                    errorDelivered = status == (failure == EFailure::Read ? EStatus::TRANSPORT_UNAVAILABLE : EStatus::UNAVAILABLE);
                    if (session->Lock.TryAcquire()) {
                        session->Lock.Release();
                        session->Close();
                        closed = true;
                    }
                    auto cancelled = NThreading::NewPromise();
                    canceller = std::thread([context, cancelled]() mutable { context->Cancel(); cancelled.SetValue(); });
                    contextReleased = cancelled.GetFuture().Wait(TDuration::Seconds(5));
                };

                if (failure == EFailure::Start) {
                    TDirectReadPartitionSession partition{.Location = {0, 1}, .State = TDirectReadPartitionSession::EState::STARTING};
                    TDeferredActions<false> deferred;
                    with_lock (session->Lock) {
                        session->DelayStartRequestImpl(partition, TPlainStatus(EStatus::UNAVAILABLE, "Start failed"), deferred);
                        wasDeferred = aborts == 0;
                    }
                } else {
                    session->Start();
                    auto connect = std::exchange(factory->Connect, {});
                    if (failure == EFailure::Connect) {
                        connect(TPlainStatus(EStatus::UNAVAILABLE, "Connection failed"), {});
                    } else {
                        connect(TPlainStatus(), IDirectReadProcessor::TPtr(stream));
                        std::exchange(stream->ReadCallback, {})(NYdbGrpc::TGrpcStatus(grpc::StatusCode::UNAVAILABLE, "Read failed"));
                    }
                }
                if (canceller.joinable()) {
                    canceller.join();
                }
                session->Close();
                context->Cancel();
                UNIT_ASSERT_VALUES_EQUAL(aborts, 1);
                UNIT_ASSERT(errorDelivered);
                UNIT_ASSERT(wasDeferred);
                UNIT_ASSERT_C(closed, "Terminal callback holds the direct reader lock");
                UNIT_ASSERT_C(contextReleased, "Terminal callback holds the SDK callback context guard");
            }
        };

        Y_UNIT_TEST_SUITE(TDirectReadSessionLifecycle) {
            Y_UNIT_TEST(TerminalCallbacksCanCloseDirectReader) {
                using TSetup = TDirectReadSessionImplTestSetup;
                for (auto failure : {TSetup::EFailure::Connect, TSetup::EFailure::Read, TSetup::EFailure::Start}) {
                    TSetup::CheckTerminalCallback(failure);
                }
            }
        } // Y_UNIT_TEST_SUITE(TDirectReadSessionLifecycle)

    } // namespace NTopic::NTests
} // namespace NYdb::inline Dev
