#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/include/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session_impl.ipp>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/core/future.h>

#include <functional>
#include <memory>
#include <thread>
#include <utility>

namespace NYdb::inline Dev {
    namespace {

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
    } // Y_UNIT_TEST_SUITE(TReadSessionLifecycle)

    namespace NTopic::NTests {

        class TDirectReadSessionImplTestSetup {
        public:
            static bool CanCloseFromTerminalCallback() {
                struct TControlCallbacks: IDirectReadSessionControlCallbacks {
                    std::function<void()> OnAbort;
                    void AbortSession(TSessionClosedEvent&&) override {
                        OnAbort();
                    }
                };
                auto callbacks = std::make_shared<TControlCallbacks>();
                TDirectReadSession session(0, "", TReadSessionSettings{}, callbacks, nullptr, nullptr, TLog{});
                bool closed = false;
                callbacks->OnAbort = [&] {
                    if (session.Lock.TryAcquire()) {
                        session.Lock.Release();
                        session.Close();
                        closed = true;
                    }
                };
                {
                    TDeferredActions<false> deferred;
                    with_lock (session.Lock) {
                        session.AbortImpl(TPlainStatus(EStatus::UNAVAILABLE, "Connection failed"), deferred);
                    }
                }
                return closed;
            }
        };

        Y_UNIT_TEST_SUITE(TDirectReadSessionLifecycle) {
            Y_UNIT_TEST(TerminalCallbackCanCloseDirectReader) {
                UNIT_ASSERT_C(TDirectReadSessionImplTestSetup::CanCloseFromTerminalCallback(),
                              "Terminal callback must run after releasing the direct reader lock");
            }
        } // Y_UNIT_TEST_SUITE(TDirectReadSessionLifecycle)

    } // namespace NTopic::NTests
} // namespace NYdb::inline Dev
