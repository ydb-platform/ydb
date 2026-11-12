#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/include/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <thread>

namespace NYdb::inline Dev {
    namespace {

        template <typename TClient, typename TClientSettings, typename TReadSettings>
        void CheckClosedReadSessionDoesNotBlockDriverStop(
            const TClientSettings& clientSettings,
            const TReadSettings& readSettings,
            bool waitForTerminalError = false,
            TDuration closeTimeout = TDuration::Zero())
        {
            TDriver driver(TDriverConfig()
                               .SetEndpoint("localhost:1")
                               .SetDatabase("/Root")
                               .SetNetworkThreadsNum(2));
            std::optional<TClient> client(std::in_place, driver, clientSettings);
            auto settings = readSettings;
            auto closed = std::make_shared<std::promise<EStatus>>();
            auto closedFuture = closed->get_future();
            if (waitForTerminalError) {
                settings.EventHandlers_.SessionClosedHandler([closed](const NTopic::TSessionClosedEvent& event) {
                    closed->set_value(event.GetStatus());
                });
            }
            auto session = client->CreateReadSession(settings);
            if (waitForTerminalError) {
                UNIT_ASSERT_C(closedFuture.wait_for(std::chrono::seconds(10)) == std::future_status::ready,
                              "Read session did not report the terminal connection error");
                UNIT_ASSERT(closedFuture.get() != EStatus::SUCCESS);
            }
            session->Close(closeTimeout);
            session->Close(closeTimeout);

            std::promise<void> stopped;
            auto done = stopped.get_future();
            std::thread stopper([&] {
                driver.Stop(true);
                stopped.set_value();
            });
            const bool stoppedWithClosedSession =
                done.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
            session.reset();
            client.reset();
            stopper.join();

            UNIT_ASSERT_C(stoppedWithClosedSession,
                          "A closed read session must not prevent driver.Stop(true) from completing");
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TReadSessionLifecycle) {
        Y_UNIT_TEST(TopicClosedReadSessionDoesNotBlockDriverStop) {
            CheckClosedReadSessionDoesNotBlockDriverStop<NTopic::TTopicClient>(
                NTopic::TTopicClientSettings(),
                NTopic::TReadSessionSettings()
                    .ConsumerName("c")
                    .AppendTopics(NTopic::TTopicReadSettings("/Root/topic")));
        }

        Y_UNIT_TEST(PersQueueClosedReadSessionDoesNotBlockDriverStop) {
            CheckClosedReadSessionDoesNotBlockDriverStop<NPersQueue::TPersQueueClient>(
                NPersQueue::TPersQueueClientSettings(),
                NPersQueue::TReadSessionSettings()
                    .DisableClusterDiscovery(true)
                    .ConsumerName("c")
                    .AppendTopics(NPersQueue::TTopicReadSettings("/Root/topic")));
        }

        Y_UNIT_TEST(TopicClosedReadSessionAfterTerminalErrorDoesNotBlockDriverStop) {
            CheckClosedReadSessionDoesNotBlockDriverStop<NTopic::TTopicClient>(
                NTopic::TTopicClientSettings(),
                NTopic::TReadSessionSettings()
                    .ConsumerName("c")
                    .AppendTopics(NTopic::TTopicReadSettings("/Root/topic"))
                    .RetryPolicy(NTopic::IRetryPolicy::GetNoRetryPolicy())
                    .ClientTimeout(TDuration::MilliSeconds(100)),
                true);
        }

        Y_UNIT_TEST(PersQueueClosedReadSessionAfterTerminalErrorDoesNotBlockDriverStop) {
            CheckClosedReadSessionDoesNotBlockDriverStop<NPersQueue::TPersQueueClient>(
                NPersQueue::TPersQueueClientSettings(),
                NPersQueue::TReadSessionSettings()
                    .DisableClusterDiscovery(true)
                    .ConsumerName("c")
                    .AppendTopics(NPersQueue::TTopicReadSettings("/Root/topic"))
                    .RetryPolicy(NPersQueue::IRetryPolicy::GetNoRetryPolicy())
                    .ClientTimeout(TDuration::MilliSeconds(100)),
                true);
        }

        Y_UNIT_TEST(TopicClosedReadSessionAfterTerminalErrorWithTimeoutDoesNotBlockDriverStop) {
            CheckClosedReadSessionDoesNotBlockDriverStop<NTopic::TTopicClient>(
                NTopic::TTopicClientSettings(),
                NTopic::TReadSessionSettings()
                    .ConsumerName("c")
                    .AppendTopics(NTopic::TTopicReadSettings("/Root/topic"))
                    .RetryPolicy(NTopic::IRetryPolicy::GetNoRetryPolicy())
                    .ClientTimeout(TDuration::MilliSeconds(100)),
                true, TDuration::Seconds(5));
        }

        Y_UNIT_TEST(PersQueueClosedReadSessionAfterTerminalErrorWithTimeoutDoesNotBlockDriverStop) {
            CheckClosedReadSessionDoesNotBlockDriverStop<NPersQueue::TPersQueueClient>(
                NPersQueue::TPersQueueClientSettings(),
                NPersQueue::TReadSessionSettings()
                    .DisableClusterDiscovery(true)
                    .ConsumerName("c")
                    .AppendTopics(NPersQueue::TTopicReadSettings("/Root/topic"))
                    .RetryPolicy(NPersQueue::IRetryPolicy::GetNoRetryPolicy())
                    .ClientTimeout(TDuration::MilliSeconds(100)),
                true, TDuration::Seconds(5));
        }
    } // Y_UNIT_TEST_SUITE(TReadSessionLifecycle)

} // namespace NYdb::inline Dev
