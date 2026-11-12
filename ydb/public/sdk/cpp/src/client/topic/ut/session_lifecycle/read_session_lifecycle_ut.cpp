#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/include/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <future>
#include <optional>
#include <thread>

namespace NYdb::inline Dev {
namespace {

template <typename TClient, typename TClientSettings, typename TReadSettings>
void CheckClosedReadSessionDoesNotBlockDriverStop(
    const TClientSettings& clientSettings,
    const TReadSettings& readSettings)
{
    TDriver driver(TDriverConfig()
        .SetEndpoint("localhost:1")
        .SetDatabase("/Root")
        .SetNetworkThreadsNum(2));
    std::optional<TClient> client(std::in_place, driver, clientSettings);
    auto session = client->CreateReadSession(readSettings);
    session->Close(TDuration::Zero());

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
}

} // namespace NYdb
