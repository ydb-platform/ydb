#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace NYdb::inline Dev::NTopic::NTests {
namespace {

class TPendingCredentialsProvider final : public ICredentialsProvider {
public:
    TPendingCredentialsProvider()
        : AuthInfo_(NThreading::NewPromise<std::string>())
    {}

    std::string GetAuthInfo() const override {
        return AuthInfo_.GetFuture().GetValueSync();
    }

    NThreading::TFuture<std::string> GetAuthInfoAsync() const override {
        return AuthInfo_.GetFuture();
    }

    bool IsValid() const override {
        return true;
    }

private:
    NThreading::TPromise<std::string> AuthInfo_;
};

class TPendingCredentialsFactory final : public ICredentialsProviderFactory {
public:
    TPendingCredentialsFactory()
        : Provider_(std::make_shared<TPendingCredentialsProvider>())
    {}

    TCredentialsProviderPtr CreateProvider() const override {
        return Provider_;
    }

private:
    std::shared_ptr<TPendingCredentialsProvider> Provider_;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TReadSessionCredentialsTest) {
    Y_UNIT_TEST(CloseWhileCredentialsPendingDoesNotDeadlock) {
        auto driver = TDriver(TDriverConfig()
            .SetEndpoint("localhost:100")
            .SetDatabase("/Root")
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetCredentialsProviderFactory(std::make_shared<TPendingCredentialsFactory>()));
        auto client = TTopicClient(driver);

        constexpr size_t ThreadCount = 4;
        constexpr size_t IterationCount = 1'000;
        std::atomic_size_t finishedThreads = 0;
        std::vector<std::thread> threads;
        threads.reserve(ThreadCount);

        for (size_t threadIndex = 0; threadIndex < ThreadCount; ++threadIndex) {
            threads.emplace_back([&] {
                for (size_t iteration = 0; iteration < IterationCount; ++iteration) {
                    auto session = client.CreateReadSession(
                        TReadSessionSettings()
                            .ConsumerName("consumer")
                            .AppendTopics(TTopicReadSettings("/Root/topic")));
                    session->Close(TDuration::Zero());
                }
                ++finishedThreads;
            });
        }

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
        while (finishedThreads.load() != ThreadCount && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(10));
        }
        Y_ABORT_UNLESS(finishedThreads.load() == ThreadCount,
            "Topic read session Close(0) deadlocked while credentials were pending");

        for (auto& thread : threads) {
            thread.join();
        }
    }
}

} // namespace NYdb::NTopic::NTests
