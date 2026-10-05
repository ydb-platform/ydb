#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <atomic>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

namespace NYdb::inline Dev::NTopic::NTests {
namespace {

void StopDriverOrFail(TDriver& driver, TDuration timeout = TDuration::Seconds(15)) {
    auto done = NThreading::NewPromise();
    std::thread stopper([&driver, done]() mutable {
        driver.Stop(true);
        done.SetValue();
    });
    if (!done.GetFuture().Wait(timeout)) {
        stopper.detach();
        UNIT_FAIL("TDriver::Stop(true) did not return in " << timeout);
    }
    stopper.join();
}

size_t CountOsThreads() {
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        constexpr std::string_view prefix = "Threads:";
        if (line.starts_with(prefix)) {
            return std::stoul(line.substr(prefix.size()));
        }
    }
    UNIT_FAIL("Threads field is missing from /proc/self/status");
    return 0;
}

// Blocks inside CompressWriteBlock so the test can destroy the client while the
// compression task is still running on the default executor.
class TBlockingGzipCodec final : public ICodec {
public:
    TBlockingGzipCodec(std::atomic<bool>* entered, std::atomic<bool>* release)
        : Entered(entered)
        , Release(release)
    {
    }

    std::string Decompress(const std::string& data) const override {
        return AsCodec().Decompress(data);
    }

    std::unique_ptr<IOutputStream> CreateCoder(TBuffer& result, int quality) const override {
        return AsCodec().CreateCoder(result, quality);
    }

    void CompressWriteBlock(TWriteBlockCompression& ctx) const override {
        Entered->store(true);
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        while (!Release->load()) {
            if (TInstant::Now() > deadline) {
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        AsCodec().CompressWriteBlock(ctx);
    }

private:
    const ICodec& AsCodec() const {
        return Inner;
    }

    std::atomic<bool>* Entered;
    std::atomic<bool>* Release;
    TGzipCodec Inner;
};

TContinuationToken WaitForWriteToken(IWriteSession& session) {
    while (true) {
        UNIT_ASSERT_C(session.WaitEvent().Wait(TDuration::Seconds(30)), "timeout waiting for write token");
        for (auto& event : session.GetEvents()) {
            if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                return std::move(ready->ContinuationToken);
            }
            if (auto* closed = std::get_if<TSessionClosedEvent>(&event)) {
                UNIT_FAIL("write session closed unexpectedly: " << closed->GetIssues().ToString());
            }
        }
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(WriteSessionConnect) {
    // After TDriver::Stop the driver scope is cancelled. DirectWriteToPartition
    // (false) makes reconnect call Connect() with a still-live ClientContext.
    // Connect must AbortImpl instead of creating children of that context.
    // Stop(true) waits for ClientContext to be destroyed; keeping it deadlocks.
    Y_UNIT_TEST(ReconnectAfterDriverStopDoesNotAbortOnNullConnectContext) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        TDriver driver(setup.MakeDriverConfig());
        TTopicClient client(driver);

        auto session = client.CreateWriteSession(
            TWriteSessionSettings()
                .Path(setup.GetTopicPath())
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .DirectWriteToPartition(false)
                .RetryPolicy(IRetryPolicy::GetFixedIntervalPolicy(
                    TDuration::MilliSeconds(10),
                    TDuration::MilliSeconds(10))));

        Y_UNUSED(WaitForWriteToken(*session));

        StopDriverOrFail(driver);
        session.reset();
    }

    // CreateProcessor delay is cancelled with ok=false and used to return
    // without OnConnect, so ClientContext stayed in the session and Stop(true)
    // waited for CQ forever. DirectWriteToPartition(false) keeps reconnect on
    // Connect() rather than DescribePartition.
    Y_UNIT_TEST(StopDuringReconnectDelayDoesNotDeadlock) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        TDriver driver(setup.MakeDriverConfig());
        TTopicClient client(driver);

        auto session = client.CreateWriteSession(
            TWriteSessionSettings()
                .Path(setup.GetTopicPath())
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .DirectWriteToPartition(false)
                .RetryPolicy(IRetryPolicy::GetFixedIntervalPolicy(
                    TDuration::Seconds(10),
                    TDuration::Seconds(10))));

        Y_UNUSED(WaitForWriteToken(*session));

        setup.GetServer().ShutdownGRpc();
        Sleep(TDuration::MilliSeconds(500));

        StopDriverOrFail(driver);
        session.reset();
    }

    // Compression used to capture the topic client. The client owns the default
    // compression executor, so the task kept that pool alive and the pool was
    // destroyed from one of its own threads. The sibling worker then finished
    // without a join and ThreadSanitizer reported a thread leak (YDBBUGS-957).
    Y_UNIT_TEST(CompressionDoesNotLeakClientExecutorThreads) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        TDriver driver(setup.MakeDriverConfig());

        {
            TTopicClient warmup(driver);
            auto session = warmup.CreateWriteSession(
                TWriteSessionSettings()
                    .Path(setup.GetTopicPath())
                    .MessageGroupId("warmup-raw")
                    .Codec(ECodec::RAW));
            auto token = WaitForWriteToken(*session);
            session->Write(std::move(token), "warmup");
            session.reset();
        }
        size_t baseline = CountOsThreads();
        size_t stableReads = 0;
        for (int attempt = 0; attempt < 30 && stableReads < 3; ++attempt) {
            Sleep(TDuration::MilliSeconds(100));
            const size_t now = CountOsThreads();
            if (now == baseline) {
                ++stableReads;
            } else {
                baseline = now;
                stableReads = 0;
            }
        }

        std::atomic<bool> entered{false};
        std::atomic<bool> release{false};
        struct TRestoreGzipCodec {
            ~TRestoreGzipCodec() {
                TCodecMap::GetTheCodecMap().Set(
                    static_cast<ui32>(ECodec::GZIP),
                    std::make_unique<TGzipCodec>());
            }
        } restoreGzipCodec;
        Y_UNUSED(restoreGzipCodec);
        TCodecMap::GetTheCodecMap().Set(
            static_cast<ui32>(ECodec::GZIP),
            std::make_unique<TBlockingGzipCodec>(&entered, &release));

        auto client = std::make_shared<TTopicClient>(driver);
        auto session = client->CreateWriteSession(
            TWriteSessionSettings()
                .Path(setup.GetTopicPath())
                .MessageGroupId("compress-leak")
                .Codec(ECodec::GZIP)
                .BatchFlushInterval(TDuration::Zero())
                .BatchFlushMessageCount(1));
        auto token = WaitForWriteToken(*session);
        session->Write(std::move(token), "payload");

        const auto enteredDeadline = TInstant::Now() + TDuration::Seconds(15);
        while (!entered.load() && TInstant::Now() < enteredDeadline) {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_C(entered.load(), "compression did not start on the default executor");

        std::atomic<bool> destroyStarted{false};
        auto destroyDone = NThreading::NewPromise<void>();
        std::thread releaser([&] {
            const auto deadline = TInstant::Now() + TDuration::Seconds(20);
            while (!destroyStarted.load() && TInstant::Now() < deadline) {
                Sleep(TDuration::MilliSeconds(10));
            }
            // Give the client destructor time to block in executor Stop before the
            // compression task is allowed to finish.
            Sleep(TDuration::MilliSeconds(200));
            release.store(true);
        });
        std::thread destroyer([&] {
            destroyStarted.store(true);
            session.reset();
            client.reset();
            destroyDone.SetValue();
        });

        const bool destroyed = destroyDone.GetFuture().Wait(TDuration::Seconds(20));
        if (!destroyed) {
            release.store(true);
        }
        destroyer.join();
        releaser.join();
        UNIT_ASSERT_C(destroyed, "write session destroy did not finish; executor Stop is stuck");

        const auto quietDeadline = TInstant::Now() + TDuration::Seconds(5);
        size_t left = CountOsThreads();
        while (left > baseline && TInstant::Now() < quietDeadline) {
            Sleep(TDuration::MilliSeconds(50));
            left = CountOsThreads();
        }
        UNIT_ASSERT_C(left <= baseline,
            "client executor threads are still alive after write session shutdown: baseline "
            << baseline << ", now " << left);

        StopDriverOrFail(driver);
    }
}

} // namespace NYdb::NTopic::NTests
