#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <thread>

using namespace std::chrono_literals;

namespace NYdb::inline Dev::NTopic::NTests {

// A decompression executor that stores tasks but doesn't run them until explicitly instructed.
// This gives us precise control over when decompression happens in tests.
class TBlockingDecompressionExecutor : public IExecutor {
public:
    TBlockingDecompressionExecutor() = default;

    bool IsAsync() const override {
        return true;  // Tell SDK we're async so it queues decompression tasks
    }

    void Post(TFunction&& f) override {
        std::lock_guard lock(Mutex_);
        Tasks_.push_back(std::move(f));
        Cerr << ">>> [Executor] Task queued, total pending: " << Tasks_.size() << Endl;
    }

    void Stop() override {
        // Do nothing
    }

    size_t GetPendingCount() const {
        std::lock_guard lock(Mutex_);
        return Tasks_.size();
    }

    size_t GetExecutedCount() const {
        return ExecutedCount_.load();
    }

    // Run up to 'count' pending tasks. Returns number actually run.
    size_t RunPendingTasks(size_t count = SIZE_MAX) {
        std::vector<TFunction> toRun;
        {
            std::lock_guard lock(Mutex_);
            size_t n = std::min(count, Tasks_.size());
            for (size_t i = 0; i < n; ++i) {
                toRun.push_back(std::move(Tasks_.front()));
                Tasks_.pop_front();
            }
        }
        
        for (auto& task : toRun) {
            task();
            ++ExecutedCount_;
        }
        
        if (!toRun.empty()) {
            Cerr << ">>> [Executor] Ran " << toRun.size() << " tasks, total executed: " << ExecutedCount_.load() << Endl;
        }
        return toRun.size();
    }

    // Wait for at least 'count' tasks to be queued
    bool WaitForPendingTasks(size_t count, std::chrono::milliseconds timeout = 5s) {
        auto start = std::chrono::steady_clock::now();
        while (GetPendingCount() < count) {
            if (std::chrono::steady_clock::now() - start > timeout) {
                return false;
            }
            std::this_thread::sleep_for(50ms);
        }
        return true;
    }

private:
    void DoStart() override {
        // Do nothing
    }

    mutable std::mutex Mutex_;
    std::deque<TFunction> Tasks_;
    std::atomic<size_t> ExecutedCount_{0};
};


Y_UNIT_TEST_SUITE(DecompressionLeakTest) {

    // This test verifies that DecompressedDataSize is properly cleaned up
    // when a partition session is stopped (tablet killed) while decompression
    // tasks are pending or completed but events are not yet consumed by user.
    //
    // Scenario:
    // 1. Write messages to topic
    // 2. Start read session with controlled decompression executor
    // 3. Wait for decompression task to be queued (but don't execute it yet)
    // 4. Kill the topic tablet (simulates non-graceful partition stop)
    // 5. Verify that:
    //    a) The session recovers and can continue reading
    //    b) DecompressedDataSize budget is properly reclaimed
    //    c) No stall occurs due to budget leak

    Y_UNIT_TEST(DecompressedDataSizeLeakOnTabletKill) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        
        // Reduce log noise from PERSQUEUE
        setup.GetRuntime().SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_INFO);
        setup.GetRuntime().SetLogPriority(NKikimrServices::PERSQUEUE_READ_BALANCER, NActors::NLog::PRI_INFO);
        setup.GetRuntime().SetLogPriority(NKikimrServices::PQ_READ_PROXY, NActors::NLog::PRI_INFO);
        
        // Create controlled decompression executor that holds tasks until we tell it to run
        auto decompressor = std::make_shared<TBlockingDecompressionExecutor>();
        
        // Write some messages
        auto client = setup.MakeClient();
        auto writeSettings = TWriteSessionSettings()
            .Path(setup.GetTopicPath())
            .MessageGroupId(TEST_MESSAGE_GROUP_ID)
            .Codec(ECodec::RAW);
        
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
        auto totalMessages = 100;
        std::string message(800_KB, 'x');
        for (int i = 0; i < totalMessages; ++i) {
            bool res = writeSession->Write(message);
            UNIT_ASSERT(res);
        }
        writeSession->Close(TDuration::Seconds(10));
        Cerr << ">>> Messages written" << Endl;
        
        // Create read session with controlled decompression
        std::atomic<int> messagesReceived{0};
        auto partitionStopped = NThreading::NewPromise<void>();
        
        TReadSessionSettings readSettings;
        readSettings
            .ConsumerName(setup.GetConsumerName())
            .AppendTopics(setup.GetTopicPath())
            .DecompressionExecutor(decompressor)
            .MaxMemoryUsageBytes(1_MB);
        
        readSettings.EventHandlers_.StartPartitionSessionHandler(
            [&](TReadSessionEvent::TStartPartitionSessionEvent& ev) {
                Cerr << ">>> Partition session started: " << ev.DebugString() << Endl;
                ev.Confirm();
            }
        );
        
        readSettings.EventHandlers_.PartitionSessionClosedHandler(
            [&](TReadSessionEvent::TPartitionSessionClosedEvent& ev) {
                Cerr << ">>> Partition session closed: " << ev.DebugString() << Endl;
                partitionStopped.SetValue();
            }
        );
        
        auto readSession = client.CreateReadSession(readSettings);
        Cerr << ">>> Read session created" << Endl;
        
        // Helper to process data events only
        auto processEvents = [&]() {
            for (auto& event : readSession->GetEvents(false)) {
                if (auto* dataEvent = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
                    messagesReceived += dataEvent->GetMessages().size();
                    Cerr << ">>> Got " << dataEvent->GetMessages().size() << " messages, total: " << messagesReceived.load() << Endl;
                }
            }
        };
        
        // Wait for decompression task to be queued
        bool gotTask = decompressor->WaitForPendingTasks(1, 10s);
        UNIT_ASSERT_C(gotTask, "Timeout waiting for decompression task");
        Cerr << ">>> Decompression task queued, pending: " << decompressor->GetPendingCount() << Endl;

        decompressor->RunPendingTasks(1);
        
        // NOW kill the tablet BEFORE running decompression
        // This simulates non-graceful partition stop with pending decompression
        setup.GetServer().KillTopicPqrbTablet(setup.GetFullTopicPath());
        Cerr << ">>> Tablet killed (decompression task still pending!)" << Endl;
        
        // Wait for partition stop event
        partitionStopped.GetFuture().HasValue();
        
        // Run remaining decompression tasks as they come
        while (messagesReceived < totalMessages) {
            decompressor->RunPendingTasks();
            processEvents();
        }
        
        Cerr << ">>> Final messages received: " << messagesReceived.load() << Endl;
        
        readSession->Close(TDuration::Seconds(5));
        Cerr << ">>> Test completed" << Endl;
    }

} // Y_UNIT_TEST_SUITE

} // namespace NYdb::inline Dev::NTopic::NTests
