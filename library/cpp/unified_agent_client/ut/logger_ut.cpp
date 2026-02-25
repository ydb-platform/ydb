#include <library/cpp/unified_agent_client/logger.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/logger/stream.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <sstream>
#include <thread>

using namespace NUnifiedAgent;
using namespace NMonitoring;

// Helper to capture log output
class TTestLogBackend : public TLogBackend {
public:
    explicit TTestLogBackend(std::ostringstream* stream)
        : Stream(stream)
    {
    }

    void WriteData(const TLogRecord& rec) override {
        (*Stream) << rec.Data;
    }

    void ReopenLog() override {
    }

private:
    std::ostringstream* Stream;
};

class TLoggerTest : public ::testing::Test {
protected:
    void SetUp() override {
        LogStream = std::make_unique<std::ostringstream>();
        Log = std::make_unique<TLog>(MakeHolder<TTestLogBackend>(LogStream.get()));

        // Create counters for testing
        Counters = MakeIntrusive<TDynamicCounters>();
        ReceivedCounter = Counters->GetCounter("ReceivedCounter", true).Get();
        DroppedCounter = Counters->GetCounter("DroppedCounter", true).Get();

        // Create logger with throttling: 20 logs per 1 second
        LoggerImpl = std::make_unique<TLogger>(*Log, Nothing(), 20, 1,
                                               TLogger::TCounters{.RecordsReceived = ReceivedCounter,
                                                                  .RecordsDropped = DroppedCounter});
        Logger = LoggerImpl->Child("");
    }

    std::string GetLogOutput() {
        return LogStream->str();
    }

    void ClearLogOutput() {
        LogStream->str("");
        LogStream->clear();
    }

    std::unique_ptr<std::ostringstream> LogStream;
    std::unique_ptr<TLog> Log;
    std::unique_ptr<TLogger> LoggerImpl;
    TScopeLogger Logger;
    TIntrusivePtr<TDynamicCounters> Counters;
    TDeprecatedCounter* ReceivedCounter;
    TDeprecatedCounter* DroppedCounter;
};

TEST_F(TLoggerTest, BasicThrottling) {
    // Log 40 messages using YLOG_T macro
    for (int i = 0; i < 40; ++i) {
        YLOG_ERROR_T("Test message {}", i);
    }

    const auto output = GetLogOutput();
    const int count = std::count(output.begin(), output.end(), '\n');

    // Should log exactly 20 messages (first slot)
    EXPECT_EQ(count, 20);

    // Check counters: 40 received, 20 dropped
    EXPECT_EQ(ReceivedCounter->Val(), 40);
    EXPECT_EQ(DroppedCounter->Val(), 20);
}

TEST_F(TLoggerTest, SuppressedCounter) {
    for (int i = 20 + 13; i >= 0; --i) {
        if (i == 0) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        YLOG_ERROR_T("Test message");
    }

    const auto output = GetLogOutput();

    // Should log 20 messages in first slot + 1 in second slot with suppressed count
    const int count = std::count(output.begin(), output.end(), '\n');
    EXPECT_EQ(count, 21);

    // Should contain "suppressed" in the output (from the message after sleep)
    EXPECT_NE(output.find("[+13 suppressed]"), std::string::npos);

    // Check counters: 34 received (33 + 1 after sleep), 13 dropped
    EXPECT_EQ(ReceivedCounter->Val(), 34);
    EXPECT_EQ(DroppedCounter->Val(), 13);
}

TEST_F(TLoggerTest, IndependentThrottlingPerLocation) {
    // Each log location should have independent throttling
    for (int i = 0; i < 40; ++i) {
        YLOG_ERROR_T("Location A");  // Line 1
        YLOG_ERROR_T("Location B");  // Line 2 - different location
    }

    const auto output = GetLogOutput();

    // Both locations should log ~20 messages each
    const int locACount = std::count(output.begin(), output.end(), 'A');
    const int locBCount = std::count(output.begin(), output.end(), 'B');

    EXPECT_EQ(locACount, 20);
    EXPECT_EQ(locBCount, 20);

    // Total counters: 80 received, 40 dropped
    EXPECT_EQ(ReceivedCounter->Val(), 80);
    EXPECT_EQ(DroppedCounter->Val(), 40);
}

TEST_F(TLoggerTest, FormatMessage) {
    YLOG_ERROR_T("Simple message");
    YLOG_ERROR_T("Formatted: {}, {}", 42, "test");

    const auto output = GetLogOutput();

    EXPECT_NE(output.find("Simple message"), std::string::npos);
    EXPECT_NE(output.find("Formatted: 42, test"), std::string::npos);
}

TEST_F(TLoggerTest, DifferentLogLevels) {
    YLOG_INFO_T("Info message");
    YLOG_WARNING_T("Warning message");
    YLOG_ERROR_T("Error message");

    const auto output = GetLogOutput();

    EXPECT_NE(output.find("Info message"), std::string::npos);
    EXPECT_NE(output.find("Warning message"), std::string::npos);
    EXPECT_NE(output.find("Error message"), std::string::npos);
}
