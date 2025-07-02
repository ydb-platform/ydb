#include <ydb/library/workload/tpcc/log_backend.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/logger/backend.h>

#include <sstream>
#include <vector>

using namespace NYdb;
using namespace NYdb::NTPCC;

namespace {

// Mock backend for testing
class TMockLogBackend : public TLogBackend {
public:
    void WriteData(const TLogRecord& rec) override {
        WrittenLogs.emplace_back(rec.Data, rec.Len);
    }

    void ReopenLog() override {
        ReopenCalled = true;
    }

    std::vector<std::string> WrittenLogs;
    bool ReopenCalled = false;
};

// Helper to create log records - takes const reference to ensure string stays alive
TLogRecord CreateLogRecord(const std::string& message) {
    return TLogRecord(TLOG_INFO, message.c_str(), message.size());
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TLogBackendWithCaptureTest) {

    Y_UNIT_TEST(ShouldCreateWithValidParameters) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 100);
        // Constructor should not throw and backend should be usable
        UNIT_ASSERT(true); // If we reach here, construction succeeded
    }

    Y_UNIT_TEST(ShouldNotCaptureInitially) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 5);

        std::vector<std::string> capturedLines;
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });

        UNIT_ASSERT(capturedLines.empty());
    }

        Y_UNIT_TEST(ShouldCaptureWhenActive) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 5);

        backend.StartCapture();

        std::string msg1 = "Test message 1";
        std::string msg2 = "Test message 2";

        auto record = CreateLogRecord(msg1);
        backend.WriteData(record);

        record = CreateLogRecord(msg2);
        backend.WriteData(record);

        std::vector<std::string> capturedLines;
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });

        UNIT_ASSERT_VALUES_EQUAL(capturedLines.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[0], "Test message 1");
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[1], "Test message 2");
    }

        Y_UNIT_TEST(ShouldStopCapturingAfterStop) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 5);

        std::string beforeMsg = "Before stop";
        std::string afterMsg = "After stop";

        backend.StartCapture();
        auto record = CreateLogRecord(beforeMsg);
        backend.WriteData(record);

        backend.StopCapture();
        record = CreateLogRecord(afterMsg);
        backend.WriteData(record);

        std::vector<std::string> capturedLines;
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });

        // Should be empty after StopCapture clears everything
        UNIT_ASSERT(capturedLines.empty());
    }

        Y_UNIT_TEST(ShouldClearLogsOnStopCapture) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 5);

        std::string testMsg = "Test message";

        backend.StartCapture();
        auto record = CreateLogRecord(testMsg);
        backend.WriteData(record);

        // Verify we have logs
        std::vector<std::string> capturedLines;
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });
        UNIT_ASSERT_VALUES_EQUAL(capturedLines.size(), 1);

        // Stop and verify logs are cleared
        backend.StopCapture();
        capturedLines.clear();
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });
        UNIT_ASSERT(capturedLines.empty());
    }

    Y_UNIT_TEST(ShouldTruncateWhenExceedingMaxLines) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 3);

        backend.StartCapture();

        // Add more logs than max capacity
        std::vector<std::string> messages;
        for (int i = 1; i <= 5; ++i) {
            messages.push_back("Message " + std::to_string(i));
        }
        for (const auto& msg : messages) {
            auto record = CreateLogRecord(msg);
            backend.WriteData(record);
        }

        std::vector<std::string> capturedLines;
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });

        // Should have truncation message + last 3 messages
        UNIT_ASSERT_VALUES_EQUAL(capturedLines.size(), 4);
        UNIT_ASSERT(capturedLines[0].find("logs truncated") != std::string::npos);
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[1], "Message 3");
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[2], "Message 4");
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[3], "Message 5");
    }

    Y_UNIT_TEST(ShouldHandleMassiveLogInflux) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 2);

        backend.StartCapture();

        // Add way more logs than capacity in one batch
        std::vector<std::string> messages;
        for (int i = 1; i <= 10; ++i) {
            messages.push_back("Msg " + std::to_string(i));
        }
        for (const auto& msg : messages) {
            auto record = CreateLogRecord(msg);
            backend.WriteData(record);
        }

        std::vector<std::string> capturedLines;
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });

        // Should handle massive influx and keep only last MaxLines
        UNIT_ASSERT_VALUES_EQUAL(capturedLines.size(), 3); // truncation msg + 2 lines
        UNIT_ASSERT(capturedLines[0].find("logs truncated") != std::string::npos);
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[1], "Msg 9");
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[2], "Msg 10");
    }

        Y_UNIT_TEST(ShouldHandleEmptyMessages) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 5);

        std::string emptyMsg = "";
        std::string nonEmptyMsg = "Non-empty";

        backend.StartCapture();

        auto record = CreateLogRecord(emptyMsg);
        backend.WriteData(record);

        record = CreateLogRecord(nonEmptyMsg);
        backend.WriteData(record);

        std::vector<std::string> capturedLines;
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });

        UNIT_ASSERT_VALUES_EQUAL(capturedLines.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[0], "");
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[1], "Non-empty");
    }

    Y_UNIT_TEST(ShouldCallReopenOnRealBackend) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 5);

        // This should not throw - we can't easily test the actual call
        // without more complex mocking, but we can verify it doesn't crash
        backend.ReopenLog();
        UNIT_ASSERT(true);
    }

        Y_UNIT_TEST(ShouldHandleMultipleStartStopCycles) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 5);

        std::string cycle1Msg = "Cycle 1";
        std::string cycle2Msg = "Cycle 2";

        // First cycle
        backend.StartCapture();
        auto record = CreateLogRecord(cycle1Msg);
        backend.WriteData(record);
        backend.StopCapture();

        // Second cycle
        backend.StartCapture();
        record = CreateLogRecord(cycle2Msg);
        backend.WriteData(record);

        std::vector<std::string> capturedLines;
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });

        // Should only have logs from second cycle
        UNIT_ASSERT_VALUES_EQUAL(capturedLines.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[0], "Cycle 2");

        backend.StopCapture();
    }

        Y_UNIT_TEST(ShouldHandleZeroMaxLines) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 0);

        std::string testMsg = "Should be truncated";

        backend.StartCapture();
        auto record = CreateLogRecord(testMsg);
        backend.WriteData(record);

        std::vector<std::string> capturedLines;
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });

        // With 0 max lines, should only show truncation message
        UNIT_ASSERT_VALUES_EQUAL(capturedLines.size(), 1);
        UNIT_ASSERT(capturedLines[0].find("logs truncated") != std::string::npos);
    }

    Y_UNIT_TEST(ShouldPreserveFIFOOrder) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 10);

        backend.StartCapture();

        std::vector<std::string> expectedOrder;
        for (int i = 1; i <= 5; ++i) {
            expectedOrder.push_back("Order test " + std::to_string(i));
        }
        for (const auto& msg : expectedOrder) {
            auto record = CreateLogRecord(msg);
            backend.WriteData(record);
        }

        std::vector<std::string> capturedLines;
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });

        UNIT_ASSERT_VALUES_EQUAL(capturedLines.size(), expectedOrder.size());
        for (size_t i = 0; i < expectedOrder.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(capturedLines[i], expectedOrder[i]);
        }
    }

    Y_UNIT_TEST(ShouldHandleIncrementalTruncation) {
        TLogBackendWithCapture backend("console", TLOG_INFO, 3);

        backend.StartCapture();

        // Add initial logs
        std::vector<std::string> initialMsgs;
        for (int i = 1; i <= 2; ++i) {
            initialMsgs.push_back("Initial " + std::to_string(i));
        }
        for (const auto& msg : initialMsgs) {
            auto record = CreateLogRecord(msg);
            backend.WriteData(record);
        }

        // Get logs to process them
        std::vector<std::string> capturedLines;
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });
        UNIT_ASSERT_VALUES_EQUAL(capturedLines.size(), 2);

        // Add more logs that will cause incremental truncation
        std::vector<std::string> additionalMsgs;
        for (int i = 3; i <= 5; ++i) {
            additionalMsgs.push_back("Additional " + std::to_string(i));
        }
        for (const auto& msg : additionalMsgs) {
            auto record = CreateLogRecord(msg);
            backend.WriteData(record);
        }

        capturedLines.clear();
        backend.GetLogLines([&](const std::string& line) {
            capturedLines.push_back(line);
        });

        // Should have truncation message + last 3 messages
        UNIT_ASSERT_VALUES_EQUAL(capturedLines.size(), 4);
        UNIT_ASSERT(capturedLines[0].find("logs truncated") != std::string::npos);
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[1], "Additional 3");
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[2], "Additional 4");
        UNIT_ASSERT_VALUES_EQUAL(capturedLines[3], "Additional 5");
    }
}
