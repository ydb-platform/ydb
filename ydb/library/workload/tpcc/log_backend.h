#pragma once

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/log.h>

#include <util/system/mutex.h>

#include <functional>
#include <deque>
#include <string>

namespace NYdb::NTPCC {

// Either writes logs to the real backend
// or captures logs to display them in TUI
class TLogBackendWithCapture : public TLogBackend {
public:
    explicit TLogBackendWithCapture(const TString& type, ELogPriority priority, size_t maxLines);
    virtual ~TLogBackendWithCapture();

    void StartCapture();
    void StopCapture();

    // Get current log lines to display in TUI
    // Assumes single consumer, multiple producers
    void GetLogLines(const std::function<void(const std::string&)>& processor);

    // TLogBackend interface (threadsafe)

    void WriteData(const TLogRecord& rec) override;

    void ReopenLog() override {
        RealBackend->ReopenLog();
    }

    ELogPriority FiltrationLevel() const override {
        return RealBackend->FiltrationLevel();
    }

private:
    void ProcessNewLines();

private:
    THolder<TLogBackend> RealBackend;
    const size_t MaxLines;

    std::deque<std::string> LogLines;
    size_t TruncatedCount = 0;

    TMutex CapturingMutex;
    bool IsCapturing = false;
    std::vector<std::string> CapturedLines;
};

} // namespace NYdb::NTPCC
