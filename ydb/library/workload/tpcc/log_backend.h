#pragma once

#include "log.h"

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/log.h>

#include <util/system/mutex.h>

#include <functional>
#include <deque>
#include <string>
#include <utility>

namespace NYdb::NTPCC {

// Either writes logs to the real backend
// or captures logs to display them in TUI
class TLogBackendWithCapture : public TLogBackend {
public:
    explicit TLogBackendWithCapture(const TString& type, ELogPriority priority, size_t maxLines);
    virtual ~TLogBackendWithCapture() = default;

    void StartCapture();
    void StopCapture();
    void StopCaptureAndFlush(IOutputStream& os);

    // Get current log lines to display in TUI
    // Assumes single consumer, multiple producers
    void GetLogLines(const TLogProcessor& processor);

    // TLogBackend interface (threadsafe)

    void WriteData(const TLogRecord& rec) override;

    void ReopenLog() override {
        RealBackend->ReopenLog();
    }

    ELogPriority FiltrationLevel() const override {
        return RealBackend->FiltrationLevel();
    }

private:
    void ProcessNewLines(bool logTaken);

private:
    THolder<TLogBackend> RealBackend;
    const size_t MaxLines;

    std::deque<std::pair<ELogPriority, std::string>> LogLines;
    size_t TruncatedCount = 0;

    TMutex CapturingMutex;
    bool IsCapturing = false;
    std::vector<std::pair<ELogPriority, std::string>> CapturedLines;
};

} // namespace NYdb::NTPCC
