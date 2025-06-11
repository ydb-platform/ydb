#pragma once

#include <deque>
#include <string>

namespace NYdb::NTPCC {

// TODO: probably we could redirect log to temp file and read it...
class TStdErrCapture {
public:
    explicit TStdErrCapture(size_t maxLines);
    ~TStdErrCapture();

    void StartCapture();

    // Read any new data from captured stderr
    void UpdateCapture();

    // Get current log lines for display
    const std::deque<std::string>& GetLogLines() const;

    // Get count of truncated lines
    size_t GetTruncatedCount() const;

    // Restore stderr and flush captured content to original stderr
    void RestoreAndFlush();

private:
    void ReadFromPipe();
    void ProcessBuffer();

private:
    size_t MaxLines;
    std::deque<std::string> LogLines;
    size_t TruncatedCount = 0;

    int OriginalStderrFd = -1;
    int CapturedStderrFd = -1;
    std::string RawBuffer;

    bool IsCapturing = false;
};

} // namespace NYdb::NTPCC
