#include "log_backend.h"

#include <library/cpp/logger/backend_creator.h>

#include <util/string/builder.h>
#include <util/system/guard.h>

namespace NYdb::NTPCC {

TLogBackendWithCapture::TLogBackendWithCapture(const TString& type, ELogPriority priority, size_t maxLines)
    : RealBackend(CreateLogBackend(type, priority, true).Release())
    , MaxLines(maxLines)
{
    CapturedLines.reserve(MaxLines * 2);
}

TLogBackendWithCapture::~TLogBackendWithCapture() {
    StopCapture();
}

void TLogBackendWithCapture::StartCapture() {
    TGuard guard(CapturingMutex);
    IsCapturing = true;
}

void TLogBackendWithCapture::StopCapture() {
    TGuard guard(CapturingMutex);
    if (!IsCapturing) {
        return;
    }

    IsCapturing = false;
    CapturedLines.clear();
    LogLines.clear();
    TruncatedCount = 0;
}

void TLogBackendWithCapture::GetLogLines(const std::function<void(const std::string&)>& processor) {
    ProcessNewLines();

    if (TruncatedCount > 0) {
        processor("... logs truncated: " + std::to_string(TruncatedCount) + " lines");
    }

    for (const auto& line: LogLines) {
        processor(line);
    }
}

void TLogBackendWithCapture::ProcessNewLines() {
    std::vector<std::string> newLines;
    newLines.reserve(MaxLines * 2);
    {
        TGuard guard(CapturingMutex);
        newLines.swap(CapturedLines);
    }

    if (!newLines.empty()) {
        auto currentSize = LogLines.size();
        auto newSize = currentSize + newLines.size();
        if (newSize > MaxLines && newLines.size() > MaxLines) {
            TruncatedCount += LogLines.size();
            LogLines.clear();

            size_t newLinesTruncateCount = newLines.size() - MaxLines;
            TruncatedCount += newLinesTruncateCount;
            for (size_t i = newLinesTruncateCount; i < newLines.size(); ++i) {
                LogLines.emplace_back(std::move(newLines[i]));
            }
        } else {
            size_t popCount = 0;
            if (newSize > MaxLines) {
                popCount = newSize - MaxLines;
            }
            TruncatedCount += popCount;
            for (size_t i = 0; i < popCount && !LogLines.empty(); ++i) {
                LogLines.pop_front();
            }
            for (auto& line: newLines) {
                LogLines.emplace_back(std::move(line));
            }
        }
    }
}

void TLogBackendWithCapture::WriteData(const TLogRecord& record) {
    {
        TGuard guard(CapturingMutex);
        if (IsCapturing) {
            CapturedLines.emplace_back(record.Data, record.Len);
            return;
        }
    }
    RealBackend->WriteData(record);
}

} // namespace NYdb::NTPCC
