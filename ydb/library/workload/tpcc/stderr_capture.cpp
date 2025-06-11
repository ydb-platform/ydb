#include "stderr_capture.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <iostream>
#include <sstream>

namespace NYdb::NTPCC {

TStdErrCapture::TStdErrCapture(size_t maxLines)
    : MaxLines(maxLines)
{
}

TStdErrCapture::~TStdErrCapture() {
    if (IsCapturing) {
        RestoreAndFlush();
    }
}

void TStdErrCapture::StartCapture() {
    if (IsCapturing) {
        return;
    }

    // Capture stderr at file descriptor level
    OriginalStderrFd = dup(STDERR_FILENO);  // Save original stderr

    // Create a pipe to capture stderr
    int pipefd[2];
    if (pipe(pipefd) == 0) {
        CapturedStderrFd = pipefd[0];  // Read end
        dup2(pipefd[1], STDERR_FILENO);  // Redirect stderr to write end
        close(pipefd[1]);  // Close write end (stderr now uses it)

        // Make the read end non-blocking
        int flags = fcntl(CapturedStderrFd, F_GETFL, 0);
        fcntl(CapturedStderrFd, F_SETFL, flags | O_NONBLOCK);

        IsCapturing = true;
    }
}

void TStdErrCapture::UpdateCapture() {
    if (!IsCapturing) {
        return;
    }

    ReadFromPipe();
    ProcessBuffer();
}

void TStdErrCapture::ReadFromPipe() {
    if (CapturedStderrFd < 0) {
        return;
    }

    char buffer[4096];
    ssize_t bytesRead;
    while ((bytesRead = read(CapturedStderrFd, buffer, sizeof(buffer) - 1)) > 0) {
        buffer[bytesRead] = '\0';
        RawBuffer += buffer;
    }
}

void TStdErrCapture::ProcessBuffer() {
    if (RawBuffer.empty()) {
        return;
    }

    std::stringstream ss(RawBuffer);
    std::string line;
    std::string remainder;

    // Process complete lines
    while (std::getline(ss, line)) {
        if (ss.eof() && !ss.fail()) {
            // This is the last line and there's no trailing newline
            remainder = line;
            break;
        }

        LogLines.push_back(line);

        // Maintain size limit
        if (LogLines.size() > MaxLines) {
            LogLines.pop_front();
            TruncatedCount++;
        }
    }

    // Keep any incomplete line for next processing
    RawBuffer = remainder;
}

const std::deque<std::string>& TStdErrCapture::GetLogLines() const {
    return LogLines;
}

size_t TStdErrCapture::GetTruncatedCount() const {
    return TruncatedCount;
}

void TStdErrCapture::RestoreAndFlush() {
    if (!IsCapturing) {
        return;
    }

    // Read any remaining data from captured stderr before restoring
    ReadFromPipe();
    ProcessBuffer();

    // Restore stderr file descriptor
    if (OriginalStderrFd >= 0) {
        dup2(OriginalStderrFd, STDERR_FILENO);  // Restore original stderr
        close(OriginalStderrFd);
        OriginalStderrFd = -1;
    }

    if (CapturedStderrFd >= 0) {
        close(CapturedStderrFd);
        CapturedStderrFd = -1;
    }

    // Output all captured logs to the restored stderr
    if (TruncatedCount > 0) {
        std::cerr << "logs truncated: " << TruncatedCount << " lines\n";
    }

    for (const auto& line : LogLines) {
        std::cerr << line << "\n";
    }
    std::cerr.flush();

    // Clear captured data
    LogLines.clear();
    TruncatedCount = 0;
    RawBuffer.clear();
    IsCapturing = false;
}

} // namespace NYdb::NTPCC