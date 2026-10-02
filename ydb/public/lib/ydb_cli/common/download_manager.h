#pragma once

#include <util/generic/string.h>
#include <util/datetime/base.h>

#include <functional>

namespace NYdb {
namespace NConsoleClient {

// Callback function for download progress updates
// Parameters: downloaded bytes, total bytes (0 if unknown)
using TDownloadProgressCallback = std::function<void(ui64 downloaded, ui64 total)>;

struct TDownloadResult {
    bool Success = false;
    TString ErrorMessage;
    ui64 BytesDownloaded = 0;
};

// Downloads a file from URL to the specified path with progress tracking
// Supports HTTP and HTTPS URLs
// Progress callback is called periodically during download
TDownloadResult DownloadFile(
    const TString& url,
    const TString& destinationPath,
    TDownloadProgressCallback progressCallback = {},
    TDuration connectTimeout = TDuration::Seconds(60),
    TDuration socketTimeout = TDuration::Seconds(60));

} // namespace NConsoleClient
} // namespace NYdb
