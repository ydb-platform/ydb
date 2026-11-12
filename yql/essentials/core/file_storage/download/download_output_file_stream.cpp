#include "download_output_file_stream.h"

#include <utility>

namespace NYql {

TDownloadOutputFileStream::TDownloadOutputFileStream(const TFile& file, TDownloadLimiter limiter)
    : Output_(file)
    , Limiter_(std::move(limiter))
{
}

void TDownloadOutputFileStream::DoWrite(const void* data, size_t size) {
    const auto* current = static_cast<const char*>(data);
    while (size != 0) {
        const size_t chunkSize = Limiter_.GetQuota(size);
        Output_.Write(current, chunkSize);
        current += chunkSize;
        size -= chunkSize;
    }
}

void TDownloadOutputFileStream::DoFlush() {
    Output_.Flush();
}

void TDownloadOutputFileStream::DoFinish() {
    Output_.Finish();
}

} // namespace NYql
