#pragma once

#include "download_limiter.h"

#include <util/stream/file.h>

namespace NYql {

class TDownloadOutputFileStream final: public IOutputStream {
public:
    TDownloadOutputFileStream(const TFile& file, TDownloadLimiter limiter);

private:
    void DoWrite(const void* data, size_t size) override;
    void DoFlush() override;
    void DoFinish() override;

    TUnbufferedFileOutput Output_;
    const TDownloadLimiter Limiter_;
};

} // namespace NYql
