#pragma once

#include <yt/cpp/mapreduce/interface/io.h>

#include <util/stream/buffered.h>
#include <util/stream/file.h>
#include <util/system/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TJobReader
    : public TRawTableReader
{
public:
    explicit TJobReader(int fd);
    explicit TJobReader(const TFile& file);

    bool Retry(
        const TMaybe<ui32>& rangeIndex,
        const TMaybe<ui64>& rowIndex,
        const std::exception_ptr& error) override;
    void ResetRetries() override;
    bool HasRangeIndices() const override;

protected:
    size_t DoRead(void* buf, size_t len) override;

private:
    TFile FdFile_;
    TUnbufferedFileInput FdInput_;
    TBufferedInput BufferedInput_;

    static const size_t BUFFER_SIZE = 64 << 10;
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT
