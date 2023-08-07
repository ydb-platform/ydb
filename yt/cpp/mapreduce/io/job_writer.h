#pragma once

#include <yt/cpp/mapreduce/interface/io.h>

#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/stream/buffered.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TJobWriter
    : public IProxyOutput
{
public:
    explicit TJobWriter(size_t outputTableCount);
    explicit TJobWriter(const TVector<TFile>& fileList);

    size_t GetStreamCount() const override;
    IOutputStream* GetStream(size_t tableIndex) const override;
    void OnRowFinished(size_t tableIndex) override;

private:
    struct TStream {
        TFile FdFile;
        TUnbufferedFileOutput FdOutput;
        TBufferedOutput BufferedOutput;

        explicit TStream(int fd);
        explicit TStream(const TFile& file);
        ~TStream();

        static const size_t BUFFER_SIZE = 1 << 20;
    };

    TVector<THolder<TStream>> Streams_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
