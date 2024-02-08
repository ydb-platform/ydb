#pragma once

#include <yt/cpp/mapreduce/interface/io.h>

#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/stream/buffered.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TJobWriterStream
{
public:
    explicit TJobWriterStream(int fd);
    explicit TJobWriterStream(const TFile& file);
    ~TJobWriterStream() = default;

public:
    static constexpr size_t BufferSize = 1 << 20;
    TFile FDFile;
    TUnbufferedFileOutput FDOutput;
    TBufferedOutput BufferedOutput;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TJobWriter
    : public IProxyOutput
{
public:
    explicit TJobWriter(size_t outputTableCount);
    explicit TJobWriter(const TVector<TFile>& fileList);

    size_t GetBufferMemoryUsage() const override;
    size_t GetStreamCount() const override;
    IOutputStream* GetStream(size_t tableIndex) const override;
    void OnRowFinished(size_t tableIndex) override;

private:
    TVector<std::unique_ptr<NDetail::TJobWriterStream>> Streams_;
};

////////////////////////////////////////////////////////////////////////////////

class TSingleStreamJobWriter
    : public IProxyOutput
{
public:
    explicit TSingleStreamJobWriter(size_t tableIndex);

    size_t GetBufferMemoryUsage() const override;
    size_t GetStreamCount() const override;
    IOutputStream* GetStream(size_t tableIndex) const override;
    void OnRowFinished(size_t tableIndex) override;

private:
    const size_t TableIndex_;
    std::unique_ptr<NDetail::TJobWriterStream> Stream_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
