#include "job_writer.h"

#include <yt/cpp/mapreduce/interface/io.h>

#include <util/system/file.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TJobWriterStream::TJobWriterStream(int fd)
    : TJobWriterStream(Duplicate(fd))
{ }

TJobWriterStream::TJobWriterStream(const TFile& file)
    : FdFile(file)
    , FdOutput(FdFile)
    , BufferedOutput(&FdOutput, BufferSize)
{ }

TJobWriterStream::~TJobWriterStream()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TJobWriter::TJobWriter(size_t outputTableCount)
{
    for (size_t i = 0; i < outputTableCount; ++i) {
        Streams_.emplace_back(MakeHolder<NDetail::TJobWriterStream>(int(i * 3 + 1)));
    }
}

TJobWriter::TJobWriter(const TVector<TFile>& fileList)
{
    for (const auto& f : fileList) {
        Streams_.emplace_back(MakeHolder<NDetail::TJobWriterStream>(f));
    }
}

size_t TJobWriter::GetStreamCount() const
{
    return Streams_.size();
}

IOutputStream* TJobWriter::GetStream(size_t tableIndex) const
{
    if (tableIndex >= Streams_.size()) {
        ythrow TIOException() <<
            "Table index " << tableIndex <<
            " is out of range [0, " << Streams_.size() << ")";
    }
    return &Streams_[tableIndex]->BufferedOutput;
}

void TJobWriter::OnRowFinished(size_t)
{ }

size_t TJobWriter::GetBufferMemoryUsage() const
{
    return NDetail::TJobWriterStream::BufferSize * GetStreamCount();
}

////////////////////////////////////////////////////////////////////////////////

THolder<IProxyOutput> CreateRawJobWriter(size_t outputTableCount)
{
    return ::MakeHolder<TJobWriter>(outputTableCount);
}

////////////////////////////////////////////////////////////////////////////////

TSingleStreamJobWriter::TSingleStreamJobWriter(size_t tableIndex)
    : TableIndex_(tableIndex)
    , Stream_(MakeHolder<NDetail::TJobWriterStream>(int(tableIndex * 3 + 1)))
{ }

size_t TSingleStreamJobWriter::GetStreamCount() const
{
    return 1;
}

IOutputStream* TSingleStreamJobWriter::GetStream(size_t tableIndex) const
{
    if (tableIndex != TableIndex_) {
        ythrow TIOException() <<
            "Table index " << tableIndex <<
            " does not match this SignleTableJobWriter with index " << TableIndex_;
    }
    return &Stream_->BufferedOutput;
}

void TSingleStreamJobWriter::OnRowFinished(size_t)
{ }

size_t TSingleStreamJobWriter::GetBufferMemoryUsage() const
{
    return NDetail::TJobWriterStream::BufferSize * GetStreamCount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
