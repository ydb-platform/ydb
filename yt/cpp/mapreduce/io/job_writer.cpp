#include "job_writer.h"

#include <yt/cpp/mapreduce/interface/io.h>

#include <util/system/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TJobWriter::TStream::TStream(int fd)
    : TStream(Duplicate(fd))
{ }

TJobWriter::TStream::TStream(const TFile& file)
    : FdFile(file)
    , FdOutput(FdFile)
    , BufferedOutput(&FdOutput, BUFFER_SIZE)
{ }

TJobWriter::TStream::~TStream()
{
}

////////////////////////////////////////////////////////////////////////////////

TJobWriter::TJobWriter(size_t outputTableCount)
{
    for (size_t i = 0; i < outputTableCount; ++i) {
        Streams_.emplace_back(MakeHolder<TStream>(int(i * 3 + 1)));
    }
}

TJobWriter::TJobWriter(const TVector<TFile>& fileList)
{
    for (const auto& f : fileList) {
        Streams_.emplace_back(MakeHolder<TStream>(f));
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

////////////////////////////////////////////////////////////////////////////////

THolder<IProxyOutput> CreateRawJobWriter(size_t outputTableCount)
{
    return ::MakeHolder<TJobWriter>(outputTableCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
