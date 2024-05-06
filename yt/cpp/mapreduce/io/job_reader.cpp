#include "job_reader.h"

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TJobReader::TJobReader(int fd)
    : TJobReader(Duplicate(fd))
{ }

TJobReader::TJobReader(const TFile& file)
    : FdFile_(file)
    , FdInput_(FdFile_)
    , BufferedInput_(&FdInput_, BUFFER_SIZE)
{ }

bool TJobReader::Retry(
    const TMaybe<ui32>& /*rangeIndex*/,
    const TMaybe<ui64>& /*rowIndex*/,
    const std::exception_ptr& /*error*/)
{
    return false;
}

void TJobReader::ResetRetries()
{ }

bool TJobReader::HasRangeIndices() const
{
    return true;
}

size_t TJobReader::DoRead(void* buf, size_t len)
{
    return BufferedInput_.Read(buf, len);
}

////////////////////////////////////////////////////////////////////////////////

TRawTableReaderPtr CreateRawJobReader(int fd)
{
    return ::MakeIntrusive<TJobReader>(fd);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
