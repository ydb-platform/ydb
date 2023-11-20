#include "io.h"

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <util/string/cast.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TMaybe<size_t> IReaderImplBase::GetReadByteCount() const
{
    return Nothing();
}

i64 IReaderImplBase::GetTabletIndex() const
{
    Y_ABORT("Unimplemented");
}

bool IReaderImplBase::IsEndOfStream() const
{
    Y_ABORT("Unimplemented");
}

bool IReaderImplBase::IsRawReaderExhausted() const
{
    Y_ABORT("Unimplemented");
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void LogTableReaderStatistics(ui64 rowCount, TMaybe<size_t> byteCount)
{
    TString byteCountStr = (byteCount ? ::ToString(*byteCount) : "<unknown>");
    YT_LOG_DEBUG("Table reader has read %v rows, %v bytes",
        rowCount,
        byteCountStr);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
