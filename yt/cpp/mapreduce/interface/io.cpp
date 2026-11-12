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

void IReaderImplBase::Abort()
{
    Y_ABORT("Unimplemented");
}

bool IReaderImplBase::IsAborted() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void LogTableReaderStatistics(ui64 rowCount, TMaybe<size_t> byteCount)
{
    YT_TLOG_DEBUG("Table reader finished")
        .With("RowCount", rowCount)
        .WithIf(byteCount.Defined(), "Size", byteCount.GetOrElse(0));
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
