#include "count_size.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void TCountAndSize::Add(ui64 bytes)
{
    ++Count;
    Size += bytes;
}

void TCountAndSize::Sub(ui64 bytes)
{
    Y_DEBUG_ABORT_UNLESS(Count > 0);
    Y_DEBUG_ABORT_UNLESS(Size >= bytes);

    if (Count > 0) {
        --Count;
    }

    if (Size >= bytes) {
        Size -= bytes;
    } else {
        Size = 0;
    }
}

TCountAndSize& TCountAndSize::operator+=(const TCountAndSize& rhs)
{
    Count += rhs.Count;
    Size += rhs.Size;
    return *this;
}

TString TCountAndSize::Print(bool humanReadable) const
{
    TStringBuilder result;
    result << Count << " / ";
    if (humanReadable) {
        result << FormatByteSize(Size);
    } else {
        result << Size;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
