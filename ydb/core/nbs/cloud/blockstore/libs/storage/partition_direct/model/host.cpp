#include "host.h"

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

bool THostRoute::operator<(const THostRoute& other) const
{
    if (SourceHostIndex != other.SourceHostIndex) {
        return SourceHostIndex < other.SourceHostIndex;
    }
    return DestinationHostIndex < other.DestinationHostIndex;
}

TString THostRoute::DebugPrint() const
{
    TStringBuilder result;
    result << PrintHostIndex(SourceHostIndex) << "->"
           << PrintHostIndex(DestinationHostIndex);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TString PrintHostIndex(THostIndex hostIndex)
{
    TStringBuilder result;
    result << "H" << static_cast<ui32>(hostIndex);
    return result;
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
