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

IOutputStream& operator<<(IOutputStream& out, THostAndNodeId value)
{
    out << "H" << static_cast<ui32>(value.HostIndex) << "#";
    if (value.NodeId == Max<ui32>()) {
        out << "??";
    } else {
        out << value.NodeId;
    }
    return out;
}

TString PrintHostIndex(THostIndex hostIndex)
{
    TStringBuilder result;
    result << "H" << static_cast<ui32>(hostIndex);
    return result;
}

TString PrintNodeId(ui32 nodeId)
{
    TStringBuilder result;
    result << "Node#" << nodeId;
    return result;
}

TString PrintHostAndNodeId(THostIndex hostIndex, ui32 nodeId)
{
    TStringBuilder result;
    result << THostAndNodeId{.HostIndex = hostIndex, .NodeId = nodeId};
    return result;
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
