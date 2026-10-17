#include "host_state.h"

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TString THostState::DebugPrint() const
{
    TStringBuilder result;
    result << ToString(State) << " Used:" << UsedPBuffers.Print(true);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
