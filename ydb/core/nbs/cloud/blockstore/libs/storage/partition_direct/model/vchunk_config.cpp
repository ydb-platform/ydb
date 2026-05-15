#include "vchunk_config.h"

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// static
TVChunkConfig
TVChunkConfig::Make(ui32 vChunkIndex, size_t hostCount, size_t primaryCount)
{
    return TVChunkConfig{
        .VChunkIndex = vChunkIndex,
        .PBufferHosts =
            THostStatusList::MakeRotating(hostCount, vChunkIndex, primaryCount),
        .DDiskHosts =
            THostStatusList::MakeRotating(hostCount, vChunkIndex, primaryCount),
    };
}

bool TVChunkConfig::IsValid() const
{
    if (PBufferHosts.HostCount() != DDiskHosts.HostCount()) {
        return false;
    }
    if (PBufferHosts.HostCount() == 0 ||
        PBufferHosts.HostCount() > MaxHostCount)
    {
        return false;
    }
    return !PBufferHosts.GetActive().Empty() && !DDiskHosts.GetActive().Empty();
}

TString TVChunkConfig::DebugPrint() const
{
    TStringBuilder result;
    result << "[" << VChunkIndex << "] PBuffer{" << PBufferHosts.DebugPrint()
           << "} DDisk{" << DDiskHosts.DebugPrint() << "}";
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
