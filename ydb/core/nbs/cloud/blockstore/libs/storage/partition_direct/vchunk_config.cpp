#include "vchunk_config.h"

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

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
