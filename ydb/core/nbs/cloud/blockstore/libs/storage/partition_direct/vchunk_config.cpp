#include "vchunk_config.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

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
