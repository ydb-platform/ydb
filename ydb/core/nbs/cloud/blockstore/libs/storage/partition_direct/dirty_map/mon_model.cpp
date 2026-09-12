#include "mon_model.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void TDirtyMapStats::Aggregate(const TDirtyMapStats& stats)
{
    InflightCount += stats.InflightCount;
    ReadyToFlushCount += stats.ReadyToFlushCount;
    ReadyToEraseCount += stats.ReadyToEraseCount;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
