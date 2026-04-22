#pragma once

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class EOperation
{
    ReadFromPBuffer,
    ReadFromDDisk,
    WriteToPBuffer,
    WriteToDDisk,
    Flush,
    FlushCrossNode,
    Erase,
};

class THostStat
{
public:
    void OnSuccess(TInstant now, TDuration executionTime, EOperation operation);
    void OnError(TInstant now, EOperation operation);

    [[nodiscard]] TDuration ErrorsDuration(
        TInstant now,
        size_t* errorCount) const;

private:
    TInstant LastSuccess;
    TInstant LastError;
    size_t ErrorCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
