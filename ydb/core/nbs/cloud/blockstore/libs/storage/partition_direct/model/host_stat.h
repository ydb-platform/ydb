#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class EOperation
{
    ReadFromPBuffer,
    ReadFromDDisk,
    WriteToPBuffer,
    WriteToDDisk,
    WriteToManyPBuffers,
    Flush,
    FlushCrossNode,
    Erase,
};

class THostStat
{
public:
    // Called right before a request is sent to the host for the given
    // operation. Increments the per-operation inflight counter.
    void OnRequest(EOperation operation);

    // OnSuccess/OnError are called when a previously sent request completes.
    // In addition to tracking the success/error state of the host, they
    // decrement the per-operation inflight counter that was incremented by
    // OnRequest.
    void OnSuccess(TInstant now, TDuration executionTime, EOperation operation);
    void OnError(TInstant now, EOperation operation);

    [[nodiscard]] TDuration ErrorsDuration(
        TInstant now,
        size_t* errorCount) const;

    // Number of currently inflight requests of a given operation type for
    // this host (i.e. OnRequest calls without a matching OnSuccess/OnError).
    [[nodiscard]] size_t InflightCount(EOperation operation) const;

private:
    TInstant LastSuccess;
    TInstant LastError;
    size_t ErrorCount = 0;

    THashMap<EOperation, size_t> InflightByOperation;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
