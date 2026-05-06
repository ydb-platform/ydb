#pragma once

#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class EOperation
{
    ReadFromPBuffer,
    ReadFromDDisk,
    WriteToPBuffer,
    WriteToManyPBuffers,
    WriteToDDisk,
    Flush,
    FlushCrossNode,
    Erase,

    // Must remain the last entry. Used to size per-operation containers.
    Count_,
};

inline constexpr size_t OperationCount =
    static_cast<size_t>(EOperation::Count_);

class THostStat
{
public:
    struct TErrorsInfo
    {
        TDuration FromFirstError;
        TDuration FromLastError;
        size_t ErrorCount = 0;
    };

    // Called right before a request is sent to the host for the given
    // operation. Increments the per-operation inflight counter.
    void OnRequest(EOperation operation);

    // OnSuccess/OnError are called when a previously sent request completes.
    // In addition to tracking the success/error state of the host, they
    // decrement the per-operation inflight counter that was incremented by
    // OnRequest.
    void OnSuccess(TInstant now, TDuration executionTime, EOperation operation);
    void OnError(TInstant now, EOperation operation);

    // Returns how much time has passed since the first error was received and
    // the number and total size of errors.
    [[nodiscard]] TErrorsInfo GetErrorsInfo(TInstant now) const;

    // Number of currently inflight requests of a given operation type for
    // this host (i.e. OnRequest calls without a matching OnSuccess/OnError).
    [[nodiscard]] size_t InflightCount(EOperation operation) const;

private:
    size_t& AccessInflightCount(EOperation operation);

    TInstant LastSuccessAt;
    TInstant FirstErrorAt;
    TInstant LastErrorAt;
    size_t ErrorCount = 0;

    TVector<size_t> InflightByOperation = TVector<size_t>(OperationCount, 0);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
