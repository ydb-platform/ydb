
#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

enum class EVChunkOperation
{
    Read,
    Write,
    Flush,
    Erase,

    MAX
};

////////////////////////////////////////////////////////////////////////////////

class TVChunkRequestCounters
{
private:
    NMonitoring::TDynamicCounters::TCounterPtr ReplyOk;
    NMonitoring::TDynamicCounters::TCounterPtr ReplyErr;
    NMonitoring::TDynamicCounters::TCounterPtr Pending;
    NMonitoring::TDynamicCounters::TCounterPtr MinLsn;

public:
    explicit TVChunkRequestCounters(NMonitoring::TDynamicCounterPtr parent);

    void RequestFinished(bool ok);
    void UpdatePending(ui64 count);
    void UpdateMinLsn(ui64 lsn);
};

////////////////////////////////////////////////////////////////////////////////

class TVChunkCounters
{
private:
    TVChunkRequestCounters Read;
    TVChunkRequestCounters Write;
    TVChunkRequestCounters Flush;
    TVChunkRequestCounters Erase;

public:
    explicit TVChunkCounters(NMonitoring::TDynamicCounterPtr parent);

    void RequestFinished(EVChunkOperation operation, bool ok);
    void UpdatePending(EVChunkOperation operation, ui64 count);
    void UpdateMinLsn(EVChunkOperation operation, ui64 lsn);

private:
    TVChunkRequestCounters& Get(EVChunkOperation operation);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
