
#pragma once

#include "vchunk_stats.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Solomon sensors of one EVChunkOperation at disk level. ReplyOk/ReplyErr are
// derivative; Pending and MinLsn are absolute.
class TVChunkRequestCounters
{
private:
    NMonitoring::TDynamicCounters::TCounterPtr ReplyOk;
    NMonitoring::TDynamicCounters::TCounterPtr ReplyErr;
    NMonitoring::TDynamicCounters::TCounterPtr Pending;
    NMonitoring::TDynamicCounters::TCounterPtr MinLsn;
    TVChunkOperationStats LastPublished;

public:
    // Binds to the operation subgroup (or no-ops when parent is null).
    explicit TVChunkRequestCounters(NMonitoring::TDynamicCounterPtr parent);

    // Writes stats into the sensors. ReplyOk/ReplyErr receive the delta
    // against LastPublished; a decrease is skipped (a vchunk dropped out).
    void Publish(const TVChunkOperationStats& stats);
};

////////////////////////////////////////////////////////////////////////////////

// Disk-level vchunk Solomon counters: one operation subgroup, no per-vchunk
// labels. Publish is called from the periodic gather, not from the datapath.
class TVChunkCounters
{
private:
    TVChunkRequestCounters Read;
    TVChunkRequestCounters Write;
    TVChunkRequestCounters Flush;
    TVChunkRequestCounters Erase;
    TVChunkRequestCounters EraseBelated;

public:
    // Binds to operation=Read|Write|Flush|Erase|EraseBelated subgroups.
    explicit TVChunkCounters(NMonitoring::TDynamicCounterPtr parent);

    // Publishes the disk-wide aggregate.
    void Publish(const TVChunkStats& total);

private:
    TVChunkRequestCounters& Get(EVChunkOperation operation);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
