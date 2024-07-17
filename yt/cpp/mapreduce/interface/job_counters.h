#pragma once

#include "fwd.h"

#include <yt/cpp/mapreduce/interface/node.h>

namespace NYT {

class TJobCounter
{
private:
    TNode Data_;
    ui64 Total_ = 0;

public:
    TJobCounter() = default;

    TJobCounter(TNode data);
    TJobCounter(ui64 total);

    ui64 GetTotal() const;

    ui64 GetValue(const TStringBuf key) const;
};

/// Class representing a collection of job counters.
class TJobCounters
{
public:
    ///
    /// Construct empty counter.
    TJobCounters() = default;

    ///
    /// Construct counter from counters node.
    TJobCounters(const TNode& counters);

    const TJobCounter& GetAborted() const;
    const TJobCounter& GetAbortedScheduled() const;
    const TJobCounter& GetAbortedNonScheduled() const;
    const TJobCounter& GetCompleted() const;
    const TJobCounter& GetCompletedNonInterrupted() const;
    const TJobCounter& GetCompletedInterrupted() const;
    const TJobCounter& GetLost() const;
    const TJobCounter& GetInvalidated() const;
    const TJobCounter& GetFailed() const;
    const TJobCounter& GetRunning() const;
    const TJobCounter& GetSuspended() const;
    const TJobCounter& GetPending() const;
    const TJobCounter& GetBlocked() const;

    ui64 GetTotal() const;

private:
    ui64 Total_ = 0;

    TJobCounter Aborted_;
    TJobCounter AbortedScheduled_;
    TJobCounter AbortedNonScheduled_;
    TJobCounter Completed_;
    TJobCounter CompletedNonInterrupted_;
    TJobCounter CompletedInterrupted_;
    TJobCounter Lost_;
    TJobCounter Invalidated_;
    TJobCounter Failed_;
    TJobCounter Running_;
    TJobCounter Suspended_;
    TJobCounter Pending_;
    TJobCounter Blocked_;
};

////////////////////////////////////////////////////////////////////

} // namespace NYT
