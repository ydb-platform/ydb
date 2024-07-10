#include "job_counters.h"

namespace NYT {

////////////////////////////////////////////////////////////////////

namespace {
    ui64 CountTotal(const TNode& data)
    {
        if (data.IsMap()) {
            if (auto totalPtr = data.AsMap().FindPtr("total")) {
                return data["total"].IntCast<ui64>();
            } else {
                ui64 total = 0;
                for (const auto& keyVal: data.AsMap()) {
                    total += CountTotal(keyVal.second);
                }
                return total;
            }
        } else {
            return data.IntCast<ui64>();
        }
    }

    TNode GetNode(const TNode& data, const TStringBuf& key)
    {
        if (auto resPtr = data.AsMap().FindPtr(key)) {
            return *resPtr;
        }
        return TNode();
    }
} // namespace

////////////////////////////////////////////////////////////////////

TJobCounter::TJobCounter(TNode data)
    : Data_(std::move(data))
{
    if (Data_.HasValue()) {
        Total_ = CountTotal(Data_);
    }
}

TJobCounter::TJobCounter(ui64 total)
    : Total_(total)
{ }

ui64 TJobCounter::GetTotal() const
{
    return Total_;
}

ui64 TJobCounter::GetValue(const TStringBuf key) const
{
    if (Data_.HasValue()) {
        return CountTotal(Data_[key]);
    }
    return 0;
}

////////////////////////////////////////////////////////////////////

TJobCounters::TJobCounters(const TNode& counters)
    : Total_(0)
{
    if (!counters.IsMap()) {
        ythrow yexception() << "TJobCounters must be initialized with Map type TNode";
    }
    auto abortedNode = GetNode(counters, "aborted");
    if (abortedNode.HasValue()) {
        Aborted_ = TJobCounter(GetNode(abortedNode, "total"));
        AbortedScheduled_ = TJobCounter(GetNode(abortedNode, "scheduled"));
        AbortedNonScheduled_ = TJobCounter(GetNode(abortedNode, "non_scheduled"));
    }
    auto completedNode = GetNode(counters, "completed");
    if (completedNode.HasValue()) {
        Completed_ = TJobCounter(GetNode(completedNode, "total"));
        CompletedNonInterrupted_ = TJobCounter(GetNode(completedNode, "non-interrupted"));
        CompletedInterrupted_ = TJobCounter(GetNode(completedNode, "interrupted"));
    }
    Lost_ = TJobCounter(GetNode(counters, "lost"));
    Invalidated_ = TJobCounter(GetNode(counters, "invalidated"));
    Failed_ = TJobCounter(GetNode(counters, "failed"));
    Running_ = TJobCounter(GetNode(counters, "running"));
    Suspended_ = TJobCounter(GetNode(counters, "suspended"));
    Pending_ = TJobCounter(GetNode(counters, "pending"));
    Blocked_ = TJobCounter(GetNode(counters, "blocked"));
    Total_ = CountTotal(counters);
}


const TJobCounter& TJobCounters::GetAborted() const
{
    return Aborted_;
}

const TJobCounter& TJobCounters::GetAbortedScheduled() const
{
    return AbortedScheduled_;
}

const TJobCounter& TJobCounters::GetAbortedNonScheduled() const
{
    return AbortedNonScheduled_;
}

const TJobCounter& TJobCounters::GetCompleted() const
{
    return Completed_;
}

const TJobCounter& TJobCounters::GetCompletedNonInterrupted() const
{
    return CompletedNonInterrupted_;
}

const TJobCounter& TJobCounters::GetCompletedInterrupted() const
{
    return CompletedInterrupted_;
}

const TJobCounter& TJobCounters::GetLost() const
{
    return Lost_;
}

const TJobCounter& TJobCounters::GetInvalidated() const
{
    return Invalidated_;
}

const TJobCounter& TJobCounters::GetFailed() const
{
    return Failed_;
}

const TJobCounter& TJobCounters::GetRunning() const
{
    return Running_;
}

const TJobCounter& TJobCounters::GetSuspended() const
{
    return Suspended_;
}

const TJobCounter& TJobCounters::GetPending() const
{
    return Pending_;
}

const TJobCounter& TJobCounters::GetBlocked() const
{
    return Blocked_;
}

ui64 TJobCounters::GetTotal() const
{
    return Total_;
}

////////////////////////////////////////////////////////////////////

} // namespace NYT
