#include "vchunk_counters.h"

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

void PublishDerivative(
    const NMonitoring::TDynamicCounters::TCounterPtr& counter,
    ui64& last,
    ui64 current)
{
    if (current >= last && counter) {
        *counter += current - last;
    }
    last = current;
}

void PublishAbsolute(
    const NMonitoring::TDynamicCounters::TCounterPtr& counter,
    ui64 value)
{
    if (counter) {
        *counter = value;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVChunkRequestCounters::TVChunkRequestCounters(
    NMonitoring::TDynamicCounterPtr parent)
    : ReplyOk(parent ? parent->GetCounter("ReplyOk", true) : nullptr)
    , ReplyErr(parent ? parent->GetCounter("ReplyErr", true) : nullptr)
    , Pending(parent ? parent->GetCounter("Pending") : nullptr)
    , MinLsn(parent ? parent->GetCounter("MinLsn") : nullptr)
{}

void TVChunkRequestCounters::Publish(const TVChunkOperationStats& stats)
{
    PublishDerivative(ReplyOk, LastPublished.ReplyOk, stats.ReplyOk);
    PublishDerivative(ReplyErr, LastPublished.ReplyErr, stats.ReplyErr);
    PublishAbsolute(Pending, stats.Pending);
    PublishAbsolute(MinLsn, stats.MinLsn);
}

////////////////////////////////////////////////////////////////////////////////

TVChunkCounters::TVChunkCounters(NMonitoring::TDynamicCounterPtr parent)
    : Read(parent ? parent->GetSubgroup("operation", "Read") : nullptr)
    , Write(parent ? parent->GetSubgroup("operation", "Write") : nullptr)
    , Flush(parent ? parent->GetSubgroup("operation", "Flush") : nullptr)
    , Erase(parent ? parent->GetSubgroup("operation", "Erase") : nullptr)
    , EraseBelated(
          parent ? parent->GetSubgroup("operation", "EraseBelated") : nullptr)
{}

void TVChunkCounters::Publish(const TVChunkStats& total)
{
    Get(EVChunkOperation::Read).Publish(total.Get(EVChunkOperation::Read));
    Get(EVChunkOperation::Write).Publish(total.Get(EVChunkOperation::Write));
    Get(EVChunkOperation::Flush).Publish(total.Get(EVChunkOperation::Flush));
    Get(EVChunkOperation::Erase).Publish(total.Get(EVChunkOperation::Erase));
    Get(EVChunkOperation::EraseBelated)
        .Publish(total.Get(EVChunkOperation::EraseBelated));
}

TVChunkRequestCounters& TVChunkCounters::Get(EVChunkOperation operation)
{
    switch (operation) {
        case EVChunkOperation::Read:
            return Read;
        case EVChunkOperation::Write:
            return Write;
        case EVChunkOperation::Flush:
            return Flush;
        case EVChunkOperation::Erase:
            return Erase;
        case EVChunkOperation::EraseBelated:
            return EraseBelated;

        case EVChunkOperation::MAX:
            Y_ABORT("Invalid operation");
    }
    return Read;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
