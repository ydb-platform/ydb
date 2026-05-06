#include "vchunk_counters.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TVChunkRequestCounters::TVChunkRequestCounters(
    NMonitoring::TDynamicCounterPtr parent)
    : ReplyOk(parent ? parent->GetCounter("ReplyOk", true) : nullptr)
    , ReplyErr(parent ? parent->GetCounter("ReplyErr", true) : nullptr)
    , Pending(parent ? parent->GetCounter("Pending") : nullptr)
    , MinLsn(parent ? parent->GetCounter("MinLsn") : nullptr)
{}

void TVChunkRequestCounters::RequestFinished(bool ok)
{
    if (ok && ReplyOk) {
        ++*ReplyOk;
    } else if (!ok && ReplyErr) {
        ++*ReplyErr;
    }
}

void TVChunkRequestCounters::UpdatePending(ui64 count)
{
    if (Pending) {
        *Pending = count;
    }
}

void TVChunkRequestCounters::UpdateMinLsn(ui64 lsn)
{
    if (MinLsn) {
        *MinLsn = lsn;
    }
}

////////////////////////////////////////////////////////////////////////////////

TVChunkCounters::TVChunkCounters(NMonitoring::TDynamicCounterPtr parent)
    : Read(parent ? parent->GetSubgroup("operation", "Read") : nullptr)
    , Write(parent ? parent->GetSubgroup("operation", "Write") : nullptr)
    , Flush(parent ? parent->GetSubgroup("operation", "Flush") : nullptr)
    , Erase(parent ? parent->GetSubgroup("operation", "Erase") : nullptr)
{}

void TVChunkCounters::RequestFinished(EVChunkOperation operation, bool ok)
{
    Get(operation).RequestFinished(ok);
}

void TVChunkCounters::UpdatePending(EVChunkOperation operation, ui64 count)
{
    Get(operation).UpdatePending(count);
}

void TVChunkCounters::UpdateMinLsn(EVChunkOperation operation, ui64 lsn)
{
    Get(operation).UpdateMinLsn(lsn);
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

        case EVChunkOperation::MAX:
            Y_ABORT("Invalid operation");
    }
    return Read;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
