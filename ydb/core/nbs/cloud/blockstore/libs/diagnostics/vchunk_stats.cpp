#include "vchunk_stats.h"

#include <util/generic/yexception.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

size_t OperationIndex(EVChunkOperation operation)
{
    const auto index = static_cast<size_t>(operation);
    Y_ABORT_UNLESS(index < VChunkOperationCount);
    return index;
}

void AccumulateMinLsn(ui64& dst, ui64 src)
{
    if (src == 0) {
        return;
    }
    if (dst == 0 || src < dst) {
        dst = src;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TVChunkOperationStats::IsZero() const
{
    return ReplyOk == 0 && ReplyErr == 0 && Pending == 0 && MinLsn == 0;
}

////////////////////////////////////////////////////////////////////////////////

void TVChunkStats::RequestFinished(EVChunkOperation operation, bool ok)
{
    auto& stats = Mutable(operation);
    if (ok) {
        ++stats.ReplyOk;
    } else {
        ++stats.ReplyErr;
    }
}

void TVChunkStats::UpdatePending(EVChunkOperation operation, ui64 count)
{
    Mutable(operation).Pending = count;
}

void TVChunkStats::UpdateMinLsn(EVChunkOperation operation, ui64 lsn)
{
    Mutable(operation).MinLsn = lsn;
}

void TVChunkStats::Accumulate(const TVChunkStats& other)
{
    for (size_t i = 0; i < VChunkOperationCount; ++i) {
        Operations[i].ReplyOk += other.Operations[i].ReplyOk;
        Operations[i].ReplyErr += other.Operations[i].ReplyErr;
        Operations[i].Pending += other.Operations[i].Pending;
        AccumulateMinLsn(Operations[i].MinLsn, other.Operations[i].MinLsn);
    }
}

const TVChunkOperationStats& TVChunkStats::Get(EVChunkOperation operation) const
{
    return Operations[OperationIndex(operation)];
}

bool TVChunkStats::IsZero() const
{
    for (const auto& stats: Operations) {
        if (!stats.IsZero()) {
            return false;
        }
    }
    return true;
}

TVChunkOperationStats& TVChunkStats::Mutable(EVChunkOperation operation)
{
    return Operations[OperationIndex(operation)];
}

////////////////////////////////////////////////////////////////////////////////

const char* VChunkOperationName(EVChunkOperation operation)
{
    switch (operation) {
        case EVChunkOperation::Read:
            return "Read";
        case EVChunkOperation::Write:
            return "Write";
        case EVChunkOperation::Flush:
            return "Flush";
        case EVChunkOperation::Erase:
            return "Erase";
        case EVChunkOperation::EraseBelated:
            return "EraseBelated";
        case EVChunkOperation::MAX:
            Y_ABORT("Invalid operation");
    }
    return "Read";
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
