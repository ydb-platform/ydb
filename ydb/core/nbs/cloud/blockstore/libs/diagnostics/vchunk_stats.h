
#pragma once

#include <util/system/types.h>

#include <array>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Kind of a vchunk-local request tracked by TVChunkStats.
enum class EVChunkOperation
{
    // User read from this vchunk.
    Read,
    // User write to this vchunk.
    Write,
    // Flush of dirty blocks from PBuffer to DDisk.
    Flush,
    // Erase of PBuffer records that have been flushed.
    Erase,
    // Erase of PBuffer records superseded by a later write.
    EraseBelated,

    MAX
};

constexpr size_t VChunkOperationCount =
    static_cast<size_t>(EVChunkOperation::MAX);

////////////////////////////////////////////////////////////////////////////////

// Counters of one EVChunkOperation. ReplyOk/ReplyErr are cumulative; Pending
// and MinLsn are instantaneous (MinLsn is 0 when nothing is pending).
struct TVChunkOperationStats
{
    ui64 ReplyOk = 0;
    ui64 ReplyErr = 0;
    ui64 Pending = 0;
    ui64 MinLsn = 0;

    // True when every field is zero.
    [[nodiscard]] bool IsZero() const;
};

////////////////////////////////////////////////////////////////////////////////

// Per-vchunk request statistics. Not thread safe by design: every instance is
// written only from the executor thread of its vchunk, so the datapath pays
// nothing for it. Readers must hop onto that executor.
class TVChunkStats final
{
public:
    // Increments ReplyOk or ReplyErr of the given operation.
    void RequestFinished(EVChunkOperation operation, bool ok);

    // Overwrites Pending of the given operation.
    void UpdatePending(EVChunkOperation operation, ui64 count);

    // Overwrites MinLsn of the given operation.
    void UpdateMinLsn(EVChunkOperation operation, ui64 lsn);

    // Folds other into this: ReplyOk/ReplyErr/Pending are summed, MinLsn is
    // the minimum of the non-zero values (0 means "nothing pending").
    void Accumulate(const TVChunkStats& other);

    // Stats of one operation.
    [[nodiscard]] const TVChunkOperationStats& Get(
        EVChunkOperation operation) const;

    // True when every operation is zero.
    [[nodiscard]] bool IsZero() const;

private:
    [[nodiscard]] TVChunkOperationStats& Mutable(EVChunkOperation operation);

    std::array<TVChunkOperationStats, VChunkOperationCount> Operations{};
};

////////////////////////////////////////////////////////////////////////////////

// Stable label of the operation (matches the Solomon "operation" subgroup).
[[nodiscard]] const char* VChunkOperationName(EVChunkOperation operation);

}   // namespace NYdb::NBS::NBlockStore
