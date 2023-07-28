#pragma once

#include <util/system/types.h>

#include <array>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TUndumpableMark;

//! Adds byte range to undumpable set.
TUndumpableMark* MarkUndumpable(void* ptr, size_t size);

//! Removes byte range from undumpable set.
void UnmarkUndumpable(TUndumpableMark* mark);

//! Add byte range to undumpable set.
/**
 *  Unlike MarkUndumpable, this method does not require user to keep pointer to TUndumpableMark.
 */
void MarkUndumpableOob(void* ptr, size_t size);

//! Remove byte range from undumpable set.
void UnmarkUndumpableOob(void* ptr);

//! Returns the total size of undumpable set.
size_t GetUndumpableMemorySize();

//! Returns the estimate of memory consumed by internal data structures.
size_t GetUndumpableMemoryFootprint();

struct TCutBlocksInfo
{
    struct TFailedInfo
    {
        int ErrorCode = 0;
        size_t Size = 0;
    };

    size_t MarkedSize = 0;

    static constexpr int MaxFailedRecordsCount = 8;
    std::array<TFailedInfo, MaxFailedRecordsCount> FailedToMarkMemory{};
};

//! CutUndumpableFromCoredump call's madvice(MADV_DONTNEED) for all current undumpable objects.
/**
 *  This function is async-signal safe. Usually, this function should be called from segfault handler.
 */
TCutBlocksInfo CutUndumpableRegionsFromCoredump();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
