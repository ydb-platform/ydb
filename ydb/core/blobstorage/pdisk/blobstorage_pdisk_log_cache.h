#pragma once
#include "defs.h"
#include <library/cpp/actors/util/rc_buf.h>
#include <util/generic/intrlist.h>
#include <util/generic/hash.h>

namespace NKikimr {
namespace NPDisk {

/**
 * Key-value cache without automatic eviction, but able to erase range of keys.
 * Entries do not intersect with each other.
 */
class TLogCache {
private:
    struct TCacheRecord {
        ui64 Offset = 0;
        TRcBuf Data;
        TVector<ui64> BadOffsets;

        TCacheRecord() = default;
        TCacheRecord(TCacheRecord&&);
        TCacheRecord(ui64 offset, TRcBuf data, TVector<ui64> badOffsets);
    };

    using TIndex = TMap<ui64, TCacheRecord>;

public:
    using TBadOffsetsHandler = std::function<void(const std::vector<ui64>&)>;

    /**
     * Gets the current size of the cache.
     */
    size_t Size() const;

    /**
     * Finds a cache record by its offset and a specified size, copies the data to the buffer.
     * @param offset The offset key to search for.
     * @param size The size of data to copy.
     * @param buffer The buffer to store the copied data.
     * @param func Optional custom function to handle bad offsets.
     * @return True if the cache record is found and data is copied; otherwise, false.
     */
    bool Find(ui64 offset, ui32 size, char* buffer, TBadOffsetsHandler func = [](const std::vector<ui64>&) {});

    /**
     * Inserts a new cache record into the cache.
     * @param dataPtr Pointer to the data to be inserted.
     * @param offset The offset key for the new cache record.
     * @param size The size of the data.
     * @param badOffsets Optional vector of bad offsets associated with the cache record.
     * @return True if the insertion was successful; otherwise, false (e.g., due to data being already cached).
     */
    bool Insert(const char* dataPtr, ui64 offset, ui32 size, const TVector<ui64>& badOffsets = {});

    /**
     * Erases a range of cache records from the cache.
     * @param begin The beginning of the range (inclusive).
     * @param end The end of the range (exclusive).
     * @return The number of cache records erased.
     */
    size_t EraseRange(ui64 begin, ui64 end);

    /**
     * Clears the entire cache, removing all cache records.
     */
    void Clear();

private:
    TIndex Index;

    /**
     * Prepares for insertion of a new cache record and calculates the left and right paddings for the data being inserted if parts of the data
     * is already in the cache.
     * @param offset The offset key for the new cache record.
     * @param size The size of the data.
     * @return A pair of i64 values representing left and right data paddings.
     */
    std::pair<i64, i64> PrepareInsertion(ui64 offset, ui32 size);
};

} // NPDisk
} // NKikimr
