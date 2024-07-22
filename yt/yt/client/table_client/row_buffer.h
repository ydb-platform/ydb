#pragma once

#include "public.h"
#include "unversioned_row.h"
#include "versioned_row.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TDefaultRowBufferPoolTag { };

//! Holds data for a bunch of rows.
/*!
 *  Acts as a ref-counted wrapped around TChunkedMemoryPool plus a bunch
 *  of helpers.
 */
class TRowBuffer
    : public TRefCounted
{
public:
    TRowBuffer(
        TRefCountedTypeCookie tagCookie,
        IMemoryChunkProviderPtr chunkProvider,
        size_t startChunkSize = TChunkedMemoryPool::DefaultStartChunkSize,
        IMemoryUsageTrackerPtr tracker = nullptr);

    template <class TTag = TDefaultRowBufferPoolTag>
    explicit TRowBuffer(
        TTag /*tag*/ = TDefaultRowBufferPoolTag(),
        size_t startChunkSize = TChunkedMemoryPool::DefaultStartChunkSize,
        IMemoryUsageTrackerPtr tracker = nullptr)
        : MemoryTracker_(std::move(tracker))
        , Pool_(
            TTag(),
            startChunkSize)
    { }

    template <class TTag>
    TRowBuffer(
        TTag /*tag*/,
        IMemoryChunkProviderPtr chunkProvider,
        IMemoryUsageTrackerPtr tracker = nullptr)
        : MemoryTracker_(std::move(tracker))
        , Pool_(
            GetRefCountedTypeCookie<TTag>(),
            std::move(chunkProvider))
    { }

    TChunkedMemoryPool* GetPool();

    TMutableUnversionedRow AllocateUnversioned(int valueCount);
    TMutableVersionedRow AllocateVersioned(
        int keyCount,
        int valueCount,
        int writeTimestampCount,
        int deleteTimestampCount);

    void CaptureValue(TUnversionedValue* value);
    TVersionedValue CaptureValue(const TVersionedValue& value);
    TUnversionedValue CaptureValue(const TUnversionedValue& value);

    TMutableUnversionedRow CaptureRow(TUnversionedRow row, bool captureValues = true);
    void CaptureValues(TMutableUnversionedRow row);
    TMutableUnversionedRow CaptureRow(TUnversionedValueRange values, bool captureValues = true);
    std::vector<TMutableUnversionedRow> CaptureRows(TRange<TUnversionedRow> rows, bool captureValues = true);

    TMutableVersionedRow CaptureRow(TVersionedRow row, bool captureValues = true);
    void CaptureValues(TMutableVersionedRow row);

    //! Captures the row applying #idMapping to value ids and placing values to the proper positions.
    //! Skips values that map to negative ids via #idMapping.
    //! The resulting row consists of a schemaful prefix consisting of values whose ids
    //! (after remapping) are less than #schemafulColumnCount
    //! and an arbitrarily-ordered suffix of values whose ids
    //! (after remapping) are greater than or equal to #schemafulColumnCount.
    TMutableUnversionedRow CaptureAndPermuteRow(
        TUnversionedRow row,
        const TTableSchema& tableSchema,
        int schemafulColumnCount,
        const TNameTableToSchemaIdMapping& idMapping,
        bool validateDuplicateAndRequiredValueColumns,
        bool preserveIds = false,
        std::optional<TUnversionedValue> addend = {});

    //! Captures the row applying #idMapping to value ids.
    //! #idMapping must be identity for key columns.
    //! Skips values that map to negative ids via #idMapping.
    TMutableVersionedRow CaptureAndPermuteRow(
        TVersionedRow row,
        const TTableSchema& tableSchema,
        const TNameTableToSchemaIdMapping& idMapping,
        bool validateDuplicateAndRequiredValueColumns,
        bool allowMissingKeyColumns = false);

    i64 GetSize() const;
    i64 GetCapacity() const;

    //! Moves all the rows from other row buffer to the current one.
    //! The other buffer becomes empty, like after Purge() call.
    void Absorb(TRowBuffer&& other);

    void Clear();
    void Purge();

private:
    const IMemoryUsageTrackerPtr MemoryTracker_;

    TChunkedMemoryPool Pool_;
    std::optional<TMemoryUsageTrackerGuard> MemoryGuard_;

    void ValidateNoOverflow();
};

DEFINE_REFCOUNTED_TYPE(TRowBuffer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
