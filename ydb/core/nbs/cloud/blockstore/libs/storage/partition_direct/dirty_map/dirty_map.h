#pragma once

#include "location.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>

#include <util/datetime/base.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TReadHint
{
    TLocationMask LocationMask;
    ui64 Lsn = 0;
    TBlockRange64 Range;
};

struct TPBufferSegment
{
    ui64 Lsn = 0;
    TBlockRange64 Range;
};

struct TFlushHint
{
    TVector<TPBufferSegment> Segments;
};

struct TEraseHint
{
    TVector<TPBufferSegment> Segments;
};

struct TInflightInfo
{
    enum class EState
    {
        // Data written to PBuffers with quorum.
        // Read from any confirmed PBuffer.
        PBufferWritten,

        // Started flushing from PBuffers to DDisk.
        // Read from any confirmed PBuffer.
        PBufferFlushing,

        // Data flushed to DDisk.
        // Read from DDisk.
        PBufferFlushed,

        // The data is now being erasing from the PBuffers.
        // Read from DDisk.
        PBufferErasing,

        // The data is erased from the PBuffers.
        // Read from DDisk.
        PBufferErased,
    };

    TInflightInfo(TLocationMask writeRequested, TLocationMask writeConfirmed);

    TLocationMask WriteRequested;
    TLocationMask WriteConfirmed;
    TLocationMask FlushRequested;
    TLocationMask FlushConfirmed;
    TLocationMask EraseRequested;
    TLocationMask EraseConfirmed;

    TInstant StartAt;

    [[nodiscard]] EState GetState() const;
    [[nodiscard]] std::optional<TLocationMask> ReadMask() const;
};

class TBlocksDirtyMap
{
public:
    [[nodiscard]] TVector<TReadHint> MakeReadHint(TBlockRange64 range);
    [[nodiscard]] TMap<ELocation, TFlushHint> MakeFlushHint(size_t batchSize);
    [[nodiscard]] TMap<ELocation, TEraseHint> MakeEraseHint(size_t batchSize);

    void WriteFinished(
        ui64 lsn,
        TBlockRange64 range,
        TLocationMask requested,
        TLocationMask completed);
    void FlushFinished(
        ELocation location,
        const TVector<ui64>& flushOk,
        const TVector<ui64>& flushFailed);
    void EraseFinished(
        ELocation location,
        const TVector<ui64>& eraseOk,
        const TVector<ui64>& eraseFailed);

private:
    using TInflightMap = TBlockRangeMap<ui64, TInflightInfo>;

    TInflightMap Inflight;
    TSet<ui64> ReadyToFlush;
    TSet<ui64> ReadyToErase;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
