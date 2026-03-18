#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TInflightInfo::TInflightInfo(
    TLocationMask writeRequested,
    TLocationMask writeConfirmed)
    : WriteRequested(writeRequested)
    , WriteConfirmed(writeConfirmed)
{
    Y_ABORT_UNLESS(WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount);
}

TInflightInfo::EState TInflightInfo::GetState() const
{
    if (!EraseRequested.Empty()) {
        return (EraseRequested == EraseConfirmed) ? EState::PBufferErased
                                                  : EState::PBufferErasing;
    }

    if (!FlushRequested.Empty()) {
        return (FlushRequested == FlushConfirmed) ? EState::PBufferFlushed
                                                  : EState::PBufferFlushing;
    }

    return EState::PBufferWritten;
}

std::optional<TLocationMask> TInflightInfo::ReadMask() const
{
    switch (GetState()) {
        case EState::PBufferWritten:
        case EState::PBufferFlushing:
        case EState::PBufferFlushed:
            return WriteConfirmed;

        case EState::PBufferErasing:
        case EState::PBufferErased:
            return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

TVector<TReadHint> TBlocksDirtyMap::MakeReadHint(TBlockRange64 range)
{
    auto makeDefaultHint = [](TBlockRange64 range)
    {
        return TReadHint{
            .LocationMask =
                TLocationMask::MakeDDisk(true, true, true, false, false),
            .Lsn = 0,
            .Range = range,
        };
    };

    if (!Inflight.HasOverlaps(range)) {
        return {makeDefaultHint(range)};
    }

    TMap<ui64, TReadHint> readHints;
    Inflight.EnumerateOverlapping(
        range,
        [&](TInflightMap::TFindItem& item)
        {
            const ui64 lsn = item.Key;
            const TInflightInfo& inflight = item.Value;
            if (auto location = inflight.ReadMask()) {
                readHints[lsn] = TReadHint{
                    .LocationMask = *location,
                    .Lsn = lsn,
                    .Range = item.Range,
                };
            }
            return TInflightMap::EEnumerateContinuation::Continue;
        });

    if (!readHints) {
        return {makeDefaultHint(range)};
    }

    // Take hint with biggest lsn.
    return {readHints.rbegin()->second};
}

TMap<ELocation, TFlushHint> TBlocksDirtyMap::MakeFlushHint(size_t batchSize)
{
    TMap<ELocation, TFlushHint> result;

    if (ReadyToFlush.size() < batchSize) {
        return result;
    }

    for (ui64 lsn: ReadyToFlush) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& val = item->Value;
        for (auto l: AllLocations) {
            if (val.WriteRequested.Get(l) && !val.FlushRequested.Get(l)) {
                result[l].Segments.push_back(
                    TPBufferSegment{.Lsn = item->Key, .Range = item->Range});
                val.FlushRequested.Set(l);
            }
        }
    }
    ReadyToFlush.clear();

    return result;
}

TMap<ELocation, TEraseHint> TBlocksDirtyMap::MakeEraseHint(size_t batchSize)
{
    TMap<ELocation, TEraseHint> result;

    if (ReadyToErase.size() < batchSize) {
        return result;
    }

    for (ui64 lsn: ReadyToErase) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& val = item->Value;
        for (auto l: AllLocations) {
            if (val.WriteRequested.Get(l) && !val.EraseRequested.Get(l)) {
                result[l].Segments.push_back(
                    TPBufferSegment{.Lsn = item->Key, .Range = item->Range});
                val.EraseRequested.Set(l);
            }
        }
    }
    ReadyToErase.clear();

    return result;
}

void TBlocksDirtyMap::WriteFinished(
    ui64 lsn,
    TBlockRange64 range,
    TLocationMask requested,
    TLocationMask written)
{
    const bool inserted =
        Inflight.AddRange(lsn, range, TInflightInfo(requested, written));
    Y_ABORT_UNLESS(inserted);

    ReadyToFlush.insert(lsn);
}

void TBlocksDirtyMap::FlushFinished(
    ELocation location,
    const TVector<ui64>& flushOk,
    const TVector<ui64>& flushFailed)
{
    for (ui64 lsn: flushOk) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;
        inflight.FlushConfirmed.Set(location);
        if (inflight.FlushConfirmed == inflight.FlushRequested) {
            ReadyToErase.insert(lsn);
        }
    }
    for (ui64 lsn: flushFailed) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;
        inflight.FlushRequested.Reset(location);
        ReadyToFlush.insert(lsn);
    }
}

void TBlocksDirtyMap::EraseFinished(
    ELocation location,
    const TVector<ui64>& eraseOk,
    const TVector<ui64>& eraseFailed)
{
    for (ui64 lsn: eraseOk) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;
        inflight.EraseConfirmed.Set(location);
        if (inflight.EraseConfirmed == inflight.EraseRequested) {
            const bool removed = Inflight.RemoveRange(item->Key);
            Y_ABORT_UNLESS(removed);
        }
    }
    for (ui64 lsn: eraseFailed) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;
        inflight.EraseRequested.Reset(location);
        ReadyToErase.insert(lsn);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
