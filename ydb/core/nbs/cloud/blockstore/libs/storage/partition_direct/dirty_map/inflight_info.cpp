#include "inflight_info.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueues,
    ui64 lsn,
    size_t byteCount,
    ELocation location)
    : State(EState::PBufferIncompleteWrite)
    , ReadyQueue(readyQueues)
    , Lsn(lsn)
    , ByteCount(byteCount)
{
    WriteRequested.Set(location);
    WriteConfirmed.Set(location);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Clone);
    ApplyBytes(location, IReadyQueue::EPBufferCounter::Total, true);
}

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueue,
    ui64 lsn,
    size_t byteCount,
    TLocationMask writeRequested,
    TLocationMask writeConfirmed)
    : State(EState::PBufferWritten)
    , ReadyQueue(readyQueue)
    , Lsn(lsn)
    , ByteCount(byteCount)
    , WriteRequested(writeRequested)
    , WriteConfirmed(writeConfirmed)
{
    Y_ABORT_UNLESS(WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount);

    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
    ApplyBytes(WriteRequested, IReadyQueue::EPBufferCounter::Total, true);
}

TInflightInfo::TInflightInfo(TInflightInfo&& other) noexcept
    : State(other.State)
    , ReadyQueue(other.ReadyQueue)
    , Lsn(other.Lsn)
    , ByteCount(other.ByteCount)
    , WriteRequested(other.WriteRequested)
    , WriteConfirmed(other.WriteConfirmed)
    , FlushRequested(other.FlushRequested)
    , FlushConfirmed(other.FlushConfirmed)
{
    other.ReadyQueue = nullptr;
}

TInflightInfo::~TInflightInfo()
{
    Y_ABORT_UNLESS(PBuffersLockCount == 0);

    ApplyBytes(WriteRequested, IReadyQueue::EPBufferCounter::Total, false);
}

void TInflightInfo::Detach()
{
    ReadyQueue = nullptr;
}

void TInflightInfo::RestorePBuffer(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(
        State == EState::PBufferIncompleteWrite ||
        State == EState::PBufferWritten);
    Y_ABORT_UNLESS(!WriteRequested.Get(location));
    Y_ABORT_UNLESS(!WriteConfirmed.Get(location));

    WriteRequested.Set(location);
    WriteConfirmed.Set(location);

    ApplyBytes(location, IReadyQueue::EPBufferCounter::Total, true);

    if (WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount) {
        if (QuorumReadyPromise.Initialized()) {
            QuorumReadyPromise.TrySetValue();
        }

        State = EState::PBufferWritten;
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
    }
}

TInflightInfo::EState TInflightInfo::GetState() const
{
    return State;
}

NThreading::TFuture<void> TInflightInfo::GetQuorumReadyFuture()
{
    if (!QuorumReadyPromise.Initialized()) {
        QuorumReadyPromise = NThreading::NewPromise<void>();
    }
    return QuorumReadyPromise.GetFuture();
}

TLocationMask TInflightInfo::ReadMask() const
{
    switch (State) {
        case EState::PBufferIncompleteWrite:
            // Reading will be possible only after receiving a quorum.
            return TLocationMask::MakeEmpty();

        case EState::PBufferWritten:
        case EState::PBufferFlushing:
            // The data is written to PBuffer, but not transferred to DDisk.
            // Will read from confirmed PBuffer.
            return WriteConfirmed;

        case EState::PBufferFlushed:
        case EState::PBufferErasing:
        case EState::PBufferErased:
            // The data has already been transferred to DDisk.
            // Will read from DDisks.
            // Filter out non-desired or fresh later.
            return TLocationMask::MakeAllDDisks();
    }
}

ELocation TInflightInfo::RequestFlush(ELocation destination)
{
    Y_ABORT_UNLESS(IsDDisk(destination));
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing);

    FlushDesired.Set(destination);

    if (FlushRequested.Get(destination)) {
        return ELocation::Unknown;
    }

    const ELocation bestSource = TranslateDDiskToPBuffer(destination);
    if (WriteConfirmed.Get(bestSource)) {
        State = EState::PBufferFlushing;
        FlushRequested.Set(destination);
        return bestSource;
    }

    for (ELocation source: PBufferLocations) {
        if (WriteConfirmed.Get(source)) {
            State = EState::PBufferFlushing;
            FlushRequested.Set(destination);
            return source;
        }
    }

    Y_ABORT_UNLESS(false);
}

void TInflightInfo::ConfirmFlush(TRoute route)
{
    Y_ABORT_UNLESS(IsDDisk(route.Destination));
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(route.Destination));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(route.Destination));

    FlushConfirmed.Set(route.Destination);

    if (FlushDesired == FlushConfirmed) {
        State = EState::PBufferFlushed;
    }

    if (State == EState::PBufferFlushed && PBuffersLockCount == 0) {
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
    }
}

void TInflightInfo::FlushFailed(TRoute route)
{
    Y_ABORT_UNLESS(IsDDisk(route.Destination));
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(route.Destination));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(route.Destination));

    FlushRequested.Reset(route.Destination);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
}

TLocationMask TInflightInfo::GetRequestedFlushes() const
{
    return FlushRequested;
}

bool TInflightInfo::RequestErase(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(
        State == EState::PBufferFlushed || State == EState::PBufferErasing);
    Y_ABORT_UNLESS(FlushConfirmed.Count() >= QuorumDirectBlockGroupHostCount);

    if (WriteRequested.Get(location) && !EraseRequested.Get(location)) {
        State = EState::PBufferErasing;
        EraseRequested.Set(location);
        return true;
    }
    return false;
}

bool TInflightInfo::ConfirmErase(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(EraseRequested.Get(location));
    Y_ABORT_UNLESS(!EraseConfirmed.Get(location));

    EraseConfirmed.Set(location);
    if (EraseConfirmed == EraseRequested) {
        State = EState::PBufferErased;
    }

    return State == EState::PBufferErased;
}

void TInflightInfo::EraseFailed(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(!EraseConfirmed.Get(location));

    EraseRequested.Reset(location);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
}

void TInflightInfo::LockPBuffer()
{
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing ||
        State == EState::PBufferFlushed);

    ++PBuffersLockCount;

    if (PBuffersLockCount == 1) {
        ReadyQueue->UnRegister(Lsn);
        ApplyBytes(WriteConfirmed, IReadyQueue::EPBufferCounter::Locked, true);
    }
}

void TInflightInfo::UnlockPBuffer()
{
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing ||
        State == EState::PBufferFlushed);
    Y_ABORT_UNLESS(PBuffersLockCount > 0);

    --PBuffersLockCount;

    if (PBuffersLockCount == 0) {
        ApplyBytes(WriteConfirmed, IReadyQueue::EPBufferCounter::Locked, false);

        if (State == EState::PBufferWritten) {
            ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
        } else if (State == EState::PBufferFlushed) {
            ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
        }
    }
}

void TInflightInfo::ApplyBytes(
    ELocation location,
    IReadyQueue::EPBufferCounter counter,
    bool add) const
{
    if (!ReadyQueue) {
        return;
    }

    if (add) {
        ReadyQueue->DataToPBufferAdded(location, counter, ByteCount);
    } else {
        ReadyQueue->DataFromPBufferReleased(location, counter, ByteCount);
    }
}

void TInflightInfo::ApplyBytes(
    TLocationMask mask,
    IReadyQueue::EPBufferCounter counter,
    bool add) const
{
    for (auto location: mask) {
        ApplyBytes(location, counter, add);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
