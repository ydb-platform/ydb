#include "inflight_info.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueues,
    ui64 lsn,
    size_t byteCount,
    THostIndex host)
    : State(EState::PBufferIncompleteWrite)
    , ReadyQueue(readyQueues)
    , Lsn(lsn)
    , ByteCount(byteCount)
{
    WriteRequested.Set(host);
    WriteConfirmed.Set(host);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Clone);
    ApplyBytes(host, IReadyQueue::EPBufferCounter::Total, true);
}

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueue,
    ui64 lsn,
    size_t byteCount,
    THostMask writeRequested,
    THostMask writeConfirmed)
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

void TInflightInfo::RestorePBuffer(THostIndex host)
{
    Y_ABORT_UNLESS(
        State == EState::PBufferIncompleteWrite ||
        State == EState::PBufferWritten);
    Y_ABORT_UNLESS(!WriteRequested.Test(host));
    Y_ABORT_UNLESS(!WriteConfirmed.Test(host));

    WriteRequested.Set(host);
    WriteConfirmed.Set(host);

    ApplyBytes(host, IReadyQueue::EPBufferCounter::Total, true);

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

TReadSource TInflightInfo::ReadMask(THostMask allDDisks) const
{
    switch (State) {
        case EState::PBufferIncompleteWrite:
            // Reading will be possible only after receiving a quorum.
            return {THostMask::MakeEmpty(), /*FromDDisk=*/false};

        case EState::PBufferWritten:
        case EState::PBufferFlushing:
            // The data is written to PBuffer, but not transferred to DDisk.
            // Will read from confirmed PBuffer.
            return {WriteConfirmed, /*FromDDisk=*/false};

        case EState::PBufferFlushed:
        case EState::PBufferErasing:
        case EState::PBufferErased:
            // The data has already been transferred to DDisk.
            // Will read from DDisks.
            // Filter out non-desired or fresh later.
            return {allDDisks, /*FromDDisk=*/true};
    }
}

THostIndex TInflightInfo::RequestFlush(THostIndex destination)
{
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing);

    FlushDesired.Set(destination);

    if (FlushRequested.Test(destination)) {
        return InvalidHostIndex;
    }

    if (WriteConfirmed.Test(destination)) {
        State = EState::PBufferFlushing;
        FlushRequested.Set(destination);
        return destination;
    }

    for (auto source: WriteConfirmed) {
        State = EState::PBufferFlushing;
        FlushRequested.Set(destination);
        return source;
    }

    Y_ABORT_UNLESS(false);
}

void TInflightInfo::ConfirmFlush(THostRoute route)
{
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Test(route.DestinationHostIndex));
    Y_ABORT_UNLESS(!FlushConfirmed.Test(route.DestinationHostIndex));

    FlushConfirmed.Set(route.DestinationHostIndex);

    if (FlushDesired == FlushConfirmed) {
        State = EState::PBufferFlushed;
    }

    if (State == EState::PBufferFlushed && PBuffersLockCount == 0) {
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
    }
}

void TInflightInfo::FlushFailed(THostRoute route)
{
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Test(route.DestinationHostIndex));
    Y_ABORT_UNLESS(!FlushConfirmed.Test(route.DestinationHostIndex));

    FlushRequested.Reset(route.DestinationHostIndex);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
}

THostMask TInflightInfo::GetRequestedFlushes() const
{
    return FlushRequested;
}

bool TInflightInfo::RequestErase(THostIndex host)
{
    Y_ABORT_UNLESS(
        State == EState::PBufferFlushed || State == EState::PBufferErasing);
    Y_ABORT_UNLESS(FlushConfirmed.Count() >= QuorumDirectBlockGroupHostCount);

    if (WriteRequested.Test(host) && !EraseRequested.Test(host)) {
        State = EState::PBufferErasing;
        EraseRequested.Set(host);
        return true;
    }
    return false;
}

bool TInflightInfo::ConfirmErase(THostIndex host)
{
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(EraseRequested.Test(host));
    Y_ABORT_UNLESS(!EraseConfirmed.Test(host));

    EraseConfirmed.Set(host);
    if (EraseConfirmed == EraseRequested) {
        State = EState::PBufferErased;
    }

    return State == EState::PBufferErased;
}

void TInflightInfo::EraseFailed(THostIndex host)
{
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(!EraseConfirmed.Test(host));

    EraseRequested.Reset(host);
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
    }

    if (State == EState::PBufferFlushed && PBuffersLockCount == 0) {
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
    }
}

void TInflightInfo::ApplyBytes(
    THostIndex host,
    IReadyQueue::EPBufferCounter counter,
    bool add) const
{
    if (!ReadyQueue) {
        return;
    }

    if (add) {
        ReadyQueue->DataToPBufferAdded(host, counter, ByteCount);
    } else {
        ReadyQueue->DataFromPBufferReleased(host, counter, ByteCount);
    }
}

void TInflightInfo::ApplyBytes(
    THostMask mask,
    IReadyQueue::EPBufferCounter counter,
    bool add) const
{
    for (auto host: mask) {
        ApplyBytes(host, counter, add);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
