#include "inflight_info.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

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
    , StartAt(TInstant::Now())
{
    WriteRequested.Set(host);
    WriteConfirmed.Set(host);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Clone);
    ApplyBytes(host, IReadyQueue::EPBufferCounter::Total, true);
}

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueue,
    ui64 lsn,
    size_t byteCount)
    : State(EState::PBufferPendingWrite)
    , ReadyQueue(readyQueue)
    , Lsn(lsn)
    , ByteCount(byteCount)
    , StartAt(TInstant::Now())
{
    // Pending: no PBuffer holds the data yet, so nothing is registered in a
    // ready queue and no bytes are accounted. The write is not acknowledged, so
    // reads ignore it (PBufferPendingWrite reads from DDisk, never blocks).
}

TInflightInfo::TInflightInfo(TInflightInfo&& other) noexcept
    : State(other.State)
    , ReadyQueue(other.ReadyQueue)
    , Lsn(other.Lsn)
    , ByteCount(other.ByteCount)
    , StartAt(other.StartAt)
    , PBuffersLockCount(other.PBuffersLockCount)
    , QuorumReadyPromise(std::move(other.QuorumReadyPromise))
    , WriteRequested(other.WriteRequested)
    , WriteConfirmed(other.WriteConfirmed)
    , FlushDesired(other.FlushDesired)
    , FlushRequested(other.FlushRequested)
    , FlushConfirmed(other.FlushConfirmed)
    , EraseRequested(other.EraseRequested)
    , EraseConfirmed(other.EraseConfirmed)
{
    other.ReadyQueue = nullptr;
    other.PBuffersLockCount = 0;
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
    Y_ABORT_UNLESS(!WriteRequested.Get(host));
    Y_ABORT_UNLESS(!WriteConfirmed.Get(host));

    WriteRequested.Set(host);
    WriteConfirmed.Set(host);

    ApplyBytes(host, IReadyQueue::EPBufferCounter::Total, true);

    if (WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount) {
        if (QuorumReadyPromise.Initialized()) {
            QuorumReadyPromise.TrySetValue();
        }

        SetState(EState::PBufferWritten);
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
    }
}

void TInflightInfo::OnWritten(
    THostMask writeRequested,
    THostMask writeConfirmed)
{
    Y_ABORT_UNLESS(State == EState::PBufferPendingWrite);
    Y_ABORT_UNLESS(WriteConfirmed.Count() == 0);
    Y_ABORT_UNLESS(writeConfirmed.Count() >= QuorumDirectBlockGroupHostCount);

    WriteRequested = writeRequested;
    WriteConfirmed = writeConfirmed;
    SetState(EState::PBufferWritten);

    ApplyBytes(WriteRequested, IReadyQueue::EPBufferCounter::Total, true);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
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

TReadSource TInflightInfo::ReadMask() const
{
    switch (State) {
        case EState::PBufferPendingWrite:
            // The write is not acknowledged yet, so it is invisible to reads:
            // read the pre-write data from DDisk (Lsn=0). Never blocks.
            return {.Mask = THostMask::MakeAll(MaxHostCount), .Lsn = 0};

        case EState::PBufferIncompleteWrite:
            // Reading will be possible only after receiving a quorum.
            return {.Mask = THostMask::MakeEmpty(), .Lsn = 0};

        case EState::PBufferWritten:
        case EState::PBufferFlushing:
            // The data is written to PBuffer, but not transferred to DDisk.
            // Will read from confirmed PBuffer at this inflight's Lsn.
            return {.Mask = WriteConfirmed, .Lsn = Lsn};

        case EState::PBufferFlushed:
        case EState::PBufferErasing:
        case EState::PBufferErased:
            // The data has already been transferred to DDisk.
            // Will read from DDisks. Lsn=0 marks a DDisk read.
            // Filter out non-desired or fresh later.
            return {.Mask = THostMask::MakeAll(MaxHostCount), .Lsn = 0};
    }
}

THostIndex TInflightInfo::RequestFlush(
    THostIndex destination,
    THostMask disabledHosts)
{
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing);

    FlushDesired.Set(destination);

    if (FlushRequested.Get(destination)) {
        return InvalidHostIndex;
    }

    if (WriteConfirmed.Get(destination)) {
        SetState(EState::PBufferFlushing);
        FlushRequested.Set(destination);
        return destination;
    }

    // Prefer enabled hosts.
    for (auto source: WriteConfirmed.Exclude(disabledHosts)) {
        SetState(EState::PBufferFlushing);
        FlushRequested.Set(destination);
        return source;
    }

    // TODO. All hosts are disabled. Need to figure out what to do in this case.
    for (auto source: WriteConfirmed) {
        SetState(EState::PBufferFlushing);
        FlushRequested.Set(destination);
        return source;
    }

    Y_ABORT_UNLESS(false);
}

void TInflightInfo::ConfirmFlush(THostRoute route)
{
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(route.DestinationHostIndex));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(route.DestinationHostIndex));

    FlushConfirmed.Set(route.DestinationHostIndex);

    if (FlushDesired == FlushConfirmed) {
        SetState(EState::PBufferFlushed);
    }

    if (State == EState::PBufferFlushed && PBuffersLockCount == 0) {
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
    }
}

void TInflightInfo::FlushFailed(THostRoute route)
{
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(route.DestinationHostIndex));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(route.DestinationHostIndex));

    FlushRequested.Reset(route.DestinationHostIndex);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
}

THostMask TInflightInfo::GetRequestedFlushes() const
{
    return FlushRequested;
}

void TInflightInfo::RequestErase(THostIndex host)
{
    Y_ABORT_UNLESS(
        State == EState::PBufferFlushed || State == EState::PBufferErasing);

    Y_ABORT_UNLESS(WriteRequested.Get(host));
    Y_ABORT_UNLESS(!EraseRequested.Get(host));
    Y_ABORT_UNLESS(!EraseConfirmed.Get(host));

    // When the DDisk is not fully filled, the flush is made into the filled
    // area. Thus, the number of completed flushes may be less than the quorum.
    Y_ABORT_UNLESS(!FlushConfirmed.Empty());

    SetState(EState::PBufferErasing);
    EraseRequested.Set(host);
}

bool TInflightInfo::ConfirmErase(THostIndex host)
{
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(EraseRequested.Get(host));
    Y_ABORT_UNLESS(!EraseConfirmed.Get(host));

    EraseConfirmed.Set(host);
    if (EraseConfirmed == WriteRequested) {
        SetState(EState::PBufferErased);
    }

    return State == EState::PBufferErased;
}

void TInflightInfo::EraseFailed(THostIndex host)
{
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(!EraseConfirmed.Get(host));

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

        if (State == EState::PBufferWritten) {
            ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
        } else if (State == EState::PBufferFlushed) {
            ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
        }
    }
}

THostMask TInflightInfo::GetEraseNeeded() const
{
    return WriteRequested.Exclude(EraseRequested).Exclude(EraseConfirmed);
}

TString TInflightInfo::DebugPrint(TInstant now) const
{
    TStringBuilder result;
    result << " " << FormatDuration(now - StartAt) << ", " << ToString(State)
           << ", size:" << ByteCount << ", locks:" << PBuffersLockCount
           << ", wr:" << WriteRequested.Print()
           << ", wc:" << WriteConfirmed.Print()
           << ", fd:" << FlushDesired.Print()
           << ", fr:" << FlushRequested.Print()
           << ", fc:" << FlushConfirmed.Print()
           << ", er:" << EraseRequested.Print()
           << ", ec:" << EraseConfirmed.Print();

    return result;
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

void TInflightInfo::SetState(EState newState)
{
    if (State == newState) {
        return;
    }

    switch (newState) {
        case EState::PBufferPendingWrite:
        case EState::PBufferIncompleteWrite:
            Y_ABORT_UNLESS(false, "Cannot transition to initial state");
            break;
        case EState::PBufferWritten:
            Y_ABORT_UNLESS(
                State == EState::PBufferPendingWrite ||
                State == EState::PBufferIncompleteWrite);
            break;
        case EState::PBufferFlushing:
            Y_ABORT_UNLESS(State == EState::PBufferWritten);
            break;
        case EState::PBufferFlushed:
            Y_ABORT_UNLESS(State == EState::PBufferFlushing);
            break;
        case EState::PBufferErasing:
            Y_ABORT_UNLESS(State == EState::PBufferFlushed);
            break;
        case EState::PBufferErased:
            Y_ABORT_UNLESS(State == EState::PBufferErasing);
            break;
    }

    State = newState;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
