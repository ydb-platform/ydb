#include "inflight_info.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueues,
    THostMask desiredDDisks,
    THostMask disabled,
    ui64 lsn,
    size_t byteCount,
    THostIndex host)
    : State(EState::PBufferIncompleteWrite)
    , ReadyQueue(readyQueues)
    , Lsn(lsn)
    , ByteCount(byteCount)
    , StartAt(TInstant::Now())
    , DesiredDDisks(desiredDDisks)
    , Disabled(disabled)
{
    WriteRequested.Set(host);
    WriteConfirmed.Set(host);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Clone);
    ApplyBytes(host, IReadyQueue::EPBufferCounter::Total, true);
}

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueue,
    THostMask desiredDDisks,
    THostMask disabled,
    ui64 lsn,
    size_t byteCount)
    : State(EState::PBufferPendingWrite)
    , ReadyQueue(readyQueue)
    , Lsn(lsn)
    , ByteCount(byteCount)
    , StartAt(TInstant::Now())
    , DesiredDDisks(desiredDDisks)
    , Disabled(disabled)
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
    , PersistGeneration(other.PersistGeneration)
    , DesiredDDisks(other.DesiredDDisks)
    , Disabled(other.Disabled)
    , WriteRequested(other.WriteRequested)
    , WriteConfirmed(other.WriteConfirmed)
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
    if (!ReadyQueue) {
        return;
    }

    Y_ABORT_UNLESS(PBuffersLockCount == 0);
    Y_ABORT_UNLESS(WriteConfirmed.Exclude(WriteRequested).Empty());

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

THostIndex TInflightInfo::RequestFlush(THostIndex destination)
{
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing);

    if (!DesiredDDisks.Exclude(Disabled).Get(destination)) {
        // Requested flush to absent or disabled host.
        return InvalidHostIndex;
    }

    if (FlushRequested.Get(destination)) {
        // Flush to destination already requested.
        return InvalidHostIndex;
    }

    if (WriteConfirmed.Get(destination)) {
        // Flush from PBuffer to DDisk inside same node.
        SetState(EState::PBufferFlushing);
        FlushRequested.Set(destination);
        return destination;
    }

    for (auto source: WriteConfirmed.Exclude(Disabled)) {
        // Cross-node flushing.
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

void TInflightInfo::ConfirmFlush(THostIndex host)
{
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(host));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(host));

    FlushConfirmed.Set(host);
    MaybeAdvanceToFlushed();
}

void TInflightInfo::FlushFailed(THostIndex host)
{
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(host));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(host));

    FlushRequested.Reset(host);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
}

THostMask TInflightInfo::GetInflightFlushes() const
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
    Y_ABORT_UNLESS(FlushConfirmed.Count() >= QuorumDirectBlockGroupHostCount);
    Y_ABORT_UNLESS(PBuffersLockCount == 0);

    SetState(EState::PBufferErasing);
    EraseRequested.Set(host);
}

bool TInflightInfo::ConfirmErase(THostIndex host)
{
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(EraseRequested.Get(host));
    Y_ABORT_UNLESS(!EraseConfirmed.Get(host));
    Y_ABORT_UNLESS(PBuffersLockCount == 0);

    EraseConfirmed.Set(host);

    MaybeAdvanceToErased();
    return State == EState::PBufferErased;
}

void TInflightInfo::EraseFailed(THostIndex host)
{
    Y_ABORT_UNLESS(
        State == EState::PBufferErasing || State == EState::PBufferErased);
    Y_ABORT_UNLESS(!EraseConfirmed.Get(host));
    Y_ABORT_UNLESS(PBuffersLockCount == 0);

    if (State == EState::PBufferErased) {
        // Belated error response after config has been changed.
        Y_ABORT_UNLESS(Disabled.Get(host));
        return;
    }

    EraseRequested.Reset(host);
    MaybeQueryErase();
}

THostMask TInflightInfo::GetEraseNeeded() const
{
    return WriteRequested.Exclude(EraseRequested).Exclude(EraseConfirmed);
}

void TInflightInfo::UpdateHosts(
    THostMask added,
    THostMask removed,
    THostMask disabled)
{
    // Removed hosts should be disabled too.
    Y_ABORT_UNLESS(removed.Exclude(disabled).Empty());

    switch (State) {
        case EState::PBufferPendingWrite:
        case EState::PBufferIncompleteWrite:
        case EState::PBufferWritten: {
            // Just update DesiredDDisks and Disabled.
            DesiredDDisks = DesiredDDisks.Include(added).Exclude(removed);
            Disabled = disabled;
            break;
        }
        case EState::PBufferFlushing: {
            // Just update DesiredDDisks and Disabled.
            DesiredDDisks = DesiredDDisks.Include(added).Exclude(removed);
            Disabled = disabled;

            auto notRequestsFlushes =
                DesiredDDisks.Exclude(Disabled).Exclude(FlushRequested);
            if (!notRequestsFlushes.Empty()) {
                // New desired added. Will flush to it.
                ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
            }
            MaybeAdvanceToFlushed();
            break;
        }
        case EState::PBufferFlushed: {
            // Already flushed to DDisk quorum, do not reconfigure
            // DesiredDDisks.
            Disabled = disabled;
            break;
        }
        case EState::PBufferErasing: {
            // Already flushed to DDisk quorum, do not reconfigure
            // DesiredDDisks.
            Disabled = disabled;
            MaybeAdvanceToErased();
            break;
        }
        case EState::PBufferErased: {
            // Nothing to do.
        } break;
    }
}

void TInflightInfo::LockPBuffer()
{
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing ||
        State == EState::PBufferFlushed);

    ++PBuffersLockCount;

    if (PBuffersLockCount == 1) {
        // When lsn locked for reading, we should not erase it.
        ReadyQueue->UnRegister(Lsn, IReadyQueue::EQueueType::Erase);
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
        MaybeQueryErase();
    }
}

void TInflightInfo::SetPersistGeneration(ui32 persistGeneration)
{
    Y_ABORT_UNLESS(PersistGeneration == 0);

    PersistGeneration = persistGeneration;
}

ui32 TInflightInfo::GetPersistGeneration() const
{
    return PersistGeneration;
}

TString TInflightInfo::DebugPrint(TInstant now) const
{
    TStringBuilder result;
    result << " " << FormatDuration(now - StartAt) << ", " << ToString(State)
           << ", size:" << ByteCount << ", locks:" << PBuffersLockCount
           << ", pgen:" << PersistGeneration << ", dd:" << DesiredDDisks.Print()
           << ", d:" << Disabled.Print() << ", wr:" << WriteRequested.Print()
           << ", wc:" << WriteConfirmed.Print()
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

void TInflightInfo::MaybeAdvanceToFlushed()
{
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);

    if (DesiredDDisks.Exclude(Disabled) == FlushConfirmed &&
        FlushConfirmed.Count() >= QuorumDirectBlockGroupHostCount)
    {
        SetState(EState::PBufferFlushed);
        MaybeQueryErase();
    }
}

void TInflightInfo::MaybeAdvanceToErased()
{
    Y_ABORT_UNLESS(
        State == EState::PBufferFlushed || State == EState::PBufferErasing);

    if (EraseConfirmed.Exclude(Disabled) == WriteRequested.Exclude(Disabled)) {
        SetState(EState::PBufferErased);
    }
}

void TInflightInfo::MaybeQueryErase()
{
    if (PBuffersLockCount != 0 || State == EState::PBufferWritten ||
        State == EState::PBufferFlushing)
    {
        return;
    }

    Y_ABORT_UNLESS(
        State == EState::PBufferFlushed || State == EState::PBufferErasing);

    if (!WriteRequested.Exclude(Disabled).Exclude(EraseRequested).Empty()) {
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
        ReadyQueue->FlushCompleted(Lsn, FlushConfirmed);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
