#include "inflight_info.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TInflightInfo::TInflightInfo(
<<<<<<< HEAD
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
=======
>>>>>>> 78a20188184 (Field map set optimization (#52565))
    IReadyQueue* readyQueue,
<<<<<<< HEAD
    ui64 lsn,
    size_t byteCount)
    : State(EState::PBufferPendingWrite)
    , ReadyQueue(readyQueue)
    , Lsn(lsn)
    , ByteCount(byteCount)
=======
    THostMask desiredDDisks,
    THostMask disabled)
    : ReadyQueue(readyQueue)
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    , StartAt(TInstant::Now())
{
    // Pending: no PBuffer holds the data yet, so nothing is registered in a
    // ready queue and no bytes are accounted. The write is not acknowledged, so
    // reads ignore it (PBufferPendingWrite reads from DDisk, never blocks).
}

TInflightInfo::TInflightInfo(TInflightInfo&& other) noexcept
<<<<<<< HEAD
    : State(other.State)
    , ReadyQueue(other.ReadyQueue)
    , Lsn(other.Lsn)
    , ByteCount(other.ByteCount)
=======
    : ReadyQueue(other.ReadyQueue)
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    , StartAt(other.StartAt)
    , QuorumReadyPromise(std::move(other.QuorumReadyPromise))
<<<<<<< HEAD
=======
    , PersistGeneration(other.PersistGeneration)
    , PBuffersLockCount(other.PBuffersLockCount)
    , State(other.State)
    , DesiredDDisks(other.DesiredDDisks)
    , Disabled(other.Disabled)
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
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
<<<<<<< HEAD

    ApplyBytes(WriteRequested, IReadyQueue::EPBufferCounter::Total, false);
=======
    Y_ABORT_UNLESS(WriteConfirmed.Exclude(WriteRequested).Empty());
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
}

void TInflightInfo::Detach()
{
    ReadyQueue = nullptr;
}

void TInflightInfo::RestorePBuffer(THostIndex host)
{
    Y_ABORT_UNLESS(
        State == EState::PBufferPendingWrite ||
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
<<<<<<< HEAD
<<<<<<< HEAD
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
=======
        ReadyQueue->Register(PBufferKey, IReadyQueue::EQueueType::Flush);
    } else {
        SetState(EState::PBufferIncompleteWrite);
        ReadyQueue->Register(PBufferKey, IReadyQueue::EQueueType::Clone);
>>>>>>> 78a20188184 (Field map set optimization (#52565))
=======
        ReadyQueue->Register(*this, IReadyQueue::EQueueType::Flush);
    } else {
        SetState(EState::PBufferIncompleteWrite);
        ReadyQueue->Register(*this, IReadyQueue::EQueueType::Clone);
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
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
<<<<<<< HEAD
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
=======
    ReadyQueue->Register(*this, IReadyQueue::EQueueType::Flush);
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
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
<<<<<<< HEAD
            return {.Mask = WriteConfirmed, .Lsn = Lsn};
=======
            return {.Mask = WriteConfirmed, .PBufferKey = GetPBufferKey()};
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

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

void TInflightInfo::ConfirmFlush(THostIndex host)
{
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(host));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(host));

    FlushConfirmed.Set(host);
<<<<<<< HEAD
<<<<<<< HEAD

    if (FlushDesired == FlushConfirmed) {
        SetState(EState::PBufferFlushed);
    }

    if (State == EState::PBufferFlushed && PBuffersLockCount == 0) {
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
    }
=======
    ReadyQueue->InflightFlushFinished(PBufferKey, host);
=======
    ReadyQueue->InflightFlushFinished(*this, host);
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    MaybeAdvanceToFlushed();
>>>>>>> 670e22678de ([NBS-7383] Fixed ddisk sync hung (#52938))
}

void TInflightInfo::FlushFailed(THostIndex host)
{
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(host));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(host));

    FlushRequested.Reset(host);
<<<<<<< HEAD
<<<<<<< HEAD
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
=======
    ReadyQueue->Register(PBufferKey, IReadyQueue::EQueueType::Flush);
    ReadyQueue->InflightFlushFinished(PBufferKey, host);
>>>>>>> 670e22678de ([NBS-7383] Fixed ddisk sync hung (#52938))
=======
    ReadyQueue->Register(*this, IReadyQueue::EQueueType::Flush);
    ReadyQueue->InflightFlushFinished(*this, host);
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
}

THostMask TInflightInfo::GetRequestedFlushes() const
{
    return FlushRequested.Exclude(FlushConfirmed);
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

void TInflightInfo::ConfirmErase(THostIndex host)
{
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(EraseRequested.Get(host));
    Y_ABORT_UNLESS(!EraseConfirmed.Get(host));

    EraseConfirmed.Set(host);
<<<<<<< HEAD
    if (EraseConfirmed == WriteRequested) {
        SetState(EState::PBufferErased);
    }

    return State == EState::PBufferErased;
=======
    MaybeAdvanceToErased();
>>>>>>> 78a20188184 (Field map set optimization (#52565))
}

void TInflightInfo::EraseFailed(THostIndex host)
{
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(!EraseConfirmed.Get(host));

    EraseRequested.Reset(host);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
}

THostMask TInflightInfo::GetEraseNeeded() const
{
    return WriteRequested.Exclude(EraseRequested).Exclude(EraseConfirmed);
}

void TInflightInfo::RemoveHosts(THostMask removed)
{
    const THostMask removedFromWrite = WriteRequested.LogicalAnd(removed);
    if (removedFromWrite.Empty()) {
        return;
    }

<<<<<<< HEAD
    // Release byte counters for removed hosts that had writes.
    ApplyBytes(removedFromWrite, IReadyQueue::EPBufferCounter::Total, false);

    if (PBuffersLockCount > 0) {
        ApplyBytes(
            WriteConfirmed.LogicalAnd(removed),
            IReadyQueue::EPBufferCounter::Locked,
            false);
    }

    // Remove hosts from all tracking masks.
    WriteRequested = WriteRequested.Exclude(removed);
    WriteConfirmed = WriteConfirmed.Exclude(removed);
    FlushDesired = FlushDesired.Exclude(removed);
    FlushRequested = FlushRequested.Exclude(removed);
    FlushConfirmed = FlushConfirmed.Exclude(removed);
    EraseRequested = EraseRequested.Exclude(removed);
    EraseConfirmed = EraseConfirmed.Exclude(removed);

    // Check if flush became complete after removing hosts.
    if (State == EState::PBufferFlushing && FlushDesired == FlushConfirmed) {
        SetState(EState::PBufferFlushed);
    }

    // Register for erase if flush is done and PBuffers are not locked.
    if (State == EState::PBufferFlushed && PBuffersLockCount == 0) {
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
    }

    // Check if erase became complete after removing hosts.
    if (State == EState::PBufferErasing && EraseConfirmed == WriteRequested) {
        SetState(EState::PBufferErased);
=======
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
            const auto droppedFlushes =
                GetInflightFlushes().LogicalAnd(disabled);

            DesiredDDisks = DesiredDDisks.Include(added).Exclude(removed);
            Disabled = disabled;
            FlushRequested = FlushRequested.Exclude(disabled);

            auto notRequestsFlushes =
                DesiredDDisks.Exclude(Disabled).Exclude(FlushRequested);
            if (!notRequestsFlushes.Empty()) {
                // New desired added. Will flush to it.
                ReadyQueue->Register(*this, IReadyQueue::EQueueType::Flush);
            }

            for (const auto host: droppedFlushes) {
                ReadyQueue->InflightFlushFinished(*this, host);
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
>>>>>>> 670e22678de ([NBS-7383] Fixed ddisk sync hung (#52938))
    }

    CheckInvariants();
}

void TInflightInfo::LockPBuffer()
{
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing ||
        State == EState::PBufferFlushed);

    ++PBuffersLockCount;

    if (PBuffersLockCount == 1) {
<<<<<<< HEAD
        ReadyQueue->UnRegister(Lsn);
=======
        // When lsn locked for reading, we should not erase it.
        ReadyQueue->UnRegister(*this, IReadyQueue::EQueueType::Erase);
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
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

TString TInflightInfo::DebugPrint(TInstant now) const
{
    TStringBuilder result;
    result << " " << FormatDuration(now - StartAt) << ", " << ToString(State)
<<<<<<< HEAD
           << ", size:" << ByteCount << ", locks:" << PBuffersLockCount
=======
           << ", locks:" << PBuffersLockCount << ", pgen:" << PersistGeneration
           << ", dd:" << DesiredDDisks.Print() << ", d:" << Disabled.Print()
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
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
        ReadyQueue->DataToPBufferAdded(*this, host, counter);
    } else {
        ReadyQueue->DataFromPBufferReleased(*this, host, counter);
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
            Y_ABORT_UNLESS(false, "Cannot transition to initial state");
            break;
        case EState::PBufferIncompleteWrite:
            Y_ABORT_UNLESS(State == EState::PBufferPendingWrite);
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
    CheckInvariants();

    if (State == EState::PBufferFlushed) {
        ReadyQueue->UnRegister(*this, IReadyQueue::EQueueType::Flush);
        ReadyQueue->FlushCompleted(*this, FlushConfirmed);
    }
    if (State == EState::PBufferErased) {
        ApplyBytes(WriteRequested, IReadyQueue::EPBufferCounter::Total, false);
    }
}

<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
void TInflightInfo::CheckInvariants() const
{
    Y_ABORT_UNLESS(WriteConfirmed.Exclude(WriteRequested).Empty());
    Y_ABORT_UNLESS(
        FlushConfirmed.Exclude(Disabled).Exclude(FlushRequested).Empty());
    Y_ABORT_UNLESS(
        FlushRequested.Exclude(Disabled).Exclude(DesiredDDisks).Empty());
    Y_ABORT_UNLESS(EraseConfirmed.Exclude(EraseRequested).Empty());
    Y_ABORT_UNLESS(EraseRequested.Exclude(WriteRequested).Empty());

    switch (State) {
        case EState::PBufferPendingWrite:
            Y_ABORT_UNLESS(WriteRequested.Empty());
            Y_ABORT_UNLESS(WriteConfirmed.Empty());
            Y_ABORT_UNLESS(FlushRequested.Empty());
            Y_ABORT_UNLESS(FlushConfirmed.Empty());
            Y_ABORT_UNLESS(EraseRequested.Empty());
            Y_ABORT_UNLESS(EraseConfirmed.Empty());
            break;
        case EState::PBufferIncompleteWrite:
            Y_ABORT_UNLESS(FlushRequested.Empty());
            Y_ABORT_UNLESS(FlushConfirmed.Empty());
            Y_ABORT_UNLESS(EraseRequested.Empty());
            Y_ABORT_UNLESS(EraseConfirmed.Empty());
            Y_ABORT_UNLESS(
                WriteConfirmed.Count() < QuorumDirectBlockGroupHostCount);
            break;
        case EState::PBufferWritten:
            Y_ABORT_UNLESS(
                WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount);
            Y_ABORT_UNLESS(FlushRequested.Empty());
            Y_ABORT_UNLESS(FlushConfirmed.Empty());
            Y_ABORT_UNLESS(EraseRequested.Empty());
            Y_ABORT_UNLESS(EraseConfirmed.Empty());
            break;
        case EState::PBufferFlushing:
            Y_ABORT_UNLESS(
                WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount);
            Y_ABORT_UNLESS(EraseRequested.Empty());
            Y_ABORT_UNLESS(EraseConfirmed.Empty());
            break;
        case EState::PBufferFlushed:
        case EState::PBufferErasing:
        case EState::PBufferErased:
            Y_ABORT_UNLESS(
                FlushConfirmed.Count() >= QuorumDirectBlockGroupHostCount);
            Y_ABORT_UNLESS(GetInflightFlushes().Empty());
            break;
    }

    if (State == EState::PBufferErased) {
        Y_ABORT_UNLESS(
            EraseConfirmed.Exclude(Disabled) ==
            WriteRequested.Exclude(Disabled));
        Y_ABORT_UNLESS(PBuffersLockCount == 0);
    }
}

>>>>>>> 670e22678de ([NBS-7383] Fixed ddisk sync hung (#52938))
void TInflightInfo::MaybeAdvanceToFlushed()
{
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);

    if (DesiredDDisks.Exclude(Disabled) == FlushConfirmed.Exclude(Disabled) &&
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

    const auto hostsToErase =
        WriteRequested.Exclude(Disabled).Exclude(EraseRequested);
    if (!hostsToErase.Empty()) {
        ReadyQueue->Register(*this, IReadyQueue::EQueueType::Erase);
    }
}

<<<<<<< HEAD
>>>>>>> 78a20188184 (Field map set optimization (#52565))
=======
TPBufferKey TInflightInfo::GetPBufferKey() const
{
    return ReadyQueue->GetPBufferKey(*this);
}

>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
