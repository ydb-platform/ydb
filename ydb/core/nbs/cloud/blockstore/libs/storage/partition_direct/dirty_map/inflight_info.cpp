#include "inflight_info.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueue,
    THostMask desiredDDisks,
    THostMask disabled)
    : ReadyQueue(readyQueue)
    , StartAt(TInstant::Now())
    , DesiredDDisks(desiredDDisks)
    , Disabled(disabled)
{
    // Pending: no PBuffer holds the data yet, so nothing is registered in a
    // ready queue and no bytes are accounted. The write is not acknowledged, so
    // reads ignore it (PBufferPendingWrite reads from DDisk, never blocks).
}

TInflightInfo::TInflightInfo(TInflightInfo&& other) noexcept
    : ReadyQueue(other.ReadyQueue)
    , StartAt(other.StartAt)
    , QuorumReadyPromise(std::move(other.QuorumReadyPromise))
    , PersistGeneration(other.PersistGeneration)
    , PBuffersLockCount(other.PBuffersLockCount)
    , State(other.State)
    , DesiredDDisks(other.DesiredDDisks)
    , Disabled(other.Disabled)
    , WriteRequested(other.WriteRequested)
    , WriteConfirmed(other.WriteConfirmed)
    , WriteAnswered(other.WriteAnswered)
    , FlushRequested(other.FlushRequested)
    , FlushConfirmed(other.FlushConfirmed)
    , EraseRequested(other.EraseRequested)
    , EraseConfirmed(other.EraseConfirmed)
    , EraseSentBeforeAnswer(other.EraseSentBeforeAnswer)
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
    WriteAnswered.Set(host);

    ApplyBytes(host, IReadyQueue::EPBufferCounter::Total, true);

    if (WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount) {
        if (QuorumReadyPromise.Initialized()) {
            QuorumReadyPromise.TrySetValue();
        }

        SetState(EState::PBufferWritten);
        ReadyQueue->Register(*this, IReadyQueue::EQueueType::Flush);
    } else {
        SetState(EState::PBufferIncompleteWrite);
        ReadyQueue->Register(*this, IReadyQueue::EQueueType::Clone);
    }
}

void TInflightInfo::OnWritten(
    THostMask writeRequested,
    THostMask writeConfirmed,
    THostMask writeAnswered)
{
    Y_ABORT_UNLESS(State == EState::PBufferPendingWrite);
    Y_ABORT_UNLESS(WriteConfirmed.Count() == 0);
    Y_ABORT_UNLESS(writeConfirmed.Count() >= QuorumDirectBlockGroupHostCount);

    WriteRequested = writeRequested;
    WriteConfirmed = writeConfirmed;
    WriteAnswered = writeAnswered;
    SetState(EState::PBufferWritten);

    ApplyBytes(WriteRequested, IReadyQueue::EPBufferCounter::Total, true);
    ReadyQueue->Register(*this, IReadyQueue::EQueueType::Flush);
}

void TInflightInfo::OnWriteWithoutQuorum(
    THostMask writeRequested,
    THostMask writeConfirmed,
    THostMask writeAnswered)
{
    Y_ABORT_UNLESS(State == EState::PBufferPendingWrite);
    Y_ABORT_UNLESS(WriteConfirmed.Count() == 0);
    Y_ABORT_UNLESS(writeConfirmed.Count() < QuorumDirectBlockGroupHostCount);

    WriteRequested = writeRequested;
    WriteConfirmed = writeConfirmed;
    WriteAnswered = writeAnswered;
    SetState(EState::PBufferDiscarded);

    ApplyBytes(WriteRequested, IReadyQueue::EPBufferCounter::Total, true);
    MaybeAdvanceToErased();
    MaybeQueryErase();
}

void TInflightInfo::OnBelatedWrite(THostMask completed, THostMask failed)
{
    const auto answered = completed.Include(failed);
    Y_ABORT_UNLESS(completed.LogicalAnd(failed).Empty());
    Y_ABORT_UNLESS(answered.Exclude(WriteRequested).Empty());
    Y_ABORT_UNLESS(State != EState::PBufferErased);

    WriteConfirmed = WriteConfirmed.Include(completed);
    WriteAnswered = WriteAnswered.Include(answered);

    const auto eraseDone = answered.LogicalAnd(EraseConfirmed);
    const auto eraseInFlight =
        answered.LogicalAnd(EraseRequested).Exclude(EraseConfirmed);

    EraseSentBeforeAnswer = EraseSentBeforeAnswer.Include(eraseInFlight);
    EraseRequested = EraseRequested.Exclude(eraseDone);
    EraseConfirmed = EraseConfirmed.Exclude(eraseDone);

    MaybeAdvanceToErased();
    MaybeQueryErase();
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
            return {.Mask = THostMask::MakeAll(MaxHostCount), .PBufferKey = {}};

        case EState::PBufferIncompleteWrite:
            // Reading will be possible only after receiving a quorum.
            return {.Mask = THostMask::MakeEmpty(), .PBufferKey = {}};

        case EState::PBufferWritten:
        case EState::PBufferFlushing:
            // The data is written to PBuffer, but not transferred to DDisk.
            // Will read from confirmed PBuffer at this inflight's Lsn.
            return {.Mask = WriteConfirmed, .PBufferKey = GetPBufferKey()};

        case EState::PBufferFlushed:
        case EState::PBufferDiscarded:
        case EState::PBufferErased:
            // The data has already been transferred to DDisk, or the write
            // was answered with an error and its copies are garbage.
            // Will read from DDisks. Lsn=0 marks a DDisk read.
            // Filter out non-desired or fresh later.
            return {.Mask = THostMask::MakeAll(MaxHostCount), .PBufferKey = {}};
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
    ReadyQueue->InflightFlushFinished(*this, host);
    MaybeAdvanceToFlushed();
}

void TInflightInfo::FlushFailed(THostIndex host)
{
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(host));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(host));

    FlushRequested.Reset(host);
    ReadyQueue->Register(*this, IReadyQueue::EQueueType::Flush);
    ReadyQueue->InflightFlushFinished(*this, host);
}

THostMask TInflightInfo::GetInflightFlushes() const
{
    return FlushRequested.Exclude(FlushConfirmed);
}

void TInflightInfo::RequestErase(THostIndex host)
{
    Y_ABORT_UNLESS(CanErase());
    Y_ABORT_UNLESS(WriteRequested.Get(host));
    Y_ABORT_UNLESS(!EraseRequested.Get(host));
    Y_ABORT_UNLESS(!EraseConfirmed.Get(host));
    Y_ABORT_UNLESS(PBuffersLockCount == 0);

    EraseRequested.Set(host);
}

void TInflightInfo::ConfirmErase(THostIndex host)
{
    Y_ABORT_UNLESS(CanErase());
    Y_ABORT_UNLESS(EraseRequested.Get(host));
    Y_ABORT_UNLESS(!EraseConfirmed.Get(host));
    Y_ABORT_UNLESS(PBuffersLockCount == 0);

    if (EraseSentBeforeAnswer.Get(host)) {
        RetryErase(host);
        return;
    }

    EraseConfirmed.Set(host);
    MaybeAdvanceToErased();
}

void TInflightInfo::EraseFailed(THostIndex host)
{
    Y_ABORT_UNLESS(CanErase() || State == EState::PBufferErased);
    Y_ABORT_UNLESS(!EraseConfirmed.Get(host));
    Y_ABORT_UNLESS(PBuffersLockCount == 0);

    if (State == EState::PBufferErased) {
        // Belated error response after config has been changed.
        Y_ABORT_UNLESS(Disabled.Get(host));
        return;
    }

    RetryErase(host);
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
        case EState::PBufferFlushed:
        case EState::PBufferDiscarded: {
            // Nothing more will be flushed, so DesiredDDisks stays as it is.
            Disabled = disabled;
            // The record does not wait for a disabled host: it may be done.
            MaybeAdvanceToErased();
            break;
        }
        case EState::PBufferErased: {
            // Nothing to do.
        } break;
    }

    CheckInvariants();
}

void TInflightInfo::LockPBuffer()
{
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing ||
        State == EState::PBufferFlushed);
    // Erasing has not started: the state alone no longer says that.
    Y_ABORT_UNLESS(EraseRequested.Empty());

    ++PBuffersLockCount;

    if (PBuffersLockCount == 1) {
        // When lsn locked for reading, we should not erase it.
        ReadyQueue->UnRegister(*this, IReadyQueue::EQueueType::Erase);
        // Counted by the same mask as Total: it does not change under a lock,
        // unlike WriteConfirmed, which a belated answer may extend.
        ApplyBytes(WriteRequested, IReadyQueue::EPBufferCounter::Locked, true);
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
        ApplyBytes(WriteRequested, IReadyQueue::EPBufferCounter::Locked, false);
        // The record could not leave while it was locked: check now.
        MaybeAdvanceToErased();
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
           << ", locks:" << PBuffersLockCount << ", pgen:" << PersistGeneration
           << ", dd:" << DesiredDDisks.Print() << ", d:" << Disabled.Print()
           << ", wr:" << WriteRequested.Print()
           << ", wc:" << WriteConfirmed.Print()
           << ", wa:" << WriteAnswered.Print()
           << ", fr:" << FlushRequested.Print()
           << ", fc:" << FlushConfirmed.Print()
           << ", er:" << EraseRequested.Print()
           << ", ec:" << EraseConfirmed.Print()
           << ", eb:" << EraseSentBeforeAnswer.Print();

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
        case EState::PBufferDiscarded:
            Y_ABORT_UNLESS(State == EState::PBufferPendingWrite);
            break;
        case EState::PBufferErased:
            Y_ABORT_UNLESS(
                State == EState::PBufferFlushed ||
                State == EState::PBufferDiscarded);
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

void TInflightInfo::CheckInvariants() const
{
    Y_ABORT_UNLESS(WriteConfirmed.Exclude(WriteRequested).Empty());
    Y_ABORT_UNLESS(WriteAnswered.Exclude(WriteRequested).Empty());
    Y_ABORT_UNLESS(
        FlushConfirmed.Exclude(Disabled).Exclude(FlushRequested).Empty());
    Y_ABORT_UNLESS(
        FlushRequested.Exclude(Disabled).Exclude(DesiredDDisks).Empty());
    Y_ABORT_UNLESS(EraseConfirmed.Exclude(EraseRequested).Empty());
    Y_ABORT_UNLESS(EraseRequested.Exclude(WriteRequested).Empty());
    Y_ABORT_UNLESS(EraseSentBeforeAnswer.Exclude(EraseRequested).Empty());
    Y_ABORT_UNLESS(EraseSentBeforeAnswer.LogicalAnd(EraseConfirmed).Empty());

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
            Y_ABORT_UNLESS(
                FlushConfirmed.Count() >= QuorumDirectBlockGroupHostCount);
            Y_ABORT_UNLESS(GetInflightFlushes().Empty());
            break;
        case EState::PBufferDiscarded:
            Y_ABORT_UNLESS(GetInflightFlushes().Empty());
            Y_ABORT_UNLESS(PBuffersLockCount == 0);
            break;
        case EState::PBufferErased:
            // Never flushed, so the flush quorum lives in PBufferFlushed.
            Y_ABORT_UNLESS(GetInflightFlushes().Empty());
            Y_ABORT_UNLESS(CanForget());
            Y_ABORT_UNLESS(PBuffersLockCount == 0);
            break;
    }
}

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
    // A read holds the copies: UnlockPBuffer checks again.
    if (PBuffersLockCount > 0 || !CanErase()) {
        return;
    }

    if (CanForget()) {
        SetState(EState::PBufferErased);
    }
}

void TInflightInfo::MaybeQueryErase()
{
    if (PBuffersLockCount > 0 || !CanErase()) {
        return;
    }

    const auto hostsToErase =
        WriteRequested.Exclude(Disabled).Exclude(EraseRequested);
    if (!hostsToErase.Empty()) {
        ReadyQueue->Register(*this, IReadyQueue::EQueueType::Erase);
    }
}

bool TInflightInfo::CanErase() const
{
    // Either the data is on DDisk, or the write was answered with an error.
    return State == EState::PBufferFlushed || State == EState::PBufferDiscarded;
}

void TInflightInfo::RetryErase(THostIndex host)
{
    EraseSentBeforeAnswer.Reset(host);
    EraseRequested.Reset(host);
    MaybeQueryErase();
}

bool TInflightInfo::CanForget() const
{
    const auto answered = WriteAnswered.Include(Disabled);

    return WriteRequested.Exclude(answered).Empty() &&
           WriteRequested.Exclude(Disabled).Exclude(EraseConfirmed).Empty();
}

TPBufferKey TInflightInfo::GetPBufferKey() const
{
    return ReadyQueue->GetPBufferKey(*this);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
