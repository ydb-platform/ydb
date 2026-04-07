#include "ready_event_reader_base.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TReadyEventReaderBase::GetReadyEvent() const
{
    auto guard = Guard(SpinLock_);
    if (!ReadyEvent_.IsSet()) {
        WaitTimer_.StartIfNotActive();
    }
    return ReadyEvent_;
}

TFuture<void> TReadyEventReaderBase::InternalGetReadyEvent() const
{
    auto guard = Guard(SpinLock_);
    return ReadyEvent_;
}

bool TReadyEventReaderBase::IsReadyEventSetAndOK() const
{
    auto guard = Guard(SpinLock_);
    return ReadyEvent_.IsSet() && ReadyEvent_.GetOrCrash().IsOK();
}

void TReadyEventReaderBase::SetReadyEvent(TFuture<void> readyEvent)
{
    auto newReadyEvent = readyEvent.Apply(BIND([this, weakThis = MakeWeak(this)] {
        if (auto this_ = weakThis.Lock()) {
            auto guard = Guard(SpinLock_);
            WaitTimer_.Stop();
        }
    }));

    {
        auto guard = Guard(SpinLock_);
        std::swap(ReadyEvent_, newReadyEvent);
    }
}

TDuration TReadyEventReaderBase::GetWaitTime() const
{
    auto guard = Guard(SpinLock_);
    return WaitTimer_.GetElapsedTime();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
