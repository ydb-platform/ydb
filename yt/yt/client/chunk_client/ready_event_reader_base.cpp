#include "ready_event_reader_base.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TReadyEventReaderBase::GetReadyEvent() const
{
    WaitTimer_.StartIfNotActive();
    if (ReadyEvent_.IsSet()) {
        WaitTimer_.Stop();
    }
    return ReadyEvent_;
}

const TFuture<void>& TReadyEventReaderBase::ReadyEvent() const
{
    return ReadyEvent_;
}

void TReadyEventReaderBase::SetReadyEvent(TFuture<void> readyEvent)
{
    // We could use TTimingGuard here but we try to not prolong
    // reader lifetime for such insignificant business as timing.
    ReadyEvent_ = readyEvent.Apply(BIND([weakThis = MakeWeak(this)] {
        if (auto strongThis = weakThis.Lock()) {
            strongThis->WaitTimer_.Stop();
        }
    }));
}

TDuration TReadyEventReaderBase::GetWaitTime() const
{
    return WaitTimer_.GetElapsedTime();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
