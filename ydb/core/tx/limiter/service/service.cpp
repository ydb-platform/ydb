#include "service.h"

namespace NKikimr::NLimiter {

TLimiterActor::TLimiterActor(const TConfig& config, const TString& limiterName, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseCounters)
    : LimiterName(limiterName)
    , Config(config)
    , Counters(LimiterName, baseCounters)
{
    Counters.InProgressLimit->Set(Config.GetLimit());
}

void TLimiterActor::HandleMain(TEvExternal::TEvAskResource::TPtr& ev) {
    const auto now = TMonotonic::Now();
    if (RequestsInFlight.empty() || (RequestsQueue.empty() && VolumeInFlight + ev->Get()->GetRequest()->GetVolume() <= Config.GetLimit())) {
        VolumeInFlight += ev->Get()->GetRequest()->GetVolume();
        RequestsInFlight.emplace_back(now, ev->Get()->GetRequest()->GetVolume());
        if (RequestsInFlight.size() == 1) {
            Schedule(now + Config.GetPeriod(), new NActors::TEvents::TEvWakeup());
        }
        ev->Get()->GetRequest()->OnResourceAllocated();
        Counters.InProgressStart->Inc();
    } else {
        RequestsQueue.emplace_back(now, ev->Get()->GetRequest());
        VolumeInWaiting += ev->Get()->GetRequest()->GetVolume();
    }
    Counters.InProgressCount->Set(RequestsInFlight.size());
    Counters.InProgressVolume->Set(VolumeInFlight);
    Counters.WaitingQueueCount->Set(RequestsQueue.size());
    Counters.WaitingQueueVolume->Set(VolumeInWaiting);
}

void TLimiterActor::HandleMain(NActors::TEvents::TEvWakeup::TPtr& /*ev*/) {
    const auto now = TMonotonic::Now();
    AFL_VERIFY(RequestsInFlight.size());
    while (RequestsInFlight.size() && RequestsInFlight.front().GetInstant() + Config.GetPeriod() <= now) {
        AFL_VERIFY(RequestsInFlight.front().GetVolume() <= VolumeInFlight);
        VolumeInFlight = VolumeInFlight - RequestsInFlight.front().GetVolume();
        RequestsInFlight.pop_front();
    }
    if (RequestsInFlight.empty()) {
        AFL_VERIFY(!VolumeInFlight);
    }
    while (RequestsQueue.size() && (RequestsInFlight.empty() || VolumeInFlight + RequestsQueue.front().GetRequest()->GetVolume() <= Config.GetLimit())) {
        Counters.WaitingHistogram->Collect((i64)(now - RequestsQueue.front().GetInstant()).MilliSeconds(), 1);
        VolumeInFlight += RequestsQueue.front().GetRequest()->GetVolume();
        RequestsInFlight.emplace_back(now, RequestsQueue.front().GetRequest()->GetVolume());
        RequestsQueue.front().GetRequest()->OnResourceAllocated();
        AFL_VERIFY(VolumeInWaiting >= RequestsQueue.front().GetRequest()->GetVolume());
        VolumeInWaiting -= RequestsQueue.front().GetRequest()->GetVolume();
        RequestsQueue.pop_front();
        Counters.InProgressStart->Inc();
    }
    if (RequestsInFlight.size()) {
        Schedule(RequestsInFlight.front().GetInstant() + Config.GetPeriod(), new NActors::TEvents::TEvWakeup());
    }
    Counters.InProgressCount->Set(RequestsInFlight.size());
    Counters.InProgressVolume->Set(VolumeInFlight);
    Counters.WaitingQueueCount->Set(RequestsQueue.size());
    Counters.WaitingQueueVolume->Set(VolumeInWaiting);
}

}
