#include "dq_compute_actor_watermarks.h"

#include <ydb/library/services/services.pb.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>

#define LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, this->LogPrefix << "Watermarks. " << s)
#define LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, this->LogPrefix << "Watermarks. " << s)
#define LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, this->LogPrefix << "Watermarks. " << s)
#define LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, this->LogPrefix << "Watermarks. " << s)
#define LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, this->LogPrefix << "Watermarks. " << s)

namespace NYql::NDq {

using namespace NActors;

TDqComputeActorWatermarks::TDqComputeActorWatermarks(const TString& logPrefix)
    : LogPrefix(logPrefix), Tracker(logPrefix) {
}

void TDqComputeActorWatermarks::RegisterInputChannel(ui64 inputId, TDuration idleDelay, TInstant systemTime) {
    RegisterInput(inputId, true, idleDelay, systemTime);
}

void TDqComputeActorWatermarks::RegisterAsyncInput(ui64 inputId, TDuration idleDelay, TInstant systemTime) {
    RegisterInput(inputId, false, idleDelay, systemTime);
}

void TDqComputeActorWatermarks::RegisterInput(ui64 inputId, bool isChannel, TDuration idleDelay, TInstant systemTime)
{
    LOG_D("Register " << (isChannel ? "channel" : "async input") << " " << inputId << ", idle delay: " << idleDelay);
    auto registered = Tracker.RegisterInput(std::make_pair(inputId, isChannel), systemTime, idleDelay);
    if (!registered) {
        LOG_E("Repeated registration " << inputId <<" " << (isChannel ? "channel" : "async input"));
    }
}

void TDqComputeActorWatermarks::UnregisterInputChannel(ui64 inputId) {
    LOG_D("Unregister input channel " << inputId);
    UnregisterInput(inputId, true);
}

void TDqComputeActorWatermarks::UnregisterAsyncInput(ui64 inputId) {
    LOG_D("Unregister async input " << inputId);
    UnregisterInput(inputId, false);
}

void TDqComputeActorWatermarks::UnregisterInput(ui64 inputId, bool isChannel) {
    auto result = Tracker.UnregisterInput(std::make_pair(inputId, isChannel));
    if (!result) {
        LOG_E("Unregistered " << (isChannel ? "input channel" : "async input") << " " << inputId << " was not found");
    }
}

bool TDqComputeActorWatermarks::NotifyInChannelWatermarkReceived(ui64 inputId, TInstant watermark, TInstant systemTime) {
    return NotifyInputWatermarkReceived(inputId, true, watermark, systemTime);
}

bool TDqComputeActorWatermarks::NotifyAsyncInputWatermarkReceived(ui64 inputId, TInstant watermark, TInstant systemTime) {
    return NotifyInputWatermarkReceived(inputId, false, watermark, systemTime);
}

bool TDqComputeActorWatermarks::NotifyInputWatermarkReceived(ui64 inputId, bool isChannel, TInstant watermark, TInstant systemTime) {
    LOG_T((isChannel ? "Channel " : "Async input ") << inputId << " notified about watermark " << watermark);
    if (MaxWatermark < watermark) {
        MaxWatermark = watermark;
    }
    auto [nextWatermark, updated] = Tracker.NotifyNewWatermark(std::make_pair(inputId, isChannel), watermark, systemTime);
    if (nextWatermark) {
        PendingWatermark = nextWatermark;
    }
    return updated;
}

bool TDqComputeActorWatermarks::NotifyWatermarkWasSent(TInstant watermark) {
    auto logPrefix = [watermark] {
        return TStringBuilder() << "Output notified about watermark '" << watermark << "'";
    };

    LOG_T(logPrefix());

    if (watermark < PendingWatermark) {
        LOG_D(logPrefix() << " before '" << PendingWatermark << "'");
        return false;
    }

    if (watermark > PendingWatermark) {
        LOG_E(logPrefix() << " when '" << PendingWatermark << "' was expected");
        // We will try to ignore this error, but something strange happened
        return false;
    }

    return true;
}

TMaybe<TInstant> TDqComputeActorWatermarks::HandleIdleness(TInstant systemTime) {
    auto nextWatermark = Tracker.HandleIdleness(systemTime);
    if (nextWatermark) {
        PendingWatermark = nextWatermark;
    }
    return nextWatermark;
}

bool TDqComputeActorWatermarks::HasPendingWatermark() const {
    return PendingWatermark.Defined();
}

TMaybe<TInstant> TDqComputeActorWatermarks::GetPendingWatermark() const {
    return PendingWatermark;
}

TMaybe<TInstant> TDqComputeActorWatermarks::GetMaxWatermark() const {
    return MaxWatermark;
}

[[nodiscard]] TMaybe<TInstant> TDqComputeActorWatermarks::GetNextIdlenessCheckAt() const {
    return Tracker.GetNextIdlenessCheckAt();
}

bool TDqComputeActorWatermarks::AddScheduledIdlenessCheck(TInstant notifyTime) {
    return Tracker.AddScheduledIdlenessCheck(notifyTime);
}

bool TDqComputeActorWatermarks::ProcessIdlenessCheck(TInstant notifyTime) {
    return Tracker.RemoveExpiredIdlenessChecks(notifyTime);
}

void TDqComputeActorWatermarks::PopPendingWatermark() {
    LOG_T("Watermark " << *PendingWatermark << " was popped. ");
    PendingWatermark = Nothing();
}

void TDqComputeActorWatermarks::SetLogPrefix(const TString& logPrefix) {
    Tracker.SetLogPrefix(logPrefix);
    LogPrefix = logPrefix;
}

} // namespace NYql::NDq
