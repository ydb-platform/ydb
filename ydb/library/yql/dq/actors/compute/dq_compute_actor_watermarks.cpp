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

TDqComputeActorWatermarks::TDqComputeActorWatermarks(const TString& logPrefix, const ::NMonitoring::TDynamicCounterPtr& counters)
    : LogPrefix(logPrefix)
    , Impl(logPrefix, counters)
{
}

TDqComputeActorWatermarks::TDqComputeActorWatermarks(const TDqComputeActorWatermarks& parent, bool)
    : LogPrefix(parent.LogPrefix)
    , Impl(parent.Impl, true)
    , NotifyHandler(parent.NotifyHandler)
{
}

void TDqComputeActorWatermarks::RegisterInputChannel(ui64 inputId, TDuration idleTimeout, TInstant systemTime) {
    RegisterInput(inputId, true, idleTimeout, systemTime);
}

void TDqComputeActorWatermarks::RegisterAsyncInput(ui64 inputId, TDuration idleTimeout, TInstant systemTime) {
    RegisterInput(inputId, false, idleTimeout, systemTime);
}

void TDqComputeActorWatermarks::RegisterInput(ui64 inputId, bool isChannel, TDuration idleTimeout, TInstant systemTime)
{
    LOG_D("Register " << (isChannel ? "channel" : "async input") << " " << inputId << ", idle timeout: " << idleTimeout);
    auto registered = Impl.RegisterInput(TInputKey {inputId, isChannel}, systemTime, idleTimeout);
    if (!registered) {
        LOG_E("Repeated registration " << inputId <<" " << (isChannel ? "channel" : "async input"));
    }
}

void TDqComputeActorWatermarks::UnregisterInputChannel(ui64 inputId, bool silent) {
    LOG_D("Unregister input channel " << inputId);
    UnregisterInput(inputId, true, silent);
}

void TDqComputeActorWatermarks::UnregisterAsyncInput(ui64 inputId, bool silent) {
    LOG_D("Unregister async input " << inputId);
    UnregisterInput(inputId, false, silent);
}

void TDqComputeActorWatermarks::UnregisterInput(ui64 inputId, bool isChannel, bool silent) {
    auto result = Impl.UnregisterInput(TInputKey {inputId, isChannel});
    if (!result && !silent) {
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
    auto [nextWatermark, updated] = Impl.NotifyNewWatermark(TInputKey {inputId, isChannel}, watermark, systemTime);
    if (nextWatermark) {
        PendingWatermark = nextWatermark;
    }
    if (updated && NotifyHandler) {
        NotifyHandler();
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
    auto nextWatermark = Impl.HandleIdleness(systemTime);
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

TMaybe<TInstant> TDqComputeActorWatermarks::GetNextIdlenessCheckAt() const {
    return Impl.GetNextIdlenessCheckAt();
}

bool TDqComputeActorWatermarks::AddScheduledIdlenessCheck(TInstant checkTime) {
    return Impl.AddScheduledIdlenessCheck(checkTime);
}

bool TDqComputeActorWatermarks::ProcessIdlenessCheck(TInstant notifyTime) {
    return Impl.RemoveExpiredIdlenessChecks(notifyTime);
}

void TDqComputeActorWatermarks::PopPendingWatermark() {
    LOG_T("Watermark " << *PendingWatermark << " was popped. ");
    PendingWatermark = Nothing();
}

void TDqComputeActorWatermarks::SetLogPrefix(const TString& logPrefix) {
    Impl.SetLogPrefix(logPrefix);
    LogPrefix = logPrefix;
}

void TDqComputeActorWatermarks::Out(IOutputStream& str) const {
    Impl.Out(str);
}

void TDqComputeActorWatermarks::TransferInput(TDqComputeActorWatermarks& otherTracker, ui64 inputId, bool isChannel) {
    Impl.TransferInput(otherTracker.Impl, TInputKey { inputId, isChannel });
}

TDuration TDqComputeActorWatermarks::GetMaxIdleTimeout() const {
    return Impl.GetMaxIdleTimeout();
}

void TDqComputeActorWatermarks::SetNotifyHandler(TNotifyHandler notifyHandler) {
    NotifyHandler = std::move(notifyHandler);
}
} // namespace NYql::NDq

template<>
void Out<NYql::NDq::NDqComputeActorWatermarksImpl::TInputKey>(IOutputStream& str, const NYql::NDq::NDqComputeActorWatermarksImpl::TInputKey& x) {
    str << "[" << (x.IsChannel ? "Channel " : "Input ") << x.InputId << "]";
}

template<>
void Out<NYql::NDq::TDqComputeActorWatermarks>(IOutputStream& str, const NYql::NDq::TDqComputeActorWatermarks& x) {
    x.Out(str);
}
