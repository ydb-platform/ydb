#include "dq_compute_actor_watermarks.h"

#include <ydb/library/services/services.pb.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>

#include <algorithm>

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

TDqComputeActorWatermarks::TDqComputeActorWatermarks(
    const TString& logPrefix
)
    : LogPrefix(logPrefix) {
}

void TDqComputeActorWatermarks::RegisterAsyncInput(ui64 inputId) {
    AsyncInputsWatermarks[inputId] = Nothing();
}

void TDqComputeActorWatermarks::UnregisterAsyncInput(ui64 inputId) {
    auto found = AsyncInputsWatermarks.erase(inputId);
    Y_ENSURE(found);
    RecalcPendingWatermark();
}

void TDqComputeActorWatermarks::RegisterInputChannel(ui64 inputId) {
    InputChannelsWatermarks[inputId] = Nothing();
}

void TDqComputeActorWatermarks::UnregisterInputChannel(ui64 inputId) {
    auto found = InputChannelsWatermarks.erase(inputId);
    Y_ENSURE(found);
    RecalcPendingWatermark();
}

bool TDqComputeActorWatermarks::NotifyAsyncInputWatermarkReceived(ui64 inputId, TInstant watermark) {
    auto it = AsyncInputsWatermarks.find(inputId);
    if (it == AsyncInputsWatermarks.end()) {
        LOG_D("Ignored watermark notification on unregistered async input " << inputId);
        return false;
    }

    LOG_T("Async input " << inputId << " notified about watermark " << watermark);

    auto& asyncInputWatermark = it->second;
    if (UpdateAndRecalcPendingWatermark(asyncInputWatermark, watermark)) {
        LOG_T("Async input " << inputId << " watermark was updated to " << watermark);
        return true;
    }

    return false;
}

bool TDqComputeActorWatermarks::NotifyInChannelWatermarkReceived(ui64 inputId, TInstant watermark) {
    auto it = InputChannelsWatermarks.find(inputId);
    if (it == InputChannelsWatermarks.end()) {
        LOG_D("Ignored watermark notification on unregistered input channel" << inputId);
        return false;
    }

    LOG_T("Input channel " << inputId << " notified about watermark " << watermark);

    auto& inputChannelWatermark = it->second;
    if (UpdateAndRecalcPendingWatermark(inputChannelWatermark, watermark)) {
        LOG_T("Input channel " << inputId << " watermark was updated to " << watermark);
        return true;
    }

    return false;
}

// Modifies and optionally recalc pending watermarks
bool TDqComputeActorWatermarks::UpdateAndRecalcPendingWatermark(TMaybe<TInstant>& storedWatermark, TInstant watermark) {
    if (storedWatermark < watermark) {
        if (PendingWatermark == std::exchange(storedWatermark, watermark)) {
            // PendingWatermark was unset; old watermark value was unset
            // -> it is possible now all channels have watermark set, needs recalc
            // PendingWatermark was set and same as old watermark value
            // -> it is possible PendingWatermark can be advanced, needs recalc
            RecalcPendingWatermark();
        } else {
            // otherwise PendingWatermark will be same
        }
        return true;
    }
    return false;
}

bool TDqComputeActorWatermarks::NotifyWatermarkWasSent(TInstant watermark) {
    auto logPrefix = [watermark] {
        return TStringBuilder() << "Output notified about watermark '" << watermark << "'";
    };

    LOG_T(logPrefix());

    if (watermark <= LastWatermark) {
        LOG_E(logPrefix() << " when LastWatermark was already forwarded to " << *LastWatermark);
        return false;
        // We will try to ignore this error, but something strange happened
    }

    if (watermark < PendingWatermark) {
        LastWatermark = PendingWatermark;
        LOG_D(logPrefix() << " before '" << PendingWatermark << "'");
        return false;
    }

    if (watermark > PendingWatermark) {
        LOG_E(logPrefix() << " when '" << PendingWatermark << "' was expected");
        // We will try to ignore this error, but something strange happened
    }
    LastWatermark = watermark;
    return true;
}

bool TDqComputeActorWatermarks::HasPendingWatermark() const {
    return PendingWatermark.Defined();
}

TMaybe<TInstant> TDqComputeActorWatermarks::GetPendingWatermark() const {
    return PendingWatermark;
}

void TDqComputeActorWatermarks::RecalcPendingWatermark() {
    if (AsyncInputsWatermarks.empty() && InputChannelsWatermarks.empty()) {
        return;
    }

    auto newWatermark = TInstant::Max();
    for (const auto& [_, watermark] : AsyncInputsWatermarks) {
        if (!watermark) {
            return;
        }

        newWatermark = std::min(newWatermark, *watermark);
    }

    for (const auto& [_, watermark] : InputChannelsWatermarks) {
        if (!watermark) {
            return;
        }

        newWatermark = std::min(newWatermark, *watermark);
    }

    if (newWatermark > LastWatermark) {
        LOG_T("New pending watermark " << newWatermark);
        PendingWatermark = newWatermark;
    }
}

void TDqComputeActorWatermarks::PopPendingWatermark() {
    LOG_T("Watermark " << *PendingWatermark << " was popped. ");
    PendingWatermark = Nothing();
}

void TDqComputeActorWatermarks::SetLogPrefix(const TString& logPrefix) {
    LogPrefix = logPrefix;
}

} // namespace NYql::NDq
