#include "dq_compute_actor_watermarks.h"

#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>

#include <algorithm>

#define LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "SelfId: " << SelfId << ", TxId: " << TxId << ", task: " << TaskId << ". Watermarks. " << s)
#define LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "SelfId: " << SelfId << ", TxId: " << TxId << ", task: " << TaskId << ". Watermarks. " << s)
#define LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "SelfId: " << SelfId << ", TxId: " << TxId << ", task: " << TaskId << ". Watermarks. " << s)
#define LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "SelfId: " << SelfId << ", TxId: " << TxId << ", task: " << TaskId << ". Watermarks. " << s)
#define LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "SelfId: " << SelfId << ", TxId: " << TxId << ", task: " << TaskId << ". Watermarks. " << s)

namespace NYql::NDq {

using namespace NActors;

TDqComputeActorWatermarks::TDqComputeActorWatermarks(
    NActors::TActorIdentity selfId,
    const TTxId txId,
    ui64 taskId
)
    : SelfId(selfId)
    , TxId(txId)
    , TaskId(taskId) {
}

void TDqComputeActorWatermarks::RegisterAsyncInput(ui64 inputId) {
    AsyncInputsWatermarks[inputId] = Nothing();
}

void TDqComputeActorWatermarks::RegisterInputChannel(ui64 inputId) {
    InputChannelsWatermarks[inputId] = Nothing();
}

void TDqComputeActorWatermarks::RegisterOutputChannel(ui64 outputId) {
    OutputChannelsWatermarks[outputId] = Nothing();
}

bool TDqComputeActorWatermarks::HasOutputChannels() const {
    return !OutputChannelsWatermarks.empty();
}

bool TDqComputeActorWatermarks::NotifyAsyncInputWatermarkReceived(ui64 inputId, TInstant watermark) {
    LOG_T("Async input " << inputId << " notified about watermark " << watermark);

    auto& asyncInputWatermark = AsyncInputsWatermarks[inputId];
    if (!asyncInputWatermark || *asyncInputWatermark < watermark) {
        LOG_T("Async input " << inputId << " watermark was updated to " << watermark);
        asyncInputWatermark = watermark;
        RecalcPendingWatermark();
        return true;
    }

    return false;
}

bool TDqComputeActorWatermarks::NotifyInChannelWatermarkReceived(ui64 inputId, TInstant watermark) {
    LOG_T("Input channel " << inputId << " notified about watermark " << watermark);

    auto& inputChannelWatermark = InputChannelsWatermarks[inputId];
    if (!inputChannelWatermark || *inputChannelWatermark < watermark) {
        LOG_T("Input channel " << inputId << " watermark was updated to " << watermark);
        inputChannelWatermark = watermark;
        RecalcPendingWatermark();
        return true;
    }

    return false;
}

bool TDqComputeActorWatermarks::NotifyOutputChannelWatermarkSent(ui64 outputId, TInstant watermark) {
    auto logPrefix = TStringBuilder() << "Output channel "
        << outputId << " notified about watermark '" << watermark << "'";

    LOG_T(logPrefix);

    if (watermark <= LastWatermark) {
        LOG_E(logPrefix << "' when LastWatermark was already forwarded to " << *LastWatermark);
        // We will try to ignore this error, but something strange happened
    }

    if (watermark != PendingWatermark) {
        LOG_E(logPrefix << " when '" << PendingWatermark << "' was expected");
        // We will try to ignore this error, but something strange happened
    }

    OutputChannelsWatermarks[outputId] = watermark;

    return MaybePopPendingWatermark();
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

    if (!LastWatermark || newWatermark != LastWatermark) {
        LOG_T("New pending watermark " << newWatermark);
        PendingWatermark = newWatermark;
    }
}

bool TDqComputeActorWatermarks::MaybePopPendingWatermark() {
    if (OutputChannelsWatermarks.empty()) {
        return true;
    }

    if (!PendingWatermark) {
        LOG_E("There is no pending watermark, but pop was called");
        // We will try to ignore this error, but something strange happened
        return true;
    }

    auto outWatermark = TInstant::Max();
    for (const auto& [_, watermark] : OutputChannelsWatermarks) {
        if (!watermark) {
            return false;
        }

        outWatermark = std::min(outWatermark, *watermark);
    }

    if (outWatermark >= *PendingWatermark) {
        LastWatermark = PendingWatermark;
        PopPendingWatermark();
        return true;
    }

    return false;
}

void TDqComputeActorWatermarks::PopPendingWatermark() {
    LOG_T("Watermark " << *PendingWatermark << " was popped. ");
    PendingWatermark = Nothing();
}

} // namespace NYql::NDq
