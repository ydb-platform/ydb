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

TDqComputeActorWatermarks::TDqComputeActorWatermarks(const TString& logPrefix)
    : LogPrefix(logPrefix) {
}

void TDqComputeActorWatermarks::RegisterInputChannel(ui64 inputId, TDuration idleDelay) {
    RegisterInput(inputId, true, idleDelay);
}

void TDqComputeActorWatermarks::UnregisterInputChannel(ui64 inputId) {
    UnregisterInput(inputId, true);
}

void TDqComputeActorWatermarks::UnregisterAsyncInput(ui64 inputId) {
    UnregisterInput(inputId, false);
}

void TDqComputeActorWatermarks::RegisterAsyncInput(ui64 inputId, TDuration idleDelay) {
    RegisterInput(inputId, false, idleDelay);
}

bool TDqComputeActorWatermarks::NotifyInChannelWatermarkReceived(ui64 inputId, TInstant watermark) {
    return NotifyInputWatermarkReceived(inputId, true, watermark);
}

bool TDqComputeActorWatermarks::NotifyAsyncInputWatermarkReceived(ui64 inputId, TInstant watermark) {
    return NotifyInputWatermarkReceived(inputId, false, watermark);
}

// TODO there are large overlap between SourceTracker and CA Watermark tracker, common code should be moved somewhere and reused
void TDqComputeActorWatermarks::RegisterInput(ui64 inputId, bool isChannel, TDuration idleDelay)
{
    LOG_D("Register " << (isChannel ? "channel" : "async input") << " " << inputId << " for watermark with idle delay " << idleDelay);
    auto [it, inserted] = InputWatermarks.try_emplace(std::make_pair(inputId, isChannel));
    if (!inserted) {
        LOG_D("Repeated registration " << inputId <<" " << (isChannel ? "channel" : "async input"));
        return;
    }
    auto& data = it->second;
    data.Watermark = Nothing();
    data.IdleDelay = idleDelay;
    if (idleDelay != TDuration::Max()) {
        data.ExpiresAt = TInstant::Now() + idleDelay;
        auto [_, inserted] = ExpiresQueue.emplace(data.ExpiresAt, it);
        Y_DEBUG_ABORT_UNLESS(inserted);
    }
    {
        auto [_, inserted] = WatermarksQueue.emplace(Nothing(), it);
        Y_DEBUG_ABORT_UNLESS(inserted);
    }
}

void TDqComputeActorWatermarks::UnregisterInput(ui64 inputId, bool isChannel) {
    auto it = InputWatermarks.find(std::make_pair(inputId, isChannel));
    Y_ENSURE(it != InputWatermarks.end());
    auto& data = it->second;
    WatermarksQueue.erase(TWatermarksQueueItem { data.Watermark, it });
    if (it->second.IdleDelay != TDuration::Max()) {
        ExpiresQueue.erase(TExpiresQueueItem { data.ExpiresAt, it });
    }
    InputWatermarks.erase(it);
}

bool TDqComputeActorWatermarks::NotifyInputWatermarkReceived(ui64 inputId, bool isChannel, TInstant watermark) {
    auto it = InputWatermarks.find(std::make_pair(inputId, isChannel));
    if (it == InputWatermarks.end()) {
        LOG_D("Ignored watermark notification on unregistered async input " << inputId);
        return false;
    }

    LOG_T((isChannel ? "Channel " : "Async input ") << inputId << " notified about watermark " << watermark);

    if (MaxWatermark < watermark) {
        MaxWatermark = watermark;
    }
    auto& data = it->second;
    TInstant systemTime = TInstant::Now();
    if (data.IdleDelay != TDuration::Max() && systemTime + data.IdleDelay > data.ExpiresAt) {
        auto expiresAt = systemTime + data.IdleDelay;
        auto rec = ExpiresQueue.extract(TExpiresQueueItem { data.ExpiresAt, it });
        if (rec.empty()) {
            ExpiresQueue.emplace(expiresAt, it);
            LOG_T("Unidle untill " << expiresAt);
        } else {
            rec.value().Time = expiresAt;
            auto inserted = ExpiresQueue.insert(std::move(rec)).inserted;
            LOG_T("Bump idle from " << data.ExpiresAt << " to " << expiresAt);
            Y_DEBUG_ABORT_UNLESS(inserted);
        }
        data.ExpiresAt = expiresAt;
    }
    bool updated = false;
    if (data.Watermark < watermark) {
        updated = true;
        auto rec = WatermarksQueue.extract(TWatermarksQueueItem { data.Watermark, it });
        if (rec.empty()) {
            WatermarksQueue.emplace(watermark, it);
        } else {
            rec.value().Time = watermark;
            auto inserted = WatermarksQueue.insert(std::move(rec)).inserted;
            Y_DEBUG_ABORT_UNLESS(inserted);
        }
        data.Watermark = watermark;
    } else if (data.IdleDelay != TDuration::Max()) {
        auto [_, inserted] = WatermarksQueue.emplace(data.Watermark, it);
        if (inserted) {
        }
    } else {
        Y_DEBUG_ABORT_UNLESS(WatermarksQueue.contains(TWatermarksQueueItem { data.Watermark, it }));
    }

    if (WatermarksQueue.empty()) {
        return updated;
    }

    if (auto nextWatermark = WatermarksQueue.begin()->Time; LastWatermark < nextWatermark) {
        PendingWatermark = LastWatermark = nextWatermark;
        return updated;
    } else if (nextWatermark < LastWatermark) {
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
    for (;;) {
        auto it = ExpiresQueue.begin();
        if (it == ExpiresQueue.end()) {
            break;
        }
        auto& data = it->Iterator->second;
        Y_DEBUG_ABORT_UNLESS (data.IdleDelay != TDuration::Max());
        if (it->Time >= systemTime) {
            break;
        }
        LOG_T("Mark " << (it->Iterator->first.second ? "channel" : "async input") << ' ' << it->Iterator->first.first << " idle " << it->Time << '<' << systemTime << '+' << data.IdleDelay );
        auto removed = WatermarksQueue.erase(TWatermarksQueueItem {
                it->Iterator->second.Watermark,
                it->Iterator
                } );
        Y_DEBUG_ABORT_UNLESS(removed); // any channel/input in ExpiresQueue must have matching record in WatermarksQueue
        ExpiresQueue.erase(it);
    }

    if (WatermarksQueue.empty()) {
        return Nothing();
    }
    if (auto nextWatermark = WatermarksQueue.begin()->Time; LastWatermark < nextWatermark) {
        LOG_T("Idleness generates watermark " << nextWatermark);
        return PendingWatermark = LastWatermark = nextWatermark;
    } else if (nextWatermark < LastWatermark) {
        LOG_D("Watermark goes backward (partition was added or unidled) " << nextWatermark << '<' << LastWatermark);
    }
    return Nothing();
}

bool TDqComputeActorWatermarks::HasPendingWatermark() const {
    return PendingWatermark.Defined();
}

TMaybe<TInstant> TDqComputeActorWatermarks::GetPendingWatermark() const {
    return PendingWatermark;
}

TDuration TDqComputeActorWatermarks::GetWatermarkDiscrepancy() const {
    return *MaxWatermark - *PendingWatermark;
}

[[nodiscard]] TMaybe<TInstant> TDqComputeActorWatermarks::GetNextIdlenessCheckAt() const {
    if (ExpiresQueue.empty()) {
        return Nothing();
    }
    Y_DEBUG_ABORT_UNLESS(ExpiresQueue.cbegin()->Iterator->second.IdleDelay != TDuration::Max());
    return ExpiresQueue.cbegin()->Time;
}

void TDqComputeActorWatermarks::PopPendingWatermark() {
    LOG_T("Watermark " << *PendingWatermark << " was popped. ");
    PendingWatermark = Nothing();
}

void TDqComputeActorWatermarks::SetLogPrefix(const TString& logPrefix) {
    LogPrefix = logPrefix;
}

} // namespace NYql::NDq
