#include "dq_watermark_generator_tracker.h"

#include <utility>

namespace NYql::NDq {

TDqWatermarkGeneratorTracker::TDqWatermarkGeneratorTracker(
    const TString& logPrefix,
    const ::NMonitoring::TDynamicCounterPtr& counters
)
    : Impl_(logPrefix, counters)
{}

void TDqWatermarkGeneratorTracker::SetSettings(
    TDuration granularity,
    bool idlePartitionsEnabled,
    TDuration lateArrivalDelay,
    TDuration idleTimeout
) {
    Granularity_ = granularity;
    IdlePartitionsEnabled_ = idlePartitionsEnabled;
    LateArrivalDelay_ = lateArrivalDelay;
    IdleTimeout_ = idleTimeout;
}

[[nodiscard]] const TString& TDqWatermarkGeneratorTracker::GetLogPrefix() const {
    return Impl_.GetLogPrefix();
}

void TDqWatermarkGeneratorTracker::SetLogPrefix(const TString& logPrefix) {
    return Impl_.SetLogPrefix(logPrefix);
}

void TDqWatermarkGeneratorTracker::SetNotifyHandler(TNotifyHandler notifyHandler) {
    NotifyHandler_ = std::move(notifyHandler);
}

bool TDqWatermarkGeneratorTracker::RegisterPartition(const TPartitionKey& partitionKey, TInstant systemTime) {
    const auto registered = Impl_.RegisterInput(
        partitionKey,
        ToNextDiscreteTime(systemTime),
        IdlePartitionsEnabled_ ? IdleTimeout_ : TDuration::Max()
    );
    PrepareIdlenessCheck(systemTime);
    return registered;
}

bool TDqWatermarkGeneratorTracker::ProcessIdlenessCheck(TInstant systemTime) {
    return Impl_.RemoveExpiredIdlenessChecks(systemTime);
}

TMaybe<TInstant> TDqWatermarkGeneratorTracker::NotifyNewPartitionTime(
    const TPartitionKey& partitionKey,
    TInstant partitionTime,
    TInstant systemTime
) {
    const auto watermark = ToDiscreteTime(partitionTime - LateArrivalDelay_);
    const auto newWatermark = Impl_.NotifyNewWatermark(
        partitionKey,
        watermark,
        ToNextDiscreteTime(systemTime)
    ).first;
    PrepareIdlenessCheck(systemTime);
    return newWatermark;
}

TMaybe<TInstant> TDqWatermarkGeneratorTracker::HandleIdleness(TInstant systemTime) {
    ProcessIdlenessCheck(systemTime);
    auto watermark = Impl_.HandleIdleness(ToDiscreteTime(systemTime));
    PrepareIdlenessCheck(systemTime);
    return watermark;
}

void TDqWatermarkGeneratorTracker::PrepareIdlenessCheck(TInstant systemTime) {
    if (!NotifyHandler_) {
        return;
    }
    if (auto nextCheck = Impl_.GetNextIdlenessCheckAt()) {
        const auto notifyTime = ToNextDiscreteTime(Max(*nextCheck, systemTime));
        if (Impl_.AddScheduledIdlenessCheck(notifyTime)) {
            NotifyHandler_(notifyTime);
        }
    }
}

TInstant TDqWatermarkGeneratorTracker::ToDiscreteTime(TInstant time) const {
    return TInstant::MicroSeconds(time.MicroSeconds() - time.MicroSeconds() % Granularity_.MicroSeconds());
}

TInstant TDqWatermarkGeneratorTracker::ToNextDiscreteTime(TInstant time) const {
    return TInstant::MicroSeconds(time.MicroSeconds() - time.MicroSeconds() % Granularity_.MicroSeconds()) + Granularity_;
}

} // namespace NYql::NDq
