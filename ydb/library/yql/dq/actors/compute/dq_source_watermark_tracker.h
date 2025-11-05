#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/system/types.h>
#include <util/string/builder.h>

namespace NYql::NDq {

#define SRC_WM_LOG_T(X) do { if constexpr (false) TStringBuilder() << X; } while(false)
#define SRC_WM_LOG_D(X) do { if constexpr (false) TStringBuilder() << X; } while(false)
template <typename TPartitionKey>
struct TDqSourceWatermarkTracker {
public:
    TDqSourceWatermarkTracker(
        TDuration granularity,
        bool idlePartitionsEnabled,
        TDuration lateArrivalDelay,
        TDuration idleDelay,
        TInstant systemTime)
        : Granularity_(granularity)
        , IdlePartitionsEnabled_(idlePartitionsEnabled)
        , LateArrivalDelay_(lateArrivalDelay)
        , IdleDelay_(idleDelay)
        , LastTimeNotifiedAt_(systemTime)
    {}

    [[nodiscard]] TMaybe<TInstant> NotifyNewPartitionTime(
        const TPartitionKey& partitionKey,
        TInstant partitionTime,
        TInstant systemTime
    ) {
        auto it = Data_.find(partitionKey);
        Y_ENSURE(it != Data_.end());

        auto& data = it->second;
        data.Time = partitionTime;

        if (IdlePartitionsEnabled_ && systemTime > data.TimeNotifiedAt) {
            auto rec = ArrivalQueue_.extract(TArrivalQueueItem { data.TimeNotifiedAt, it });
            if (rec.empty()) {
                SRC_WM_LOG_T("Added arrival time for " << it->first << " to " << systemTime);
                ArrivalQueue_.emplace(systemTime, it);
            } else {
                SRC_WM_LOG_T("Update arrival time for " << it->first << " from " << rec.value().Time << " to " << systemTime);
                rec.value().Time = systemTime;
                auto inserted = ArrivalQueue_.insert(std::move(rec)).inserted;
                Y_DEBUG_ABORT_UNLESS(inserted);
            }
            data.TimeNotifiedAt = systemTime;
        }

        const auto watermark = ToDiscreteTime(partitionTime - LateArrivalDelay_);
        if (data.Watermark < watermark) {
            auto rec = WatermarksQueue_.extract(TWatermarksQueueItem { data.Watermark, it });
            if (rec.empty()) {
                SRC_WM_LOG_T("Unidle partition " << it->first << " got " << watermark << " > " << data.Watermark);
                WatermarksQueue_.emplace(watermark, it);
            } else {
                SRC_WM_LOG_T("Update partition " << it->first << " watermark from " << rec.value().Time << " to " << watermark);
                rec.value().Time = watermark;
                auto inserted = WatermarksQueue_.insert(std::move(rec)).inserted;
                Y_DEBUG_ABORT_UNLESS(inserted);
            }
            data.Watermark = watermark;
        } else if (IdlePartitionsEnabled_) {
            auto [_, inserted] = WatermarksQueue_.emplace(data.Watermark, it);
            if (inserted) {
                SRC_WM_LOG_T("Unidle partition " << it->first << " got " << watermark << " <= " << data.Watermark);
            }
        } else {
            Y_DEBUG_ABORT_UNLESS(WatermarksQueue_.contains(TWatermarksQueueItem { data.Watermark, it }));
        }

        return RecalcWatermark();
    }

    bool RegisterPartition(const TPartitionKey& partitionKey, TInstant systemTime) {
        auto [it, inserted] = Data_.try_emplace(partitionKey);
        if (!inserted) {
            return false;
        }
        auto& data = it->second;
        // data.Time = undefined
        // data.Watermark = Nothing()
        data.TimeNotifiedAt = systemTime;
        if (IdlePartitionsEnabled_) {
            auto [_, inserted] = ArrivalQueue_.emplace(systemTime, it);
            Y_DEBUG_ABORT_UNLESS(inserted);
        }
        {
            auto [_, inserted] = WatermarksQueue_.emplace(Nothing(), it);
            Y_DEBUG_ABORT_UNLESS(inserted);
        }
        return true;
    }

    [[nodiscard]] TMaybe<TInstant> HandleIdleness(TInstant systemTime) {
        if (!IdlePartitionsEnabled_ || !ShouldCheckIdlenessNow(systemTime)) {
            return Nothing();
        }

        for (;;) {
           auto it = ArrivalQueue_.begin();
           if (it == ArrivalQueue_.end()) {
               break;
           }
           if (it->Time + IdleDelay_ >= systemTime) {
               break;
           }
           SRC_WM_LOG_T("Mark partition " << it->Iterator->first << " idle " << it->Time << '+' << IdleDelay_ << ">=" << systemTime);
           auto removed = WatermarksQueue_.erase(TWatermarksQueueItem {
                   it->Iterator->second.Watermark,
                   it->Iterator
           } );
           Y_DEBUG_ABORT_UNLESS(removed); // any partition in ArrivalQueue_ must have matching record in WatermarksQueue_
           ArrivalQueue_.erase(it);
        }

        return RecalcWatermark();
    }

    [[nodiscard]] TMaybe<TInstant> GetNextIdlenessCheckAt(TInstant systemTime) {
        return IdlePartitionsEnabled_
            ? ToDiscreteTime(systemTime + Granularity_)
            : TMaybe<TInstant>();
    }

private:
    struct TPartitionState {
        TInstant Time;  // partition time, notified outside
        TInstant TimeNotifiedAt; // system time when notification was received
        TMaybe<TInstant> Watermark;
    };

    template <typename TTimeType, typename TMapType>
    struct TTimeState {
        using TDataIterator = typename TMapType::iterator;
        TTimeType Time;
        TDataIterator Iterator;
        bool operator< (const TTimeState& other) const noexcept {
            // assumes iterator returns stable references
            return Time < other.Time ||
                ( Time == other.Time &&
                  (uintptr_t)&*Iterator < (uintptr_t)&*other.Iterator );
            // 1) there are no `operator<` for hashmap iterator (ForwardIterator);
            // 2) comparing pointers belonging to different allocations is UB;
            // Hence, we use node addresses
            // (we don't actually care about actual ordering of Iterator's, the only
            // requirement it would be stable, unique and passes strict weak ordering;
            // node addresses comparison qualifies)
        }
    };
private:
    TInstant ToDiscreteTime(TInstant time) const {
        return TInstant::MicroSeconds(time.MicroSeconds() - time.MicroSeconds() % Granularity_.MicroSeconds());
    }

    bool ShouldCheckIdlenessNow(TInstant systemTime) {
        const auto discreteSystemTime = ToDiscreteTime(systemTime);
        if (discreteSystemTime < NextIdlenessCheckAt_) {
            return false;
        }

        NextIdlenessCheckAt_ = discreteSystemTime + Granularity_;
        return true;
    }

    TMaybe<TInstant> RecalcWatermark() {
        if (WatermarksQueue_.empty()) {
            return Nothing();
        }

        if (auto nextWatermark = WatermarksQueue_.begin()->Time; Watermark_ < nextWatermark) {
            return Watermark_ = nextWatermark;
        } else if (nextWatermark < Watermark_) {
            SRC_WM_LOG_D("Watermark goes backward (some events may be dropped) " << nextWatermark << '<' << Watermark_);
        }

        return Nothing();
    }

private:
    const TDuration Granularity_;
    const bool IdlePartitionsEnabled_;
    const TDuration LateArrivalDelay_;
    const TDuration IdleDelay_;

    THashMap<TPartitionKey, TPartitionState> Data_;
    using TArrivalQueueItem = TTimeState<TInstant, decltype(Data_)>;
    TSet<TArrivalQueueItem> ArrivalQueue_; // tracked only when IdlePartitionsEnabled_
    // TODO: replace with heap
    using TWatermarksQueueItem = TTimeState<TMaybe<TInstant>, decltype(Data_)>;
    TSet<TWatermarksQueueItem> WatermarksQueue_;
    // TODO: replace with heap
    TMaybe<TInstant> Watermark_;
    TInstant LastTimeNotifiedAt_; // last system time when tracker received notification for any partition
    TMaybe<TInstant> NextIdlenessCheckAt_;
};
#undef SRC_WM_LOG_T
#undef SRC_WM_LOG_D

}
