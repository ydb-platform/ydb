#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/system/types.h>
#include <util/string/builder.h>
#include <util/generic/set.h>
#include <algorithm>
#include <deque>

namespace NYql::NDq {

#define WATERMARK_LOG_T(X) do { if constexpr (false) TStringBuilder() << "Watermarks. " << X; } while(false)
#define WATERMARK_LOG_D(X) do { if constexpr (false) TStringBuilder() << "Watermarks. " << X; } while(false)
template <typename TInputKey>
struct TDqWatermarkTracker {
public:
    explicit TDqWatermarkTracker(const TString& logPrefix)
        : LogPrefix_(logPrefix)
    {}

    void SetLogPrefix(const TString& logPrefix) {
        LogPrefix_ = logPrefix;
    }

    [[nodiscard]] std::pair<TMaybe<TInstant>, bool> NotifyNewWatermark(
        const TInputKey& inputKey,
        TInstant watermark,
        TInstant systemTime
    ) {
        auto it = Data_.find(inputKey);
        if (it == Data_.end()) {
            return { Nothing(), false };
        }

        auto& data = it->second;

        if (data.IdleDelay != TDuration::Max() && systemTime + data.IdleDelay > data.ExpiresAt) {
            auto expiresAt = systemTime + data.IdleDelay;
            auto rec = ExpiresQueue_.extract(TExpiresQueueItem { data.ExpiresAt, it });
            if (rec.empty()) {
                WATERMARK_LOG_T("Idle time for " << it->first << " expires at " << expiresAt);
                ExpiresQueue_.emplace(expiresAt, it);
            } else {
                WATERMARK_LOG_T("Idle time for " << it->first << " expires at " << expiresAt << ", was " << rec.value().Time);
                rec.value().Time = expiresAt;
                auto inserted = ExpiresQueue_.insert(std::move(rec)).inserted;
                Y_DEBUG_ABORT_UNLESS(inserted);
            }
            data.ExpiresAt = expiresAt;
        }

        bool updated = false;
        if (data.Watermark < watermark) {
            updated = true;
            auto rec = WatermarksQueue_.extract(TWatermarksQueueItem { data.Watermark, it });
            if (rec.empty()) {
                WATERMARK_LOG_T("Unidle " << it->first << " got " << watermark << " > " << data.Watermark);
                WatermarksQueue_.emplace(watermark, it);
            } else {
                WATERMARK_LOG_T("Update " << it->first << " watermark from " << rec.value().Time << " to " << watermark);
                rec.value().Time = watermark;
                auto inserted = WatermarksQueue_.insert(std::move(rec)).inserted;
                Y_DEBUG_ABORT_UNLESS(inserted);
            }
            data.Watermark = watermark;
        } else if (data.IdleDelay != TDuration::Max()) {
            auto [_, inserted] = WatermarksQueue_.emplace(data.Watermark, it);
            if (inserted) {
                //updated = true;
                WATERMARK_LOG_T("Unidle " << it->first << " got " << watermark << " <= " << data.Watermark);
            }
        } else {
            Y_DEBUG_ABORT_UNLESS(WatermarksQueue_.contains(TWatermarksQueueItem { data.Watermark, it }));
        }
        return { updated ? RecalcWatermark() : Nothing(), updated } ;
    }

    bool RegisterInput(const TInputKey& inputKey, TInstant systemTime, TDuration idleDelay) {
        auto [it, inserted] = Data_.try_emplace(inputKey);
        if (!inserted) {
            return false;
        }
        auto& data = it->second;
        // data.Watermark = Nothing()
        data.IdleDelay = idleDelay;
        if (idleDelay != TDuration::Max()) {
            data.ExpiresAt = systemTime + idleDelay;
            auto [_, inserted] = ExpiresQueue_.emplace(data.ExpiresAt, it);
            Y_DEBUG_ABORT_UNLESS(inserted);
        }
        {
            auto [_, inserted] = WatermarksQueue_.emplace(Nothing(), it);
            Y_DEBUG_ABORT_UNLESS(inserted);
        }
        return true;
    }

    bool UnregisterInput(const TInputKey& inputKey) {
        auto it = Data_.find(inputKey);
        if (it == Data_.end()) {
            return false;
        }
        auto& data = it->second;
        WatermarksQueue_.erase(TWatermarksQueueItem { data.Watermark, it });
        // note: erases nothing if input was idle
        if (data.IdleDelay != TDuration::Max()) {
            ExpiresQueue_.erase(TExpiresQueueItem { data.ExpiresAt, it });
            // note: erases nothing if input was idle
        }
        Data_.erase(it);
        return true;
    }

    [[nodiscard]] TMaybe<TInstant> HandleIdleness(TInstant systemTime) {
        if (ExpiresQueue_.empty()) {
            return Nothing();
        }

        for (auto it = ExpiresQueue_.begin(); it != ExpiresQueue_.end() && it->Time <= systemTime; ) {
           auto& [key, data] = *it->Iterator;
           Y_DEBUG_ABORT_UNLESS (data.IdleDelay != TDuration::Max());
           WATERMARK_LOG_T("Mark " << key << " idle: " << it->Time <<  " >= " << systemTime);
           auto removed = WatermarksQueue_.erase(TWatermarksQueueItem { data.Watermark, it->Iterator } );
           Y_DEBUG_ABORT_UNLESS(removed); // any partition in ExpiresQueue_ must have matching record in WatermarksQueue_
           it = ExpiresQueue_.erase(it);
        }

        return RecalcWatermark();
    }

    [[nodiscard]] TMaybe<TInstant> GetNextIdlenessCheckAt() const {
        if (ExpiresQueue_.empty()) {
            return Nothing();
        }
        return ExpiresQueue_.cbegin()->Time;
    }

    TMaybe<TDuration> GetWatermarkDiscrepancy() const {
        auto first = WatermarksQueue_.cbegin();
        auto last = WatermarksQueue_.cend();
        if (first == last || !first->Time) {
            return Nothing();
        }
        --last;
        Y_DEBUG_ABORT_UNLESS(last->Time); // when first watermark is defined, last watermark must be defined too
        return *last->Time - *first->Time;
    }

    // return true if any checks was expired
    bool RemoveExpiredIdlenessChecks(TInstant notifyTime) {
        bool removedAny = false;
        while (HasEarlierIdlenessChecks(notifyTime)) {
            InflyIdlenessChecks_.pop_front();
            removedAny = true;
        }
        return removedAny;
    }

    // return true if check needs to be scheduled
    bool AddScheduledIdlenessCheck(TInstant notifyTime) {
        if (HasEarlierIdlenessChecks(notifyTime)) {
            // There are already idleness check scheduled at this or earlier time;
            // Try to minimize infly checks
            return false;
        }
        InflyIdlenessChecks_.push_front(notifyTime);
        WATERMARK_LOG_T("Next idleness check scheduled at " << notifyTime);
        return true;
    }

private:
    TMaybe<TInstant> RecalcWatermark() {
        if (WatermarksQueue_.empty()) {
            return Nothing();
        }

        if (auto nextWatermark = WatermarksQueue_.begin()->Time; Watermark_ < nextWatermark) {
            return Watermark_ = nextWatermark;
        } else if (nextWatermark < Watermark_) {
            WATERMARK_LOG_D("Watermark goes backward (some events may be dropped) " << nextWatermark << '<' << Watermark_);
        }

        return Nothing();
    }

private:
    bool HasEarlierIdlenessChecks(TInstant notifyTime) {
        return !InflyIdlenessChecks_.empty() && InflyIdlenessChecks_.front() <= notifyTime;
    }

    struct TInputState {
        TDuration IdleDelay = TDuration::Max(); // expiration delay or ::Max() if disabled
        TInstant ExpiresAt; // input will be marked idle and ignored at this time
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
    using TData = THashMap<TInputKey, TInputState>;
    using TExpiresQueueItem = TTimeState<TInstant, TData>;
    using TWatermarksQueueItem = TTimeState<TMaybe<TInstant>, TData>;
    TData Data_;
    TSet<TExpiresQueueItem> ExpiresQueue_;
    // tracked for idle-aware partitions
    // item->Iterator is a valid iterator into Data_
    // item->Iterator->ExpiresAt == item->Time
    // item->Iterator->IdleDelay != TDuration::Max()
    TSet<TWatermarksQueueItem> WatermarksQueue_;
    // either item->Iterator->IdleDelay == TDuration::Max(), or matching item must be present in ExpiresQueue_
    // item->Iterator is a valid iterator into Data_
    // item->Iterator->Watermark == item->Time
    // TODO: replace both TSet with custom binary heap
    TMaybe<TInstant> Watermark_;
    TString LogPrefix_;
    std::deque<TInstant> InflyIdlenessChecks_;
};
#undef WATERMARK_LOG_T
#undef WATERMARK_LOG_D

}
