#pragma once

#include "fwd.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NKqp::NScheduler {

// The proxy-object between any schedulable actor and the scheduler itself

struct TSchedulableTask : public std::enable_shared_from_this<TSchedulableTask> {
    explicit TSchedulableTask(const NHdrf::NDynamic::TQueryPtr& query);
    ~TSchedulableTask();

    void RegisterForResume(const NActors::TActorId& actorId);
    void Resume();

    using TResumeEventType = NActors::TEvents::TEvWakeup;
    static inline bool IsResumeEvent(const TResumeEventType::TPtr& ev) {
        return ev->Get()->Tag == TAG_WAKEUP_RESUME;
    }
    static inline auto GetResumeEvent() {
        return std::make_unique<TResumeEventType>(TAG_WAKEUP_RESUME);
    }

    bool TryIncreaseUsage();
    void IncreaseUsage();
    void DecreaseUsage(const TDuration& burstUsage, bool forcedResume);

    // Returns parent pool's 'fair-share' - 'usage'
    size_t GetSpareUsage() const;

    // Account extra usage which doesn't affect scheduling
    void IncreaseExtraUsage();
    void DecreaseExtraUsage(const TDuration& burstUsage);

    void IncreaseBurstThrottle(const TDuration& burstThrottle);
    void IncreaseThrottle();
    void DecreaseThrottle();

    const NHdrf::NDynamic::TQueryPtr Query; // TODO: should be private

private:
    static constexpr ui64 TAG_WAKEUP_RESUME = 201; // TODO: why this value for magic number?

    std::optional<TSchedulableTaskList::iterator> Iterator; // TODO: improve iterator to not allow access to adjacent values in list.
    NActors::TActorId ActorId;
};

} // namespace NKikimr::NKqp::NScheduler
