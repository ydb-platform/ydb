#pragma once

#include <library/cpp/time_provider/monotonic.h>

namespace NKikimr::NKqp {

class TSchedulerEntity;

class TSchedulerEntityHandle {
private:
    std::unique_ptr<TSchedulerEntity> Ptr;

public:
    TSchedulerEntityHandle(TSchedulerEntity*);

    TSchedulerEntityHandle();
    TSchedulerEntityHandle(TSchedulerEntityHandle&&); 

    TSchedulerEntityHandle& operator = (TSchedulerEntityHandle&&);

    bool Defined() const {
        return Ptr.get() != nullptr;
    }

    operator bool () const {
        return Defined();
    }

    TSchedulerEntity& operator*() {
        return *Ptr;
    }

    void TrackTime(TDuration time, TMonotonic now);
    void ReportBatchTime(TDuration time);

    TMaybe<TDuration> Delay(TMonotonic now);

    void MarkThrottled();
    void MarkResumed();

    double EstimateWeight(TMonotonic now, TDuration minTime);

    void Clear();

    ~TSchedulerEntityHandle();
};

}  // namespace NKikimr::NKqp
