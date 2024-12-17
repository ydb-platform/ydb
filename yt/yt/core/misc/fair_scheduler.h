#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TTask>
struct IFairScheduler
    : public TRefCounted
{
    virtual void Enqueue(TTask task, const std::string& user) = 0;
    virtual std::optional<TTask> TryDequeue() = 0;

    virtual void ChargeUser(const std::string& user, TDuration time) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TTask>
IFairSchedulerPtr<TTask> CreateFairScheduler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define FAIR_SCHEDULER_INL_H_
#include "fair_scheduler-inl.h"
#undef FAIR_SCHEDULER_INL_H_
