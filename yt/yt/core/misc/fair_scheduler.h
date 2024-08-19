#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TTask>
struct IFairScheduler
    : public TRefCounted
{
    virtual void Enqueue(TTask task, const TString& user) = 0;

    virtual TTask Dequeue() = 0;

    virtual bool IsEmpty() const = 0;

    virtual void ChargeUser(const TString& user, TDuration time) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TTask>
IFairSchedulerPtr<TTask> CreateFairScheduler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define FAIR_SCHEDULER_INL_H_
#include "fair_scheduler-inl.h"
#undef FAIR_SCHEDULER_INL_H_
