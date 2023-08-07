#pragma once

#include "public.h"
#include "invoker.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TInvokerWrapper
    : public virtual IInvoker
{
public:
    void Invoke(TClosure callback) override;

    void Invoke(TMutableRange<TClosure> callbacks) override;

    NThreading::TThreadId GetThreadId() const override;
    bool CheckAffinity(const IInvokerPtr& invoker) const override;
    bool IsSerialized() const override;
    void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver) override;

protected:
    explicit TInvokerWrapper(IInvokerPtr underlyingInvoker);

    IInvokerPtr UnderlyingInvoker_;
};

////////////////////////////////////////////////////////////////////////////////

//! A helper base which makes callbacks track their invocation time and profile their wait time.
class TInvokerProfileWrapper
{
public:
    /*!
    *  #registry defines a profile registry where sensors data is stored.
    *  #invokerFamily defines a family of invokers, e.g. "serialized" or "prioriized" and appears in sensor's name.
    *  #tagSet defines a particular instance of the invoker and appears in sensor's tags.
    */
    TInvokerProfileWrapper(NProfiling::IRegistryImplPtr registry, const TString& invokerFamily, const NProfiling::TTagSet& tagSet);

protected:
    TClosure WrapCallback(TClosure callback);

private:
    NProfiling::TEventTimer WaitTimer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
