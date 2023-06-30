#pragma once

#include <yt/cpp/mapreduce/interface/wait_proxy.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TDefaultWaitProxy
    : public IWaitProxy
{
public:
    bool WaitFuture(const ::NThreading::TFuture<void>& future, TDuration timeout) override;
    bool WaitEvent(TSystemEvent& event, TDuration timeout) override;
    bool WaitCondVar(TCondVar& condVar, TMutex& mutex, TDuration timeout) override;
    void Sleep(TDuration timeout) override;
};

class TWaitProxy {
public:
    TWaitProxy();

    static TWaitProxy* Get();

    // NB: Non thread-safe, should be called only in initialization code.
    void SetProxy(::TIntrusivePtr<IWaitProxy> proxy);

    bool WaitFuture(const ::NThreading::TFuture<void>& future);
    bool WaitFuture(const ::NThreading::TFuture<void>& future, TInstant deadLine);
    bool WaitFuture(const ::NThreading::TFuture<void>& future, TDuration timeout);

    bool WaitEventD(TSystemEvent& event, TInstant deadLine);
    bool WaitEventT(TSystemEvent& event, TDuration timeout);
    void WaitEventI(TSystemEvent& event);
    bool WaitEvent(TSystemEvent& event);

    bool WaitCondVarD(TCondVar& condVar, TMutex& m, TInstant deadLine);
    bool WaitCondVarT(TCondVar& condVar, TMutex& m, TDuration timeOut);
    void WaitCondVarI(TCondVar& condVar, TMutex& m);
    void WaitCondVar(TCondVar& condVar, TMutex& m);

    void Sleep(TDuration timeout);
    void SleepUntil(TInstant instant);

private:
    ::TIntrusivePtr<IWaitProxy> Proxy_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
