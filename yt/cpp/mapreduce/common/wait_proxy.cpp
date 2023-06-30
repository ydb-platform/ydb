#include "wait_proxy.h"


#include <library/cpp/threading/future/future.h>

#include <util/system/event.h>
#include <util/system/condvar.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

bool TDefaultWaitProxy::WaitFuture(const NThreading::TFuture<void>& future, TDuration timeout)
{
    return future.Wait(timeout);
}

bool TDefaultWaitProxy::WaitEvent(TSystemEvent& event, TDuration timeout)
{
    return event.WaitT(timeout);
}

bool TDefaultWaitProxy::WaitCondVar(TCondVar &condVar, TMutex &mutex, TDuration timeout)
{
    return condVar.WaitT(mutex, timeout);
}

void TDefaultWaitProxy::Sleep(TDuration timeout)
{
    ::Sleep(timeout);
}

////////////////////////////////////////////////////////////////////////////////

TWaitProxy::TWaitProxy()
    : Proxy_(::MakeIntrusive<TDefaultWaitProxy>())
{ }

TWaitProxy* TWaitProxy::Get()
{
    return Singleton<TWaitProxy>();
}

void TWaitProxy::SetProxy(::TIntrusivePtr<IWaitProxy> proxy)
{
    Proxy_ = std::move(proxy);
}

bool TWaitProxy::WaitFuture(const NThreading::TFuture<void>& future)
{
    return Proxy_->WaitFuture(future, TDuration::Max());
}

bool TWaitProxy::WaitFuture(const NThreading::TFuture<void>& future, TInstant deadLine)
{
    return Proxy_->WaitFuture(future, deadLine - TInstant::Now());
}

bool TWaitProxy::WaitFuture(const NThreading::TFuture<void>& future, TDuration timeout)
{
    return Proxy_->WaitFuture(future, timeout);
}

bool TWaitProxy::WaitEventD(TSystemEvent& event, TInstant deadLine)
{
    return Proxy_->WaitEvent(event, deadLine - TInstant::Now());
}

bool TWaitProxy::WaitEventT(TSystemEvent& event, TDuration timeout)
{
    return Proxy_->WaitEvent(event, timeout);
}

void TWaitProxy::WaitEventI(TSystemEvent& event)
{
    Proxy_->WaitEvent(event, TDuration::Max());
}

bool TWaitProxy::WaitEvent(TSystemEvent& event)
{
    return Proxy_->WaitEvent(event, TDuration::Max());
}

bool TWaitProxy::WaitCondVarD(TCondVar& condVar, TMutex& m, TInstant deadLine)
{
    return Proxy_->WaitCondVar(condVar, m, deadLine - TInstant::Now());
}

bool TWaitProxy::WaitCondVarT(TCondVar& condVar, TMutex& m, TDuration timeOut)
{
    return Proxy_->WaitCondVar(condVar, m, timeOut);
}

void TWaitProxy::WaitCondVarI(TCondVar& condVar, TMutex& m)
{
    Proxy_->WaitCondVar(condVar, m, TDuration::Max());
}

void TWaitProxy::WaitCondVar(TCondVar& condVar, TMutex& m)
{
    Proxy_->WaitCondVar(condVar, m, TDuration::Max());
}

void TWaitProxy::Sleep(TDuration timeout)
{
    Proxy_->Sleep(timeout);
}

void TWaitProxy::SleepUntil(TInstant instant)
{
    Proxy_->Sleep(instant - TInstant::Now());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
