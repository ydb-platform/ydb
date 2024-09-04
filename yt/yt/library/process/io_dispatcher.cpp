#include "io_dispatcher.h"

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/poller.h>

#include <yt/yt/core/misc/singleton.h>

namespace NYT::NPipes {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TIODispatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_polling_period", &TThis::ThreadPoolPollingPeriod)
        .Default(TDuration::MilliSeconds(10));
}

////////////////////////////////////////////////////////////////////////////////

TIODispatcher::TIODispatcher()
    : Poller_(BIND([] { return CreateThreadPoolPoller(1, "Pipes"); }))
{ }

TIODispatcher::~TIODispatcher() = default;

TIODispatcher* TIODispatcher::Get()
{
    return Singleton<TIODispatcher>();
}

void TIODispatcher::Configure(const TIODispatcherConfigPtr& config)
{
    Poller_->Reconfigure(config->ThreadPoolPollingPeriod);
}

IInvokerPtr TIODispatcher::GetInvoker()
{
    return Poller_.Value()->GetInvoker();
}

IPollerPtr TIODispatcher::GetPoller()
{
    return Poller_.Value();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
