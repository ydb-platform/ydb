#include "io_dispatcher.h"

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/poller.h>

#include <yt/yt/core/misc/singleton.h>

namespace NYT::NPipes {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TIODispatcher::TIODispatcher()
    : Poller_(BIND([] { return CreateThreadPoolPoller(1, "Pipes"); }))
{ }

TIODispatcher::~TIODispatcher() = default;

TIODispatcher* TIODispatcher::Get()
{
    return Singleton<TIODispatcher>();
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
