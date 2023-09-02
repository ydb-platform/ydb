#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/lazy_ptr.h>

namespace NYT::NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcher
{
public:
    ~TIODispatcher();

    static TIODispatcher* Get();

    IInvokerPtr GetInvoker();

    NConcurrency::IPollerPtr GetPoller();

private:
    TIODispatcher();

    Y_DECLARE_SINGLETON_FRIEND()

    TLazyIntrusivePtr<NConcurrency::IThreadPoolPoller> Poller_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
