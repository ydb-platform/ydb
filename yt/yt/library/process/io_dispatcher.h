#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcher
{
public:
    static TIODispatcher* Get();
    ~TIODispatcher();

    void Configure(const TIODispatcherConfigPtr& config);

    IInvokerPtr GetInvoker();
    NConcurrency::IPollerPtr GetPoller();

private:
    TIODispatcher();

    Y_DECLARE_SINGLETON_FRIEND()

    TLazyIntrusivePtr<NConcurrency::IThreadPoolPoller> Poller_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
