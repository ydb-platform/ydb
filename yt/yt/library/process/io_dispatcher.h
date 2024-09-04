#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcherConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration ThreadPoolPollingPeriod;

    REGISTER_YSON_STRUCT(TIODispatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIODispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TIODispatcher
{
public:
    ~TIODispatcher();

    static TIODispatcher* Get();

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
