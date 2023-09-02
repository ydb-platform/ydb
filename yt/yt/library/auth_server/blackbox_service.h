#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

//! Abstracts away Blackbox.
//! See https://doc.yandex-team.ru/blackbox/ for API reference.
struct IBlackboxService
    : public virtual TRefCounted
{
    virtual TFuture<NYTree::INodePtr> Call(
        const TString& method,
        const THashMap<TString, TString>& params) = 0;
    virtual TErrorOr<TString> GetLogin(const NYTree::INodePtr& reply) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IBlackboxService)

////////////////////////////////////////////////////////////////////////////////

IBlackboxServicePtr CreateBlackboxService(
    TBlackboxServiceConfigPtr config,
    ITvmServicePtr tvmService,
    NConcurrency::IPollerPtr poller,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
