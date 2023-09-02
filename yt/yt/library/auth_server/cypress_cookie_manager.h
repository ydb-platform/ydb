#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/http/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ICypressCookieManager
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual const ICypressCookieStorePtr& GetCookieStore() const = 0;

    virtual const ICookieAuthenticatorPtr& GetCookieAuthenticator() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ICypressCookieManager)

////////////////////////////////////////////////////////////////////////////////

ICypressCookieManagerPtr CreateCypressCookieManager(
    TCypressCookieManagerConfigPtr config,
    NApi::IClientPtr client,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
