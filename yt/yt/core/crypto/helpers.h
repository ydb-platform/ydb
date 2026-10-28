#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

struct ISslCertificateUpdater
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual TFuture<void> Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISslCertificateUpdater)

////////////////////////////////////////////////////////////////////////////////

ISslCertificateUpdaterPtr CreateSslCertificateUpdater(
    IInvokerPtr controlInvoker,
    TSslContextPtr sslContext,
    TServerSslContextConfigPtr sslConfig,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
