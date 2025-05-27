#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/net/address.h>

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

struct TDnsResolveOptions
{
    bool EnableIPv4 = true;
    bool EnableIPv6 = true;
};

void FormatValue(TStringBuilderBase* builder, const TDnsResolveOptions options, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct IDnsResolver
    : public TRefCounted
{
    virtual TFuture<NNet::TNetworkAddress> Resolve(
        const std::string& hostName,
        const TDnsResolveOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDnsResolver)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns

