#pragma once

#include <yt/yt/core/misc/public.h>

#include <util/generic/hash.h>

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

struct TDnsResolveOptions
{
    bool EnableIPv4 = true;
    bool EnableIPv6 = true;

    bool operator==(const TDnsResolveOptions&) const = default;
};

DECLARE_REFCOUNTED_STRUCT(TAresDnsResolverConfig)

DECLARE_REFCOUNTED_STRUCT(IDnsResolver)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NDns::TDnsResolveOptions>
{
    size_t operator()(NYT::NDns::TDnsResolveOptions options) const;
};

////////////////////////////////////////////////////////////////////////////////
