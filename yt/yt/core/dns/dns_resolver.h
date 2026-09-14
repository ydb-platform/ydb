#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TDnsResolveOptions options, TStringBuf spec);

void Serialize(const TDnsResolveOptions& options, NYson::IYsonConsumer* consumer);
void Deserialize(TDnsResolveOptions& options, NYTree::INodePtr node);
void Deserialize(TDnsResolveOptions& options, NYson::TYsonPullParserCursor* cursor);

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
