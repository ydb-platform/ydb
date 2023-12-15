#pragma once

#include "client_common.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TGetBundleConfigOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct TBundleConfigDescriptor
{
    TString BundleName;
    int RpcProxyCount;
    int TabletNodeCount;
};

////////////////////////////////////////////////////////////////////////////////

struct IBundleControllerClient
{
    virtual ~IBundleControllerClient() = default;

    virtual TFuture<TBundleConfigDescriptor> GetBundleConfig(
        const TString& bundleName,
        const TGetBundleConfigOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
