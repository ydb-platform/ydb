#pragma once

#include "bundle_controller_settings.h"

#include <yt/yt/client/api/client_common.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TBundleConfigDescriptor)

////////////////////////////////////////////////////////////////////////////////

struct TGetBundleConfigOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct TBundleConfigDescriptor
    : public NYTree::TYsonStruct
{
    TString BundleName;

    NBundleControllerClient::TCpuLimitsPtr CpuLimits;
    NBundleControllerClient::TMemoryLimitsPtr MemoryLimits;

    int RpcProxyCount;
    NBundleControllerClient::TInstanceResourcesPtr RpcProxyResourceGuarantee;

    int TabletNodeCount;
    NBundleControllerClient::TInstanceResourcesPtr TabletNodeResourceGuarantee;

    REGISTER_YSON_STRUCT(TBundleConfigDescriptor);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleConfigDescriptor)

////////////////////////////////////////////////////////////////////////////////

struct IBundleControllerClient
{
    virtual ~IBundleControllerClient() = default;

    virtual TFuture<TBundleConfigDescriptorPtr> GetBundleConfig(
        const TString& bundleName,
        const TGetBundleConfigOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
