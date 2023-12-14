#pragma once

#include "client_common.h"

#include <yt/yt/ytlib/bundle_controller/bundle_controller_settings.h>

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

    NCellBalancer::TCpuLimitsPtr CpuLimits;
    NCellBalancer::TMemoryLimitsPtr MemoryLimits;

    int RpcProxyCount;
    NCellBalancer::TInstanceResourcesPtr RpcProxyResourceGuarantee;

    int TabletNodeCount;
    NCellBalancer::TInstanceResourcesPtr TabletNodeResourceGuarantee;

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
