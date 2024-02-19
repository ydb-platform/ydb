#pragma once

#include "public.h"
#include "bundle_controller_settings.h"

#include <yt/yt/client/api/client_common.h>

namespace NYT::NBundleControllerClient {

////////////////////////////////////////////////////////////////////////////////

struct TGetBundleConfigOptions
    : public NApi::TTimeoutOptions
{ };

struct TSetBundleConfigOptions
    : public NApi::TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct TBundleConfigDescriptor
    : public NYTree::TYsonStruct
{
    TString BundleName;

    TBundleTargetConfigPtr Config;
    TBundleConfigConstraintsPtr ConfigConstraints;
    TBundleResourceQuotaPtr ResourceQuota;

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

    virtual TFuture<void> SetBundleConfig(
        const TString& bundleName,
        const NBundleControllerClient::TBundleTargetConfigPtr& bundleConfig,
        const TSetBundleConfigOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleControllerClient
