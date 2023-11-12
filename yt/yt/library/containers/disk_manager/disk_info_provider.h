#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

class TDiskInfoProvider
    : public TRefCounted
{
public:
    TDiskInfoProvider(
        IDiskManagerProxyPtr diskManagerProxy,
        TDiskInfoProviderConfigPtr config);

    const std::vector<TString>& GetConfigDiskIds() const;

    TFuture<std::vector<TDiskInfo>> GetYTDiskInfos();

    TFuture<void> RecoverDisk(const TString& diskId);

    TFuture<void> FailDisk(
        const TString& diskId,
        const TString& reason);

private:
    const IDiskManagerProxyPtr DiskManagerProxy_;
    const TDiskInfoProviderConfigPtr Config_;
};

DEFINE_REFCOUNTED_TYPE(TDiskInfoProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
