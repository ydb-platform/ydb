#pragma once

#include "public.h"

#include <yt/yt/library/containers/disk_manager/config.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

struct IDiskManagerProxy
    : public virtual TRefCounted
{
    virtual TFuture<THashSet<TString>> GetYtDiskMountPaths() = 0;

    virtual TFuture<std::vector<TDiskInfo>> GetDisks() = 0;

    virtual TFuture<void> RecoverDiskById(const TString& diskId, ERecoverPolicy recoverPolicy) = 0;

    virtual TFuture<void> FailDiskById(const TString& diskId, const TString& reason) = 0;

    virtual void OnDynamicConfigChanged(const TDiskManagerProxyDynamicConfigPtr& newConfig) = 0;

};

DEFINE_REFCOUNTED_TYPE(IDiskManagerProxy)

////////////////////////////////////////////////////////////////////////////////

IDiskManagerProxyPtr CreateDiskManagerProxy(TDiskManagerProxyConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
