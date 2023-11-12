#include "disk_manager_proxy.h"

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

struct TDiskManagerProxyMock
    : public IDiskManagerProxy
{
    virtual TFuture<THashSet<TString>> GetYtDiskMountPaths()
    {
        THROW_ERROR_EXCEPTION("Disk manager library is not available under this build configuration");
    }

    virtual TFuture<std::vector<TDiskInfo>> GetDisks()
    {
        THROW_ERROR_EXCEPTION("Disk manager library is not available under this build configuration");
    }

    virtual TFuture<void> RecoverDiskById(const TString& /*diskId*/, ERecoverPolicy /*recoverPolicy*/)
    {
        THROW_ERROR_EXCEPTION("Disk manager library is not available under this build configuration");
    }

    virtual TFuture<void> FailDiskById(const TString& /*diskId*/, const TString& /*reason*/)
    {
        THROW_ERROR_EXCEPTION("Disk manager library is not available under this build configuration");
    }

    virtual void OnDynamicConfigChanged(const TDiskManagerProxyDynamicConfigPtr& /*newConfig*/)
    {
        // Do nothing
    }
};

DEFINE_REFCOUNTED_TYPE(TDiskManagerProxyMock)

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IDiskManagerProxyPtr CreateDiskManagerProxy(TDiskManagerProxyConfigPtr /*config*/)
{
    // This implementation is used when disk_manager_proxy_impl.cpp is not linked.

    return New<TDiskManagerProxyMock>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
