#include "bundle_controller_client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

void TBundleConfigDescriptor::Register(TRegistrar registrar)
{
    registrar.Parameter("bundle_name", &TThis::BundleName)
        .Default();

    registrar.Parameter("cpu_limits", &TThis::CpuLimits)
        .DefaultNew();
    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
        .DefaultNew();

    registrar.Parameter("rpc_proxy_count", &TThis::RpcProxyCount)
        .Default(0);
    registrar.Parameter("rpc_proxy_resource_guarantee", &TThis::RpcProxyResourceGuarantee)
        .DefaultNew();
    registrar.Parameter("tablet_node_count", &TThis::TabletNodeCount)
        .Default(0);
    registrar.Parameter("tablet_node_resource_guarantee", &TThis::TabletNodeResourceGuarantee)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
