#include "bundle_controller_client.h"

namespace NYT::NBundleControllerClient {

////////////////////////////////////////////////////////////////////////////////

void TBundleConfigDescriptor::Register(TRegistrar registrar)
{
    registrar.Parameter("bundle_name", &TThis::BundleName)
        .Default();
    registrar.Parameter("bundle_config", &TThis::Config)
        .DefaultNew();
    registrar.Parameter("bundle_constraints", &TThis::ConfigConstraints)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleControllerClient
