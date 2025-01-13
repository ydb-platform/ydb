#include "config.h"

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

void TResourceTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("cpu_to_vcpu_factor", &TThis::CpuToVCpuFactor)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (config->CpuToVCpuFactor && !config->Enable) {
            THROW_ERROR_EXCEPTION("\"cpu_to_vcpu_factor\" requires \"enabled\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
