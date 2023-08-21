#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TWorkloadConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("workload_descriptor", &TThis::WorkloadDescriptor)
        .Default(TWorkloadDescriptor(EWorkloadCategory::UserBatch));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
