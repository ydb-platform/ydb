#include "mixed.h"
#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

namespace NMixed {

TWorkloadFactory::TRegistrator<TMixedWorkloadParams> Registrar("mixed");

} // namespace NMixed

} // namespace NYdbWorkload
