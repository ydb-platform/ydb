#include "json_workload_params.h"

#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TJsonWorkloadParams> JsonWorkloadRegistrar("json");

} // namespace NYdbWorkload
