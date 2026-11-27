#include "yaem.h"
#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

namespace NYaem {

TWorkloadFactory::TRegistrator<TYaemWorkloadParams> Registrar("yaem");

} // namespace NYaem

} // namespace NYdbWorkload
