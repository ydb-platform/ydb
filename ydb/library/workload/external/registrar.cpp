#include "external.h"
#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

namespace NExternal {

TWorkloadFactory::TRegistrator<TExternalWorkloadParams> Registrar("external");

} // namespace NLog

} // namespace NYdbWorkload
