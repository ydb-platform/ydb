#include "log.h"
#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

namespace NLog {

TWorkloadFactory::TRegistrator<TLogWorkloadParams> Registrar("log");

} // namespace NLog

} // namespace NYdbWorkload
