#include "query.h"
#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

namespace NQuery {

TWorkloadFactory::TRegistrator<TQueryWorkloadParams> Registrar("query");

} // namespace NLog

} // namespace NYdbWorkload
