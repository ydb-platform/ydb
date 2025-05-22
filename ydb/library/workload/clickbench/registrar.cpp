#include "clickbench.h"
#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TClickbenchWorkloadParams> ClickbenchRegistrar("clickbench");

}