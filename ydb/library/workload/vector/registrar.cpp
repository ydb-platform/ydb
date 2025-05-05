#include "vector.h"
#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TVectorWorkloadParams> VectorRegistrar("vector");

}
