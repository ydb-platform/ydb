#include "kv.h"
#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TKvWorkloadParams> KvRegistrar("kv");

}