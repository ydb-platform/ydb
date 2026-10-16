#include "split_merge.h"
#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TSplitMergeWorkloadParams> SplitMergeRegistrar("splitmerge");

}
