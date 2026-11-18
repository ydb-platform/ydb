#include "imdb.h"

#include <ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TImdbWorkloadParams> ImdbRegistrar("imdb");

} // namespace NYdbWorkload
