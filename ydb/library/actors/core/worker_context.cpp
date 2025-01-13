#include "worker_context.h"
#include "probes.h"
#include "debug.h"

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    TWorkerContext::TWorkerContext(TWorkerId workerId, TCpuId cpuId)
        : WorkerId(workerId)
        , CpuId(cpuId)
        , Lease(WorkerId, NeverExpire)
    {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TWorkerContext::ctor ", WorkerId, ' ', cpuId);
    }

    TWorkerContext::~TWorkerContext() {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TWorkerContext::dtor ", WorkerId, ' ', CpuId);
    }

}
