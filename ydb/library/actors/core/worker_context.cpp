#include "worker_context.h"
#include "probes.h"
#include "debug.h"

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    TWorkerContext::TWorkerContext(TWorkerId workerId)
        : WorkerId(workerId)
    {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TWorkerContext::ctor ", WorkerId);
    }

    TWorkerContext::~TWorkerContext() {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TWorkerContext::dtor ", WorkerId);
    }

}
