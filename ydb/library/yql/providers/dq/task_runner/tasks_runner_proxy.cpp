#include "tasks_runner_proxy.h"

namespace NYql::NTaskRunnerProxy {

using namespace NKikimr;
using namespace NDq;

TDqTaskRunnerMemoryLimits DefaultMemoryLimits() {
    TDqTaskRunnerMemoryLimits limits;
    limits.ChannelBufferSize = 20_MB;
    limits.OutputChunkMaxSize = 2_MB;
    return limits;
}

} // namespace NYql::NTaskRunnerProxy
