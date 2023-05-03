#include "kqp_resource_estimation.h"

namespace NKikimr::NKqp {

using namespace NYql::NDqProto;
using namespace NKikimrConfig;

TTaskResourceEstimation EstimateTaskResources(const TDqTask& task,
    const TTableServiceConfig::TResourceManager& config, const ui32 tasksCount)
{
    TTaskResourceEstimation ret = BuildInitialTaskResources(task);
    EstimateTaskResources(config, ret, tasksCount);
    return ret;
}

TTaskResourceEstimation BuildInitialTaskResources(const TDqTask& task) {
    TTaskResourceEstimation ret;
    const auto& opts = task.GetProgram().GetSettings();
    ret.TaskId = task.GetId();
    ret.ChannelBuffersCount += task.GetInputs().size() ? 1 : 0;
    ret.ChannelBuffersCount += task.GetOutputs().size() ? 1 : 0;
    ret.HeavyProgram = opts.GetHasMapJoin();
    return ret;
}

void EstimateTaskResources(const TTableServiceConfig::TResourceManager& config,
    TTaskResourceEstimation& ret, const ui32 tasksCount)
{

    ui64 channelBuffersSize = ret.ChannelBuffersCount * config.GetChannelBufferSize();
    if (channelBuffersSize > config.GetMaxTotalChannelBuffersSize()) {
        ret.ChannelBufferMemoryLimit = std::max(config.GetMinChannelBufferSize(),
                                                config.GetMaxTotalChannelBuffersSize() / ret.ChannelBuffersCount);
    } else {
        ret.ChannelBufferMemoryLimit = config.GetChannelBufferSize();
    }

    if (ret.HeavyProgram) {
        ret.MkqlProgramMemoryLimit = config.GetMkqlHeavyProgramMemoryLimit() / tasksCount;
    } else {
        ret.MkqlProgramMemoryLimit = config.GetMkqlLightProgramMemoryLimit() / tasksCount;
    }

    ret.TotalMemoryLimit = ret.ChannelBuffersCount * ret.ChannelBufferMemoryLimit
        + ret.MkqlProgramMemoryLimit;
}

} // namespace NKikimr::NKqp
