#include "kqp_resource_estimation.h"

namespace NKikimr::NKqp {

using namespace NYql::NDqProto;
using namespace NKikimrConfig;

TTaskResourceEstimation EstimateTaskResources(const TDqTask& task,
    const TTableServiceConfig::TResourceManager& config)
{
    TTaskResourceEstimation ret;
    EstimateTaskResources(task, config, ret);
    return ret;
}

void EstimateTaskResources(const TDqTask& task, const TTableServiceConfig::TResourceManager& config,
    TTaskResourceEstimation& ret)
{
    ret.TaskId = task.GetId();
    for (const auto& input : task.GetInputs()) {
        ret.ChannelBuffersCount += input.ChannelsSize();
    }
    for (const auto& output : task.GetOutputs()) {
        ret.ChannelBuffersCount += output.ChannelsSize();
    }

    ui64 channelBuffersSize = ret.ChannelBuffersCount * config.GetChannelBufferSize();
    if (channelBuffersSize > config.GetMaxTotalChannelBuffersSize()) {
        ret.ChannelBufferMemoryLimit = std::max(config.GetMinChannelBufferSize(),
                                                config.GetMaxTotalChannelBuffersSize() / ret.ChannelBuffersCount);
    } else {
        ret.ChannelBufferMemoryLimit = config.GetChannelBufferSize();
    }

    const auto& opts = task.GetProgram().GetSettings();
    if (/* opts.GetHasSort() || */opts.GetHasMapJoin()) {
        ret.MkqlProgramMemoryLimit = config.GetMkqlHeavyProgramMemoryLimit();
    } else {
        ret.MkqlProgramMemoryLimit = config.GetMkqlLightProgramMemoryLimit();
    }

    ret.TotalMemoryLimit = ret.ChannelBuffersCount * ret.ChannelBufferMemoryLimit
        + ret.MkqlProgramMemoryLimit;
}

TVector<TTaskResourceEstimation> EstimateTasksResources(const TVector<NYql::NDqProto::TDqTask>& tasks,
    const TTableServiceConfig::TResourceManager& config)
{
    TVector<TTaskResourceEstimation> ret;
    ret.resize(tasks.size());
    for (ui64 i = 0; i < tasks.size(); ++i) {
        EstimateTaskResources(tasks[i], config, ret[i]);
    }
    return ret;
}

} // namespace NKikimr::NKqp
