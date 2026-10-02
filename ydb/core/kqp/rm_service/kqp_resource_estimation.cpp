#include "kqp_resource_estimation.h"

namespace NKikimr::NKqp {

using namespace NYql::NDqProto;
using namespace NKikimrConfig;



TTaskResourceEstimation BuildInitialTaskResources(const TDqTask& task) {
    TTaskResourceEstimation ret;
    const auto& opts = task.GetProgram().GetSettings();
    ret.TaskId = task.GetId();
    ret.ChannelBuffersCount += task.GetInputs().size() ? 1 : 0;
    ret.ChannelBuffersCount += task.GetOutputs().size() ? 1 : 0;
    ret.HeavyProgram = opts.GetHasMapJoin();
    return ret;
}

void EstimateTaskResources(TTaskResourceEstimation& ret, const TTaskResourceEstimationParams& params, ui32 tasksCount) {
    ui64 totalChannels = std::max(tasksCount, (ui32)1) * std::max(ret.ChannelBuffersCount, (ui32)1);
    ui64 optimalChannelBufferSizeEstimation = totalChannels * params.ChannelBufferSize;

    optimalChannelBufferSizeEstimation = std::min(optimalChannelBufferSizeEstimation, params.MaxTotalChannelBuffersSize);

    ret.ChannelBufferMemoryLimit = std::max(params.MinChannelBufferSize, optimalChannelBufferSizeEstimation / totalChannels);

    if (ret.HeavyProgram) {
        ret.MkqlProgramMemoryLimit = params.MkqlHeavyProgramMemoryLimit / std::max(tasksCount, (ui32)1);
    } else {
        ret.MkqlProgramMemoryLimit = params.MkqlLightProgramMemoryLimit / std::max(tasksCount, (ui32)1);
    }

    ret.TotalMemoryLimit = ret.ChannelBuffersCount * ret.ChannelBufferMemoryLimit
        + ret.MkqlProgramMemoryLimit;
}

} // namespace NKikimr::NKqp
