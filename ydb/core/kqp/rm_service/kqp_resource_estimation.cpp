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

} // namespace NKikimr::NKqp
