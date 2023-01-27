#include "task_controller.h"
#include "task_controller_impl.h"

namespace NYql {

namespace {

class TTaskController: public TTaskControllerImpl<TTaskController> {
public:
    TTaskController(
        const TString& traceId,
        const NActors::TActorId& executerId,
        const NActors::TActorId& resultId,
        const TDqConfiguration::TPtr& settings,
        const NYql::NCommon::TServiceCounters& serviceCounters,
        const TDuration& pingPeriod,
        const TDuration& aggrPeriod
    )
        : TTaskControllerImpl<TTaskController>(
            traceId,
            executerId,
            resultId,
            settings,
            serviceCounters,
            pingPeriod,
            aggrPeriod,
            &TTaskControllerImpl<TTaskController>::Handler)
    {
    }
};

} /* namespace */

THolder<NActors::IActor> MakeTaskController(
    const TString& traceId,
    const NActors::TActorId& executerId,
    const NActors::TActorId& resultId,
    const TDqConfiguration::TPtr& settings,
    const NYql::NCommon::TServiceCounters& serviceCounters,
    const TDuration& pingPeriod,
    const TDuration& aggrPeriod
) {
    return MakeHolder<NDq::TLogWrapReceive>(new TTaskController(traceId, executerId, resultId, settings, serviceCounters, pingPeriod, aggrPeriod), traceId);
}

} /* namespace NYql */
