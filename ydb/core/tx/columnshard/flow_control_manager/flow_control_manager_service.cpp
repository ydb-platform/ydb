#include "flow_control_manager_service.h"

#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_actor.h>

namespace NKikimr::NColumnShard::NFlowControl {

std::unique_ptr<NActors::IActor> TFlowControlManagerServiceOperator::CreateService(
    TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup) {
    return std::make_unique<TFlowControlManager>(countersGroup);
}

}   // namespace NKikimr::NColumnShard::NFlowControl
