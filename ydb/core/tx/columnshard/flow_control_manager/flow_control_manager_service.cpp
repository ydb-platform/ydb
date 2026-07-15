#include "flow_control_manager_service.h"

#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_actor.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_events.h>

namespace NKikimr::NColumnShard::NFlowControl {

NActors::TActorId TFlowControlManagerServiceOperator::MakeServiceId() {
    return NActors::TActorId(0, "FlowCtrlMng");
}

std::unique_ptr<NActors::IActor> TFlowControlManagerServiceOperator::CreateService(
    TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup) {
    return std::make_unique<TFlowControlManager>(countersGroup);
}

void TFlowControlManagerServiceOperator::StartLongTxWrite(const TActorContext& ctx, TLongTxWrite&& longTxWrite) {
    ctx.Send(MakeServiceId(), std::make_unique<TEvLongTxWrite>(std::move(longTxWrite)));
}

}   // namespace NKikimr::NColumnShard::NFlowControl
