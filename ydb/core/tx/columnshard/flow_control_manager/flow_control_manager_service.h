#pragma once

#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_types.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NColumnShard::NFlowControl {

class TFlowControlManagerServiceOperator {
private:
    using TSelf = TFlowControlManagerServiceOperator;

public:
    static NActors::TActorId MakeServiceId(ui32 nodeId) {
        return NActors::TActorId(nodeId, "FlowCtrlMng");
    }

    static std::unique_ptr<NActors::IActor> CreateService(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup);

    static void StartLongTxWrite(const TActorContext& ctx, TLongTxWrite&& longTxWrite);
};

}   // namespace NKikimr::NColumnShard::NFlowControl
