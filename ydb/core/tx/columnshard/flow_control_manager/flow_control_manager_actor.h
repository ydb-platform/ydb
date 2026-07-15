#pragma once

#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_counters.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard::NFlowControl {

class TFlowControlManager: public NActors::TActor<TFlowControlManager> {
    TCSFlowControlManagerCounters Counters;

    // clang-format off
    STRICT_STFUNC(StateMain,
                  HFunc(NFlowControl::TEvLongTxWrite, Handle)
    )
    // clang-format on

    void Handle(const NFlowControl::TEvLongTxWrite::TPtr& ev, const TActorContext& /*ctx*/);

public:
    TFlowControlManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup);
};

}   // namespace NKikimr::NColumnShard::NFlowControl
