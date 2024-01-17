#pragma once
#include "defs.h"
#include "immediate_control_board_impl.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {

inline NActors::TActorId MakeIcbId(ui32 node) {
    char x[12] = {'i','c','b','_','a','c','t','o','r'};
    return NActors::TActorId(node, TStringBuf(x, 12));
}

class TImmediateControlActor;

NActors::IActor* CreateImmediateControlActor(TIntrusivePtr<TControlBoard> board, const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters);

}
