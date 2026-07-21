#pragma once

#include "kqp_tasks_graph.h"

#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NKikimr::NKqp {

NActors::IActor* CreateKqpTableResolver(const TActorId& owner, ui64 txId,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpTasksGraph& tasksGraph, bool skipUnresolvedNames,
    NWilson::TTraceId resolveTraceId = {});

} // namespace NKikimr::NKqp
