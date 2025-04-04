#pragma once

#include "kqp_tasks_graph.h"

namespace NKikimr::NKqp {

NActors::IActor* CreateKqpTableResolver(const TActorId& owner, ui64 txId,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpTasksGraph& tasksGraph);

} // namespace NKikimr::NKqp
