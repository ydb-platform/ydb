#pragma once

#include "kqp_tasks_graph.h"

namespace NKikimr::NKqp {

NActors::IActor* CreateKqpTableResolver(const TActorId& owner, ui64 txId, TMaybe<TString> userToken,
    const TVector<IKqpGateway::TPhysicalTxData>& transactions, TKqpTableKeys& tableKeys, TKqpTasksGraph& tasksGraph);

} // namespace NKikimr::NKqp
