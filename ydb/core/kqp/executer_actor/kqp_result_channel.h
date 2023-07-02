#pragma once

#include "kqp_tasks_graph.h"
#include "kqp_executer.h"

#include <library/cpp/actors/core/actor.h>

namespace NYql {

class TTypeAnnotationNode;

namespace NDqProto {

class TData;

} // namespace NDqProto
} // namespace NYql

namespace NKikimrMiniKQL {
class TType;
} // namespace NKikimrMiniKQL

namespace NKikimr::NKqp {

struct TQueryExecutionStats;
struct TKqpExecuterTxResult;

NActors::IActor* CreateResultStreamChannelProxy(ui64 txId, ui64 channelId, NKikimr::NMiniKQL::TType* itemType,
    const TVector<ui32>* columnOrder, ui32 queryResultIndex, NActors::TActorId target, std::shared_ptr<TQueryExecutionStats> stats,
    NActors::TActorId executer);

NActors::IActor* CreateResultDataChannelProxy(ui64 txId, ui64 channelId, std::shared_ptr<TQueryExecutionStats> stats,
    NActors::TActorId executer, ui32 inputIndex, TEvKqpExecuter::TEvTxResponse* receiver);

} // namespace NKikimr::NKqp
