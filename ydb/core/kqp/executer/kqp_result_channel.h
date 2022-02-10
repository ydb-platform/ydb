#pragma once

#include "kqp_tasks_graph.h"

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

NActors::IActor* CreateResultStreamChannelProxy(ui64 txId, ui64 channelId, const NKikimrMiniKQL::TType& itemType,
    const NKikimrMiniKQL::TType* resultItemType, NActors::TActorId target, TQueryExecutionStats* stats,
    NActors::TActorId executer);

NActors::IActor* CreateResultDataChannelProxy(ui64 txId, ui64 channelId, TQueryExecutionStats* stats,
    NActors::TActorId executer, TVector<NYql::NDqProto::TData>* resultReceiver);

} // namespace NKikimr::NKqp
