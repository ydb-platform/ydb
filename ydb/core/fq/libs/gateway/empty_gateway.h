#pragma once
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NFq {

TIntrusivePtr<NYql::IDqGateway> CreateEmptyGateway(NActors::TActorId runActorId);

} // namespace NFq
