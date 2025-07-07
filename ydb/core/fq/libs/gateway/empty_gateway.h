#pragma once
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>

#include <ydb/library/actors/core/actorid.h>

namespace NFq {

TIntrusivePtr<NYql::IDqGateway> CreateEmptyGateway(NActors::TActorId runActorId);

} // namespace NFq
