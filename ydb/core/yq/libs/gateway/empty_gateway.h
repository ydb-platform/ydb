#pragma once
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>

#include <library/cpp/actors/core/actorsystem.h>

namespace NYq {

TIntrusivePtr<NYql::IDqGateway> CreateEmptyGateway(NActors::TActorId runActorId);

} // namespace NYq
