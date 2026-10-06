#pragma once
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
namespace NYql::NDq {
void RegisterYtMessageStreamReadActorFactory(TDqAsyncIoFactory& factory, IStructuredTokenCredentialsFactory::TPtr credentials);
}
