#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NYql::NDq {

void RegisterDqInputTransformLookupActorFactory(NDq::TDqAsyncIoFactory& factory);

} // namespace NYql::NDq
