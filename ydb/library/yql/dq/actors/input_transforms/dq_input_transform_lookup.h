#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace NYql::NDq {

using namespace NKikimr;

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateInputTransformStreamLookup(
    NDqProto::TDqInputTransformLookupSettings&& settings,
    IDqAsyncIoFactory::TInputTransformArguments&& args
);

} // namespace NYql::NDq
