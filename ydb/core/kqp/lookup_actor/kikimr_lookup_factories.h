#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NYql::NDq {
    void RegisterKikimrLookupProviderFactories(TDqAsyncIoFactory& factory);
} // namespace NYql::NDq
