#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>

namespace NYql::NDq {
//Only YT emulator is supported currently
void RegisterYtLookupActorFactory(
    TDqAsyncIoFactory& factory, NFile::TYtFileServices::TPtr ytServcies, NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry);
}   // namespace NYql::NDq
