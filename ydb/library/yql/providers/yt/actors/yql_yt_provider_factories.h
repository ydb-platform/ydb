#pragma once

#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

namespace NYql::NDq {
    //Only YT emulator is supported currently
    void RegisterYtLookupActorFactory(TDqAsyncIoFactory& factory, NFile::TYtFileServices::TPtr ytServcies, NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry);
}
