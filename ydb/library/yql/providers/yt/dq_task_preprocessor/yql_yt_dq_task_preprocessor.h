#pragma once

#include <ydb/library/yql/providers/dq/interface/yql_dq_task_preprocessor.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

namespace NYql::NDq {

TDqTaskPreprocessorFactory CreateYtDqTaskPreprocessorFactory(bool ytEmulationMode, NKikimr::NMiniKQL::IFunctionRegistry::TPtr funcRegistry);

} // namespace NYql::NDq
