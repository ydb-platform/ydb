#include "registry.h"

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimr::NTabletFlatExecutor {

const NMiniKQL::IFunctionRegistry* TestBuiltinFunctionRegistry() {
    static const auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
    return registry.Get();
}

}
