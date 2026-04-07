#pragma once

#include <yql/essentials/minikql/mkql_function_metadata.h>

namespace NKikimr::NMiniKQL {

IBuiltinFunctionRegistry::TPtr CreateBuiltinRegistry();

} // namespace NKikimr::NMiniKQL
