#pragma once

#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <functional>

namespace NYql {

using TTaskTransformFactory = std::function<NKikimr::NMiniKQL::TCallableVisitFuncProvider(const THashMap<TString, TString>&, const NKikimr::NMiniKQL::IFunctionRegistry*)>;

TTaskTransformFactory CreateCompositeTaskTransformFactory(TVector<TTaskTransformFactory> factories);

} // namespace NYql
