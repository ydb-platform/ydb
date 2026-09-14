#pragma once

#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <functional>

namespace NYql {

struct TTaskTransformArguments {
    THashMap<TString, TString> TaskParams;
    TVector<TString> ReadRanges;
};

using TTaskTransformFactory = std::function<NKikimr::NMiniKQL::TCallableVisitFuncProvider(const TTaskTransformArguments&, const NKikimr::NMiniKQL::IFunctionRegistry*)>;

TTaskTransformFactory CreateCompositeTaskTransformFactory(TVector<TTaskTransformFactory> factories);

} // namespace NYql
