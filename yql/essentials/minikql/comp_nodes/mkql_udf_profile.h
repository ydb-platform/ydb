#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr::NMiniKQL {

// Decorates the boxed value produced by a UDF wrapper node with call-site
// profiling (call count, slow-call count, total time, argument cardinality),
// as controlled by the UdfProfile* runtime settings (see YQL-21019).
//
// `functionName` is the fully qualified "Module.Func" name used both for the
// exclude-modules check and as the reported counter-key suffix.
// `funcType` describes the signature that `value.Run()` itself implements
// (i.e. what `value` will be called with) -- for a UDF with run-config
// currying this is the *actual* function type, not the outer curried one.
//
// Returns `value` unchanged if profiling is disabled, the module is
// excluded, or the leaf (non-Callable-returning) call's argument types
// aren't all hashable.
NUdf::TUnboxedValue MaybeWrapUdfProfiling(
    NUdf::TUnboxedValue value,
    const TCallableType* funcType,
    const TString& functionName,
    TComputationContext& ctx,
    ui32 profileStateIndex);

} // namespace NKikimr::NMiniKQL
