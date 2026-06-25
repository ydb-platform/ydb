#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

// KqpStreamEnumerate(input) -> a sequence of (Uint64 rank, item) tuples, rank 1-based.
//
// Unlike the builtin `Enumerate` (List-only, materialized), this works over a Stream/Flow so it can run
// inside a DQ compute stage -- letting the hybrid-search rule assign a per-row rank to the already-ordered,
// single-partition branch stream without first materializing it via TDqPrecompute.
IComputationNode* WrapKqpStreamEnumerate(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NMiniKQL
} // namespace NKikimr
