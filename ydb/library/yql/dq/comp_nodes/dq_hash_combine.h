#pragma once

#include <ydb/library/yql/dq/runtime/dq_compute.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapDqHashCombine(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NMiniKQL
} // namespace NKikimr
