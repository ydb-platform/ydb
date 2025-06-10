#pragma once
#include <ydb/library/yql/dq/runtime/dq_compute.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapBlockHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // NKikimr
} // NMiniKQL
