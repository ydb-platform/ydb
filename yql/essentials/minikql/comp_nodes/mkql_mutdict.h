#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapToMutDict(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictCreate(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictInsert(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictUpsert(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictUpdate(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictRemove(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictPop(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictContains(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictLookup(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictLength(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictHasItems(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictItems(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictKeys(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMutDictPayloads(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapFromMutDict(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NMiniKQL
} // namespace NKikimr
