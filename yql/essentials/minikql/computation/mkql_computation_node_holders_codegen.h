#pragma once

#include "mkql_computation_node_holders.h"

#ifndef MKQL_DISABLE_CODEGEN
namespace llvm {
    class Value;
    class BasicBlock;
}
#endif

namespace NKikimr {
namespace NMiniKQL {

class TMemoryUsageInfo;

#ifndef MKQL_DISABLE_CODEGEN
struct TCodegenContext;
#endif

struct TContainerCacheOnContext : private TNonCopyable {
    TContainerCacheOnContext(TComputationMutables& mutables);

    NUdf::TUnboxedValuePod NewArray(TComputationContext& ctx, ui64 size, NUdf::TUnboxedValue*& items) const;
#ifndef MKQL_DISABLE_CODEGEN
    llvm::Value* GenNewArray(ui64 sz, llvm::Value* items, const TCodegenContext& ctx, llvm::BasicBlock*& block) const;
#endif
    const ui32 Index;
};

//////////////////////////////////////////////////////////////////////////////
// TNodeFactory
//////////////////////////////////////////////////////////////////////////////
class TNodeFactory: private TNonCopyable
{
public:
    TNodeFactory(TMemoryUsageInfo& memInfo, TComputationMutables& mutables);

    IComputationNode* CreateTypeNode(TType* type) const;

    IComputationNode* CreateImmutableNode(NUdf::TUnboxedValue&& value) const;

    IComputationNode* CreateEmptyNode() const;

    IComputationNode* CreateArrayNode(TComputationNodePtrVector&& items) const;

    IComputationNode* CreateOptionalNode(IComputationNode* item) const;

    IComputationNode* CreateDictNode(
            std::vector<std::pair<IComputationNode*, IComputationNode*>>&& items,
            const TKeyTypes& types, bool isTuple, TType* encodedType,
            NUdf::IHash::TPtr hash, NUdf::IEquate::TPtr equate, NUdf::ICompare::TPtr compare, bool isSorted) const;

    IComputationNode* CreateVariantNode(IComputationNode* item, ui32 index) const;

private:
    TMemoryUsageInfo& MemInfo;
    TComputationMutables& Mutables;
};

} // namespace NMiniKQL
} // namespace NKikimr
