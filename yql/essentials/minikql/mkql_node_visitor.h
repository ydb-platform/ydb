#pragma once
#include "defs.h"
#include "mkql_node.h"
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <functional>

namespace NKikimr {
namespace NMiniKQL {

class INodeVisitor {
public:
    virtual ~INodeVisitor() {}
    virtual void Visit(TTypeType& node) = 0;
    virtual void Visit(TVoidType& node) = 0;
    virtual void Visit(TNullType& node) = 0;
    virtual void Visit(TEmptyListType& node) = 0;
    virtual void Visit(TEmptyDictType& node) = 0;
    virtual void Visit(TDataType& node) = 0;
    virtual void Visit(TPgType& node) = 0;
    virtual void Visit(TStructType& node) = 0;
    virtual void Visit(TListType& node) = 0;
    virtual void Visit(TOptionalType& node) = 0;
    virtual void Visit(TDictType& node) = 0;
    virtual void Visit(TCallableType& node) = 0;
    virtual void Visit(TAnyType& node) = 0;
    virtual void Visit(TTupleType& node) = 0;
    virtual void Visit(TResourceType& node) = 0;
    virtual void Visit(TVariantType& node) = 0;
    virtual void Visit(TVoid& node) = 0;
    virtual void Visit(TNull& node) = 0;
    virtual void Visit(TEmptyList& node) = 0;
    virtual void Visit(TEmptyDict& node) = 0;
    virtual void Visit(TDataLiteral& node) = 0;
    virtual void Visit(TStructLiteral& node) = 0;
    virtual void Visit(TListLiteral& node) = 0;
    virtual void Visit(TOptionalLiteral& node) = 0;
    virtual void Visit(TDictLiteral& node) = 0;
    virtual void Visit(TCallable& node) = 0;
    virtual void Visit(TAny& node) = 0;
    virtual void Visit(TTupleLiteral& node) = 0;
    virtual void Visit(TVariantLiteral& node) = 0;
    virtual void Visit(TStreamType& node) = 0;
    virtual void Visit(TFlowType& node) = 0;
    virtual void Visit(TTaggedType& node) = 0;
    virtual void Visit(TBlockType& node) = 0;
    virtual void Visit(TMultiType& node) = 0;
};

class TThrowingNodeVisitor : public INodeVisitor {
public:
    void Visit(TTypeType& node) override;
    void Visit(TVoidType& node) override;
    void Visit(TNullType& node) override;
    void Visit(TEmptyListType& node) override;
    void Visit(TEmptyDictType& node) override;
    void Visit(TDataType& node) override;
    void Visit(TPgType& node) override;
    void Visit(TStructType& node) override;
    void Visit(TListType& node) override;
    void Visit(TOptionalType& node) override;
    void Visit(TDictType& node) override;
    void Visit(TCallableType& node) override;
    void Visit(TAnyType& node) override;
    void Visit(TTupleType& node) override;
    void Visit(TResourceType& node) override;
    void Visit(TVariantType& node) override;
    void Visit(TVoid& node) override;
    void Visit(TNull& node) override;
    void Visit(TEmptyList& node) override;
    void Visit(TEmptyDict& node) override;
    void Visit(TDataLiteral& node) override;
    void Visit(TStructLiteral& node) override;
    void Visit(TListLiteral& node) override;
    void Visit(TOptionalLiteral& node) override;
    void Visit(TDictLiteral& node) override;
    void Visit(TCallable& node) override;
    void Visit(TAny& node) override;
    void Visit(TTupleLiteral& node) override;
    void Visit(TVariantLiteral& node) override;
    void Visit(TStreamType& node) override;
    void Visit(TFlowType& node) override;
    void Visit(TTaggedType& node) override;
    void Visit(TBlockType& node) override;
    void Visit(TMultiType& node) override;

protected:
    static void ThrowUnexpectedNodeType();
};

class TEmptyNodeVisitor: public INodeVisitor {
public:
    void Visit(TTypeType& node) override;
    void Visit(TVoidType& node) override;
    void Visit(TNullType& node) override;
    void Visit(TEmptyListType& node) override;
    void Visit(TEmptyDictType& node) override;
    void Visit(TDataType& node) override;
    void Visit(TPgType& node) override;
    void Visit(TStructType& node) override;
    void Visit(TListType& node) override;
    void Visit(TOptionalType& node) override;
    void Visit(TDictType& node) override;
    void Visit(TCallableType& node) override;
    void Visit(TAnyType& node) override;
    void Visit(TTupleType& node) override;
    void Visit(TResourceType& node) override;
    void Visit(TVariantType& node) override;
    void Visit(TVoid& node) override;
    void Visit(TNull& node) override;
    void Visit(TEmptyList& node) override;
    void Visit(TEmptyDict& node) override;
    void Visit(TDataLiteral& node) override;
    void Visit(TStructLiteral& node) override;
    void Visit(TListLiteral& node) override;
    void Visit(TOptionalLiteral& node) override;
    void Visit(TDictLiteral& node) override;
    void Visit(TCallable& node) override;
    void Visit(TAny& node) override;
    void Visit(TTupleLiteral& node) override;
    void Visit(TVariantLiteral& node) override;
    void Visit(TStreamType& node) override;
    void Visit(TFlowType& node) override;
    void Visit(TTaggedType& node) override;
    void Visit(TBlockType& node) override;
    void Visit(TMultiType& node) override;
};

class TExploringNodeVisitor : public INodeVisitor {
public:
    using TNodesVec = TStackVec<TNode*, 2>;
public:
    void Visit(TTypeType& node) override;
    void Visit(TVoidType& node) override;
    void Visit(TNullType& node) override;
    void Visit(TEmptyListType& node) override;
    void Visit(TEmptyDictType& node) override;
    void Visit(TDataType& node) override;
    void Visit(TPgType& node) override;
    void Visit(TStructType& node) override;
    void Visit(TListType& node) override;
    void Visit(TOptionalType& node) override;
    void Visit(TDictType& node) override;
    void Visit(TCallableType& node) override;
    void Visit(TAnyType& node) override;
    void Visit(TTupleType& node) override;
    void Visit(TResourceType& node) override;
    void Visit(TVariantType& node) override;
    void Visit(TVoid& node) override;
    void Visit(TNull& node) override;
    void Visit(TEmptyList& node) override;
    void Visit(TEmptyDict& node) override;
    void Visit(TDataLiteral& node) override;
    void Visit(TStructLiteral& node) override;
    void Visit(TListLiteral& node) override;
    void Visit(TOptionalLiteral& node) override;
    void Visit(TDictLiteral& node) override;
    void Visit(TCallable& node) override;
    void Visit(TAny& node) override;
    void Visit(TTupleLiteral& node) override;
    void Visit(TVariantLiteral& node) override;
    void Visit(TStreamType& node) override;
    void Visit(TFlowType& node) override;
    void Visit(TTaggedType& node) override;
    void Visit(TBlockType& node) override;
    void Visit(TMultiType& node) override;

    void Walk(TNode* root, const TTypeEnvironment& env, const std::vector<TNode*>& terminalNodes = std::vector<TNode*>(),
        bool buildConsumersMap = false, size_t nodesCountHint = 0);
    const std::vector<TNode*>& GetNodes();
    const TNodesVec& GetConsumerNodes(TNode& node);
    void Clear();

private:
    void AddChildNode(TNode* parent, TNode& child);

private:
    std::vector<TNode*> NodeList;
    std::vector<TNode*>* Stack = nullptr;
    bool BuildConsumersMap = false;
    std::unordered_map<TNode*, TNodesVec> ConsumersMap;
};

class TTypeEnvironment;
typedef std::function<TRuntimeNode (TCallable& callable, const TTypeEnvironment& env)> TCallableVisitFunc;
typedef std::function<TCallableVisitFunc (const TInternName& name)> TCallableVisitFuncProvider;

TRuntimeNode SinglePassVisitCallables(TRuntimeNode root, TExploringNodeVisitor& explorer,
    const TCallableVisitFuncProvider& funcProvider, const TTypeEnvironment& env, bool inPlace, bool& wereChanges);

}
}
