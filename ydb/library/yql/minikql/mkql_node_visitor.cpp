#include "mkql_node_visitor.h"
#include "mkql_node.h"

#include <util/generic/algorithm.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NDetail;

const ui64 IS_NODE_ENTERED = 1;
const ui64 IS_NODE_EXITED = 2;

void TThrowingNodeVisitor::Visit(TTypeType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TVoidType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TNullType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TEmptyListType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TEmptyDictType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TDataType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TPgType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TStructType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TListType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TOptionalType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TDictType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TCallableType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TAnyType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TTupleType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TResourceType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TVariantType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TVoid& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TNull& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TEmptyList& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TEmptyDict& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TDataLiteral& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TStructLiteral& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TListLiteral& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TOptionalLiteral& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TDictLiteral& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TCallable& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TAny& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TTupleLiteral& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TVariantLiteral& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TStreamType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TFlowType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TTaggedType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TBlockType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::Visit(TMultiType& node) {
    Y_UNUSED(node);
    ThrowUnexpectedNodeType();
}

void TThrowingNodeVisitor::ThrowUnexpectedNodeType() {
    THROW yexception() << "Unexpected node type";
}

void TEmptyNodeVisitor::Visit(TTypeType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TVoidType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TNullType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TEmptyListType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TEmptyDictType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TDataType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TPgType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TStructType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TListType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TOptionalType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TDictType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TCallableType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TAnyType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TTupleType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TResourceType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TVariantType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TVoid& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TNull& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TEmptyList& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TEmptyDict& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TDataLiteral& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TStructLiteral& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TListLiteral& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TOptionalLiteral& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TDictLiteral& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TCallable& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TAny& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TTupleLiteral& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TVariantLiteral& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TStreamType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TFlowType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TTaggedType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TBlockType& node) {
    Y_UNUSED(node);
}

void TEmptyNodeVisitor::Visit(TMultiType& node) {
    Y_UNUSED(node);
}

void TExploringNodeVisitor::Visit(TTypeType& node) {
    Y_DEBUG_ABORT_UNLESS(node.GetType() == &node);
}

void TExploringNodeVisitor::Visit(TVoidType& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TNullType& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TEmptyListType& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TEmptyDictType& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TDataType& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TPgType& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TStructType& node) {
    AddChildNode(&node, *node.GetType());
    for (ui32 i = 0, e = node.GetMembersCount(); i < e; ++i) {
        AddChildNode(&node, *node.GetMemberType(i));
    }
}

void TExploringNodeVisitor::Visit(TListType& node) {
    AddChildNode(&node, *node.GetType());
    AddChildNode(&node, *node.GetItemType());
}

void TExploringNodeVisitor::Visit(TOptionalType& node) {
    AddChildNode(&node, *node.GetType());
    AddChildNode(&node, *node.GetItemType());
}

void TExploringNodeVisitor::Visit(TDictType& node) {
    AddChildNode(&node, *node.GetType());
    AddChildNode(&node, *node.GetKeyType());
    AddChildNode(&node, *node.GetPayloadType());
}

void TExploringNodeVisitor::Visit(TCallableType& node) {
    AddChildNode(&node, *node.GetType());
    AddChildNode(&node, *node.GetReturnType());
    for (ui32 i = 0, e = node.GetArgumentsCount(); i < e; ++i) {
        AddChildNode(&node, *node.GetArgumentType(i));
    }

    if (node.GetPayload()) {
        AddChildNode(&node, *node.GetPayload());
    }
}

void TExploringNodeVisitor::Visit(TAnyType& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TTupleType& node) {
    AddChildNode(&node, *node.GetType());
    for (ui32 i = 0, e = node.GetElementsCount(); i < e; ++i) {
        AddChildNode(&node, *node.GetElementType(i));
    }
}

void TExploringNodeVisitor::Visit(TResourceType& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TVariantType& node) {
    AddChildNode(&node, *node.GetType());
    AddChildNode(&node, *node.GetUnderlyingType());
}

void TExploringNodeVisitor::Visit(TVoid& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TNull& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TEmptyList& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TEmptyDict& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TDataLiteral& node) {
    AddChildNode(&node, *node.GetType());
}

void TExploringNodeVisitor::Visit(TStructLiteral& node) {
    AddChildNode(&node, *node.GetType());
    for (ui32 i = 0, e = node.GetValuesCount(); i < e; ++i) {
        AddChildNode(&node, *node.GetValue(i).GetNode());
    }
}

void TExploringNodeVisitor::Visit(TListLiteral& node) {
    AddChildNode(&node, *node.GetType());
    for (ui32 i = 0; i < node.GetItemsCount(); ++i) {
        AddChildNode(&node, *node.GetItems()[i].GetNode());
    }
}

void TExploringNodeVisitor::Visit(TOptionalLiteral& node) {
    AddChildNode(&node, *node.GetType());
    if (node.HasItem()) {
        AddChildNode(&node, *node.GetItem().GetNode());
    }
}

void TExploringNodeVisitor::Visit(TDictLiteral& node) {
    AddChildNode(&node, *node.GetType());
    for (ui32 i = 0, e = node.GetItemsCount(); i < e; ++i) {
        auto item = node.GetItem(i);
        AddChildNode(&node, *item.first.GetNode());
        AddChildNode(&node, *item.second.GetNode());
    }
}

void TExploringNodeVisitor::Visit(TCallable& node) {
    AddChildNode(&node, *node.GetType());
    for (ui32 i = 0, e = node.GetInputsCount(); i < e; ++i) {
        AddChildNode(&node, *node.GetInput(i).GetNode());
    }

    if (node.HasResult())
        AddChildNode(&node, *node.GetResult().GetNode());
}

void TExploringNodeVisitor::Visit(TAny& node) {
    AddChildNode(&node, *node.GetType());
    if (node.HasItem()) {
        AddChildNode(&node, *node.GetItem().GetNode());
    }
}

void TExploringNodeVisitor::Visit(TTupleLiteral& node) {
    AddChildNode(&node, *node.GetType());
    for (ui32 i = 0, e = node.GetValuesCount(); i < e; ++i) {
        AddChildNode(&node, *node.GetValue(i).GetNode());
    }
}

void TExploringNodeVisitor::Visit(TVariantLiteral& node) {
    AddChildNode(&node, *node.GetType());
    AddChildNode(&node, *node.GetItem().GetNode());
}

void TExploringNodeVisitor::Visit(TStreamType& node) {
    AddChildNode(&node, *node.GetType());
    AddChildNode(&node, *node.GetItemType());
}

void TExploringNodeVisitor::Visit(TFlowType& node) {
    AddChildNode(&node, *node.GetType());
    AddChildNode(&node, *node.GetItemType());
}

void TExploringNodeVisitor::Visit(TTaggedType& node) {
    AddChildNode(&node, *node.GetType());
    AddChildNode(&node, *node.GetBaseType());
}

void TExploringNodeVisitor::Visit(TBlockType& node) {
    AddChildNode(&node, *node.GetType());
    AddChildNode(&node, *node.GetItemType());
}

void TExploringNodeVisitor::Visit(TMultiType& node) {
    AddChildNode(&node, *node.GetType());
    for (ui32 i = 0, e = node.GetElementsCount(); i < e; ++i) {
        AddChildNode(&node, *node.GetElementType(i));
    }
}

void TExploringNodeVisitor::AddChildNode(TNode* parent, TNode& child) {
    Stack->push_back(&child);

    if (BuildConsumersMap) {
        if (parent != nullptr) {
            ConsumersMap[&child].push_back(parent);
        } else {
            ConsumersMap[&child] = {};
        }
    }
}

void TExploringNodeVisitor::Clear() {
    NodeList.clear();
    Stack = nullptr;
    ConsumersMap.clear();
}

void TExploringNodeVisitor::Walk(TNode* root, const TTypeEnvironment& env, const std::vector<TNode*>& terminalNodes,
    bool buildConsumersMap, size_t nodesCountHint)
{
    BuildConsumersMap = buildConsumersMap;

    Clear();

    if (BuildConsumersMap && nodesCountHint) {
        ConsumersMap.reserve(nodesCountHint);
    }

    Stack = &env.GetNodeStack();
    Stack->clear();
    AddChildNode(nullptr, *root);
    while (!Stack->empty()) {
        auto node = Stack->back();

        if (node->GetCookie() == 0) {
            node->SetCookie(IS_NODE_ENTERED);

            if (Find(terminalNodes.begin(), terminalNodes.end(), node) == terminalNodes.end()) {
                node->Accept(*this);
            }
        } else {
            if (node->GetCookie() == IS_NODE_ENTERED) {
                NodeList.push_back(node);
                node->SetCookie(IS_NODE_EXITED);
            } else {
                Y_ABORT_UNLESS(node->GetCookie() <= IS_NODE_EXITED, "TNode graph should not be reused");
            }

            Stack->pop_back();
            continue;
        }
    }

    for (auto node : NodeList) {
        node->SetCookie(0);
    }

    Stack = nullptr;
}

const std::vector<TNode*>& TExploringNodeVisitor::GetNodes() {
    return NodeList;
}

const TExploringNodeVisitor::TNodesVec& TExploringNodeVisitor::GetConsumerNodes(TNode& node) {
    Y_ABORT_UNLESS(BuildConsumersMap);
    const auto consumers = ConsumersMap.find(&node);
    Y_ABORT_UNLESS(consumers != ConsumersMap.cend());
    return consumers->second;
}

template <bool InPlace>
TRuntimeNode SinglePassVisitCallablesImpl(TRuntimeNode root, TExploringNodeVisitor& explorer,
    const TCallableVisitFuncProvider& funcProvider, const TTypeEnvironment& env, bool& wereChanges)
{
    auto& nodes = explorer.GetNodes();

    wereChanges = false;

    for (TNode* exploredNode : nodes) {
        TNode* node;
        if (!InPlace) {
            node = exploredNode->CloneOnCallableWrite(env);
        } else {
            node = exploredNode;
            node->Freeze(env);
        }

        if (node->GetType()->IsCallable()) {
            auto& callable = static_cast<TCallable&>(*node);
            if (!callable.HasResult()) {
                const auto& callableType = callable.GetType();
                const auto& name = callableType->GetNameStr();
                const auto& func = funcProvider(name);
                if (func) {
                    TRuntimeNode result = func(callable, env);
                    result.Freeze();
                    if (result.GetNode() != node) {
                        if (InPlace) {
                            callable.SetResult(result, env);
                            wereChanges = true;
                        } else {
                            TNode* wrappedResult = TCallable::Create(result, callable.GetType(), env);
                            exploredNode->SetCookie((ui64)wrappedResult);
                        }

                        continue;
                    }
                }
            }
        }

        if (!InPlace && node != exploredNode) {
            exploredNode->SetCookie((ui64)node);
        }
    }

    if (!InPlace) {
        auto newRoot = (TNode*)root.GetNode()->GetCookie();
        if (newRoot) {
            root = TRuntimeNode(newRoot, root.IsImmediate());
            wereChanges = true;
        }
    }

    root.Freeze();
    if (!InPlace) {
        for (TNode* exploredNode : nodes) {
            exploredNode->SetCookie(0);
        }
    }

    return root;
}

TRuntimeNode SinglePassVisitCallables(TRuntimeNode root, TExploringNodeVisitor& explorer,
    const TCallableVisitFuncProvider& funcProvider, const TTypeEnvironment& env, bool inPlace, bool& wereChanges) {
    if (inPlace) {
        return SinglePassVisitCallablesImpl<true>(root, explorer, funcProvider, env, wereChanges);
    } else {
        return SinglePassVisitCallablesImpl<false>(root, explorer, funcProvider, env, wereChanges);
    }
}

}
}
