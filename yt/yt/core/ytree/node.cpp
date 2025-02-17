#include "node.h"
#include "convert.h"
#include "node_detail.h"
#include "tree_visitor.h"

#include <yt/yt/core/yson/writer.h>

#include <library/cpp/yt/misc/cast.h>

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

INodePtr IMapNode::GetChildOrThrow(const std::string& key) const
{
    auto child = FindChild(key);
    if (!child) {
        ThrowNoSuchChildKey(this, key);
    }
    return child;
}

std::string IMapNode::GetChildKeyOrThrow(const IConstNodePtr& child)
{
    auto optionalKey = FindChildKey(child);
    if (!optionalKey) {
        THROW_ERROR_EXCEPTION("Node is not a child");
    }
    return *optionalKey;
}

////////////////////////////////////////////////////////////////////////////////

INodePtr IListNode::GetChildOrThrow(int index) const
{
    auto child = FindChild(index);
    if (!child) {
        ThrowNoSuchChildIndex(this, index);
    }
    return child;
}

int IListNode::GetChildIndexOrThrow(const IConstNodePtr& child)
{
    auto optionalIndex = FindChildIndex(child);
    if (!optionalIndex) {
        THROW_ERROR_EXCEPTION("Node is not a child");
    }
    return *optionalIndex;
}

int IListNode::AdjustChildIndexOrThrow(int index) const
{
    auto adjustedIndex = NYPath::TryAdjustListIndex(index, GetChildCount());
    if (!adjustedIndex) {
        ThrowNoSuchChildIndex(this, index);
    }
    return *adjustedIndex;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const INode& value, IYsonConsumer* consumer)
{
    VisitTree(const_cast<INode*>(&value), consumer, /*stable*/ true, TAttributeFilter());
}

void Deserialize(INodePtr& value, const INodePtr& node)
{
    value = node;
}

#define DESERIALIZE_TYPED(type) \
    void Deserialize(I##type##NodePtr& value, const INodePtr& node) \
    { \
        value = node->As##type(); \
    } \
    \
    void Deserialize(I##type##NodePtr& value, NYson::TYsonPullParserCursor* cursor) \
    { \
        auto node = ExtractTo<INodePtr>(cursor); \
        value = node->As##type(); \
    }

DESERIALIZE_TYPED(String)
DESERIALIZE_TYPED(Int64)
DESERIALIZE_TYPED(Uint64)
DESERIALIZE_TYPED(Double)
DESERIALIZE_TYPED(Boolean)
DESERIALIZE_TYPED(Map)
DESERIALIZE_TYPED(List)
DESERIALIZE_TYPED(Entity)

#undef DESERIALIZE_TYPED

////////////////////////////////////////////////////////////////////////////////

void Deserialize(INodePtr& value, NYson::TYsonPullParserCursor* cursor)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    cursor->TransferComplexValue(builder.get());
    value = builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
