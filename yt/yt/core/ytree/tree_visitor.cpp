#include "tree_visitor.h"
#include "helpers.h"
#include "attributes.h"
#include "node.h"
#include "convert.h"

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/yson/producer.h>
#include <yt/yt/core/yson/async_consumer.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

//! Traverses a YTree and invokes appropriate methods of IYsonConsumer.
class TTreeVisitor
    : private TNonCopyable
{
public:
    TTreeVisitor(
        IAsyncYsonConsumer* consumer,
        bool stable,
        const TAttributeFilter& attributeFilter,
        bool skipEntityMapChildren)
        : Consumer(consumer)
        , Stable_(stable)
        , AttributeFilter_(attributeFilter)
        , SkipEntityMapChildren(skipEntityMapChildren)
    { }

    void Visit(const INodePtr& root)
    {
        VisitAny(root, true);
    }

private:
    IAsyncYsonConsumer* const Consumer;
    const bool Stable_;
    const TAttributeFilter AttributeFilter_;
    const bool SkipEntityMapChildren;

    void VisitAny(const INodePtr& node, bool isRoot = false)
    {
        node->WriteAttributes(Consumer, AttributeFilter_, Stable_);

        static const TString opaqueAttributeName("opaque");
        if (!isRoot &&
            node->Attributes().Get<bool>(opaqueAttributeName, false))
        {
            // This node is opaque, i.e. replaced by entity during tree traversal.
            Consumer->OnEntity();
            return;
        }

        switch (node->GetType()) {
            case ENodeType::String:
            case ENodeType::Int64:
            case ENodeType::Uint64:
            case ENodeType::Double:
            case ENodeType::Boolean:
                VisitScalar(node);
                break;

            case ENodeType::Entity:
                VisitEntity(node);
                break;

            case ENodeType::List:
                VisitList(node->AsList());
                break;

            case ENodeType::Map:
                VisitMap(node->AsMap());
                break;

            default:
                YT_ABORT();
        }
    }

    void VisitScalar(const INodePtr& node)
    {
        switch (node->GetType()) {
            case ENodeType::String:
                Consumer->OnStringScalar(node->AsString()->GetValue());
                break;

            case ENodeType::Int64:
                Consumer->OnInt64Scalar(node->AsInt64()->GetValue());
                break;

            case ENodeType::Uint64:
                Consumer->OnUint64Scalar(node->AsUint64()->GetValue());
                break;

            case ENodeType::Double:
                Consumer->OnDoubleScalar(node->AsDouble()->GetValue());
                break;

            case ENodeType::Boolean:
                Consumer->OnBooleanScalar(node->AsBoolean()->GetValue());
                break;

            default:
                YT_ABORT();
        }
    }

    void VisitEntity(const INodePtr& /*node*/)
    {
        Consumer->OnEntity();
    }

    void VisitList(const IListNodePtr& node)
    {
        Consumer->OnBeginList();
        for (int i = 0; i < node->GetChildCount(); ++i) {
            Consumer->OnListItem();
            VisitAny(node->GetChildOrThrow(i));
        }
        Consumer->OnEndList();
    }

    void VisitMap(const IMapNodePtr& node)
    {
        Consumer->OnBeginMap();
        auto children = node->GetChildren();
        if (Stable_) {
            using TPair = std::pair<TString, INodePtr>;
            std::sort(
                children.begin(),
                children.end(),
                [] (const TPair& lhs, const TPair& rhs) {
                    return lhs.first < rhs.first;
                });
        }
        for (const auto& [key, child] : children) {
            if (SkipEntityMapChildren && child->GetType() == ENodeType::Entity) {
                continue;
            }
            Consumer->OnKeyedItem(key);
            VisitAny(child);
        }
        Consumer->OnEndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

void VisitTree(
    INodePtr root,
    IYsonConsumer* consumer,
    bool stable,
    const TAttributeFilter& attributeFilter,
    bool skipEntityMapChildren)
{
    TAsyncYsonConsumerAdapter adapter(consumer);
    VisitTree(
        std::move(root),
        &adapter,
        stable,
        attributeFilter,
        skipEntityMapChildren);
}

void VisitTree(
    INodePtr root,
    IAsyncYsonConsumer* consumer,
    bool stable,
    const TAttributeFilter& attributeFilter,
    bool skipEntityMapChildren)
{
    TTreeVisitor treeVisitor(
        consumer,
        stable,
        attributeFilter,
        skipEntityMapChildren);
    treeVisitor.Visit(root);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
