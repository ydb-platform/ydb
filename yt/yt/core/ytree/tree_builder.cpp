#include "tree_builder.h"
#include "attribute_consumer.h"
#include "helpers.h"
#include "attributes.h"
#include "attribute_consumer.h"
#include "node.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/yson/forwarding_consumer.h>

#include <library/cpp/yt/assert/assert.h>

#include <stack>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TTreeBuilder
    : public NYson::TForwardingYsonConsumer
    , public ITreeBuilder
{
public:
    explicit TTreeBuilder(INodeFactory* factory)
        : Factory(factory)
    {
        YT_ASSERT(Factory);
    }

    void BeginTree() override
    {
        YT_VERIFY(NodeStack.size() == 0);
    }

    INodePtr EndTree() override
    {
        // Failure here means that the tree is not fully constructed yet.
        YT_VERIFY(NodeStack.size() == 0);
        YT_VERIFY(ResultNode);

        return ResultNode;
    }

    void OnNode(INodePtr node) override
    {
        AddNode(node, false);
    }

    void OnMyStringScalar(TStringBuf value) override
    {
        auto node = Factory->CreateString();
        node->SetValue(TString(value));
        AddNode(node, false);
    }

    void OnMyInt64Scalar(i64 value) override
    {
        auto node = Factory->CreateInt64();
        node->SetValue(value);
        AddNode(node, false);
    }

    void OnMyUint64Scalar(ui64 value) override
    {
        auto node = Factory->CreateUint64();
        node->SetValue(value);
        AddNode(node, false);
    }

    void OnMyDoubleScalar(double value) override
    {
        auto node = Factory->CreateDouble();
        node->SetValue(value);
        AddNode(node, false);
    }

    void OnMyBooleanScalar(bool value) override
    {
        auto node = Factory->CreateBoolean();
        node->SetValue(value);
        AddNode(node, false);
    }

    void OnMyEntity() override
    {
        AddNode(Factory->CreateEntity(), false);
    }


    void OnMyBeginList() override
    {
        AddNode(Factory->CreateList(), true);
    }

    void OnMyListItem() override
    {
        YT_ASSERT(!Key);
    }

    void OnMyEndList() override
    {
        NodeStack.pop();
    }


    void OnMyBeginMap() override
    {
        AddNode(Factory->CreateMap(), true);
    }

    void OnMyKeyedItem(TStringBuf key) override
    {
        Key = TString(key);
    }

    void OnMyEndMap() override
    {
        NodeStack.pop();
    }

    void OnMyBeginAttributes() override
    {
        YT_ASSERT(!AttributeConsumer);
        Attributes = CreateEphemeralAttributes();
        AttributeConsumer = std::make_unique<TAttributeConsumer>(Attributes.Get());
        Forward(AttributeConsumer.get(), nullptr, NYson::EYsonType::MapFragment);
    }

    void OnMyEndAttributes() override
    {
        AttributeConsumer.reset();
        YT_ASSERT(Attributes);
    }

private:
    INodeFactory* const Factory;

    //! Contains nodes forming the current path in the tree.
    std::stack<INodePtr> NodeStack;
    std::optional<TString> Key;
    INodePtr ResultNode;
    std::unique_ptr<TAttributeConsumer> AttributeConsumer;
    IAttributeDictionaryPtr Attributes;

    void AddNode(INodePtr node, bool push)
    {
        if (Attributes) {
            node->MutableAttributes()->MergeFrom(*Attributes);
            Attributes = nullptr;
        }

        if (NodeStack.empty()) {
            ResultNode = node;
        } else {
            auto collectionNode = NodeStack.top();
            if (Key) {
                if (!collectionNode->AsMap()->AddChild(*Key, node)) {
                    THROW_ERROR_EXCEPTION("Duplicate key %Qv", *Key);
                }
                Key.reset();
            } else {
                collectionNode->AsList()->AddChild(node);
            }
        }

        if (push) {
            NodeStack.push(node);
        }
    }
};

std::unique_ptr<ITreeBuilder> CreateBuilderFromFactory(INodeFactory* factory)
{
    return std::unique_ptr<ITreeBuilder>(new TTreeBuilder(std::move(factory)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
