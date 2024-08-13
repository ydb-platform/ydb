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
    TTreeBuilder(INodeFactory* factory, int treeSizeLimit)
        : Factory_(factory)
        , TreeSizeLimit_(treeSizeLimit)
    {
        YT_ASSERT(Factory_);
    }

    void BeginTree() override
    {
        YT_VERIFY(NodeStack_.size() == 0);
    }

    INodePtr EndTree() override
    {
        // Failure here means that the tree is not fully constructed yet.
        YT_VERIFY(NodeStack_.size() == 0);
        YT_VERIFY(ResultNode_);

        return ResultNode_;
    }

    void OnNode(INodePtr node) override
    {
        AddNode(node, false);
    }

    void OnMyStringScalar(TStringBuf value) override
    {
        auto node = Factory_->CreateString();
        node->SetValue(TString(value));
        AddNode(node, false);
    }

    void OnMyInt64Scalar(i64 value) override
    {
        auto node = Factory_->CreateInt64();
        node->SetValue(value);
        AddNode(node, false);
    }

    void OnMyUint64Scalar(ui64 value) override
    {
        auto node = Factory_->CreateUint64();
        node->SetValue(value);
        AddNode(node, false);
    }

    void OnMyDoubleScalar(double value) override
    {
        auto node = Factory_->CreateDouble();
        node->SetValue(value);
        AddNode(node, false);
    }

    void OnMyBooleanScalar(bool value) override
    {
        auto node = Factory_->CreateBoolean();
        node->SetValue(value);
        AddNode(node, false);
    }

    void OnMyEntity() override
    {
        AddNode(Factory_->CreateEntity(), false);
    }


    void OnMyBeginList() override
    {
        AddNode(Factory_->CreateList(), true);
    }

    void OnMyListItem() override
    {
        YT_ASSERT(!Key_);
    }

    void OnMyEndList() override
    {
        NodeStack_.pop();
    }


    void OnMyBeginMap() override
    {
        AddNode(Factory_->CreateMap(), true);
    }

    void OnMyKeyedItem(TStringBuf key) override
    {
        Key_ = TString(key);
    }

    void OnMyEndMap() override
    {
        NodeStack_.pop();
    }

    void OnMyBeginAttributes() override
    {
        YT_ASSERT(!AttributeConsumer_);
        Attributes_ = CreateEphemeralAttributes();
        AttributeConsumer_ = std::make_unique<TAttributeConsumer>(Attributes_.Get());
        Forward(AttributeConsumer_.get(), nullptr, NYson::EYsonType::MapFragment);
    }

    void OnMyEndAttributes() override
    {
        AttributeConsumer_.reset();
        YT_ASSERT(Attributes_);
    }

private:
    INodeFactory* const Factory_;

    //! Contains nodes forming the current path in the tree.
    std::stack<INodePtr> NodeStack_;
    std::optional<TString> Key_;
    INodePtr ResultNode_;
    std::unique_ptr<TAttributeConsumer> AttributeConsumer_;
    IAttributeDictionaryPtr Attributes_;

    const int TreeSizeLimit_;
    int TreeSize_ = 0;

    void AddNode(INodePtr node, bool push)
    {
        if (Attributes_) {
            node->MutableAttributes()->MergeFrom(*Attributes_);
            Attributes_ = nullptr;
        }

        if (++TreeSize_ > TreeSizeLimit_) {
            THROW_ERROR_EXCEPTION("Tree size limit exceeded")
                << TErrorAttribute("tree_size_limit", TreeSizeLimit_);
        }

        if (NodeStack_.empty()) {
            ResultNode_ = node;
        } else {
            auto collectionNode = NodeStack_.top();
            if (Key_) {
                if (!collectionNode->AsMap()->AddChild(*Key_, node)) {
                    THROW_ERROR_EXCEPTION("Duplicate key %Qv", *Key_);
                }
                Key_.reset();
            } else {
                collectionNode->AsList()->AddChild(node);
            }
        }

        if (push) {
            NodeStack_.push(node);
        }
    }
};

std::unique_ptr<ITreeBuilder> CreateBuilderFromFactory(
    INodeFactory* factory,
    int treeSizeLimit)
{
    return std::unique_ptr<ITreeBuilder>(new TTreeBuilder(std::move(factory), treeSizeLimit));
}

std::unique_ptr<ITreeBuilder> CreateBuilderFromFactory(
    INodeFactory* factory)
{
    return CreateBuilderFromFactory(factory, std::numeric_limits<int>::max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
