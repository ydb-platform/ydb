#include "yql_expr_builder.h"
#include "yql_expr.h"

namespace NYql {

TExprNodeBuilder::TExprNodeBuilder(TPositionHandle pos, TExprContext& ctx)
    : Ctx_(ctx)
    , Parent_(nullptr)
    , ParentReplacer_(nullptr)
    , Container_(nullptr)
    , Pos_(pos)
    , CurrentNode_(nullptr)
{}

TExprNodeBuilder::TExprNodeBuilder(TPositionHandle pos, TExprContext& ctx, ExtArgsFuncType extArgsFunc)
    : Ctx_(ctx)
    , Parent_(nullptr)
    , ParentReplacer_(nullptr)
    , Container_(nullptr)
    , Pos_(pos)
    , CurrentNode_(nullptr)
    , ExtArgsFunc_(extArgsFunc)
{}

TExprNodeBuilder::TExprNodeBuilder(TPositionHandle pos, TExprNodeBuilder* parent, const TExprNode::TPtr& container)
    : Ctx_(parent->Ctx_)
    , Parent_(parent)
    , ParentReplacer_(nullptr)
    , Container_(std::move(container))
    , Pos_(pos)
    , CurrentNode_(nullptr)
{
    if (Parent_) {
        ExtArgsFunc_ = Parent_->ExtArgsFunc_;
    }
}

TExprNodeBuilder::TExprNodeBuilder(TPositionHandle pos, TExprNodeReplaceBuilder* parentReplacer)
    : Ctx_(parentReplacer->Owner_->Ctx_)
    , Parent_(nullptr)
    , ParentReplacer_(parentReplacer)
    , Container_(nullptr)
    , Pos_(pos)
    , CurrentNode_(nullptr)
{
}

TExprNode::TPtr TExprNodeBuilder::Build() {
    Y_ENSURE(CurrentNode_, "No current node");
    Y_ENSURE(!Parent_, "Build is allowed only on top level");
    if (CurrentNode_->Type() == TExprNode::Lambda) {
        Y_ENSURE(CurrentNode_->ChildrenSize() > 0U, "Lambda is not complete");
    }

    return CurrentNode_;
}

TExprNodeBuilder& TExprNodeBuilder::Seal() {
    Y_ENSURE(Parent_, "Seal is allowed only on non-top level");
    if (Container_->Type() == TExprNode::Lambda) {
        if (CurrentNode_) {
            Y_ENSURE(Container_->ChildrenSize() == 1, "Lambda is already complete.");
            Container_->Children_.emplace_back(std::move(CurrentNode_));
        } else {
            Y_ENSURE(Container_->ChildrenSize() > 0U, "Lambda isn't complete.");
        }
    }

    return *Parent_;
}

TExprNodeReplaceBuilder& TExprNodeBuilder::Done() {
    Y_ENSURE(ParentReplacer_, "Done is allowed only if parent is a replacer");
    Y_ENSURE(CurrentNode_, "No current node");
    for (auto& body : ParentReplacer_->Body_)
        body = Ctx_.ReplaceNode(std::move(body), ParentReplacer_->CurrentNode_ ? *ParentReplacer_->CurrentNode_ : *ParentReplacer_->Args_->Child(ParentReplacer_->CurrentIndex_), CurrentNode_);
    return *ParentReplacer_;
}

TExprNodeBuilder& TExprNodeBuilder::Atom(ui32 index, TPositionHandle pos, const TStringBuf& content, ui32 flags) {
    Y_ENSURE(Container_ && !Container_->IsLambda(), "Container expected");
    Y_ENSURE(Container_->ChildrenSize() == index,
        "Container position mismatch, expected: " << Container_->ChildrenSize() <<
        ", actual: " << index);
    auto child = Ctx_.NewAtom(pos, content, flags);
    Container_->Children_.push_back(child);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Atom(TPositionHandle pos, const TStringBuf& content, ui32 flags) {
    Y_ENSURE(!Container_ || Container_->IsLambda(), "No container expected");
    Y_ENSURE(!CurrentNode_, "Node is already build");
    CurrentNode_ = Ctx_.NewAtom(pos, content, flags);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Atom(ui32 index, const TStringBuf& content, ui32 flags) {
    return Atom(index, Pos_, content, flags);
}

TExprNodeBuilder& TExprNodeBuilder::Atom(const TStringBuf& content, ui32 flags) {
    return Atom(Pos_, content, flags);
}

TExprNodeBuilder& TExprNodeBuilder::Atom(ui32 index, ui32 literalIndexValue) {
    return Atom(index, Pos_, Ctx_.GetIndexAsString(literalIndexValue), TNodeFlags::Default);
}

TExprNodeBuilder& TExprNodeBuilder::Atom(ui32 literalIndexValue) {
    return Atom(Pos_, Ctx_.GetIndexAsString(literalIndexValue), TNodeFlags::Default);
}

TExprNodeBuilder TExprNodeBuilder::List(ui32 index, TPositionHandle pos) {
    Y_ENSURE(Container_, "Container expected");
    Y_ENSURE(Container_->ChildrenSize() == index + (Container_->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container_->ChildrenSize() <<
        ", actual: " << index);
    const auto child = Ctx_.NewList(pos, {});
    Container_->Children_.push_back(child);
    return TExprNodeBuilder(pos, this, child);
}

TExprNodeBuilder TExprNodeBuilder::List(TPositionHandle pos) {
    Y_ENSURE(!Container_ || Container_->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode_, "Node is already build");
    CurrentNode_ = Ctx_.NewList(pos, {});
    return TExprNodeBuilder(pos, this, CurrentNode_);
}

TExprNodeBuilder TExprNodeBuilder::List(ui32 index) {
    return List(index, Pos_);
}

TExprNodeBuilder TExprNodeBuilder::List() {
    return List(Pos_);
}

TExprNodeBuilder& TExprNodeBuilder::Add(ui32 index, const TExprNode::TPtr& child) {
    Y_ENSURE(Container_, "Container expected");
    Y_ENSURE(Container_->ChildrenSize() == index + (Container_->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container_->ChildrenSize() <<
        ", actual: " << index);
    Y_ENSURE(child, "child should not be nullptr");
    Container_->Children_.push_back(child);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Add(ui32 index, TExprNode::TPtr&& child) {
    Y_ENSURE(Container_, "Container expected");
    Y_ENSURE(Container_->ChildrenSize() == index + (Container_->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container_->ChildrenSize() <<
        ", actual: " << index);
    Y_ENSURE(child, "child should not be nullptr");
    Container_->Children_.push_back(std::move(child));
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Add(TExprNode::TListType&& children) {
    Y_ENSURE(Container_ && Container_->Type() != TExprNode::Lambda, "Container expected");
    Y_ENSURE(Container_->Children_.empty(), "container should be empty");
    Container_->Children_ = std::move(children);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Set(TExprNode::TPtr&& body) {
    Y_ENSURE(Container_ && Container_->Type() == TExprNode::Lambda, "Lambda expected");
    Y_ENSURE(!CurrentNode_, "Lambda already has a body");
    CurrentNode_ = std::move(body);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Set(const TExprNode::TPtr& body) {
    Y_ENSURE(Container_ && Container_->Type() == TExprNode::Lambda, "Lambda expected");
    Y_ENSURE(!CurrentNode_, "Lambda already has a body");
    CurrentNode_ = body;
    return *this;
}

TExprNodeBuilder TExprNodeBuilder::Callable(ui32 index, TPositionHandle pos, const TStringBuf& content) {
    Y_ENSURE(Container_, "Container expected");
    Y_ENSURE(Container_->ChildrenSize() == index + (Container_->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container_->ChildrenSize() <<
        ", actual: " << index);
    auto child = Ctx_.NewCallable(pos, content, {});
    Container_->Children_.push_back(child);
    return TExprNodeBuilder(pos, this, child);
}

TExprNodeBuilder TExprNodeBuilder::Callable(TPositionHandle pos, const TStringBuf& content) {
    Y_ENSURE(!Container_ || Container_->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode_, "Node is already build");
    CurrentNode_ = Ctx_.NewCallable(pos, content, {});
    return TExprNodeBuilder(pos, this, CurrentNode_);
}

TExprNodeBuilder TExprNodeBuilder::Callable(ui32 index, const TStringBuf& content) {
    return Callable(index, Pos_, content);
}

TExprNodeBuilder TExprNodeBuilder::Callable(const TStringBuf& content) {
    return Callable(Pos_, content);
}

TExprNodeBuilder& TExprNodeBuilder::World(ui32 index, TPositionHandle pos) {
    Y_ENSURE(Container_, "Container expected");
    Y_ENSURE(Container_->ChildrenSize() == index + (Container_->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container_->ChildrenSize() <<
        ", actual: " << index);
    auto child = Ctx_.NewWorld(pos);
    Container_->Children_.push_back(child);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::World(TPositionHandle pos) {
    Y_ENSURE(!Container_, "No container expected");
    Y_ENSURE(!CurrentNode_, "Node is already build");
    CurrentNode_ = Ctx_.NewWorld(pos);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::World(ui32 index) {
    return World(index, Pos_);
}

TExprNodeBuilder& TExprNodeBuilder::World() {
    return World(Pos_);
}

TExprNodeBuilder TExprNodeBuilder::Lambda(ui32 index, TPositionHandle pos) {
    Y_ENSURE(Container_, "Container expected");
    Y_ENSURE(Container_->ChildrenSize() == index + (Container_->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container_->ChildrenSize() <<
        ", actual: " << index);
    auto child = Ctx_.NewLambda(pos, Ctx_.NewArguments(pos, {}), nullptr);
    Container_->Children_.push_back(child);
    return TExprNodeBuilder(pos, this, child);
}

TExprNodeBuilder TExprNodeBuilder::Lambda(TPositionHandle pos) {
    Y_ENSURE(!Container_ || Container_->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode_, "Node is already build");
    CurrentNode_ = Ctx_.NewLambda(pos, Ctx_.NewArguments(pos, {}), nullptr);
    return TExprNodeBuilder(pos, this, CurrentNode_);
}

TExprNodeBuilder TExprNodeBuilder::Lambda(ui32 index) {
    return Lambda(index, Pos_);
}

TExprNodeBuilder TExprNodeBuilder::Lambda() {
    return Lambda(Pos_);
}

TExprNodeBuilder& TExprNodeBuilder::Param(TPositionHandle pos, const TStringBuf& name) {
    Y_ENSURE(!name.empty(), "Empty parameter name is not allowed");
    Y_ENSURE(Container_, "Container expected");
    Y_ENSURE(Container_->Type() == TExprNode::Lambda, "Container must be a lambda");
    Y_ENSURE(!CurrentNode_, "Lambda already has a body");
    for (auto arg : Container_->Head().Children()) {
        Y_ENSURE(arg->Content() != name, "Duplicate of lambda param name: " << name);
    }

    Container_->Head().Children_.push_back(Ctx_.NewArgument(pos, name));
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Param(const TStringBuf& name) {
    return Param(Pos_, name);
}

TExprNodeBuilder& TExprNodeBuilder::Params(const TStringBuf& name, ui32 width) {
    Y_ENSURE(!name.empty(), "Empty parameter name is not allowed");
    for (ui32 i = 0U; i < width; ++i)
        Param(Pos_, TString(name) += ToString(i));
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Arg(ui32 index, const TStringBuf& name) {
    Y_ENSURE(!name.empty(), "Empty parameter name is not allowed");
    Y_ENSURE(Container_, "Container expected");
    Y_ENSURE(Container_->ChildrenSize() == index + (Container_->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container_->ChildrenSize() <<
        ", actual: " << index);
    auto arg = FindArgument(name);
    Container_->Children_.push_back(arg);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Arg(const TStringBuf& name) {
    Y_ENSURE(!Container_ || Container_->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode_, "Node is already build");
    CurrentNode_ = FindArgument(name);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Arg(ui32 index, const TStringBuf& name, ui32 toIndex) {
    Y_ENSURE(!name.empty(), "Empty parameter name is not allowed");
    return Arg(index, TString(name) += ToString(toIndex));
}

TExprNodeBuilder& TExprNodeBuilder::Arg(const TStringBuf& name, ui32 toIndex) {
    Y_ENSURE(!name.empty(), "Empty parameter name is not allowed");
    return Arg(TString(name) += ToString(toIndex));
}

TExprNodeBuilder& TExprNodeBuilder::Arg(const TExprNodePtr& arg) {
    Y_ENSURE(arg->Type() == TExprNode::Argument, "Argument expected");
    Y_ENSURE(!Container_ || Container_->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode_, "Node is already build");
    CurrentNode_ = arg;
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Args(ui32 index, const TStringBuf& name, ui32 toIndex) {
    Y_ENSURE(!name.empty(), "Empty parameter name is not allowed");
    for (auto i = 0U; index < toIndex; ++i)
        Arg(index++, TString(name) += ToString(i));
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Args(const TStringBuf& name, ui32 toIndex) {
    Y_ENSURE(!name.empty(), "Empty parameter name is not allowed");
    for (auto i = 0U; i < toIndex; ++i)
        Arg(i, TString(name) += ToString(i));
    return *this;
}

TExprNode::TPtr TExprNodeBuilder::FindArgument(const TStringBuf& name) {
    for (auto builder = this; builder; builder = builder->Parent_) {
        while (builder->ParentReplacer_) {
            builder = builder->ParentReplacer_->Owner_;
        }

        if (builder->Container_ && builder->Container_->IsLambda()) {
            for (const auto& arg : builder->Container_->Head().Children()) {
                if (arg->Content() == name) {
                    return arg;
                }
            }
        }
    }

    if (ExtArgsFunc_) {
        if (const auto arg = ExtArgsFunc_(name)) {
            return arg;
        }
    }

    ythrow yexception() << "Parameter not found: " << name;
}

TExprNodeReplaceBuilder TExprNodeBuilder::Apply(ui32 index, const TExprNode& lambda) {
    Y_ENSURE(Container_, "Container expected");
    Y_ENSURE(Container_->ChildrenSize() == index + (Container_->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container_->ChildrenSize() <<
        ", actual: " << index);
    return TExprNodeReplaceBuilder(this, Container_, lambda.HeadPtr(), GetLambdaBody(lambda));
}

TExprNodeReplaceBuilder TExprNodeBuilder::Apply(const TExprNode& lambda) {
    Y_ENSURE(!Container_ || Container_->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode_, "Node is already build");
    return TExprNodeReplaceBuilder(this, Container_, lambda.HeadPtr(), GetLambdaBody(lambda));
}

TExprNodeReplaceBuilder TExprNodeBuilder::Apply(ui32 index, const TExprNode::TPtr& lambda) {
    return Apply(index, *lambda);
}

TExprNodeReplaceBuilder TExprNodeBuilder::Apply(const TExprNode::TPtr& lambda) {
    return Apply(*lambda);
}

TExprNodeReplaceBuilder TExprNodeBuilder::ApplyPartial(ui32 index, TExprNode::TPtr args, TExprNode::TPtr body) {
    Y_ENSURE(Container_, "Container expected");
    Y_ENSURE(Container_->ChildrenSize() == index + (Container_->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container_->ChildrenSize() <<
        ", actual: " << index);
    Y_ENSURE(!args || args->IsArguments());
    return TExprNodeReplaceBuilder(this, Container_, std::move(args), std::move(body));
}

TExprNodeReplaceBuilder TExprNodeBuilder::ApplyPartial(ui32 index, TExprNode::TPtr args, TExprNode::TListType body) {
    Y_ENSURE(Container_, "Container expected");
    Y_ENSURE(Container_->ChildrenSize() == index + (Container_->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container_->ChildrenSize() <<
        ", actual: " << index);
    Y_ENSURE(!args || args->IsArguments());
    return TExprNodeReplaceBuilder(this, Container_, std::move(args), std::move(body));
}

TExprNodeReplaceBuilder TExprNodeBuilder::ApplyPartial(TExprNode::TPtr args, TExprNode::TPtr body) {
    Y_ENSURE(!Container_ || Container_->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode_, "Node is already build");
    Y_ENSURE(!args || args->IsArguments());
    return TExprNodeReplaceBuilder(this, Container_, std::move(args), std::move(body));
}

TExprNodeReplaceBuilder TExprNodeBuilder::ApplyPartial(TExprNode::TPtr args, TExprNode::TListType body) {
    Y_ENSURE(!Container_ || Container_->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode_, "Node is already build");
    Y_ENSURE(!args || args->IsArguments());
    return TExprNodeReplaceBuilder(this, Container_, std::move(args), std::move(body));
}

TExprNodeReplaceBuilder::TExprNodeReplaceBuilder(TExprNodeBuilder* owner, TExprNode::TPtr container,
    TExprNode::TPtr&& args, TExprNode::TPtr&& body)
    : Owner_(owner)
    , Container_(std::move(container))
    , Args_(std::move(args))
    , Body_({std::move(body)})
    , CurrentIndex_(0)
    , CurrentNode_(nullptr)
{
}

TExprNodeReplaceBuilder::TExprNodeReplaceBuilder(TExprNodeBuilder* owner, TExprNode::TPtr container,
    TExprNode::TPtr&& args, TExprNode::TListType&& body)
    : Owner_(owner)
    , Container_(std::move(container))
    , Args_(std::move(args))
    , Body_(std::move(body))
    , CurrentIndex_(0)
    , CurrentNode_(nullptr)
{
}

TExprNodeReplaceBuilder::TExprNodeReplaceBuilder(TExprNodeBuilder* owner, TExprNode::TPtr container,
    const TExprNode& lambda)
    : TExprNodeReplaceBuilder(owner, std::move(container), lambda.HeadPtr(), lambda.TailPtr())
{
    Y_ENSURE(lambda.Type() == TExprNode::Lambda, "Expected lambda");
}

TExprNodeReplaceBuilder& TExprNodeReplaceBuilder::With(
    ui32 argIndex, const TStringBuf& toName) {
    Y_ENSURE(Args_, "No arguments");
    Y_ENSURE(argIndex < Args_->ChildrenSize(), "Wrong argument index");
    Body_ = Owner_->Ctx_.ReplaceNodes(std::move(Body_), {{Args_->Child(argIndex), Owner_->FindArgument(toName)}});
    return *this;
}

TExprNodeReplaceBuilder& TExprNodeReplaceBuilder::With(
    ui32 argIndex, TExprNode::TPtr toNode) {
    Y_ENSURE(Args_, "No arguments");
    Y_ENSURE(argIndex < Args_->ChildrenSize(), "Wrong argument index");
    Body_ = Owner_->Ctx_.ReplaceNodes(std::move(Body_), {{Args_->Child(argIndex), std::move(toNode)}});
    return *this;
}

TExprNodeReplaceBuilder& TExprNodeReplaceBuilder::With(const TStringBuf& toName) {
    Y_ENSURE(Args_, "No arguments");
    Y_ENSURE(!toName.empty(), "Empty parameter name is not allowed");
    TNodeOnNodeOwnedMap replaces(Args_->ChildrenSize());
    for (ui32 i = 0U; i < Args_->ChildrenSize(); ++i)
        Y_ENSURE(replaces.emplace(Args_->Child(i), Owner_->FindArgument(TString(toName) += ToString(i))).second, "Must be uinique.");
    Body_ = Owner_->Ctx_.ReplaceNodes(std::move(Body_), replaces);
    return *this;
}

TExprNodeReplaceBuilder& TExprNodeReplaceBuilder::With(const TStringBuf& toName, ui32 toIndex) {
    Y_ENSURE(!toName.empty(), "Empty parameter name is not allowed");
    return With(TString(toName) += ToString(toIndex));
}

TExprNodeReplaceBuilder& TExprNodeReplaceBuilder::With(ui32 argIndex, const TStringBuf& toName, ui32 toIndex) {
    Y_ENSURE(!toName.empty(), "Empty parameter name is not allowed");
    return With(argIndex, TString(toName) += ToString(toIndex));
}

TExprNodeReplaceBuilder& TExprNodeReplaceBuilder::WithArguments(TExprNodeSpan nodes) {
    Y_ENSURE(Args_, "No arguments");
    Y_ENSURE(nodes.size() == Args_->ChildrenSize(), "Wrong arguments size");
    TNodeOnNodeOwnedMap replaces;
    for (size_t argIndex = 0; argIndex < nodes.size(); ++argIndex) {
        replaces[Args_->Child(argIndex)] = nodes[argIndex];
    }
    Body_ = Owner_->Ctx_.ReplaceNodes(std::move(Body_), replaces);
    return *this;
}

TExprNodeReplaceBuilder& TExprNodeReplaceBuilder::WithNode(const TExprNode& fromNode, TExprNode::TPtr&& toNode) {
    Body_ = Owner_->Ctx_.ReplaceNodes(std::move(Body_), {{&fromNode, std::move(toNode)}});
    return *this;
}

TExprNodeReplaceBuilder& TExprNodeReplaceBuilder::WithNode(const TExprNode& fromNode, const TStringBuf& toName) {
    return WithNode(fromNode, Owner_->FindArgument(toName));
}

TExprNodeBuilder TExprNodeReplaceBuilder::With(ui32 argIndex) {
    CurrentIndex_ = argIndex;
    return TExprNodeBuilder(Owner_->Pos_, this);
}

TExprNodeBuilder  TExprNodeReplaceBuilder::WithNode(TExprNode::TPtr&& fromNode) {
    CurrentNode_ = std::move(fromNode);
    return TExprNodeBuilder(Owner_->Pos_, this);
}

TExprNodeBuilder& TExprNodeReplaceBuilder::Seal() {
    if (Container_) {
        std::move(Body_.begin(), Body_.end(), std::back_inserter(Container_->Children_));
    } else {
        Y_ENSURE(1U == Body_.size() && Body_.front(), "Expected single node.");
        Owner_->CurrentNode_ = std::move(Body_.front());
    }
    Body_.clear();
    return *Owner_;
}

} // namespace NYql

