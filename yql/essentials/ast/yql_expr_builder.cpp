#include "yql_expr_builder.h"
#include "yql_expr.h"

namespace NYql {

TExprNodeBuilder::TExprNodeBuilder(TPositionHandle pos, TExprContext& ctx)
    : Ctx(ctx)
    , Parent(nullptr)
    , ParentReplacer(nullptr)
    , Container(nullptr)
    , Pos(pos)
    , CurrentNode(nullptr)
{}

TExprNodeBuilder::TExprNodeBuilder(TPositionHandle pos, TExprContext& ctx, ExtArgsFuncType extArgsFunc)
    : Ctx(ctx)
    , Parent(nullptr)
    , ParentReplacer(nullptr)
    , Container(nullptr)
    , Pos(pos)
    , CurrentNode(nullptr)
    , ExtArgsFunc(extArgsFunc)
{}

TExprNodeBuilder::TExprNodeBuilder(TPositionHandle pos, TExprNodeBuilder* parent, const TExprNode::TPtr& container)
    : Ctx(parent->Ctx)
    , Parent(parent)
    , ParentReplacer(nullptr)
    , Container(std::move(container))
    , Pos(pos)
    , CurrentNode(nullptr)
{
    if (Parent) {
        ExtArgsFunc = Parent->ExtArgsFunc;
    }
}

TExprNodeBuilder::TExprNodeBuilder(TPositionHandle pos, TExprNodeReplaceBuilder* parentReplacer)
    : Ctx(parentReplacer->Owner->Ctx)
    , Parent(nullptr)
    , ParentReplacer(parentReplacer)
    , Container(nullptr)
    , Pos(pos)
    , CurrentNode(nullptr)
{
}

TExprNode::TPtr TExprNodeBuilder::Build() {
    Y_ENSURE(CurrentNode, "No current node");
    Y_ENSURE(!Parent, "Build is allowed only on top level");
    if (CurrentNode->Type() == TExprNode::Lambda) {
        Y_ENSURE(CurrentNode->ChildrenSize() > 0U, "Lambda is not complete");
    }

    return CurrentNode;
}

TExprNodeBuilder& TExprNodeBuilder::Seal() {
    Y_ENSURE(Parent, "Seal is allowed only on non-top level");
    if (Container->Type() == TExprNode::Lambda) {
        if (CurrentNode) {
            Y_ENSURE(Container->ChildrenSize() == 1, "Lambda is already complete.");
            Container->Children_.emplace_back(std::move(CurrentNode));
        } else {
            Y_ENSURE(Container->ChildrenSize() > 0U, "Lambda isn't complete.");
        }
    }

    return *Parent;
}

TExprNodeReplaceBuilder& TExprNodeBuilder::Done() {
    Y_ENSURE(ParentReplacer, "Done is allowed only if parent is a replacer");
    Y_ENSURE(CurrentNode, "No current node");
    for (auto& body : ParentReplacer->Body)
        body = Ctx.ReplaceNode(std::move(body), ParentReplacer->CurrentNode ? *ParentReplacer->CurrentNode : *ParentReplacer->Args->Child(ParentReplacer->CurrentIndex), CurrentNode);
    return *ParentReplacer;
}

TExprNodeBuilder& TExprNodeBuilder::Atom(ui32 index, TPositionHandle pos, const TStringBuf& content, ui32 flags) {
    Y_ENSURE(Container && !Container->IsLambda(), "Container expected");
    Y_ENSURE(Container->ChildrenSize() == index,
        "Container position mismatch, expected: " << Container->ChildrenSize() <<
        ", actual: " << index);
    auto child = Ctx.NewAtom(pos, content, flags);
    Container->Children_.push_back(child);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Atom(TPositionHandle pos, const TStringBuf& content, ui32 flags) {
    Y_ENSURE(!Container || Container->IsLambda(), "No container expected");
    Y_ENSURE(!CurrentNode, "Node is already build");
    CurrentNode = Ctx.NewAtom(pos, content, flags);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Atom(ui32 index, const TStringBuf& content, ui32 flags) {
    return Atom(index, Pos, content, flags);
}

TExprNodeBuilder& TExprNodeBuilder::Atom(const TStringBuf& content, ui32 flags) {
    return Atom(Pos, content, flags);
}

TExprNodeBuilder& TExprNodeBuilder::Atom(ui32 index, ui32 literalIndexValue) {
    return Atom(index, Pos, Ctx.GetIndexAsString(literalIndexValue), TNodeFlags::Default);
}

TExprNodeBuilder& TExprNodeBuilder::Atom(ui32 literalIndexValue) {
    return Atom(Pos, Ctx.GetIndexAsString(literalIndexValue), TNodeFlags::Default);
}

TExprNodeBuilder TExprNodeBuilder::List(ui32 index, TPositionHandle pos) {
    Y_ENSURE(Container, "Container expected");
    Y_ENSURE(Container->ChildrenSize() == index + (Container->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container->ChildrenSize() <<
        ", actual: " << index);
    const auto child = Ctx.NewList(pos, {});
    Container->Children_.push_back(child);
    return TExprNodeBuilder(pos, this, child);
}

TExprNodeBuilder TExprNodeBuilder::List(TPositionHandle pos) {
    Y_ENSURE(!Container || Container->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode, "Node is already build");
    CurrentNode = Ctx.NewList(pos, {});
    return TExprNodeBuilder(pos, this, CurrentNode);
}

TExprNodeBuilder TExprNodeBuilder::List(ui32 index) {
    return List(index, Pos);
}

TExprNodeBuilder TExprNodeBuilder::List() {
    return List(Pos);
}

TExprNodeBuilder& TExprNodeBuilder::Add(ui32 index, const TExprNode::TPtr& child) {
    Y_ENSURE(Container, "Container expected");
    Y_ENSURE(Container->ChildrenSize() == index + (Container->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container->ChildrenSize() <<
        ", actual: " << index);
    Y_ENSURE(child, "child should not be nullptr");
    Container->Children_.push_back(child);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Add(ui32 index, TExprNode::TPtr&& child) {
    Y_ENSURE(Container, "Container expected");
    Y_ENSURE(Container->ChildrenSize() == index + (Container->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container->ChildrenSize() <<
        ", actual: " << index);
    Y_ENSURE(child, "child should not be nullptr");
    Container->Children_.push_back(std::move(child));
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Add(TExprNode::TListType&& children) {
    Y_ENSURE(Container && Container->Type() != TExprNode::Lambda, "Container expected");
    Y_ENSURE(Container->Children_.empty(), "container should be empty");
    Container->Children_ = std::move(children);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Set(TExprNode::TPtr&& body) {
    Y_ENSURE(Container && Container->Type() == TExprNode::Lambda, "Lambda expected");
    Y_ENSURE(!CurrentNode, "Lambda already has a body");
    CurrentNode = std::move(body);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Set(const TExprNode::TPtr& body) {
    Y_ENSURE(Container && Container->Type() == TExprNode::Lambda, "Lambda expected");
    Y_ENSURE(!CurrentNode, "Lambda already has a body");
    CurrentNode = body;
    return *this;
}

TExprNodeBuilder TExprNodeBuilder::Callable(ui32 index, TPositionHandle pos, const TStringBuf& content) {
    Y_ENSURE(Container, "Container expected");
    Y_ENSURE(Container->ChildrenSize() == index + (Container->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container->ChildrenSize() <<
        ", actual: " << index);
    auto child = Ctx.NewCallable(pos, content, {});
    Container->Children_.push_back(child);
    return TExprNodeBuilder(pos, this, child);
}

TExprNodeBuilder TExprNodeBuilder::Callable(TPositionHandle pos, const TStringBuf& content) {
    Y_ENSURE(!Container || Container->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode, "Node is already build");
    CurrentNode = Ctx.NewCallable(pos, content, {});
    return TExprNodeBuilder(pos, this, CurrentNode);
}

TExprNodeBuilder TExprNodeBuilder::Callable(ui32 index, const TStringBuf& content) {
    return Callable(index, Pos, content);
}

TExprNodeBuilder TExprNodeBuilder::Callable(const TStringBuf& content) {
    return Callable(Pos, content);
}

TExprNodeBuilder& TExprNodeBuilder::World(ui32 index, TPositionHandle pos) {
    Y_ENSURE(Container, "Container expected");
    Y_ENSURE(Container->ChildrenSize() == index + (Container->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container->ChildrenSize() <<
        ", actual: " << index);
    auto child = Ctx.NewWorld(pos);
    Container->Children_.push_back(child);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::World(TPositionHandle pos) {
    Y_ENSURE(!Container, "No container expected");
    Y_ENSURE(!CurrentNode, "Node is already build");
    CurrentNode = Ctx.NewWorld(pos);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::World(ui32 index) {
    return World(index, Pos);
}

TExprNodeBuilder& TExprNodeBuilder::World() {
    return World(Pos);
}

TExprNodeBuilder TExprNodeBuilder::Lambda(ui32 index, TPositionHandle pos) {
    Y_ENSURE(Container, "Container expected");
    Y_ENSURE(Container->ChildrenSize() == index + (Container->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container->ChildrenSize() <<
        ", actual: " << index);
    auto child = Ctx.NewLambda(pos, Ctx.NewArguments(pos, {}), nullptr);
    Container->Children_.push_back(child);
    return TExprNodeBuilder(pos, this, child);
}

TExprNodeBuilder TExprNodeBuilder::Lambda(TPositionHandle pos) {
    Y_ENSURE(!Container || Container->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode, "Node is already build");
    CurrentNode = Ctx.NewLambda(pos, Ctx.NewArguments(pos, {}), nullptr);
    return TExprNodeBuilder(pos, this, CurrentNode);
}

TExprNodeBuilder TExprNodeBuilder::Lambda(ui32 index) {
    return Lambda(index, Pos);
}

TExprNodeBuilder TExprNodeBuilder::Lambda() {
    return Lambda(Pos);
}

TExprNodeBuilder& TExprNodeBuilder::Param(TPositionHandle pos, const TStringBuf& name) {
    Y_ENSURE(!name.empty(), "Empty parameter name is not allowed");
    Y_ENSURE(Container, "Container expected");
    Y_ENSURE(Container->Type() == TExprNode::Lambda, "Container must be a lambda");
    Y_ENSURE(!CurrentNode, "Lambda already has a body");
    for (auto arg : Container->Head().Children()) {
        Y_ENSURE(arg->Content() != name, "Duplicate of lambda param name: " << name);
    }

    Container->Head().Children_.push_back(Ctx.NewArgument(pos, name));
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Param(const TStringBuf& name) {
    return Param(Pos, name);
}

TExprNodeBuilder& TExprNodeBuilder::Params(const TStringBuf& name, ui32 width) {
    Y_ENSURE(!name.empty(), "Empty parameter name is not allowed");
    for (ui32 i = 0U; i < width; ++i)
        Param(Pos, TString(name) += ToString(i));
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Arg(ui32 index, const TStringBuf& name) {
    Y_ENSURE(!name.empty(), "Empty parameter name is not allowed");
    Y_ENSURE(Container, "Container expected");
    Y_ENSURE(Container->ChildrenSize() == index + (Container->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container->ChildrenSize() <<
        ", actual: " << index);
    auto arg = FindArgument(name);
    Container->Children_.push_back(arg);
    return *this;
}

TExprNodeBuilder& TExprNodeBuilder::Arg(const TStringBuf& name) {
    Y_ENSURE(!Container || Container->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode, "Node is already build");
    CurrentNode = FindArgument(name);
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
    Y_ENSURE(!Container || Container->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode, "Node is already build");
    CurrentNode = arg;
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
    for (auto builder = this; builder; builder = builder->Parent) {
        while (builder->ParentReplacer) {
            builder = builder->ParentReplacer->Owner;
        }

        if (builder->Container && builder->Container->IsLambda()) {
            for (const auto& arg : builder->Container->Head().Children()) {
                if (arg->Content() == name) {
                    return arg;
                }
            }
        }
    }

    if (ExtArgsFunc) {
        if (const auto arg = ExtArgsFunc(name)) {
            return arg;
        }
    }

    ythrow yexception() << "Parameter not found: " << name;
}

TExprNodeReplaceBuilder TExprNodeBuilder::Apply(ui32 index, const TExprNode& lambda) {
    Y_ENSURE(Container, "Container expected");
    Y_ENSURE(Container->ChildrenSize() == index + (Container->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container->ChildrenSize() <<
        ", actual: " << index);
    return TExprNodeReplaceBuilder(this, Container, lambda.HeadPtr(), GetLambdaBody(lambda));
}

TExprNodeReplaceBuilder TExprNodeBuilder::Apply(const TExprNode& lambda) {
    Y_ENSURE(!Container || Container->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode, "Node is already build");
    return TExprNodeReplaceBuilder(this, Container, lambda.HeadPtr(), GetLambdaBody(lambda));
}

TExprNodeReplaceBuilder TExprNodeBuilder::Apply(ui32 index, const TExprNode::TPtr& lambda) {
    return Apply(index, *lambda);
}

TExprNodeReplaceBuilder TExprNodeBuilder::Apply(const TExprNode::TPtr& lambda) {
    return Apply(*lambda);
}

TExprNodeReplaceBuilder TExprNodeBuilder::ApplyPartial(ui32 index, TExprNode::TPtr args, TExprNode::TPtr body) {
    Y_ENSURE(Container, "Container expected");
    Y_ENSURE(Container->ChildrenSize() == index + (Container->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container->ChildrenSize() <<
        ", actual: " << index);
    Y_ENSURE(!args || args->IsArguments());
    return TExprNodeReplaceBuilder(this, Container, std::move(args), std::move(body));
}

TExprNodeReplaceBuilder TExprNodeBuilder::ApplyPartial(ui32 index, TExprNode::TPtr args, TExprNode::TListType body) {
    Y_ENSURE(Container, "Container expected");
    Y_ENSURE(Container->ChildrenSize() == index + (Container->IsLambda() ? 1U : 0U),
        "Container position mismatch, expected: " << Container->ChildrenSize() <<
        ", actual: " << index);
    Y_ENSURE(!args || args->IsArguments());
    return TExprNodeReplaceBuilder(this, Container, std::move(args), std::move(body));
}

TExprNodeReplaceBuilder TExprNodeBuilder::ApplyPartial(TExprNode::TPtr args, TExprNode::TPtr body) {
    Y_ENSURE(!Container || Container->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode, "Node is already build");
    Y_ENSURE(!args || args->IsArguments());
    return TExprNodeReplaceBuilder(this, Container, std::move(args), std::move(body));
}

TExprNodeReplaceBuilder TExprNodeBuilder::ApplyPartial(TExprNode::TPtr args, TExprNode::TListType body) {
    Y_ENSURE(!Container || Container->Type() == TExprNode::Lambda, "No container expected");
    Y_ENSURE(!CurrentNode, "Node is already build");
    Y_ENSURE(!args || args->IsArguments());
    return TExprNodeReplaceBuilder(this, Container, std::move(args), std::move(body));
}

TExprNodeReplaceBuilder::TExprNodeReplaceBuilder(TExprNodeBuilder* owner, TExprNode::TPtr container,
    TExprNode::TPtr&& args, TExprNode::TPtr&& body)
    : Owner(owner)
    , Container(std::move(container))
    , Args(std::move(args))
    , Body({std::move(body)})
    , CurrentIndex(0)
    , CurrentNode(nullptr)
{
}

TExprNodeReplaceBuilder::TExprNodeReplaceBuilder(TExprNodeBuilder* owner, TExprNode::TPtr container,
    TExprNode::TPtr&& args, TExprNode::TListType&& body)
    : Owner(owner)
    , Container(std::move(container))
    , Args(std::move(args))
    , Body(std::move(body))
    , CurrentIndex(0)
    , CurrentNode(nullptr)
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
    Y_ENSURE(Args, "No arguments");
    Y_ENSURE(argIndex < Args->ChildrenSize(), "Wrong argument index");
    Body = Owner->Ctx.ReplaceNodes(std::move(Body), {{Args->Child(argIndex), Owner->FindArgument(toName)}});
    return *this;
}

TExprNodeReplaceBuilder& TExprNodeReplaceBuilder::With(
    ui32 argIndex, TExprNode::TPtr toNode) {
    Y_ENSURE(Args, "No arguments");
    Y_ENSURE(argIndex < Args->ChildrenSize(), "Wrong argument index");
    Body = Owner->Ctx.ReplaceNodes(std::move(Body), {{Args->Child(argIndex), std::move(toNode)}});
    return *this;
}

TExprNodeReplaceBuilder& TExprNodeReplaceBuilder::With(const TStringBuf& toName) {
    Y_ENSURE(Args, "No arguments");
    Y_ENSURE(!toName.empty(), "Empty parameter name is not allowed");
    TNodeOnNodeOwnedMap replaces(Args->ChildrenSize());
    for (ui32 i = 0U; i < Args->ChildrenSize(); ++i)
        Y_ENSURE(replaces.emplace(Args->Child(i), Owner->FindArgument(TString(toName) += ToString(i))).second, "Must be uinique.");
    Body = Owner->Ctx.ReplaceNodes(std::move(Body), replaces);
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

TExprNodeReplaceBuilder& TExprNodeReplaceBuilder::WithNode(const TExprNode& fromNode, TExprNode::TPtr&& toNode) {
    Body = Owner->Ctx.ReplaceNodes(std::move(Body), {{&fromNode, std::move(toNode)}});
    return *this;
}

TExprNodeReplaceBuilder& TExprNodeReplaceBuilder::WithNode(const TExprNode& fromNode, const TStringBuf& toName) {
    return WithNode(fromNode, Owner->FindArgument(toName));
}

TExprNodeBuilder TExprNodeReplaceBuilder::With(ui32 argIndex) {
    CurrentIndex = argIndex;
    return TExprNodeBuilder(Owner->Pos, this);
}

TExprNodeBuilder  TExprNodeReplaceBuilder::WithNode(TExprNode::TPtr&& fromNode) {
    CurrentNode = std::move(fromNode);
    return TExprNodeBuilder(Owner->Pos, this);
}

TExprNodeBuilder& TExprNodeReplaceBuilder::Seal() {
    if (Container) {
        std::move(Body.begin(), Body.end(), std::back_inserter(Container->Children_));
    } else {
        Y_ENSURE(1U == Body.size() && Body.front(), "Expected single node.");
        Owner->CurrentNode = std::move(Body.front());
    }
    Body.clear();
    return *Owner;
}

} // namespace NYql

