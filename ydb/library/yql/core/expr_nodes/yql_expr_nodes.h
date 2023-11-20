#pragma once

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.gen.h>

#include <util/generic/map.h>
#include <util/string/cast.h>

namespace NYql {
namespace NNodes {

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.decl.inl.h>

class TCoAtom : public NGenerated::TCoAtomStub<TExprBase> {
public:
    explicit TCoAtom(const TExprNode* node)
        : TCoAtomStub(node) {}

    explicit TCoAtom(const TExprNode::TPtr& node)
        : TCoAtomStub(node) {}

    operator TStringBuf() const {
        return Value();
    }

    TString StringValue() const {
        return TString(Value());
    }
};

inline bool operator==(const TCoAtom& lhs, const TStringBuf& rhs) {
    return lhs.Value() == rhs;
}

class TCoArguments : public NGenerated::TCoArgumentsStub<TExprBase> {
public:
    explicit TCoArguments(const TExprNode* node)
        : TCoArgumentsStub(node) {}

    explicit TCoArguments(const TExprNode::TPtr& node)
        : TCoArgumentsStub(node) {}

    TCoArgument Arg(size_t index) const { return TCoArgument(Ref().ChildPtr(index)); }
    size_t Size() const { return Ref().ChildrenSize(); }

    TChildIterator<TCoArgument> begin() const { return TChildIterator<TCoArgument>(*this); }
    TChildIterator<TCoArgument> end() const { return TChildIterator<TCoArgument>(); }
};

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.defs.inl.h>

template<typename TParent>
class TNodeBuilder<TParent, TCoWorld> : public NGenerated::TCoWorldBuilder<TParent>
{
public:
    TNodeBuilder(TExprContext& ctx, TPositionHandle pos,
        typename NGenerated::TCoWorldBuilder<TParent>::BuildFuncType buildFunc,
        typename NGenerated::TCoWorldBuilder<TParent>::GetArgFuncType getArgFunc)
        : NGenerated::TCoWorldBuilder<TParent>(ctx, pos, buildFunc, getArgFunc) {}

    TCoWorld DoBuild() {
        auto node = this->Ctx.NewWorld(this->Pos);
        return TCoWorld(node);
    }
};

template<typename TParent>
class TNodeBuilder<TParent, TCoArgument> : public NGenerated::TCoArgumentBuilder<TParent>
{
public:
    TNodeBuilder(TExprContext& ctx, TPositionHandle pos,
        typename NGenerated::TCoArgumentBuilder<TParent>::BuildFuncType buildFunc,
        typename NGenerated::TCoArgumentBuilder<TParent>::GetArgFuncType getArgFunc)
        : NGenerated::TCoArgumentBuilder<TParent>(ctx, pos, buildFunc, getArgFunc) {}

    TNodeBuilder<TParent, TCoArgument>& Name(const TStringBuf& value) {
        this->NameHolder = this->Ctx.AppendString(value);
        return *this;
    }

    TCoArgument DoBuild() {
        YQL_ENSURE(!NameHolder.empty());
        auto node = this->Ctx.NewArgument(this->Pos, NameHolder);
        return TCoArgument(node);
    }

private:
    TStringBuf NameHolder;
};

template<typename TParent>
class TNodeBuilder<TParent, TCoAtom> : public NGenerated::TCoAtomBuilder<TParent>
{
public:
    TNodeBuilder(TExprContext& ctx, TPositionHandle pos,
        typename NGenerated::TCoAtomBuilder<TParent>::BuildFuncType buildFunc,
        typename NGenerated::TCoAtomBuilder<TParent>::GetArgFuncType getArgFunc)
        : NGenerated::TCoAtomBuilder<TParent>(ctx, pos, buildFunc, getArgFunc) {}

    TNodeBuilder<TParent, TCoAtom>& Value(const TStringBuf& value, ui32 flags = TNodeFlags::ArbitraryContent) {
        this->ValueHolder = this->Ctx.AppendString(value);
        this->Flags = flags;
        return *this;
    }

    TNodeBuilder<TParent, TCoAtom>& Value(i64 value) {
        return value >= 0 && value <= i64(std::numeric_limits<ui32>::max()) ?
            Value(this->Ctx.GetIndexAsString(ui32(value)), TNodeFlags::Default) :
            Value(ToString(value), TNodeFlags::Default);
    }

    TCoAtom DoBuild() {
        auto node = this->Ctx.NewAtom(this->Pos, ValueHolder, this->Flags);
        return TCoAtom(node);
    }

    TParent& Build() {
        return this->NGenerated::TCoAtomBuilder<TParent>::Build();
    }

    TParent& Build(const TStringBuf& value, ui32 flags = TNodeFlags::ArbitraryContent) {
        this->ValueHolder = this->Ctx.AppendString(value);
        this->Flags = flags;
        return this->Build();
    }

    TParent& Build(i64 value) {
        return value >= 0 && value <= i64(std::numeric_limits<ui32>::max()) ?
            Build(this->Ctx.GetIndexAsString(ui32(value)), TNodeFlags::Default) :
            Build(ToString(value), TNodeFlags::Default);
    }
private:
    TStringBuf ValueHolder;
    ui32 Flags = TNodeFlags::ArbitraryContent;
};

template<typename TParent>
class TNodeBuilder<TParent, TCoLambda> : public NGenerated::TCoLambdaBuilder<TParent>
{
public:
    TNodeBuilder(TExprContext& ctx, TPositionHandle pos,
        typename NGenerated::TCoLambdaBuilder<TParent>::BuildFuncType buildFunc,
        typename NGenerated::TCoLambdaBuilder<TParent>::GetArgFuncType getArgFunc,
        std::shared_ptr<TMap<TStringBuf, TExprBase>> argsStore = std::make_shared<TMap<TStringBuf, TExprBase>>())
        : NGenerated::TCoLambdaBuilder<TParent>(ctx, pos, buildFunc, [argsStore, getArgFunc] (const TStringBuf& argName) {
            if (auto res = argsStore->FindPtr(argName)) {
                return *res;
            }

            return getArgFunc(argName);
        })
        , ArgsMap(argsStore)
        {}

    TNodeBuilder<TParent, TCoLambda>& Args(const TCoArgument& node) {
        Y_DEBUG_ABORT_UNLESS(!this->ArgsHolder.IsValid());

        auto argsNode = this->Ctx.NewArguments(this->Pos, { node.Ptr() });
        this->ArgsHolder = TCoArguments(argsNode);

        return *this;
    }

    TNodeBuilder<TParent, TCoLambda>& Args(const TCoArguments& node) {
        Y_DEBUG_ABORT_UNLESS(!this->ArgsHolder.IsValid());

        this->ArgsHolder = node;
        return *this;
    }

    TNodeBuilder<TParent, TCoLambda>& Args(std::initializer_list<TStringBuf> list)
    {
        Y_DEBUG_ABORT_UNLESS(!this->ArgsHolder.IsValid());

        TExprNode::TListType argNodes;
        for (auto name : list) {
            auto argName = this->Ctx.AppendString(name);
            auto argNode = this->Ctx.NewArgument(this->Pos, argName);
            argNodes.push_back(argNode);

            ArgsMap->emplace(argName, TExprBase(argNode));
        }

        auto argsNode = this->Ctx.NewArguments(this->Pos, std::move(argNodes));
        this->ArgsHolder = TCoArguments(argsNode);
        return *this;
    }

    TNodeBuilder<TParent, TCoLambda>& Args(const std::vector<TCoArgument>& list)
    {
        Y_DEBUG_ABORT_UNLESS(!this->ArgsHolder.IsValid());

        TExprNode::TListType argNodes;
        for (auto arg : list) {
            argNodes.push_back(arg.Ptr());
        }

        auto argsNode = this->Ctx.NewArguments(this->Pos, std::move(argNodes));
        this->ArgsHolder = TCoArguments(argsNode);
        return *this;
    }

    TNodeBuilder<TParent, TCoLambda>& Args(const TExprNode::TListType& list)
    {
        Y_DEBUG_ABORT_UNLESS(!this->ArgsHolder.IsValid());

        auto argsNode = this->Ctx.NewArguments(this->Pos, TExprNode::TListType(list));
        this->ArgsHolder = TCoArguments(argsNode);
        return *this;
    }

    TCoLambda DoBuild() {
        auto node = this->Ctx.NewLambda(this->Pos, this->ArgsHolder.Cast().Ptr(), this->BodyHolder.Cast().Ptr());

        return TCoLambda(node);
    }

private:
    std::shared_ptr<TMap<TStringBuf, TExprBase>> ArgsMap;
};

class TExprApplier : public TExprBase {
    friend class TMaybeNode<TExprApplier>;

    template<typename TParent, typename TNode>
    friend class TNodeBuilder;

    TExprApplier(const TExprNode::TPtr& node)
        : TExprBase(node) {}

    TExprApplier(const TExprBase node)
        : TExprBase(node) {}
};

template<>
class TMaybeNode<TExprApplier> : public TMaybeNode<TExprBase> {
public:
    TMaybeNode()
        : TMaybeNode<TExprBase>() {}

    TMaybeNode(const TExprNode* node)
        : TMaybeNode<TExprBase>(node) {}

    TMaybeNode(const TExprNode::TPtr& node)
        : TMaybeNode<TExprBase>(node) {}

    TExprApplier Cast() const {
        return TExprApplier(TMaybeNode<TExprBase>::Cast());
    }
};

template<typename TParent>
class TNodeBuilder<TParent, TExprApplier> : TNodeBuilderBase
{
public:
    typedef std::function<TParent& (const TExprApplier&)> BuildFuncType;
    typedef std::function<TExprBase (const TStringBuf& arg)> GetArgFuncType;
    typedef TExprApplier ResultType;

    TNodeBuilder(TExprContext& ctx, TPositionHandle pos, BuildFuncType buildFunc, GetArgFuncType getArgFunc)
        : TNodeBuilderBase(ctx, pos, getArgFunc)
        , BuildFunc(buildFunc) {}

    TParent& Build() {
        YQL_ENSURE(Body);
        return BuildFunc(TExprApplier(Body.Cast().Ptr()));
    }

    typename TParent::ResultType Done() {
        TParent& parent = Build();
        return parent.Value();
    }

    TNodeBuilder<TParent, TExprApplier>& Apply(TCoLambda lambda) {
        YQL_ENSURE(!Body);

        Body = lambda.Body();
        Args = lambda.Args();

        return *this;
    }

    TNodeBuilder<TParent, TExprApplier>& Apply(TExprBase node) {
        YQL_ENSURE(!Body);

        Body = node;

        return *this;
    }

    TNodeBuilder<TParent, TExprApplier>& With(TExprBase from, TExprBase to) {
        YQL_ENSURE(Body);

        DoApply(from, to);

        return *this;
    }

    TNodeBuilder<TParent, TExprApplier>& With(ui32 argIndex, TExprBase to) {
        YQL_ENSURE(Body);
        YQL_ENSURE(Args);

        DoApply(Args.Cast().Arg(argIndex), to);
        return *this;
    }

    TNodeBuilder<TParent, TExprApplier>& With(ui32 argIndex, const TStringBuf& argName) {
        YQL_ENSURE(Body);
        YQL_ENSURE(Args);

        DoApply(Args.Cast().Arg(argIndex), GetArgFunc(argName));

        return *this;
    }

    TNodeBuilder<TParent, TExprApplier>& With(TExprBase from, const TStringBuf& argName) {
        YQL_ENSURE(Body);

        DoApply(from, GetArgFunc(argName));

        return *this;
    }

    template<typename TNode>
    TNodeBuilder<TNodeBuilder<TParent, TExprApplier>, TNode> With(ui32 argIndex) {
        YQL_ENSURE(Body);
        YQL_ENSURE(Args);

        auto buildFunc = [argIndex, this] (const TNode& node) mutable -> TNodeBuilder<TParent, TExprApplier>& {
            DoApply(Args.Cast().Arg(argIndex), node);
            return *this;
        };

        return TNodeBuilder<TNodeBuilder<TParent, TExprApplier>, TNode>(this->Ctx, this->Pos,
            buildFunc, GetArgFunc);
    }

    template<typename TNode>
    TNodeBuilder<TNodeBuilder<TParent, TExprApplier>, TNode> With(TExprBase from) {
        YQL_ENSURE(Body);

        auto buildFunc = [from, this] (const TNode& node) mutable -> TNodeBuilder<TParent, TExprApplier>& {
            DoApply(from, node);
            return *this;
        };

        return TNodeBuilder<TNodeBuilder<TParent, TExprApplier>, TNode>(this->Ctx, this->Pos,
            buildFunc, GetArgFunc);
    }

private:
    void DoApply(TExprBase applyFrom, TExprBase applyTo) {
        TExprNodeBuilder builder(this->Pos, this->Ctx, [this] (const TStringBuf& argName) {
            return GetArgFunc(argName).Ptr();
        });

        auto args = Args.IsValid() ? Args.Cast().Ptr() : nullptr;

        Body = builder
            .ApplyPartial(std::move(args), Body.Cast().Ptr())
            .WithNode(applyFrom.Ref(), applyTo.Ptr())
            .Seal().Build();
    }

private:
    BuildFuncType BuildFunc;

    TMaybeNode<TExprBase> Body;
    TMaybeNode<TCoArguments> Args;
};

} // namespace NNodes
} // namespace NYql
