#pragma once

#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.gen.h>

#include <util/generic/map.h>
#include <util/string/cast.h>

namespace NYql::NNodes {

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.decl.inl.h>

class TCoAtom: public NGenerated::TCoAtomStub<TExprBase> {
public:
    explicit TCoAtom(const TExprNode* node)
        : TCoAtomStub(node)
    {
    }

    explicit TCoAtom(const TExprNode::TPtr& node)
        : TCoAtomStub(node)
    {
    }

    // TODO(YQL-20095): there are YDB usages
    // NOLINTNEXTLINE(google-explicit-constructor)
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

class TCoArguments: public NGenerated::TCoArgumentsStub<TExprBase> {
public:
    explicit TCoArguments(const TExprNode* node)
        : TCoArgumentsStub(node)
    {
    }

    explicit TCoArguments(const TExprNode::TPtr& node)
        : TCoArgumentsStub(node)
    {
    }

    TCoArgument Arg(size_t index) const {
        return TCoArgument(Ref().ChildPtr(index));
    }
    size_t Size() const {
        return Ref().ChildrenSize();
    }

    TChildIterator<TCoArgument> begin() const {
        return TChildIterator<TCoArgument>(*this);
    }
    TChildIterator<TCoArgument> end() const {
        return TChildIterator<TCoArgument>();
    }
};

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.defs.inl.h>

template <typename TParent>
class TNodeBuilder<TParent, TCoWorld>: public NGenerated::TCoWorldBuilder<TParent> {
public:
    TNodeBuilder(TExprContext& ctx, TPositionHandle pos,
                 typename NGenerated::TCoWorldBuilder<TParent>::BuildFuncType buildFunc,
                 typename NGenerated::TCoWorldBuilder<TParent>::GetArgFuncType getArgFunc)
        : NGenerated::TCoWorldBuilder<TParent>(ctx, pos, buildFunc, getArgFunc) {
    }

    TCoWorld DoBuild() {
        auto node = this->Ctx_.NewWorld(this->Pos_);
        return TCoWorld(node);
    }
};

template <typename TParent>
class TNodeBuilder<TParent, TCoArgument>: public NGenerated::TCoArgumentBuilder<TParent> {
public:
    TNodeBuilder(TExprContext& ctx, TPositionHandle pos,
                 typename NGenerated::TCoArgumentBuilder<TParent>::BuildFuncType buildFunc,
                 typename NGenerated::TCoArgumentBuilder<TParent>::GetArgFuncType getArgFunc)
        : NGenerated::TCoArgumentBuilder<TParent>(ctx, pos, buildFunc, getArgFunc) {
    }

    TNodeBuilder<TParent, TCoArgument>& Name(const TStringBuf& value) {
        this->NameHolder_ = this->Ctx_.AppendString(value);
        return *this;
    }

    TCoArgument DoBuild() {
        YQL_ENSURE(!NameHolder_.empty());
        auto node = this->Ctx_.NewArgument(this->Pos_, NameHolder_);
        return TCoArgument(node);
    }

private:
    TStringBuf NameHolder_;
};

template <typename TParent>
class TNodeBuilder<TParent, TCoAtom>: public NGenerated::TCoAtomBuilder<TParent> {
public:
    TNodeBuilder(TExprContext& ctx, TPositionHandle pos,
                 typename NGenerated::TCoAtomBuilder<TParent>::BuildFuncType buildFunc,
                 typename NGenerated::TCoAtomBuilder<TParent>::GetArgFuncType getArgFunc)
        : NGenerated::TCoAtomBuilder<TParent>(ctx, pos, buildFunc, getArgFunc) {
    }

    TNodeBuilder<TParent, TCoAtom>& Value(const TStringBuf& value, ui32 flags = TNodeFlags::ArbitraryContent) {
        this->ValueHolder_ = this->Ctx_.AppendString(value);
        this->Flags_ = flags;
        return *this;
    }

    TNodeBuilder<TParent, TCoAtom>& Value(i64 value) {
        return value >= 0 && value <= i64(std::numeric_limits<ui32>::max()) ? Value(this->Ctx_.GetIndexAsString(ui32(value)), TNodeFlags::Default) : Value(ToString(value), TNodeFlags::Default);
    }

    TCoAtom DoBuild() {
        auto node = this->Ctx_.NewAtom(this->Pos_, ValueHolder_, this->Flags_);
        return TCoAtom(node);
    }

    TParent& Build() {
        return this->NGenerated::TCoAtomBuilder<TParent>::Build();
    }

    TParent& Build(const TStringBuf& value, ui32 flags = TNodeFlags::ArbitraryContent) {
        this->ValueHolder_ = this->Ctx_.AppendString(value);
        this->Flags_ = flags;
        return this->Build();
    }

    TParent& Build(i64 value) {
        return value >= 0 && value <= i64(std::numeric_limits<ui32>::max()) ? Build(this->Ctx_.GetIndexAsString(ui32(value)), TNodeFlags::Default) : Build(ToString(value), TNodeFlags::Default);
    }

private:
    TStringBuf ValueHolder_;
    ui32 Flags_ = TNodeFlags::ArbitraryContent;
};

template <typename TParent>
class TNodeBuilder<TParent, TCoLambda>: public NGenerated::TCoLambdaBuilder<TParent> {
public:
    TNodeBuilder(TExprContext& ctx, TPositionHandle pos,
                 typename NGenerated::TCoLambdaBuilder<TParent>::BuildFuncType buildFunc,
                 typename NGenerated::TCoLambdaBuilder<TParent>::GetArgFuncType getArgFunc,
                 std::shared_ptr<TMap<TStringBuf, TExprBase>> argsStore = std::make_shared<TMap<TStringBuf, TExprBase>>())
        : NGenerated::TCoLambdaBuilder<TParent>(ctx, pos, buildFunc, [argsStore, getArgFunc](const TStringBuf& argName) {
            if (auto res = argsStore->FindPtr(argName)) {
                return *res;
            }

            return getArgFunc(argName);
        })
        , ArgsMap_(argsStore)
    {
    }

    TNodeBuilder<TParent, TCoLambda>& Args(const TCoArgument& node) {
        Y_DEBUG_ABORT_UNLESS(!this->ArgsHolder.IsValid());

        auto argsNode = this->Ctx_.NewArguments(this->Pos_, {node.Ptr()});
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
            auto argName = this->Ctx_.AppendString(name);
            auto argNode = this->Ctx_.NewArgument(this->Pos_, argName);
            argNodes.push_back(argNode);

            ArgsMap_->emplace(argName, TExprBase(argNode));
        }

        auto argsNode = this->Ctx_.NewArguments(this->Pos_, std::move(argNodes));
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

        auto argsNode = this->Ctx_.NewArguments(this->Pos_, std::move(argNodes));
        this->ArgsHolder = TCoArguments(argsNode);
        return *this;
    }

    TNodeBuilder<TParent, TCoLambda>& Args(const TExprNode::TListType& list)
    {
        Y_DEBUG_ABORT_UNLESS(!this->ArgsHolder.IsValid());

        auto argsNode = this->Ctx_.NewArguments(this->Pos_, TExprNode::TListType(list));
        this->ArgsHolder = TCoArguments(argsNode);
        return *this;
    }

    TCoLambda DoBuild() {
        auto node = this->Ctx_.NewLambda(this->Pos_, this->ArgsHolder.Cast().Ptr(), this->BodyHolder.Cast().Ptr());

        return TCoLambda(node);
    }

private:
    std::shared_ptr<TMap<TStringBuf, TExprBase>> ArgsMap_;
};

class TExprApplier: public TExprBase {
    friend class TMaybeNode<TExprApplier>;

    template <typename TParent, typename TNode>
    friend class TNodeBuilder;

    explicit TExprApplier(const TExprNode::TPtr& node)
        : TExprBase(node)
    {
    }

    explicit TExprApplier(const TExprBase node)
        : TExprBase(node)
    {
    }
};

template <>
class TMaybeNode<TExprApplier>: public TMaybeNode<TExprBase> {
public:
    TMaybeNode()
        : TMaybeNode<TExprBase>() {
    }

    // Implicit item to Maybe lifting is not surprising
    // NOLINTNEXTLINE(google-explicit-constructor)
    TMaybeNode(const TExprNode* node)
        : TMaybeNode<TExprBase>(node) {
    }

    // Implicit item to Maybe lifting is not surprising
    // NOLINTNEXTLINE(google-explicit-constructor)
    TMaybeNode(const TExprNode::TPtr& node)
        : TMaybeNode<TExprBase>(node) {
    }

    TExprApplier Cast() const {
        return TExprApplier(TMaybeNode<TExprBase>::Cast());
    }
};

template <typename TParent>
class TNodeBuilder<TParent, TExprApplier>: TNodeBuilderBase {
public:
    typedef std::function<TParent&(const TExprApplier&)> BuildFuncType;
    typedef std::function<TExprBase(const TStringBuf& arg)> GetArgFuncType;
    typedef TExprApplier ResultType;

    TNodeBuilder(TExprContext& ctx, TPositionHandle pos, BuildFuncType buildFunc, GetArgFuncType getArgFunc)
        : TNodeBuilderBase(ctx, pos, getArgFunc)
        , BuildFunc_(buildFunc)
    {
    }

    TParent& Build() {
        YQL_ENSURE(Body_);
        return BuildFunc_(TExprApplier(Body_.Cast().Ptr()));
    }

    typename TParent::ResultType Done() {
        TParent& parent = Build();
        return parent.Value();
    }

    TNodeBuilder<TParent, TExprApplier>& Apply(TCoLambda lambda) {
        YQL_ENSURE(!Body_);

        Body_ = lambda.Body();
        Args_ = lambda.Args();

        return *this;
    }

    TNodeBuilder<TParent, TExprApplier>& Apply(TExprBase node) {
        YQL_ENSURE(!Body_);

        Body_ = node;

        return *this;
    }

    TNodeBuilder<TParent, TExprApplier>& With(TExprBase from, TExprBase to) {
        YQL_ENSURE(Body_);

        DoApply(from, to);

        return *this;
    }

    TNodeBuilder<TParent, TExprApplier>& With(ui32 argIndex, TExprBase to) {
        YQL_ENSURE(Body_);
        YQL_ENSURE(Args_);

        DoApply(Args_.Cast().Arg(argIndex), to);
        return *this;
    }

    TNodeBuilder<TParent, TExprApplier>& With(ui32 argIndex, const TStringBuf& argName) {
        YQL_ENSURE(Body_);
        YQL_ENSURE(Args_);

        DoApply(Args_.Cast().Arg(argIndex), GetArgFunc_(argName));

        return *this;
    }

    TNodeBuilder<TParent, TExprApplier>& With(TExprBase from, const TStringBuf& argName) {
        YQL_ENSURE(Body_);

        DoApply(from, GetArgFunc_(argName));

        return *this;
    }

    template <typename TNode>
    TNodeBuilder<TNodeBuilder<TParent, TExprApplier>, TNode> With(ui32 argIndex) {
        YQL_ENSURE(Body_);
        YQL_ENSURE(Args_);

        auto buildFunc = [argIndex, this](const TNode& node) mutable -> TNodeBuilder<TParent, TExprApplier>& {
            DoApply(Args_.Cast().Arg(argIndex), node);
            return *this;
        };

        return TNodeBuilder<TNodeBuilder<TParent, TExprApplier>, TNode>(this->Ctx_, this->Pos_,
                                                                        buildFunc, GetArgFunc_);
    }

    template <typename TNode>
    TNodeBuilder<TNodeBuilder<TParent, TExprApplier>, TNode> With(TExprBase from) {
        YQL_ENSURE(Body_);

        auto buildFunc = [from, this](const TNode& node) mutable -> TNodeBuilder<TParent, TExprApplier>& {
            DoApply(from, node);
            return *this;
        };

        return TNodeBuilder<TNodeBuilder<TParent, TExprApplier>, TNode>(this->Ctx_, this->Pos_,
                                                                        buildFunc, GetArgFunc_);
    }

private:
    void DoApply(TExprBase applyFrom, TExprBase applyTo) {
        TExprNodeBuilder builder(this->Pos_, this->Ctx_, [this](const TStringBuf& argName) {
            return GetArgFunc_(argName).Ptr();
        });

        auto args = Args_.IsValid() ? Args_.Cast().Ptr() : nullptr;

        Body_ = builder
                    .ApplyPartial(std::move(args), Body_.Cast().Ptr())
                    .WithNode(applyFrom.Ref(), applyTo.Ptr())
                    .Seal()
                    .Build();
    }

private:
    BuildFuncType BuildFunc_;

    TMaybeNode<TExprBase> Body_;
    TMaybeNode<TCoArguments> Args_;
};

} // namespace NYql::NNodes
