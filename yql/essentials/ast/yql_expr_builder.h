#pragma once

#include "yql_ast.h"
#include "yql_errors.h"
#include "yql_pos_handle.h"

#include <functional>

namespace NYql {

struct TExprContext;
class TExprNode;
typedef TIntrusivePtr<TExprNode> TExprNodePtr;
typedef std::vector<TExprNodePtr> TExprNodeList;
typedef std::span<const TExprNodePtr> TExprNodeSpan;

class TExprNodeReplaceBuilder;

class TExprNodeBuilder {
friend class TExprNodeReplaceBuilder;
public:
    typedef std::function<TExprNodePtr(const TStringBuf&)> ExtArgsFuncType;
public:
    TExprNodeBuilder(TPositionHandle pos, TExprContext& ctx);
    TExprNodeBuilder(TPositionHandle pos, TExprContext& ctx, ExtArgsFuncType extArgsFunc);
    TExprNodePtr Build();
    TExprNodeBuilder& Seal();
    TExprNodeReplaceBuilder& Done();

    // Indexed version of methods must be used inside of Callable or List, otherwise
    // non-indexed version must be used (at root or as lambda body)
    TExprNodeBuilder& Atom(ui32 index, TPositionHandle pos, const TStringBuf& content, ui32 flags = TNodeFlags::ArbitraryContent);
    TExprNodeBuilder& Atom(TPositionHandle pos, const TStringBuf& content, ui32 flags = TNodeFlags::ArbitraryContent);
    TExprNodeBuilder& Atom(ui32 index, const TStringBuf& content, ui32 flags = TNodeFlags::ArbitraryContent);
    TExprNodeBuilder& Atom(const TStringBuf& content, ui32 flags = TNodeFlags::ArbitraryContent);
    TExprNodeBuilder& Atom(ui32 index, ui32 literalIndexValue);
    TExprNodeBuilder& Atom(ui32 literalIndexValue);

    TExprNodeBuilder List(ui32 index, TPositionHandle pos);
    TExprNodeBuilder List(TPositionHandle pos);
    TExprNodeBuilder List(ui32 index);
    TExprNodeBuilder List();

    TExprNodeBuilder& Add(ui32 index, TExprNodePtr&& child);
    TExprNodeBuilder& Add(ui32 index, const TExprNodePtr& child);
    TExprNodeBuilder& Add(TExprNodeList&& children);

    // only for lambda bodies
    TExprNodeBuilder& Set(TExprNodePtr&& body);
    TExprNodeBuilder& Set(const TExprNodePtr& body);

    TExprNodeBuilder Callable(ui32 index, TPositionHandle pos, const TStringBuf& content);
    TExprNodeBuilder Callable(TPositionHandle pos, const TStringBuf& content);
    TExprNodeBuilder Callable(ui32 index, const TStringBuf& content);
    TExprNodeBuilder Callable(const TStringBuf& content);

    TExprNodeBuilder& World(ui32 index, TPositionHandle pos);
    TExprNodeBuilder& World(TPositionHandle pos);
    TExprNodeBuilder& World(ui32 index);
    TExprNodeBuilder& World();

    TExprNodeBuilder Lambda(ui32 index, TPositionHandle pos);
    TExprNodeBuilder Lambda(TPositionHandle pos);
    TExprNodeBuilder Lambda(ui32 index);
    TExprNodeBuilder Lambda();

    TExprNodeBuilder& Param(TPositionHandle pos, const TStringBuf& name);
    TExprNodeBuilder& Param(const TStringBuf& name);
    TExprNodeBuilder& Params(const TStringBuf& name, ui32 width);

    TExprNodeBuilder& Arg(ui32 index, const TStringBuf& name);
    TExprNodeBuilder& Arg(const TStringBuf& name);
    TExprNodeBuilder& Arg(ui32 index, const TStringBuf& name, ui32 toIndex);
    TExprNodeBuilder& Arg(const TStringBuf& name, ui32 toIndex);
    TExprNodeBuilder& Arg(const TExprNodePtr& arg);

    TExprNodeBuilder& Args(ui32 index, const TStringBuf& name, ui32 toIndex);
    TExprNodeBuilder& Args(const TStringBuf& name, ui32 toIndex);

    TExprNodeReplaceBuilder Apply(ui32 index, const TExprNode& lambda);
    TExprNodeReplaceBuilder Apply(ui32 index, const TExprNodePtr& lambda);
    TExprNodeReplaceBuilder Apply(const TExprNode& lambda);
    TExprNodeReplaceBuilder Apply(const TExprNodePtr& lambda);
    TExprNodeReplaceBuilder ApplyPartial(ui32 index, TExprNodePtr args, TExprNodePtr body);
    TExprNodeReplaceBuilder ApplyPartial(ui32 index, TExprNodePtr args, TExprNodeList body);
    TExprNodeReplaceBuilder ApplyPartial(TExprNodePtr args, TExprNodePtr body);
    TExprNodeReplaceBuilder ApplyPartial(TExprNodePtr args, TExprNodeList body);

    template <typename TFunc>
    TExprNodeBuilder& Do(const TFunc& func) {
        return func(*this);
    }

private:
    TExprNodeBuilder(TPositionHandle pos, TExprNodeBuilder* parent, const TExprNodePtr& container);
    TExprNodeBuilder(TPositionHandle pos, TExprNodeReplaceBuilder* parentReplacer);
    TExprNodePtr FindArgument(const TStringBuf& name);

private:
    TExprContext& Ctx_;
    TExprNodeBuilder* Parent_;
    TExprNodeReplaceBuilder* ParentReplacer_;
    TExprNodePtr Container_;
    TPositionHandle Pos_;
    TExprNodePtr CurrentNode_;
    ExtArgsFuncType ExtArgsFunc_;
};

namespace NNodes {
    template<typename TParent, typename TNode>
    class TNodeBuilder;
}

class TExprNodeReplaceBuilder {
friend class TExprNodeBuilder;
private:
    struct TBuildAdapter {
        typedef TExprNodeReplaceBuilder& ResultType;

        TBuildAdapter(TExprNodeReplaceBuilder& builder)
            : Builder(builder) {}

        ResultType Value() {
            return Builder;
        }

        TExprNodeReplaceBuilder& Builder;
    };

public:
    TExprNodeReplaceBuilder(TExprNodeBuilder* owner, TExprNodePtr container, const TExprNode& lambda);
    TExprNodeReplaceBuilder(TExprNodeBuilder* owner, TExprNodePtr container, TExprNodePtr&& args, TExprNodePtr&& body);
    TExprNodeReplaceBuilder(TExprNodeBuilder* owner, TExprNodePtr container, TExprNodePtr&& args, TExprNodeList&& body);
    TExprNodeReplaceBuilder& With(ui32 argIndex, const TStringBuf& toName);
    TExprNodeReplaceBuilder& With(ui32 argIndex, const TStringBuf& toName, ui32 toIndex);
    TExprNodeReplaceBuilder& With(ui32 argIndex, TExprNodePtr toNode);
    TExprNodeReplaceBuilder& With(const TStringBuf& toName);
    TExprNodeReplaceBuilder& With(const TStringBuf& toName, ui32 toIndex);
    TExprNodeReplaceBuilder& WithNode(const TExprNode& fromNode, TExprNodePtr&& toNode);
    TExprNodeReplaceBuilder& WithNode(const TExprNode& fromNode, const TStringBuf& toName);
    TExprNodeReplaceBuilder& WithArguments(TExprNodeSpan nodes);
    TExprNodeBuilder With(ui32 argIndex);
    TExprNodeBuilder WithNode(TExprNodePtr&& fromNode);

    template<typename TNode>
    NNodes::TNodeBuilder<TBuildAdapter, TNode> With(ui32 argIndex) {
        TBuildAdapter adapter(*this);

        NNodes::TNodeBuilder<TBuildAdapter, TNode> builder(Owner_->Ctx_, Owner_->Pos_,
            [adapter, argIndex](const TNode& node) mutable -> TBuildAdapter& {
                adapter.Builder = adapter.Builder.With(argIndex, node.Get());
                return adapter;
            },
            [adapter] (const TStringBuf& argName) {
                return adapter.Builder.Owner_->FindArgument(argName);
            });

        return builder;
    }

    TExprNodeBuilder& Seal();

    template <typename TFunc>
    TExprNodeReplaceBuilder& Do(const TFunc& func) {
        return func(*this);
    }

private:
    TExprNodeBuilder* Owner_;
    TExprNodePtr Container_;
    TExprNodePtr Args_;
    TExprNodeList Body_;
    ui32 CurrentIndex_;
    TExprNodePtr CurrentNode_;
};

} // namespace NYql

