#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>

#include <functional>
#include <initializer_list>

namespace NYql {

class TVisitorTransformerBase: public TSyncTransformerBase {
public:
    using THandler = std::function<TStatus(const TExprNode::TPtr&, TExprNode::TPtr&, TExprContext&)>;

    TVisitorTransformerBase(bool failOnUnknown)
        : FailOnUnknown(failOnUnknown)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

    void Rewind() final {
    }

    bool CanParse(const TExprNode& node) const {
        return Handlers.contains(node.Content());
    }

protected:
    void AddHandler(std::initializer_list<TStringBuf> names, THandler handler);

    template <class TDerived>
    THandler Hndl(TStatus(TDerived::* handler)(const TExprNode::TPtr&, TExprNode::TPtr&, TExprContext&)) {
        return [this, handler] (TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
            return (static_cast<TDerived*>(this)->*handler)(input, output, ctx);
        };
    }

    template <class TDerived>
    THandler Hndl(TStatus(TDerived::* handler)(const TExprNode::TPtr&, TExprContext&)) {
        return [this, handler] (TExprNode::TPtr input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
            return (static_cast<TDerived*>(this)->*handler)(input, ctx);
        };
    }

    template <class TDerived>
    THandler Hndl(TStatus(TDerived::* handler)(NNodes::TExprBase, TExprContext&)) {
        return [this, handler] (TExprNode::TPtr input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
            return (static_cast<TDerived*>(this)->*handler)(NNodes::TExprBase(input), ctx);
        };
    }

    THandler Hndl(TStatus(*handler)(const TExprNode::TPtr&, TExprContext&)) {
        return [handler] (TExprNode::TPtr input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
            return handler(input, ctx);
        };
    }

    THandler Hndl(TStatus(*handler)(NNodes::TExprBase, TExprContext&)) {
        return [handler] (TExprNode::TPtr input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
            return handler(NNodes::TExprBase(input), ctx);
        };
    }

protected:
    const bool FailOnUnknown;
    THashMap<TStringBuf, THandler> Handlers;
};

} // NYql
