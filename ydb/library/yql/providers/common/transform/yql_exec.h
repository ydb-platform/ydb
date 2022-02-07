#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/strbuf.h>
#include <util/generic/hash.h>

#include <functional>
#include <initializer_list>

namespace NYql {

class TExecTransformerBase : public TAsyncCallbackTransformer<TExecTransformerBase> {
public:
    using TStatusCallbackPair = std::pair<TStatus, TAsyncTransformCallbackFuture>;

    using TPrerequisite = std::function<TStatus(const TExprNode::TPtr&)>;
    using THandler = std::function<TStatusCallbackPair(const TExprNode::TPtr&, TExprNode::TPtr&, TExprContext&)>;

    TStatusCallbackPair CallbackTransform(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx);

    bool CanExec(const TExprNode& node) const {
        return Handlers.contains(node.Content());
    }

protected:
    void AddHandler(std::initializer_list<TStringBuf> names, TPrerequisite prerequisite, THandler handler);

    static TPrerequisite RequireAll();
    static TPrerequisite RequireNone();
    static TPrerequisite RequireFirst();
    static TPrerequisite RequireAllOf(std::initializer_list<size_t> children);
    static TPrerequisite RequireSequenceOf(std::initializer_list<size_t> children);

    template <class TDerived>
    TPrerequisite Require(TStatus(TDerived::* prerequisite)(const TExprNode::TPtr&)) {
        return [this, prerequisite] (const TExprNode::TPtr& input) {
            return (static_cast<TDerived*>(this)->*prerequisite)(input);
        };
    }


    static TStatusCallbackPair ExecPass(const TExprNode::TPtr& input, TExprContext& ctx);
    static THandler Pass();

    template <class TDerived>
    THandler Hndl(TStatusCallbackPair(TDerived::* handler)(const TExprNode::TPtr&, TExprNode::TPtr&, TExprContext&)) {
        return [this, handler] (const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            return (static_cast<TDerived*>(this)->*handler)(input, output, ctx);
        };
    }

    template <class TDerived>
    THandler Hndl(TStatusCallbackPair(TDerived::* handler)(const TExprNode::TPtr&, TExprContext&)) {
        return [this, handler] (const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
            return (static_cast<TDerived*>(this)->*handler)(input, ctx);
        };
    }

protected:
    struct THandlerInfo {
        TPrerequisite Prerequisite;
        THandler Handler;
    };
    THashMap<TStringBuf, THandlerInfo> Handlers;
};

} // NYql
