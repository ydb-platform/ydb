#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/log/log_component.h>

#include <util/generic/vector.h>
#include <util/generic/strbuf.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/string.h>

#include <functional>
#include <initializer_list>

namespace NYql {

class TOptimizeTransformerBase: public TSyncTransformerBase {
public:
    using TGetParents = std::function<const TParentsMap*()>;
    using THandler = std::function<NNodes::TMaybeNode<NNodes::TExprBase>(NNodes::TExprBase, TExprContext&, IOptimizationContext&, const TGetParents&)>;
    using TFilter = std::function<bool(const TExprNode*)>;

    TOptimizeTransformerBase(TTypeAnnotationContext* types, NLog::EComponent logComponent, const TSet<TString>& disabledOpts);
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override;
    void Rewind() override;

protected:
    class TIgnoreOptimizationContext;
    class TRemapOptimizationContext;

    static TFilter Any();
    static TFilter Names(std::initializer_list<TStringBuf> names);
    static TFilter Or(std::initializer_list<TFilter> filters);

    void AddHandler(size_t step, TFilter filter, TStringBuf optName, THandler handler);
    void SetGlobal(size_t step);

    template <class TDerived>
    THandler Hndl(NNodes::TMaybeNode<NNodes::TExprBase>(TDerived::* handler)(NNodes::TExprBase, TExprContext&, const TGetParents&)) {
        return [this, handler] (NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& /*optCtx*/, const TGetParents& parents) {
            return (static_cast<TDerived*>(this)->*handler)(node, ctx, parents);
        };
    }

    template <class TDerived>
    THandler Hndl(NNodes::TMaybeNode<NNodes::TExprBase>(TDerived::* handler)(NNodes::TExprBase, TExprContext&, const TGetParents&) const) const {
        return [this, handler] (NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& /*optCtx*/, const TGetParents& parents) {
            return (static_cast<const TDerived*>(this)->*handler)(node, ctx, parents);
        };
    }

    template <class TDerived>
    THandler Hndl(NNodes::TMaybeNode<NNodes::TExprBase>(TDerived::* handler)(NNodes::TExprBase, TExprContext&, IOptimizationContext&, const TGetParents&)) {
        return [this, handler] (NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& parents) {
            return (static_cast<TDerived*>(this)->*handler)(node, ctx, optCtx, parents);
        };
    }

    template <class TDerived>
    THandler Hndl(NNodes::TMaybeNode<NNodes::TExprBase>(TDerived::* handler)(NNodes::TExprBase, TExprContext&, IOptimizationContext&, const TGetParents&) const) const {
        return [this, handler] (NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& parents) {
            return (static_cast<const TDerived*>(this)->*handler)(node, ctx, optCtx, parents);
        };
    }

    template <class TDerived>
    THandler Hndl(NNodes::TMaybeNode<NNodes::TExprBase>(TDerived::* handler)(NNodes::TExprBase, TExprContext&, IOptimizationContext&)) {
        return [this, handler] (NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& /*parents*/) {
            return (static_cast<TDerived*>(this)->*handler)(node, ctx, optCtx);
        };
    }

    template <class TDerived>
    THandler Hndl(NNodes::TMaybeNode<NNodes::TExprBase>(TDerived::* handler)(NNodes::TExprBase, TExprContext&, IOptimizationContext&) const) const {
        return [this, handler] (NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& /*parents*/) {
            return (static_cast<const TDerived*>(this)->*handler)(node, ctx, optCtx);
        };
    }

    template <class TDerived>
    THandler Hndl(NNodes::TMaybeNode<NNodes::TExprBase>(TDerived::* handler)(NNodes::TExprBase, TExprContext&)) {
        return [this, handler] (NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& /*optCtx*/, const TGetParents& /*parents*/) {
            return (static_cast<TDerived*>(this)->*handler)(node, ctx);
        };
    }

    template <class TDerived>
    THandler Hndl(NNodes::TMaybeNode<NNodes::TExprBase>(TDerived::* handler)(NNodes::TExprBase, TExprContext&) const) const {
        return [this, handler] (NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& /*optCtx*/, const TGetParents& /*parents*/) {
            return (static_cast<const TDerived*>(this)->*handler)(node, ctx);
        };
    }

protected:
    struct TOptInfo {
        TString OptName;
        TFilter Filter;
        THandler Handler;
    };
    struct TStep {
        TProcessedNodesSet ProcessedNodes;
        TVector<TOptInfo> Optimizers;
        bool Global = false;
    };
    TTypeAnnotationContext* Types;
    const NLog::EComponent LogComponent;
    TSet<TString> DisabledOpts;
    TVector<TStep> Steps;
};

} // NYql
