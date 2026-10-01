#include "yql_ydb_remote_provider_impl.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/ydb_remote/expr_nodes/yql_ydb_remote_expr_nodes.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>

namespace NYql::NYdbRemote {
namespace {

using namespace NNodes;

class TLogicalOptimizer final : public TOptimizeTransformerBase {
public:
    explicit TLogicalOptimizer(TState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderDq, {})
        , State_(std::move(state))
    {
        AddHandler(0, &TCoExtractMembers::Match, "YdbRemoteExtractMembersRight", Hndl(&TLogicalOptimizer::ExtractMembersRead<TCoRight>));
        AddHandler(0, &TCoExtractMembers::Match, "YdbRemoteExtractMembersReadWrap", Hndl(&TLogicalOptimizer::ExtractMembersRead<TDqReadWrap>));
        AddHandler(0, &TCoExtractMembers::Match, "YdbRemoteExtractMembersSource", Hndl(&TLogicalOptimizer::ExtractMembersSource));
        AddHandler(0, &TDqLookupSourceWrap::Match, "YdbRemoteRejectLookup", Hndl(&TLogicalOptimizer::RejectLookup));
    }

private:
    template <class TWrap>
    TMaybeNode<TExprBase> ExtractMembersRead(TExprBase node, TExprContext& ctx) const {
        const auto extract = node.Cast<TCoExtractMembers>();
        const auto wrapper = extract.Input().Maybe<TWrap>();
        const auto read = wrapper.Input().template Maybe<TYdbRemoteReadTable>();
        if (!read) {
            return node;
        }
        // Only push a projection directly on a read. Common optimizers retain
        // columns used by intervening local filters before producing this shape.
        return Build<TWrap>(ctx, node.Pos())
            .InitFrom(wrapper.Cast())
            .template Input<TYdbRemoteReadTable>()
                .InitFrom(read.Cast())
                .Columns(extract.Members())
            .Build()
            .Done();
    }

    TMaybeNode<TExprBase> ExtractMembersSource(TExprBase node, TExprContext& ctx) const {
        const auto extract = node.Cast<TCoExtractMembers>();
        const auto wrapper = extract.Input().Maybe<TDqSourceWrap>();
        const auto source = wrapper.Input().Maybe<TYdbRemoteSourceSettings>();
        if (!source) {
            return node;
        }
        auto columns = extract.Members().Ptr();
        if (!columns->ChildrenSize()) {
            // A physical carrier preserves row count for COUNT(*) and constant
            // projections. The public RowType remains the empty projected type.
            const auto& table = State_->Tables.at(TState::TTableKey(
                source.Cast().Cluster().StringValue(), source.Cast().Table().StringValue()));
            columns = ctx.NewList(node.Pos(), {ctx.NewAtom(node.Pos(), table.RowType->GetItems().front()->GetName())});
        }
        return Build<TDqSourceWrap>(ctx, node.Pos())
            .InitFrom(wrapper.Cast())
            .Input<TYdbRemoteSourceSettings>()
                .InitFrom(source.Cast())
                .Columns(columns)
            .Build()
            .RowType(ExpandType(node.Pos(), GetSeqItemType(*extract.Ref().GetTypeAnn()), ctx))
            .Done();
    }

    TMaybeNode<TExprBase> RejectLookup(TExprBase node, TExprContext& ctx) const {
        if (node.Cast<TDqLookupSourceWrap>().Input().Maybe<TYdbRemoteSourceSettings>()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Native YDB streamlookup joins are not supported"));
            return {};
        }
        return node;
    }

    const TState::TPtr State_;
};

} // namespace

THolder<IGraphTransformer> CreateLogicalOptimizer(TState::TPtr state) {
    return MakeHolder<TLogicalOptimizer>(std::move(state));
}

} // namespace NYql::NYdbRemote
