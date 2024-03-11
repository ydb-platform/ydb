#include "yql_clickhouse_provider_impl.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>


namespace NYql {

using namespace NNodes;

namespace {

class TClickHouseLogicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TClickHouseLogicalOptProposalTransformer(TClickHouseState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderClickHouse, {})
        , State_(state)
    {
#define HNDL(name) "LogicalOptimizer-"#name, Hndl(&TClickHouseLogicalOptProposalTransformer::name)
        AddHandler(0, &TCoLeft::Match, HNDL(TrimReadWorld));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembers));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverDqWrap));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverDqSourceWrap));
#undef HNDL
    }

    TMaybeNode<TExprBase> TrimReadWorld(TExprBase node, TExprContext& ctx) const {
        if (const auto maybeRead = node.Cast<TCoLeft>().Input().Maybe<TClReadTable>())
            return TExprBase(ctx.NewWorld(node.Pos()));
        return node;

    }

    TMaybeNode<TExprBase> ExtractMembers(TExprBase node, TExprContext& ctx) const {
        const auto extract = node.Cast<TCoExtractMembers>();
        const auto input = extract.Input();
        const auto read = input.Maybe<TCoRight>().Input().Maybe<TClReadTable>();
        if (!read) {
            return node;
        }

        return Build<TCoRight>(ctx, extract.Pos())
            .Input<TClReadTable>()
                .InitFrom(read.Cast())
                .Columns(extract.Members())
            .Build()
            .Done();
    }

    TMaybeNode<TExprBase> ExtractMembersOverDqWrap(TExprBase node, TExprContext& ctx) const {
        auto extract = node.Cast<TCoExtractMembers>();
        auto input = extract.Input();
        auto read = input.Maybe<TDqReadWrap>().Input().Maybe<TClReadTable>();
        if (!read) {
            return node;
        }

        return Build<TDqReadWrap>(ctx, node.Pos())
            .InitFrom(input.Cast<TDqReadWrap>())
            .Input<TClReadTable>()
                .InitFrom(read.Cast())
                .Columns(extract.Members())
            .Build()
            .Done();
    }

    TMaybeNode<TExprBase> ExtractMembersOverDqSourceWrap(TExprBase node, TExprContext& ctx) const {
        const auto extract = node.Cast<TCoExtractMembers>();
        const auto input = extract.Input();
        const auto read = input.Maybe<TDqSourceWrap>().Input().Maybe<TClSourceSettings>();
        if (!read) {
            return node;
        }

        return Build<TDqSourceWrap>(ctx, node.Pos())
            .Input<TClSourceSettings>()
                .InitFrom(read.Cast())
                .Columns(extract.Members())
            .Build()
            .DataSource(input.Cast<TDqSourceWrap>().DataSource())
            .RowType(ExpandType(node.Pos(), GetSeqItemType(*extract.Ref().GetTypeAnn()), ctx))
            .Done();
    }
private:
    const TClickHouseState::TPtr State_;
};

}

THolder<IGraphTransformer> CreateClickHouseLogicalOptProposalTransformer(TClickHouseState::TPtr state) {
    return MakeHolder<TClickHouseLogicalOptProposalTransformer>(state);
}

} // namespace NYql
