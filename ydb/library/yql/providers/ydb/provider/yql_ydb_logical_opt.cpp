#include "yql_ydb_provider_impl.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>
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

class TYdbLogicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TYdbLogicalOptProposalTransformer(TYdbState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYdb, {})
        , State_(state)
    {
#define HNDL(name) "LogicalOptimizer-"#name, Hndl(&TYdbLogicalOptProposalTransformer::name)
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembers));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverDqWrap));
#undef HNDL
    }

    TMaybeNode<TExprBase> ExtractMembers(TExprBase node, TExprContext& ctx) const {
        const auto& extract = node.Cast<TCoExtractMembers>();
        const auto& read = extract.Input().Maybe<TCoRight>().Input().Maybe<TYdbReadTable>();
        if (!read) {
            return node;
        }

        const auto& cast = read.Cast();
        return Build<TCoRight>(ctx, extract.Pos())
            .Input<TYdbReadTable>()
                .World(cast.World())
                .DataSource(cast.DataSource())
                .Table(cast.Table())
                .Columns(extract.Members())
            .Build()
            .Done();
    }

    TMaybeNode<TExprBase> ExtractMembersOverDqWrap(TExprBase node, TExprContext& ctx) const {
        const auto& extract = node.Cast<TCoExtractMembers>();
        const auto& input = extract.Input();
        if (const auto& read = input.Maybe<TDqSourceWrap>().Input().Maybe<TYdbSourceSettings>()) {
            const auto& cast = read.Cast();
            return Build<TDqSourceWrap>(ctx, node.Pos())
                .Input<TYdbSourceSettings>()
                    .InitFrom(cast)
                    .Columns(extract.Members())
                    .Build()
                .DataSource(input.Cast<TDqSourceWrap>().DataSource())
                .RowType(ExpandType(node.Pos(), GetSeqItemType(*extract.Ref().GetTypeAnn()), ctx))
                .Done();
        }
        if (const auto& read = input.Maybe<TDqReadWrap>().Input().Maybe<TYdbReadTable>()) {
            const auto& cast = read.Cast();
            return Build<TDqReadWrap>(ctx, node.Pos())
                .InitFrom(input.Cast<TDqReadWrap>())
                .Input<TYdbReadTable>()
                    .InitFrom(cast)
                    .Columns(extract.Members())
                    .Build()
                .Done();
        }
        return node;

    }
private:
    const TYdbState::TPtr State_;
};

}

THolder<IGraphTransformer> CreateYdbLogicalOptProposalTransformer(TYdbState::TPtr state) {
    return MakeHolder<TYdbLogicalOptProposalTransformer>(state);
}

} // namespace NYql
