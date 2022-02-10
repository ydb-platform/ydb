#include "yql_pq_provider_impl.h"

#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h> 


namespace NYql {

using namespace NNodes;

namespace {

class TPqLogicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TPqLogicalOptProposalTransformer(TPqState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderPq, {})
        , State_(state)
    {
#define HNDL(name) "LogicalOptimizer-"#name, Hndl(&TPqLogicalOptProposalTransformer::name)
        AddHandler(0, &TCoLeft::Match, HNDL(TrimReadWorld));
      //  AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembers));
      //  AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverDqWrap));
        #undef HNDL
    }

    TMaybeNode<TExprBase> TrimReadWorld(TExprBase node, TExprContext& ctx) const {
        const auto& maybeRead = node.Cast<TCoLeft>().Input().Maybe<TPqReadTopic>();
        if (!maybeRead) {
            return node;
        }

        return TExprBase(ctx.NewWorld(node.Pos()));
    }
    /*
    TMaybeNode<TExprBase> ExtractMembers(TExprBase node, TExprContext& ctx) const {
        const auto& extract = node.Cast<TCoExtractMembers>();
        const auto& input = extract.Input();
        const auto& read = input.Maybe<TCoRight>().Input().Maybe<TPqReadTopic>();
        if (!read) {
            return node;
        }

        const auto& cast = read.Cast();
        return Build<TCoRight>(ctx, extract.Pos())
            .Input<TPqReadTopic>()
            .World(cast.World())
            .DataSource(cast.DataSource())
            .Topic(cast.Topic())
            .Columns(extract.Members())
            .Build()
            .Done();
    }

    TMaybeNode<TExprBase> ExtractMembersOverDqWrap(TExprBase node, TExprContext& ctx) const {
        const auto& extract = node.Cast<TCoExtractMembers>();
        const auto& input = extract.Input();
        const auto& read = input.Maybe<TDqReadWrap>().Input().Maybe<TPqReadTopic>();
        if (!read) {
            return node;
        }

        const auto& cast = read.Cast();
        return Build<TDqReadWrap>(ctx, node.Pos())
            .InitFrom(input.Cast<TDqReadWrap>())
            .Input<TPqReadTopic>()
            .InitFrom(cast)
            .Columns(extract.Members())
            .Build()
            .Done();
    }*/

private:
    TPqState::TPtr State_;
};

}

THolder<IGraphTransformer> CreatePqLogicalOptProposalTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqLogicalOptProposalTransformer>(state);
}

} // namespace NYql
