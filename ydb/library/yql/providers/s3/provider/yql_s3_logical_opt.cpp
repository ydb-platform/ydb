#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
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

class TS3LogicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TS3LogicalOptProposalTransformer(TS3State::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderS3, {})
        , State_(state)
    {
#define HNDL(name) "LogicalOptimizer-"#name, Hndl(&TS3LogicalOptProposalTransformer::name)
        AddHandler(0, &TCoLeft::Match, HNDL(TrimReadWorld));
#undef HNDL
    }

    TMaybeNode<TExprBase> TrimReadWorld(TExprBase node, TExprContext& ctx) const {
        const auto& maybeRead = node.Cast<TCoLeft>().Input().Maybe<TS3ReadObject>();
        if (!maybeRead) {
            return node;
        }

        return TExprBase(ctx.NewWorld(node.Pos()));
    }
private:
    const TS3State::TPtr State_;
};

}

THolder<IGraphTransformer> CreateS3LogicalOptProposalTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3LogicalOptProposalTransformer>(state);
}

} // namespace NYql
