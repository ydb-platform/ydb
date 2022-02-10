#include "kqp_prepare_impl.h"

#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_expr_optimize.h> 

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

class TKqpSubstituteTransformer : public TSyncTransformerBase {
public:
    TKqpSubstituteTransformer(TIntrusivePtr<TKqpTransactionState> txState,
        TIntrusivePtr<TKqlTransformContext> transformCtx)
        : TxState(txState)
        , TransformCtx(transformCtx) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        const auto& analyzeResults = TransformCtx->AnalyzeResults;

        if (analyzeResults.CanExecute) {
            return TStatus::Ok;
        }

        TNodeOnNodeOwnedMap replaceMap;
        for (size_t i = 0; i < analyzeResults.ExecutionRoots.size(); ++i) {
            auto newParamName = TxState->Tx().NewParamName();

            auto node = analyzeResults.ExecutionRoots[i].Node;

            YQL_ENSURE(node.Ref().GetTypeAnn());
            auto paramNode = Build<TCoParameter>(ctx, node.Pos())
                .Name().Build(newParamName)
                .Type(ExpandType(node.Pos(), *node.Ref().GetTypeAnn(), ctx))
                .Done();

            YQL_ENSURE(!TransformCtx->MkqlResults.empty());
            ui32 mkqlIndex = TransformCtx->MkqlResults.size() - 1;
            ui32 resultIndex = i;
            auto indexTuple = Build<TCoAtomList>(ctx, paramNode.Pos())
                .Add().Build(ToString(mkqlIndex))
                .Add().Build(ToString(resultIndex))
                .Done();

            paramNode.Ptr()->SetResult(indexTuple.Ptr());

            replaceMap.emplace(node.Raw(), paramNode.Ptr());
        }

        output = ctx.ReplaceNodes(std::move(input), replaceMap);

        return TStatus(TStatus::Repeat, true);
    }

private:
    TIntrusivePtr<TKqpTransactionState> TxState;
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKqpSubstituteTransformer(TIntrusivePtr<TKqpTransactionState> txState,
    TIntrusivePtr<TKqlTransformContext> transformCtx)
{
    return new TKqpSubstituteTransformer(txState, transformCtx);
}

} // namespace NKqp
} // namespace NKikimr
