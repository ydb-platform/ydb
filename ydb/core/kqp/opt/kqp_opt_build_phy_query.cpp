#include "kqp_opt_impl.h"

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

namespace {

class TKqpBuildPhysicalQueryTransformer : public TSyncTransformerBase {
public:
    TKqpBuildPhysicalQueryTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
        const TIntrusivePtr<TKqpBuildQueryContext>& buildCtx)
        : KqpCtx(kqpCtx)
        , BuildCtx(buildCtx) {
    }

    TStatus DoTransform(TExprNode::TPtr inputExpr, TExprNode::TPtr& outputExpr, TExprContext& ctx) final {
        if (TKqpPhysicalQuery::Match(inputExpr.Get())) {
            outputExpr = inputExpr;
            return TStatus::Ok;
        }

        TKqpPhyQuerySettings querySettings;
        switch (KqpCtx->QueryCtx->Type) {
            case EKikimrQueryType::Dml: {
                querySettings.Type = EPhysicalQueryType::Data;
                break;
            }
            case EKikimrQueryType::Scan: {
                querySettings.Type = EPhysicalQueryType::Scan;
                break;
            }
            case EKikimrQueryType::Query: {
                querySettings.Type = EPhysicalQueryType::GenericQuery;
                break;
            }
            case EKikimrQueryType::Script: {
                querySettings.Type = EPhysicalQueryType::GenericScript;
                break;
            }
            default: {
                YQL_ENSURE(false, "Unexpected query type: " << KqpCtx->QueryCtx->Type);
            }
        }

        auto phyQuery = Build<TKqpPhysicalQuery>(ctx, inputExpr->Pos())
            .Transactions()
                .Add(BuildCtx->PhysicalTxs)
                .Build()
            .Results()
                .Add(BuildCtx->QueryResults)
                .Build()
            .Settings(querySettings.BuildNode(ctx, inputExpr->Pos()))
            .Done();

        outputExpr = phyQuery.Ptr();
        return TStatus(TStatus::Repeat, true);
    }

    void Rewind() final {
    }
private:
    TIntrusivePtr<TKqpOptimizeContext> KqpCtx;
    TIntrusivePtr<TKqpBuildQueryContext> BuildCtx;
};
} // namespace

TAutoPtr<IGraphTransformer> CreateKqpBuildPhysicalQueryTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    const TIntrusivePtr<TKqpBuildQueryContext>& buildCtx)
{
    return new TKqpBuildPhysicalQueryTransformer(kqpCtx, buildCtx);
}

} // namespace NKikimr::NKqp::NOpt
