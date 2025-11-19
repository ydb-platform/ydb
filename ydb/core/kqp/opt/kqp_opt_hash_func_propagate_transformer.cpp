#include "kqp_opt_hash_func_propagate_transformer.h"

#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>

#include <yql/essentials/core/yql_expr_optimize.h>

#include <ydb/library/yql/dq/common/dq_common.h>

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;
using namespace NYql::NDq;

TVector<TDqPhyStage> TopSortStages(const TDqPhyStageList& stages) {
    TVector<TDqPhyStage> topSortedStages;
    topSortedStages.reserve(stages.Size());
    std::function<void(const TDqPhyStage&)> topSort;
    THashSet<ui64 /*uniqueId*/> visitedStages;

    // Assume there is no cycles.
    topSort = [&](const TDqPhyStage& stage) {
        if (visitedStages.contains(stage.Ref().UniqueId())) {
            return;
        }

        for (const auto& input : stage.Inputs()) {
            if (auto connection = input.Maybe<TDqConnection>()) {
                // NOTE: somehow `Output()` is actually an input.
                if (auto phyStage = connection.Cast().Output().Stage().Maybe<TDqPhyStage>()) {
                    topSort(phyStage.Cast());
                }
            }
        }

        visitedStages.insert(stage.Ref().UniqueId());
        topSortedStages.push_back(stage);
    };

    for (const auto& stage : stages) {
        topSort(stage);
    }

    return topSortedStages;
}

TMaybeNode<TKqpPhysicalTx> PropogateHashFuncToHashShuffles(
    const TKqpPhysicalTx& tx,
    TExprContext& ctx,
    const TKikimrConfiguration::TPtr& config
) {
    const auto topSortedStages = TopSortStages(tx.Stages());

    TVector<TDqPhyStage> newStages;
    newStages.reserve(topSortedStages.size());

    THashMap<ui64, NDq::EHashShuffleFuncType> hashTypeByStageID;
    TNodeOnNodeOwnedMap stagesMap;
    for (const auto& stage : topSortedStages) {
        bool isRead = false;
        VisitExpr(
            stage.Program().Body().Ptr(),
            [&isRead](const TExprNode::TPtr& node) {
                if (TKqpTable::Match(node.Get())) {
                    isRead = true;
                }
                return !isRead;
            }
        );

        bool enableShuffleElimination = config->OptShuffleElimination.Get().GetOrElse(config->DefaultEnableShuffleElimination);
        auto stageHashType = config->HashShuffleFuncType.Get().GetOrElse(config->DefaultHashShuffleFuncType);
        if (isRead && enableShuffleElimination) {
            stageHashType =  config->ColumnShardHashShuffleFuncType.Get().GetOrElse(config->DefaultColumnShardHashShuffleFuncType);
        } else {
            for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
                auto input = stage.Inputs().Item(i);
                if (auto maybeMap = input.Maybe<TDqCnMap>()) {
                    ui64 inputStageID = maybeMap.Cast().Output().Stage().Ptr()->UniqueId();
                    if (hashTypeByStageID.contains(inputStageID)) {
                        stageHashType = hashTypeByStageID[inputStageID];
                    }
                }
            }
        }

        ui64 stageID = stage.Ptr()->UniqueId();
        hashTypeByStageID[stageID] = stageHashType;

        TNodeOnNodeOwnedMap stagesInputMap;
        for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
            auto input = stage.Inputs().Item(i);

            if (auto maybeHashShuffle = input.Maybe<TDqCnHashShuffle>()) {
                auto hashShuffle = maybeHashShuffle.Cast();
                auto withHashFunc =
                    Build<TDqCnHashShuffle>(ctx, hashShuffle.Pos())
                        .InitFrom(maybeHashShuffle.Cast());

                if (!hashShuffle.UseSpilling().IsValid()) {
                    // this is YQL bug when we can't init theу next field by index, if the previous wasn't initialized
                    withHashFunc
                        .UseSpilling().Build(false);
                }

                withHashFunc
                    .HashFunc()
                        .Build(ToString(hashTypeByStageID[stageID]));

                stagesInputMap.emplace(input.Raw(), withHashFunc.Done().Ptr());
            }
        }

        auto newStage = Build<TDqPhyStage>(ctx, stage.Pos())
            .InitFrom(stage)
            .Inputs(ctx.ReplaceNodes(ctx.ReplaceNodes(stage.Inputs().Ptr(), stagesInputMap), stagesMap))
            .Done();
        stagesMap.emplace(stage.Raw(), newStage.Ptr());
        newStages.emplace_back(std::move(newStage));
    }

    return
        Build<TKqpPhysicalTx>(ctx, tx.Pos())
            .InitFrom(tx)
            .Stages()
                .Add(newStages)
            .Build()
            .Results(ctx.ReplaceNodes(tx.Results().Ptr(), stagesMap))
        .Done();
}

class TKqpTxHashFuncPropagateTransformer : public TSyncTransformerBase {
public:
    TKqpTxHashFuncPropagateTransformer(const TKikimrConfiguration::TPtr& config)
        : Config(config)
    {}

    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_ENSURE(TExprBase(input).Maybe<TKqpPhysicalTx>());

        auto tx = TExprBase(input).Cast<TKqpPhysicalTx>();
        auto optimizedTx = PropogateHashFuncToHashShuffles(tx, ctx, Config);
        if (!optimizedTx) {
            return TStatus::Error;
        }

        output = optimizedTx.Cast().Ptr();
        return IGraphTransformer::TStatus::Ok;
    }

    void Rewind() override {}

private:
    TKikimrConfiguration::TPtr Config;
};

TAutoPtr<IGraphTransformer> CreateKqpTxHashFuncPropagateTransformer(const TKikimrConfiguration::TPtr& config) {
    return THolder<IGraphTransformer>(new TKqpTxHashFuncPropagateTransformer(config));
}

class TKqpTxsHashFuncPropagateTransformer : public TSyncTransformerBase {
public:
    TKqpTxsHashFuncPropagateTransformer(
        TAutoPtr<NYql::IGraphTransformer> typeAnnTransformer,
        TTypeAnnotationContext& typesCtx,
        const TKikimrConfiguration::TPtr& config
    )
        : TypeAnnTransformer(std::move(typeAnnTransformer))
    {
        TxTransformer =
            TTransformationPipeline(&typesCtx)
                .AddServiceTransformers()
                .Add(*TypeAnnTransformer, "TypeAnnotation")
                .AddPostTypeAnnotation(/* forSubgraph */ true)
                .Add(CreateKqpTxHashFuncPropagateTransformer(config), "Peephole")
            .Build(false);
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        if (!TKqpPhysicalQuery::Match(input.Get())) {
            return TStatus::Error;
        }

        TKqpPhysicalQuery query(input);

        TVector<TKqpPhysicalTx> txs;
        txs.reserve(query.Transactions().Size());
        for (const auto& tx : query.Transactions()) {
            auto expr = TransformTx(tx, ctx);
            txs.push_back(expr.Cast());
        }

        auto phyQuery = Build<TKqpPhysicalQuery>(ctx, query.Pos())
            .Transactions()
                .Add(txs)
                .Build()
            .Results(query.Results())
            .Settings(query.Settings())
            .Done();

        output = phyQuery.Ptr();
        return TStatus::Ok;
    }

    void Rewind() final {
        TxTransformer->Rewind();
    }

private:
    TMaybeNode<TKqpPhysicalTx> TransformTx(const TKqpPhysicalTx& tx, TExprContext& ctx) {
        TxTransformer->Rewind();

        auto expr = tx.Ptr();

        while (true) {
            auto status = InstantTransform(*TxTransformer, expr, ctx);
            if (status == TStatus::Error) {
                return {};
            }
            if (status == TStatus::Ok) {
                break;
            }
        }
        return TKqpPhysicalTx(expr);
    }

    TAutoPtr<IGraphTransformer> TxTransformer;
    TAutoPtr<NYql::IGraphTransformer> TypeAnnTransformer;
};

TAutoPtr<IGraphTransformer>  NKikimr::NKqp::CreateKqpTxsHashFuncPropagateTransformer(
    TAutoPtr<NYql::IGraphTransformer> typeAnnTransformer,
    TTypeAnnotationContext& typesCtx,
    const TKikimrConfiguration::TPtr& config
) {
    return THolder<IGraphTransformer>(new TKqpTxsHashFuncPropagateTransformer(typeAnnTransformer, typesCtx, config));
}
