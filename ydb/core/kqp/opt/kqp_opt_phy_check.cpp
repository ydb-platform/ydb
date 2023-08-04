#include "kqp_opt_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

TAutoPtr<IGraphTransformer> CreateKqpCheckPhysicalQueryTransformer() {
    return CreateFunctorTransformer(
        [](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> TStatus {
            output = input;

            YQL_ENSURE(TMaybeNode<TKqlQuery>(input));
            auto query = TKqlQuery(input);
            YQL_ENSURE(query.Ref().GetTypeAnn());

            for (const auto& result : query.Results()) {
                if (!result.Value().Maybe<TDqConnection>()) {
                    ctx.AddError(TIssue(ctx.GetPosition(result.Pos()), "Failed to build query results."));
                    return TStatus::Error;
                }

                if (!result.Value().Maybe<TDqCnUnionAll>()) {
                    ctx.AddError(TIssue(ctx.GetPosition(result.Pos()), TStringBuilder()
                        << "Unexpected query result connection: "
                        << result.Value().Cast<TDqConnection>().CallableName()));
                    return TStatus::Error;
                }
            }

            for (const auto& effect : query.Effects()) {
                if (!IsBuiltEffect(effect)) {
                    ctx.AddError(TIssue(ctx.GetPosition(effect.Pos()), "Failed to build query effects."));
                    return TStatus::Error;
                }
            }

            TParentsMap parentsMap;
            GatherParents(*input, parentsMap);

            bool hasMultipleConsumers = false;
            bool hasBrokenStage = false;

            VisitExpr(input, [&](const TExprNode::TPtr& expr) {
                TExprBase node{expr};

                if (auto maybeConnection = node.Maybe<TDqConnection>()) {
                    auto connection = maybeConnection.Cast();

                    if (!IsSingleConsumerConnection(connection, parentsMap)) {
                        hasMultipleConsumers = true;
                        YQL_CLOG(ERROR, ProviderKqp) << "Connection #" << connection.Ref().UniqueId()
                            << " (" << connection.CallableName() << ") has multiple consumers.";
                        return false;
                    }
                }

                if (auto maybeOutput = node.Maybe<TDqOutput>()) {
                    auto output = maybeOutput.Cast();

                    // Suppose that particular stage output is used only through single connection
                    // i.e. it's not allowed to consume particular stage output via several connections
                    if (!IsSingleConsumer(output, parentsMap)) {
                        hasMultipleConsumers = true;
                        TStringBuilder sb;
                        sb << "Stage #" << output.Stage().Ref().UniqueId()
                           << " output " << output.Index().Value() << " has multiple consumers: " << Endl
                           << " output: " << KqpExprToPrettyString(output, ctx) << Endl;
                        for (const auto& consumer : GetConsumers(output, parentsMap)) {
                            sb << "consumer: " << KqpExprToPrettyString(*consumer, ctx) << Endl;
                        }
                        YQL_CLOG(ERROR, ProviderKqp) << sb;
                        return false;
                    }
                }

                if (auto maybeStage = node.Maybe<TDqStage>()) {
                    auto stage = maybeStage.Cast();
                    auto stageType = stage.Ref().GetTypeAnn();
                    YQL_ENSURE(stageType);
                    auto stageResultType = stageType->Cast<TTupleExprType>();
                    const auto& stageConsumers = GetConsumers(stage, parentsMap);
                    bool stageWithResult = false;

                    TDynBitMap usedOutputs;
                    for (auto consumer : stageConsumers) {
                        if (auto maybeOutput = TExprBase(consumer).Maybe<TDqOutput>()) {
                            stageWithResult = true;
                            auto output = maybeOutput.Cast();
                            auto outputIndex = FromString<ui32>(output.Index().Value());
                            if (usedOutputs.Test(outputIndex)) {
                                hasMultipleConsumers = true;
                                YQL_CLOG(ERROR, ProviderKqp) << "Stage #" << node.Ref().UniqueId()
                                    << ", output " << outputIndex << " has multiple consumers";
                                return false;
                            }
                            usedOutputs.Set(outputIndex);
                        } else {
                            // There can be also an effect with stage that has dq sinks
                            // Check the following structure:
                            // TKqlQuery (tuple with 2 elems) - results and effects
                            auto stageParentsIt = parentsMap.find(stage.Raw());
                            YQL_ENSURE(stageParentsIt != parentsMap.end());
                            if (stageParentsIt->second.size() != 1) {
                                hasMultipleConsumers = true;
                            } else {
                                const TExprNode* effectNode = *stageParentsIt->second.begin();
                                auto effectParentIt = parentsMap.find(effectNode);
                                YQL_ENSURE(effectParentIt != parentsMap.end());
                                if (effectParentIt->second.size() != 1) {
                                    hasMultipleConsumers = true;
                                } else {
                                    const TExprNode* queryNode = *effectParentIt->second.begin();
                                    YQL_ENSURE(queryNode->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple,
                                        "Stage #" << PrintKqpStageOnly(stage, ctx) << " has unexpected consumer: "
                                            << consumer->Content());
                                }
                            }
                        }
                    }

                    if (stageWithResult) {
                        for (size_t i = 0; i < stageResultType->GetSize(); ++i) {
                            if (!usedOutputs.Test(i)) {
                                hasBrokenStage = true;
                                YQL_CLOG(ERROR, ProviderKqp) << "Stage #" << PrintKqpStageOnly(stage, ctx)
                                    << ", output " << i << " (" << FormatType(stageResultType->GetItems()[i]) << ")"
                                    << " not used";
                                return false;
                            }
                        }
                    }
                }

                YQL_ENSURE(!node.Maybe<TDqPhyStage>());

                return true;
            });

            if (hasMultipleConsumers) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()),
                    "Failed to build physical query: some connection(s) have several consumers"));
                return TStatus::Error;
            }

            if (hasBrokenStage) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()),
                    "Failed to build physical query: some stages are broken"));
                return TStatus::Error;
            }

            return TStatus::Ok;
        });
}

} // namespace NKikimr::NKqp::NOpt
