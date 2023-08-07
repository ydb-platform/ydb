#include "kqp_opt_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/core/services/yql_out_transformers.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

namespace {

TAutoPtr<NYql::IGraphTransformer> CreateKqpBuildPhyStagesTransformer(bool allowDependantConsumers, TTypeAnnotationContext& typesCtx) {
    EChannelMode mode = EChannelMode::CHANNEL_SCALAR;
    return NDq::CreateDqBuildPhyStagesTransformer(allowDependantConsumers, typesCtx, mode);
}

class TKqpBuildTxTransformer : public TSyncTransformerBase {
public:
    TKqpBuildTxTransformer()
        : QueryType(EKikimrQueryType::Unspecified)
        , IsPrecompute(false) {}

    void Init(EKikimrQueryType queryType, bool isPrecompute) {
        QueryType = queryType;
        IsPrecompute = isPrecompute;
    }

    TStatus DoTransform(TExprNode::TPtr inputExpr, TExprNode::TPtr& outputExpr, TExprContext& ctx) final {
        if (TKqpPhysicalTx::Match(inputExpr.Get())) {
            outputExpr = inputExpr;
            return TStatus::Ok;
        }

        YQL_ENSURE(inputExpr->IsList() && inputExpr->ChildrenSize() > 0);

        if (TKqlQueryResult::Match(inputExpr->Child(0))) {
            YQL_CLOG(DEBUG, ProviderKqp) << ">>> TKqpBuildTxTransformer[" << QueryType << "/"
                << (IsPrecompute ? "precompute" : "result") << "]: " << KqpExprToPrettyString(*inputExpr, ctx);

            return DoBuildTxResults(inputExpr, outputExpr, ctx);
        } else {
            YQL_CLOG(DEBUG, ProviderKqp) << ">>> TKqpBuildTxTransformer[effects]: "
                << KqpExprToPrettyString(*inputExpr, ctx);

            return DoBuildTxEffects(inputExpr, outputExpr, ctx);
        }
    }

    void Rewind() final {
    }

private:
    EPhysicalTxType GetPhyTxType(bool allStagesArePure) {
        if (QueryType == EKikimrQueryType::Scan) {
            if (IsPrecompute && allStagesArePure) {
                return EPhysicalTxType::Compute;
            }

            return EPhysicalTxType::Scan;
        }

        if (QueryType == EKikimrQueryType::Query || QueryType == EKikimrQueryType::Script) {
            if (IsPrecompute && allStagesArePure) {
                return EPhysicalTxType::Compute;
            }

            return EPhysicalTxType::Generic;
        }

        if (allStagesArePure) {
            return EPhysicalTxType::Compute;
        }

        return EPhysicalTxType::Data;
    }

    TStatus DoBuildTxResults(TExprNode::TPtr inputExpr, TExprNode::TPtr& outputExpr, TExprContext& ctx) {
        auto stages = CollectStages(inputExpr, ctx);
        Y_VERIFY_DEBUG(!stages.empty());

        auto results = TKqlQueryResultList(inputExpr);
        auto txResults = BuildTxResults(results, stages, ctx);
        if (!txResults) {
            return TStatus::Error;
        }

        TKqpPhyTxSettings txSettings;
        txSettings.Type = GetPhyTxType(AreAllStagesKqpPure(stages));
        txSettings.WithEffects = false;

        auto tx = Build<TKqpPhysicalTx>(ctx, inputExpr->Pos())
            .Stages()
                .Add(stages)
                .Build()
            .Results(txResults.Cast())
            .ParamBindings()
                .Build()
            .Settings(txSettings.BuildNode(ctx, inputExpr->Pos()))
            .Done();

        auto newTx = ExtractParamsFromTx(tx, ctx);
        if (!newTx) {
            return TStatus::Error;
        }

        outputExpr = newTx.Cast().Ptr();
        return TStatus(TStatus::Repeat, true);
    }

    TStatus DoBuildTxEffects(TExprNode::TPtr inputExpr, TExprNode::TPtr& outputExpr, TExprContext& ctx) {
        auto stages = CollectStages(inputExpr, ctx);
        Y_VERIFY_DEBUG(!stages.empty());

        TKqpPhyTxSettings txSettings;
        txSettings.Type = EPhysicalTxType::Data;
        txSettings.WithEffects = true;

        auto tx = Build<TKqpPhysicalTx>(ctx, inputExpr->Pos())
            .Stages()
                .Add(stages)
                .Build()
            .Results()
                .Build()
            .ParamBindings()
                .Build()
            .Settings(txSettings.BuildNode(ctx, inputExpr->Pos()))
            .Done();

        auto newTx = ExtractParamsFromTx(tx, ctx);
        if (!newTx) {
            return TStatus::Error;
        }

        outputExpr = newTx.Cast().Ptr();
        return TStatus(TStatus::Repeat, true);
    }

private:
    static TVector<TDqPhyStage> CollectStages(const TExprNode::TPtr& node, TExprContext& /* ctx */) {
        TVector<TDqPhyStage> stages;

        auto filter = [](const TExprNode::TPtr& exprNode) {
            return !exprNode->IsLambda();
        };

        auto collector = [&stages](const TExprNode::TPtr& exprNode) {
            if (TDqPhyStage::Match(exprNode.Get())) {
                stages.emplace_back(TDqPhyStage(exprNode));
            } else {
                YQL_ENSURE(!TDqStage::Match(exprNode.Get()));
            }
            return true;
        };

        VisitExpr(node, filter, collector);

        return stages;
    }

    static bool AreAllStagesKqpPure(const TVector<TDqPhyStage>& stages) {
        // TODO: Avoid lambda analysis here, use sources/sinks for table interaction.
        return std::all_of(stages.begin(), stages.end(), [](const auto& x) { return IsKqpPureLambda(x.Program()) && IsKqpPureInputs(x.Inputs()); });
    }

    static TMaybeNode<TExprList> BuildTxResults(const TKqlQueryResultList& results, TVector<TDqPhyStage>& stages,
        TExprContext& ctx)
    {
        if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::ProviderKqp, NYql::NLog::ELevel::TRACE)) {
            TStringBuilder sb;
            sb << "-- BuildTxResults" << Endl;
            sb << "  results:" << Endl;
            for (const auto& r : results) {
                sb << "    * [" << r.Raw()->UniqueId() << "] " << KqpExprToPrettyString(r.Value(), ctx) << Endl;
            }
            YQL_CLOG(TRACE, ProviderKqp) << sb;
        }

        TVector<TExprBase> builtResults;
        builtResults.reserve(results.Size());

        for (const auto& result : results) {
            if (auto maybeUnionAll = result.Value().Maybe<TDqCnUnionAll>()) {
                auto resultConnection = maybeUnionAll.Cast();
                auto resultStage = resultConnection.Output().Stage().Cast<TDqPhyStage>();
                ui32 resultIndex = FromString<ui32>(resultConnection.Output().Index());

//                if (resultIndex != 0) {
//                    ctx.AddError(TIssue(ctx.GetPosition(result.Pos()), TStringBuilder()
//                        << "Unexpected result index: " << resultIndex));
//                    return {};
//                }

                bool needsCollectStage = true;

                // TODO: This is a temporary workaround until we have a proper constraints support.
                // If result stage has single UnionAll/Merge input, we don't have to build a separate stage
                // for results collection as it's already in single partition.
                // Proper check should use partitioning information for results stage via opt constraints.
                if (resultStage.Inputs().Size() == 1) {
                    if (resultStage.Inputs().Item(0).Maybe<TDqCnUnionAll>() ||
                        resultStage.Inputs().Item(0).Maybe<TDqCnMerge>())
                    {
                        needsCollectStage = false;
                    }
                }

                // If results stage is marked as single_partition, no collect stage needed.
                // Once we have partitioning constraint we should check it instead of stage setting.
                auto settings = TDqStageSettings::Parse(resultStage);
                if (settings.SinglePartition) {
                    needsCollectStage = false;
                }

                TDqPhyStage collectStage = resultStage;
                if (needsCollectStage) {
                    collectStage = Build<TDqPhyStage>(ctx, results.Pos())
                        .Inputs()
                            .Add(resultConnection)
                            .Build()
                        .Program()
                            .Args({"row"})
                            .Body("row")
                            .Build()
                        .Settings(NDq::TDqStageSettings::New().BuildNode(ctx, results.Pos()))
                        .Done();
                    resultIndex = 0;
                    stages.emplace_back(collectStage);
                }

                auto newResult = Build<TDqCnResult>(ctx, results.Pos())
                    .Output()
                        .Stage(collectStage)
                        .Index().Build(ToString(resultIndex))
                        .Build()
                    .ColumnHints(result.ColumnHints())
                    .Done();

                builtResults.emplace_back(newResult);
                continue;
            } // DqCnUnionAll

            if (result.Value().Maybe<TDqCnValue>()) {
                builtResults.emplace_back(result.Value());
                continue;
            } // DqCnValue

            if (auto maybeConnection = result.Value().Maybe<TDqConnection>()) {
                ctx.AddError(TIssue(ctx.GetPosition(result.Pos()), TStringBuilder()
                    << "Unexpected connection in results transaction: " << maybeConnection.Cast().CallableName()));
                return {};
            } // any other DqConnection

            ctx.AddError(TIssue(ctx.GetPosition(result.Pos()), TStringBuilder()
                << "Unexpected node in results: " << KqpExprToPrettyString(result.Value(), ctx)));
            return {};
        }

        return Build<TExprList>(ctx, results.Pos())
            .Add(builtResults)
            .Done();
    }

    static TMaybeNode<TDqPhyStage> ExtractParamsFromStage(const TDqPhyStage& stage, const TNodeOnNodeOwnedMap& stagesMap,
        TMap<TString, TKqpParamBinding>& bindingsMap, TExprContext& ctx)
    {
        auto bindingsBuilder = [&bindingsMap, &ctx] (const TExprNode::TPtr& node) {
            auto maybeParam = TMaybeNode<TCoParameter>(node);

            if (!maybeParam.IsValid()) {
                return true;
            }

            auto param = maybeParam.Cast();

            TString paramName(param.Name());

            if (bindingsMap.contains(paramName)) {
                return true;
            }

            auto paramBinding = Build<TKqpParamBinding>(ctx, param.Pos())
                .Name().Build(paramName)
                .Done();

            bindingsMap.emplace(std::move(paramName), std::move(paramBinding));

            YQL_ENSURE(!TDqConnection::Match(node.Get()));
            return true;
        };

        VisitExpr(stage.Program().Body().Ptr(), bindingsBuilder);
        for (ui32 i = 0; i < stage.Inputs().Size(); ++i) {
            auto input = stage.Inputs().Item(i);
            if (input.Maybe<TDqSource>()) {
                VisitExpr(input.Ptr(), bindingsBuilder);
            }
        }

        TVector<TExprBase> newInputs;
        TVector<TCoArgument> newArgs;
        TNodeOnNodeOwnedMap argsMap;

        auto makeParameterBinding = [&ctx, &bindingsMap] (TKqpTxResultBinding binding, TPositionHandle pos) {
            TString paramName = TStringBuilder() << ParamNamePrefix
                << "tx_result_binding_" << binding.TxIndex().Value() << "_" << binding.ResultIndex().Value();

            auto type = binding.Type().Ref().GetTypeAnn();
            YQL_ENSURE(type);
            YQL_ENSURE(type->GetKind() == ETypeAnnotationKind::Type);
            type = type->Cast<TTypeExprType>()->GetType();
            YQL_ENSURE(type);

            TExprBase parameter = Build<TCoParameter>(ctx, pos)
                .Name().Build(paramName)
                .Type(ExpandType(pos, *type, ctx))
                .Done();

            // TODO: (Iterator|ToStream (Parameter ...)) -> (ToFlow (Parameter ...))
//            if (type->GetKind() == ETypeAnnotationKind::List) {
//                parameter = Build<TCoToFlow>(ctx, input.Pos()) // TODO: TDqInputReader?
//                    .Input(parameter)
//                    .Done();
//            }

            auto paramBinding = Build<TKqpParamBinding>(ctx, pos)
                .Name().Build(paramName)
                .Binding(binding)
                .Done();

            auto inserted = bindingsMap.emplace(paramName, paramBinding);
            if (!inserted.second) {
                YQL_ENSURE(inserted.first->second.Binding().Raw() == binding.Raw(),
                    "duplicated parameter " << paramName
                    << ", first: " << KqpExprToPrettyString(inserted.first->second.Binding().Ref(), ctx)
                    << ", second: " << KqpExprToPrettyString(binding, ctx));
            }
            return parameter;
        };

        TNodeOnNodeOwnedMap sourceReplaceMap;
        for (ui32 i = 0; i < stage.Inputs().Size(); ++i) {
            const auto& input = stage.Inputs().Item(i);
            const auto& inputArg = stage.Program().Args().Arg(i);

            if (auto source = input.Maybe<TDqSource>()) {
                VisitExpr(input.Ptr(),
                    [&](const TExprNode::TPtr& node) {
                        TExprBase expr(node);
                        YQL_ENSURE(!expr.Maybe<TDqConnection>().IsValid());
                        if (auto binding = expr.Maybe<TKqpTxResultBinding>()) {
                            sourceReplaceMap.emplace(node.Get(), makeParameterBinding(binding.Cast(), node->Pos()).Ptr());
                        }
                        return true;
                    });
            }

            auto maybeBinding = input.Maybe<TKqpTxResultBinding>();

            if (!maybeBinding.IsValid()) {
                auto newArg = ctx.NewArgument(inputArg.Pos(), inputArg.Name());
                newInputs.push_back(input);
                newArgs.emplace_back(TCoArgument(newArg));
                argsMap.emplace(inputArg.Raw(), std::move(newArg));
                continue;
            }

            argsMap.emplace(inputArg.Raw(), makeParameterBinding(maybeBinding.Cast(), input.Pos()).Ptr());
        }

        auto inputs = Build<TExprList>(ctx, stage.Pos())
            .Add(newInputs)
            .Done();

        return Build<TDqPhyStage>(ctx, stage.Pos())
            .Inputs(ctx.ReplaceNodes(ctx.ReplaceNodes(inputs.Ptr(), stagesMap), sourceReplaceMap))
            .Program()
                .Args(newArgs)
                .Body(ctx.ReplaceNodes(stage.Program().Body().Ptr(), argsMap))
                .Build()
            .Settings(stage.Settings())
            .Outputs(stage.Outputs())
            .Done();
    }

    static TMaybeNode<TKqpPhysicalTx> ExtractParamsFromTx(TKqpPhysicalTx& tx, TExprContext& ctx) {
        TVector<TDqPhyStage> newStages;
        newStages.reserve(tx.Stages().Size());
        TNodeOnNodeOwnedMap stagesMap;
        TMap<TString, TKqpParamBinding> bindingsMap;

        for (const auto& stage : tx.Stages()) {
            auto newStage = ExtractParamsFromStage(stage, stagesMap, bindingsMap, ctx);
            if (!newStage) {
                return {};
            }

            newStages.emplace_back(newStage.Cast());
            stagesMap.emplace(stage.Raw(), newStage.Cast().Ptr());
        }

        TVector<TKqpParamBinding> bindings;
        bindings.reserve(bindingsMap.size());
        for (auto& pair : bindingsMap) {
            bindings.push_back(pair.second);
        }

        return Build<TKqpPhysicalTx>(ctx, tx.Pos())
            .Stages()
                .Add(newStages)
                .Build()
            .Results(ctx.ReplaceNodes(tx.Results().Ptr(), stagesMap))
            .ParamBindings()
                .Add(bindings)
                .Build()
            .Settings(tx.Settings())
            .Done();
    }

private:
    EKikimrQueryType QueryType;
    bool IsPrecompute;
};

TVector<TDqPhyPrecompute> PrecomputeInputs(const TDqStage& stage) {
    TVector<TDqPhyPrecompute> result;
    for (const auto& input : stage.Inputs()) {
        if (auto maybePrecompute = input.Maybe<TDqPhyPrecompute>()) {
            result.push_back(maybePrecompute.Cast());
        } else if (auto maybeSource = input.Maybe<TDqSource>()) {
            VisitExpr(maybeSource.Cast().Ptr(),
                  [&] (const TExprNode::TPtr& ptr) {
                    TExprBase node(ptr);
                    if (auto maybePrecompute = node.Maybe<TDqPhyPrecompute>()) {
                        result.push_back(maybePrecompute.Cast());
                        return false;
                    }
                    if (auto maybeConnection = node.Maybe<TDqConnection>()) {
                        YQL_ENSURE(false, "unexpected connection in source");
                    }
                    return true;
                  });
        }
    }
    return result;
}

class TKqpBuildTxsTransformer : public TSyncTransformerBase {
public:
    TKqpBuildTxsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
        const TIntrusivePtr<TKqpBuildQueryContext>& buildCtx, TAutoPtr<IGraphTransformer>&& typeAnnTransformer,
        TTypeAnnotationContext& typesCtx, TKikimrConfiguration::TPtr& config)
        : KqpCtx(kqpCtx)
        , BuildCtx(buildCtx)
        , TypeAnnTransformer(std::move(typeAnnTransformer))
    {
        BuildTxTransformer = new TKqpBuildTxTransformer();

        DataTxTransformer = TTransformationPipeline(&typesCtx)
            .AddServiceTransformers()
            .Add(TExprLogTransformer::Sync("TxOpt", NYql::NLog::EComponent::ProviderKqp, NYql::NLog::ELevel::TRACE), "TxOpt")
            .Add(*TypeAnnTransformer, "TypeAnnotation")
            .AddPostTypeAnnotation(/* forSubgraph */ true)
            .Add(CreateKqpBuildPhyStagesTransformer(/* allowDependantConsumers */ false, typesCtx), "BuildPhysicalStages")
            .Add(*BuildTxTransformer, "BuildPhysicalTx")
            .Add(CreateKqpTxPeepholeTransformer(TypeAnnTransformer.Get(), typesCtx, config, /* withFinalStageRules */ false), "Peephole")
            .Build(false);

        ScanTxTransformer = TTransformationPipeline(&typesCtx)
            .AddServiceTransformers()
            .Add(TExprLogTransformer::Sync("TxOpt", NYql::NLog::EComponent::ProviderKqp, NYql::NLog::ELevel::TRACE), "TxOpt")
            .Add(*TypeAnnTransformer, "TypeAnnotation")
            .AddPostTypeAnnotation(/* forSubgraph */ true)
            .Add(CreateKqpBuildPhyStagesTransformer(config->SpillingEnabled(), typesCtx), "BuildPhysicalStages")
            .Add(*BuildTxTransformer, "BuildPhysicalTx")
            .Add(CreateKqpTxPeepholeTransformer(TypeAnnTransformer.Get(), typesCtx, config, /* withFinalStageRules */ false), "Peephole")
            .Build(false);
    }

    TStatus DoTransform(TExprNode::TPtr inputExpr, TExprNode::TPtr& outputExpr, TExprContext& ctx) final {
        outputExpr = inputExpr;

        YQL_CLOG(DEBUG, ProviderKqp) << ">>> TKqpBuildTxsTransformer: " << KqpExprToPrettyString(*inputExpr, ctx);

        TKqlQuery query(inputExpr);

        if (auto status = TryBuildPrecomputeTx(query, outputExpr, ctx)) {
            return *status;
        }

        if (!query.Results().Empty()) {
            auto tx = BuildTx(query.Results().Ptr(), ctx, false);
            if (!tx) {
                return TStatus::Error;
            }

            BuildCtx->PhysicalTxs.emplace_back(tx.Cast());

            for (ui32 i = 0; i < query.Results().Size(); ++i) {
                const auto& result = query.Results().Item(i);
                auto binding = Build<TKqpTxResultBinding>(ctx, query.Pos())
                    .Type(ExpandType(query.Pos(), *result.Value().Ref().GetTypeAnn(), ctx))
                    .TxIndex()
                        .Build(ToString(BuildCtx->PhysicalTxs.size() - 1))
                    .ResultIndex()
                        .Build(ToString(i))
                    .Done();

                BuildCtx->QueryResults.emplace_back(std::move(binding));
            }
        }

        if (!query.Effects().Empty()) {
            auto tx = BuildTx(query.Effects().Ptr(), ctx, /* isPrecompute */ false);
            if (!tx) {
                return TStatus::Error;
            }

            if (!CheckEffectsTx(tx.Cast(), ctx)) {
                return TStatus::Error;
            }

            BuildCtx->PhysicalTxs.emplace_back(tx.Cast());
        }

        return TStatus::Ok;
    }

    void Rewind() final {
        DataTxTransformer->Rewind();
        ScanTxTransformer->Rewind();
    }

private:
    bool CheckEffectsTx(TKqpPhysicalTx tx, TExprContext& ctx) const {
        TMaybeNode<TExprBase> blackistedNode;
        VisitExpr(tx.Ptr(), [&blackistedNode](const TExprNode::TPtr& exprNode) {
            if (blackistedNode) {
                return false;
            }

            if (auto maybeCallable = TMaybeNode<TCallable>(exprNode)) {
                auto callable = maybeCallable.Cast();

                if (callable.Maybe<TCoUdf>() || callable.Maybe<TCoScriptUdf>() ||
                    callable.Maybe<TCoUnwrap>() ||
                    callable.Maybe<TCoEnsure>() || callable.Maybe<TKqpEnsure>())
                {
                    blackistedNode = callable;
                    return false;
                }
            }

            return true;
        });

        if (blackistedNode) {
            ctx.AddError(TIssue(ctx.GetPosition(blackistedNode.Cast().Pos()), TStringBuilder()
                << "Callable not expected in effects tx: " << blackistedNode.Cast<TCallable>().CallableName()));
            return false;
        }

        return true;
    }

    std::pair<TNodeOnNodeOwnedMap, TNodeOnNodeOwnedMap> GatherPrecomputeDependencies(const TKqlQuery& query) {
        TNodeOnNodeOwnedMap precomputes;
        TNodeOnNodeOwnedMap dependencies;

        auto filter = [](const TExprNode::TPtr& exprNode) {
            return !exprNode->IsLambda();
        };

        auto gather = [&precomputes, &dependencies](const TExprNode::TPtr& exprNode) {
            TExprBase node(exprNode);

            auto maybeStage = node.Maybe<TDqStage>();

            if (!maybeStage.IsValid()) {
                return true;
            }

            auto stage = maybeStage.Cast();
            auto precomputeInputs = PrecomputeInputs(stage);
            for (auto& precompute : precomputeInputs) {
                auto precomputeStage = precompute.Connection().Output().Stage();
                precomputes.emplace(precomputeStage.Raw(), precomputeStage.Ptr());
                dependencies.emplace(stage.Raw(), stage.Ptr());
            }

            for (const auto& input : stage.Inputs()) {
                if (input.Maybe<TDqPhyPrecompute>()) {
                    continue;
                } else if (auto maybeConnection = input.Maybe<TDqConnection>()) {
                    const TExprNode* inputStage = maybeConnection.Cast().Output().Stage().Raw();
                    if (dependencies.contains(inputStage)) {
                        dependencies.emplace(stage.Raw(), stage.Ptr());
                    }
                } else if (auto maybeSource = input.Maybe<TDqSource>()) {
                    // handled in PrecomputeInputs
                    continue;
                } else if (input.Maybe<TKqpTxResultBinding>()) {
                    // ok
                    continue;
                } else {
                    YQL_ENSURE(false, "Unexpected stage input: " << input.Ref().Content());
                }
            }

            return true;
        };

        VisitExpr(query.Ptr(), filter, gather);

        return std::make_pair(std::move(precomputes), std::move(dependencies));
    }

    TMaybe<TStatus> TryBuildPrecomputeTx(const TKqlQuery& query, TExprNode::TPtr& output, TExprContext& ctx) {
        auto [precomputeStagesMap, dependantStagesMap] = GatherPrecomputeDependencies(query);
        if (precomputeStagesMap.empty()) {
            return {};
        }

        TNodeOnNodeOwnedMap phaseStagesMap;
        TVector<TKqlQueryResult> phaseResults;
        TVector<TDqPhyPrecompute> computedInputs;
        TNodeSet computedInputsSet;

        // Gather all Precompute stages, that are independent of any other stage and form phase of execution
        for (auto [raw, ptr] : precomputeStagesMap) {
            if (dependantStagesMap.contains(raw)) {
                continue;
            }

            // precompute stage _NOT_IN_ dependant stages
            YQL_ENSURE(!IsKqpEffectsStage(TDqStage(ptr)));
            phaseStagesMap.emplace(raw, ptr);
        }

        if (phaseStagesMap.empty()) {
            output = query.Ptr();
            ctx.AddError(TIssue(ctx.GetPosition(query.Pos()), "Phase stages is empty"));
            return TStatus::Error;
        }

        for (auto& [_, stagePtr] : dependantStagesMap) {
            TDqStage stage(stagePtr);
            auto precomputes = PrecomputeInputs(stage);

            for (const auto& precompute : precomputes) {
                auto precomputeConnection = precompute.Connection();
                auto precomputeStage = precomputeConnection.Output().Stage();

                if (!phaseStagesMap.contains(precomputeStage.Raw())) {
                    continue;
                }

                if (computedInputsSet.contains(precompute.Raw())) {
                    continue;
                }

                auto result = Build<TKqlQueryResult>(ctx, precompute.Pos())
                    .Value(precomputeConnection)
                    .ColumnHints() // no column hints on intermediate phases
                        .Build()
                    .Done();

                phaseResults.emplace_back(result);
                computedInputs.emplace_back(precompute);
                computedInputsSet.insert(precompute.Raw());
            }
        }
        Y_VERIFY_DEBUG(phaseResults.size() == computedInputs.size());

        auto phaseResultsNode = Build<TKqlQueryResultList>(ctx, query.Pos())
            .Add(phaseResults)
            .Done();

        auto tx = BuildTx(phaseResultsNode.Ptr(), ctx, /* isPrecompute */ true);

        if (!tx.IsValid()) {
            return TStatus::Error;
        }

        BuildCtx->PhysicalTxs.emplace_back(tx.Cast());

        TNodeOnNodeOwnedMap replaceMap;
        for (ui64 i = 0; i < computedInputs.size(); ++i) {
            // N.B.: each precompute stage is stored in the `phaseResults` and `computedInputs` at the same index
            auto& input = computedInputs[i];
            auto newInput = Build<TKqpTxResultBinding>(ctx, input.Pos())
                .Type(ExpandType(input.Pos(), *input.Ref().GetTypeAnn(), ctx))
                .TxIndex().Build(ToString(BuildCtx->PhysicalTxs.size() - 1))
                .ResultIndex().Build(ToString(i))
                .Done();

            replaceMap.emplace(input.Raw(), newInput.Ptr());
        }

        output = ctx.ReplaceNodes(query.Ptr(), replaceMap);

        return TStatus(TStatus::Repeat, true);
    }

    TMaybeNode<TKqpPhysicalTx> BuildTx(const TExprNode::TPtr& result, TExprContext& ctx, bool isPrecompute) {
        YQL_CLOG(TRACE, ProviderKqp) << "[BuildTx] " << KqpExprToPrettyString(*result, ctx)
            << ", isPrecompute: " << isPrecompute;

        auto& transformer = KqpCtx->IsDataQuery() ? *DataTxTransformer : *ScanTxTransformer;
        transformer.Rewind();
        BuildTxTransformer->Init(KqpCtx->QueryCtx->Type, isPrecompute);
        auto expr = result;

        while (true) {
            auto status = InstantTransform(transformer, expr, ctx);
            if (status == TStatus::Error) {
                return {};
            }
            if (status == TStatus::Ok) {
                break;
            }
        }
        return TKqpPhysicalTx(expr);
    }

private:
    TIntrusivePtr<TKqpOptimizeContext> KqpCtx;
    TIntrusivePtr<TKqpBuildQueryContext> BuildCtx;
    TAutoPtr<IGraphTransformer> TypeAnnTransformer;
    TAutoPtr<TKqpBuildTxTransformer> BuildTxTransformer;
    TAutoPtr<IGraphTransformer> DataTxTransformer;
    TAutoPtr<IGraphTransformer> ScanTxTransformer;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKqpBuildTxsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    const TIntrusivePtr<TKqpBuildQueryContext>& buildCtx, TAutoPtr<IGraphTransformer>&& typeAnnTransformer,
    TTypeAnnotationContext& typesCtx, TKikimrConfiguration::TPtr& config)
{
    return new TKqpBuildTxsTransformer(kqpCtx, buildCtx, std::move(typeAnnTransformer), typesCtx, config);
}

} // namespace NKikimr::NKqp::NOpt
