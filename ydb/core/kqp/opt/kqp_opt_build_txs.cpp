#include "kqp_opt_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <yql/essentials/core/services/yql_out_transformers.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/protos/table_service_config.pb.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

namespace {

EChannelMode GetChannelMode(NKikimrConfig::TTableServiceConfig_EBlockChannelsMode blockChannelsMode) {
    switch (blockChannelsMode) {
        case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_SCALAR:
            return EChannelMode::CHANNEL_SCALAR;
        case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO:
            return EChannelMode::CHANNEL_WIDE_AUTO_BLOCK;
        case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE:
            return EChannelMode::CHANNEL_WIDE_FORCE_BLOCK;
        default:
            YQL_ENSURE(false);
    }
}

TAutoPtr<NYql::IGraphTransformer> CreateKqpBuildWideBlockChannelsTransformer(
        TTypeAnnotationContext& typesCtx,
        NKikimrConfig::TTableServiceConfig_EBlockChannelsMode blockChannelsMode) {
    const EChannelMode mode = GetChannelMode(blockChannelsMode);
    return NDq::CreateDqBuildWideBlockChannelsTransformer(typesCtx, mode);
}

TAutoPtr<NYql::IGraphTransformer> CreateKqpBuildPhyStagesTransformer(
        bool allowDependantConsumers,
        TTypeAnnotationContext& typesCtx,
        NKikimrConfig::TTableServiceConfig_EBlockChannelsMode blockChannelsMode) {
    const EChannelMode mode = GetChannelMode(blockChannelsMode);
    return NDq::CreateDqBuildPhyStagesTransformer(allowDependantConsumers, typesCtx, mode);
}

class TKqpBuildTxTransformer : public TSyncTransformerBase {
public:
    TKqpBuildTxTransformer()
        : QueryType(EKikimrQueryType::Unspecified)
        , IsPrecompute(false)
        , MergeOutputAndEffect(false)
    {
    }

    void Init(EKikimrQueryType queryType, bool isPrecompute, bool mergeOutputAndEffect) {
        QueryType = queryType;
        IsPrecompute = isPrecompute;
        MergeOutputAndEffect = mergeOutputAndEffect;
    }

    TStatus DoTransform(TExprNode::TPtr inputExpr, TExprNode::TPtr& outputExpr, TExprContext& ctx) final {
        if (TKqpPhysicalTx::Match(inputExpr.Get())) {
            outputExpr = inputExpr;
            return TStatus::Ok;
        }

        YQL_ENSURE(inputExpr->IsList() && inputExpr->ChildrenSize() > 0);

        if (MergeOutputAndEffect) {
            return DoBuildTxImpl(inputExpr, outputExpr, ctx);
        }

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

    TStatus DoBuildTxImpl(TExprNode::TPtr inputExpr, TExprNode::TPtr& outputExpr, TExprContext& ctx) {
        auto stages = CollectStages(inputExpr, ctx);
        Y_DEBUG_ABORT_UNLESS(!stages.empty());

        TKqlQuery query(inputExpr);
        TMaybeNode<TExprList> txResults;
        if (!query.Results().Empty()) {
            txResults = BuildTxResults(query.Results().Ptr(), stages, ctx);
            if (!txResults) {
                return TStatus::Error;
            }
        } else {
            txResults = Build<TExprList>(ctx, inputExpr->Pos()).Done();
        }

        TKqpPhyTxSettings txSettings;
        txSettings.Type = GetPhyTxType(query.Effects().Empty() && AreAllStagesKqpPure(stages));
        txSettings.WithEffects = !query.Effects().Empty();

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

    TStatus DoBuildTxResults(TExprNode::TPtr inputExpr, TExprNode::TPtr& outputExpr, TExprContext& ctx) {
        auto stages = CollectStages(inputExpr, ctx);
        Y_DEBUG_ABORT_UNLESS(!stages.empty());

        auto txResults = BuildTxResults(inputExpr, stages, ctx);
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
        Y_DEBUG_ABORT_UNLESS(!stages.empty());

        TKqpPhyTxSettings txSettings;
        YQL_ENSURE(QueryType != EKikimrQueryType::Scan);
        txSettings.Type = GetPhyTxType(false);
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

    TMaybeNode<TExprList> BuildTxResults(TExprNode::TPtr inputExpr, TVector<TDqPhyStage>& stages,
        TExprContext& ctx)
    {
        auto results = TKqlQueryResultList(inputExpr);
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
                if (settings.PartitionMode == TDqStageSettings::EPartitionMode::Single) {
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

        // Helper to collect TKqpTxResultBinding nodes and replace them with parameters
        TNodeOnNodeOwnedMap sourceReplaceMap;
        auto collectBindings = [&](const TExprNode::TPtr& root) {
            VisitExpr(root,
                [&](const TExprNode::TPtr& node) {
                    TExprBase expr(node);
                    if (auto binding = expr.Maybe<TKqpTxResultBinding>()) {
                        sourceReplaceMap.emplace(node.Get(), makeParameterBinding(binding.Cast(), node->Pos()).Ptr());
                    }
                    return true;
                });
        };

        for (ui32 i = 0; i < stage.Inputs().Size(); ++i) {
            const auto& input = stage.Inputs().Item(i);
            const auto& inputArg = stage.Program().Args().Arg(i);

            // Scan inputs that may contain TKqpTxResultBinding
            if (input.Maybe<TDqSource>() || input.Maybe<TKqpCnStreamLookup>()) {
                collectBindings(input.Ptr());
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

        // Scan program body for TKqpTxResultBinding (e.g. in TKqpReadTableRanges VectorTopK settings)
        collectBindings(stage.Program().Body().Ptr());

        auto inputs = Build<TExprList>(ctx, stage.Pos())
            .Add(newInputs)
            .Done();

        return Build<TDqPhyStage>(ctx, stage.Pos())
            .Inputs(ctx.ReplaceNodes(ctx.ReplaceNodes(inputs.Ptr(), stagesMap), sourceReplaceMap))
            .Program()
                .Args(newArgs)
                .Body(ctx.ReplaceNodes(ctx.ReplaceNodes(stage.Program().Body().Ptr(), argsMap), sourceReplaceMap))
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
    bool MergeOutputAndEffect;
};

TVector<TDqPhyPrecompute> PrecomputeInputs(const TDqStage& stage) {
    TVector<TDqPhyPrecompute> result;

    // Helper to collect precomputes from an expression tree
    auto collectPrecomputes = [&result](const TExprNode::TPtr& root, bool checkConnections = false) {
        VisitExpr(root,
            [&](const TExprNode::TPtr& ptr) {
                TExprBase node(ptr);
                if (auto maybePrecompute = node.Maybe<TDqPhyPrecompute>()) {
                    result.push_back(maybePrecompute.Cast());
                    return false;
                }
                if (checkConnections) {
                    if (auto maybeConnection = node.Maybe<TDqConnection>()) {
                        YQL_ENSURE(false, "unexpected connection in source");
                    }
                }
                return true;
            });
    };

    // Scan stage inputs for precomputes
    for (const auto& input : stage.Inputs()) {
        if (auto maybePrecompute = input.Maybe<TDqPhyPrecompute>()) {
            result.push_back(maybePrecompute.Cast());
        } else if (auto maybeSource = input.Maybe<TDqSource>()) {
            collectPrecomputes(maybeSource.Cast().Ptr(), /* checkConnections */ true);
        } else if (auto maybeStreamLookup = input.Maybe<TKqpCnStreamLookup>()) {
            collectPrecomputes(maybeStreamLookup.Cast().Settings().Ptr());
        }
    }

    // Scan program body for precomputes (e.g. in TKqpReadTableRanges VectorTopK settings)
    collectPrecomputes(stage.Program().Body().Ptr());

    return result;
}

class TKqpBuildTxsTransformer : public TSyncTransformerBase {
    class TEffectsInfo {
    public:
        enum class EType {
            KQP_EFFECT,
            KQP_SINK,
            KQP_BATCH_SINK,
        };

        EType Type;
        THashSet<TStringBuf> TablesPathIds;
        THashMap<std::pair<const TExprNode*, ui64>, TKqpSinkEffect> SinkEffects; // (stage, sink index) -> effect

        const TVector<TExprNode::TPtr>& GetExprs() const {
            return Exprs;
        }

        TMaybeNode<TKqpSinkEffect> GetSinkEffect(const TDqStageBase& stage, ui64 index) const {
            const auto it = SinkEffects.find(std::pair(stage.Raw(), index));
            if (it == SinkEffects.end()) {
                return {};
            }
            return it->second;
        }

        void AddExpr(const TExprNode::TPtr& expr) {
            if (const auto maybeSinkEffect = TMaybeNode<TKqpSinkEffect>(expr)) {
                const auto sinkEffect = maybeSinkEffect.Cast();
                YQL_ENSURE(SinkEffects.emplace(std::pair(sinkEffect.Stage().Raw(), FromString<ui64>(sinkEffect.SinkIndex())), sinkEffect).second, "Found two effects with same sink");
            }
            Exprs.push_back(expr);
        }

    private:
        TVector<TExprNode::TPtr> Exprs;
    };

public:
    TKqpBuildTxsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
        const TIntrusivePtr<TKqpBuildQueryContext>& buildCtx, TAutoPtr<IGraphTransformer>&& typeAnnTransformer,
        TTypeAnnotationContext& typesCtx, TKikimrConfiguration::TPtr& config)
        : KqpCtx(kqpCtx)
        , BuildCtx(buildCtx)
        , TypeAnnTransformer(std::move(typeAnnTransformer))
    {
        BuildTxTransformer = new TKqpBuildTxTransformer();

        bool enableSpilling = config->GetEnableQueryServiceSpilling() && (kqpCtx->IsGenericQuery() || kqpCtx->IsScanQuery()) && config->SpillingEnabled();

        DataTxTransformer = TTransformationPipeline(&typesCtx)
            .AddServiceTransformers()
            .Add(TExprLogTransformer::Sync("TxOpt", NYql::NLog::EComponent::ProviderKqp, NYql::NLog::ELevel::TRACE), "TxOpt")
            .Add(*TypeAnnTransformer, "TypeAnnotation")
            .AddPostTypeAnnotation(/* forSubgraph */ true)
            .Add(CreateKqpBuildPhyStagesTransformer(enableSpilling, typesCtx, config->GetBlockChannelsMode()), "BuildPhysicalStages")
            .Add(CreateKqpPhyOptTransformer(kqpCtx, typesCtx, config,
                CreateTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(kqpCtx->Cluster, kqpCtx->Tables, typesCtx, config), typesCtx)), "KqpPhysicalOptimize")
            // TODO(ilezhankin): "BuildWideBlockChannels" transformer is required only for BLOCK_CHANNELS_FORCE mode.
            .Add(CreateKqpBuildWideBlockChannelsTransformer(typesCtx, config->GetBlockChannelsMode()), "BuildWideBlockChannels")
            .Add(*BuildTxTransformer, "BuildPhysicalTx")
            .Add(CreateKqpTxPeepholeTransformer(
                TypeAnnTransformer.Get(), typesCtx, config,
                /* withFinalStageRules */ config->GetBlockChannelsMode() == NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE,
                {"KqpPeephole-RewriteCrossJoin"}),
                "Peephole")
            .Build(false);
    }

    TStatus DoTransform(TExprNode::TPtr inputExpr, TExprNode::TPtr& outputExpr, TExprContext& ctx) final {
        outputExpr = inputExpr;

        YQL_CLOG(DEBUG, ProviderKqp) << ">>> TKqpBuildTxsTransformer: " << KqpExprToPrettyString(*inputExpr, ctx);

        TKqlQuery query(inputExpr);

        if (auto status = TryBuildPrecomputeTx(query, outputExpr, ctx)) {
            return *status;
        }

        auto updateResultBinding = [this, &ctx](const TKqlQuery& query) {
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
        };

        if (KqpCtx->Config->GetEnableIndexStreamWrite()) {
            auto collectedEffects = CollectEffects(query.Effects(), ctx, *KqpCtx);

            if (!query.Results().Empty()) {
                auto resultsQuery = Build<TKqlQuery>(ctx, query.Pos())
                    .Results(query.Results())
                    .Effects()
                        .Build()
                    .Done();

                auto tx = BuildTx(resultsQuery.Ptr(), ctx, false);
                if (!tx) {
                    return TStatus::Error;
                }

                BuildCtx->PhysicalTxs.emplace_back(tx.Cast());

                updateResultBinding(resultsQuery);
            }

            for (auto& effects : *collectedEffects) {
                auto effectsQuery = Build<TKqlQuery>(ctx, query.Pos())
                    .Results()
                        .Build()
                    .Effects(effects)
                    .Done();

                auto tx = BuildTx(effectsQuery.Ptr(), ctx, /* isPrecompute */ false);
                if (!tx) {
                    return TStatus::Error;
                }

                BuildCtx->PhysicalTxs.emplace_back(tx.Cast());

                updateResultBinding(effectsQuery);
            }
            
            return TStatus::Ok;
        }

        if (!query.Results().Empty()) {
            auto tx = BuildTx(query.Results().Ptr(), ctx, false);
            if (!tx) {
                return TStatus::Error;
            }

            BuildCtx->PhysicalTxs.emplace_back(tx.Cast());

            updateResultBinding(query);
        }

        if (!query.Effects().Empty()) {
            auto collectedEffects = CollectEffects(query.Effects(), ctx, *KqpCtx);
            if (!collectedEffects) {
                return TStatus::Error;
            }

            for (auto& effects : *collectedEffects) {
                auto tx = BuildTx(effects.Ptr(), ctx, /* isPrecompute */ false);
                if (!tx) {
                    return TStatus::Error;
                }

                if (!CheckEffectsTx(tx.Cast(), effects, ctx)) {
                    return TStatus::Error;
                }

                BuildCtx->PhysicalTxs.emplace_back(tx.Cast());
            }
        }

        return TStatus::Ok;
    }

    void Rewind() final {
        DataTxTransformer->Rewind();
    }

private:
    static TMaybeNode<TDqSink> GetStageSink(const TKqpSinkEffect& effect) {
        // (KqpSinkEffect (DqStage (... ((DqSink '0 (DataSink '"kikimr") ...)))) '0)
        const auto maybeStage = effect.Stage().Maybe<TDqStageBase>();
        YQL_ENSURE(maybeStage);
        const auto stage = maybeStage.Cast();
        YQL_ENSURE(stage.Outputs());
        const auto outputs = stage.Outputs().Cast();
        const size_t sinkIndex = FromString(effect.SinkIndex().Value());
        YQL_ENSURE(sinkIndex < outputs.Size());
        const auto maybeSink = outputs.Item(sinkIndex).Maybe<TDqSink>();
        return maybeSink;
    }

    std::optional<TVector<TExprList>> CollectEffects(const TExprList& list, TExprContext& ctx, TKqpOptimizeContext& kqpCtx) {
        TVector<TEffectsInfo> effectsInfos;
        TVector<TKqpSinkEffect> externalEffects;

        for (const auto& expr : list) {
            if (auto sinkEffect = expr.Maybe<TKqpSinkEffect>()) {
                const auto dqSink = GetStageSink(sinkEffect.Cast());
                if (!dqSink) {
                    // TODO: process output transfroms
                    continue;
                }
                
                const auto sinkSettings = dqSink.Cast().Settings().Maybe<TKqpTableSinkSettings>();
                if (!sinkSettings) {
                    /// ???
                    externalEffects.emplace_back(sinkEffect.Cast());
                } else {
                    // Two table sinks can't be executed in one physical transaction if they write into same table and have same priority.

                    const bool needSingleEffect = sinkSettings.Cast().Mode() == "fill_table"
                        || sinkSettings.Cast().InconsistentWrite().Value() == "true"sv
                        || (kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, sinkSettings.Cast().Table().Path()).Metadata->Kind == EKikimrTableKind::Olap);

                    if (needSingleEffect) {
                        const TStringBuf tablePathId = sinkSettings.Cast().Table().PathId().Value();

                        auto it = std::find_if(
                            std::begin(effectsInfos),
                            std::end(effectsInfos),
                            [&tablePathId](const auto& effectsInfo) {
                                return effectsInfo.Type == TEffectsInfo::EType::KQP_SINK
                                    && !effectsInfo.TablesPathIds.contains(tablePathId);
                            });
                        if (it == std::end(effectsInfos)) {
                            effectsInfos.emplace_back();
                            it = std::prev(std::end(effectsInfos));
                            it->Type = TEffectsInfo::EType::KQP_SINK;
                        }
                        it->TablesPathIds.insert(tablePathId);
                        it->AddExpr(expr.Ptr());
                    } else {
                        auto it = std::find_if(
                            std::begin(effectsInfos),
                            std::end(effectsInfos),
                            [](const auto& effectsInfo) { return effectsInfo.Type == TEffectsInfo::EType::KQP_BATCH_SINK; });
                        if (it == std::end(effectsInfos)) {
                            effectsInfos.emplace_back();
                            it = std::prev(std::end(effectsInfos));
                            it->Type = TEffectsInfo::EType::KQP_BATCH_SINK;
                        }
                        it->AddExpr(expr.Ptr());
                    }
                }
            } else {
                // Table effects are executed all in one physical transaction.
                auto it = std::find_if(
                    std::begin(effectsInfos),
                    std::end(effectsInfos),
                    [](const auto& effectsInfo) { return effectsInfo.Type == TEffectsInfo::EType::KQP_EFFECT; });
                if (it == std::end(effectsInfos)) {
                    effectsInfos.emplace_back();
                    it = std::prev(std::end(effectsInfos));
                    it->Type = TEffectsInfo::EType::KQP_EFFECT;
                }
                it->AddExpr(expr.Ptr());
            }
        }

        for (const auto& sinkEffect : externalEffects) {
            // Two external writes can not be in single physical transaction if they use same sink of same stage.

            auto it = std::find_if(
                effectsInfos.begin(),
                effectsInfos.end(),
                [id = std::pair(sinkEffect.Stage().Raw(), FromString<ui64>(sinkEffect.SinkIndex()))](const TEffectsInfo& effectsInfo) {
                    return !effectsInfo.SinkEffects.contains(id);
                });

            if (it == effectsInfos.end()) {
                effectsInfos.emplace_back();
                it = std::prev(effectsInfos.end());
            }

            it->AddExpr(sinkEffect.Ptr());
        }

        TVector<TExprList> results;

        for (const auto& effects : effectsInfos) {
            auto builder = Build<TExprList>(ctx, list.Pos());
            for (const auto& expr : effects.GetExprs()) {
                builder.Add(expr);
            }
            auto effect = builder.Done();

            // Check that effect does not contain DQ sinks from other effects.
            TVector<TDqStage> stagesToReplace;
            VisitExpr(effect.Ptr(), [](const TExprNode::TPtr& node) {
                return !node->IsLambda();
            }, [&](const TExprNode::TPtr& node) {
                if (const auto maybeStage = TMaybeNode<TDqStage>(node)) {
                    if (const auto stage = maybeStage.Cast(); const auto maybeOutputs = stage.Outputs()) {
                        for (size_t i = 0; i < maybeOutputs.Cast().Size(); ++i) {
                            if (!effects.GetSinkEffect(stage, i)) {
                                stagesToReplace.emplace_back(stage);
                                break;
                            }
                        }
                    }
                }
                return true;
            });

            // Remove sinks corresponding to other effects and adjust sink indices.
            TNodeOnNodeOwnedMap replaces(stagesToReplace.size());
            for (const auto& stage : stagesToReplace) {
                size_t sinkIndex = 0;
                auto outputsBuilder = Build<TDqStageOutputsList>(ctx, stage.Pos());
                const auto outputs = stage.Outputs().Cast();
                for (size_t i = 0; i < outputs.Size(); ++i) {
                    const auto maybeEffectNode = effects.GetSinkEffect(stage, i);
                    if (!maybeEffectNode) {
                        continue;
                    }

                    outputsBuilder.Add(outputs.Item(i));

                    const auto effectNode = maybeEffectNode.Cast();
                    const auto effectReplace = Build<TKqpSinkEffect>(ctx, effectNode.Pos())
                        .InitFrom(effectNode)
                        .SinkIndex().Build(sinkIndex++)
                        .Done();
                    YQL_ENSURE(replaces.emplace(effectNode.Raw(), effectReplace.Ptr()).second);
                }

                auto stageBuilder = Build<TDqStage>(ctx, stage.Pos()).InitFrom(stage);
                if (sinkIndex) {
                    stageBuilder.Outputs(outputsBuilder.Done());
                } else {
                    stageBuilder.Outputs(TMaybeNode<TDqStageOutputsList>{});
                }
                YQL_ENSURE(replaces.emplace(stage.Raw(), stageBuilder.Done().Ptr()).second);
            }

            if (replaces.empty()) {
                results.emplace_back(effect);
            } else {
                TExprNode::TPtr output;
                TOptimizeExprSettings settings(nullptr);
                settings.VisitLambdas = false;
                if (RemapExpr(effect.Ptr(), output, replaces, ctx, settings).Level == TStatus::Error) {
                    return std::nullopt;
                }
                results.emplace_back(output);
            }
        }

        return std::move(results);
    }

    bool HasTableEffects(const TExprList& effectsList) const {
        for (const TExprBase& effect : effectsList) {
            if (auto maybeSinkEffect = effect.Maybe<TKqpSinkEffect>()) {
                const auto dqSink = GetStageSink(maybeSinkEffect.Cast());
                if (!dqSink) {
                    // TODO: process output transforms
                    continue;
                }
                auto dataSink = TCoDataSink(dqSink.Cast().DataSink().Ptr());
                if (dataSink.Category() == YdbProviderName || dataSink.Category() == KikimrProviderName) {
                    return true;
                }
            } else { // Not a SinkEffect, => a YDB table effect
                return true;
            }
        }
        return false;
    }

    bool CheckEffectsTx(TKqpPhysicalTx tx, const TExprList& effectsList, TExprContext& ctx) const {
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

        if (blackistedNode && HasTableEffects(effectsList)) {
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
        Y_DEBUG_ABORT_UNLESS(phaseResults.size() == computedInputs.size());

        auto phaseQueryNode = KqpCtx->Config->GetEnableIndexStreamWrite()
            ? Build<TKqlQuery>(ctx, query.Pos())
                .Results()
                    .Add(phaseResults)
                    .Build()
                .Effects()
                    .Build()
                .Done().Ptr()
            : Build<TKqlQueryResultList>(ctx, query.Pos())
                .Add(phaseResults)
                .Done().Ptr();

        auto tx = BuildTx(phaseQueryNode, ctx, /* isPrecompute */ true);

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

        auto& transformer = *DataTxTransformer;

        transformer.Rewind();
        BuildTxTransformer->Init(
            KqpCtx->QueryCtx->Type,
            isPrecompute,
            KqpCtx->Config->GetEnableIndexStreamWrite());
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
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKqpBuildTxsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    const TIntrusivePtr<TKqpBuildQueryContext>& buildCtx, TAutoPtr<IGraphTransformer>&& typeAnnTransformer,
    TTypeAnnotationContext& typesCtx, TKikimrConfiguration::TPtr& config)
{
    return new TKqpBuildTxsTransformer(kqpCtx, buildCtx, std::move(typeAnnTransformer), typesCtx, config);
}

} // namespace NKikimr::NKqp::NOpt
