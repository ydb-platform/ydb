#include "kqp_opt_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/core/services/yql_out_transformers.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
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
        , IsBlocks(false) {}

    void Init(EKikimrQueryType queryType, bool isPrecompute, bool isBlocks) {
        QueryType = queryType;
        IsPrecompute = isPrecompute;
        IsBlocks = isBlocks;
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

                if (IsBlocks) {
                    const auto* tupleOutputType = resultStage.Ref().GetTypeAnn()->Cast<TTupleExprType>();
                    YQL_ENSURE(tupleOutputType->GetSize() == 1);
                    const auto* structExprType = tupleOutputType->GetItems()[0]->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

                    TDqPhyStage beforeCollectStage = collectStage;
                    collectStage = ConvertResultStageFromBlocksToScalar(collectStage, structExprType, ctx);
                    if (collectStage.Ptr() != beforeCollectStage.Ptr()) {
                        for (auto& stage : stages) {
                            if (stage.Ptr() == beforeCollectStage.Ptr()) {
                                stage = collectStage;
                                break;
                            }
                        }
                    }
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

    static TDqPhyStage ConvertResultStageFromBlocksToScalar(TDqPhyStage& stage, const TStructExprType* rowType, TExprContext& ctx) {
        TCoLambda program(ctx.DeepCopyLambda(stage.Program().Ref()));

        TVector<TCoArgument> args;
        args.reserve(rowType->GetSize());
        for (ui32 i = 0; i < rowType->GetSize(); ++i) {
            args.push_back(TCoArgument(ctx.NewArgument(stage.Pos(), "arg")));
        }

        TVector<TExprBase> structItems;
        structItems.reserve(args.size());
        for (ui32 i = 0; i < args.size(); ++i) {
            structItems.emplace_back(
                Build<TCoNameValueTuple>(ctx, stage.Pos())
                    .Name().Build(rowType->GetItems()[i]->GetName())
                    .Value(args[i])
                    .Done());
        }

        auto resultStream = Build<TCoFromFlow>(ctx, program.Body().Pos())
            .Input<TCoNarrowMap>()
                .Input<TCoWideFromBlocks>()
                    .Input<TCoToFlow>()
                        .Input(program.Body())
                        .Build()
                    .Build()
                .Lambda()
                    .Args(args)
                    .Body<TCoAsStruct>()
                        .Add(structItems)
                        .Build()
                    .Build()
                .Build()
            .Done();


        auto finalChannelSettings = TDqStageSettings::Parse(stage);
        finalChannelSettings.WideChannels = false;
        finalChannelSettings.OutputNarrowType = nullptr;
        finalChannelSettings.BlockStatus = NYql::NDq::TDqStageSettings::EBlockStatus::None;

        auto output = Build<TDqPhyStage>(ctx, stage.Pos())
            .InitFrom(stage)
            .Program()
                .Args(program.Args())
                .Body(resultStream.Ptr())
            .Build()
            .Settings(finalChannelSettings.BuildNode(ctx, stage.Pos()))
            .Outputs(stage.Outputs())
            .Done().Ptr();

        return TDqPhyStage(output);
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
    bool IsBlocks;
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
        , TypesCtx(typesCtx)
        , TypeAnnTransformer(std::move(typeAnnTransformer))
    {
        BuildTxTransformer = new TKqpBuildTxTransformer();

        const bool enableSpillingGenericQuery =
            kqpCtx->IsGenericQuery() && config->SpillingEnabled() &&
            config->EnableSpillingGenericQuery;

        DataTxTransformer = TTransformationPipeline(&typesCtx)
            .AddServiceTransformers()
            .Add(TExprLogTransformer::Sync("TxOpt", NYql::NLog::EComponent::ProviderKqp, NYql::NLog::ELevel::TRACE), "TxOpt")
            .Add(*TypeAnnTransformer, "TypeAnnotation")
            .AddPostTypeAnnotation(/* forSubgraph */ true)
            .Add(CreateKqpBuildPhyStagesTransformer(enableSpillingGenericQuery, typesCtx, config->BlockChannelsMode), "BuildPhysicalStages")
            // TODO(ilezhankin): "BuildWideBlockChannels" transformer is required only for BLOCK_CHANNELS_FORCE mode.
            .Add(CreateKqpBuildWideBlockChannelsTransformer(typesCtx, config->BlockChannelsMode), "BuildWideBlockChannels")
            .Add(*BuildTxTransformer, "BuildPhysicalTx")
            .Add(CreateKqpTxPeepholeTransformer(
                TypeAnnTransformer.Get(), typesCtx, config,
                /* withFinalStageRules */ config->BlockChannelsMode == NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE,
                {"KqpPeephole-RewriteCrossJoin"}),
                "Peephole")
            .Build(false);

        ScanTxTransformer = TTransformationPipeline(&typesCtx)
            .AddServiceTransformers()
            .Add(TExprLogTransformer::Sync("TxOpt", NYql::NLog::EComponent::ProviderKqp, NYql::NLog::ELevel::TRACE), "TxOpt")
            .Add(*TypeAnnTransformer, "TypeAnnotation")
            .AddPostTypeAnnotation(/* forSubgraph */ true)
            .Add(CreateKqpBuildPhyStagesTransformer(config->SpillingEnabled(), typesCtx, config->BlockChannelsMode), "BuildPhysicalStages")
            .Add(*BuildTxTransformer, "BuildPhysicalTx")
            .Add(CreateKqpTxPeepholeTransformer(TypeAnnTransformer.Get(), typesCtx, config, /* withFinalStageRules */ false, {"KqpPeephole-RewriteCrossJoin"}), "Peephole")
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
            auto tx = BuildTx(query.Results().Ptr(), ctx, false, TypesCtx.BlockEngineMode == EBlockEngineMode::Force);
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
            auto tx = BuildTx(query.Effects().Ptr(), ctx, /* isPrecompute */ false, TypesCtx.BlockEngineMode == EBlockEngineMode::Force);
            if (!tx) {
                return TStatus::Error;
            }

            if (!CheckEffectsTx(tx.Cast(), query, ctx)) {
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
    bool HasTableEffects(const TKqlQuery& query) const {
        for (const TExprBase& effect : query.Effects()) {
            if (auto maybeSinkEffect = effect.Maybe<TKqpSinkEffect>()) {
                // (KqpSinkEffect (DqStage (... ((DqSink '0 (DataSink '"kikimr") ...)))) '0)
                auto sinkEffect = maybeSinkEffect.Cast();
                const size_t sinkIndex = FromString(TStringBuf(sinkEffect.SinkIndex()));
                auto stageExpr = sinkEffect.Stage();
                auto maybeStageBase = stageExpr.Maybe<TDqStageBase>();
                YQL_ENSURE(maybeStageBase);
                auto stage = maybeStageBase.Cast();
                YQL_ENSURE(stage.Outputs());
                auto outputs = stage.Outputs().Cast();
                YQL_ENSURE(sinkIndex < outputs.Size());
                auto maybeSink = outputs.Item(sinkIndex);
                YQL_ENSURE(maybeSink.Maybe<TDqSink>());
                auto sink = maybeSink.Cast<TDqSink>();
                auto dataSink = TCoDataSink(sink.DataSink().Ptr());
                if (dataSink.Category() == YdbProviderName || dataSink.Category() == KikimrProviderName) {
                    return true;
                }
            } else { // Not a SinkEffect, => a YDB table effect
                return true;
            }
        }
        return false;
    }

    bool CheckEffectsTx(TKqpPhysicalTx tx, const TKqlQuery& query, TExprContext& ctx) const {
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

        if (blackistedNode && HasTableEffects(query)) {
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

        auto phaseResultsNode = Build<TKqlQueryResultList>(ctx, query.Pos())
            .Add(phaseResults)
            .Done();

        auto tx = BuildTx(phaseResultsNode.Ptr(), ctx, /* isPrecompute */ true, TypesCtx.BlockEngineMode == EBlockEngineMode::Force);

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

    TMaybeNode<TKqpPhysicalTx> BuildTx(const TExprNode::TPtr& result, TExprContext& ctx, bool isPrecompute, bool isBlocks) {
        YQL_CLOG(TRACE, ProviderKqp) << "[BuildTx] " << KqpExprToPrettyString(*result, ctx)
            << ", isPrecompute: " << isPrecompute << ", isBlocks: " << isBlocks;

        auto& transformer = KqpCtx->IsScanQuery() ? *ScanTxTransformer : *DataTxTransformer;

        transformer.Rewind();
        BuildTxTransformer->Init(KqpCtx->QueryCtx->Type, isPrecompute, isBlocks);
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
    TTypeAnnotationContext& TypesCtx;
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
