#include "kqp_opt_peephole.h"
#include "kqp_opt_peephole_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/host/kqp_transform.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/library/naming_conventions/naming_conventions.h>

#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>

#include <util/generic/size_literals.h>
#include <util/string/cast.h>

namespace NKikimr::NKqp::NOpt {

namespace {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

TStatus ReplaceNonDetFunctionsWithParams(TExprNode::TPtr& input, TExprContext& ctx,
    THashMap<TString, TKqpParamBinding>* paramBindings)
{
    static const std::unordered_set<std::string_view> nonDeterministicFunctions = {
        "RandomNumber",
        "Random",
        "RandomUuid",
        "Now",
        "CurrentUtcDate",
        "CurrentUtcDatetime",
        "CurrentUtcTimestamp"
    };

    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = true;

    TExprNode::TPtr output;
    auto status = OptimizeExpr(input, output, [paramBindings](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (auto maybeCallable = TMaybeNode<TCallable>(node)) {
            auto callable = maybeCallable.Cast();
            if (nonDeterministicFunctions.contains(callable.CallableName()) && callable.Ref().ChildrenSize() == 0) {
                const auto paramName = TStringBuilder() << ParamNamePrefix
                    << NNaming::CamelToSnakeCase(TString(callable.CallableName()));

                auto param = Build<TCoParameter>(ctx, node->Pos())
                    .Name().Build(paramName)
                    .Type(ExpandType(node->Pos(), *node->GetTypeAnn(), ctx))
                    .Done();

                if (paramBindings && !paramBindings->contains(paramName)) {
                    auto binding = Build<TKqpTxInternalBinding>(ctx, node->Pos())
                        .Type(ExpandType(node->Pos(), *node->GetTypeAnn(), ctx))
                        .Kind().Build(callable.CallableName())
                        .Done();

                    auto paramBinding = Build<TKqpParamBinding>(ctx, param.Pos())
                        .Name().Build(paramName)
                        .Binding(binding)
                        .Done();

                    paramBindings->insert({paramName, std::move(paramBinding)});
                }
                return param.Ptr();
            }
        }

        return node;
    }, ctx, settings);

    if (output) {
        input = output;
    }

    return status;
}

class TKqpPeepholeTransformer : public TOptimizeTransformerBase {
public:
    TKqpPeepholeTransformer(TTypeAnnotationContext& typesCtx, TSet<TString> disabledOpts)
        : TOptimizeTransformerBase(&typesCtx, NYql::NLog::EComponent::ProviderKqp, disabledOpts)
    {
#define HNDL(name) "KqpPeephole-"#name, Hndl(&TKqpPeepholeTransformer::name)
        AddHandler(0, &TDqReplicate::Match, HNDL(RewriteReplicate));
        AddHandler(0, &TDqPhyMapJoin::Match, HNDL(RewriteMapJoin));
        AddHandler(0, &TDqPhyCrossJoin::Match, HNDL(RewriteCrossJoin));
        AddHandler(0, &TDqPhyJoinDict::Match, HNDL(RewriteDictJoin));
        AddHandler(0, &TDqJoin::Match, HNDL(RewritePureJoin));
        AddHandler(0, TOptimizeTransformerBase::Any(), HNDL(BuildWideReadTable));
        AddHandler(0, &TDqPhyLength::Match, HNDL(RewriteLength));
        AddHandler(0, &TKqpWriteConstraint::Match, HNDL(RewriteKqpWriteConstraint));
#undef HNDL
    }

protected:
    TMaybeNode<TExprBase> RewriteReplicate(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqPeepholeRewriteReplicate(node, ctx);
        DumpAppliedRule("RewriteReplicate", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteMapJoin(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqPeepholeRewriteMapJoin(node, ctx);
        DumpAppliedRule("RewriteMapJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteCrossJoin(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqPeepholeRewriteCrossJoin(node, ctx);
        DumpAppliedRule("RewriteCrossJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteDictJoin(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqPeepholeRewriteJoinDict(node, ctx);
        DumpAppliedRule("RewriteDictJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewritePureJoin(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqPeepholeRewritePureJoin(node, ctx);
        DumpAppliedRule("RewritePureJoin", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> BuildWideReadTable(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpBuildWideReadTable(node, ctx, *Types);
        DumpAppliedRule("BuildWideReadTable", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteLength(TExprBase node, TExprContext& ctx) {
        TExprBase output = DqPeepholeRewriteLength(node, ctx, *Types);
        DumpAppliedRule("RewriteLength", node.Ptr(), output.Ptr(), ctx);
        return output;
    }

    TMaybeNode<TExprBase> RewriteKqpWriteConstraint(TExprBase node, TExprContext& ctx) {
        TExprBase output = KqpRewriteWriteConstraint(node, ctx);
        DumpAppliedRule("RewriteKqpWriteConstraint", node.Ptr(), output.Ptr(), ctx);
        return output;
    }
};

struct TKqpPeepholePipelineConfigurator : IPipelineConfigurator {
    TKqpPeepholePipelineConfigurator(
        TKikimrConfiguration::TPtr config,
        TSet<TString> disabledOpts
    )
        : Config(config)
        , DisabledOpts(disabledOpts)
    {}

    void AfterCreate(TTransformationPipeline*) const override {
    }

    void AfterTypeAnnotation(TTransformationPipeline*) const override {
    }

    void AfterOptimize(TTransformationPipeline* pipeline) const override {
        pipeline->Add(new TKqpPeepholeTransformer(*pipeline->GetTypeAnnotationContext(), DisabledOpts), "KqpPeephole");
    }

private:
    TKikimrConfiguration::TPtr Config;
    TSet<TString> DisabledOpts;
};

class TKqpPeepholeFinalTransformer : public TOptimizeTransformerBase {
public:
    TKqpPeepholeFinalTransformer(TTypeAnnotationContext& ctx, TKikimrConfiguration::TPtr config)
        : TOptimizeTransformerBase(&ctx, NYql::NLog::EComponent::ProviderKqp, {}), Config(config)
    {
#define HNDL(name) "KqpPeepholeFinal-"#name, Hndl(&TKqpPeepholeFinalTransformer::name)
        AddHandler(0, &TCoWideCombiner::Match, HNDL(SetCombinerMemoryLimit));
#undef HNDL
    }
private:
    TMaybeNode<TExprBase> SetCombinerMemoryLimit(TExprBase node, TExprContext& ctx) {
        if (const auto limit = node.Ref().Child(TCoWideCombiner::idx_MemLimit); limit->IsAtom("0")) {
            if (const auto limitSetting = Config->_KqpYqlCombinerMemoryLimit.Get(); limitSetting && *limitSetting) {
                return ctx.ChangeChild(node.Ref(), TCoWideCombiner::idx_MemLimit, ctx.RenameNode(*limit, ToString(-i64(*limitSetting))));
            }
        }
        return node;
    }

    const TKikimrConfiguration::TPtr Config;
};

struct TKqpPeepholePipelineFinalConfigurator : IPipelineConfigurator {
    TKqpPeepholePipelineFinalConfigurator(TKikimrConfiguration::TPtr config)
        : Config(config)
    {}

    void AfterCreate(TTransformationPipeline*) const override {}

    void AfterTypeAnnotation(TTransformationPipeline* pipeline) const override {
        pipeline->Add(new TKqpPeepholeFinalTransformer(*pipeline->GetTypeAnnotationContext(), Config), "KqpPeepholeFinal");
    }

    void AfterOptimize(TTransformationPipeline*) const override {}
private:
    const TKikimrConfiguration::TPtr Config;
};

// Sort stages in topological order by their inputs, so that we optimize the ones without inputs first.
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

// TODO: copy-paste from https://github.com/ydb-platform/ydb/blob/122f053354c5df4fc559bf06fe0302f92d813032/ydb/library/yql/dq/opt/dq_opt_build.cpp#L444
bool IsCompatibleWithBlocks(TPositionHandle pos, const TStructExprType& type, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    TVector<const TTypeAnnotationNode*> types;
    for (auto& item : type.GetItems()) {
        types.emplace_back(item->GetItemType());
    }

    auto resolveStatus = typesCtx.ArrowResolver->AreTypesSupported(ctx.GetPosition(pos), types, ctx);
    YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
    return resolveStatus == IArrowResolver::OK;
}

// TODO: composite copy-paste from https://github.com/ydb-platform/ydb/blob/122f053354c5df4fc559bf06fe0302f92d813032/ydb/library/yql/dq/opt/dq_opt_build.cpp#L388
bool CanPropagateWideBlockThroughChannel(
    const TDqOutput& output,
    const THashMap<ui64, TKqpProgram>& programs,
    const TDqStageSettings& stageSettings,
    TExprContext& ctx,
    TTypeAnnotationContext& typesCtx)
{
    const auto& program = programs.at(output.Stage().Ref().UniqueId());

    ui32 index = FromString<ui32>(output.Index().Value());
    if (index != 0) {
        // stage has multiple outputs
        return false;
    }

    auto outputItemType = program.Lambda().Ref().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
    if (IsWideBlockType(*outputItemType)) {
        // output is already wide block
        return false;
    }

    if (!stageSettings.WideChannels) {
        return false;
    }

    YQL_ENSURE(stageSettings.OutputNarrowType);

    if (!IsCompatibleWithBlocks(program.Pos(), *stageSettings.OutputNarrowType, ctx, typesCtx)) {
        return false;
    }

    // Ensure that stage has blocks on top level (i.e. FromFlow(WideFromBlocks(...)))
    if (!program.Lambda().Body().Maybe<TCoFromFlow>() ||
        !program.Lambda().Body().Cast<TCoFromFlow>().Input().Maybe<TCoWideFromBlocks>())
    {
        return false;
    }

    return true;
}

TStatus PeepHoleOptimize(const TExprBase& program, TExprNode::TPtr& newProgram, TExprContext& ctx,
    IGraphTransformer& typeAnnTransformer, TTypeAnnotationContext& typesCtx, TKikimrConfiguration::TPtr config,
    bool allowNonDeterministicFunctions, bool withFinalStageRules, TSet<TString> disabledOpts)
{
    TKqpPeepholePipelineConfigurator kqpPeephole(config, disabledOpts);
    TKqpPeepholePipelineFinalConfigurator kqpPeepholeFinal(config);
    TPeepholeSettings peepholeSettings;
    peepholeSettings.CommonConfig = &kqpPeephole;
    peepholeSettings.FinalConfig = &kqpPeepholeFinal;
    peepholeSettings.WithFinalStageRules = withFinalStageRules;
    peepholeSettings.WithNonDeterministicRules = false;

    bool hasNonDeterministicFunctions;
    auto status = PeepHoleOptimizeNode(program.Ptr(), newProgram, ctx, typesCtx, &typeAnnTransformer,
        hasNonDeterministicFunctions, peepholeSettings);
    if (status == TStatus::Error) {
        return status;
    }

    if (!allowNonDeterministicFunctions && hasNonDeterministicFunctions) {
        ctx.AddError(TIssue(ctx.GetPosition(program.Pos()), "Unexpected non-deterministic functions in KQP program"));
        return TStatus::Error;
    }

    return status;
}

TMaybeNode<TKqpPhysicalTx> PeepholeOptimize(const TKqpPhysicalTx& tx, TExprContext& ctx,
    IGraphTransformer& typeAnnTransformer, TTypeAnnotationContext& typesCtx, THashSet<ui64>& optimizedStages,
    TKikimrConfiguration::TPtr config, bool withFinalStageRules, TSet<TString> disabledOpts)
{
    THashMap<ui64 /*stage uid*/, TKqpProgram> programs;
    THashMap<TString, TKqpParamBinding> nonDetParamBindings;

    const auto topSortedStages = TopSortStages(tx.Stages());
    for (const auto& stage : topSortedStages) {
        YQL_ENSURE(!optimizedStages.contains(stage.Ref().UniqueId()));

        TCoLambda lambda = stage.Program();
        TVector<TCoArgument> newArgs;
        newArgs.reserve(stage.Inputs().Size());

        // Propagate "WideFromBlock" through connections.
        // TODO(ilezhankin): this peephole optimization should be implemented instead as
        //       the original whole-graph transformer |CreateDqBuildWideBlockChannelsTransformer|.
        if (config->BlockChannelsMode == NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO) {
            TNodeOnNodeOwnedMap argsMap;

            YQL_ENSURE(stage.Inputs().Size() == stage.Program().Args().Size());

            for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
                auto oldArg = stage.Program().Args().Arg(i);
                auto newArg = TCoArgument(ctx.NewArgument(oldArg.Pos(), oldArg.Name()));
                newArg.MutableRef().SetTypeAnn(oldArg.Ref().GetTypeAnn());
                newArgs.emplace_back(newArg);

                if (auto connection = stage.Inputs().Item(i).Maybe<TDqConnection>(); connection &&
                    CanPropagateWideBlockThroughChannel(connection.Cast().Output(), programs, TDqStageSettings::Parse(stage), ctx, typesCtx))
                {
                    TExprNode::TPtr newArgNode = ctx.Builder(oldArg.Pos())
                        .Callable("FromFlow")
                            .Callable(0, "WideFromBlocks")
                                .Callable(0, "ToFlow")
                                    .Add(0, newArg.Ptr())
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                    argsMap.emplace(oldArg.Raw(), newArgNode);

                    auto stageUid = connection.Cast().Output().Stage().Ref().UniqueId();

                    // Update input program with: FromFlow(WideFromBlocks($1)) → FromFlow($1)
                    if (const auto& inputProgram = programs.at(stageUid); inputProgram.Lambda().Body().Maybe<TCoFromFlow>() &&
                        inputProgram.Lambda().Body().Cast<TCoFromFlow>().Input().Maybe<TCoWideFromBlocks>())
                    {
                        auto newBody = Build<TCoFromFlow>(ctx, inputProgram.Lambda().Body().Cast<TCoFromFlow>().Pos())
                            .Input(inputProgram.Lambda().Body().Cast<TCoFromFlow>().Input().Cast<TCoWideFromBlocks>().Input())
                            .Done();

                        auto newInputProgram = Build<TKqpProgram>(ctx, inputProgram.Pos())
                            .Lambda()
                                .Args(inputProgram.Lambda().Args())
                                .Body(newBody)
                            .Build()
                            .ArgsType(inputProgram.ArgsType())
                            .Done();

                        // Run the peephole optimization on new program again to update type annotations.
                        // TODO(ilezhankin): refactor to run only the update of type annotations - not the whole optimization.
                        bool allowNonDeterministicFunctions = !newInputProgram.Lambda().Body().Maybe<TKqpEffects>();
                        TExprNode::TPtr newInputProgramNode;

                        auto status = PeepHoleOptimize(newInputProgram, newInputProgramNode, ctx, typeAnnTransformer, typesCtx, config,
                            allowNonDeterministicFunctions, withFinalStageRules, disabledOpts);
                        if (status != TStatus::Ok) {
                            ctx.AddError(TIssue(ctx.GetPosition(stage.Pos()), "Peephole optimization failed for KQP transaction"));
                            return {};
                        }

                        programs.at(stageUid) = TKqpProgram(newInputProgramNode);
                    }

                    // Update the type annotation for an argument with return value of the input program.
                    newArg.MutableRef().SetTypeAnn(programs.at(stageUid).Lambda().Body().Ref().GetTypeAnn());
                } else {
                    argsMap.emplace(oldArg.Raw(), newArg.Ptr());
                }
            }

            // Rebuild lambda with new arguments.
            lambda = Build<TCoLambda>(ctx, lambda.Pos())
                .Args(newArgs)
                .Body(ctx.ReplaceNodes(stage.Program().Body().Ptr(), argsMap))
            .Done();
        } else {
            for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
                auto oldArg = stage.Program().Args().Arg(i);
                auto newArg = TCoArgument(ctx.NewArgument(oldArg.Pos(), oldArg.Name()));
                newArg.MutableRef().SetTypeAnn(oldArg.Ref().GetTypeAnn());
                newArgs.emplace_back(newArg);
            }
        }

        TVector<const TTypeAnnotationNode*> argTypes;
        for (const auto& arg : newArgs) {
            YQL_ENSURE(arg.Ref().GetTypeAnn());
            argTypes.push_back(arg.Ref().GetTypeAnn());
        }

        // TODO: get rid of TKqpProgram-callable (YQL-10078)
        auto program = Build<TKqpProgram>(ctx, stage.Pos())
            .Lambda(lambda)
            .ArgsType(ExpandType(stage.Pos(), *ctx.MakeType<TTupleExprType>(argTypes), ctx))
            .Done();

        YQL_ENSURE(programs.emplace(stage.Ref().UniqueId(), program).second);

        const bool allowNonDeterministicFunctions = !program.Lambda().Body().Maybe<TKqpEffects>();

        TExprNode::TPtr newProgram;
        auto status = PeepHoleOptimize(program, newProgram, ctx, typeAnnTransformer, typesCtx, config,
            allowNonDeterministicFunctions, withFinalStageRules, disabledOpts);
        if (status != TStatus::Ok) {
            ctx.AddError(TIssue(ctx.GetPosition(stage.Pos()), "Peephole optimization failed for KQP transaction"));
            return {};
        }

        if (allowNonDeterministicFunctions) {
            status = ReplaceNonDetFunctionsWithParams(newProgram, ctx, &nonDetParamBindings);

            if (status != TStatus::Ok) {
                ctx.AddError(TIssue(ctx.GetPosition(stage.Pos()),
                    "Failed to replace non deterministic functions with params for KQP transaction"));
                return {};
            }
        }

        optimizedStages.emplace(stage.Ref().UniqueId());
        programs.at(stage.Ref().UniqueId()) = TKqpProgram(newProgram);
    }

    TVector<TKqpParamBinding> bindings(tx.ParamBindings().begin(), tx.ParamBindings().end());

    for (const auto& [_, binding] : nonDetParamBindings) {
        bindings.emplace_back(std::move(binding));
    }

    TVector<TDqPhyStage> newStages;
    TNodeOnNodeOwnedMap stagesMap;

    // Rebuild stages only after all new programs are ready.
    for (const auto& stage : topSortedStages) {
        auto newStage = Build<TDqPhyStage>(ctx, stage.Pos())
            .InitFrom(stage)
            .Inputs(ctx.ReplaceNodes(stage.Inputs().Ptr(), stagesMap))
            .Program(ctx.DeepCopyLambda(programs.at(stage.Ref().UniqueId()).Lambda().Ref()))
            .Done();

        newStages.emplace_back(newStage);
        stagesMap.emplace(stage.Raw(), newStage.Ptr());
    }

    return Build<TKqpPhysicalTx>(ctx, tx.Pos())
        .Stages()
            .Add(newStages)
            .Build()
        .Results(ctx.ReplaceNodes(tx.Results().Ptr(), stagesMap))
        .ParamBindings().Add(bindings).Build()
        .Settings(tx.Settings())
        .Done();
}

class TKqpTxPeepholeTransformer : public TSyncTransformerBase {
public:
    TKqpTxPeepholeTransformer(
        IGraphTransformer* typeAnnTransformer,
        TTypeAnnotationContext& typesCtx,
        TKikimrConfiguration::TPtr config,
        bool withFinalStageRules,
        TSet<TString> disabledOpts
    )
        : TypeAnnTransformer(typeAnnTransformer)
        , TypesCtx(typesCtx)
        , Config(config)
        , WithFinalStageRules(withFinalStageRules)
        , DisabledOpts(disabledOpts)
    {}

    TStatus DoTransform(TExprNode::TPtr inputExpr, TExprNode::TPtr& outputExpr, TExprContext& ctx) final {
        if (Optimized) {
            YQL_CLOG(DEBUG, ProviderKqp) << ">>> TKqpTxPeepholeTransformer[skip]: " << KqpExprToPrettyString(*inputExpr, ctx);
            outputExpr = inputExpr;
            return TStatus::Ok;
        }

        YQL_CLOG(DEBUG, ProviderKqp) << ">>> TKqpTxPeepholeTransformer: " << KqpExprToPrettyString(*inputExpr, ctx);

        TExprBase input(inputExpr);
        YQL_ENSURE(input.Maybe<TKqpPhysicalTx>());

        auto tx = input.Cast<TKqpPhysicalTx>();

        THashSet<ui64> optimizedStages;
        auto optimizedTx = PeepholeOptimize(tx, ctx, *TypeAnnTransformer, TypesCtx, optimizedStages, Config, WithFinalStageRules, DisabledOpts);

        if (!optimizedTx) {
            return TStatus::Error;
        }

        outputExpr = optimizedTx.Cast().Ptr();
        Optimized = true;

        return TStatus(TStatus::Repeat, true);
    }

    void Rewind() final {
        Optimized = false;
    }

private:
    IGraphTransformer* TypeAnnTransformer;
    TTypeAnnotationContext& TypesCtx;
    TKikimrConfiguration::TPtr Config;
    bool Optimized = false;
    bool WithFinalStageRules = true;
    TSet<TString> DisabledOpts;
};

class TKqpTxsPeepholeTransformer : public TSyncTransformerBase {
public:
    TKqpTxsPeepholeTransformer(TAutoPtr<NYql::IGraphTransformer> typeAnnTransformer,
        TTypeAnnotationContext& typesCtx, TKikimrConfiguration::TPtr config)
        : TypeAnnTransformer(std::move(typeAnnTransformer))
    {
        TxTransformer = TTransformationPipeline(&typesCtx)
            .AddServiceTransformers()
            .Add(TLogExprTransformer::Sync("TxsPeephole", NYql::NLog::EComponent::ProviderKqp, NYql::NLog::ELevel::TRACE), "TxsPeephole")
            .Add(*TypeAnnTransformer, "TypeAnnotation")
            .AddPostTypeAnnotation(/* forSubgraph */ true)
            .Add(CreateKqpTxPeepholeTransformer(TypeAnnTransformer.Get(), typesCtx, config), "Peephole")
            .Build(false);
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {

        if (!TKqpPhysicalQuery::Match(input.Get())) {
            return TStatus::Error;
        }

        YQL_CLOG(DEBUG, ProviderKqp) << ">>> TKqpTxsPeepholeTransformer: " << KqpExprToPrettyString(*input, ctx);

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

} // anonymous namespace

TAutoPtr<IGraphTransformer> CreateKqpTxPeepholeTransformer(
    NYql::IGraphTransformer* typeAnnTransformer,
    TTypeAnnotationContext& typesCtx,
    const TKikimrConfiguration::TPtr& config,
    bool withFinalStageRules,
    TSet<TString> disabledOpts
)
{
    return new TKqpTxPeepholeTransformer(typeAnnTransformer, typesCtx, config, withFinalStageRules, disabledOpts);
}

TAutoPtr<IGraphTransformer> CreateKqpTxsPeepholeTransformer(
    TAutoPtr<NYql::IGraphTransformer> typeAnnTransformer,
    TTypeAnnotationContext& typesCtx,
    const TKikimrConfiguration::TPtr& config
)
{
    return new TKqpTxsPeepholeTransformer(std::move(typeAnnTransformer), typesCtx, config);
}

} // namespace NKikimr::NKqp::NOpt
