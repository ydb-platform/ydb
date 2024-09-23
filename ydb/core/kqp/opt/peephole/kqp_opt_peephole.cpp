#include "kqp_opt_peephole.h"
#include "kqp_opt_peephole_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/host/kqp_transform.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/library/naming_conventions/naming_conventions.h>

#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>
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
    TVector<TDqPhyStage> stages;
    stages.reserve(tx.Stages().Size());
    TNodeOnNodeOwnedMap stagesMap;
    TVector<TKqpParamBinding> bindings(tx.ParamBindings().begin(), tx.ParamBindings().end());
    THashMap<TString, TKqpParamBinding> nonDetParamBindings;

    for (const auto& stage : tx.Stages()) {
        YQL_ENSURE(!optimizedStages.contains(stage.Ref().UniqueId()));

        TVector<const TTypeAnnotationNode*> argTypes;
        for (const auto& arg : stage.Program().Args()) {
            YQL_ENSURE(arg.Ref().GetTypeAnn());
            argTypes.push_back(arg.Ref().GetTypeAnn());
        }

        // TODO: get rid of TKqpProgram-callable (YQL-10078)
        TNodeOnNodeOwnedMap tmp;
        auto program = Build<TKqpProgram>(ctx, stage.Pos())
            //.Lambda(ctx.DeepCopy(stage.Program().Ref(), ctx, tmp, true /* internStrings */, false /* copyTypes */))
            .Lambda(stage.Program())
            .ArgsType(ExpandType(stage.Pos(), *ctx.MakeType<TTupleExprType>(argTypes), ctx))
            .Done();

        bool allowNonDeterministicFunctions = !stage.Program().Body().Maybe<TKqpEffects>();

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

        auto newStage = Build<TDqPhyStage>(ctx, stage.Pos())
            .Inputs(ctx.ReplaceNodes(stage.Inputs().Ptr(), stagesMap))
            .Program(ctx.DeepCopyLambda(TKqpProgram(newProgram).Lambda().Ref()))
            .Settings(stage.Settings())
            .Outputs(stage.Outputs())
            .Done();

        stages.emplace_back(newStage);
        stagesMap.emplace(stage.Raw(), newStage.Ptr());

        optimizedStages.emplace(stage.Ref().UniqueId());
    }

    for (const auto& [_, binding] : nonDetParamBindings) {
        bindings.emplace_back(std::move(binding));
    }

    return Build<TKqpPhysicalTx>(ctx, tx.Pos())
        .Stages()
            .Add(stages)
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
