#include "dq_task_program.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>

namespace NYql::NDq {

using namespace NKikimr::NMiniKQL;
using namespace NYql::NNodes;


class TSpillingTransformProvider {
public:

    TSpillingTransformProvider(const TSpillingSettings& spillingSettings): SpillingSettings(spillingSettings){};

    TCallableVisitFunc operator()(TInternName name) {
        if (RuntimeVersion >= 50U && SpillingSettings.IsGraceJoinSpillingEnabled() && (name == "GraceJoin" || name == "GraceSelfJoin")) {
            return [name](NKikimr::NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
                TCallableBuilder callableBuilder(env,
                    TStringBuilder() << callable.GetType()->GetName() << "WithSpilling",
                    callable.GetType()->GetReturnType(), false);
                for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
                    callableBuilder.Add(callable.GetInput(i));
                }
                return TRuntimeNode(callableBuilder.Build(), false);
            };
        }

        return TCallableVisitFunc();
    }

private:

    TSpillingSettings SpillingSettings;
};

const TStructExprType* CollectParameters(NNodes::TCoLambda program, TExprContext& ctx) {
    TVector<const TItemExprType*> memberTypes;

    VisitExpr(program.Ptr(), [&memberTypes, &ctx] (const TExprNode::TPtr& node) {
      if (auto maybeParam = TMaybeNode<TCoParameter>(node)) {
          auto param = maybeParam.Cast();

          memberTypes.push_back(ctx.MakeType<TItemExprType>(param.Name(), param.Ref().GetTypeAnn()));
      }

      return true;
    });

    return ctx.MakeType<TStructExprType>(memberTypes);
}

TString BuildProgram(NNodes::TCoLambda program, const TStructExprType& paramsType,
                     const NCommon::IMkqlCallableCompiler& compiler, const TTypeEnvironment& typeEnv,
                     const IFunctionRegistry& funcRegistry, TExprContext& exprCtx, const TVector<TExprBase>& reads, const TSpillingSettings& spillingSettings)
{
    TProgramBuilder pgmBuilder(typeEnv, funcRegistry);

    TVector<TRuntimeNode> inputNodes;
    NCommon::TMkqlBuildContext::TArgumentsMap arguments;

    auto paramsNode = pgmBuilder.Arg(NCommon::BuildType(program.Ref(), paramsType, pgmBuilder));

    for (const auto& arg : program.Args()) {
        YQL_ENSURE(arg.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream, "program: " << program.Ref().Dump());
        auto itemType = NCommon::BuildType(arg.Ref(), *arg.Ref().GetTypeAnn(), pgmBuilder);
        TRuntimeNode inputNode = pgmBuilder.Arg(itemType);
        arguments[arg.Raw()] = inputNode;
        inputNodes.push_back(inputNode);
    }

    NYql::NCommon::TMkqlBuildContext ctx(compiler, pgmBuilder, exprCtx, program.Ref().UniqueId(), std::move(arguments));
    ctx.Parameters = paramsNode;

    TRuntimeNode rootNode = MkqlBuildExpr(program.Body().Ref(), ctx);

    TExploringNodeVisitor explorer;
    if (spillingSettings) {
        explorer.Walk(rootNode.GetNode(), typeEnv);
        bool wereChanges = false;
        rootNode = SinglePassVisitCallables(rootNode, explorer, TSpillingTransformProvider(spillingSettings), typeEnv, true, wereChanges);
    }

    TStructLiteralBuilder structBuilder(typeEnv);
    structBuilder.Add("Program", rootNode);
    structBuilder.Add("Inputs", pgmBuilder.NewTuple(inputNodes));
    structBuilder.Add("Parameters", paramsNode);

    if (!reads.empty()) {
        auto readsExpr = Build<TExprList>(exprCtx, program.Pos())
            .Add(reads)
            .Done();
        TRuntimeNode readsNode = MkqlBuildExpr(readsExpr.Ref(), ctx);
        structBuilder.Add("Reads", readsNode);
    }

    auto programNode = structBuilder.Build();

    explorer.Walk(programNode, typeEnv);
    ui32 uniqueId = 0;
    for (auto& node : explorer.GetNodes()) {
        if (node->GetType()->GetKind() == TType::EKind::Callable) {
            auto& callable = static_cast<NKikimr::NMiniKQL::TCallable&>(*node);
            callable.SetUniqueId(++uniqueId);
        }
    }

    return SerializeNode(programNode, typeEnv);
}

} // namespace NYql::NDq
