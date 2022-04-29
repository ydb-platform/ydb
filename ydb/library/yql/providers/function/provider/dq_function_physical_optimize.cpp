#include "dq_function_provider_impl.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/providers/function/expr_nodes/dq_function_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

namespace NYql::NDqFunction {
namespace {

using namespace NNodes;
using namespace NDq;

class TDqFunctionPhysicalOptTransformer : public TOptimizeTransformerBase {
public:
    TDqFunctionPhysicalOptTransformer(TDqFunctionState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderDq, {})
        , State(state)
    {
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TDqFunctionPhysicalOptTransformer::name)
        // (Apply (SqlExternalFunction ..) ..) to stage
        AddHandler(0, &TCoApply::Match, HNDL(DqBuildExtFunctionStage));
#undef HNDL

        SetGlobal(0); // Stage 0 of this optimizer is global => we can remap nodes.
    }

    TMaybeNode<TExprBase> DqBuildExtFunctionStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const {
        Y_UNUSED(optCtx);

        auto apply = node.Cast<TCoApply>();
        auto callable = apply.Callable().Maybe<TDqSqlExternalFunction>();
        if (!callable
            || apply.Args().Count() != 2
            || !apply.Arg(1).Maybe<TDqCnUnionAll>()) {

            return node;
        }
        callable = callable.Cast();
        TDqCnUnionAll nodeInput {apply.Arg(1).Cast<TDqCnUnionAll>()};

        const TParentsMap* parentsMap = getParents();
        if (!IsSingleConsumerConnection(nodeInput, *parentsMap, false)) {
            YQL_ENSURE(false, "Allow only single external function stage usage");
        }

        const auto shuffleColumn = Build<TCoAtom>(ctx, node.Pos())
                .Value("_yql_transform_shuffle")
                .Done();
        auto addShuffleColumn = Build<TCoLambda>(ctx, node.Pos())
                .Args({"stream"})
                .Body<TCoMap>()
                    .Input("stream")
                    .Lambda()
                        .Args({"row"})
                        .Body<TCoAddMember>()
                            .Struct("row")
                            .Name(shuffleColumn)
                            .Item<TCoRandom>().Add<TCoDependsOn>().Input("row").Build().Build()
                            .Build()
                        .Build()
                    .Build()
                .Done();
        auto removeShuffleColumn = Build<TCoLambda>(ctx, node.Pos())
                    .Args({"row"})
                    .Body<TCoForceRemoveMember>()
                        .Struct("row")
                        .Name(shuffleColumn)
                        .Build()
                    .Done();

        auto transformType = callable.TransformType().Cast<TCoString>().Literal().StringValue();
        auto transformName = callable.TransformName().Cast<TCoString>().Literal().StringValue();

        TString connectionName;
        TExprNode::TPtr inputType;
        TExprNode::TPtr outputType;

        for (const auto &tuple: callable.Settings().Ref().Children()) {
            const auto paramName = tuple->Head().Content();
            if (paramName == "connection") {
                connectionName = TString{tuple->Tail().Tail().Content()};
            } else if (paramName == "input_type") {
                inputType = tuple->TailPtr();
            } else if (paramName == "output_type") {
                outputType = tuple->TailPtr();
            } else {
            }
        }

        const auto description = State->FunctionsDescription.find(TDqFunctionDescription{
            .Type = transformType,
            .FunctionName = transformName,
            .Connection = connectionName
        });

        YQL_ENSURE(description != State->FunctionsDescription.end(), "External function meta doesn't found " << transformName);

        auto stage = nodeInput.Output().Stage().Cast<TDqStage>();
        auto dutyColumn = DqPushLambdaToStage(stage, nodeInput.Output().Index(), addShuffleColumn, {}, ctx, optCtx);
        YQL_ENSURE(dutyColumn);

        auto transformSink = Build<TFunctionDataSink>(ctx, node.Pos())
                .Category().Build(FunctionProviderName)
                .Type<TCoAtom>().Build(transformType)
                .Connection().Build(connectionName)
                .Done();

        auto settings = Build<TFunctionTransformSettings>(ctx, node.Pos())
                .InvokeUrl<TCoAtom>().Build(description->InvokeUrl)
                .Done();

        auto dqTransform = Build<TDqTransform>(ctx, node.Pos())
                .Index().Build("0")
                .DataSink(transformSink)
                .Type<TCoAtom>().Build(transformType)
                .InputType(inputType)
                .OutputType(outputType)
                .Settings(settings)
                .Done();

        auto transformStage = Build<TDqStage>(ctx, node.Pos())
                .Inputs()
                    .Add<TDqCnHashShuffle>()
                    .KeyColumns()
                        .Add({shuffleColumn})
                    .Build()
                    .Output()
                        .Stage(dutyColumn.Cast())
                        .Index(nodeInput.Output().Index())
                        .Build()
                    .Build()
                .Build()
                .Program()
                    .Args({"row"})
                    .Body<TCoMap>()
                        .Lambda(removeShuffleColumn)
                        .Input("row")
                        .Build()
                    .Build()
                .Settings().Build()
                .Outputs().Add(dqTransform).Build()
                .Done();

        auto externalStage = Build<TDqCnUnionAll>(ctx, node.Pos())
                .Output()
                    .Stage(transformStage)
                    .Index().Build("0")
                    .Build()
                .Done();

        return externalStage;
    }

private:
    TDqFunctionState::TPtr State;
};

} // namespace

THolder<IGraphTransformer> CreateDqFunctionPhysicalOptTransformer(TDqFunctionState::TPtr state) {
    return MakeHolder<TDqFunctionPhysicalOptTransformer>(state);
}

}