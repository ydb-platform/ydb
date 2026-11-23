#include "kqp_rbo_physical_filter_builder.h"
#include <yql/essentials/core/yql_expr_optimize.h>
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

NPhysicalConvertionUtils::TStageBody TPhysicalFilterBuilder::BuildPhysicalOp(const NPhysicalConvertionUtils::TStageBody& input) {
    const auto inputColumns = NPhysicalConvertionUtils::GetLiveInputIUs(Filter, 0);

    auto wideInput = input.AsWide(inputColumns, Ctx, Names);

    THashMap<TString, ui32> colNamesToIndices;
    TVector<TExprNode::TPtr> lambdaArgs;

    for (ui32 i = 0; i < inputColumns.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "arg_" + ToString(i)));
        colNamesToIndices.emplace(Names.Get(inputColumns[i]), i);
    }

    auto lambda = TCoLambda(Filter.GetFilterExpression().Node);
    auto lambdaBody = lambda.Body().Ptr();

    auto isMember = [&](const TExprNode::TPtr& node) -> bool {
        if (node->IsCallable("Member") && &node->Head() == lambda.Args().Arg(0).Raw()) {
            return true;
        }
        return false;
    };

    TNodeOnNodeOwnedMap replaces;
    auto members = FindNodes(lambdaBody, isMember);
    for (const auto& member : members) {
        const auto colName = Names.Get(GetMemberId(*member));
        auto it = colNamesToIndices.find(colName);
        Y_ENSURE(it != colNamesToIndices.end(), colName + " column not found.");
        replaces[member.Get()] = lambdaArgs[it->second];
    }

    auto lambdaResult = Ctx.ReplaceNodes(std::move(lambdaBody), replaces);
    // clang-format off
    lambdaResult = Build<TCoCoalesce>(Ctx, Pos)
        .Predicate(lambdaResult)
        .Value<TCoBool>()
            .Literal().Build("false")
        .Build()
    .Done().Ptr();
    // clang-format on

    // Create a wide lambda.
    auto wideLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), {lambdaResult});

    // clang-format off
    auto output = Build<TCoWideFilter>(Ctx, Pos)
        .Input(wideInput)
        .Lambda(std::move(wideLambda))
    .Done().Ptr();
    // clang-format on

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical filter] " << KqpExprToPrettyString(TExprBase(output), Ctx);

    // WideFilter passes every input slot through, so drop the ones that are dead past this operator.
    return NPhysicalConvertionUtils::BuildLiveWideBody(output, NPhysicalConvertionUtils::GetFullNames(inputColumns, Names),
        NPhysicalConvertionUtils::BuildNameSet(NPhysicalConvertionUtils::GetLiveOutputIUs(Filter), Names), Ctx);
}
