#include <yql/essentials/core/yql_expr_optimize.h>
#include "kqp_rbo_physical_filter_builder.h"
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TExprNode::TPtr TPhysicalFilterBuilder::BuildPhysicalFilter(TExprNode::TPtr input) {
    const auto inputColumns = Filter->GetInput()->GetOutputIUs();

    // clang-format off
    input = Build<TCoToFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    input = NPhysicalConvertionUtils::BuildExpandMapForNarrowInput(input, inputColumns, Ctx);

    THashMap<TString, ui32> colNamesToIndices;
    TVector<TExprNode::TPtr> lambdaArgs;

    for (ui32 i = 0; i < inputColumns.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "arg_" + ToString(i)));
        colNamesToIndices.emplace(inputColumns[i].GetFullName(), i);
    }

    auto lambda = TCoLambda(Filter->FilterLambda);
    auto lambdaBody = lambda.Body().Ptr();
    const bool isPg = lambdaBody->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg;

    auto isMember = [&](const TExprNode::TPtr& node) -> bool {
        if (node->IsCallable("Member")) {
            return true;
        }
        return false;
    };

    TNodeOnNodeOwnedMap replaces;
    auto members = FindNodes(lambdaBody, isMember);
    for (const auto& member : members) {
        const auto colName = TString(TCoMember(member).Name().StringValue());
        auto it = colNamesToIndices.find(colName);
        Y_ENSURE(it != colNamesToIndices.end(), colName + " column not found.");
        replaces[member.Get()] = lambdaArgs[it->second];
    }

    auto lambdaResult = Ctx.ReplaceNodes(std::move(lambdaBody), replaces);
    if (isPg) {
        // Fixes coalesce type mismatch.
        // clang-format off
        lambdaResult = Ctx.Builder(Pos)
            .Callable("FromPg")
                .Add(0, lambdaResult)
            .Seal()
        .Build();
        // clang-format on
    }

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
    input = Build<TCoWideFilter>(Ctx, Pos)
        .Input(input)
        .Lambda(std::move(wideLambda))
    .Done().Ptr();
    // clang-format on

    input = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(input, inputColumns, Ctx);

    // clang-format off
    input = Build<TCoFromFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical filter] " << KqpExprToPrettyString(TExprBase(input), Ctx);

    return input;
}
