#include "kqp_rbo_physical_map_builder.h"
#include <yql/essentials/core/yql_expr_optimize.h>
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

NPhysicalConvertionUtils::TStageBody TPhysicalMapBuilder::BuildPhysicalOp(const NPhysicalConvertionUtils::TStageBody& input) {
    const auto inputColumns = NPhysicalConvertionUtils::GetLiveInputIUs(Map, 0);
    const auto liveOutputs = NPhysicalConvertionUtils::BuildNameSet(NPhysicalConvertionUtils::GetLiveOutputIUs(Map), Names);

    auto wideInput = input.AsWide(inputColumns, Ctx, Names);

    THashMap<TString, ui32> colNamesToIndices;
    TVector<TExprNode::TPtr> lambdaArgs;
    TVector<TExprNode::TPtr> lambdaResults;

    TVector<TString> outputColumns;

    for (ui32 i = 0; i < inputColumns.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "arg_" + ToString(i)));
        colNamesToIndices.emplace(Names.Get(inputColumns[i]), i);
    }

    for (const auto& column : inputColumns) {
        const auto& fullName = Names.Get(column);
        if (!liveOutputs.contains(fullName)) {
            continue;
        }
        auto it = colNamesToIndices.find(fullName);
        Y_ENSURE(it != colNamesToIndices.end());
        lambdaResults.push_back(lambdaArgs[it->second]);
        outputColumns.push_back(fullName);
    }

    for (const auto& [output, mapElement] : Map.GetMapElements().Items()) {
        const auto outColName = Names.Get(output);
        if (!liveOutputs.contains(outColName)) {
            continue;
        }

        auto lambda = TCoLambda(mapElement.GetExpression().Node);
        auto lambdaBody = lambda.Body().Ptr();

        auto isMember = [&](const TExprNode::TPtr& node) -> bool {
            if (node->IsCallable("Member") && &node->Head() == lambda.Args().Arg(0).Raw()) {
                return true;
            }
            return false;
        };

        // For expressions - we want to find all members and replace them with lambda args.
        TNodeOnNodeOwnedMap replaces;
        auto members = FindNodes(lambdaBody, isMember);
        for (const auto& member : members) {
            const auto colName = Names.Get(GetMemberId(*member));
            auto it = colNamesToIndices.find(colName);
            Y_ENSURE(it != colNamesToIndices.end(), colName + " column not found.");
            replaces[member.Get()] = lambdaArgs[it->second];
        }
        lambdaResults.push_back(Ctx.ReplaceNodes(std::move(lambdaBody), replaces));

        outputColumns.push_back(outColName);
    }

    // Create a wide lambda.
    auto wideLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));

    // clang-format off
    auto output = Build<TCoWideMap>(Ctx, Pos)
        .Input(wideInput)
        .Lambda(std::move(wideLambda))
    .Done().Ptr();
    // clang-format on

    // The wide lambda already emits exactly the live outputs, in outputColumns order.
    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical map] " << KqpExprToPrettyString(TExprBase(output), Ctx);

    return NPhysicalConvertionUtils::TStageBody::Wide(output, std::move(outputColumns));
}
