#include "kqp_rbo_physical_map_builder.h"
#include <yql/essentials/core/yql_expr_optimize.h>
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TExprNode::TPtr TPhysicalMapBuilder::BuildPhysicalOp(TExprNode::TPtr input) {
    const auto inputColumns = Map->GetInput()->GetOutputIUs();

    // clang-format off
    input = Build<TCoToFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    input = NPhysicalConvertionUtils::BuildExpandMapForNarrowInput(input, inputColumns, Ctx);

    THashMap<TString, ui32> colNamesToIndices;
    TVector<TExprNode::TPtr> lambdaArgs;
    TVector<TExprNode::TPtr> lambdaResults;

    TVector<TString> outputColumns;
    THashMap<ui32, TString> renameMap;

    for (ui32 i = 0; i < inputColumns.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "arg_" + ToString(i)));
        colNamesToIndices.emplace(inputColumns[i].GetFullName(), i);
    }

    if (!Map->Project) {
        for (const auto& input : inputColumns) {
            const auto& fullName = input.GetFullName();
            auto it = colNamesToIndices.find(fullName);
            Y_ENSURE(it != colNamesToIndices.end());
            lambdaResults.push_back(lambdaArgs[it->second]);
            outputColumns.push_back(fullName);
        }
    }

    for (const auto& mapElement : Map->MapElements) {
        if (mapElement.IsRename()){
            const auto colName = mapElement.GetRename().GetFullName();
            auto it = colNamesToIndices.find(colName);
            Y_ENSURE(it != colNamesToIndices.end(), colName + " column not found.");
            lambdaResults.push_back(lambdaArgs[it->second]);
        }
        else {
            auto lambda = TCoLambda(mapElement.GetExpression().Node);
            auto lambdaBody = lambda.Body().Ptr();

            auto isMember = [&](const TExprNode::TPtr& node) -> bool {
                if (node->IsCallable("Member")) {
                    return true;
                }
                return false;
            };

            // For expressions - we want to find all members and replace them with lambda args.
            TNodeOnNodeOwnedMap replaces;
            auto members = FindNodes(lambdaBody, isMember);
            for (const auto& member : members) {
                const auto colName = TString(TCoMember(member).Name().StringValue());
                auto it = colNamesToIndices.find(colName);
                Y_ENSURE(it != colNamesToIndices.end(), colName + " column not found.");
                replaces[member.Get()] = lambdaArgs[it->second];
            }
            lambdaResults.push_back(Ctx.ReplaceNodes(std::move(lambdaBody), replaces));
        }

        const auto outColName = mapElement.GetElementName().GetFullName();
        renameMap.emplace(outputColumns.size(), outColName);
        outputColumns.push_back(outColName);
    }

    // Create a wide lambda.
    auto wideLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));

    // clang-format off
    input = Build<TCoWideMap>(Ctx, Pos)
        .Input(input)
        .Lambda(std::move(wideLambda))
    .Done().Ptr();
    // clang-format on

    input = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(input, outputColumns, renameMap, Ctx);

    // clang-format off
    input = Build<TCoFromFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical map] " << KqpExprToPrettyString(TExprBase(input), Ctx);

    return input;
}
