#include "kqp_rules_include.h"

namespace {
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TString GetColName(const TString& colName, bool stripAliasPrefix = true) {
    if (!stripAliasPrefix)
        return colName;

    auto it = colName.find(".");
    if (it != TString::npos) {
        return colName.substr(it + 1);
    }
    return colName;
}

bool IsSuitableToPushProjectionToColumnTables(const TIntrusivePtr<IOperator>& input) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    const auto filter = CastOperator<TOpMap>(input);
    const auto maybeRead = filter->GetInput();
    return ((maybeRead->Kind == EOperator::Source) && (CastOperator<TOpRead>(maybeRead)->GetTableStorageType() == NYql::EStorageType::ColumnStorage) &&
            filter->GetTypeAnn());
}
}

namespace NKikimr {
namespace NKqp {

TIntrusivePtr<IOperator> TPushOlapProjectionRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(props);
    if (!(ctx.KqpCtx.Config->HasOptEnableOlapPushdown() && ctx.KqpCtx.Config->GetEnableOlapPushdownProjections())) {
        return input;
    }

    if (!IsSuitableToPushProjectionToColumnTables(input)) {
        return input;
    }

    const auto map = CastOperator<TOpMap>(input);
    const auto read = CastOperator<TOpRead>(map->GetInput());
    const TPushdownOptions pushdownOptions(false, false, /*StripAliasPrefixForColumnName=*/true);

    TVector<std::pair<TString, TExprNode::TPtr>> olapOperationsForProjections;
    auto memberPred = [](const TExprNode::TPtr& node) -> bool { return !!TMaybeNode<TCoMember>(node); };
    THashSet<TString> projectionMembers;
    THashSet<TString> predicateMembers;
    THashSet<TString> notSuitableToPushMembers;
    ui32 nextMemberId = 0;

    TVector<std::tuple<TString, TExprNode::TPtr, TExprNode::TPtr, TExprNode::TPtr>> projectionCandidates;
    TVector<ui32> inMapIndices;
    const auto& mapElements = map->GetMapElements();
    // Iterate over map elements and try to find an expression to push down to column shard.
    for (ui32 mapIndex = 0; mapIndex < mapElements.size(); ++mapIndex) {
        const auto& mapElement = mapElements[mapIndex];
        if (!mapElement.IsRename()) {
            const auto lambda = TCoLambda(mapElement.GetExpression().Node);
            const auto& arg = lambda.Args().Arg(0).Ref();
            auto body = lambda.Body().Ptr();
            if (!CollectOlapOperationForProjection(body, arg, predicateMembers, projectionMembers, projectionCandidates, nextMemberId, ctx.ExprCtx,
                                                   pushdownOptions)) {
                auto members = FindNodes(body, memberPred);
                for (const auto& member : members) {
                    notSuitableToPushMembers.insert(TString(TExprBase(member).Cast<TCoMember>().Name()));
                }
            } else {
                inMapIndices.push_back(mapIndex);
            }
        }
    }

    if (projectionCandidates.empty()) {
        return input;
    }

    ui32 projectionIndex = 0;
    TVector<TMapElement> newMapElements;
    for (ui32 mapIndex = 0; mapIndex < mapElements.size(); ++mapIndex) {
        TMapElement mapElement = mapElements[mapIndex];
        if (inMapIndices[projectionIndex] == mapIndex) {
            const auto& [colName, projection, replace, olapOperation] = projectionCandidates[projectionIndex++];
            Y_ENSURE(colName.find("__kqp_olap_projection") == TString::npos, "Multiple projections for same column is not supported");
            if (!notSuitableToPushMembers.count(colName)) {
                olapOperationsForProjections.emplace_back(GetColName(colName), olapOperation);
                // Replace old expression with new.
                auto oldLambda = TCoLambda(mapElement.GetExpression().Node);
                // clang-format off
                auto newLambda = Build<TCoLambda>(ctx.ExprCtx, projection->Pos())
                    .Args({"arg"})
                    .Body<TExprApplier>()
                        .Apply(TExprBase(ctx.ExprCtx.ReplaceNode(oldLambda.Body().Ptr(), *projection, replace)))
                        .With(oldLambda.Args().Arg(0), "arg")
                    .Build()
                .Done().Ptr();
                // clang-format on
                mapElement = TMapElement(mapElements[mapIndex].GetElementName(), TExpression(newLambda, &ctx.ExprCtx, &props));
            }
        }
        newMapElements.push_back(mapElement);
    }

    if (olapOperationsForProjections.empty()) {
        return input;
    }

    TVector<TExprBase> projections;
    for (const auto& [columnName, olapOperation] : olapOperationsForProjections) {
        // clang-format off
        auto olapProjection = Build<TKqpOlapProjection>(ctx.ExprCtx, olapOperation->Pos())
            .OlapOperation(olapOperation)
            .ColumnName().Build(columnName)
        .Done();
        // clang-format on
        projections.push_back(olapProjection);
    }

    const auto olapProcess =
        read->OlapFilterLambda ? TCoLambda(read->OlapFilterLambda) :
            // clang-format off
            Build<TCoLambda>(ctx.ExprCtx, read->Pos)
                .Args({"arg_0"})
                .Body("arg_0")
            .Done();
            // clang-format on

    // clang-format off
    auto olapProjections = Build<TKqpOlapProjections>(ctx.ExprCtx, olapProcess.Pos())
        .Input(olapProcess.Body())
        .Projections()
            .Add(projections)
        .Build()
    .Done();
    // clang-format on

    // clang-format off
    auto newLambda = Build<TCoLambda>(ctx.ExprCtx, olapProcess.Pos())
        .Args({"arg"})
        .Body<TExprApplier>()
            .Apply(olapProjections)
            .With(olapProcess.Args().Arg(0), "arg")
        .Build()
    .Done().Ptr();
    // clang-format on

    YQL_CLOG(TRACE, ProviderKqp) << "Pushed OLAP projection: " << KqpExprToPrettyString(TExprBase(newLambda), ctx.ExprCtx);

    auto newRead = MakeIntrusive<TOpRead>(read->Alias, read->Columns, read->GetOutputIUs(), read->StorageType, read->TableCallable, newLambda, read->Limit,
                                          read->Ranges, read->SortDir, read->Props, read->Pos);
    return MakeIntrusive<TOpMap>(newRead, map->Pos, newMapElements, map->Project, map->Ordered);
}
} // namespace NKqp
}
