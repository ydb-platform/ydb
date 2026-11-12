#include <ydb/core/kqp/opt/physical/kqp_opt_phy_olap_filter.h>
#include <ydb/core/kqp/opt/physical/predicate_collector.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

// The input of the map without the filter the column shard could not run, which stays between the map and the read.
IOperator* SkipRemainingFilter(IOperator* input) {
    return input->Kind == EOperator::Filter ? input->GetChildren().front() : input;
}

bool IsSuitableToPushProjectionToColumnTables(IOperator* input) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    const auto map = CastOperator<TOpMap>(input);
    const auto maybeRead = SkipRemainingFilter(map->GetInput().Get());
    return ((maybeRead->Kind == EOperator::Source) && (CastOperator<TOpRead>(maybeRead)->GetTableStorageType() == NYql::EStorageType::ColumnStorage) &&
            map->GetTypeAnn());
}

} // anonymous namespace

bool TPushOlapProjectionRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Map &&
        SkipRemainingFilter(input->GetChildren().front())->Kind == EOperator::Source;
}

TIntrusivePtr<IOperator> TPushOlapProjectionRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (!(ctx.KqpCtx.Config->HasOptEnableOlapPushdown() && ctx.KqpCtx.Config->GetEnableOlapPushdownProjections())) {
        return input;
    }

    if (!IsSuitableToPushProjectionToColumnTables(input.get())) {
        return input;
    }

    const auto map = CastOperator<TOpMap>(input);
    TIntrusivePtr<TOpFilter> filter;
    if (map->GetInput()->Kind == EOperator::Filter) {
        filter = CastOperator<TOpFilter>(map->GetInput());
    }
    const auto read = CastOperator<TOpRead>(SkipRemainingFilter(map->GetInput().Get()));
    // Preserve the input's IU-ID atoms and row-schema fields during construction.
    const TPushdownOptions pushdownOptions(false, false, /*StripAliasPrefixForColumnName=*/false);

    TVector<std::pair<TString, TExprNode::TPtr>> olapOperationsForProjections;
    auto memberPred = [](const TExprNode::TPtr& node) -> bool { return !!TMaybeNode<TCoMember>(node); };
    THashSet<TString> projectionMembers;
    // As in the old optimizer, a projection cannot overwrite a column the remaining filter still reads.
    THashSet<TString> predicateMembers;
    if (filter) {
        for (const auto id : filter->GetFilterExpression().GetRawInputIUs()) {
            predicateMembers.insert(ToString(id));
        }
    }
    THashSet<TString> notSuitableToPushMembers;
    // OLAP projection overwrites the source field. An implicit pass-through
    // or an explicit copy still needs its original value and type.
    auto passthrough = GetLiveOut(map.Get());
    passthrough.IntersectWith(read->GetOutputIUs());
    for (const auto id : passthrough) {
        notSuitableToPushMembers.insert(ToString(id));
    }
    for (const auto& [id, element] : map->GetMapElements().Items()) {
        if (element.IsColumnAccess()) {
            notSuitableToPushMembers.insert(ToString(element.GetColumnAccess()));
        }
    }
    ui32 nextMemberId = 0;

    TVector<std::tuple<TString, TExprNode::TPtr, TExprNode::TPtr, TExprNode::TPtr>> projectionCandidates;
    TVector<TInfoUnitId> inMapIds;
    const auto& mapElements = map->GetMapElements();
    // Iterate over map elements and try to find an expression to push down to column shard.
    for (const auto output : mapElements.Keys()) {
        const auto& mapElement = *mapElements.Find(output);
        if (!mapElement.IsColumnAccess()) {
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
                inMapIds.push_back(output);
            }
        }
    }

    if (projectionCandidates.empty()) {
        return input;
    }

    ui32 projectionIndex = 0;
    TMapIUs newMapElements;
    // The elements which read the projections, kept apart when they have to move below the remaining filter.
    TMapIUs projectedElements;
    for (const auto output : mapElements.Keys()) {
        TMapElement mapElement = *mapElements.Find(output);
        bool projected = false;
        if (projectionIndex < inMapIds.size() && inMapIds[projectionIndex] == output) {
            const auto& [colName, projection, replace, olapOperation] = projectionCandidates[projectionIndex++];
            Y_ENSURE(colName.find(NOpt::KqpOlapProjectionNamePrefix) == TString::npos, "Multiple projections for same column is not supported");
            if (!notSuitableToPushMembers.count(colName)) {
                olapOperationsForProjections.emplace_back(colName, olapOperation);
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
                mapElement = TMapElement(TExpression(newLambda, &ctx.ExprCtx, &props));
                projected = true;
            }
        }
        (projected && filter ? projectedElements : newMapElements).Add(output, std::move(mapElement));
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

    TIntrusivePtr<IOperator> newInput =
        MakeIntrusive<TOpRead>(read->Alias, read->GetColumns(), read->StorageType, read->TableCallable, newLambda, read->Limit,
                               read->RangeInfo, read->OriginalPredicate, read->SortDir, read->Props, read->Pos, read->RequireSortedRead);
    if (filter) {
        // A projection replaces its source column in the read, with the type of the projection. The filter is typed over the
        // stored column, so the elements which read the projections move below it, onto the read: as in the old optimizer,
        // the column shard computes them before the remaining filter.
        newInput = MakeIntrusive<TOpMap>(newInput, map->Pos, std::move(projectedElements));
        newInput = MakeIntrusive<TOpFilter>(newInput, filter->Pos, filter->Props, filter->GetFilterExpression(), filter->PartiallyPushedDown);
    }
    return MakeIntrusive<TOpMap>(newInput, map->Pos, newMapElements);
}

} // namespace NKikimr::NKqp
