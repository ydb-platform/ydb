#include "kqp_opt_phy_rules.h"
#include "kqp_opt_phy_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>

#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <library/cpp/iterator/zip.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NDq;

// Temporary solution, should be replaced with constraints
// copy-past from old engine algo: https://a.yandex-team.ru/arc_vcs/yql/providers/kikimr/yql_kikimr_opt.cpp?rev=e592a5a9509952f1c29f1ec02343dd4c05fe426d#L122

using TTableData = std::pair<const NYql::TKikimrTableDescription*, NYql::TKqpReadTableSettings>;

TKqpTable GetTable(TExprBase input, bool isReadRanges) {
    if (isReadRanges) {
        return input.Cast<TKqlReadTableRangesBase>().Table();
    }

    return input.Cast<TKqpReadTable>().Table();
};

TExprBase KqpRemoveRedundantSortOverReadTable(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    auto maybeSort = node.Maybe<TCoSortBase>();
    auto maybeTopBase = node.Maybe<TCoTopBase>();

    if (!maybeSort && !maybeTopBase) {
        return node;
    }

    auto input = maybeSort ? maybeSort.Cast().Input() : maybeTopBase.Cast().Input();
    auto sortDirections = maybeSort ? maybeSort.Cast().SortDirections() : maybeTopBase.Cast().SortDirections();
    auto keySelector = maybeSort ? maybeSort.Cast().KeySelectorLambda() : maybeTopBase.Cast().KeySelectorLambda();

    auto maybeFlatmap = input.Maybe<TCoFlatMap>();

    TMaybe<THashSet<TStringBuf>> passthroughFields;
    if (maybeFlatmap) {
        auto flatmap = input.Cast<TCoFlatMap>();

        if (!IsPassthroughFlatMap(flatmap, &passthroughFields)) {
            return node;
        }

        input = flatmap.Input();
    }

    auto direction = GetSortDirection(sortDirections);
    if (direction != ESortDirection::Forward && direction != ESortDirection::Reverse) {
        return node;
    }

    bool isReadTable = input.Maybe<TKqpReadTable>().IsValid();
    bool isReadTableRanges = input.Maybe<TKqpReadTableRanges>().IsValid() || input.Maybe<TKqpReadOlapTableRanges>().IsValid() ;
    if (!isReadTable && !isReadTableRanges) {
        return node;
    }

    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, GetReadTablePath(input, isReadTableRanges));

    if (tableDesc.Metadata->Kind == EKikimrTableKind::Olap) {
        // OLAP tables are read in parallel, so we need to keep the out sort.
        return node;
    }

    auto settings = GetReadTableSettings(input, isReadTableRanges);

    ui64 pointPrefix = 0;
    if (input.Maybe<TKqpReadTableRanges>()) {
        auto prompt = TKqpReadTableExplainPrompt::Parse(input.Maybe<TKqpReadTableRanges>().Cast().ExplainPrompt());
        if (prompt.ExpectedMaxRanges.Defined() && *prompt.ExpectedMaxRanges == 1) {
            pointPrefix = prompt.PointPrefixLen;
        }
    } else if (input.Maybe<TKqpReadTable>()) {
        pointPrefix = settings.PointPrefixLen;
    }

    if (!kqpCtx.Config->EnablePointPredicateSortAutoSelectIndex){
        pointPrefix = 0;
    }

    if (!IsSortKeyPrimary(keySelector, tableDesc, passthroughFields, pointPrefix)) {
        return node;
    }

    if (direction == ESortDirection::Reverse) {
        // For sys views, we need to set reverse flag even if UseSource returns false
        // because sys view actors can handle reverse direction
        bool isSysView = tableDesc.Metadata->Kind == EKikimrTableKind::SysView;
        if (isSysView) {
            return node;
        }

        if (!UseSource(kqpCtx, tableDesc) && kqpCtx.IsScanQuery()) {
            return node;
        }

        if (settings.IsReverse()) {
            return node;
        }

        settings.SetSorting(ERequestSorting::DESC);

        input = BuildReadNode(input.Pos(), ctx, input, settings);
    } else if (direction == ESortDirection::Forward) {
        if (UseSource(kqpCtx, tableDesc)) {
            settings.SetSorting(ERequestSorting::ASC);
            input = BuildReadNode(input.Pos(), ctx, input, settings);
        }
    }

    if (maybeFlatmap) {
        input = Build<TCoFlatMap>(ctx, node.Pos())
            .Input(input)
            .Lambda(maybeFlatmap.Cast().Lambda())
            .Done();
    }

    if (maybeTopBase) {
        return Build<TCoTake>(ctx, node.Pos())
            .Input(input)
            .Count(maybeTopBase.Cast().Count())
            .Done();
    } else {
        return input;
    }
}

TExprBase KqpRemoveRedundantSortOverReadTableFSM(
    TExprBase node,
    TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx,
    const TTypeAnnotationContext& typeCtx
) {
    auto maybeSortBase = node.Maybe<TCoSortBase>();
    auto maybeTopBase = node.Maybe<TCoTopBase>();

    if (!maybeSortBase && !maybeTopBase) {
        return node;
    }

    auto input = maybeSortBase ? maybeSortBase.Cast().Input() : maybeTopBase.Cast().Input();
    auto sortDirections = maybeSortBase ? maybeSortBase.Cast().SortDirections() : maybeTopBase.Cast().SortDirections();
    auto keySelector = maybeSortBase ? maybeSortBase.Cast().KeySelectorLambda() : maybeTopBase.Cast().KeySelectorLambda();

    auto maybeFlatmap = input.Maybe<TCoFlatMap>();

    TMaybe<THashSet<TStringBuf>> passthroughFields;
    if (maybeFlatmap) {
        auto flatmap = input.Cast<TCoFlatMap>();

        if (!IsPassthroughFlatMap(flatmap, &passthroughFields)) {
            return node;
        }

        input = flatmap.Input();
    }

    bool isReadTable = input.Maybe<TKqpReadTable>().IsValid();
    bool isReadTableRanges = input.Maybe<TKqpReadTableRanges>().IsValid() || input.Maybe<TKqpReadOlapTableRanges>().IsValid() ;
    if (!isReadTable && !isReadTableRanges) {
        return node;
    }

    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, GetReadTablePath(input, isReadTableRanges));

    if (tableDesc.Metadata->Kind == EKikimrTableKind::Olap) {
        // OLAP tables are read in parallel, so we need to keep the out sort.
        return node;
    }

    auto settings = GetReadTableSettings(input, isReadTableRanges);
    auto table = GetTable(input, isReadTableRanges);

    bool isReversed = false;
    auto isSorted = [&](){
        auto tableStats = typeCtx.GetStats(table.Raw());
        auto sortStats = typeCtx.GetStats(node.Raw());
        if (!tableStats || !sortStats || !typeCtx.SortingsFSM) {
            return false;
        }

        YQL_CLOG(TRACE, CoreDq) << "Statistics of the input of the sort: " << tableStats->ToString();
        YQL_CLOG(TRACE, CoreDq) << "Statistics of the sort: " << sortStats->ToString();

        auto sortingIdx = sortStats->SortingOrderingIdx;

        auto& inputSortings = tableStats->SortingOrderings;
        if (inputSortings.ContainsSorting(sortingIdx)) {
            return true;
        }

        auto& reversedInputSortings = tableStats->ReversedSortingOrderings;
        if (reversedInputSortings.ContainsSorting(sortingIdx)) {
            isReversed = true;
            return true;
        }

        return false;
    };

    if (!isSorted()) {
        return node;
    }

    if (isReversed) {
        // For sys views, we need to set reverse flag even if UseSource returns false
        // because sys view actors can handle reverse direction
        bool isSysView = tableDesc.Metadata->Kind == EKikimrTableKind::SysView;
        if (isSysView) {
            return node;
        }

        if (!UseSource(kqpCtx, tableDesc) && kqpCtx.IsScanQuery()) {
            return node;
        }

        AFL_ENSURE(settings.GetSorting() == ERequestSorting::NONE);
        settings.SetSorting(ERequestSorting::DESC);
        input = BuildReadNode(input.Pos(), ctx, input, settings);
    } else {
        if (UseSource(kqpCtx, tableDesc)) {
            AFL_ENSURE(settings.GetSorting() == ERequestSorting::NONE);
            settings.SetSorting(ERequestSorting::ASC);
            input = BuildReadNode(input.Pos(), ctx, input, settings);
        }
    }

    if (maybeFlatmap) {
        input = Build<TCoFlatMap>(ctx, node.Pos())
            .Input(input)
            .Lambda(maybeFlatmap.Cast().Lambda())
            .Done();
    }

    if (maybeTopBase) {
        return Build<TCoTake>(ctx, node.Pos())
            .Input(input)
            .Count(maybeTopBase.Cast().Count())
            .Done();
    } else {
        return input;
    }
}

using namespace NYql::NDq;

bool CompatibleSort(TOptimizerStatistics::TSortColumns& existingOrder, const TCoLambda& keySelector, const TExprBase& sortDirections, TVector<TString>& sortKeys) {
    if (auto body = keySelector.Body().Maybe<TCoMember>()) {
        auto attrRef = body.Cast().Name().StringValue();
        auto attrName = existingOrder.Columns[0];
        auto attrNameWithAlias = existingOrder.Aliases[0] + "." + attrName;
        if (attrName == attrRef || attrNameWithAlias == attrRef){
            auto sortValue = sortDirections.Cast<TCoDataCtor>().Literal().Value();
            if (FromString<bool>(sortValue)) {
                sortKeys.push_back(attrRef);
                return true;
            }
        }
    }
    else if (auto body = keySelector.Body().Maybe<TExprList>()) {
        if (body.Cast().Size() > existingOrder.Columns.size()) {
            return false;
        }

        bool allMatched = false;
        auto dirs = sortDirections.Cast<TExprList>();
        for (size_t i=0; i < body.Cast().Size(); i++) {
            allMatched = false;
            auto item = body.Cast().Item(i);
            if (auto member = item.Maybe<TCoMember>()) {
                auto attrRef = member.Cast().Name().StringValue();
                auto attrName = existingOrder.Columns[i];
                auto attrNameWithAlias = existingOrder.Aliases[i] + "." + attrName;
                if (attrName == attrRef || attrNameWithAlias == attrRef){
                    auto sortValue = dirs.Item(i).Cast<TCoDataCtor>().Literal().Value();
                    if (FromString<bool>(sortValue)) {
                        sortKeys.push_back(attrRef);
                        allMatched = true;
                    }
                }
            }
            if (!allMatched) {
                return false;
            }
        }
        return true;
    }
    return false;
}

TExprBase KqpBuildTopStageRemoveSort(
    TExprBase node,
    TExprContext& ctx,
    IOptimizationContext& optCtx,
    TTypeAnnotationContext& typeCtx,
    const TParentsMap& parentsMap,
    bool allowStageMultiUsage,
    bool ruleEnabled
) {
    Y_UNUSED(optCtx);

    if (!ruleEnabled) {
        return node;
    }

    if (!node.Maybe<TCoTopBase>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    const auto top = node.Cast<TCoTopBase>();
    const auto dqUnion = top.Input().Cast<TDqCnUnionAll>();

    // skip this rule to activate KqpRemoveRedundantSortByPk later to reduce readings count
    auto stageBody = dqUnion.Output().Stage().Program().Body();
    if (stageBody.Maybe<TCoFlatMap>()) {
        auto flatmap = dqUnion.Output().Stage().Program().Body().Cast<TCoFlatMap>();
        auto input = flatmap.Input();
        bool isReadTable = input.Maybe<TKqpReadTable>().IsValid();
        bool isReadTableRanges = input.Maybe<TKqpReadTableRanges>().IsValid() || input.Maybe<TKqpReadOlapTableRanges>().IsValid() ;
        if (IsPassthroughFlatMap(flatmap, nullptr)) {
            if (isReadTable || isReadTableRanges) {
                return node;
            }
        }
    } else if (
        stageBody.Maybe<TKqpReadTable>().IsValid() ||
        stageBody.Maybe<TKqpReadTableRanges>().IsValid() ||
        stageBody.Maybe<TKqpReadOlapTableRanges>().IsValid()
    ) {
        return node;
    }

    auto inputStats = typeCtx.GetStats(dqUnion.Output().Raw());

    if (!inputStats || !inputStats->SortColumns) {
        return node;
    }

    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!CanPushDqExpr(top.Count(), dqUnion) || !CanPushDqExpr(top.KeySelectorLambda(), dqUnion)) {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoTop::idx_Input, std::move(connToPushableStage)));
    }

    const auto sortKeySelector = top.KeySelectorLambda();
    const auto sortDirections = top.SortDirections();
    TVector<TString> sortKeys;

    if (!CompatibleSort(*inputStats->SortColumns, sortKeySelector, sortDirections, sortKeys)) {
        return node;
    }

    auto builder = Build<TDqSortColumnList>(ctx, node.Pos());
    for (auto columnName : sortKeys ) {
        builder.Add<TDqSortColumn>()
            .Column<TCoAtom>().Build(columnName)
            .SortDirection().Build(TTopSortSettings::AscendingSort)
            .Build();
    }
    auto columnList = builder.Build().Value();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage<TDqStage>()
                .Inputs()
                    .Add<TDqCnMerge>()
                        .Output()
                            .Stage(dqUnion.Output().Stage())
                            .Index(dqUnion.Output().Index())
                            .Build()
                        .SortColumns(columnList)
                        .Build()
                    .Build()
                .Program()
                    .Args({"stream"})
                    .Body<TCoTake>()
                        .Input("stream")
                        .Count(top.Count())
                        .Build()
                    .Build()
                .Settings(NDq::TDqStageSettings::New().BuildNode(ctx, top.Pos()))
                .Build()
            .Index().Build(0U)
            .Build()
        //.SortColumns(columnList)
        .Done();
}

TExprBase KqpBuildTopStageRemoveSortFSM(
    TExprBase node,
    TExprContext& ctx,
    IOptimizationContext& /* optCtx */,
    TTypeAnnotationContext& typeCtx,
    const TParentsMap& parentsMap,
    bool allowStageMultiUsage,
    bool ruleEnabled
) {
    if (!ruleEnabled) {
        return node;
    }

    if (!node.Maybe<TCoTopBase>().Input().Maybe<TDqCnUnionAll>() && !node.Maybe<TCoSort>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto maybeSortBase = node.Maybe<TCoSort>();
    auto maybeTopBase = node.Maybe<TCoTopBase>();

    const auto dqUnion = maybeSortBase? maybeSortBase.Cast().Input().Cast<TDqCnUnionAll>(): maybeTopBase.Cast().Input().Cast<TDqCnUnionAll>();

    // skip this rule to activate KqpRemoveRedundantSortOverReadTable later to reduce readings count
    auto stageBody = dqUnion.Output().Stage().Program().Body();
    if (stageBody.Maybe<TCoFlatMap>()) {
        auto flatmap = dqUnion.Output().Stage().Program().Body().Cast<TCoFlatMap>();
        auto input = flatmap.Input();
        bool isReadTable = input.Maybe<TKqpReadTable>().IsValid();
        bool isReadTableRanges = input.Maybe<TKqpReadTableRanges>().IsValid() || input.Maybe<TKqpReadOlapTableRanges>().IsValid() ;
        if (IsPassthroughFlatMap(flatmap, nullptr)) {
            if (isReadTable || isReadTableRanges) {
                return node;
            }
        }
    } else if (
        stageBody.Maybe<TKqpReadTable>().IsValid() ||
        stageBody.Maybe<TKqpReadTableRanges>().IsValid() ||
        stageBody.Maybe<TKqpReadOlapTableRanges>().IsValid()
    ) {
        return node;
    }

    if (!typeCtx.SortingsFSM) {
        return node;
    }

    auto inputStats = typeCtx.GetStats(dqUnion.Output().Raw());
    if (!inputStats) {
        YQL_CLOG(TRACE, CoreDq) << "No statistics for the sort, skip";
        return node;
    }

    auto nodeStats = typeCtx.GetStats(node.Raw());
    if (!nodeStats) {
        return node;
    }

    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    const auto& keySelector = maybeSortBase? maybeSortBase.Cast().KeySelectorLambda() : maybeTopBase.Cast().KeySelectorLambda();

    if (!CanPushDqExpr(keySelector, dqUnion)) {
        return node;
    }

    if (maybeTopBase.IsValid() && !CanPushDqExpr(maybeTopBase.Cast().Count(), dqUnion)) {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoTop::idx_Input, std::move(connToPushableStage)));
    }

    YQL_CLOG(TRACE, CoreDq) << "Statistics of the input of the sort: " << inputStats->ToString();
    YQL_CLOG(TRACE, CoreDq) << "Statistics of the sort: " << nodeStats->ToString();
    if (!inputStats->SortingOrderings.ContainsSorting(nodeStats->SortingOrderingIdx)) {
        return node;
    }

    auto orderingInfo =
        maybeSortBase?
            GetSortBaseSortingOrderingInfo(maybeSortBase.Cast(), nullptr, nullptr) :
            GetTopBaseSortingOrderingInfo(maybeTopBase.Cast(), nullptr, nullptr)   ;

    if (orderingInfo.Directions.size() != orderingInfo.Ordering.size()) {
        return node;
    }

    auto builder = Build<TDqSortColumnList>(ctx, node.Pos());
    for (const auto& [dir, column] : Zip(orderingInfo.Directions, orderingInfo.Ordering)) {
        TString columnName = column.AttributeName;

        if (column.RelName) {
            columnName = column.RelName + "." + columnName;
        }

        TString topSortDir;
        switch (dir) {
            using enum NYql::NDq::TOrdering::TItem::EDirection;
            case EAscending: { topSortDir = TTopSortSettings::AscendingSort; break; }
            case EDescending: { topSortDir = TTopSortSettings::DescendingSort; break; }
            case ENone: { return node; }
        }

        builder
            .Add<TDqSortColumn>()
                .Column<TCoAtom>()
            .Build(std::move(columnName))
                .SortDirection()
                .Build(std::move(topSortDir))
            .Build();
    }
    auto columnList = builder.Build().Value();

    auto programBuilder =
            Build<TCoLambda>(ctx, node.Pos())
                .Args({"stream"});

    if (maybeTopBase) {
        programBuilder
            .Body<TCoTake>()
                .Input("stream")
                .Count(maybeTopBase.Cast().Count())
            .Build();
    } else {
        programBuilder
            .Body("stream")
            .Build();
    }

    auto program = programBuilder.Build().Value();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage<TDqStage>()
                .Inputs()
                    .Add<TDqCnMerge>()
                        .Output()
                            .Stage(dqUnion.Output().Stage())
                            .Index(dqUnion.Output().Index())
                            .Build()
                        .SortColumns(columnList)
                        .Build()
                    .Build()
                .Program(std::move(program))
                .Settings(NDq::TDqStageSettings::New().BuildNode(ctx, node.Pos()))
                .Build()
            .Index().Build(0U)
            .Build()
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

