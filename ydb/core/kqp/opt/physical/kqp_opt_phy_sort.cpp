#include "kqp_opt_phy_rules.h"
#include "kqp_opt_phy_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>

#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

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

TExprBase KqpRemoveRedundantSortByPk(
    TExprBase node,
    TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx,
    const TTypeAnnotationContext& typeCtx
) {
    auto maybeSort = node.Maybe<TCoSort>();
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

TExprBase KqpBuildTopStageRemoveSort(
    TExprBase node,
    TExprContext& ctx,
    IOptimizationContext& /* optCtx */,
    TTypeAnnotationContext& typeCtx,
    const TParentsMap& parentsMap,
    bool allowStageMultiUsage,
    bool ruleEnabled
) {
    YQL_CLOG(TRACE, CoreDq) << "Kal1" << Endl;
    if (!ruleEnabled) {
        YQL_CLOG(TRACE, CoreDq) << "Kal123" << Endl;
        return node;
    }

    if (!node.Maybe<TCoTopBase>().Input().Maybe<TDqCnUnionAll>()) {
        YQL_CLOG(TRACE, CoreDq) << "kal23113" << Endl;
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

    if (!typeCtx.SortingsFSM) {
        return node;
    }

    auto inputStats = typeCtx.GetStats(dqUnion.Output().Raw());
    if (!inputStats) {
        YQL_CLOG(TRACE, CoreDq) << "No statistics for the TopBase, skip";
        return node;
    }

    auto topStats = typeCtx.GetStats(top.Raw());
    if (!topStats) {
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

    YQL_CLOG(TRACE, CoreDq) << "Statistics of the input of the top sort: " << inputStats->ToString();
    YQL_CLOG(TRACE, CoreDq) << "Statistics of the top sort: " << topStats->ToString();
    if (!inputStats->SortingOrderings.ContainsSorting(topStats->SortingOrderingIdx)) {
        return node;
    }

    const auto& keySelector = top.KeySelectorLambda();
    TVector<TString> sortKeys;
    if (auto body = keySelector.Body().Maybe<TCoMember>()) {
        sortKeys.push_back(body.Cast().Name().StringValue());
    } else if (auto body = keySelector.Body().Maybe<TExprList>()) {
        for (size_t i = 0; i < body.Cast().Size(); ++i) {
            auto item = body.Cast().Item(i);
            if (auto member = item.Maybe<TCoMember>()) {
                sortKeys.push_back(member.Cast().Name().StringValue());
            }
        }
    }

    auto builder = Build<TDqSortColumnList>(ctx, node.Pos());
    for (auto columnName : sortKeys ) {
        builder
            .Add<TDqSortColumn>()
                .Column<TCoAtom>()
            .Build(columnName)
                .SortDirection()
                .Build(TTopSortSettings::AscendingSort)
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
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

