#include "kqp_opt_phy_rules.h"
#include "kqp_opt_phy_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>

#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NDq;

// Temporary solution, should be replaced with constraints
// copy-past from old engine algo: https://a.yandex-team.ru/arc_vcs/yql/providers/kikimr/yql_kikimr_opt.cpp?rev=e592a5a9509952f1c29f1ec02343dd4c05fe426d#L122

using TTableData = std::pair<const NYql::TKikimrTableDescription*, NYql::TKqpReadTableSettings>;

TExprBase KqpRemoveRedundantSortByPk(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
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

    if (!IsSortKeyPrimary(keySelector, tableDesc, passthroughFields)) {
        return node;
    }

    if (direction == ESortDirection::Reverse) {
        if (!UseSource(kqpCtx, tableDesc) && kqpCtx.IsScanQuery()) {
            return node;
        }

        if (settings.Reverse) {
            return node;
        }

        settings.SetReverse();
        settings.SetSorted();

        input = BuildReadNode(input.Pos(), ctx, input, settings);
    } else if (direction == ESortDirection::Forward) {
        if (UseSource(kqpCtx, tableDesc)) {
            settings.SetSorted();
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
    IOptimizationContext& /* optCtx */, 
    TTypeAnnotationContext& typeCtx,
    const TParentsMap& parentsMap, 
    bool allowStageMultiUsage,
    bool ruleEnabled
) {
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

} // namespace NKikimr::NKqp::NOpt

