#include "kqp_opt_phy_rules.h"
#include "kqp_opt_phy_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>

#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

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

} // namespace NKikimr::NKqp::NOpt

