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

    enum : ui32 {
        SortDirectionNone = 0,
        SortDirectionForward = 1,
        SortDirectionReverse = 2,
        SortDirectionUnknown = 4,
    };

    auto getDirection = [] (TExprBase expr) -> ui32 {
        if (!expr.Maybe<TCoBool>()) {
            return SortDirectionUnknown;
        }

        if (!FromString<bool>(expr.Cast<TCoBool>().Literal().Value())) {
            return SortDirectionReverse;
        }

        return SortDirectionForward;
    };

    ui32 direction = SortDirectionNone;

    if (auto maybeList = sortDirections.Maybe<TExprList>()) {
        for (const auto& expr : maybeList.Cast()) {
            direction |= getDirection(expr);
            if (direction != SortDirectionForward && direction != SortDirectionReverse) {
                return node;
            }
        }
    } else {
        direction |= getDirection(sortDirections);
        if (direction != SortDirectionForward && direction != SortDirectionReverse) {
            return node;
        }
    }

    bool isReadTable = input.Maybe<TKqpReadTable>().IsValid();
    bool isReadTableRanges = input.Maybe<TKqpReadTableRanges>().IsValid() || input.Maybe<TKqpReadOlapTableRanges>().IsValid() ;
    if (!isReadTable && !isReadTableRanges) {
        return node;
    }
    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, GetReadTablePath(input, isReadTableRanges));
    auto settings = GetReadTableSettings(input, isReadTableRanges);

    auto checkKey = [keySelector, &tableDesc, &passthroughFields] (TExprBase key, ui32 index) {
        if (!key.Maybe<TCoMember>()) {
            return false;
        }

        auto member = key.Cast<TCoMember>();
        if (member.Struct().Raw() != keySelector.Args().Arg(0).Raw()) {
            return false;
        }

        auto column = TString(member.Name().Value());
        auto columnIndex = tableDesc.GetKeyColumnIndex(column);
        if (!columnIndex || *columnIndex != index) {
            return false;
        }

        if (passthroughFields && !passthroughFields->contains(column)) {
            return false;
        }

        return true;
    };

    auto lambdaBody = keySelector.Body();
    if (auto maybeTuple = lambdaBody.Maybe<TExprList>()) {
        auto tuple = maybeTuple.Cast();
        for (size_t i = 0; i < tuple.Size(); ++i) {
            if (!checkKey(tuple.Item(i), i)) {
                return node;
            }
        }
    } else {
        if (!checkKey(lambdaBody, 0)) {
            return node;
        }
    }

    bool olapTable = tableDesc.Metadata->Kind == EKikimrTableKind::Olap;
    if (direction == SortDirectionReverse) {
        if (!UseSource(kqpCtx, tableDesc) && !olapTable && kqpCtx.IsScanQuery()) {
            return node;
        }

        if (settings.Reverse) {
            return node;
        }

        settings.SetReverse();
        settings.SetSorted();

        input = BuildReadNode(input.Pos(), ctx, input, settings);
    } else if (direction == SortDirectionForward) {
        if (olapTable || UseSource(kqpCtx, tableDesc)) {
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

