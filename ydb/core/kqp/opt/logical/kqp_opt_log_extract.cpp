#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {
static constexpr size_t TKqlReadColumnsNodeIdx = 2;
static_assert(TKqlReadTableBase::idx_Columns == TKqlReadColumnsNodeIdx);
static_assert(TKqlLookupTableBase::idx_Columns == TKqlReadColumnsNodeIdx);
static_assert(TKqlReadTableRangesBase::idx_Columns == TKqlReadColumnsNodeIdx);
static_assert(TKqlReadTableFullTextIndex::idx_Columns == TKqlReadColumnsNodeIdx);


TMaybeNode<TCoAtomList> GetUsedColumns(TExprBase read, TCoAtomList columns, const TParentsMap& parentsMap,
    bool allowMultiUsage, TExprContext& ctx)
{
    TSet<TString> usedColumnsSet;

    auto consumers = GetConsumers(read, parentsMap);
    if (!allowMultiUsage && consumers.size() > 1) {
        return {};
    }

    for (const auto& consumer : consumers) {
        auto maybeExtractMembers = TMaybeNode<TCoExtractMembers>(consumer);
        if (!maybeExtractMembers) {
            return {};
        }

        auto columns = maybeExtractMembers.Cast().Members();
        for (const auto& column : columns) {
            usedColumnsSet.emplace(column);
        }
    }

    YQL_ENSURE(usedColumnsSet.size() <= columns.Size());

    if (usedColumnsSet.size() == columns.Size()) {
        return {};
    }

    TVector<TExprNode::TPtr> usedColumns;
    usedColumns.reserve(usedColumnsSet.size());
    for (const auto& column : usedColumnsSet) {
        usedColumns.emplace_back(ctx.NewAtom(columns.Pos(), column));
    }

    return Build<TCoAtomList>(ctx, columns.Pos())
        .Add(usedColumns)
        .Done();
}

} // namespace

TExprBase KqpApplyExtractMembersToReadTable(TExprBase node, TExprContext& ctx, const TParentsMap& parentsMap,
    bool allowMultiUsage)
{
    if (!node.Maybe<TKqlReadTableBase>() &&
        !node.Maybe<TKqlLookupTableBase>() &&
        !node.Maybe<TKqlReadTableRangesBase>() &&
        !node.Maybe<TKqlReadTableFullTextIndex>()
    ) {
        return node;
    }

    // TKqpReadOlapTableRangesBase is derived from TKqlReadTableRangesBase, but should be handled separately
    if (node.Maybe<TKqpReadOlapTableRangesBase>()) {
        return node;
    }

    auto slt = node.Maybe<TKqlStreamLookupTable>();
    if (slt && TKqpStreamLookupSettings::HasVectorTopColumn(slt.Cast())) {
        return node;
    }

    TCoAtomList columnsNode = TExprBase(node.Ptr()->Child(TKqlReadColumnsNodeIdx)).Cast<TCoAtomList>();
    auto usedColumns = GetUsedColumns(node, columnsNode, parentsMap, allowMultiUsage, ctx);
    if (!usedColumns) {
        return node;
    }

    return TExprBase(ctx.ChangeChild(*node.Raw(), TKqlReadColumnsNodeIdx, usedColumns.Cast().Ptr()));
}

TCoAtomList GetFirstColumn(const TCoAtomList &columns, TExprContext &ctx) {
    TVector<TExprNode::TPtr> memberColumns;
    Y_ENSURE(columns.Size());

    memberColumns.emplace_back(ctx.NewAtom(columns.Pos(), columns.Item(0).Value()));
    return Build<TCoAtomList>(ctx, columns.Pos())
            .Add(memberColumns)
            .Done();
}

TExprBase KqpApplyExtractMembersToReadOlapTable(TExprBase node, TExprContext& ctx, const TParentsMap& parentsMap,
    bool allowMultiUsage)
{
    if (!node.Maybe<TKqpReadOlapTableRangesBase>()) {
        return node;
    }

    auto read = node.Cast<TKqpReadOlapTableRangesBase>();
    if (read.Columns().Size() == 1) {
        return node;
    }

    auto usedColumns = GetUsedColumns(read, read.Columns(), parentsMap, allowMultiUsage, ctx);
    if (!usedColumns) {
        return node;
    }

    if (TExprBase(read.Process().Body()).Maybe<TKqpOlapExtractMembers>()) {
        return node;
    }

    if (read.Process().Body().Raw() != read.Process().Args().Arg(0).Raw()) {
        auto extractMembers = Build<TKqpOlapExtractMembers>(ctx, node.Pos())
            .Input(read.Process().Args().Arg(0))
            .Members(usedColumns.Cast().Size() ? usedColumns.Cast() : GetFirstColumn(read.Columns(), ctx))
            .Done();

        auto extractMembersLambda = Build<TCoLambda>(ctx, node.Pos())
            .Args({"row"})
            .Body<TExprApplier>()
                .Apply(extractMembers)
                .With(read.Process().Args().Arg(0), "row")
                .Build()
            .Done();

        auto newProcessLambda = ctx.FuseLambdas(extractMembersLambda.Ref(), read.Process().Ref());

        YQL_CLOG(INFO, ProviderKqp) << "Pushed ExtractMembers lambda: " << KqpExprToPrettyString(*newProcessLambda, ctx);

        return TExprBase(ctx.ChangeChild(*node.Raw(), TKqpReadOlapTableRangesBase::idx_Process, std::move(newProcessLambda)));
    } else {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TKqlReadColumnsNodeIdx, usedColumns.Cast().Ptr()));
    }
}

} // namespace NKikimr::NKqp::NOpt

