#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/common/kqp_yql.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

TExprBase KqpDeleteOverLookup(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext &kqpCtx) {
    if (!node.Maybe<TKqlDeleteRows>()) {
        return node;
    }

    auto deleteRows = node.Cast<TKqlDeleteRows>();

    TMaybeNode<TKqlLookupTableBase> lookup;
    TMaybeNode<TCoSkipNullMembers> skipNulMembers;

    if (deleteRows.Input().Maybe<TKqlLookupTableBase>()) {
        lookup = deleteRows.Input().Cast<TKqlLookupTableBase>();
    } else if (deleteRows.Input().Maybe<TCoSkipNullMembers>().Input().Maybe<TKqlLookupTableBase>()) {
        skipNulMembers = deleteRows.Input().Cast<TCoSkipNullMembers>();
        lookup = skipNulMembers.Input().Cast<TKqlLookupTableBase>();
    } else {
        return node;
    }

    YQL_ENSURE(lookup);
    if (deleteRows.Table().Raw() != lookup.Cast().Table().Raw()) {
        return node;
    }

    auto lookupKeysType = lookup.Cast().LookupKeys().Ref().GetTypeAnn();
    auto lookupKeyType = lookupKeysType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    YQL_ENSURE(lookupKeyType);

    // Only consider complete PK lookups
    const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, deleteRows.Table().Path());
    if (lookupKeyType->GetSize() != tableDesc.Metadata->KeyColumnNames.size()) {
        return node;
    }

    TExprBase deleteInput = lookup.Cast().LookupKeys();
    if (skipNulMembers) {
        deleteInput = Build<TCoSkipNullMembers>(ctx, skipNulMembers.Cast().Pos())
            .Input(deleteInput)
            .Members(skipNulMembers.Cast().Members())
            .Done();
    }

    return Build<TKqlDeleteRows>(ctx, deleteRows.Pos())
        .Table(deleteRows.Table())
        .Input(deleteInput)
        .Done();
}

TExprBase KqpExcessUpsertInputColumns(const TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TKqlUpsertRows>() &&
        !node.Maybe<TKqlUpsertRowsIndex>())
    {
        return node;
    }

    auto upsertRows = node.Cast<TKqlUpsertRowsBase>();
    auto inputItemType = upsertRows.Input().Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
    auto inputRowType = inputItemType->Cast<TStructExprType>();

    if (inputRowType->GetItems().size() <= upsertRows.Columns().Size()) {
        return node;
    }

    auto newInput = Build<TCoExtractMembers>(ctx, node.Pos())
        .Input(upsertRows.Input())
        .Members(upsertRows.Columns())
        .Done();

    auto newUpsert = ctx.ChangeChild(upsertRows.Ref(), TKqlUpsertRowsBase::idx_Input, newInput.Ptr());
    return TExprBase(newUpsert);
}

} // namespace NKikimr::NKqp::NOpt
