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
    TMaybeNode<TKqlReadTable> read;
    TMaybeNode<TCoSkipNullMembers> skipNulMembers;

    if (deleteRows.Input().Maybe<TKqlLookupTableBase>()) {
        lookup = deleteRows.Input().Cast<TKqlLookupTableBase>();
    } else if (deleteRows.Input().Maybe<TCoSkipNullMembers>().Input().Maybe<TKqlLookupTableBase>()) {
        skipNulMembers = deleteRows.Input().Cast<TCoSkipNullMembers>();
        lookup = skipNulMembers.Input().Cast<TKqlLookupTableBase>();
    } else if (deleteRows.Input().Maybe<TKqlReadTable>()) {
        read = deleteRows.Input().Cast<TKqlReadTable>();
    } else {
        return node;
    }

    TMaybeNode<TExprBase> deleteInput;
    const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, deleteRows.Table().Path());
    if (lookup) {
        if (deleteRows.Table().Raw() != lookup.Cast().Table().Raw()) {
            return node;
        }

        auto lookupKeysType = lookup.Cast().LookupKeys().Ref().GetTypeAnn();
        auto lookupKeyType = lookupKeysType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        YQL_ENSURE(lookupKeyType);

        // Only consider complete PK lookups
        if (lookupKeyType->GetSize() != tableDesc.Metadata->KeyColumnNames.size()) {
            return node;
        }

        deleteInput = lookup.Cast().LookupKeys();
        if (skipNulMembers) {
            deleteInput = Build<TCoSkipNullMembers>(ctx, skipNulMembers.Cast().Pos())
                .Input(deleteInput.Cast())
                .Members(skipNulMembers.Cast().Members())
                .Done();
        }
    } else if (read) {
        if (deleteRows.Table().Raw() != read.Cast().Table().Raw()) {
            return node;
        }

        const auto& rangeFrom = read.Cast().Range().From();
        const auto& rangeTo = read.Cast().Range().To();

        if (!rangeFrom.Maybe<TKqlKeyInc>() || !rangeTo.Maybe<TKqlKeyInc>()) {
            return node;
        }

        if (rangeFrom.Raw() != rangeTo.Raw()) {
            return node;
        }

        if (rangeFrom.ArgCount() != tableDesc.Metadata->KeyColumnNames.size()) {
            return node;
        }

        TVector<TExprBase> structMembers;
        for (ui32 i = 0; i < rangeFrom.ArgCount(); ++i) {
            TCoAtom columnNameAtom(ctx.NewAtom(node.Pos(), tableDesc.Metadata->KeyColumnNames[i]));

            auto member = Build<TCoNameValueTuple>(ctx, node.Pos())
                .Name(columnNameAtom)
                .Value(rangeFrom.Arg(i))
                .Done();

            structMembers.push_back(member);
        }

         deleteInput = Build<TCoAsList>(ctx, node.Pos())
            .Add<TCoAsStruct>()
                .Add(structMembers)
                .Build()
            .Done();
    } 

    YQL_ENSURE(deleteInput);

    return Build<TKqlDeleteRows>(ctx, deleteRows.Pos())
        .Table(deleteRows.Table())
        .Input(deleteInput.Cast())
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
