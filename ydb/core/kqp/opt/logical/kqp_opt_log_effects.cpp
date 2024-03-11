#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace {

bool CanPushFlatMap(const NYql::NNodes::TCoFlatMapBase& flatMap, const NYql::TKikimrTableDescription& tableDesc, const NYql::TParentsMap& parentsMap, TVector<TString> & extraColumns) {
    auto flatMapLambda = flatMap.Lambda();
    if (!NYql::IsFilterFlatMap(flatMapLambda)) {
        return false;
    }

    const auto & flatMapLambdaArgument = flatMapLambda.Args().Arg(0).Ref();
    auto flatMapLambdaConditional = flatMapLambda.Body().Cast<NYql::NNodes::TCoConditionalValueBase>();

    TSet<TString> lambdaSubset;
    auto isSubSet = HaveFieldsSubset(flatMapLambdaConditional.Predicate().Ptr(), flatMapLambdaArgument, lambdaSubset, parentsMap, true);
    auto argType = NYql::RemoveOptionalType(flatMapLambdaArgument.GetTypeAnn());
    if (argType->GetKind() != NYql::ETypeAnnotationKind::Struct) {
        return false;
    }
    // helper doesn't accept if all columns are used
    if (!isSubSet && lambdaSubset.size() != argType->Cast<NYql::TStructExprType>()->GetSize()) {
        return false;
    }

    for (auto & lambdaColumn : lambdaSubset) {
        auto columnIndex = tableDesc.GetKeyColumnIndex(lambdaColumn);
        if (!columnIndex) {
            return false;
        }
    }

    extraColumns.insert(extraColumns.end(), lambdaSubset.begin(), lambdaSubset.end());
    return true;
}

}

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

TExprBase KqpDeleteOverLookup(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext &kqpCtx, const NYql::TParentsMap& parentsMap) {
    if (!node.Maybe<TKqlDeleteRows>()) {
        return node;
    }

    auto deleteRows = node.Cast<TKqlDeleteRows>();

    TMaybeNode<TCoFlatMap> filter;

    TMaybeNode<TKqlLookupTableBase> lookup;
    TMaybeNode<TKqlReadTable> read;
    TMaybeNode<TCoSkipNullMembers> skipNulMembers;
    TMaybeNode<TKqlReadTableRanges> readranges;

    if (deleteRows.Input().Maybe<TKqlLookupTableBase>()) {
        lookup = deleteRows.Input().Cast<TKqlLookupTableBase>();
    } else if (deleteRows.Input().Maybe<TCoSkipNullMembers>().Input().Maybe<TKqlLookupTableBase>()) {
        skipNulMembers = deleteRows.Input().Cast<TCoSkipNullMembers>();
        lookup = skipNulMembers.Input().Cast<TKqlLookupTableBase>();
    } else if (deleteRows.Input().Maybe<TKqlReadTable>()) {
        read = deleteRows.Input().Cast<TKqlReadTable>();
    } else {
        TMaybeNode<TExprBase> input = deleteRows.Input();
        if (input.Maybe<TCoFlatMap>()) {
            filter = deleteRows.Input().Cast<TCoFlatMap>();
            input = filter.Input();
        }
        readranges = input.Maybe<TKqlReadTableRanges>();
        if (!readranges) {
            return node;
        }
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
    } else if (readranges) {
        if (deleteRows.Table().Raw() != readranges.Cast().Table().Raw()) {
            return node;
        }

        if (!readranges.Cast().PrefixPointsExpr()) {
            return node;
        }

        const auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, readranges.Cast().Table().Path().Value());
        auto hint = TKqpReadTableExplainPrompt::Parse(readranges.Cast().ExplainPrompt());
        if (hint.PointPrefixLen != tableDesc.Metadata->KeyColumnNames.size()) {
            return node;
        }

        if (filter) {
            TVector<TString> extraColumns;
            if (!CanPushFlatMap(filter.Cast(), tableDesc, parentsMap, extraColumns)) {
                return node;
            }
            deleteInput = Build<TCoFlatMap>(ctx, node.Pos())
                .Lambda(filter.Lambda().Cast())
                .Input(readranges.PrefixPointsExpr().Cast())
                .Done();
        } else {
            deleteInput = readranges.PrefixPointsExpr();
        }
    }

    YQL_ENSURE(deleteInput);

    return Build<TKqlDeleteRows>(ctx, deleteRows.Pos())
        .Table(deleteRows.Table())
        .Input(deleteInput.Cast())
        .ReturningColumns(deleteRows.ReturningColumns())
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
