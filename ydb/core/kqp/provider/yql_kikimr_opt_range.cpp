#include "yql_kikimr_provider_impl.h"
#include "yql_kikimr_opt_utils.h"

#include <ydb/library/yql/core/common_opt/yql_co_sqlin.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/utf8.h>

namespace NYql {
namespace {

using namespace NNodes;
using namespace NCommon;

struct TTakeNode {
    const TExprBase Input;
    const TExprBase Count;
    const bool IsPartial;
};

TTakeNode GetTakeChildren(TExprBase node) {
    if (auto maybeCoTake = node.Maybe<TCoTake>()) {
        auto coTake = maybeCoTake.Cast();
        return TTakeNode {coTake.Input(), coTake.Count(), false};
    } else {
        auto kiPartialTake = node.Maybe<TKiPartialTake>().Cast();
        return TTakeNode {kiPartialTake.Input(), kiPartialTake.Count(), true};
    }
}

template<typename T>
TTableLookup::TCompareResult::TResult CompareValues(const T& left, const T& right) {
    if (left == right) {
        return TTableLookup::TCompareResult::Equal;
    } else {
        return left > right
            ? TTableLookup::TCompareResult::Greater
            : TTableLookup::TCompareResult::Less;
    }
}

template<typename T>
TTableLookup::TCompareResult CompareIntegralNodes(TCoAtom left, TCoAtom right, NKikimr::NUdf::EDataSlot slot) {
    T leftValue = FromString<T>(left.Ref(), slot);
    T rightValue = FromString<T>(right.Ref(), slot);
    auto compareResult = CompareValues(leftValue, rightValue);

    TMaybe<bool> adjacent;
    switch (compareResult) {
        case TTableLookup::TCompareResult::Equal:
            break;

        case TTableLookup::TCompareResult::Greater:
            adjacent = leftValue == rightValue + 1;
            break;

        case TTableLookup::TCompareResult::Less:
            adjacent = rightValue == leftValue + 1;
            break;
    }

    return TTableLookup::TCompareResult(compareResult, adjacent);
}

template<typename T>
TTableLookup::TCompareResult CompareNodes(TCoAtom left, TCoAtom right, NKikimr::NUdf::EDataSlot slot) {
    T leftValue = FromString<T>(left.Ref(), slot);
    T rightValue = FromString<T>(right.Ref(), slot);
    return TTableLookup::TCompareResult(CompareValues(leftValue, rightValue));
}

template<>
TTableLookup::TCompareResult CompareNodes<bool>(TCoAtom left, TCoAtom right, NKikimr::NUdf::EDataSlot slot) {
    bool leftValue = FromString<bool>(left.Ref(), slot);
    bool rightValue = FromString<bool>(right.Ref(), slot);
    auto compareResult = CompareValues(leftValue, rightValue);

    return TTableLookup::TCompareResult(compareResult);
}

template<>
TTableLookup::TCompareResult CompareNodes<ui64>(TCoAtom left, TCoAtom right, NKikimr::NUdf::EDataSlot slot) {
    return CompareIntegralNodes<ui64>(left, right, slot);
}

template<>
TTableLookup::TCompareResult CompareNodes<i64>(TCoAtom left, TCoAtom right, NKikimr::NUdf::EDataSlot slot) {
    return CompareIntegralNodes<i64>(left, right, slot);
}

template<>
TTableLookup::TCompareResult CompareNodes<TString>(TCoAtom left, TCoAtom right, NKikimr::NUdf::EDataSlot slot) {
    Y_UNUSED(slot);

    const auto& leftValue = left.Value();
    const auto& rightValue = right.Value();
    return TTableLookup::TCompareResult(CompareValues(leftValue, rightValue));
}

} // namespace

bool KiTableLookupCanCompare(TExprBase node) {
    if (node.Maybe<TCoBool>()) {
        return true;
    }

    if (node.Maybe<TCoIntegralCtor>()) {
        return true;
    }

    if (node.Maybe<TCoString>()) {
        return true;
    }

    if (node.Maybe<TCoUtf8>()) {
        return true;
    }

    return false;
}

TMaybeNode<TExprBase> KiTableLookupGetValue(TExprBase node, const TTypeAnnotationNode* type,
    TExprContext& ctx)
{
    const TTypeAnnotationNode* targetType = type;
    bool isTargetOptional = false;
    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        targetType = type->Cast<TOptionalExprType>()->GetItemType();
        isTargetOptional = true;
    }

    if (targetType->GetKind() != ETypeAnnotationKind::Data) {
        return TMaybeNode<TExprBase>();
    }

    THashSet<const TExprNode*> knownArgs;
    bool canPush = true;
    VisitExpr(node.Ptr(), [&knownArgs, &canPush] (const TExprNode::TPtr& exprNode) {
        auto node = TExprBase(exprNode);

        if (!canPush) {
            return false;
        }

        if (auto maybeLambda = node.Maybe<TCoLambda>()) {
            for (const auto& arg : maybeLambda.Cast().Args()) {
                knownArgs.emplace(arg.Raw());
            }
        }

        if (auto maybeArg = node.Maybe<TCoArgument>()) {
            if (!knownArgs.contains(maybeArg.Cast().Raw())) {
                canPush = false;
                return false;
            }
        }

        if (auto maybeCallable = node.Maybe<TCallable>()) {
            auto callable = maybeCallable.Cast();

            if (KikimrKqlFunctions().contains(callable.CallableName())) {
                if (callable.Maybe<TKiSelectRow>()) {
                    return true;
                }

                if (callable.Maybe<TKiSelectRange>()) {
                    return true;
                }

                canPush = false;
                return false;
            }
        }

        return true;
    });

    if (!canPush) {
        return TMaybeNode<TExprBase>();
    }

    const auto& dataTypeName = targetType->Cast<TDataExprType>()->GetName();

    TExprBase valueNode = node;
    if (isTargetOptional) {
        if (auto maybeJust = node.Maybe<TCoJust>()) {
            valueNode = maybeJust.Cast().Input();
        }

        if (node.Maybe<TCoNothing>()) {
            return Build<TCoNothing>(ctx, node.Pos())
                .OptionalType(ExpandType(node.Pos(), *type, ctx))
                .Done()
                .Ptr();
        }
    }

    TExprNode::TPtr literal;
    if (auto maybeInt = valueNode.Maybe<TCoIntegralCtor>()) {
        if (maybeInt.Cast().CallableName() == dataTypeName) {
            return valueNode;
        }

        if (AllowIntegralConversion(maybeInt.Cast(), false, NKikimr::NUdf::GetDataSlot(dataTypeName))) {
            literal = maybeInt.Cast().Literal().Ptr();
        }
    }

    if (auto maybeString = valueNode.Maybe<TCoString>()) {
        if (dataTypeName == "String") {
            return valueNode;
        }

        if (dataTypeName == "Utf8") {
            auto atom = maybeString.Cast().Literal();
            auto value = atom.Value();
            if (!IsUtf8(value)) {
                return {};
            }

            literal = atom.Ptr();
        }
    }

    if (auto maybeUtf8 = valueNode.Maybe<TCoUtf8>()) {
        if (dataTypeName == "String" || dataTypeName == "Utf8") {
            literal = maybeUtf8.Cast().Literal().Ptr();
        }
    }

    if (auto maybeBool = valueNode.Maybe<TCoBool>()) {
        if (dataTypeName == "Bool") {
            literal = maybeBool.Cast().Literal().Ptr();
        }
    }

    if (literal) {
        auto ret = ctx.Builder(valueNode.Pos())
            .Callable(dataTypeName)
                .Add(0, literal)
                .Seal()
            .Build();

        return ret;
    }

    auto valueType = valueNode.Ref().GetTypeAnn();
    if (isTargetOptional && valueType->GetKind() == ETypeAnnotationKind::Optional) {
        valueType = valueType->Cast<TOptionalExprType>()->GetItemType();
    }

    if (valueType->GetKind() == ETypeAnnotationKind::Data &&
        valueType->Cast<TDataExprType>()->GetName() == dataTypeName)
    {
        return node;
    }

    return Build<TCoConvert>(ctx, node.Pos())
        .Input(node)
        .Type().Build(dataTypeName)
        .Done();
}

TTableLookup::TCompareResult KiTableLookupCompare(TExprBase left, TExprBase right) {
    if (left.Maybe<TCoBool>() && right.Maybe<TCoBool>()) {
        return CompareNodes<bool>(left.Cast<TCoBool>().Literal(),
            right.Cast<TCoBool>().Literal(), NKikimr::NUdf::EDataSlot::Bool);
    }

    if (left.Maybe<TCoUint64>() && right.Maybe<TCoUint64>()) {
        return CompareNodes<ui64>(left.Cast<TCoUint64>().Literal(),
            right.Cast<TCoUint64>().Literal(), NKikimr::NUdf::EDataSlot::Uint64);
    }

    if (left.Maybe<TCoIntegralCtor>() && right.Maybe<TCoIntegralCtor>()) {
        return CompareNodes<i64>(left.Cast<TCoIntegralCtor>().Literal(),
            right.Cast<TCoIntegralCtor>().Literal(), NKikimr::NUdf::EDataSlot::Int64);
    }

    if (left.Maybe<TCoString>() && right.Maybe<TCoString>() ||
        left.Maybe<TCoUtf8>() && right.Maybe<TCoUtf8>())
    {
        return CompareNodes<TString>(left.Cast<TCoDataCtor>().Literal(),
            right.Cast<TCoDataCtor>().Literal(), NKikimr::NUdf::EDataSlot::String);
    }

    YQL_ENSURE(false, "Unexpected nodes in Kikimr TableLookupCompare: (" << left.Ref().Content()
        << ", " << right.Ref().Content() << ")");
}


TExprNode::TPtr KiApplyLimitToSelectRange(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoTake>() && !node.Maybe<TKiPartialTake>()) {
        return node.Ptr();
    }

    auto takeNode = GetTakeChildren(node);

    if (!takeNode.Input.Maybe<TCoSkip>().Input().Maybe<TKiSelectRangeBase>() &&
        !takeNode.Input.Maybe<TKiSelectRangeBase>())
    {
        return node.Ptr();
    }

    auto maybeSkip = takeNode.Input.Maybe<TCoSkip>();
    TMaybeNode<TExprBase> limitValue;

    if (auto maybeTakeCount = takeNode.Count.Maybe<TCoUint64>()) {
        ui64 totalLimit;
        auto takeValue = FromString<ui64>(maybeTakeCount.Cast().Literal().Value());

        if (maybeSkip) {
            if (auto maybeSkipCount = maybeSkip.Count().Maybe<TCoUint64>()) {
                auto skipValue = FromString<ui64>(maybeSkipCount.Cast().Literal().Value());
                totalLimit = takeValue + skipValue;
            } else {
                return node.Ptr();
            }
        } else {
            totalLimit = takeValue;
        }

        limitValue = Build<TCoUint64>(ctx, node.Pos())
            .Literal<TCoAtom>()
                .Value(ToString(totalLimit))
                .Build()
            .Done();
    } else {
        limitValue = takeNode.Count;
        if (maybeSkip) {
            limitValue = Build<TCoPlus>(ctx, node.Pos())
                .Left(limitValue.Cast())
                .Right(maybeSkip.Cast().Count())
                .Done();
        }
    }

    YQL_ENSURE(limitValue);

    auto input = maybeSkip
        ? takeNode.Input.Cast<TCoSkip>().Input()
        : takeNode.Input;

    bool isIndexRange = input.Maybe<TKiSelectIndexRange>().IsValid();

    auto select = input.Cast<TKiSelectRangeBase>();

    if (HasSetting(select.Settings().Ref(), "ItemsLimit")) {
        return node.Ptr();
    }

    auto newSettings = Build<TCoNameValueTupleList>(ctx, select.Settings().Pos())
        .Add(TVector<TExprBase>(select.Settings().begin(), select.Settings().end()))
        .Add<TCoNameValueTuple>()
            .Name().Build("ItemsLimit")
            .Value(limitValue)
            .Build()
        .Done();

    TExprNode::TPtr newSelect;
    if (isIndexRange) {
        newSelect = Build<TKiSelectIndexRange>(ctx, select.Pos())
            .Cluster(select.Cluster())
            .Table(select.Table())
            .Range(select.Range())
            .Select(select.Select())
            .IndexName(input.Cast<TKiSelectIndexRange>().IndexName())
            .Settings(newSettings)
            .Done()
            .Ptr();
    } else {
        newSelect = Build<TKiSelectRange>(ctx, select.Pos())
            .Cluster(select.Cluster())
            .Table(select.Table())
            .Range(select.Range())
            .Select(select.Select())
            .Settings(newSettings)
            .Done()
            .Ptr();
    }

    YQL_CLOG(INFO, ProviderKikimr) << "KiApplyLimitToSelectRange";

    if (maybeSkip) {
        if (takeNode.IsPartial) {
            return Build<TKiPartialTake>(ctx, node.Pos())
                .Input<TCoSkip>()
                    .Input(newSelect)
                    .Count(maybeSkip.Cast().Count())
                    .Build()
                .Count(takeNode.Count)
                .Done().Ptr();
        } else {
            return Build<TCoTake>(ctx, node.Pos())
                .Input<TCoSkip>()
                    .Input(newSelect)
                    .Count(maybeSkip.Cast().Count())
                    .Build()
                .Count(takeNode.Count)
                .Done().Ptr();
        }
    } else {
        if (takeNode.IsPartial) {
            return Build<TKiPartialTake>(ctx, node.Pos())
                .Input(newSelect)
                .Count(takeNode.Count)
                .Done().Ptr();

        } else {
            return Build<TCoTake>(ctx, node.Pos())
                .Input(newSelect)
                .Count(takeNode.Count)
                .Done().Ptr();
        }
    }
}

TExprNode::TPtr BuildFallbackSelectRange(TCoFlatMap oldFlatMap,
    TKiSelectRangeBase select, TMaybeNode<TCoFilterNullMembers> filterNull,
    TMaybeNode<TCoSkipNullMembers> skipNull, const TKikimrTableDescription& tableDesc,
    TExprContext& ctx)
{
    TKikimrKeyRange range(ctx, tableDesc);
    TExprNode::TPtr input;

    input = Build<TKiSelectRange>(ctx, select.Pos())
        .Cluster(select.Cluster())
        .Table(select.Table())
        .Range(range.ToRangeExpr(select, ctx))
        .Select(select.Select())
        .Settings(select.Settings())
        .Done()
        .Ptr();

    if (filterNull) {
        input = Build<TCoFilterNullMembers>(ctx, oldFlatMap.Pos())
            .Input(input)
            .Members(filterNull.Cast().Members())
            .Done()
            .Ptr();
    }

    if (skipNull) {
        input = Build<TCoSkipNullMembers>(ctx, oldFlatMap.Pos())
            .Input(input)
            .Members(skipNull.Cast().Members())
            .Done()
            .Ptr();
    }

    return Build<TCoFlatMap>(ctx, oldFlatMap.Pos())
        .Input(input)
        .Lambda(oldFlatMap.Lambda())
        .Done()
        .Ptr();
}

TExprNode::TPtr KiPushPredicateToSelectRange(TExprBase node, TExprContext& ctx,
    const TKikimrTablesData& tablesData, const TKikimrConfiguration& config)
{
    if (!node.Maybe<TCoFlatMap>()) {
        return node.Ptr();
    }
    auto flatmap = node.Cast<TCoFlatMap>();

    if (!IsPredicateFlatMap(flatmap.Lambda().Body().Ref())) {
        return node.Ptr();
    }

    TMaybeNode<TKiSelectRangeBase> select;
    TMaybeNode<TCoFilterNullMembers> filterNull;
    TMaybeNode<TCoSkipNullMembers> skipNull;
    TMaybeNode<TCoAtom> indexTable;

    if (auto maybeRange = flatmap.Input().Maybe<TKiSelectRangeBase>()) {
        select = maybeRange.Cast();
    }

    if (auto maybeRange = flatmap.Input().Maybe<TCoFilterNullMembers>().Input().Maybe<TKiSelectRangeBase>()) {
        select = maybeRange.Cast();
        filterNull = flatmap.Input().Cast<TCoFilterNullMembers>();
    }

    if (auto maybeRange = flatmap.Input().Maybe<TCoSkipNullMembers>().Input().Maybe<TKiSelectRangeBase>()) {
        select = maybeRange.Cast();
        skipNull = flatmap.Input().Cast<TCoSkipNullMembers>();
    }

    if (!select) {
        return node.Ptr();
    }

    if (auto indexSelect = select.Maybe<TKiSelectIndexRange>()) {
        indexTable = indexSelect.Cast().IndexName();
    }

    auto selectRange = select.Cast();
    const auto& cluster = TString(selectRange.Cluster());

    const auto& lookupTableName = indexTable ? TString(indexTable.Cast()) : TString(selectRange.Table().Path());
    auto& lookupTableDesc = tablesData.ExistingTable(cluster, lookupTableName);

    if (!TKikimrKeyRange::IsFull(selectRange.Range())) {
        return node.Ptr();
    }

    if (indexTable) {
        bool needDataRead = false;
        for (const auto& col : selectRange.Select()) {
            if (!lookupTableDesc.Metadata->Columns.contains(TString(col.Value()))) {
                needDataRead = true;
                break;
            }
        }

        if (!needDataRead) {
            // All selected data present in index table
            // apply this optimizer again after TKiSelectIndexRange -> TKiSelectRange rewriting
            return node.Ptr();
        }
    }

    auto row = flatmap.Lambda().Args().Arg(0);
    auto predicate = TExprBase(flatmap.Lambda().Body().Ref().ChildPtr(0));
    TTableLookup lookup = ExtractTableLookup(row, predicate, lookupTableDesc.Metadata->KeyColumnNames,
        &KiTableLookupGetValue, &KiTableLookupCanCompare, &KiTableLookupCompare, ctx,
        config.HasAllowNullCompareInIndex());

    auto& dataTableDesc = tablesData.ExistingTable(cluster, TString(selectRange.Table().Path()));

    if (lookup.IsFullScan()) {
        // WARNING: Sometimes we want to check index table usage here.
        // Ok, but we must be aware about other optimizations.
        // Example SqlIn using secondary index rewriten to EquiJoin on
        // KiSelectIndexRange. The problem is if predicate contains also
        // "AND non_index_fiels" clause
        // (WHERE secondary_key IN (...) AND non_index == )
        // check for lookup.IsFullScan() returns true here because
        // EquiJoin optimization has bot been made yet.
        // See KIKIMR-11041 for more examples and consider KiRewriteSelectIndexRange method

        return node.Ptr();
    }

    TVector<TExprBase> fetches;

    for (auto& keyRange : lookup.GetKeyRanges()) {
        TExprBase predicate = keyRange.GetResidualPredicate()
            ? keyRange.GetResidualPredicate().Cast()
            : Build<TCoBool>(ctx, node.Pos())
                .Literal().Build("true")
                .Done();

        auto newBody = ctx.ChangeChild(flatmap.Lambda().Body().Ref(), 0, predicate.Ptr());

        TExprNode::TPtr input;

        if (indexTable) {
            if (!keyRange.IsEquiRange()) {
                input = TKikimrKeyRange::BuildIndexReadRangeExpr(lookupTableDesc, keyRange, selectRange.Select(),
                    config.HasAllowNullCompareInIndex(), dataTableDesc, ctx).Ptr();
            } else {
                TCoAtomList columnsToSelect = BuildKeyColumnsList(dataTableDesc, node.Pos(), ctx);

                input = TKikimrKeyRange::BuildReadRangeExpr(lookupTableDesc, keyRange, columnsToSelect,
                           config.HasAllowNullCompareInIndex(), ctx).Ptr();
                const auto& fetchItemArg = Build<TCoArgument>(ctx, node.Pos())
                .Name("fetchItem")
                .Done();

                input = Build<TCoFlatMap>(ctx, node.Pos())
                    .Input(input)
                    .Lambda()
                        .Args(fetchItemArg)
                        .Body<TKiSelectRow>()
                            .Cluster(selectRange.Cluster())
                            .Table(selectRange.Table())
                            .Key(ExtractNamedKeyTuples(fetchItemArg, dataTableDesc, ctx))
                            .Select(selectRange.Select())
                        .Build()
                    .Build()
                    .Done()
                    .Ptr();
            }
        } else {
            input = TKikimrKeyRange::BuildReadRangeExpr(lookupTableDesc, keyRange, selectRange.Select(),
                       config.HasAllowNullCompareInIndex(), ctx).Ptr();
        }

        if (filterNull) {
            input = Build<TCoFilterNullMembers>(ctx, node.Pos())
                .Input(input)
                .Members(filterNull.Cast().Members())
                .Done()
                .Ptr();
        }

        if (skipNull) {
            input = Build<TCoSkipNullMembers>(ctx, node.Pos())
                .Input(input)
                .Members(skipNull.Cast().Members())
                .Done()
                .Ptr();
        }

        auto fetch = Build<TCoFlatMap>(ctx, node.Pos())
            .Input(input)
            .Lambda()
                .Args({"item"})
                .Body<TExprApplier>()
                    .Apply(TExprBase(newBody))
                    .With(flatmap.Lambda().Args().Arg(0), "item")
                    .Build()
                .Build()
            .Done();

        fetches.push_back(fetch);
    }

    YQL_CLOG(INFO, ProviderKikimr) << "KiPushPredicateToSelectRange";
    return Build<TCoExtend>(ctx, node.Pos())
        .Add(fetches)
        .Done()
        .Ptr();
}

TExprNode::TPtr KiApplyExtractMembersToSelectRow(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoExtractMembers>().Input().Maybe<TKiSelectRow>()) {
        return node.Ptr();
    }

    auto extract = node.Cast<TCoExtractMembers>();
    auto selectRow = extract.Input().Cast<TKiSelectRow>();
    auto input = extract.Input();

    YQL_CLOG(INFO, ProviderKikimr) << "KiApplyExtractMembersToSelectRow";
    return Build<TKiSelectRow>(ctx, node.Pos())
        .Cluster(selectRow.Cluster())
        .Table(selectRow.Table())
        .Key(selectRow.Key())
        .Select(extract.Members())
        .Done().Ptr();
}

TKikimrKeyRange::TKikimrKeyRange(TExprContext& ctx, const TKikimrTableDescription& table)
    : Table(table)
    , KeyRange(ctx, Table.Metadata->KeyColumnNames.size(), TMaybeNode<TExprBase>())
{}

TKikimrKeyRange::TKikimrKeyRange(const TKikimrTableDescription& table, const TKeyRange& keyRange)
    : Table(table)
    , KeyRange(keyRange)
{
    YQL_ENSURE(Table.Metadata->KeyColumnNames.size() == KeyRange.GetColumnRangesCount());
}

bool TKikimrKeyRange::IsFull(TExprList list) {
    for (auto node : list) {
        if (auto maybeColumnRange = node.Maybe<TKiColumnRangeTuple>()) {
            auto columnRange = maybeColumnRange.Cast();

            if (!columnRange.From().Maybe<TCoNothing>()) {
                return false;
            }

            if (!columnRange.To().Maybe<TCoVoid>()) {
                return false;
            }
        }

        if (auto maybeAtom = node.Maybe<TCoAtom>()) {
            auto atom = maybeAtom.Cast();

            if (atom.Value() != "IncFrom" && atom.Value() != "IncTo") {
                return false;
            }
        }
    }

    return true;
}

TMaybe<TKeyRange> TKikimrKeyRange::GetPointKeyRange(TExprContext& ctx, const TKikimrTableDescription& table, TExprList range) {
    size_t keyColumnsCount = table.Metadata->KeyColumnNames.size();
    TVector<TMaybeNode<TExprBase>> fromValues(keyColumnsCount);
    TVector<TMaybeNode<TExprBase>> toValues(keyColumnsCount);
    bool incFrom = false;
    bool incTo = false;

    for (auto node : range) {
        if (auto maybeColumnRange = node.Maybe<TKiColumnRangeTuple>()) {
            auto columnRange = maybeColumnRange.Cast();

            auto idx = table.GetKeyColumnIndex(TString(columnRange.Column()));
            YQL_ENSURE(idx);

            fromValues[*idx] = columnRange.From();
            toValues[*idx] = columnRange.To();
        }

        if (auto maybeAtom = node.Maybe<TCoAtom>()) {
            auto value = maybeAtom.Cast().Value();

            if (value == "IncFrom") {
                incFrom = true;
            }

            if (value == "IncTo") {
                incTo = true;
            }
        }
    }

    if (!incFrom || !incTo) {
        return {};
    }

    TVector<TColumnRange> columnRanges(keyColumnsCount);
    for (size_t i = 0; i < keyColumnsCount; ++i) {
        YQL_ENSURE(fromValues[i]);
        YQL_ENSURE(toValues[i]);

        if (!fromValues[i].Maybe<TCoVoid>() && !toValues[i].Maybe<TCoVoid>()) {
            if (fromValues[i].Cast().Raw() != toValues[i].Cast().Raw()) {
                return {};
            }

            columnRanges[i] = TColumnRange::MakePoint(fromValues[i].Cast());
            YQL_ENSURE(i == 0 || columnRanges[i - 1].IsPoint());
        } else {
            if (!fromValues[i].Maybe<TCoNothing>() || !toValues[i].Maybe<TCoVoid>()) {
                return {};
            }
        }
    }

    return TKeyRange(ctx, columnRanges, {});
}

TExprList TKikimrKeyRange::ToRangeExpr(TExprBase owner, TExprContext& ctx) {
    auto pInf = [owner, &ctx] () -> TExprBase {
        return Build<TCoVoid>(ctx, owner.Pos()).Done();
    };

    auto nInf = [owner, &ctx] (const TTypeAnnotationNode* type) -> TExprBase {
        /* required optional type for TCoNothing */
        if (type->GetKind() != ETypeAnnotationKind::Optional) {
            type = ctx.MakeType<TOptionalExprType>(type);
        }
        return Build<TCoNothing>(ctx, owner.Pos())
            .OptionalType(BuildTypeExpr(owner.Pos(), *type, ctx))
            .Done();
    };

    bool fromInclusive = true;
    bool toInclusive = true;
    TVector<TExprBase> columnRanges;

    for (size_t i = 0; i < KeyRange.GetColumnRangesCount(); ++i) {
        const auto& column = Table.Metadata->KeyColumnNames[i];
        const auto& range = KeyRange.GetColumnRange(i);

        auto type = Table.GetColumnType(column);
        YQL_ENSURE(type);

        TMaybeNode<TExprBase> from;
        TMaybeNode<TExprBase> to;

        if (range.GetFrom().IsDefined()) {
            fromInclusive = range.GetFrom().IsInclusive();
            from = range.GetFrom().GetValue();
        } else {
            from = fromInclusive ? nInf(type) : pInf();
        }

        if (range.GetTo().IsDefined()) {
            toInclusive = range.GetTo().IsInclusive();
            to = range.GetTo().GetValue();
        } else {
            to = toInclusive ? pInf() : nInf(type);
        }

        auto rangeExpr = Build<TKiColumnRangeTuple>(ctx, owner.Pos())
            .Column().Build(column)
            .From(from.Cast())
            .To(to.Cast())
            .Done();

        columnRanges.push_back(rangeExpr);
    }

    return Build<TExprList>(ctx, owner.Pos())
        .Add<TCoAtom>()
            .Value(fromInclusive ? "IncFrom" : "ExcFrom")
            .Build()
        .Add<TCoAtom>()
            .Value(toInclusive ? "IncTo" : "ExcTo")
            .Build()
        .Add(columnRanges)
        .Done();
}

static TVector<TExprNodePtr> GetSkipNullKeys(const NCommon::TKeyRange& keyRange,
    const TKikimrTableDescription& tableDesc, const NYql::TPositionHandle& pos, TExprContext& ctx)
{
    TVector<TExprNodePtr> skipNullKeys;
    for (size_t i = 0; i < keyRange.GetColumnRangesCount(); ++i) {
        const auto& column = tableDesc.Metadata->KeyColumnNames[i];
        auto& range = keyRange.GetColumnRange(i);
        if (range.IsDefined() && !range.IsNull()) {
            skipNullKeys.push_back(ctx.NewAtom(pos, column));
        }
    }
    return skipNullKeys;
}

NNodes::TExprBase TKikimrKeyRange::BuildReadRangeExpr(const TKikimrTableDescription& tableDesc,
    const NCommon::TKeyRange& keyRange, NNodes::TCoAtomList select, bool allowNulls,
    TExprContext& ctx)
{
    YQL_ENSURE(tableDesc.Metadata);
    TString cluster = tableDesc.Metadata->Cluster;

    const auto versionedTable = BuildVersionedTable(*tableDesc.Metadata, select.Pos(), ctx);

    if (keyRange.IsEquiRange()) {
        TVector<TExprBase> columnTuples;
        for (size_t i = 0; i < keyRange.GetColumnRangesCount(); ++i) {
            const auto& column = tableDesc.Metadata->KeyColumnNames[i];
            auto& range = keyRange.GetColumnRange(i);

            auto tuple = Build<TCoNameValueTuple>(ctx, select.Pos())
                .Name().Build(column)
                .Value(range.GetFrom().GetValue())
                .Done();

            columnTuples.push_back(tuple);
        }

        return Build<TCoToList>(ctx, select.Pos())
            .Optional<TKiSelectRow>()
                .Cluster().Build(cluster)
                .Table(versionedTable)
                .Key<TCoNameValueTupleList>()
                    .Add(columnTuples)
                    .Build()
                .Select(select)
                .Build()
            .Done();
    } else {
        TVector<TExprNodePtr> skipNullKeys;
        if (!allowNulls) {
            skipNullKeys = GetSkipNullKeys(keyRange, tableDesc, select.Pos(), ctx);
        }

        TVector<TCoNameValueTuple> settings;
        if (!skipNullKeys.empty()) {
            auto setting = Build<TCoNameValueTuple>(ctx, select.Pos())
                .Name().Build("SkipNullKeys")
                .Value<TCoAtomList>()
                    .Add(skipNullKeys)
                    .Build()
                .Done();

            settings.push_back(setting);
        }

        TKikimrKeyRange tableKeyRange(tableDesc, keyRange);

        return Build<TKiSelectRange>(ctx, select.Pos())
            .Cluster().Build(cluster)
            .Table(versionedTable)
            .Range(tableKeyRange.ToRangeExpr(select, ctx))
            .Select(select)
            .Settings()
                .Add(settings)
                .Build()
            .Done();
    }
}

NNodes::TExprBase TKikimrKeyRange::BuildIndexReadRangeExpr(const TKikimrTableDescription& lookupTableDesc,
    const NCommon::TKeyRange& keyRange, NNodes::TCoAtomList select, bool allowNulls,
    const TKikimrTableDescription& dataTableDesc, TExprContext& ctx)
{
    YQL_ENSURE(lookupTableDesc.Metadata);
    YQL_ENSURE(dataTableDesc.Metadata);
    TString cluster = lookupTableDesc.Metadata->Cluster;

    const auto versionedTable = BuildVersionedTable(*dataTableDesc.Metadata, select.Pos(), ctx);

    YQL_ENSURE(!keyRange.IsEquiRange());

    TVector<TExprNodePtr> skipNullKeys;
    if (!allowNulls) {
        skipNullKeys = GetSkipNullKeys(keyRange, lookupTableDesc, select.Pos(), ctx);
    }

    TVector<TCoNameValueTuple> settings;
    if (!skipNullKeys.empty()) {
        auto setting = Build<TCoNameValueTuple>(ctx, select.Pos())
            .Name().Build("SkipNullKeys")
            .Value<TCoAtomList>()
                .Add(skipNullKeys)
                .Build()
            .Done();

        settings.push_back(setting);
    }

    TKikimrKeyRange tableKeyRange(lookupTableDesc, keyRange);

    return Build<TKiSelectIndexRange>(ctx, select.Pos())
        .Cluster().Build(cluster)
        .Table(versionedTable)
        .Range(tableKeyRange.ToRangeExpr(select, ctx))
        .Select(select)
        .Settings()
            .Add(settings)
            .Build()
        .IndexName()
            .Value(lookupTableDesc.Metadata->Name)
            .Build()
        .Done();
}

TExprNode::TPtr KiSqlInToEquiJoin(NNodes::TExprBase node, const TKikimrTablesData& tablesData,
    const TKikimrConfiguration& config, TExprContext& ctx)
{
    if (config.HasOptDisableSqlInToJoin()) {
        return node.Ptr();
    }

    if (!node.Maybe<TCoFlatMap>()) {
        return node.Ptr();
    }
    auto flatMap = node.Cast<TCoFlatMap>();

    // SqlIn expected to be rewritten to (FlatMap <collection> (OptionalIf ...))
    // or (FlatMap <collection> (FlatListIf ...))
    if (!flatMap.Lambda().Body().Maybe<TCoOptionalIf>() && !flatMap.Lambda().Body().Maybe<TCoFlatListIf>()) {
        return node.Ptr();
    }

    if (!flatMap.Input().Maybe<TKiSelectRangeBase>()) {
        return node.Ptr();
    }

    auto selectRange = flatMap.Input().Cast<TKiSelectRangeBase>();

    TMaybeNode<TCoAtom> indexTable;
    if (auto indexSelect = selectRange.Maybe<TKiSelectIndexRange>()) {
        indexTable = indexSelect.Cast().IndexName();
    }

    // retrieve selected ranges
    const TStringBuf lookupTable = indexTable ? indexTable.Cast().Value() : selectRange.Table().Path().Value();
    const TKikimrTableDescription& tableDesc = tablesData.ExistingTable(selectRange.Cluster().Value(), lookupTable);
    auto selectKeyRange = TKikimrKeyRange::GetPointKeyRange(ctx, tableDesc, selectRange.Range());
    if (!selectKeyRange) {
        return node.Ptr();
    }

    // check which key prefixes are used (and only with points)
    TVector<TStringBuf> keys; // remaining key parts, that can be used in SqlIn (only in asc order)
    for (size_t idx = 0; idx < selectKeyRange->GetColumnRanges().size(); ++idx) {
        const auto& columnRange = selectKeyRange->GetColumnRange(idx);
        if (columnRange.IsDefined()) {
            if (!keys.empty()) {
                return node.Ptr();
            }
            if (columnRange.IsPoint()) {
                continue;
            }
            return node.Ptr();
        }
        keys.emplace_back(tableDesc.Metadata->KeyColumnNames[idx]);
    }
    if (keys.empty()) {
        return node.Ptr();
    }

    auto flatMapLambdaArg = flatMap.Lambda().Args().Arg(0);

    auto findMemberIndexInKeys = [&keys](const TCoArgument& flatMapLambdaArg, const TCoMember& member) {
        if (member.Struct().Raw() != flatMapLambdaArg.Raw()) {
            return -1;
        }
        for (size_t i = 0; i < keys.size(); ++i) {
            if (member.Name().Value() == keys[i]) {
                return (int) i;
            }
        }
        return -1;
    };

    auto shouldConvertSqlInToJoin = [&flatMapLambdaArg, &findMemberIndexInKeys](const TCoSqlIn& sqlIn, bool negated) {
        if (negated) {
            // negated can't be rewritten to the index-lookup, so skip it
            return false;
        }

        // validate key prefix
        if (sqlIn.Lookup().Maybe<TCoMember>()) {
            if (findMemberIndexInKeys(flatMapLambdaArg, sqlIn.Lookup().Cast<TCoMember>()) != 0) {
                return false;
            }
        } else if (sqlIn.Lookup().Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
            auto children = sqlIn.Lookup().Ref().ChildrenList();
            TVector<int> usedKeyIndexes{Reserve(children.size())};
            for (const auto& itemPtr : children) {
                TExprBase item{itemPtr};
                if (!item.Maybe<TCoMember>()) {
                    return false;
                }
                int keyIndex = findMemberIndexInKeys(flatMapLambdaArg, item.Cast<TCoMember>());
                if (keyIndex >= 0) {
                    usedKeyIndexes.push_back(keyIndex);
                }
            }
            if (usedKeyIndexes.empty()) {
                return false;
            }
            ::Sort(usedKeyIndexes);
            for (size_t i = 0; i < usedKeyIndexes.size(); ++i) {
                if (usedKeyIndexes[i] != (int) i) {
                    return false;
                }
            }
        } else {
            return false;
        }

        return CanRewriteSqlInToEquiJoin(sqlIn.Lookup().Ref().GetTypeAnn(), sqlIn.Collection().Ref().GetTypeAnn());
    };

    const bool prefixOnly = true;
    if (auto ret = TryConvertSqlInPredicatesToJoins(flatMap, shouldConvertSqlInToJoin, ctx, prefixOnly)) {
        YQL_CLOG(INFO, ProviderKikimr) << "KiSqlInToEquiJoin";
        return ret;
    }

    return node.Ptr();
}

} // namespace NYql
