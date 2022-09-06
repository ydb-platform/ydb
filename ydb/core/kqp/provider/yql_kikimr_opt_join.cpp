#include "yql_kikimr_provider_impl.h"
#include "yql_kikimr_opt_utils.h"

#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>
#include <util/string/cast.h>

namespace NYql {
namespace {

using namespace NNodes;
using namespace NCommon;

using TGetExprFunc = std::function<TExprBase()>;

TExprBase GetEquiJoinInputList(TCoAtom scope, const TJoinLabels& joinLabels,
    const TVector<TCoEquiJoinInput>& joinInputs)
{
    auto inputIndex = joinLabels.FindInputIndex(scope.Value());
    YQL_ENSURE(inputIndex);

    return joinInputs[*inputIndex].List();
}

void GatherEquiJoinLables(TExprBase joinScope, TVector<TString>& labels) {
    if (joinScope.Maybe<TCoAtom>()) {
        labels.emplace_back(TString(joinScope.Cast<TCoAtom>()));
        return;
    }

    auto joinTuple = joinScope.Cast<TCoEquiJoinTuple>();
    GatherEquiJoinLables(joinTuple.LeftScope(), labels);
    GatherEquiJoinLables(joinTuple.RightScope(), labels);
}

TSet<TString> GetEquiJoinLabelReps(TExprBase joinScope) {
    TVector<TString> labels;
    GatherEquiJoinLables(joinScope, labels);
    return TSet<TString>(labels.begin(), labels.end());
}

TExprBase ConvertToTuples(const TSet<TString>& columns, const TCoArgument& structArg, TExprContext& ctx,
    TPositionHandle pos)
{
    TVector<TExprBase> tuples{Reserve(columns.size())};

    for (const auto& key : columns) {
        tuples.emplace_back(Build<TCoMember>(ctx, pos)
            .Struct(structArg)
            .Name().Build(key)
            .Done());
    }

    if (tuples.size() == 1) {
        return tuples[0];
    }

    return Build<TExprList>(ctx, pos)
        .Add(tuples)
        .Done();
}

TVector<TExprBase> ConvertToAtoms(const TSet<TString>& columns, TExprContext& ctx, TPositionHandle pos) {
    TVector<TExprBase> list{Reserve(columns.size())};
    for (const auto& column : columns) {
        list.emplace_back(TCoAtom(ctx.NewAtom(pos, column)));
    }
    return list;
};


TMaybeNode<TExprBase> EquiJoinGetIdxLookupValue(const TStringBuf& leftDataName, const TStringBuf& rightDataName,
    TExprBase leftRow, const TString& leftMemberName, TPositionHandle pos, TExprContext& ctx)
{
    auto leftMember = Build<TCoMember>(ctx, pos)
        .Struct(leftRow)
        .Name().Build(leftMemberName)
        .Done();

    TExprBase value = leftMember;
    if (leftDataName != rightDataName) {
        bool canConvert = false;

        if (IsDataTypeNumeric(NKikimr::NUdf::GetDataSlot(leftDataName)) && IsDataTypeNumeric(NKikimr::NUdf::GetDataSlot(rightDataName))) {
            canConvert = GetNumericDataTypeLevel(NKikimr::NUdf::GetDataSlot(rightDataName))
            >= GetNumericDataTypeLevel(NKikimr::NUdf::GetDataSlot(leftDataName));
        }

        if (leftDataName == "Utf8" && rightDataName == "String") {
            canConvert = true;
        }

        if (!canConvert) {
            return TMaybeNode<TExprBase>();
        }

        value = Build<TCoConvert>(ctx, pos)
            .Input(value)
            .Type().Build(rightDataName)
            .Done();
    }

    return value;
}

bool EquiJoinToIdxLookup(TGetExprFunc getLeftExpr, TCoEquiJoinTuple joinTuple, const TJoinLabels& joinLabels,
    const TVector<TCoEquiJoinInput>& joinInputs, const TKikimrTablesData& tablesData, TExprContext& ctx,
    TMaybeNode<TExprBase>& idxLookupExpr)
{
    if (!joinTuple.RightScope().Maybe<TCoAtom>()) {
        // Can't lookup in join result
        return false;
    }

    static const struct {
        const TStringBuf Left = "Left";
        const TStringBuf Inner = "Inner";
        const TStringBuf LeftSemi = "LeftSemi";
        const TStringBuf LeftOnly = "LeftOnly";
        const TStringBuf RightSemi = "RightSemi";
    } joinNames;

    static const TVector<TStringBuf> allowedJoins {
        joinNames.Left,
        joinNames.Inner,
        joinNames.LeftSemi,
        joinNames.LeftOnly,
        joinNames.RightSemi
    };

    const TStringBuf joinType = joinTuple.Type().Value();
    if (Find(allowedJoins, joinType) == allowedJoins.cend()) {
        return false;
    }

    auto linkSettings = GetEquiJoinLinkSettings(joinTuple.Options().Ref());
    if (linkSettings.LeftHints.contains("any") || linkSettings.RightHints.contains("any")) {
        return false;
    }

    auto rightExpr = GetEquiJoinInputList(joinTuple.RightScope().Cast<TCoAtom>(), joinLabels, joinInputs);

    TMaybeNode<TKiSelectRangeBase> rightSelect;
    TMaybeNode<TCoFlatMap> rightFlatmap;
    TMaybeNode<TCoFilterNullMembers> rightFilterNull;
    TMaybeNode<TCoSkipNullMembers> rightSkipNull;
    TMaybeNode<TCoAtom> indexTable;

    if (auto select = rightExpr.Maybe<TKiSelectRangeBase>()) {
        rightSelect = select;
    }

    if (auto select = rightExpr.Maybe<TCoFlatMap>().Input().Maybe<TKiSelectRangeBase>()) {
        rightSelect = select;
        rightFlatmap = rightExpr.Cast<TCoFlatMap>();
    }

    if (auto select = rightExpr.Maybe<TCoFlatMap>().Input().Maybe<TCoFilterNullMembers>()
        .Input().Maybe<TKiSelectRangeBase>())
    {
        rightSelect = select;
        rightFlatmap = rightExpr.Cast<TCoFlatMap>();
        rightFilterNull = rightFlatmap.Input().Cast<TCoFilterNullMembers>();
    }

    if (auto select = rightExpr.Maybe<TCoFlatMap>().Input().Maybe<TCoSkipNullMembers>()
        .Input().Maybe<TKiSelectRangeBase>())
    {
        rightSelect = select;
        rightFlatmap = rightExpr.Cast<TCoFlatMap>();
        rightSkipNull = rightFlatmap.Input().Cast<TCoSkipNullMembers>();
    }

    if (auto indexSelect = rightSelect.Maybe<TKiSelectIndexRange>()) {
        indexTable = indexSelect.Cast().IndexName();
    }

    if (!rightSelect) {
        return false;
    }

    if (rightFlatmap && !IsPassthroughFlatMap(rightFlatmap.Cast(), nullptr)) {
        // Can't lookup in modified table
        return false;
    }

    const auto selectRange = rightSelect.Cast();
    const TStringBuf cluster = selectRange.Cluster().Value();
    const TStringBuf lookupTable = indexTable ? indexTable.Cast().Value() : selectRange.Table().Path().Value();
    const TKikimrTableDescription& lookupTableDesc = tablesData.ExistingTable(cluster, lookupTable);

    auto rightKeyRange = TKikimrKeyRange::GetPointKeyRange(ctx, lookupTableDesc, selectRange.Range());
    if (!rightKeyRange) {
        // Don't rewrite join with arbitrary range
        return false;
    }

    for (size_t i = 0; i < rightKeyRange->GetColumnRangesCount(); ++i) {
        auto columnRange = rightKeyRange->GetColumnRange(i);
        if (columnRange.IsDefined()) {
            YQL_ENSURE(columnRange.IsPoint());
            auto argument = FindNode(columnRange.GetFrom().GetValue().Ptr(), [] (const TExprNode::TPtr& node) {
                return node->IsArgument();
            });

            if (argument) {
                // Can't lookup in dependent range
                return false;
            }
        }
    }

    auto leftLabelReps = GetEquiJoinLabelReps(joinTuple.LeftScope());
    YQL_ENSURE(!leftLabelReps.empty());
    auto rightLabelReps = GetEquiJoinLabelReps(joinTuple.RightScope());
    YQL_ENSURE(rightLabelReps.size() == 1);
    YQL_ENSURE(rightLabelReps.contains(joinTuple.RightScope().Cast<TCoAtom>().Value()));

    auto joinKeyCount = joinTuple.RightKeys().Size() / 2;

    bool prefixLeftMembers = !joinTuple.LeftScope().Maybe<TCoAtom>();

    TCoArgument leftRowArg = Build<TCoArgument>(ctx, joinTuple.Pos())
        .Name("leftRow")
        .Done();

    TSet<TString> leftKeyColumnsSet;
    TSet<TString> rightKeyColumnsSet;
    TVector<TColumnRange> keyColumnRanges(lookupTableDesc.Metadata->KeyColumnNames.size());

    for (size_t i = 0; i < joinKeyCount; ++i) {
        auto joinLeftLabel = joinTuple.LeftKeys().Item(i * 2);
        auto joinLeftColumn = joinTuple.LeftKeys().Item(i * 2 + 1);
        auto joinRightLabel = joinTuple.RightKeys().Item(i * 2);
        auto joinRightColumn = joinTuple.RightKeys().Item(i * 2 + 1);

        if (!rightLabelReps.contains(joinRightLabel) || !leftLabelReps.contains(joinLeftLabel)) {
            // Join already rewritten, skip opt
            return false;
        }

        TString leftColumnName = ToString(joinLeftColumn.Value());
        TString leftMemberName = prefixLeftMembers
            ? FullColumnName(joinLeftLabel.Value(), joinLeftColumn.Value())
            : leftColumnName;

        TString rightColumnName = ToString(joinRightColumn.Value());

        auto keyColumnIdx = lookupTableDesc.GetKeyColumnIndex(rightColumnName);
        if (!keyColumnIdx) {
            // Non-key columns in join key currently not supported
            return false;
        }

        if (rightKeyColumnsSet.contains(rightColumnName)) {
            // Can't lookup same column twice
            return false;
        }

        leftKeyColumnsSet.insert(leftMemberName);
        rightKeyColumnsSet.insert(rightColumnName);

        auto leftInput = GetEquiJoinInputList(joinLeftLabel, joinLabels, joinInputs);
        const TDataExprType* leftData;
        const TDataExprType* rightData;
        if (!GetEquiJoinKeyTypes(leftInput, leftColumnName, lookupTableDesc, rightColumnName, leftData, rightData)) {
            return false;
        }

        auto value = EquiJoinGetIdxLookupValue(leftData->GetName(), rightData->GetName(), leftRowArg,
            leftMemberName, joinTuple.Pos(), ctx);
        if (!value) {
            return false;
        }

        keyColumnRanges[*keyColumnIdx] = TColumnRange::MakePoint(value.Cast());
    }

    for (size_t i = 0; i < keyColumnRanges.size(); ++i) {
        bool leftColumnDefined = keyColumnRanges[i].IsDefined();
        if (leftColumnDefined && rightKeyRange->GetColumnRange(i).IsDefined()) {
            return false;
        }
        if (!leftColumnDefined) {
            keyColumnRanges[i] = rightKeyRange->GetColumnRange(i);
        }

        YQL_ENSURE(keyColumnRanges[i].IsPoint() || !keyColumnRanges[i].IsDefined());
    }

    for (size_t i = 0; i < keyColumnRanges.size() - 1; ++i) {
        if (!keyColumnRanges[i].IsDefined() && keyColumnRanges[i + 1].IsDefined()) {
            // Invalid lookup key
            // TODO: Move part of lookup key to residual predicate
            return false;
        }
    }

    TCoAtomList lookupColumns = selectRange.Select();
    bool requireIndexValues = false; // 'true' means that some requested columns are not presented in the index-table,
                                     // so read from data-table is required
    if (indexTable) {
        // In this case lookupTableDesc == indexTable,
        // so check whether index-table contains all lookup-columns
        for (const auto& lookupColumn : lookupColumns) {
            if (!lookupTableDesc.Metadata->Columns.contains(TString(lookupColumn.Value()))) {
                requireIndexValues = true;
                break;
            }
        }
    }

    auto selectedColumns = (indexTable && requireIndexValues)
        ? BuildKeyColumnsList(lookupTableDesc, selectRange.Pos(), ctx)
        : lookupColumns;

    auto lookup = TKikimrKeyRange::BuildReadRangeExpr(lookupTableDesc, TKeyRange(ctx, keyColumnRanges, {}),
                                                      selectedColumns, false /* allowNulls */, ctx);

    // Skip null keys in lookup part as for equijoin semantics null != null,
    // so we can't have nulls in lookup part
    lookup = Build<TCoSkipNullMembers>(ctx, joinTuple.Pos())
        .Input(lookup)
        .Members()
            .Add(ConvertToAtoms(rightKeyColumnsSet, ctx, joinTuple.Pos()))
            .Build()
        .Done();

    if (rightFilterNull) {
        lookup = Build<TCoFilterNullMembers>(ctx, joinTuple.Pos())
            .Input(lookup)
            .Members(rightFilterNull.Cast().Members())
            .Done();
    }

    if (rightSkipNull) {
        lookup = Build<TCoSkipNullMembers>(ctx, joinTuple.Pos())
            .Input(lookup)
            .Members(rightSkipNull.Cast().Members())
            .Done();
    }

    // If we have index table AND need data from main we cand add this flat map here
    // because lookup is index table and does not have all columns
    if (rightFlatmap && !requireIndexValues) {
        lookup = Build<TCoFlatMap>(ctx, joinTuple.Pos())
            .Input(lookup)
            .Lambda(rightFlatmap.Cast().Lambda())
            .Done();
    }

    auto addJoinResults = [&joinLabels, &ctx]
        (const TExprBase& joinTuple, const TExprBase& rowArg, TVector<TExprBase>& resultTuples,
            TVector<TString>* resultColumns)
    {
        TVector<std::pair<TString, TString>> joinColumns;
        if (auto maybeAtom = joinTuple.Maybe<TCoAtom>()) {
            auto maybeInput = joinLabels.FindInput(maybeAtom.Cast().Value());
            YQL_ENSURE(maybeInput);

            auto input = *maybeInput;

            for (auto item: input->InputType->GetItems()) {
                joinColumns.emplace_back(item->GetName(), input->FullName(item->GetName()));
            }
        } else {
            auto columns = GetJoinColumnTypes(joinTuple.Ref(), joinLabels, ctx);
            for (auto it: joinLabels.Inputs) {
                for (auto item: it.InputType->GetItems()) {
                    auto member = item->GetName();
                    auto column = it.FullName(member);

                    if (columns.FindPtr(column)) {
                        joinColumns.emplace_back(member, column);
                    }
                }
            }
        }

        for (auto& pair : joinColumns) {
            auto& member = pair.first;
            auto& column = pair.second;

            if (resultColumns) {
                resultColumns->push_back(column);
            }

            auto tuple = Build<TCoNameValueTuple>(ctx, joinTuple.Pos())
                .Name().Build(column)
                .Value<TCoMember>()
                    .Struct(rowArg)
                    .Name().Build(joinTuple.Maybe<TCoAtom>() ? member : column)
                    .Build()
                .Done();

            resultTuples.emplace_back(std::move(tuple));
        }
    };

    auto injectRightDataKey = [&tablesData, &selectRange, &ctx]
        (TMaybeNode<TExprBase>& rightRow, TVector<TExprBase>& joinResultTuples)
    {
        const TStringBuf cluster = selectRange.Cluster();
        const TStringBuf table = selectRange.Table().Path();
        const auto& desc = tablesData.ExistingTable(cluster, table);
        for (const auto& col : desc.Metadata->KeyColumnNames) {
            auto tuple = Build<TCoNameValueTuple>(ctx, selectRange.Pos())
                .Name().Build(FullColumnName(desc.Metadata->Name, col))
                .Value<TCoMember>()
                    .Struct(rightRow.Cast())
                    .Name().Build(col)
                    .Build()
                .Done();
            joinResultTuples.push_back(tuple);
        }
    };

    auto getJoinResultExpr = [requireIndexValues, &indexTable, &addJoinResults,
        &injectRightDataKey, &ctx, &joinTuple]
        (TMaybeNode<TExprBase> leftRowArg, TMaybeNode<TExprBase> rightRowArg)
    {
        TVector<TString> resultColumns;
        TVector<TExprBase> joinResultTuples;

        if (leftRowArg.IsValid()) {
            addJoinResults(joinTuple.LeftScope(), leftRowArg.Cast(), joinResultTuples, &resultColumns);
        }

        if (rightRowArg.IsValid()) {
            if (!requireIndexValues) {
                addJoinResults(joinTuple.RightScope(), rightRowArg.Cast(), joinResultTuples, &resultColumns);
            } else {
                // we need data from the right table that not contains in the lookup-table (not indexed columns),
                // so we add reads from the right table
                YQL_ENSURE(indexTable.IsValid());
                //TODO: May be it'a a good idea to get some collumns data from the index-read?
                injectRightDataKey(rightRowArg, joinResultTuples);
            }
        } else {
            YQL_ENSURE(!requireIndexValues);
        }

        auto expr = Build<TCoAsStruct>(ctx, joinTuple.Pos())
                .Add(joinResultTuples)
                .Done();

        return std::make_pair(expr, resultColumns);
    };

    auto finalizeJoinResultExpr = [&joinTuple, &tablesData, &ctx,
                                   &selectRange, &addJoinResults, &rightFlatmap, &joinType]
            (const TExprBase& input, const TVector<TString>& finishedColumns, bool needExtraRead)
                -> NNodes::TExprBase
    {
        if (!needExtraRead) {
            return input;
        }

        TCoArgument joinResult = Build<TCoArgument>(ctx, joinTuple.Pos())
            .Name("joinResult")
            .Done();

        TVector<TExprBase> joinResultTuples;
        for (const auto& col : finishedColumns) {
            auto tuple = Build<TCoNameValueTuple>(ctx, joinTuple.Pos())
                .Name().Build(col)
                .Value<TCoMember>()
                    .Struct(joinResult)
                    .Name().Build(col)
                    .Build()
                .Done();
            joinResultTuples.push_back(tuple);
        }

        const TStringBuf cluster = selectRange.Cluster();
        const TStringBuf table = selectRange.Table().Path();
        const auto& tableDesc = tablesData.ExistingTable(cluster, table);
        TExprBase select = Build<TKiSelectRow>(ctx, joinTuple.Pos())
            .Cluster(selectRange.Cluster())
            .Table(selectRange.Table())
            .Key(ExtractNamedKeyTuples(joinResult, tableDesc, ctx, tableDesc.Metadata->Name))
            .Select(selectRange.Select())
            .Done();

        if (rightFlatmap) {
            select = Build<TCoFlatMap>(ctx, joinTuple.Pos())
                .Input(select)
                .Lambda(rightFlatmap.Cast().Lambda())
                .Done();
        }

        addJoinResults(joinTuple.RightScope(), select, joinResultTuples, nullptr);

        if (joinType == joinNames.Inner || joinType == joinNames.RightSemi) {
            return Build<TCoFlatMap>(ctx, joinTuple.Pos())
                .Input(input)
                .Lambda()
                    .Args({joinResult})
                    .Body<TCoOptionalIf>()
                        .Predicate<TCoHasItems>()
                            .List<TCoToList>()
                                .Optional(select)
                                .Build()
                            .Build()
                        .Value<TCoAsStruct>()
                            .Add(joinResultTuples)
                            .Build()
                        .Build()
                    .Build()
                .Done();
        } else if (joinType == joinNames.Left){
            return Build<TCoMap>(ctx, joinTuple.Pos())
                .Input(input)
                .Lambda()
                    .Args({joinResult})
                    .Body<TCoAsStruct>()
                        .Add(joinResultTuples)
                        .Build()
                    .Build()
                .Done();
        } else {
            YQL_ENSURE(false, "unknown join type to call finalizeJoinResultExpr " << joinType);
        }
    };

    auto leftExpr = getLeftExpr();
    TCoArgument rightRowArg = Build<TCoArgument>(ctx, joinTuple.Pos())
        .Name("rightRow")
        .Done();

    if (joinType == joinNames.Left) {
        TExprBase rightNothing = Build<TCoNothing>(ctx, joinTuple.Pos())
            .OptionalType<TCoTypeOf>()
                .Value<TCoToOptional>()
                    .List(lookup)
                    .Build()
                .Build()
            .Done();

        auto joinResultExpr = getJoinResultExpr(leftRowArg, rightRowArg);

        auto joinMap = Build<TCoFlatMap>(ctx, joinTuple.Pos())
            .Input(leftExpr)
            .Lambda()
                .Args({leftRowArg})
                .Body<TCoIf>()
                    .Predicate<TCoHasItems>()
                        .List(lookup)
                        .Build()
                    .ThenValue<TCoMap>()
                        .Input(lookup)
                        .Lambda()
                            .Args({rightRowArg})
                            .Body(joinResultExpr.first)
                            .Build()
                        .Build()
                    .ElseValue<TCoAsList>()
                        .Add(getJoinResultExpr(leftRowArg, rightNothing).first)
                        .Build()
                    .Build()
                .Build()
            .Done();
        idxLookupExpr = finalizeJoinResultExpr(joinMap, joinResultExpr.second, requireIndexValues);
        return true;
    }

    if (joinType == joinNames.Inner) {
        const auto joinResultExpr = getJoinResultExpr(leftRowArg, rightRowArg);

        auto joinMap = Build<TCoFlatMap>(ctx, joinTuple.Pos())
            .Input<TCoSkipNullMembers>()
                .Input(leftExpr)
                .Members()
                    .Add(ConvertToAtoms(leftKeyColumnsSet, ctx, joinTuple.Pos()))
                    .Build()
                .Build()
            .Lambda()
                .Args({leftRowArg})
                .Body<TCoMap>()
                    .Input(lookup)
                    .Lambda()
                        .Args({rightRowArg})
                        .Body(joinResultExpr.first)
                        .Build()
                    .Build()
                .Build()
            .Done();
        idxLookupExpr = finalizeJoinResultExpr(joinMap, joinResultExpr.second, requireIndexValues);
        return true;
    }

    if (joinType == joinNames.LeftSemi) {
        idxLookupExpr = Build<TCoFlatMap>(ctx, joinTuple.Pos())
            .Input<TCoSkipNullMembers>()
                .Input(leftExpr)
                .Members()
                    .Add(ConvertToAtoms(leftKeyColumnsSet, ctx, joinTuple.Pos()))
                    .Build()
                .Build()
            .Lambda()
                .Args({leftRowArg})
                .Body<TCoOptionalIf>()
                    .Predicate<TCoHasItems>()
                        .List(lookup)
                        .Build()
                    .Value(getJoinResultExpr(leftRowArg, {}).first)
                    .Build()
                .Build()
            .Done();
        return true;
    }

    if (joinType == joinNames.LeftOnly) {
        idxLookupExpr = Build<TCoFlatMap>(ctx, joinTuple.Pos())
            .Input(leftExpr)
            .Lambda()
                .Args({leftRowArg})
                .Body<TCoOptionalIf>()
                    .Predicate<TCoNot>()
                        .Value<TCoHasItems>()
                            .List(lookup)
                            .Build()
                        .Build()
                    .Value(getJoinResultExpr(leftRowArg, {}).first)
                    .Build()
                .Build()
            .Done();
        return true;
    }

    if (joinType == joinNames.RightSemi) {
        // In this case we iterate over left table (with deduplication)
        // and do index-lookup in the right one.

        auto joinResultExpr = getJoinResultExpr({}, rightRowArg);

        // drop nulls
        leftExpr = Build<TCoSkipNullMembers>(ctx, joinTuple.Pos())
                .Input(leftExpr)
                .Members()
                    .Add(ConvertToAtoms(leftKeyColumnsSet, ctx, joinTuple.Pos()))
                    .Build()
                .Done();

        // deduplicate keys in the left table
        leftExpr = DeduplicateByMembers(leftExpr, leftKeyColumnsSet, ctx, joinTuple.Pos());

        auto joinMap = Build<TCoFlatMap>(ctx, joinTuple.Pos())
                .Input(leftExpr)
                .Lambda()
                    .Args({leftRowArg})
                    .Body<TCoMap>()
                        .Input(lookup)
                        .Lambda()
                            .Args({rightRowArg})
                            .Body(joinResultExpr.first)
                            .Build()
                        .Build()
                    .Build()
                .Done();

        // add extra reads if required
        idxLookupExpr = finalizeJoinResultExpr(joinMap, joinResultExpr.second, requireIndexValues);
        return true;
    }

    YQL_ENSURE(false, "Unexpected join type " << joinType);
    return false;
}

TExprBase GetEquiJoinLabelsNode(const TVector<TString>& labels, TPositionHandle pos, TExprContext& ctx) {
    TVector<TExprBase> labelAtoms;
    for (auto& label : labels) {
        auto atom = Build<TCoAtom>(ctx, pos)
            .Value(label)
            .Done();
        labelAtoms.push_back(atom);
    }

    if (labelAtoms.size() == 1) {
        return labelAtoms.front();
    }

    return Build<TCoAtomList>(ctx, pos)
        .Add(labelAtoms)
        .Done();
}

TCoEquiJoin BuildPairEquiJoin(TCoEquiJoinTuple joinTuple, TExprBase leftList, TExprBase rightList,
    TExprContext& ctx)
{
    TVector<TString> leftLabels;
    GatherEquiJoinLables(joinTuple.LeftScope(), leftLabels);
    YQL_ENSURE(!leftLabels.empty());

    TVector<TString> rightLabels;
    GatherEquiJoinLables(joinTuple.RightScope(), rightLabels);
    YQL_ENSURE(!rightLabels.empty());

    auto join = Build<TCoEquiJoin>(ctx, joinTuple.Pos())
        .Add<TCoEquiJoinInput>()
            .List(leftList)
            .Scope(GetEquiJoinLabelsNode(leftLabels, joinTuple.Pos(), ctx))
            .Build()
        .Add<TCoEquiJoinInput>()
            .List(rightList)
            .Scope(GetEquiJoinLabelsNode(rightLabels, joinTuple.Pos(), ctx))
            .Build()
        .Add<TCoEquiJoinTuple>()
            .Type(joinTuple.Type())
            .LeftScope<TCoAtom>().Build(leftLabels.front())
            .RightScope<TCoAtom>().Build(rightLabels.front())
            .LeftKeys(joinTuple.LeftKeys())
            .RightKeys(joinTuple.RightKeys())
            .Options(joinTuple.Options())
            .Build()
        .Add<TExprList>().Build()
        .Done();

    return join;
}

TExprBase GetEquiJoinTreeExpr(const TExprBase& joinScope, const TVector<TCoEquiJoinInput>& joinInputs,
    const TJoinLabels& joinLabels, TExprContext& ctx)
{
    if (joinScope.Maybe<TCoAtom>()) {
        return GetEquiJoinInputList(joinScope.Cast<TCoAtom>(), joinLabels, joinInputs);
    }

    auto joinTree = joinScope.Cast<TCoEquiJoinTuple>();

    TVector<TString> labels;
    GatherEquiJoinLables(joinTree, labels);

    TVector<TExprBase> newJoinInputs;

    THashSet<ui32> usedIndices;
    for (auto& label : labels) {
        auto index = joinLabels.FindInputIndex(label);
        YQL_ENSURE(index);

        if (!usedIndices.contains(*index)) {
            newJoinInputs.push_back(joinInputs[*index]);
            usedIndices.insert(*index);
        }
    }

    auto join = Build<TCoEquiJoin>(ctx, joinScope.Pos())
        .Add(newJoinInputs)
        .Add(joinTree)
        .Add<TExprList>().Build()
        .Done();

    return join;
}

TMaybe<TStringBuf> TryFlipJoinType(TStringBuf joinType) {
    if (joinType == TStringBuf("Inner")) {
        return TStringBuf("Inner");
    }
    if (joinType == TStringBuf("LeftSemi")) {
        return TStringBuf("RightSemi");
    }
    if (joinType == TStringBuf("RightSemi")) {
        return TStringBuf("LeftSemi");
    }
    if (joinType == TStringBuf("Right")) {
        return TStringBuf("Left");
    }
    if (joinType == TStringBuf("RightOnly")) {
        return TStringBuf("LeftOnly");
    }
    return Nothing();
}

bool RewriteEquiJoinInternal(const TCoEquiJoinTuple& joinTree, const TVector<TCoEquiJoinInput>& joinInputs,
    const TJoinLabels& joinLabels, const TKikimrTablesData& tablesData, TExprContext& ctx,
    const TKikimrConfiguration& config, TMaybeNode<TExprBase>& rewrittenExpr)
{
    bool leftRewritten = false;
    TMaybeNode<TExprBase> leftExpr;
    if (!joinTree.LeftScope().Maybe<TCoAtom>()) {
        leftRewritten = RewriteEquiJoinInternal(joinTree.LeftScope().Cast<TCoEquiJoinTuple>(), joinInputs,
            joinLabels, tablesData, ctx, config, leftExpr);
    }

    bool rightRewritten = false;
    TMaybeNode<TExprBase> rightExpr;
    if (!joinTree.RightScope().Maybe<TCoAtom>()) {
        rightRewritten = RewriteEquiJoinInternal(joinTree.RightScope().Cast<TCoEquiJoinTuple>(), joinInputs,
            joinLabels, tablesData, ctx, config, rightExpr);
    }

    auto getLeftExpr = [leftRewritten, leftExpr, joinTree, &joinInputs, &joinLabels, &ctx] () {
        return leftRewritten
            ? leftExpr.Cast()
            : GetEquiJoinTreeExpr(joinTree.LeftScope(), joinInputs, joinLabels, ctx);
    };

    auto getRightExpr = [rightRewritten, rightExpr, joinTree, &joinInputs, &joinLabels, &ctx] () {
        return rightRewritten
            ? rightExpr.Cast()
            : GetEquiJoinTreeExpr(joinTree.RightScope(), joinInputs, joinLabels, ctx);
    };

    if (!config.HasOptDisableJoinTableLookup()) {
        if (EquiJoinToIdxLookup(getLeftExpr, joinTree, joinLabels, joinInputs, tablesData, ctx, rewrittenExpr)) {
            return true;
        }

        bool tryFlip = joinTree.Type().Value() == TStringBuf("LeftSemi")
            ? !config.HasOptDisableJoinReverseTableLookupLeftSemi()
            : !config.HasOptDisableJoinReverseTableLookup();

        if (tryFlip) {
            // try to switch left and right subtrees and do rewrite one more time
            if (auto flipJoinType = TryFlipJoinType(joinTree.Type().Value())) {
                auto flipJoinTree = Build<TCoEquiJoinTuple>(ctx, joinTree.Pos())
                        .Type().Build(*flipJoinType)
                        .LeftScope(joinTree.RightScope())
                        .LeftKeys(joinTree.RightKeys())
                        .RightScope(joinTree.LeftScope())
                        .RightKeys(joinTree.LeftKeys())
                        .Options(joinTree.Options())
                        .Done();

                if (EquiJoinToIdxLookup(getRightExpr, flipJoinTree, joinLabels, joinInputs, tablesData, ctx,
                        rewrittenExpr))
                {
                    return true;
                }
            }
        }
    }

    if (leftRewritten || rightRewritten) {
        rewrittenExpr = BuildPairEquiJoin(joinTree, getLeftExpr(), getRightExpr(), ctx);
        return true;
    }

    return false;
}

} // namespace

TExprBase DeduplicateByMembers(const TExprBase& expr, const TSet<TString>& members, TExprContext& ctx,
        TPositionHandle pos)
{
    auto structArg = Build<TCoArgument>(ctx, pos)
            .Name("struct")
            .Done();

    return Build<TCoPartitionByKey>(ctx, pos)
            .Input(expr)
            .KeySelectorLambda()
                .Args(structArg)
                .Body(ConvertToTuples(members, structArg, ctx, pos))
                .Build()
            .SortDirections<TCoVoid>()
                .Build()
            .SortKeySelectorLambda<TCoVoid>()
                .Build()
            .ListHandlerLambda()
                .Args({"stream"})
                .Body<TCoFlatMap>()
                    .Input("stream")
                    .Lambda()
                        .Args({"tuple"})
                        .Body<TCoTake>()
                            .Input<TCoNth>()
                                .Tuple("tuple")
                                .Index().Value("1").Build()
                                .Build()
                            .Count<TCoUint64>()
                                .Literal().Value("1").Build()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Done();
}

TExprNode::TPtr KiRewriteEquiJoin(TExprBase node, const TKikimrTablesData& tablesData,
    const TKikimrConfiguration& config, TExprContext& ctx)
{
    if (!node.Maybe<TCoEquiJoin>()) {
        return node.Ptr();
    }

    if (config.HasOptDisableJoinRewrite()) {
        return node.Ptr();
    }

    auto join = node.Cast<TCoEquiJoin>();
    auto joinTree = join.Arg(join.ArgCount() - 2).Cast<TCoEquiJoinTuple>();

    TVector<TCoEquiJoinInput> joinInputs;
    TJoinLabels joinLabels;
    for (size_t i = 0; i < join.ArgCount() - 2; ++i) {
        auto input = join.Arg(i).Cast<TCoEquiJoinInput>();
        joinInputs.push_back(input);

        auto itemType = input.List().Ptr()->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        joinLabels.Add(ctx, *input.Scope().Ptr(), itemType->Cast<TStructExprType>());
    }

    TMaybeNode<TExprBase> rewrittenJoin;
    if (!RewriteEquiJoinInternal(joinTree, joinInputs, joinLabels, tablesData, ctx, config, rewrittenJoin)) {
        return node.Ptr();
    }

    YQL_ENSURE(rewrittenJoin.IsValid());
    YQL_CLOG(INFO, ProviderKikimr) << "KiRewriteEquiJoin";

    auto joinOptions = join.Arg(join.ArgCount() - 1).Cast<TExprList>();

    if (rewrittenJoin.Maybe<TCoEquiJoin>()) {
        auto equiJoin = rewrittenJoin.Cast<TCoEquiJoin>();
        return ctx.ChangeChild(*equiJoin.Ptr(), equiJoin.ArgCount() - 1, joinOptions.Ptr());
    }

    TExprBase joinExpr = rewrittenJoin.Cast();
    auto resultType = node.Ptr()->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto renameMap = LoadJoinRenameMap(*joinOptions.Ptr());
    TCoLambda renamingLambda = BuildJoinRenameLambda(joinExpr.Pos(), renameMap, *resultType, ctx);

    return Build<TCoMap>(ctx, joinExpr.Pos())
        .Input(joinExpr)
        .Lambda(renamingLambda)
        .Done()
        .Ptr();
}

} // namespace NYql
