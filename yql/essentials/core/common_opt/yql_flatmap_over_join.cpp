#include "yql_flatmap_over_join.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_join.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <yql/essentials/utils/log/log.h>

#include <library/cpp/disjoint_sets/disjoint_sets.h>

namespace NYql {

using namespace NNodes;

namespace {

TExprNode::TPtr ConstantPredicatePushdownOverEquiJoin(TExprNode::TPtr equiJoin, TExprNode::TPtr predicate, bool ordered, TExprContext& ctx) {
    auto lambda = ctx.Builder(predicate->Pos())
        .Lambda()
            .Param("row")
            .Set(predicate)
            .Seal()
        .Build();

    auto ret = ctx.ShallowCopy(*equiJoin);
    auto inputsCount = ret->ChildrenSize() - 2;
    for (ui32 i = 0; i < inputsCount; ++i) {
        ret->ChildRef(i) = ctx.ShallowCopy(*ret->Child(i));
        ret->Child(i)->ChildRef(0) = ctx.Builder(predicate->Pos())
            .Callable(ordered ? "OrderedFilter" : "Filter")
                .Add(0, ret->Child(i)->ChildPtr(0))
                .Add(1, lambda)
            .Seal()
            .Build();
    }

    return ret;
}

void GatherKeyAliases(const TExprNode::TPtr& joinTree, TMap<TString, TSet<TString>>& aliases, const TJoinLabels& labels) {
    auto left = joinTree->ChildPtr(1);
    if (!left->IsAtom()) {
        GatherKeyAliases(left, aliases, labels);
    }

    auto right = joinTree->ChildPtr(2);
    if (!right->IsAtom()) {
        GatherKeyAliases(right, aliases, labels);
    }

    auto leftColumns = joinTree->Child(3);
    auto rightColumns = joinTree->Child(4);
    for (ui32 i = 0; i < leftColumns->ChildrenSize(); i += 2) {
        auto leftColumn = FullColumnName(leftColumns->Child(i)->Content(), leftColumns->Child(i + 1)->Content());
        auto rightColumn = FullColumnName(rightColumns->Child(i)->Content(), rightColumns->Child(i + 1)->Content());
        auto leftType = *labels.FindColumn(leftColumn);
        auto rightType = *labels.FindColumn(rightColumn);
        if (IsSameAnnotation(*leftType, *rightType)) {
            aliases[leftColumn].insert(rightColumn);
            aliases[rightColumn].insert(leftColumn);
        }
    }
}

void MakeTransitiveClosure(TMap<TString, TSet<TString>>& aliases) {
    for (;;) {
        bool hasChanges = false;
        for (auto& x : aliases) {
            for (auto& y : x.second) {
                // x.first->y
                for (auto& z : aliases[y]) {
                    // add x.first->z
                    if (x.first != z) {
                        hasChanges = x.second.insert(z).second || hasChanges;
                    }
                }
            }
        }

        if (!hasChanges) {
            return;
        }
    }
}

void GatherOptionalKeyColumnsFromEquality(TExprNode::TPtr columns, const TJoinLabels& labels, ui32 inputIndex,
    TSet<TString>& optionalKeyColumns) {
    for (ui32 i = 0; i < columns->ChildrenSize(); i += 2) {
        auto table = columns->Child(i)->Content();
        auto column = columns->Child(i + 1)->Content();
        if (*labels.FindInputIndex(table) == inputIndex) {
            auto type = *labels.FindColumn(table, column);
            if (type->GetKind() == ETypeAnnotationKind::Optional) {
                optionalKeyColumns.insert(FullColumnName(table, column));
            }
        }
    }
}

void GatherOptionalKeyColumns(TExprNode::TPtr joinTree, const TJoinLabels& labels, ui32 inputIndex,
    TSet<TString>& optionalKeyColumns) {
    auto left = joinTree->Child(1);
    auto right = joinTree->Child(2);
    if (!left->IsAtom()) {
        GatherOptionalKeyColumns(left, labels, inputIndex, optionalKeyColumns);
    }

    if (!right->IsAtom()) {
        GatherOptionalKeyColumns(right, labels, inputIndex, optionalKeyColumns);
    }

    auto joinType = joinTree->Child(0)->Content();
    if (joinType == "Inner" || joinType == "LeftSemi") {
        GatherOptionalKeyColumnsFromEquality(joinTree->Child(3), labels, inputIndex, optionalKeyColumns);
    }

    if (joinType == "Inner" || joinType == "RightSemi") {
        GatherOptionalKeyColumnsFromEquality(joinTree->Child(4), labels, inputIndex, optionalKeyColumns);
    }
}

bool IsRequiredAndFilteredSide(const TExprNode::TPtr& joinTree, const TJoinLabels& labels, ui32 inputIndex) {
    TMaybe<bool> isFiltered = IsFilteredSide(joinTree, labels, inputIndex);
    return isFiltered.Defined() && *isFiltered;
}

TExprNode::TPtr ApplyJoinPredicate(const TExprNode::TPtr& predicate, const TExprNode::TPtr& filterInput,
    const TExprNode::TPtr& args, const TJoinLabels& labels, const THashMap<ui32, THashMap<TString, TString>>& aliasedKeys,
    const TMap<TStringBuf, TVector<TStringBuf>>& renameMap, bool onlyKeys,
    ui32 firstCandidate, ui32 inputIndex, bool ordered, bool substituteWithNulls, bool forceOptional, TExprContext& ctx
) {
    return ctx.Builder(predicate->Pos())
    .Callable(ordered ? "OrderedFilter" : "Filter")
        .Add(0, filterInput)
        .Lambda(1)
            .Param("row")
            .ApplyPartial(args, predicate).With(0)
                .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 index = 0;
                        const auto& label = labels.Inputs[inputIndex];
                        for (auto column : label.EnumerateAllColumns()) {
                            TVector<TString> targetColumns;
                            targetColumns.push_back(column);
                            if (onlyKeys && inputIndex != firstCandidate) {
                                if (auto aliasedKey = aliasedKeys.at(inputIndex).FindPtr(column)) {
                                    targetColumns[0] = *aliasedKey;
                                } else {
                                    continue;
                                }
                            }

                            TStringBuf part1;
                            TStringBuf part2;
                            SplitTableName(column, part1, part2);
                            auto memberName = label.MemberName(part1, part2);
                            auto memberType = label.FindColumn(part1, part2);
                            Y_ENSURE(memberType);
                            const bool memberIsOptional = (*memberType)->IsOptionalOrNull();
                            const TTypeAnnotationNode* optMemberType = memberIsOptional ? *memberType : ctx.MakeType<TOptionalExprType>(*memberType);

                            if (auto renamed = renameMap.FindPtr(targetColumns[0])) {
                                if (renamed->empty()) {
                                    continue;
                                }

                                targetColumns.clear();
                                for (auto& r : *renamed) {
                                    targetColumns.push_back(TString(r));
                                }
                            }

                            for (auto targetColumn : targetColumns) {
                                if (substituteWithNulls) {
                                    auto typeNode = ExpandType(predicate->Pos(), *optMemberType, ctx);
                                    parent.List(index++)
                                        .Atom(0, targetColumn)
                                        .Callable(1, "Nothing")
                                            .Add(0, typeNode)
                                        .Seal()
                                    .Seal();
                                } else if (forceOptional && !memberIsOptional) {
                                    parent.List(index++)
                                        .Atom(0, targetColumn)
                                        .Callable(1, "Just")
                                            .Callable(0, "Member")
                                                .Arg(0, "row")
                                                .Atom(1, memberName)
                                            .Seal()
                                        .Seal()
                                    .Seal();
                                } else {
                                    parent.List(index++)
                                        .Atom(0, targetColumn)
                                        .Callable(1, "Member")
                                            .Arg(0, "row")
                                            .Atom(1, memberName)
                                        .Seal()
                                    .Seal();
                                }
                            }
                        }

                        return parent;
                    })
                .Seal()
            .Done().Seal()
        .Seal()
    .Seal()
    .Build();
}

TExprNode::TPtr SingleInputPredicatePushdownOverEquiJoin(TExprNode::TPtr equiJoin, TExprNode::TPtr predicate,
    const TSet<TStringBuf>& usedFields, TExprNode::TPtr args, const TJoinLabels& labels,
    ui32 firstCandidate, const TMap<TStringBuf, TVector<TStringBuf>>& renameMap, bool ordered, bool skipNulls, TExprContext& ctx)
{
    auto inputsCount = equiJoin->ChildrenSize() - 2;
    auto joinTree = equiJoin->Child(inputsCount);

    if (!IsRequiredSide(joinTree, labels, firstCandidate).first) {
        return equiJoin;
    }

    YQL_ENSURE(args->ChildrenSize() == 1);
    YQL_ENSURE(args->Head().IsArgument());
    if (HasDependsOn(predicate, args->HeadPtr())) {
        return equiJoin;
    }

    const bool isStrict = IsStrict(predicate);
    if (!isStrict && IsRequiredAndFilteredSide(joinTree, labels, firstCandidate)) {
        return equiJoin;
    }

    TMap<TString, TSet<TString>> aliases;
    GatherKeyAliases(joinTree, aliases, labels);
    MakeTransitiveClosure(aliases);
    TSet<ui32> candidates;
    candidates.insert(firstCandidate);
    // check whether some used fields are not aliased
    bool onlyKeys = true;
    for (auto& x : usedFields) {
        if (!aliases.contains(TString(x))) {
            onlyKeys = false;
            break;
        }
    }

    THashMap<ui32, THashMap<TString, TString>> aliasedKeys;
    if (onlyKeys) {
        // try to extend inputs
        for (ui32 i = 0; i < inputsCount; ++i) {
            if (i == firstCandidate) {
                continue;
            }

            TSet<TString> coveredKeys;
            for (auto field : labels.Inputs[i].EnumerateAllColumns()) {
                if (auto aliasSet = aliases.FindPtr(field)) {
                    for (auto alias : *aliasSet) {
                        if (usedFields.contains(alias)) {
                            coveredKeys.insert(TString(alias));
                            aliasedKeys[i].insert({ field, TString(alias) });
                        }
                    }
                }
            }

            if (coveredKeys.size() == usedFields.size()) {
                candidates.insert(i);
            }
        }
    }

    auto ret = ctx.ShallowCopy(*equiJoin);
    for (auto& inputIndex : candidates) {
        auto x = IsRequiredSide(joinTree, labels, inputIndex);
        if (!x.first) {
            continue;
        }

        if (!isStrict && IsRequiredAndFilteredSide(joinTree, labels, inputIndex)) {
            continue;
        }

        auto prevInput = equiJoin->Child(inputIndex)->ChildPtr(0);
        auto newInput = prevInput;
        if (x.second && skipNulls) {
            // skip null key columns
            TSet<TString> optionalKeyColumns;
            GatherOptionalKeyColumns(joinTree, labels, inputIndex, optionalKeyColumns);
            newInput = FilterOutNullJoinColumns(predicate->Pos(),
                prevInput, labels.Inputs[inputIndex], optionalKeyColumns, ctx);
        }

        // then apply predicate
        newInput = ApplyJoinPredicate(
            predicate, /*filterInput=*/newInput, args, labels, aliasedKeys, renameMap, onlyKeys,
            firstCandidate, inputIndex, ordered, /*substituteWithNulls=*/false, /*forceOptional=*/false, ctx
        );

        // then return reassembled join
        ret->ChildRef(inputIndex) = ctx.ShallowCopy(*ret->Child(inputIndex));
        ret->Child(inputIndex)->ChildRef(0) = newInput;
    }

    return ret;
}

void CountLabelsInputUsage(TExprNode::TPtr joinTree, THashMap<TString, int>& counters) {
    if (joinTree->IsAtom()) {
        counters[joinTree->Content()]++;
    } else {
        CountLabelsInputUsage(joinTree->ChildPtr(1), counters);
        CountLabelsInputUsage(joinTree->ChildPtr(2), counters);
    }
}

// returns the path to join child
std::pair<TExprNode::TPtr, TExprNode::TPtr> IsRightSideForLeftJoin(
    const TExprNode::TPtr& joinTree, const TJoinLabels& labels, ui32 inputIndex, const TExprNode::TPtr& parent = nullptr
) {
    auto joinType = joinTree->Child(0)->Content();
    auto left = joinTree->ChildPtr(1);
    auto right = joinTree->ChildPtr(2);
    if (joinType == "Inner" || joinType == "Left" || joinType == "LeftOnly" || joinType == "LeftSemi" || joinType == "RightSemi" || joinType == "Cross") {
        if (!left->IsAtom()) {
            auto x = IsRightSideForLeftJoin(left, labels, inputIndex, joinTree);
            if (x.first) {
                return x;
            }
        }
    }

    if (joinType == "Inner" || joinType == "Right" || joinType == "RightOnly" || joinType == "RightSemi" || joinType == "LeftSemi" || joinType == "Cross" || joinType == "Left") {
        if (!right->IsAtom()) {
            auto x = IsRightSideForLeftJoin(right, labels, inputIndex, joinTree);
            if (x.first) {
                return x;
            }
        } else if (joinType == "Left") {
            auto table = right->Content();
            if (*labels.FindInputIndex(table) == inputIndex) {
                return {joinTree, parent};
            }
        }
    }

    return {nullptr, nullptr};
}

TExprNode::TPtr FilterPushdownOverJoinOptionalSide(TExprNode::TPtr equiJoin, TExprNode::TPtr predicate,
    const TSet<TStringBuf>& usedFields, TExprNode::TPtr args, const TJoinLabels& labels,
    ui32 inputIndex, const TMap<TStringBuf, TVector<TStringBuf>>& renameMap, bool ordered, bool skipNulls, TExprContext& ctx,
    const TPositionHandle& pos)
{
    auto inputsCount = equiJoin->ChildrenSize() - 2;
    auto joinTree = equiJoin->Child(inputsCount);

    if (!IsRightSideForLeftJoin(joinTree, labels, inputIndex).first) {
        return equiJoin;
    }

    YQL_ENSURE(args->ChildrenSize() == 1);
    YQL_ENSURE(args->Head().IsArgument());
    if (HasDependsOn(predicate, args->HeadPtr())) {
        return equiJoin;
    }

    const bool isStrict = IsStrict(predicate);
    if (!isStrict/* && IsRequiredAndFilteredSide(joinTree, labels, firstCandidate)*/) {
        return equiJoin;
    }

    TMap<TString, TSet<TString>> aliases;
    GatherKeyAliases(joinTree, aliases, labels);
    MakeTransitiveClosure(aliases);

    // check whether some used fields are not aliased
    bool onlyKeys = true;
    for (auto& x : usedFields) {
        if (!aliases.contains(TString(x))) {
            onlyKeys = false;
            break;
        }
    }

    if (onlyKeys) {
        return equiJoin;
    }

    THashMap<TString, TExprNode::TPtr> equiJoinLabels;
    for (size_t i = 0; i < equiJoin->ChildrenSize() - 2; i++) {
        auto label = equiJoin->Child(i);
        equiJoinLabels.emplace(label->Child(1)->Content(), label->ChildPtr(0));
    }

    THashMap<TString, int> joinLabelCounters;
    CountLabelsInputUsage(joinTree, joinLabelCounters);

    auto [leftJoinTree, parentJoinPtr] = IsRightSideForLeftJoin(joinTree, labels, inputIndex);
    YQL_ENSURE(leftJoinTree);
    joinLabelCounters[leftJoinTree->Child(1)->Content()]--;
    joinLabelCounters[leftJoinTree->Child(2)->Content()]--;

    auto leftJoinSettings = equiJoin->ChildPtr(equiJoin->ChildrenSize() - 1);

    auto innerJoinTree = ctx.ChangeChild(*leftJoinTree, 0, ctx.NewAtom(leftJoinTree->Pos(), "Inner"));
    auto leftOnlyJoinTree = ctx.ChangeChild(*leftJoinTree, 0, ctx.NewAtom(leftJoinTree->Pos(), "LeftOnly"));

    THashMap<TString, int> leftSideJoinLabels;
    CountLabelsInputUsage(leftJoinTree->Child(1), leftSideJoinLabels);

    YQL_ENSURE(leftJoinTree->Child(2)->IsAtom());
    auto rightSideInput = equiJoinLabels.at(leftJoinTree->Child(2)->Content());

    if (skipNulls) {
        // skip null key columns
        TSet<TString> optionalKeyColumns;
        GatherOptionalKeyColumns(joinTree, labels, inputIndex, optionalKeyColumns);
        rightSideInput = FilterOutNullJoinColumns(predicate->Pos(),
            rightSideInput, labels.Inputs[inputIndex], optionalKeyColumns, ctx);
    }

    // then apply predicate
    auto filteredInput = ApplyJoinPredicate(
        predicate, /*filterInput=*/rightSideInput, args, labels, {}, renameMap, onlyKeys,
        inputIndex, inputIndex, ordered, /*substituteWithNulls=*/false, /*forceOptional=*/true, ctx
    );

    // then create unionall of two joins.
    //firstly, join same labels with inner join:

    size_t i = 0;
    auto innerJoin = ctx.Builder(pos)
        .Callable("EquiJoin")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                for (const auto& [labelName, _] : leftSideJoinLabels) {
                    parent.List(i++)
                        .Add(0, equiJoinLabels.at(labelName))
                        .Atom(1, labelName)
                    .Seal();
                }
                return parent;
            })
            .List(i++)
                .Add(0, filteredInput)
                .Atom(1, innerJoinTree->ChildRef(2)->Content())
            .Seal()
            .Add(i++, innerJoinTree)
            .Add(i++, leftJoinSettings)
        .Seal()
    .Build();

    //then, do leftOnly join:

    i = 0;
    auto leftOnlyJoin = ctx.Builder(pos)
        .Callable("EquiJoin")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                for (const auto& [labelName, _] : leftSideJoinLabels) {
                    parent.List(i++)
                        .Add(0, equiJoinLabels.at(labelName))
                        .Atom(1, labelName)
                    .Seal();
                }
                return parent;
            })
            .List(i++)
                .Add(0, rightSideInput)
                .Atom(1, leftOnlyJoinTree->ChildRef(2)->Content())
            .Seal()
            .Add(i++, leftOnlyJoinTree)
            .Add(i++, leftJoinSettings)
        .Seal()
    .Build();


    //extend left only join with nulls as left part and apply same predicate
    auto nullPredicateFilter = ApplyJoinPredicate(
        predicate, /*filterInput=*/leftOnlyJoin, args, labels, {}, renameMap, onlyKeys,
        inputIndex, inputIndex, ordered, /*substituteWithNulls=*/true, /*forceOptional=*/false, ctx
    );

    //then unite the results;
    auto unionAll = ctx.Builder(pos)
        .Callable("UnionAll")
            .Add(0, innerJoin)
            .Add(1, nullPredicateFilter)
        .Seal()
        .Build();

    if (!parentJoinPtr) {
        return unionAll;
    }

    THashSet <TString> joinColumns;
    for (const auto& [labelName, _] : leftSideJoinLabels) {
        auto tableName = labels.FindInputIndex(labelName);
        YQL_ENSURE(tableName);
        for (auto column : labels.Inputs[*tableName].EnumerateAllColumns()) {
            joinColumns.emplace(std::move(column));
        }
    }
    auto rightSideTableName = labels.FindInputIndex(innerJoinTree->Child(2)->Content());
    YQL_ENSURE(rightSideTableName);
    for (auto column : labels.Inputs[*rightSideTableName].EnumerateAllColumns()) {
        joinColumns.emplace(std::move(column));
    }

    auto newJoinLabel = ctx.Builder(pos)
        .Atom("__yql_right_side_pushdown_input_label")
    .Build();


    TExprNode::TPtr remJoinKeys;
    bool changedLeftSide = false;
    if (leftJoinTree == parentJoinPtr->ChildPtr(1)) {
        changedLeftSide = true;
        remJoinKeys = parentJoinPtr->ChildPtr(3);
    } else {
        remJoinKeys = parentJoinPtr->ChildPtr(4);
    }

    TExprNode::TListType newKeys;
    newKeys.reserve(remJoinKeys->ChildrenSize());

    for (ui32 i = 0; i < remJoinKeys->ChildrenSize(); i += 2) {
        auto table = remJoinKeys->ChildPtr(i);
        auto column = remJoinKeys->ChildPtr(i + 1);

        YQL_ENSURE(table->IsAtom());
        YQL_ENSURE(column->IsAtom());

        auto fcn = FullColumnName(table->Content(), column->Content());

        if (joinColumns.contains(fcn)) {
        newKeys.push_back(newJoinLabel);
        newKeys.push_back(ctx.NewAtom(column->Pos(), fcn));
        } else {
            newKeys.push_back(table);
            newKeys.push_back(column);
        }
    }

    auto newKeysList = ctx.NewList(remJoinKeys->Pos(), std::move(newKeys));

    auto newParentJoin = ctx.Builder(joinTree->Pos())
        .List()
            .Add(0, parentJoinPtr->ChildPtr(0))
            .Add(1, changedLeftSide ? newJoinLabel : parentJoinPtr->ChildPtr(1))
            .Add(2, !changedLeftSide ? newJoinLabel : parentJoinPtr->ChildPtr(2))
            .Add(3, changedLeftSide ? newKeysList : parentJoinPtr->ChildPtr(3))
            .Add(4, !changedLeftSide ? newKeysList : parentJoinPtr->ChildPtr(4))
            .Add(5, parentJoinPtr->ChildPtr(5))
        .Seal()
        .Build();

    auto newJoinTree = ctx.ReplaceNode(std::move(joinTree), *parentJoinPtr, newParentJoin);

    i = 0;
    auto newJoinSettings = ctx.Builder(pos)
        .List()
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                for (const auto& child : equiJoin->TailPtr()->ChildrenList()) {
                    parent.Add(i++, child);
                }
                return parent;
            })
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                for (const auto& column : joinColumns) {
                    parent.List(i++)
                        .Atom(0, "rename")
                        .Atom(1, FullColumnName("__yql_right_side_pushdown_input_label", column))
                        .Atom(2, column)
                    .Seal();
                }
                return parent;
            })
        .Seal()
    .Build();

    i = 0;
    auto newEquiJoin = ctx.Builder(pos)
        .Callable("EquiJoin")
        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            for (const auto& label : joinLabelCounters) {
                if (label.second > 0) {
                    auto equiJoinInput = equiJoinLabels.at(TString(label.first));
                    parent.List(i++)
                        .Add(0, equiJoinInput)
                        .Atom(1, label.first)
                    .Seal();
                }
            }
            return parent;
        })
        .List(i++)
            .Add(0, unionAll)
            .Add(1, newJoinLabel)
        .Seal()
        .Add(i++, newJoinTree)
        .Add(i++, newJoinSettings)
        .Seal()
    .Build();

    return newEquiJoin;
}

class TJoinTreeRebuilder {
public:
    TJoinTreeRebuilder(TExprNode::TPtr joinTree, TStringBuf label1, TStringBuf column1, TStringBuf label2, TStringBuf column2,
        TExprContext& ctx, bool rotateJoinTree)
        : JoinTree(joinTree)
        , Labels{ label1, label2 }
        , Columns{ column1, column2 }
        , Ctx(ctx)
        , RotateJoinTree(rotateJoinTree)
    {}

    TExprNode::TPtr Run() {
        auto joinTree = JoinTree;
        if (RotateJoinTree) {
            joinTree = RotateCrossJoin(JoinTree->Pos(), JoinTree);
        }
        auto newJoinTree = std::get<0>(AddLink(joinTree));
        YQL_ENSURE(Updated);
        return newJoinTree;
    }

private:
    TExprNode::TPtr RotateCrossJoin(TPositionHandle pos, TExprNode::TPtr joinTree) {
        if (joinTree->Child(0)->Content() != "Cross") {
            auto children = joinTree->ChildrenList();
            auto& left = children[1];
            auto& right = children[2];

            if (!left->IsAtom()) {
                left = RotateCrossJoin(pos, left);
            }

            if (!right->IsAtom()) {
                right = RotateCrossJoin(pos, right);
            }

            return Ctx.ChangeChildren(*joinTree, std::move(children));
        }

        CrossJoins.clear();
        RestJoins.clear();
        GatherCross(joinTree);
        auto inCross1 = FindPtr(CrossJoins, Labels[0]);
        auto inCross2 = FindPtr(CrossJoins, Labels[1]);
        if (inCross1 || inCross2) {
            if (inCross1 && inCross2) {
                // make them a leaf
                joinTree = MakeCrossJoin(pos, Ctx.NewAtom(pos, Labels[0]), Ctx.NewAtom(pos, Labels[1]), Ctx);
                for (auto label : CrossJoins) {
                    if (label != Labels[0] && label != Labels[1]) {
                        joinTree = MakeCrossJoin(pos, joinTree, Ctx.NewAtom(pos, label), Ctx);
                    }
                }

                joinTree = AddRestJoins(pos, joinTree, nullptr);
            }
            else if (inCross1) {
                // leaf with table1 and subtree with table2
                auto rest = FindRestJoin(Labels[1]);
                YQL_ENSURE(rest);
                joinTree = MakeCrossJoin(pos, Ctx.NewAtom(pos, Labels[0]), rest, Ctx);
                for (auto label : CrossJoins) {
                    if (label != Labels[0]) {
                        joinTree = MakeCrossJoin(pos, joinTree, Ctx.NewAtom(pos, label), Ctx);
                    }
                }

                joinTree = AddRestJoins(pos, joinTree, rest);
            }
            else {
                // leaf with table2 and subtree with table1
                auto rest = FindRestJoin(Labels[0]);
                YQL_ENSURE(rest);
                joinTree = MakeCrossJoin(pos, Ctx.NewAtom(pos, Labels[1]), rest, Ctx);
                for (auto label : CrossJoins) {
                    if (label != Labels[1]) {
                        joinTree = MakeCrossJoin(pos, joinTree, Ctx.NewAtom(pos, label), Ctx);
                    }
                }

                joinTree = AddRestJoins(pos, joinTree, rest);
            }
        }

        return joinTree;
    }

    TExprNode::TPtr AddRestJoins(TPositionHandle pos, TExprNode::TPtr joinTree, TExprNode::TPtr exclude) {
        for (auto join : RestJoins) {
            if (join == exclude) {
                continue;
            }

            joinTree = MakeCrossJoin(pos, joinTree, join, Ctx);
        }

        return joinTree;
    }

    TExprNode::TPtr FindRestJoin(TStringBuf label) {
        for (auto join : RestJoins) {
            if (HasTable(join, label)) {
                return join;
            }
        }

        return nullptr;
    }

    bool HasTable(TExprNode::TPtr joinTree, TStringBuf label) {
        auto left = joinTree->ChildPtr(1);
        if (left->IsAtom()) {
            if (left->Content() == label) {
                return true;
            }
        }
        else {
            if (HasTable(left, label)) {
                return true;
            }
        }

        auto right = joinTree->ChildPtr(2);
        if (right->IsAtom()) {
            if (right->Content() == label) {
                return true;
            }
        }
        else {
            if (HasTable(right, label)) {
                return true;
            }
        }

        return false;
    }

    void GatherCross(TExprNode::TPtr joinTree) {
        auto type = joinTree->Child(0)->Content();
        if (type != "Cross") {
            RestJoins.push_back(joinTree);
            return;
        }

        auto left = joinTree->ChildPtr(1);
        if (left->IsAtom()) {
            CrossJoins.push_back(left->Content());
        }
        else {
            GatherCross(left);
        }

        auto right = joinTree->ChildPtr(2);
        if (right->IsAtom()) {
            CrossJoins.push_back(right->Content());
        }
        else {
            GatherCross(right);
        }
    }

    std::tuple<TExprNode::TPtr, TMaybe<ui32>, TMaybe<ui32>> AddLink(TExprNode::TPtr joinTree) {
        auto children = joinTree->ChildrenList();

        TMaybe<ui32> found1;
        TMaybe<ui32> found2;
        auto& left = children[1];
        if (!left->IsAtom()) {
            TMaybe<ui32> leftFound1, leftFound2;
            std::tie(left, leftFound1, leftFound2) = AddLink(left);
            if (leftFound1) {
                found1 = 1u;
            }

            if (leftFound2) {
                found2 = 1u;
            }
        }
        else {
            if (left->Content() == Labels[0]) {
                found1 = 1u;
            }

            if (left->Content() == Labels[1]) {
                found2 = 1u;
            }
        }

        auto& right = children[2];
        if (!right->IsAtom()) {
            TMaybe<ui32> rightFound1, rightFound2;
            std::tie(right, rightFound1, rightFound2) = AddLink(right);
            if (rightFound1) {
                found1 = 2u;
            }

            if (rightFound2) {
                found2 = 2u;
            }
        }
        else {
            if (right->Content() == Labels[0]) {
                found1 = 2u;
            }

            if (right->Content() == Labels[1]) {
                found2 = 2u;
            }
        }

        if (found1 && found2) {
            if (!Updated) {
                if (joinTree->Child(0)->Content() == "Cross") {
                    children[0] = Ctx.NewAtom(joinTree->Pos(), "Inner");
                }
                else {
                    YQL_ENSURE(joinTree->Child(0)->Content() == "Inner");
                }

                ui32 index1 = *found1 - 1; // 0/1
                ui32 index2 = 1 - index1;

                auto link1 = children[3]->ChildrenList();
                link1.push_back(Ctx.NewAtom(joinTree->Pos(), Labels[index1]));
                link1.push_back(Ctx.NewAtom(joinTree->Pos(), Columns[index1]));
                children[3] = Ctx.ChangeChildren(*children[3], std::move(link1));

                auto link2 = children[4]->ChildrenList();
                link2.push_back(Ctx.NewAtom(joinTree->Pos(), Labels[index2]));
                link2.push_back(Ctx.NewAtom(joinTree->Pos(), Columns[index2]));
                children[4] = Ctx.ChangeChildren(*children[4], std::move(link2));

                Updated = true;
            }
        }

        return { Ctx.ChangeChildren(*joinTree, std::move(children)), found1, found2 };
    }

private:
    TVector<TStringBuf> CrossJoins;
    TVector<TExprNode::TPtr> RestJoins;

    bool Updated = false;

    TExprNode::TPtr JoinTree;
    TStringBuf Labels[2];
    TStringBuf Columns[2];
    TExprContext& Ctx;
    bool RotateJoinTree;
};

TExprNode::TPtr DecayCrossJoinIntoInner(TExprNode::TPtr equiJoin, const TExprNode::TPtr& predicate,
    const TJoinLabels& labels, ui32 index1, ui32 index2,  const TExprNode& row, const THashMap<TString, TString>& backRenameMap,
    const TParentsMap& parentsMap, TExprContext& ctx, bool rotateJoinTree) {
    YQL_ENSURE(index1 != index2);
    TExprNode::TPtr left, right;
    if (!IsEquality(predicate, left, right)) {
        return equiJoin;
    }

    TSet<ui32> leftInputs, rightInputs;
    TSet<TStringBuf> usedFields;
    GatherJoinInputs(left, row, parentsMap, backRenameMap, labels, leftInputs, usedFields);
    GatherJoinInputs(right, row, parentsMap, backRenameMap, labels, rightInputs, usedFields);
    bool good = false;
    if (leftInputs.size() == 1 && rightInputs.size() == 1) {
        if (*leftInputs.begin() == index1 && *rightInputs.begin() == index2) {
            good = true;
        } else if (*leftInputs.begin() == index2 && *rightInputs.begin() == index1) {
            good = true;
        }
    }

    if (!good) {
        return equiJoin;
    }

    auto inputsCount = equiJoin->ChildrenSize() - 2;
    auto joinTree = equiJoin->Child(inputsCount);
    if (!IsRequiredSide(joinTree, labels, index1).first ||
        !IsRequiredSide(joinTree, labels, index2).first) {
        return equiJoin;
    }

    TStringBuf label1, column1, label2, column2;
    if (left->IsCallable("Member") && left->Child(0) == &row) {
        auto x = left->Tail().Content();
        if (auto ptr = backRenameMap.FindPtr(x)) {
            x = *ptr;
        }

        SplitTableName(x, label1, column1);
    } else {
        return equiJoin;
    }

    if (right->IsCallable("Member") && right->Child(0) == &row) {
        auto x = right->Tail().Content();
        if (auto ptr = backRenameMap.FindPtr(x)) {
            x = *ptr;
        }

        SplitTableName(x, label2, column2);
    } else {
        return equiJoin;
    }

    TJoinTreeRebuilder rebuilder(joinTree, label1, column1, label2, column2, ctx, rotateJoinTree);
    auto newJoinTree = rebuilder.Run();
    return ctx.ChangeChild(*equiJoin, inputsCount, std::move(newJoinTree));
}

bool NeedEmitSkipNullMembers(const TTypeAnnotationContext* types) {
    YQL_ENSURE(types);
    static const TString emitFlag = to_lower(TString("EmitSkipNullOnPushdown"));
    static const TString noEmitFlag = to_lower(TString("DisableEmitSkipNullOnPushdown"));
    if (types->OptimizerFlags.contains(emitFlag)) {
        return true;
    }
    if (types->OptimizerFlags.contains(noEmitFlag)) {
        return false;
    }
    return true;
}

bool IsEqualityFilterOverJoinEnabled(const TTypeAnnotationContext* types) {
    YQL_ENSURE(types);
    static const char flag[] = "EqualityFilterOverJoin";
    return IsOptimizerEnabled<flag>(*types) && !IsOptimizerDisabled<flag>(*types);
}

struct TExtraInputPredicates {
    TExprNode::TPtr Row;
    TExprNodeList Preds;
    TString MainColumn;
};

void AppendEquality(TPositionHandle pos, TExtraInputPredicates& dst, const TString& left, const TString& right, const TJoinLabel& label, TExprContext& ctx) {
    if (!dst.Row) {
        YQL_ENSURE(dst.Preds.empty());
        dst.Row = ctx.NewArgument(pos, "row");
    }

    TStringBuf lTable = label.TableName(left);
    TStringBuf rTable = label.TableName(right);
    TStringBuf lColumn = label.ColumnName(left);
    TStringBuf rColumn = label.ColumnName(right);

    dst.Preds.push_back(ctx.Builder(pos)
        .Callable("Coalesce")
            .Callable(0, "==")
                .Callable(0, "Member")
                    .Add(0, dst.Row)
                    .Atom(1, label.MemberName(lTable, lColumn))
                .Seal()
                .Callable(1, "Member")
                    .Add(0, dst.Row)
                    .Atom(1, label.MemberName(rTable, rColumn))
                .Seal()
            .Seal()
            .Add(1, MakeBool<false>(pos, ctx))
        .Seal()
        .Build()
    );
}

struct TJoinEqRebuildResult {
    TExprNode::TPtr JoinTree;
    TSet<ui32> InputsInScope;
};

struct TEqColumn {
    TString Name;
    size_t UseCount = 0;
};

template<typename T, typename U>
bool HasIntersection(const T& a, const U& b) {
    return AnyOf(a, [&b](const auto& item) { return b.contains(item); });
}

TVector<ui32> FilterByScope(const TMap<ui32, TEqColumn>& input, const TSet<ui32>& scope) {
    TVector<ui32> result;
    for (auto [i, _] : input) {
        if (scope.contains(i)) {
            result.push_back(i);
        }
    }
    return result;
}

TJoinEqRebuildResult RebuildJoinTreeForEquality(TVector<TMap<ui32, TEqColumn>>& equalitySetsByInput, const THashSet<ui32>& notNullInputs, const TJoinLabels& labels, TCoEquiJoinTuple joinTree, TExprContext& ctx) {
    const TStringBuf joinType = joinTree.Type().Value();

    TJoinEqRebuildResult left;
    if (joinType != "RightOnly" && joinType != "RightSemi") {
        if (auto maybeAtom = joinTree.LeftScope().Maybe<TCoAtom>()) {
            left.JoinTree = joinTree.LeftScope().Ptr();
            auto inputIdx = labels.FindInputIndex(maybeAtom.Cast().Value());
            YQL_ENSURE(inputIdx);
            left.InputsInScope.insert(*inputIdx);
        } else {
            left = RebuildJoinTreeForEquality(equalitySetsByInput, notNullInputs, labels, joinTree.LeftScope().Cast<TCoEquiJoinTuple>(), ctx);
        }
    } else {
        left.JoinTree = joinTree.LeftScope().Ptr();
    }

    TJoinEqRebuildResult right;
    if (joinType != "LeftOnly" && joinType != "LeftSemi") {
        if (auto maybeAtom = joinTree.RightScope().Maybe<TCoAtom>()) {
            right.JoinTree = joinTree.RightScope().Ptr();
            auto inputIdx = labels.FindInputIndex(maybeAtom.Cast().Value());
            YQL_ENSURE(inputIdx);
            right.InputsInScope.insert(*inputIdx);
        } else {
            right = RebuildJoinTreeForEquality(equalitySetsByInput, notNullInputs, labels, joinTree.RightScope().Cast<TCoEquiJoinTuple>(), ctx);
        }
    } else {
        right.JoinTree = joinTree.RightScope().Ptr();
    }

    YQL_ENSURE(!HasIntersection(left.InputsInScope, right.InputsInScope));

    if (joinType == "Exclusion" || !left.JoinTree || !right.JoinTree) {
        // TODO: support equality over exclustion join
        return {};
    }

    const bool leftNotNull = HasIntersection(left.InputsInScope, notNullInputs);
    const bool rightNotNull = HasIntersection(right.InputsInScope, notNullInputs);

    TStringBuf newJoinType = joinType;
    if (joinType == "Full") {
        if (leftNotNull && rightNotNull) {
            newJoinType = "Inner";
        } else if (leftNotNull) {
            newJoinType = "Left";
        } else if (rightNotNull) {
            newJoinType = "Right";
        }
    } else if (joinType == "Left" && rightNotNull || joinType == "Right" && leftNotNull) {
        newJoinType = "Inner";
    }

    TExprNodeList leftKeys = joinTree.LeftKeys().Ref().ChildrenList();
    TExprNodeList rightKeys = joinTree.RightKeys().Ref().ChildrenList();
    for (auto& es : equalitySetsByInput) {
        auto leftInputs = FilterByScope(es, left.InputsInScope);
        auto rightInputs = FilterByScope(es, right.InputsInScope);

        const size_t sz = std::min(leftInputs.size(), rightInputs.size());
        for (size_t i = 0; i < sz; ++i) {
            auto lIdx = leftInputs[i];
            auto rIdx = rightInputs[i];
            if (es[lIdx].UseCount && es[rIdx].UseCount) {
                continue;
            }
            es[lIdx].UseCount++;
            es[rIdx].UseCount++;

            TStringBuf table;
            TStringBuf column;

            SplitTableName(es[lIdx].Name, table, column);
            leftKeys.emplace_back(ctx.NewAtom(joinTree.LeftKeys().Pos(), table));
            leftKeys.emplace_back(ctx.NewAtom(joinTree.LeftKeys().Pos(), column));

            SplitTableName(es[rIdx].Name, table, column);
            rightKeys.emplace_back(ctx.NewAtom(joinTree.RightKeys().Pos(), table));
            rightKeys.emplace_back(ctx.NewAtom(joinTree.RightKeys().Pos(), column));

            if (newJoinType == "Cross") {
                newJoinType = "Inner";
            }
        }
    }

    TJoinEqRebuildResult result;
    result.JoinTree = Build<TCoEquiJoinTuple>(ctx, joinTree.Pos())
        .Type().Build(newJoinType)
        .LeftScope(left.JoinTree)
        .RightScope(right.JoinTree)
        .LeftKeys(ctx.NewList(joinTree.LeftKeys().Pos(), std::move(leftKeys)))
        .RightKeys(ctx.NewList(joinTree.RightKeys().Pos(), std::move(rightKeys)))
        .Options(joinTree.Options())
        .Done().Ptr();
    if (newJoinType != "RightSemi" && newJoinType != "RightOnly") {
        result.InputsInScope.insert(left.InputsInScope.begin(), left.InputsInScope.end());
    }
    if (newJoinType != "LeftSemit" && newJoinType != "LeftOnly") {
        result.InputsInScope.insert(right.InputsInScope.begin(), right.InputsInScope.end());
    }
    return result;
}


TExprBase HandleEqualityFilterOverJoin(const TCoFlatMapBase& node, const TJoinLabels& labels,
    const THashMap<TString, TString>& backRenameMap, TExprContext& ctx)
{
    const auto& row = node.Lambda().Args().Arg(0).Ref();
    auto predicate = node.Lambda().Body().Ref().ChildPtr(0);

    TExprNodeList andComponents;
    if (predicate->IsCallable("And")) {
        andComponents = predicate->ChildrenList();
    } else {
        andComponents.push_back(predicate);
    }

    TExprNodeList rest;

    TVector<TString> columns;
    TVector<TString> uniqColumns;
    THashMap<TString, size_t> column2id;
    THashSet<ui32> makeNoNullInputs;
    for (auto pred : andComponents) {
        TExprNode::TPtr left, right;
        if (!IsEquality(pred, left, right) ||
            !left->IsCallable("Member") || left->Child(0) != &row ||
            !right->IsCallable("Member") || right->Child(0) != &row)
        {
            rest.push_back(pred);
            continue;
        }

        TString leftCol{left->Child(1)->Content()};
        if (auto it = backRenameMap.find(leftCol); it != backRenameMap.end()) {
            leftCol = it->second;
        }

        TString rightCol{right->Child(1)->Content()};
        if (auto it = backRenameMap.find(rightCol); it != backRenameMap.end()) {
            rightCol = it->second;
        }

        if (leftCol == rightCol) {
            // TODO: add optimizer for "==" over same arguments with optional types
            rest.push_back(pred);
            continue;
        }

        TStringBuf leftTable, rightTable;
        TStringBuf column;
        SplitTableName(leftCol, leftTable, column);
        SplitTableName(rightCol, rightTable, column);

        const auto leftInput = labels.FindInputIndex(leftTable);
        YQL_ENSURE(leftInput);
        const auto rightInput = labels.FindInputIndex(rightTable);
        YQL_ENSURE(rightInput);

        makeNoNullInputs.insert(*leftInput);
        makeNoNullInputs.insert(*rightInput);

        auto processColumn = [&](const TString& col) {
            columns.push_back(col);
            if (column2id.insert({ col, uniqColumns.size() }).second) {
                uniqColumns.push_back(col);
            }
        };

        processColumn(leftCol);
        processColumn(rightCol);
    }

    if (columns.empty()) {
        return node;
    }

    YQL_ENSURE(columns.size() % 2 == 0);

    TDisjointSets ds(uniqColumns.size());
    for (size_t i = 0; i < columns.size(); i += 2) {
        ds.UnionSets(column2id[columns[i]], column2id[columns[i + 1]]);
    }

    TVector<TSet<TString>> equalitySets(uniqColumns.size());
    for (const auto& col : uniqColumns) {
        equalitySets[ds.CanonicSetElement(column2id[col])].insert(col);
    }

    EraseIf(equalitySets, [](const auto& s) { return s.empty(); });
    YQL_ENSURE(!equalitySets.empty());

    const TCoEquiJoin equiJoin = node.Input().Cast<TCoEquiJoin>();
    const size_t inputsCount = equiJoin.ArgCount() - 2;
    YQL_ENSURE(labels.Inputs.size() == inputsCount);

    TVector<TMap<ui32, TEqColumn>> equalitySetsByInput; // single column for each input (other instances are pushed directly to input)
    TVector<TExtraInputPredicates> extraInputPreds(inputsCount);

    for (const TSet<TString>& eqSet : equalitySets) {
        TMap<ui32, TEqColumn>& eqSetByInput = equalitySetsByInput.emplace_back();
        for (const auto& col : eqSet) {
            YQL_ENSURE(!col.empty());

            TStringBuf table;
            TStringBuf column;
            SplitTableName(col, table, column);
            auto idx = labels.FindInputIndex(table);
            YQL_ENSURE(idx && *idx < inputsCount);

            auto it = eqSetByInput.find(*idx);
            if (it != eqSetByInput.end()) {
                YQL_ENSURE(col != it->second.Name);
                const auto& label = labels.Inputs[*idx];
                extraInputPreds[*idx].MainColumn = it->second.Name;
                AppendEquality(predicate->Pos(), extraInputPreds[*idx], it->second.Name, col, label, ctx);
            } else {
                eqSetByInput.insert({*idx, {col, 0}});
            }
        }
    }

    auto res = RebuildJoinTreeForEquality(equalitySetsByInput, makeNoNullInputs, labels, equiJoin.Arg(inputsCount).Cast<TCoEquiJoinTuple>(), ctx);
    if (!res.JoinTree) {
        return node;
    }

    for (const TMap<ui32, TEqColumn>& es : equalitySetsByInput) {
        for (auto& [_, eqCol] : es) {
            YQL_ENSURE(eqCol.UseCount || AnyOf(extraInputPreds, [&](const TExtraInputPredicates& item) { return item.MainColumn == eqCol.Name; } ));
        }
    }

    YQL_CLOG(DEBUG, Core) << "Equality filter over EquiJoin: processed " << (columns.size() / 2) << " predicates";

    TExprNodeList equiJoinArgs = equiJoin.Ref().ChildrenList();
    equiJoinArgs[inputsCount] = res.JoinTree;
    for (size_t i = 0; i < inputsCount; ++i) {
        auto& toPush = extraInputPreds[i];
        if (toPush.Preds.empty()) {
            continue;
        }
        YQL_ENSURE(toPush.Row);

        const auto pos = toPush.Row->Pos();
        TExprNode::TPtr pred = ctx.NewCallable(pos, "And", std::move(toPush.Preds));

        TExprNode::TPtr& inputTuple = equiJoinArgs[i];
        TExprNode::TPtr oldInput = inputTuple->ChildPtr(TCoEquiJoinInput::idx_List);
        auto newInput = ctx.Builder(oldInput->Pos())
            .Callable("OrderedFilter")
                .Add(0, oldInput)
                .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, { toPush.Row }), std::move(pred)))
            .Seal()
            .Build();

        inputTuple = ctx.ChangeChild(*inputTuple, TCoEquiJoinInput::idx_List, std::move(newInput));
    }

    auto origJoinItemTypeNode = ExpandType(equiJoin.Pos(), *GetSeqItemType(*node.Input().Ref().GetTypeAnn()).Cast<TStructExprType>(), ctx);
    auto newEquiJoin = ctx.Builder(equiJoin.Pos())
        .Callable(node.CallableName() == "OrderedFlatMap" ? "OrderedMap" : "Map")
            .Add(0, ctx.NewCallable(equiJoin.Pos(), "EquiJoin", std::move(equiJoinArgs)))
            .Lambda(1)
                .Param("row")
                .Callable("EnsureType")
                    .Callable(0, "SafeCast")
                        .Arg(0, "row")
                        .Add(1, origJoinItemTypeNode)
                    .Seal()
                    .Add(1, origJoinItemTypeNode)
                    .Atom(2, "Mismatch type while performing Equality over EquiJoin optimizer")
                .Seal()
            .Seal()
        .Seal()
        .Build();

    if (rest.empty()) {
        rest.push_back(MakeBool<true>(predicate->Pos(), ctx));
    }

    YQL_ENSURE(TCoConditionalValueBase::Match(node.Lambda().Body().Raw()));
    auto newPred = ctx.NewCallable(predicate->Pos(), "And", std::move(rest));
    auto newCond = ctx.ChangeChild(node.Lambda().Body().Ref(), TCoConditionalValueBase::idx_Predicate, std::move(newPred));
    auto newLambda = ctx.ChangeChild(node.Lambda().Ref(), TCoLambda::idx_Body, std::move(newCond));

    return TExprBase(ctx.Builder(node.Pos())
        .Callable(node.CallableName())
            .Add(0, newEquiJoin)
            .Lambda(1)
                .Param("row")
                .Apply(newLambda)
                    .With(0, "row")
                .Seal()
            .Seal()
        .Seal()
        .Build());
}

} // namespace

TExprBase FlatMapOverEquiJoin(
    const TCoFlatMapBase& node,
    TExprContext& ctx,
    const TParentsMap& parentsMap,
    bool multiUsage,
    const TTypeAnnotationContext* types
) {
    auto equiJoin = node.Input();

    auto structType = equiJoin.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()
        ->Cast<TStructExprType>();
    if (structType->GetSize() == 0) {
        return node;
    }

    TExprNode::TPtr structNode;
    if (!multiUsage && IsRenameFlatMap(node, structNode)) {
        YQL_CLOG(DEBUG, Core) << "Rename in " << node.CallableName() << " over EquiJoin";
        auto joinSettings = equiJoin.Ref().ChildPtr(equiJoin.Ref().ChildrenSize() - 1);
        auto renameMap = LoadJoinRenameMap(*joinSettings);
        joinSettings = RemoveSetting(*joinSettings, "rename", ctx);
        auto structType = equiJoin.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()
            ->Cast<TStructExprType>();
        THashSet<TStringBuf> usedFields;
        TMap<TStringBuf, TVector<TStringBuf>> memberUsageMap;
        for (auto& child : structNode->Children()) {
            auto item = child->Child(1);
            usedFields.insert(item->Child(1)->Content());
            memberUsageMap[item->Child(1)->Content()].push_back(child->Child(0)->Content());
        }

        TMap<TStringBuf, TStringBuf> reversedRenameMap;
        TMap<TStringBuf, TVector<TStringBuf>> newRenameMap;
        for (auto& x : renameMap) {
            if (!x.second.empty()) {
                for (auto& y : x.second) {
                    reversedRenameMap[y] = x.first;
                }
            }
            else {
                // previous drops
                newRenameMap[x.first].clear();
            }
        }

        for (auto& x : structType->GetItems()) {
            if (!usedFields.contains(x->GetName())) {
                // new drops
                auto name = x->GetName();
                if (auto renamed = reversedRenameMap.FindPtr(name)) {
                    name = *renamed;
                }

                newRenameMap[name].clear();
            }
        }

        for (auto& x : memberUsageMap) {
            auto prevName = x.first;
            if (auto renamed = reversedRenameMap.FindPtr(prevName)) {
                prevName = *renamed;
            }

            for (auto& y : x.second) {
                newRenameMap[prevName].push_back(y);
            }
        }

        TExprNode::TListType joinSettingsNodes = joinSettings->ChildrenList();
        AppendEquiJoinRenameMap(node.Pos(), newRenameMap, joinSettingsNodes, ctx);
        joinSettings = ctx.ChangeChildren(*joinSettings, std::move(joinSettingsNodes));
        auto ret = ctx.ShallowCopy(equiJoin.Ref());
        ret->ChildRef(ret->ChildrenSize() - 1) = joinSettings;
        return TExprBase(ret);
    }

    TSet<TStringBuf> usedFields;
    auto& arg = node.Lambda().Args().Arg(0).Ref();
    auto body = node.Lambda().Body().Ptr();
    if (!multiUsage && HaveFieldsSubset(body, arg, usedFields, parentsMap)) {
        YQL_CLOG(DEBUG, Core) << "FieldsSubset in " << node.CallableName() << " over EquiJoin";
        auto joinSettings = equiJoin.Ref().ChildPtr(equiJoin.Ref().ChildrenSize() - 1);
        auto renameMap = LoadJoinRenameMap(*joinSettings);
        joinSettings = RemoveSetting(*joinSettings, "rename", ctx);
        auto newRenameMap = UpdateUsedFieldsInRenameMap(renameMap, usedFields, structType);
        auto newLambda = ctx.Builder(node.Pos())
            .Lambda()
            .Param("item")
                .ApplyPartial(node.Lambda().Args().Ptr(), body).With(0, "item").Seal()
            .Seal()
            .Build();

        TExprNode::TListType joinSettingsNodes = joinSettings->ChildrenList();
        AppendEquiJoinRenameMap(node.Pos(), newRenameMap, joinSettingsNodes, ctx);
        joinSettings = ctx.ChangeChildren(*joinSettings, std::move(joinSettingsNodes));
        auto updatedEquiJoin = ctx.ShallowCopy(equiJoin.Ref());
        updatedEquiJoin->ChildRef(updatedEquiJoin->ChildrenSize() - 1) = joinSettings;

        return TExprBase(ctx.Builder(node.Pos())
            .Callable(node.CallableName())
                .Add(0, updatedEquiJoin)
                .Add(1, newLambda)
            .Seal()
            .Build());
    }

    if (IsPredicateFlatMap(node.Lambda().Body().Ref())) {
        // predicate pushdown
        const auto& row = node.Lambda().Args().Arg(0).Ref();
        auto predicate = node.Lambda().Body().Ref().ChildPtr(0);
        auto value = node.Lambda().Body().Ref().ChildPtr(1);
        TJoinLabels labels;
        for (ui32 i = 0; i < equiJoin.Ref().ChildrenSize() - 2; ++i) {
            auto err = labels.Add(ctx, *equiJoin.Ref().Child(i)->Child(1),
                equiJoin.Ref().Child(i)->Child(0)->GetTypeAnn()->Cast<TListExprType>()
                ->GetItemType()->Cast<TStructExprType>());
            if (err) {
                ctx.AddError(*err);
                return TExprBase(nullptr);
            }
        }

        const auto joinSettings = equiJoin.Ref().Child(equiJoin.Ref().ChildrenSize() - 1);
        const auto renameMap = LoadJoinRenameMap(*joinSettings);
        THashMap<TString, TString> backRenameMap;
        for (auto& x : renameMap) {
            if (!x.second.empty()) {
                for (auto& y : x.second) {
                    backRenameMap[y] = x.first;
                }
            }
        }

        if (IsEqualityFilterOverJoinEnabled(types)) {
            auto newNode = HandleEqualityFilterOverJoin(node, labels, backRenameMap, ctx);
            if (newNode.Raw() != node.Raw()) {
                return newNode;
            }
        }

        TExprNode::TListType andTerms;
        bool isPg;
        GatherAndTerms(predicate, andTerms, isPg, ctx);
        TExprNode::TPtr ret;
        TExprNode::TPtr extraPredicate;

        const bool ordered = node.Maybe<TCoOrderedFlatMap>().IsValid();
        const bool skipNulls = NeedEmitSkipNullMembers(types);

        for (const auto& andTerm : andTerms) {
            if (andTerm->IsCallable("Likely")) {
                continue;
            }

            TSet<ui32> inputs;
            GatherJoinInputs(andTerm, row, parentsMap, backRenameMap, labels, inputs, usedFields);

            if (!multiUsage && inputs.size() == 0) {
                YQL_CLOG(DEBUG, Core) << "ConstantPredicatePushdownOverEquiJoin";
                ret = ConstantPredicatePushdownOverEquiJoin(equiJoin.Ptr(), andTerm, ordered, ctx);
                extraPredicate = FuseAndTerms(node.Pos(), andTerms, andTerm, isPg, ctx);
                break;
            }

            if (!multiUsage && inputs.size() == 1) {
                auto newJoin = SingleInputPredicatePushdownOverEquiJoin(equiJoin.Ptr(), andTerm, usedFields,
                    node.Lambda().Args().Ptr(), labels, *inputs.begin(), renameMap, ordered, skipNulls, ctx);
                if (newJoin != equiJoin.Ptr()) {
                    YQL_CLOG(DEBUG, Core) << "SingleInputPredicatePushdownOverEquiJoin";
                    ret = newJoin;
                    extraPredicate = FuseAndTerms(node.Pos(), andTerms, andTerm, isPg, ctx);
                    break;
                } else if (types->FilterPushdownOverJoinOptionalSide) {
                    auto twoJoins = FilterPushdownOverJoinOptionalSide(equiJoin.Ptr(), andTerm, usedFields,
                        node.Lambda().Args().Ptr(), labels, *inputs.begin(), renameMap, ordered, skipNulls, ctx, node.Pos());
                    if (twoJoins != equiJoin.Ptr()) {
                        YQL_CLOG(DEBUG, Core) << "RightSidePredicatePushdownOverLeftJoin";
                        ret = twoJoins;
                        extraPredicate = FuseAndTerms(node.Pos(), andTerms, andTerm, isPg, ctx);
                        break;
                    }

                }
            }

            if (!IsEqualityFilterOverJoinEnabled(types) && inputs.size() == 2) {
                auto newJoin = DecayCrossJoinIntoInner(equiJoin.Ptr(), andTerm,
                    labels, *inputs.begin(), *(++inputs.begin()), row, backRenameMap, parentsMap, ctx, types->RotateJoinTree);
                if (newJoin != equiJoin.Ptr()) {
                    YQL_CLOG(DEBUG, Core) << "DecayCrossJoinIntoInner";
                    ret = newJoin;
                    extraPredicate = FuseAndTerms(node.Pos(), andTerms, andTerm, isPg, ctx);
                    break;
                }
            }
        }

        if (!ret) {
            return node;
        }

        if (extraPredicate) {
            ret = ctx.Builder(node.Pos())
                .Callable(ordered ? "OrderedFilter" : "Filter")
                    .Add(0, std::move(ret))
                    .Lambda(1)
                        .Param("item")
                        .ApplyPartial(node.Lambda().Args().Ptr(), std::move(extraPredicate)).WithNode(row, "item").Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        if (value != &row) {
            TString name = node.Lambda().Body().Ref().Content().StartsWith("Flat") ? "FlatMap" : "Map";
            if (ordered) {
                name.prepend("Ordered");
            }
            ret = ctx.Builder(node.Pos())
                .Callable(name)
                    .Add(0, std::move(ret))
                    .Lambda(1)
                        .Param("item")
                        .ApplyPartial(node.Lambda().Args().Ptr(), std::move(value)).With(0, "item").Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        return TExprBase(ret);
    }

    return node;
}

}
