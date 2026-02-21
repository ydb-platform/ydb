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

struct TGatherOptionalKeyColumnsOptions {
    const TJoinLabels& Labels;
    ui32 InputIndex;
    bool WithInnerOptionals;
    bool BothSides;
};

void GatherOptionalKeyColumnsFromEquality(
    TExprNode::TPtr columns,
    TSet<TString>& optionalKeyColumns,
    const TGatherOptionalKeyColumnsOptions& options
) {
    for (ui32 i = 0; i < columns->ChildrenSize(); i += 2) {
        const auto table = columns->Child(i)->Content();
        if (*options.Labels.FindInputIndex(table) == options.InputIndex) {
            const auto column = columns->Child(i + 1)->Content();
            const auto type = *options.Labels.FindColumn(table, column);
            const bool isOptional = options.WithInnerOptionals ? type->HasOptionalOrNull() : type->GetKind() == ETypeAnnotationKind::Optional;
            if (isOptional) {
                optionalKeyColumns.insert(FullColumnName(table, column));
            }
        }
    }
}

void GatherOptionalKeyColumns(
    TExprNode::TPtr joinTree,
    TSet<TString>& optionalKeyColumns,
    const TGatherOptionalKeyColumnsOptions& options
) {
    auto left = joinTree->Child(1);
    auto right = joinTree->Child(2);
    if (!left->IsAtom()) {
        GatherOptionalKeyColumns(left, optionalKeyColumns, options);
    }

    if (!right->IsAtom()) {
        GatherOptionalKeyColumns(right, optionalKeyColumns, options);
    }

    const auto joinType = joinTree->Child(0)->Content();
    const auto leftColumns = joinTree->Child(3);
    const auto rightColumns = joinTree->Child(4);

    if (joinType == "Inner" || joinType == "LeftSemi") {
        GatherOptionalKeyColumnsFromEquality(leftColumns, optionalKeyColumns, options);
    }

    if (joinType == "Inner" || joinType == "RightSemi") {
        GatherOptionalKeyColumnsFromEquality(rightColumns, optionalKeyColumns, options);
    }

    if (options.BothSides) {
        if (joinType == "Right" || joinType == "RightSemi" || joinType == "RightOnly") {
            GatherOptionalKeyColumnsFromEquality(leftColumns, optionalKeyColumns, options);
        }

        if (joinType == "Left" || joinType == "LeftSemi" || joinType == "LeftOnly") {
            GatherOptionalKeyColumnsFromEquality(rightColumns, optionalKeyColumns, options);
        }
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

bool NeedEmitSkipNullMembers(const TTypeAnnotationContext* types) {
    YQL_ENSURE(types);
    static const char Flag[] = "EmitSkipNullOnPushdown";
    return IsOptimizerEnabled<Flag>(*types) || !IsOptimizerDisabled<Flag>(*types);
}

bool IsPredicatePushdownOverEquiJoinBothSides(const TTypeAnnotationContext* types) {
    YQL_ENSURE(types);
    static const char Flag[] = "PredicatePushdownOverEquiJoinBothSides";
    return IsOptimizerEnabled<Flag>(*types) && !IsOptimizerDisabled<Flag>(*types);
}

TExprNode::TPtr SingleInputPredicatePushdownOverEquiJoin(
    TExprNode::TPtr equiJoin,
    TExprNode::TPtr predicate,
    const TSet<TStringBuf>& usedFields,
    TExprNode::TPtr args,
    const TJoinLabels& labels,
    ui32 firstCandidate,
    const TMap<TStringBuf, TVector<TStringBuf>>& renameMap,
    bool ordered,
    TExprContext& ctx,
    const TTypeAnnotationContext* types
) {
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


    const bool skipNullsEnabled = NeedEmitSkipNullMembers(types);
    const bool pushdownBothSides = IsPredicatePushdownOverEquiJoinBothSides(types);

    auto ret = ctx.ShallowCopy(*equiJoin);
    for (const ui32 inputIndex : candidates) {
        auto [required, skipNullsPossible] = IsRequiredSide(joinTree, labels, inputIndex);
        if (!pushdownBothSides && !required) {
            continue;
        }
        YQL_ENSURE(required || onlyKeys);

        if (!isStrict && IsRequiredAndFilteredSide(joinTree, labels, inputIndex)) {
            continue;
        }

        // TODO: Remove IsRequiredSide.second after enabling PredicatePushdownOverEquiJoinBothSides
        if (pushdownBothSides) {
            skipNullsPossible = true;
        }

        auto newInput = equiJoin->Child(inputIndex)->ChildPtr(0);
        if (skipNullsPossible && skipNullsEnabled) {
            // skip null key columns
            TSet<TString> optionalKeyColumns;
            GatherOptionalKeyColumns(
                joinTree,
                optionalKeyColumns,
                {
                    .Labels = labels,
                    .InputIndex = inputIndex,
                    .WithInnerOptionals = IsSkipNullsUnessential(types),
                    .BothSides = pushdownBothSides,
                }
            );
            newInput = FilterOutNullJoinColumns(
                predicate->Pos(),
                newInput,
                labels.Inputs[inputIndex],
                optionalKeyColumns,
                ordered,
                types,
                ctx
            );
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

TExprNode::TListType ExtractOrPredicatesOverEquiJoin(const TExprNode::TPtr& predicate,
    const TExprNode& row, const THashMap<TString, TString>& backRenameMap,
    const TJoinLabels& labels, TExprContext& ctx, const TTypeAnnotationContext& types)
{
    if (!predicate->IsCallable("Or")) {
        return {};
    }

    auto orTerms = predicate->Children();
    if (!AnyOf(orTerms, [](const auto& orTerm) { return orTerm->IsCallable("And"); })) {
        return {};
    }

    TNodeMap<TSet<ui32>> joinInputs;
    if (!GatherJoinInputsForAllNodes(predicate, row, backRenameMap, labels, joinInputs)) {
        // non-Member usages of row struct
        return {};
    }
    auto it = joinInputs.find(predicate.Get());
    YQL_ENSURE(it != joinInputs.end());
    auto& predicateInputs = it->second;

    THashMap<ui32, TExprNode::TListType> extractedPredicatesByInput;
    TExprNode::TListType extractedConstantPredicates;

    THashSet<ui32> forbiddenInputs;
    bool constantPredicateForbidden = false;

    size_t expansionSize = 0;
    TDeque<TExprNode::TPtr> orTermsToProcess(orTerms.begin(), orTerms.end());
    while (!orTermsToProcess.empty()) {
        auto processOrTerm = [&]() {
            auto orTerm = orTermsToProcess.front();
            orTermsToProcess.pop_front();

            TSet<ui32> orTermInputs;
            THashMap<ui32, TExprNode::TListType> orTermExtractedPredicatesByInput;
            TExprNode::TListType orTermExtractedConstantPredicates;
            bool seenConstantPredicatesInOrTerm = false;

            TExprNode::TListType innerAndTerms;
            GetAndTerms(orTerm, innerAndTerms);
            for (const auto& innerAndTerm : innerAndTerms) {
                auto it = joinInputs.find(innerAndTerm.Get());
                YQL_ENSURE(it != joinInputs.end());
                auto& innerInputs = it->second;

                if (!IsStrict(innerAndTerm)) {
                    // Non-strict predicate can't be pushed down
                    forbiddenInputs.insert(innerInputs.begin(), innerInputs.end());
                    continue;
                }

                if (innerInputs.empty()) {
                    orTermExtractedConstantPredicates.push_back(innerAndTerm);
                    seenConstantPredicatesInOrTerm = true;
                } else if (innerInputs.size() == 1) {
                    ui32 input = *innerInputs.begin();
                    orTermExtractedPredicatesByInput[input].push_back(innerAndTerm);
                    orTermInputs.insert(input);
                } else {
                    auto expanded = ExpandAndOverOr(orTerm, ctx, types);
                    if (!expanded.empty() && expansionSize + expanded.size() <= types.AndOverOrExpansionLimit) {
                        // Update orTermsToProcess with terms after expansion and repeat
                        for (auto it = expanded.rbegin(); it != expanded.rend(); it++) {
                            orTermsToProcess.push_front(std::move(*it));
                            // joinInputs update is not needed
                            // as innerAndTerm can't be newly constructed node
                        }
                        expansionSize += expanded.size();
                        return;
                    }

                    // Predicates with multiple inputs cannot be handled
                    forbiddenInputs.insert(innerInputs.begin(), innerInputs.end());
                }
            }

            if (orTermInputs.size() < predicateInputs.size()) {
                for (ui32 input : predicateInputs) {
                    if (!orTermInputs.contains(input)) {
                        // Can't construct narrowing predicate for the input
                        // if some OR term doesn't impose any conditions for that input
                        forbiddenInputs.insert(input);
                    }
                }
            }
            for (auto& [input, terms]: orTermExtractedPredicatesByInput) {
                if (!forbiddenInputs.contains(input)) {
                    auto& predicates = extractedPredicatesByInput[input];
                    predicates.insert(predicates.end(), terms.begin(), terms.end());
                }
            }

            if (!constantPredicateForbidden && !seenConstantPredicatesInOrTerm) {
                // Can't construct narrowing constant predicate
                // if some OR term doesn't impose any conditions independent from inputs
                constantPredicateForbidden = true;
            }
            if (!constantPredicateForbidden) {
                extractedConstantPredicates.insert(
                    extractedConstantPredicates.end(),
                    orTermExtractedConstantPredicates.begin(),
                    orTermExtractedConstantPredicates.end()
                );
            }
        };
        processOrTerm();
    }

    auto trueLiteral = Build<TCoBool>(ctx, predicate->Pos())
        .Literal().Build("true")
        .Done().Ptr();

    TExprNode::TListType resultingPredicates;
    for (ui32 input : predicateInputs) {
        auto& predicates = extractedPredicatesByInput[input];
        if (!predicates.empty() && !forbiddenInputs.contains(input)) {
            resultingPredicates.push_back(ctx.NewCallable(predicate->Pos(), "Unessential", {ctx.NewCallable(predicate->Pos(), "Or", std::move(predicates)), trueLiteral}));
        }
    }
    if (!constantPredicateForbidden && !extractedConstantPredicates.empty()) {
        resultingPredicates.push_back(ctx.NewCallable(predicate->Pos(), "Unessential", {ctx.NewCallable(predicate->Pos(), "Or", std::move(extractedConstantPredicates)), trueLiteral}));
    }

    return resultingPredicates;
}

void CountLabelsInputUsage(TExprNode::TPtr joinTree, THashMap<TString, int>& counters) {
    if (joinTree->IsAtom()) {
        counters[joinTree->Content()]++;
    } else {
        CountLabelsInputUsage(joinTree->ChildPtr(1), counters);
        CountLabelsInputUsage(joinTree->ChildPtr(2), counters);
    }
}

void CollectJoinLabels(TExprNode::TPtr joinTree, THashSet<TString> &labels) {
    if (joinTree->IsAtom()) {
        labels.emplace(joinTree->Content());
    } else {
        CollectJoinLabels(joinTree->ChildPtr(1), labels);
        CollectJoinLabels(joinTree->ChildPtr(2), labels);
    }
}

void DecrementCountLabelsInputUsage(TExprNode::TPtr joinTree, THashMap<TString, int>& counters) {
    if (joinTree->IsAtom()) {
        counters[joinTree->Content()]--;
    } else {
        DecrementCountLabelsInputUsage(joinTree->ChildPtr(1), counters);
        DecrementCountLabelsInputUsage(joinTree->ChildPtr(2), counters);
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

// Maps the given `labelNames` collected from join tree to `joinLabels` associated with `EquiJoin`.
TVector<std::pair<THashSet<TString>, TExprNode::TPtr>> MapLabelNamesToJoinLabels(const TVector<std::pair<THashSet<TString>, TExprNode::TPtr>>& joinLabels,
                                                                                 const THashSet<TString>& labelNames) {
    const ui32 joinLabelSize = joinLabels.size();
    TVector<bool> taken(joinLabelSize, false);
    TVector<std::pair<THashSet<TString>, TExprNode::TPtr>> result;

    // We could have a situation with multiple labels associated with one set of join keys, so we want to match it ones.
    for (const auto& labelName : labelNames) {
        for (ui32 i = 0; i < joinLabelSize; ++i) {
            const auto& labelNamesSet = joinLabels[i].first;
            if (!taken[i] && labelNamesSet.count(labelName)) {
                result.push_back(joinLabels[i]);
                taken[i] = true;
            }
        }
    }
    return result;
}

// Combines labels from the given `labels` vector to one hash set.
THashSet<TString> CombineLabels(const TVector<std::pair<THashSet<TString>, TExprNode::TPtr>>& labels) {
    THashSet<TString> combinedResult;
    for (const auto &[labelNames, _] : labels) {
        combinedResult.insert(labelNames.begin(), labelNames.end());
    }
    return combinedResult;
}

// Creates a list from the given `labels`.
TExprNode::TPtr CreateLabelList(const THashSet<TString>& labels, TExprContext& ctx, const TPositionHandle& position) {
    TExprNode::TListType newKeys;
    for (const auto& label : labels) {
        newKeys.push_back(ctx.NewAtom(position, label));
    }
    return ctx.NewList(position, std::move(newKeys));
}

bool FilterPushdownOverJoinOptionalSideIgnoreOnlyKeys(const TTypeAnnotationContext* types) {
    YQL_ENSURE(types);
    static const char Flag[] = "FilterPushdownOverJoinOptionalSideIgnoreOnlyKeys";
    return IsOptimizerEnabled<Flag>(*types) && !IsOptimizerDisabled<Flag>(*types);
}

TExprNode::TPtr FilterPushdownOverJoinOptionalSide(
    TExprNode::TPtr equiJoin,
    TExprNode::TPtr predicate,
    const TSet<TStringBuf>& usedFields,
    TExprNode::TPtr args,
    const TJoinLabels& labels,
    ui32 inputIndex,
    const TMap<TStringBuf, TVector<TStringBuf>>& renameMap,
    bool ordered,
    TExprContext& ctx,
    const TTypeAnnotationContext* types,
    const TPositionHandle& pos
) {
    auto inputsCount = equiJoin->ChildrenSize() - 2;
    auto joinTree = equiJoin->Child(inputsCount);

    auto [leftJoinTree, parentJoinPtr] = IsRightSideForLeftJoin(joinTree, labels, inputIndex);
    if (!leftJoinTree) {
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

    bool onlyKeys = false;
    // ignoreOnlyKeys (aka FilterPushdownOverJoinOptionalSideIgnoreOnlyKeys) was added to canonize ydb tests without breaking them
    if (!FilterPushdownOverJoinOptionalSideIgnoreOnlyKeys(types)) {
        // TODO: Remove this after all YDB tests are properly canonized. See YQL-19896 for details.

        // check whether some used fields are not aliased
        onlyKeys = true;
        for (auto& x : usedFields) {
            if (!aliases.contains(TString(x))) {
                onlyKeys = false;
                break;
            }
        }

        if (onlyKeys) {
            return equiJoin;
        }
    }

    THashMap<TString, TExprNode::TPtr> equiJoinLabels;
    // Stores labels as hash set and associated join input.
    TVector<std::pair<THashSet<TString>, TExprNode::TPtr>> joinLabels;
    for (size_t i = 0; i < equiJoin->ChildrenSize() - 2; i++) {
        auto label = equiJoin->Child(i);
        THashSet<TString> labelsName;
        if (auto value = TMaybeNode<TCoAtom>(label->Child(1))) {
            labelsName.emplace(value.Cast().Value());
            equiJoinLabels.emplace(value.Cast().Value(), label->ChildPtr(0));
        } else if (auto tuple = TMaybeNode<TCoAtomList>(label->Child(1))) {
            for (const auto& value : tuple.Cast()) {
                labelsName.emplace(value.Value());
                equiJoinLabels.emplace(value.Value(), label->ChildPtr(0));
            }
        }
        joinLabels.push_back({labelsName, label->ChildPtr(0)});
    }

    THashMap<TString, int> joinLabelCounters;
    CountLabelsInputUsage(joinTree, joinLabelCounters);

    YQL_ENSURE(leftJoinTree);
    // Left child of the `leftJoinTree` could be a tree, need to walk and decrement them all, the do not need be at fina EquiJoin.
    DecrementCountLabelsInputUsage(leftJoinTree, joinLabelCounters);

    const auto joinSettings = equiJoin->TailPtr();
    const auto innerSettings = parentJoinPtr ? RemoveSetting(*joinSettings, "rename", ctx) : joinSettings;

    auto innerJoinTree = ctx.ChangeChild(*leftJoinTree, 0, ctx.NewAtom(leftJoinTree->Pos(), "Inner"));
    auto leftOnlyJoinTree = ctx.ChangeChild(*leftJoinTree, 0, ctx.NewAtom(leftJoinTree->Pos(), "LeftOnly"));

    // Collect join labels for left child of the `Left` join tree, they are used in `EquiJoin` for `Left Only` and `Inner`.
    THashSet<TString> leftLabelsNoRightChild;
    CollectJoinLabels(leftJoinTree->Child(1), leftLabelsNoRightChild);
    auto leftJoinLabelsNoRightChild = MapLabelNamesToJoinLabels(joinLabels, leftLabelsNoRightChild);

    // Collect join labels for the full `Left` join tree, the are used list of labels associated with result of `EquiJoin`.
    THashSet<TString> leftLabelsFull;
    CollectJoinLabels(leftJoinTree, leftLabelsFull);
    auto leftJoinLabelsFull = MapLabelNamesToJoinLabels(joinLabels, leftLabelsFull);

    YQL_ENSURE(leftJoinTree->Child(2)->IsAtom());
    auto rightSideInput = equiJoinLabels.at(leftJoinTree->Child(2)->Content());
    auto filteredInput = rightSideInput;

    if (NeedEmitSkipNullMembers(types)) {
        // skip null key columns
        TSet<TString> optionalKeyColumns;
        GatherOptionalKeyColumns(
            joinTree,
            optionalKeyColumns,
            {
                .Labels = labels,
                .InputIndex = inputIndex,
                .WithInnerOptionals = IsSkipNullsUnessential(types),
                .BothSides = IsPredicatePushdownOverEquiJoinBothSides(types),
            }
        );
        filteredInput = FilterOutNullJoinColumns(
            predicate->Pos(),
            filteredInput,
            labels.Inputs[inputIndex],
            optionalKeyColumns,
            ordered,
            types,
            ctx
        );
    }

    // then apply predicate
    filteredInput = ApplyJoinPredicate(
        predicate, /*filterInput=*/filteredInput, args, labels, {}, renameMap, onlyKeys,
        inputIndex, inputIndex, ordered, /*substituteWithNulls=*/false, /*forceOptional=*/true, ctx
    );

    // then create unionall of two joins.
    //firstly, join same labels with inner join:

    size_t i = 0;
    auto innerJoin = ctx.Builder(pos)
        .Callable("EquiJoin")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                for (const auto& [labelNames, input] : leftJoinLabelsNoRightChild) {
                    if (labelNames.size() == 1) {
                        parent.List(i++)
                            .Add(0, input)
                            .Atom(1, *labelNames.begin())
                        .Seal();
                    } else {
                        // Create a label list if them more than 1.
                        auto labelList = CreateLabelList(labelNames, ctx, pos);
                        parent.List(i++)
                            .Add(0, input)
                            .Add(1, labelList)
                        .Seal();
                    }
                }
                return parent;
            })
            .List(i++)
                .Add(0, filteredInput)
                .Atom(1, innerJoinTree->ChildRef(2)->Content())
            .Seal()
            .Add(i++, innerJoinTree)
            .Add(i++, innerSettings)
        .Seal()
    .Build();

    //then, do leftOnly join:

    i = 0;
    auto leftOnlyJoin = ctx.Builder(pos)
        .Callable("EquiJoin")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                for (const auto& [labelNames, input] : leftJoinLabelsNoRightChild) {
                    if (labelNames.size() == 1) {
                        parent.List(i++)
                            .Add(0, input)
                            .Atom(1, *labelNames.begin())
                        .Seal();
                    } else {
                        // Create a label list if them more than 1.
                        auto labelList = CreateLabelList(labelNames, ctx, pos);
                        parent.List(i++)
                            .Add(0, input)
                            .Add(1, labelList)
                        .Seal();
                    }
                }
                return parent;
            })
            .List(i++)
                .Add(0, rightSideInput)
                .Atom(1, leftOnlyJoinTree->ChildRef(2)->Content())
            .Seal()
            .Add(i++, leftOnlyJoinTree)
            .Add(i++, innerSettings)
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
        // TODO: Evaluate constraints in UnionAll automatically. See https://st.yandex-team.ru/YQL-20085#685bb01e8a10e760cdd58750.
        return KeepUniqueDistinct(unionAll, *equiJoin, ctx);
    }

    TExprNode::TPtr remJoinKeys;
    bool changedLeftSide = false;
    if (leftJoinTree == parentJoinPtr->ChildPtr(1)) {
        changedLeftSide = true;
        remJoinKeys = parentJoinPtr->ChildPtr(3);
    } else {
        remJoinKeys = parentJoinPtr->ChildPtr(4);
    }

    TExprNode::TPtr parentJoinLabel;
    if (remJoinKeys->ChildrenSize()) {
        parentJoinLabel = remJoinKeys->ChildPtr(0);
    } else {
        // Parent join does not have a join keys, probably it's a Cross join,
        // so we can take any label from Left join, because it associated with multi label input.
        if (leftJoinTree->ChildPtr(1)->IsAtom()) {
            parentJoinLabel = leftJoinTree->ChildPtr(1);
        } else {
            YQL_ENSURE(leftJoinTree->ChildPtr(2)->IsAtom());
            parentJoinLabel = leftJoinTree->ChildPtr(2);
        }
    }

    auto newParentJoin = ctx.Builder(joinTree->Pos())
        .List()
            .Add(0, parentJoinPtr->ChildPtr(0))
            .Add(1, changedLeftSide ? parentJoinLabel : parentJoinPtr->ChildPtr(1))
            .Add(2, !changedLeftSide ? parentJoinLabel : parentJoinPtr->ChildPtr(2))
            .Add(3, parentJoinPtr->ChildPtr(3))
            .Add(4, parentJoinPtr->ChildPtr(4))
            .Add(5, parentJoinPtr->ChildPtr(5))
        .Seal()
        .Build();

    auto newJoinTree = ctx.ReplaceNode(std::move(joinTree), *parentJoinPtr, newParentJoin);

    // Combine join labels from left tree and associate them with result of `EquiJoin` from above.
    auto combinedLabelList = CombineLabels(leftJoinLabelsFull);
    auto combinedJoinLabels = CreateLabelList(combinedLabelList, ctx, pos);

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
            .Add(1, combinedJoinLabels)
        .Seal()
        .Add(i++, newJoinTree)
        .Add(i++, joinSettings)
        .Seal()
    .Build();

    return KeepUniqueDistinct(newEquiJoin, *equiJoin, ctx);
}

class TJoinTreeRebuilder {
public:
    TJoinTreeRebuilder(const TJoinLabels& labels, TExprNode::TPtr joinTree, TStringBuf label1, TStringBuf column1, TStringBuf label2, TStringBuf column2,
        TExprContext& ctx, bool rotateJoinTree)
        : JoinLabels_(labels)
        , JoinTree_(joinTree)
        , Labels_{ label1, label2 }
        , Columns_{ column1, column2 }
        , Ctx_(ctx)
        , RotateJoinTree_(rotateJoinTree)
    {}

    TExprNode::TPtr Run() {
        auto joinTree = JoinTree_;
        if (RotateJoinTree_) {
            joinTree = RotateCrossJoin(JoinTree_->Pos(), JoinTree_);
        }
        auto newJoinTree = std::get<0>(AddLink(joinTree));
        YQL_ENSURE(Updated_);
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

            return Ctx_.ChangeChildren(*joinTree, std::move(children));
        }

        CrossJoins_.clear();
        RestJoins_.clear();
        GatherCross(joinTree);
        TStringBuf foundCross1, foundCross2;
        auto inCross1 = FindCrossJoinLabel(Labels_[0], foundCross1);
        auto inCross2 = FindCrossJoinLabel(Labels_[1], foundCross2);
        if (inCross1 || inCross2) {
            if (inCross1 && inCross2) {
                // make them a leaf
                joinTree = MakeCrossJoin(pos, Ctx_.NewAtom(pos, foundCross1), Ctx_.NewAtom(pos, foundCross2), Ctx_);
                for (auto label : CrossJoins_) {
                    if (label != foundCross1 && label != foundCross2) {
                        joinTree = MakeCrossJoin(pos, joinTree, Ctx_.NewAtom(pos, label), Ctx_);
                    }
                }

                joinTree = AddRestJoins(pos, joinTree, nullptr);
            }
            else if (inCross1) {
                // leaf with table1 and subtree with table2
                auto rest = FindRestJoin(Labels_[1]);
                if (!rest) {
                    return joinTree;
                }

                joinTree = MakeCrossJoin(pos, Ctx_.NewAtom(pos, foundCross1), rest, Ctx_);
                for (auto label : CrossJoins_) {
                    if (label != foundCross1) {
                        joinTree = MakeCrossJoin(pos, joinTree, Ctx_.NewAtom(pos, label), Ctx_);
                    }
                }

                joinTree = AddRestJoins(pos, joinTree, rest);
            }
            else {
                // leaf with table2 and subtree with table1
                auto rest = FindRestJoin(Labels_[0]);
                if (!rest) {
                    return joinTree;
                }

                joinTree = MakeCrossJoin(pos, Ctx_.NewAtom(pos, foundCross2), rest, Ctx_);
                for (auto label : CrossJoins_) {
                    if (label != foundCross2) {
                        joinTree = MakeCrossJoin(pos, joinTree, Ctx_.NewAtom(pos, label), Ctx_);
                    }
                }

                joinTree = AddRestJoins(pos, joinTree, rest);
            }
        }

        return joinTree;
    }

    bool FindCrossJoinLabel(TStringBuf label, TStringBuf& foundTable) const {
        auto input = JoinLabels_.FindInput(label);
        YQL_ENSURE(input);
        for (const auto& t : (*input)->Tables) {
            if (FindPtr(CrossJoins_, t)) {
                foundTable = t;
                return true;
            }
        }

        return false;
    }

    TExprNode::TPtr AddRestJoins(TPositionHandle pos, TExprNode::TPtr joinTree, TExprNode::TPtr exclude) {
        for (auto join : RestJoins_) {
            if (join == exclude) {
                continue;
            }

            joinTree = MakeCrossJoin(pos, joinTree, join, Ctx_);
        }

        return joinTree;
    }

    TExprNode::TPtr FindRestJoin(TStringBuf label) {
        for (auto join : RestJoins_) {
            if (HasTable(join, label)) {
                return join;
            }
        }

        return nullptr;
    }

    bool HasTable(TExprNode::TPtr joinTree, TStringBuf label) {
        auto left = joinTree->ChildPtr(1);
        if (left->IsAtom()) {
            auto input = JoinLabels_.FindInput(left->Content());
            YQL_ENSURE(input);
            for (const auto& t : (*input)->Tables) {
                if (t == label) {
                    return true;
                }
            }
        }
        else {
            if (HasTable(left, label)) {
                return true;
            }
        }

        auto right = joinTree->ChildPtr(2);
        if (right->IsAtom()) {
            auto input = JoinLabels_.FindInput(right->Content());
            YQL_ENSURE(input);
            for (const auto& t : (*input)->Tables) {
                if (t == label) {
                    return true;
                }
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
            RestJoins_.push_back(joinTree);
            return;
        }

        auto left = joinTree->ChildPtr(1);
        if (left->IsAtom()) {
            CrossJoins_.push_back(left->Content());
        }
        else {
            GatherCross(left);
        }

        auto right = joinTree->ChildPtr(2);
        if (right->IsAtom()) {
            CrossJoins_.push_back(right->Content());
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
        } else {
            auto input1 = JoinLabels_.FindInput(Labels_[0]);
            YQL_ENSURE(input1);
            for (const auto& t : (*input1)->Tables) {
                if (left->Content() == t) {
                    found1 = 1u;
                }
            }

            auto input2 = JoinLabels_.FindInput(Labels_[1]);
            YQL_ENSURE(input2);
            for (const auto& t : (*input2)->Tables) {
                if (left->Content() == t) {
                    found2 = 1u;
                }
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
            auto input1 = JoinLabels_.FindInput(Labels_[0]);
            YQL_ENSURE(input1);
            for (const auto& t : (*input1)->Tables) {
                if (right->Content() == t) {
                    found1 = 2u;
                }
            }

            auto input2 = JoinLabels_.FindInput(Labels_[1]);
            YQL_ENSURE(input2);
            for (const auto& t : (*input2)->Tables) {
                if (right->Content() == t) {
                    found2 = 2u;
                }
            }
        }

        if (found1 && found2) {
            if (!Updated_) {
                if (joinTree->Child(0)->Content() == "Cross") {
                    children[0] = Ctx_.NewAtom(joinTree->Pos(), "Inner");
                }
                else {
                    YQL_ENSURE(joinTree->Child(0)->Content() == "Inner");
                }

                ui32 index1 = *found1 - 1; // 0/1
                ui32 index2 = 1 - index1;

                auto link1 = children[3]->ChildrenList();
                link1.push_back(Ctx_.NewAtom(joinTree->Pos(), Labels_[index1]));
                link1.push_back(Ctx_.NewAtom(joinTree->Pos(), Columns_[index1]));
                children[3] = Ctx_.ChangeChildren(*children[3], std::move(link1));

                auto link2 = children[4]->ChildrenList();
                link2.push_back(Ctx_.NewAtom(joinTree->Pos(), Labels_[index2]));
                link2.push_back(Ctx_.NewAtom(joinTree->Pos(), Columns_[index2]));
                children[4] = Ctx_.ChangeChildren(*children[4], std::move(link2));

                Updated_ = true;
            }
        }

        return { Ctx_.ChangeChildren(*joinTree, std::move(children)), found1, found2 };
    }

private:
    TVector<TStringBuf> CrossJoins_;
    TVector<TExprNode::TPtr> RestJoins_;

    bool Updated_ = false;

    const TJoinLabels& JoinLabels_;
    TExprNode::TPtr JoinTree_;
    std::array<TStringBuf, 2> Labels_;
    std::array<TStringBuf, 2> Columns_;
    TExprContext& Ctx_;
    bool RotateJoinTree_;
};

TExprNode::TPtr DecayCrossJoinIntoInner(TExprNode::TPtr equiJoin, const TExprNode::TPtr& predicate,
    const TJoinLabels& labels, const TExprNode& row, const THashMap<TString, TString>& backRenameMap,
    TExprContext& ctx, bool rotateJoinTree)
{
    TExprNode::TPtr left, right;
    if (!IsMemberEquality(predicate, row, left, right)) {
        return equiJoin;
    }

    TVector<ui32> inputs;
    TVector<TStringBuf> columnLabels;
    TVector<TStringBuf> columns;
    for (auto member : { left, right }) {
        // rename used fields
        TStringBuf memberName(member->Child(1)->Content());
        if (auto renamed = backRenameMap.FindPtr(memberName)) {
            memberName = *renamed;
        }

        TStringBuf label;
        TStringBuf column;
        SplitTableName(memberName, label, column);
        TMaybe<ui32> maybeIndex = labels.FindInputIndex(label);
        YQL_ENSURE(maybeIndex, "Unable to find input for label " << ToString(label).Quote());
        inputs.push_back(*maybeIndex);
        columnLabels.push_back(label);
        columns.push_back(column);
    }

    YQL_ENSURE(inputs.size() == 2 && inputs.size() == columnLabels.size() && columnLabels.size() == columns.size());
    if (inputs.front() == inputs.back()) {
        return equiJoin;
    }

    auto inputsCount = equiJoin->ChildrenSize() - 2;
    auto joinTree = equiJoin->Child(inputsCount);
    if (!IsRequiredSide(joinTree, labels, inputs.front()).first ||
        !IsRequiredSide(joinTree, labels, inputs.back()).first)
    {
        return equiJoin;
    }

    TJoinTreeRebuilder rebuilder(labels, joinTree, columnLabels.front(), columns.front(), columnLabels.back(), columns.back(), ctx, rotateJoinTree);
    auto newJoinTree = rebuilder.Run();
    return ctx.ChangeChild(*equiJoin, inputsCount, std::move(newJoinTree));
}

bool IsEqualityFilterOverJoinEnabled(const TTypeAnnotationContext* types) {
    YQL_ENSURE(types);
    static const char Flag[] = "EqualityFilterOverJoin";
    return IsOptimizerEnabled<Flag>(*types) && !IsOptimizerDisabled<Flag>(*types);
}

bool IsExtractOrPredicatesOverEquiJoinEnabled(const TTypeAnnotationContext* types) {
    YQL_ENSURE(types);
    static const char Flag[] = "ExtractOrPredicatesOverEquiJoin";
    return IsOptimizerEnabled<Flag>(*types) && !IsOptimizerDisabled<Flag>(*types);
}

bool IsNormalizeEqualityFilterOverJoinEnabled(const TTypeAnnotationContext* types) {
    YQL_ENSURE(types);
    static const char Flag[] = "NormalizeEqualityFilterOverJoin";
    return IsOptimizerEnabled<Flag>(*types) && !IsOptimizerDisabled<Flag>(*types);
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


TExprBase NormalizeEqualityFilterOverJoin(const TCoFlatMapBase& node, const TJoinLabels& labels,
    const THashMap<TString, TString>& backRenameMap, const TParentsMap& parentsMap, TExprContext& ctx)
{
    TCoEquiJoin equiJoin = node.Input().Cast<TCoEquiJoin>();
    const TExprNode::TPtr joinTree = equiJoin.Arg(equiJoin.ArgCount() - 2).Ptr();
    const TExprNode::TPtr rowPtr = node.Lambda().Args().Arg(0).Ptr();
    const auto& row = *rowPtr;
    const auto body = node.Lambda().Body().Cast<TCoConditionalValueBase>();

    auto predicate = body.Predicate().Ptr();

    TExprNodeList andComponents;
    if (predicate->IsCallable("And")) {
        andComponents = predicate->ChildrenList();
    } else {
        andComponents.push_back(predicate);
    }

    TExprNodeList rest;
    TNodeOnNodeOwnedMap remaps;
    TVector<TExprNodeList> nodesByInput(equiJoin.ArgCount() - 2);

    for (auto pred : andComponents) {
        TExprNodeList sides(2);
        if (!IsEquality(pred, sides.front(), sides.back()) || HasDependsOn(pred, rowPtr) || !IsStrict(pred)) {
            rest.push_back(pred);
            continue;
        }

        // TODO: remove after YQL-20354 is fixed
        if (AnyOf(sides, [](const TExprNode::TPtr& side) { return side->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg; }) &&
            !IsSameAnnotation(*sides[0]->GetTypeAnn(), *sides[1]->GetTypeAnn()))
        {
            rest.push_back(pred);
            continue;
        }

        TVector<ui32> indexes;
        TSet<ui32> inputs;
        TSet<TStringBuf> usedFields;

        GatherJoinInputs(sides.front(), row, parentsMap, backRenameMap, labels, inputs, usedFields);
        if (inputs.size() != 1) {
            rest.push_back(pred);
            continue;
        }
        indexes.push_back(*inputs.begin());
        YQL_ENSURE(indexes.back() < nodesByInput.size());

        inputs.clear();
        GatherJoinInputs(sides.back(), row, parentsMap, backRenameMap, labels, inputs, usedFields);
        if (inputs.size() != 1 || *inputs.begin() == indexes.front()) {
            rest.push_back(pred);
            continue;
        }
        indexes.push_back(*inputs.begin());
        YQL_ENSURE(indexes.back() < nodesByInput.size());


        size_t count = 0;
        for (size_t i = 0; i < 2; ++i) {
            TExprNode::TPtr side = sides[i];
            if (!side->IsCallable("Member") || &side->Head() != &row) {
                ++count;
                if (!remaps.contains(side.Get())) {
                    TExprNode::TPtr output;
                    TOptimizeExprSettings settings(nullptr);
                    auto ret = OptimizeExpr(side, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                        if (node->IsCallable("Member") && &node->Head() == &row) {
                            const TString originalMemberName{node->Tail().Content()};
                            TString memberName = originalMemberName;
                            if (auto renamed = backRenameMap.FindPtr(memberName)) {
                                memberName = *renamed;
                            }
                            const auto& label = labels.Inputs[indexes[i]];
                            TString sourceMemberName = label.MemberName(label.TableName(memberName), label.ColumnName(memberName));
                            if (sourceMemberName != originalMemberName) {
                                return ctx.ChangeChild(*node, TCoMember::idx_Name, ctx.NewAtom(node->Pos(), sourceMemberName));
                            }
                        }
                        return node;
                    }, ctx, settings);
                    YQL_ENSURE(ret != IGraphTransformer::TStatus::Error);
                    remaps[side.Get()] = output;
                }
                nodesByInput[indexes[i]].push_back(side);
            }
        }

        if (!count) {
            rest.push_back(pred);
        }
    }

    if (remaps.empty()) {
        return node;
    }

    TExprNodeList newJoinArgs;
    YQL_ENSURE(labels.Inputs.size() == nodesByInput.size());
    TNodeOnNodeOwnedMap outputRemaps;
    TSet<TString> outputMembers;
    for (size_t i = 0; i < nodesByInput.size(); ++i) {
        TCoEquiJoinInput ejInput = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
        if (nodesByInput[i].empty()) {
            newJoinArgs.push_back(ejInput.Ptr());
            continue;
        }

        const TStructExprType* itemType = ejInput.List().Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        const TStructExprType* castedType = itemType;
        if (!IsRequiredSide(joinTree, labels, i).first) {
            TVector<const TItemExprType *> items;
            for (auto item : itemType->GetItems()) {
                auto type = item->GetItemType();
                if (type->IsOptionalOrNull()) {
                    items.push_back(item);
                } else {
                    items.push_back(ctx.MakeType<TItemExprType>(item->GetName(), ctx.MakeType<TOptionalExprType>(type)));
                }
            }
            castedType = ctx.MakeType<TStructExprType>(items);
        }
        TString prefix;
        bool isMultiTableInput = false;
        if (labels.Inputs[i].Tables.size() > 1) {
            prefix = TStringBuilder() << labels.Inputs[i].Tables[0] << ".";
            isMultiTableInput = true;
        }
        prefix += YqlJoinKeyColumnName;
        TVector<TString> remappedNames = GenNoClashColumns(*itemType, prefix, nodesByInput[i].size());
        newJoinArgs.push_back(ctx.Builder(equiJoin.Arg(i).Pos())
            .List()
                .Callable(0, "OrderedMap")
                    .Add(0, ejInput.List().Ptr())
                    .Lambda(1)
                        .Param("row")
                        .Callable("FlattenMembers")
                            .List(0)
                                .Atom(0, "")
                                .Arg(1, "row")
                            .Seal()
                            .List(1)
                                .Atom(0, "")
                                .Callable(1, "AsStruct")
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        for (size_t j = 0; j < nodesByInput[i].size(); ++j) {
                                            auto calcLambda = ctx.ChangeChild(node.Lambda().Ref(), TCoLambda::idx_Body, TExprNode::TPtr(remaps[nodesByInput[i][j].Get()]));
                                            parent
                                                .List(j)
                                                    .Atom(0, remappedNames[j])
                                                    .Apply(1, calcLambda)
                                                        .With(0)
                                                            .Callable("SafeCast")
                                                                .Arg(0, "row")
                                                                .Add(1, ExpandType(equiJoin.Arg(i).Pos(), *castedType, ctx))
                                                            .Seal()
                                                        .Done()
                                                    .Seal()
                                                .Seal();
                                            TString outputMember = isMultiTableInput ? remappedNames[j] : labels.Inputs[i].FullName(remappedNames[j]);
                                            outputRemaps[nodesByInput[i][j].Get()] = ctx.Builder(nodesByInput[i][j]->Pos())
                                                .Callable("Member")
                                                    .Add(0, rowPtr)
                                                    .Atom(1, outputMember)
                                                .Seal()
                                                .Build();
                                            YQL_ENSURE(outputMembers.insert(outputMember).second);
                                        }
                                        return parent;
                                    })
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Add(1, ejInput.Scope().Ptr())
            .Seal()
            .Build());
    }
    newJoinArgs.push_back(equiJoin.Arg(equiJoin.ArgCount() - 2).Ptr());
    newJoinArgs.push_back(equiJoin.Arg(equiJoin.ArgCount() - 1).Ptr());

    auto newJoin = ctx.ChangeChildren(equiJoin.Ref(), std::move(newJoinArgs));
    TExprNode::TPtr newPredicate;
    auto status = RemapExpr(body.Predicate().Ptr(), newPredicate, outputRemaps, ctx, TOptimizeExprSettings{nullptr});
    YQL_ENSURE(status != IGraphTransformer::TStatus::Error);

    TExprNode::TPtr newPredicateLambda = ctx.ChangeChild(node.Lambda().Ref(), TCoLambda::idx_Body, std::move(newPredicate));
    TExprNode::TPtr valueLambda = ctx.ChangeChild(node.Lambda().Ref(), TCoLambda::idx_Body, body.Value().Ptr());

    return TExprBase(ctx.Builder(node.Pos())
        .Callable(node.CallableName())
            .Add(0, newJoin)
            .Lambda(1)
                .Param("row")
                .Callable(body.CallableName())
                    .Callable(0, "Coalesce")
                        .Apply(0, newPredicateLambda)
                            .With(0, "row")
                        .Seal()
                        .Callable(1, "Bool")
                            .Atom(0, "false")
                        .Seal()
                    .Seal()
                    .Apply(1, valueLambda)
                        .With(0)
                            .Callable("RemoveMembers")
                                .Arg(0, "row")
                                .List(1)
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        size_t i = 0;
                                        for (auto name : outputMembers) {
                                            parent.Atom(i++, name);
                                        }
                                        return parent;
                                    })
                                .Seal()
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build());
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
        // TODO: handle case IsEquality() && !IsMemberEquality()
        if (!IsMemberEquality(pred, row, left, right)) {
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

    if (!node.Raw()->HasSideEffects() && IsPredicateFlatMap(node.Lambda().Body().Ref())) {
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

        if (IsNormalizeEqualityFilterOverJoinEnabled(types)) {
            auto newNode = NormalizeEqualityFilterOverJoin(node, labels, backRenameMap, parentsMap, ctx);
            if (newNode.Raw() != node.Raw()) {
                YQL_CLOG(DEBUG, Core) << "NormalizeEqualityFilterOverJoin";
                return newNode;
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

        for (auto andTerm : andTerms) {
            if (IsNoPush(*andTerm)) {
                continue;
            }

            TSet<ui32> inputs;
            GatherJoinInputs(andTerm, row, parentsMap, backRenameMap, labels, inputs, usedFields);

            if (!multiUsage && inputs.size() == 0) {
                YQL_CLOG(DEBUG, Core) << "ConstantPredicatePushdownOverEquiJoin";
                ret = ConstantPredicatePushdownOverEquiJoin(equiJoin.Ptr(), andTerm, ordered, ctx);
                extraPredicate = FuseAndTerms(node.Pos(), andTerms, andTerm, {}, isPg, ctx);
                break;
            }

            if (!multiUsage && inputs.size() == 1) {
                auto newJoin = SingleInputPredicatePushdownOverEquiJoin(
                    equiJoin.Ptr(),
                    andTerm,
                    usedFields,
                    node.Lambda().Args().Ptr(),
                    labels,
                    *inputs.begin(),
                    renameMap,
                    ordered,
                    ctx,
                    types
                );
                if (newJoin != equiJoin.Ptr()) {
                    YQL_CLOG(DEBUG, Core) << "SingleInputPredicatePushdownOverEquiJoin";
                    ret = newJoin;
                    extraPredicate = FuseAndTerms(node.Pos(), andTerms, andTerm, {}, isPg, ctx);
                    break;
                } else if (types->FilterPushdownOverJoinOptionalSide) {
                    auto twoJoins = FilterPushdownOverJoinOptionalSide(
                        equiJoin.Ptr(),
                        andTerm,
                        usedFields,
                        node.Lambda().Args().Ptr(),
                        labels,
                        *inputs.begin(),
                        renameMap,
                        ordered,
                        ctx,
                        types,
                        node.Pos()
                    );
                    if (twoJoins != equiJoin.Ptr()) {
                        YQL_CLOG(DEBUG, Core) << "RightSidePredicatePushdownOverLeftJoin";
                        ret = twoJoins;
                        extraPredicate = FuseAndTerms(node.Pos(), andTerms, andTerm, {}, isPg, ctx);
                        break;
                    }

                }
            }

            if (!IsEqualityFilterOverJoinEnabled(types) && inputs.size() == 2) {
                auto newJoin = DecayCrossJoinIntoInner(equiJoin.Ptr(), andTerm,
                    labels, row, backRenameMap, ctx, types->RotateJoinTree);
                if (newJoin != equiJoin.Ptr()) {
                    YQL_CLOG(DEBUG, Core) << "DecayCrossJoinIntoInner";
                    ret = newJoin;
                    extraPredicate = FuseAndTerms(node.Pos(), andTerms, andTerm, {}, isPg, ctx);
                    break;
                }
            }

            if (IsExtractOrPredicatesOverEquiJoinEnabled(types)) {
                // This optimizer tries to extract predicates from OR terms that can be pushed down to EquiJoin inputs as pre-conditions
                // For example, in SELECT ... FROM a JOIN b ON ... WHERE (f(a) AND g(b)) OR (x(a) AND y(b)) statement
                // f(a) OR x(a), g(b) OR y(b) are extracted for respective inputs

                auto extractedPredicates = ExtractOrPredicatesOverEquiJoin(andTerm, row, backRenameMap, labels, ctx, *types);
                if (!extractedPredicates.empty()) {
                    YQL_CLOG(DEBUG, Core) << "ExtractOrPredicatesOverEquiJoin";
                    ret = equiJoin.Ptr();
                    auto newAndTerm = ctx.NewCallable(andTerm->Pos(), "NoPush", {andTerm});
                    andTerms.insert(andTerms.end(), extractedPredicates.begin(), extractedPredicates.end());
                    extraPredicate = FuseAndTerms(node.Pos(), andTerms, andTerm, std::move(newAndTerm), isPg, ctx);
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
