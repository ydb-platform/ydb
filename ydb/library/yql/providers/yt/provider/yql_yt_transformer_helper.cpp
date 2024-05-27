
#include "yql_yt_transformer_helper.h"

#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/xrange.h>
#include <util/string/type.h>

namespace NYql::NPrivate {

TYtSectionList ConvertInputTable(TExprBase input, TExprContext& ctx, const TConvertInputOpts& opts)
{
    TVector<TYtSection> sections;
    TExprBase columns = opts.CustomFields_ ? TExprBase(opts.CustomFields_.Cast()) : TExprBase(Build<TCoVoid>(ctx, input.Pos()).Done());
    if (auto out = input.Maybe<TYtOutput>()) {
        auto settings = opts.Settings_;
        if (!settings) {
            settings = Build<TCoNameValueTupleList>(ctx, input.Pos()).Done();
        }
        TMaybeNode<TCoAtom> mode = out.Mode();
        if (opts.MakeUnordered_) {
            mode = Build<TCoAtom>(ctx, out.Cast().Pos()).Value(ToString(EYtSettingType::Unordered)).Done();
        } else if (opts.ClearUnordered_) {
            mode = {};
        }
        sections.push_back(Build<TYtSection>(ctx, input.Pos())
            .Paths()
                .Add()
                    .Table<TYtOutput>()
                        .InitFrom(out.Cast())
                        .Mode(mode)
                    .Build()
                    .Columns(columns)
                    .Ranges<TCoVoid>().Build()
                    .Stat<TCoVoid>().Build()
                .Build()
            .Build()
            .Settings(settings.Cast())
            .Done());
    }
    else {
        auto read = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>();
        YQL_ENSURE(read, "Unknown operation input");

        for (auto section: read.Cast().Input()) {
            bool makeUnordered = opts.MakeUnordered_;

            auto mergedSettings = section.Settings().Ptr();
            if (NYql::HasSetting(*mergedSettings, EYtSettingType::Unordered)) {
                mergedSettings = NYql::RemoveSetting(*mergedSettings, EYtSettingType::Unordered, ctx);
                makeUnordered = false;
            }
            if (!opts.KeepDirecRead_) {
                mergedSettings = NYql::RemoveSetting(*mergedSettings, EYtSettingType::DirectRead, ctx);
            }
            if (opts.Settings_) {
                mergedSettings = MergeSettings(*mergedSettings, opts.Settings_.Cast().Ref(), ctx);
            }

            section = Build<TYtSection>(ctx, section.Pos())
                .InitFrom(section)
                .Settings(mergedSettings)
                .Done();

            if (makeUnordered) {
                section = MakeUnorderedSection(section, ctx);
            } else if (opts.ClearUnordered_) {
                section = ClearUnorderedSection(section, ctx);
            }

            if (opts.CustomFields_) {
                section = UpdateInputFields(section, opts.CustomFields_.Cast(), ctx);
            }

            sections.push_back(section);
        }
    }

    return Build<TYtSectionList>(ctx, input.Pos())
        .Add(sections)
        .Done();
}

bool CollectMemberPaths(TExprBase row, const TExprNode::TPtr& lookupItem,
    TMap<TStringBuf, TVector<ui32>>& memberPaths, TVector<ui32>& currPath)
{
    if (lookupItem->IsCallable("Member") && lookupItem->Child(0) == row.Raw()) {
        TStringBuf memberName = lookupItem->Child(1)->Content();
        memberPaths.insert({ memberName, currPath });
        return true;
    }

    if (lookupItem->IsList()) {
        for (ui32 i = 0; i < lookupItem->ChildrenSize(); ++i) {
            currPath.push_back(i);
            auto res = CollectMemberPaths(row, lookupItem->ChildPtr(i), memberPaths, currPath);
            currPath.pop_back();
            if (!res) {
                return false;
            }
        }
        return true;
    }

    return !IsDepended(*lookupItem, row.Ref());
}

bool CollectMemberPaths(TExprBase row, const TExprNode::TPtr& lookupItem,
    TMap<TStringBuf, TVector<ui32>>& memberPaths)
{
    TVector<ui32> currPath;
    return CollectMemberPaths(row, lookupItem, memberPaths, currPath);
}

bool CollectKeyPredicatesFromLookup(TExprBase row, TCoLookupBase lookup, TVector<TKeyFilterPredicates>& ranges,
    size_t maxTables)
{
    TExprNode::TPtr collection;
    if (lookup.Collection().Ref().IsList()) {
        collection = lookup.Collection().Ptr();
    } else if (auto maybeAsList = lookup.Collection().Maybe<TCoAsList>()) {
        collection = maybeAsList.Cast().Ptr();
    } else if (auto maybeDictFromKeys = lookup.Collection().Maybe<TCoDictFromKeys>()) {
        collection = maybeDictFromKeys.Cast().Keys().Ptr();
    } else {
        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": unsupported collection " << lookup.Collection().Ref().Content();
        return false;
    }

    auto size = collection->ChildrenSize();
    if (!size) {
        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": empty keys collection";
        return false;
    }

    if (size + ranges.size() > maxTables) {
        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": too many dict keys - " << size;
        return false;
    }

    if (IsDepended(*collection, row.Ref())) {
        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": depends on lambda arg";
        return false;
    }

    TExprNode::TPtr lookupItem = lookup.Lookup().Ptr();
    TMap<TStringBuf, TVector<ui32>> memberPaths;
    if (!CollectMemberPaths(row, lookupItem, memberPaths)) {
        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": unsupported lookup item";
        return false;
    }

    if (memberPaths.empty()) {
        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": no key predicates in lookup item";
        return false;
    }

    for (auto& collectionItem : collection->Children()) {
        ranges.emplace_back();
        for (auto& memberAndPath : memberPaths) {
            auto member = memberAndPath.first;
            auto& path = memberAndPath.second;
            auto value = collectionItem;
            for (auto idx : path) {
                if (!value->IsList() || idx >= value->ChildrenSize()) {
                    YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": unexpected literal structure";
                    return false;
                }
                value = value->ChildPtr(idx);
            }
            ranges.back().emplace(member, std::make_pair(TString{TCoCmpEqual::CallableName()}, value));
        }
    }

    return true;
}

bool CollectKeyPredicatesAnd(TExprBase row, const std::vector<TExprBase>& predicates, TVector<TKeyFilterPredicates>& ranges, size_t maxTables)
{
    TVector<TKeyFilterPredicates> leftRanges;
    for (const auto& predicate : predicates) {
        TVector<TKeyFilterPredicates> rightRanges;
        if (CollectKeyPredicates(row, predicate, rightRanges, maxTables)) {
            if (leftRanges.empty()) {
                leftRanges = std::move(rightRanges);
            } else {
                const auto total = leftRanges.size() * rightRanges.size();
                if (total + ranges.size() > maxTables) {
                    YQL_CLOG(DEBUG, ProviderYt) << __func__  << ": too many tables - " << (total + ranges.size());
                    return false;
                }

                if (1U == total) {
                    leftRanges.front().insert(rightRanges.front().cbegin(), rightRanges.front().cend());
                } else {
                    TVector<TKeyFilterPredicates> temp;
                    temp.reserve(total);
                    for (const auto& left : leftRanges) {
                        for (const auto& right : rightRanges) {
                            temp.emplace_back(left);
                            temp.back().insert(right.cbegin(), right.cend());
                        }
                    }
                    leftRanges = std::move(temp);
                }
            }
        }
    }

    if (leftRanges.empty()) {
        return false;
    }

    std::move(leftRanges.begin(), leftRanges.end(), std::back_inserter(ranges));
    return true;
}

bool CollectKeyPredicatesOr(TExprBase row, const std::vector<TExprBase>& predicates, TVector<TKeyFilterPredicates>& ranges, size_t maxTables)
{
    for (const auto& predicate : predicates) {
        if (!CollectKeyPredicates(row, predicate, ranges, maxTables)) {
            return false;
        }
    }
    return true;
}

bool CollectKeyPredicates(TExprBase row, TExprBase predicate, TVector<TKeyFilterPredicates>& ranges, size_t maxTables)
{
    if (const auto maybeAnd = predicate.Maybe<TCoAnd>()) {
        const auto size = maybeAnd.Cast().Args().size();
        std::vector<TExprBase> predicates;
        predicates.reserve(size);
        for (auto i = 0U; i < size; ++i) {
            predicates.emplace_back(maybeAnd.Cast().Arg(i));
        }
        return CollectKeyPredicatesAnd(row, predicates, ranges, maxTables);
    }

    if (const auto maybeOr = predicate.Maybe<TCoOr>()) {
        const auto size = maybeOr.Cast().Args().size();
        std::vector<TExprBase> predicates;
        predicates.reserve(size);
        for (auto i = 0U; i < size; ++i) {
            predicates.emplace_back(maybeOr.Cast().Arg(i));
        }
        return CollectKeyPredicatesOr(row, predicates, ranges, maxTables);
    }

    TMaybeNode<TCoCompare> maybeCompare = predicate.Maybe<TCoCompare>();
    if (auto maybeLiteral = predicate.Maybe<TCoCoalesce>().Value().Maybe<TCoBool>().Literal()) {
        if (maybeLiteral.Cast().Value() == "false") {
            maybeCompare = predicate.Cast<TCoCoalesce>().Predicate().Maybe<TCoCompare>();
        }
    }

    auto getRowMember = [row] (TExprBase expr) {
        if (auto maybeMember = expr.Maybe<TCoMember>()) {
            if (maybeMember.Cast().Struct().Raw() == row.Raw()) {
                return maybeMember;
            }
        }

        return TMaybeNode<TCoMember>();
    };

    if (maybeCompare) {
        auto left = maybeCompare.Cast().Left();
        auto right = maybeCompare.Cast().Right();

        TMaybeNode<TCoMember> maybeMember = getRowMember(left);
        TMaybeNode<TExprBase> maybeValue = right;
        bool invert = false;
        if (!maybeMember) {
            maybeMember = getRowMember(right);
            maybeValue = left;
            invert = true;
        }

        if (!maybeMember) {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": comparison with non-member";
            return false;
        }

        const auto value = maybeValue.Cast();
        if (IsDepended(value.Ref(), row.Ref())) {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": depends on lambda arg";
            return false;
        }

        auto column = maybeMember.Cast().Name().Value();
        TString cmpOp = TString{maybeCompare.Cast().Ref().Content()};
        if (!IsRangeComparison(cmpOp)) {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": non-range comparison " << cmpOp;
            return false;
        }

        if (invert) {
            if (TCoCmpStartsWith::CallableName() == cmpOp)
                return false;

            switch (cmpOp.front()) {
                case '<': cmpOp.replace(0, 1, 1, '>'); break;
                case '>': cmpOp.replace(0, 1, 1, '<'); break;
                default: break;
            }
        }

        ranges.emplace_back();
        ranges.back().emplace(column, std::make_pair(cmpOp, value.Ptr()));
        return true;
    }

    if (auto maybeLookup = predicate.Maybe<TCoLookupBase>()) {
        return CollectKeyPredicatesFromLookup(row, maybeLookup.Cast(), ranges, maxTables);
    }

    if (auto maybeLiteral = predicate.Maybe<TCoCoalesce>().Value().Maybe<TCoBool>().Literal()) {
        if (maybeLiteral.Cast().Value() == "false") {
            if (auto maybeLookup = predicate.Maybe<TCoCoalesce>().Predicate().Maybe<TCoLookupBase>()) {
                return CollectKeyPredicatesFromLookup(row, maybeLookup.Cast(), ranges, maxTables);
            }
        }
    }

    if (auto maybeNotExists = predicate.Maybe<TCoNot>().Value().Maybe<TCoExists>().Optional()) {
        TMaybeNode<TCoMember> maybeMember = getRowMember(maybeNotExists.Cast());
        if (!maybeMember) {
            YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": Not Exists for non-member";
            return false;
        }
        auto column = maybeMember.Cast().Name().Value();
        ranges.emplace_back();
        ranges.back().emplace(column, std::make_pair(TString{TCoCmpEqual::CallableName()}, TExprNode::TPtr()));
        return true;
    }

    YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": unsupported predicate " << predicate.Ref().Content();
    return false;
}

TMaybeNode<TCoLambda> GetLambdaWithPredicate(TCoLambda lambda) {
    if (auto innerFlatMap = lambda.Body().Maybe<TCoFlatMapBase>()) {
        if (auto arg = innerFlatMap.Input().Maybe<TCoFilterNullMembersBase>().Input().Maybe<TCoJust>().Input()) {
            if (arg.Cast().Raw() == lambda.Args().Arg(0).Raw()) {
                lambda = innerFlatMap.Lambda().Cast();
            }
        }
    }
    if (!lambda.Body().Maybe<TCoConditionalValueBase>()) {
        return {};
    }
    return lambda;
}

bool IsAscending(const TExprNode& node) {
    TMaybe<bool> ascending;
    if (node.IsCallable("Bool")) {
        ascending = IsTrue(node.Child(0)->Content());
    }
    return ascending.Defined() && *ascending;
}

bool CollectSortSet(const TExprNode& sortNode, TSet<TVector<TStringBuf>>& sortSets) {
    if (sortNode.IsCallable("Sort")) {
        auto directions = sortNode.ChildPtr(1);

        auto lambdaArg = sortNode.Child(2)->Child(0)->Child(0);
        auto lambdaBody = sortNode.Child(2)->ChildPtr(1);

        TExprNode::TListType directionItems;
        if (directions->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
            directionItems = directions->ChildrenList();
        } else {
            directionItems.push_back(directions);
        }

        if (AnyOf(directionItems, [](const TExprNode::TPtr& direction) { return !IsAscending(*direction); })) {
            return false;
        }

        TExprNode::TListType lambdaBodyItems;
        if (directions->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
            lambdaBodyItems = lambdaBody->ChildrenList();
        } else {
            lambdaBodyItems.push_back(lambdaBody);
        }

        TVector<TStringBuf> sortBy;
        for (auto& item : lambdaBodyItems) {
            if (!item->IsCallable("Member") || item->Child(0) != lambdaArg) {
                return false;
            }
            YQL_ENSURE(item->Child(1)->IsAtom());
            sortBy.push_back(item->Child(1)->Content());
        }

        return sortSets.insert(sortBy).second;
    } else if (sortNode.IsCallable("Aggregate")) {
        if (!HasSetting(TCoAggregate(&sortNode).Settings().Ref(), "compact")) {
            return false;
        }
        auto keys = sortNode.Child(1);
        const auto keyNum = keys->ChildrenSize();
        if (keyNum == 0) {
            return false;
        }

        TVector<TStringBuf> keyList;
        keyList.reserve(keys->ChildrenSize());

        for (const auto& key : keys->ChildrenList()) {
            keyList.push_back(key->Content());
        }

        do {
            TVector<TStringBuf> sortBy;
            sortBy.reserve(keyNum);
            copy(keyList.begin(), keyList.end(), std::back_inserter(sortBy));
            sortSets.insert(sortBy);
            if (sortSets.size() > 20) {
                YQL_CLOG(WARN, ProviderYt) << __FUNCTION__ << ": join's preferred_sort can't have more than 20 key combinations";
                return true;
            }
        } while(next_permutation(keyList.begin(), keyList.end()));
        sortSets.insert(keyList);

        return true;
    } else {
        return false;
    }
}

TExprNode::TPtr CollectPreferredSortsForEquiJoinOutput(TExprBase join, const TExprNode::TPtr& options,
                                                              TExprContext& ctx, const TParentsMap& parentsMap)
{
    auto parentsIt = parentsMap.find(join.Raw());
    if (parentsIt == parentsMap.end()) {
        return options;
    }

    TSet<TVector<TStringBuf>> sortSets = LoadJoinSortSets(*options);
    size_t collected = 0;
    for (auto& parent : parentsIt->second) {
        if (CollectSortSet(*parent, sortSets)) {
            ++collected;
        }
    }

    if (!collected) {
        return options;
    }

    YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": Collected " << collected << " new sorts";

    auto removedOptions = RemoveSetting(*options, "preferred_sort", ctx);
    TExprNode::TListType optionsNodes = removedOptions->ChildrenList();
    AppendEquiJoinSortSets(options->Pos(), sortSets, optionsNodes, ctx);

    return ctx.NewList(options->Pos(), std::move(optionsNodes));
}

bool CanExtraColumnBePulledIntoEquiJoin(const TTypeAnnotationNode* type) {
    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
    }

    switch (type->GetKind()) {
        case ETypeAnnotationKind::Data:
            return IsFixedSizeData(type);
        case ETypeAnnotationKind::Null:
        case ETypeAnnotationKind::Void:
            return true;
        default:
            return false;
    }
}

bool IsYtOrPlainTablePropsDependent(NNodes::TExprBase input) {
    bool found = false;
    VisitExpr(input.Ref(), [&found](const TExprNode& n) {
        found = found || TYtTablePropBase::Match(&n) || TCoTablePropBase::Match(&n);
        return !found;
    });
    return found;
}

bool IsLambdaSuitableForPullingIntoEquiJoin(const TCoFlatMapBase& flatMap, const TExprNode& label,
                                                   const THashMap<TStringBuf, THashSet<TStringBuf>>& tableKeysMap,
                                                   const TExprNode* extractedMembers)
{
    if (!label.IsAtom()) {
        return false;
    }

    auto inputSeqItem = SilentGetSequenceItemType(flatMap.Input().Ref(), false);
    if (!inputSeqItem || !inputSeqItem->IsPersistable()) {
        return false;
    }

    auto outputSeqItem = SilentGetSequenceItemType(flatMap.Ref(), false);
    if (!outputSeqItem || !outputSeqItem->IsPersistable()) {
        return false;
    }

    if (IsYtOrPlainTablePropsDependent(flatMap.Lambda().Body())) {
        return false;
    }

    // allow only projective FlatMaps
    if (!IsJustOrSingleAsList(flatMap.Lambda().Body().Ref())) {
        return false;
    }

    // all input column should be either renamed, removed or passed as is
    // all join keys should be passed as-is
    // only fixed-size data type can be added
    auto arg = flatMap.Lambda().Args().Arg(0).Raw();
    auto outItem = flatMap.Lambda().Body().Ref().Child(0);

    if (outItem->IsCallable("AsStruct")) {
        size_t joinKeysPassed = 0;
        auto it = tableKeysMap.find(label.Content());
        const size_t joinKeysCount = (it == tableKeysMap.end()) ? 0 : it->second.size();

        TMaybe<THashSet<TStringBuf>> filteredMemberSet;
        if (extractedMembers) {
            filteredMemberSet.ConstructInPlace();
            for (auto member : extractedMembers->ChildrenList()) {
                YQL_ENSURE(member->IsAtom());
                filteredMemberSet->insert(member->Content());
            }
        }
        for (auto& item : outItem->Children()) {
            TStringBuf outMemberName = item->Child(0)->Content();
            if (filteredMemberSet && !filteredMemberSet->contains(outMemberName)) {
                // member will be filtered out by parent ExtractMembers
                continue;
            }
            if (item->Child(1)->IsCallable("Member") && item->Child(1)->Child(0) == arg) {
                TStringBuf inMemberName = item->Child(1)->Child(1)->Content();
                bool isJoinKey = joinKeysCount && it->second.contains(outMemberName);
                if (isJoinKey && inMemberName != outMemberName) {
                    return false;
                }
                joinKeysPassed += isJoinKey;
            } else if (!CanExtraColumnBePulledIntoEquiJoin(item->Child(1)->GetTypeAnn())) {
                return false;
            }
        }

        YQL_ENSURE(joinKeysPassed <= joinKeysCount);
        return joinKeysPassed == joinKeysCount;
    } else {
        return outItem == arg;
    }
}

TExprNode::TPtr BuildYtEquiJoinPremap(TExprBase list, TMaybeNode<TCoLambda> premapLambda, TExprContext& ctx) {
    if (auto type = GetSequenceItemType(list, false, ctx)) {
        if (!EnsurePersistableType(list.Pos(), *type, ctx)) {
            return {};
        }
        if (premapLambda) {
            return premapLambda.Cast().Ptr();
        }
        return Build<TCoVoid>(ctx, list.Pos()).Done().Ptr();
    }
    return {};
}

// label -> pair(<asc sort keys>, <inputs matched by keys>)
THashMap<TStringBuf, std::pair<TVector<TStringBuf>, ui32>> CollectTableSortKeysUsage(const TYtState::TPtr& state, const TCoEquiJoin& equiJoin) {
    THashMap<TStringBuf, std::pair<TVector<TStringBuf>, ui32>> tableSortKeys;
    for (size_t i = 0; i + 2 < equiJoin.ArgCount(); ++i) {
        auto joinInput = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
        auto list = joinInput.List();
        if (joinInput.Scope().Ref().IsAtom()) {
            TVector<TStringBuf> sortKeys;
            if (const auto sorted = list.Ref().GetConstraint<TSortedConstraintNode>()) {
                for (const auto& key : sorted->GetContent()) {
                    bool appropriate = false;
                    if (key.second && !key.first.empty()) {
                        for (auto& alt: key.first) {
                            if (alt.size() == 1U) {
                                sortKeys.emplace_back(alt.front());
                                appropriate = true;
                                break;
                            }
                        }
                    }
                    if (!appropriate) {
                        break;
                    }
                }
            }
            tableSortKeys[joinInput.Scope().Ref().Content()] = std::make_pair(std::move(sortKeys), 0);
        }
    }

    // Only Lookup, Merge, and Star strategies use a table sort
    if (!state->Configuration->JoinMergeTablesLimit.Get().GetOrElse(0)
        && !(state->Configuration->LookupJoinLimit.Get().GetOrElse(0) && state->Configuration->LookupJoinMaxRows.Get().GetOrElse(0))
        && !(state->Configuration->JoinEnableStarJoin.Get().GetOrElse(false) && state->Configuration->JoinAllowColumnRenames.Get().GetOrElse(true))
    ) {
        return tableSortKeys;
    }

    TVector<const TExprNode*> joinTreeNodes;
    joinTreeNodes.push_back(equiJoin.Arg(equiJoin.ArgCount() - 2).Raw());
    while (!joinTreeNodes.empty()) {
        const TExprNode* joinTree = joinTreeNodes.back();
        joinTreeNodes.pop_back();

        if (!joinTree->Child(1)->IsAtom()) {
            joinTreeNodes.push_back(joinTree->Child(1));
        }

        if (!joinTree->Child(2)->IsAtom()) {
            joinTreeNodes.push_back(joinTree->Child(2));
        }

        if (joinTree->Child(0)->Content() != "Cross") {
            THashMap<TStringBuf, THashSet<TStringBuf>> tableJoinKeys;
            for (auto keys: {joinTree->Child(3), joinTree->Child(4)}) {
                for (ui32 i = 0; i < keys->ChildrenSize(); i += 2) {
                    auto tableName = keys->Child(i)->Content();
                    auto column = keys->Child(i + 1)->Content();
                    tableJoinKeys[tableName].insert(column);
                }
            }

            for (auto& [label, joinKeys]: tableJoinKeys) {
                if (auto sortKeys = tableSortKeys.FindPtr(label)) {
                    if (joinKeys.size() <= sortKeys->first.size()) {
                        bool matched = true;
                        for (size_t i = 0; i < joinKeys.size(); ++i) {
                            if (!joinKeys.contains(sortKeys->first[i])) {
                                matched = false;
                                break;
                            }
                        }
                        if (matched) {
                            ++sortKeys->second;
                        }
                    }
                }
            }
        }
    }

    return tableSortKeys;
}

TCoLambda FallbackLambdaInput(TCoLambda lambda, TExprContext& ctx) {
    if (const auto narrow = FindNode(lambda.Ref().TailPtr(), [&](const TExprNode::TPtr& node) { return node->IsCallable("NarrowMap") && &lambda.Args().Arg(0).Ref() == &node->Head(); })) {
        return TCoLambda(ctx.DeepCopyLambda(lambda.Ref(), ctx.ReplaceNodes(lambda.Ref().TailPtr(), {{narrow.Get(), narrow->HeadPtr()}})));
    }

    return lambda;
}

TCoLambda FallbackLambdaOutput(TCoLambda lambda, TExprContext& ctx) {
    if (auto body = lambda.Ref().TailPtr(); body->IsCallable("ExpandMap")) {
        return TCoLambda(ctx.DeepCopyLambda(lambda.Ref(), body->HeadPtr()));
    }

    return lambda;
}

TYtDSink GetDataSink(TExprBase input, TExprContext& ctx) {
    if (auto read = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
        return TYtDSink(ctx.RenameNode(read.Cast().DataSource().Ref(), "DataSink"));
    } else if (auto out = input.Maybe<TYtOutput>()) {
        return GetOutputOp(out.Cast()).DataSink();
    } else {
        YQL_ENSURE(false, "Unknown operation input");
    }
}

TExprBase GetWorld(TExprBase input, TMaybeNode<TExprBase> main, TExprContext& ctx) {
    if (!main) {
        main = ctx.NewWorld(input.Pos());
    }
    TSyncMap syncList;
    if (auto maybeRead = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
        auto read = maybeRead.Cast();
        if (read.World().Ref().Type() != TExprNode::World) {
            syncList.emplace(read.World().Ptr(), syncList.size());
        }
    } else {
        YQL_ENSURE(input.Maybe<TYtOutput>(), "Unknown operation input: " << input.Ref().Content());
    }
    return TExprBase(ApplySyncListToWorld(main.Cast().Ptr(), syncList, ctx));
}

TConvertInputOpts::TConvertInputOpts()
            : KeepDirecRead_(false)
            , MakeUnordered_(false)
            , ClearUnordered_(false)
{
}

TConvertInputOpts& 
TConvertInputOpts::Settings(const TMaybeNode<TCoNameValueTupleList>& settings) {
    Settings_ = settings;
    return *this;
}

TConvertInputOpts&
TConvertInputOpts::CustomFields(const TMaybeNode<TCoAtomList>& customFields) {
        CustomFields_ = customFields;
    return *this;
}

TConvertInputOpts&
TConvertInputOpts::ExplicitFields(const TYqlRowSpecInfo& rowSpec, TPositionHandle pos, TExprContext& ctx) {
    TVector<TString> columns;
    for (auto item: rowSpec.GetType()->GetItems()) {
        columns.emplace_back(item->GetName());
    }
    for (auto item: rowSpec.GetAuxColumns()) {
        columns.emplace_back(item.first);
    }
    CustomFields_ = ToAtomList(columns, pos, ctx);
    return *this;
}

TConvertInputOpts&
TConvertInputOpts::ExplicitFields(const TStructExprType& type, TPositionHandle pos, TExprContext& ctx) {
    TVector<TString> columns;
    for (auto item: type.GetItems()) {
        columns.emplace_back(item->GetName());
    }
    CustomFields_ = ToAtomList(columns, pos, ctx);
    return *this;
}

TConvertInputOpts&
TConvertInputOpts::KeepDirecRead(bool keepDirecRead) {
    KeepDirecRead_ = keepDirecRead;
    return *this;
}

TConvertInputOpts&
TConvertInputOpts::MakeUnordered(bool makeUnordered) {
    YQL_ENSURE(!makeUnordered || !ClearUnordered_);
    MakeUnordered_ = makeUnordered;
    return *this;
}

TConvertInputOpts&
TConvertInputOpts::ClearUnordered() {
    YQL_ENSURE(!MakeUnordered_);
    ClearUnordered_ = true;
    return *this;
}
        
void TYtSortMembersCollection::BuildKeyFilters(TPositionHandle pos, size_t tableCount, size_t orGroupCount, TExprNode::TListType& result, TExprContext& ctx) {
        TExprNode::TListType prefix;
        for (size_t orGroup = 0; orGroup < orGroupCount; ++orGroup) {
            BuildKeyFiltersImpl(pos, tableCount, orGroup, prefix, Members, result, ctx);
        }
    }

void TYtSortMembersCollection::BuildKeyFiltersImpl(TPositionHandle pos, size_t tableCount, size_t orGroup, TExprNode::TListType& prefix,
    const TMemberDescrMap& members, TExprNode::TListType& result, TExprContext& ctx)
{
    for (auto& item: members) {
        size_t prefixLen = prefix.size();

        auto keyPredicateBuilder = Build<TCoNameValueTupleList>(ctx, pos);
        auto iterRange = item.second->Ranges.equal_range(orGroup);
        if (iterRange.first != iterRange.second) {
            for (auto it = iterRange.first; it != iterRange.second; ++it) {
                TString cmpOp;
                TExprNode::TPtr value;
                const TDataExprType* dataType = nullptr;
                std::tie(cmpOp, value, dataType) = it->second;

                if (!value) {
                    keyPredicateBuilder
                        .Add()
                            .Name()
                                .Value(cmpOp)
                            .Build()
                            .Value<TCoNull>()
                            .Build()
                        .Build();
                } else {
                    keyPredicateBuilder
                        .Add()
                            .Name()
                                .Value(cmpOp)
                            .Build()
                            .Value(value)
                        .Build();
                }
            }

            prefix.push_back(
                Build<TCoNameValueTuple>(ctx, pos)
                    .Name()
                        .Value(item.first)
                    .Build()
                    .Value(keyPredicateBuilder.Done())
                    .Done().Ptr()
            );
        }

        if (!item.second->Tables.empty()) {
            YQL_ENSURE(!prefix.empty());
            if (item.second->Tables.size() == tableCount) {
                result.push_back(Build<TCoNameValueTuple>(ctx, pos)
                    .Name()
                        .Value(ToString(EYtSettingType::KeyFilter))
                    .Build()
                    .Value<TExprList>()
                        .Add<TExprList>()
                            .Add(prefix)
                        .Build()
                    .Build()
                    .Done().Ptr()
                );
            }
            else {
                for (auto tableNdx: item.second->Tables) {
                    result.push_back(Build<TCoNameValueTuple>(ctx, pos)
                        .Name()
                            .Value(ToString(EYtSettingType::KeyFilter))
                        .Build()
                        .Value<TExprList>()
                            .Add<TExprList>()
                                .Add(prefix)
                            .Build()
                            .Add<TCoAtom>()
                                .Value(ToString(tableNdx))
                            .Build()
                        .Build()
                        .Done().Ptr()
                    );
                }
            }
        }
        YQL_ENSURE(item.second->Tables.size() != tableCount || item.second->NextMembers.empty());
        BuildKeyFiltersImpl(pos, tableCount, orGroup, prefix, item.second->NextMembers, result, ctx);

        prefix.erase(prefix.begin() + prefixLen, prefix.end());
    }
}

TExprBase WrapOp(TYtOutputOpBase op, TExprContext& ctx) {
    if (op.Output().Size() > 1) {
        return Build<TCoRight>(ctx, op.Pos())
            .Input(op)
            .Done();
    }

    return Build<TYtOutput>(ctx, op.Pos())
        .Operation(op)
        .OutIndex().Value("0").Build()
        .Done();
}

TCoLambda MapEmbedInputFieldsFilter(TCoLambda lambda, bool ordered, TCoAtomList fields, TExprContext& ctx) {
    auto filter = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
        ui32 index = 0;
        for (const auto& x : fields) {
            parent
                .List(index++)
                    .Add(0, x.Ptr())
                    .Callable(1, "Member")
                        .Arg(0, "row")
                        .Add(1, x.Ptr())
                    .Seal()
                .Seal();
        }

        return parent;
    };

    return TCoLambda(ctx.Builder(lambda.Pos())
        .Lambda()
            .Param("stream")
            .Apply(lambda.Ptr()).With(0)
                .Callable(ordered ? "OrderedFlatMap" : "FlatMap")
                    .Arg(0, "stream")
                    .Lambda(1)
                        .Param("row")
                        .Callable("Just")
                            .Callable(0, "AsStruct")
                                .Do(filter)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Done()
            .Seal()
        .Seal()
        .Build());
}

TVector<TYtOutTable> ConvertMultiOutTables(TPositionHandle pos, const TTypeAnnotationNode* outItemType, TExprContext& ctx,
    const TYtState::TPtr& state, const TMultiConstraintNode* multi) {
    TVector<TYtOutTable> outTables;
    YQL_ENSURE(outItemType->GetKind() == ETypeAnnotationKind::Variant);
    const TTupleExprType* tupleType = outItemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>();
    size_t ndx = 0;
    const ui64 nativeTypeFlags = state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE;
    for (auto tupleItemType: tupleType->GetItems()) {
        TYtOutTableInfo outTableInfo(tupleItemType->Cast<TStructExprType>(), nativeTypeFlags);
        const TConstraintSet* constraints = multi ? multi->GetItem(ndx) : nullptr;
        if (constraints)
            outTableInfo.RowSpec->SetConstraints(*constraints);
        outTables.push_back(outTableInfo.SetUnique(constraints ? constraints->GetConstraint<TDistinctConstraintNode>() : nullptr, pos, ctx).ToExprNode(ctx, pos).Cast<TYtOutTable>());
        ++ndx;
    }
    return outTables;
}

TVector<TYtOutTable> ConvertOutTables(TPositionHandle pos, const TTypeAnnotationNode* outItemType, TExprContext& ctx,
    const TYtState::TPtr& state, const TConstraintSet* constraint) {
    if (outItemType->GetKind() == ETypeAnnotationKind::Variant) {
        return ConvertMultiOutTables(pos, outItemType, ctx, state, constraint ? constraint->GetConstraint<TMultiConstraintNode>() : nullptr);
    }

    TYtOutTableInfo outTableInfo(outItemType->Cast<TStructExprType>(), state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
    if (constraint)
        outTableInfo.RowSpec->SetConstraints(*constraint);
    return TVector<TYtOutTable>{outTableInfo.SetUnique(constraint ? constraint->GetConstraint<TDistinctConstraintNode>() : nullptr, pos, ctx).ToExprNode(ctx, pos).Cast<TYtOutTable>()};
}

TVector<TYtOutTable> ConvertMultiOutTablesWithSortAware(TExprNode::TPtr& lambda, bool& ordered, TPositionHandle pos,
    const TTypeAnnotationNode* outItemType, TExprContext& ctx, const TYtState::TPtr& state, const TConstraintSet& constraints) {

    YQL_ENSURE(outItemType->GetKind() == ETypeAnnotationKind::Variant);

    const ui64 nativeTypeFlags = state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE;
    const bool useNativeDescSort = state->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
    const auto multi = constraints.GetConstraint<TMultiConstraintNode>();
    const TTupleExprType* tupleType = outItemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>();

    ordered = false;
    TVector<TYtOutTable> outTables;
    size_t ndx = 0;
    TVector<TExprBase> switchArgs;
    for (auto tupleItemType: tupleType->GetItems()) {
        const TConstraintSet* itemConstraints = multi ? multi->GetItem(ndx) : nullptr;
        TYtOutTableInfo outTable(tupleItemType->Cast<TStructExprType>(), nativeTypeFlags);
        TExprNode::TPtr remapper;
        if (auto sorted = itemConstraints ? itemConstraints->GetConstraint<TSortedConstraintNode>() : nullptr) {
            TKeySelectorBuilder builder(pos, ctx, useNativeDescSort, tupleItemType->Cast<TStructExprType>());
            builder.ProcessConstraint(*sorted);
            builder.FillRowSpecSort(*outTable.RowSpec);
            if (builder.NeedMap()) {
                remapper = builder.MakeRemapLambda(true);
            }
            ordered = true;
        }
        if (remapper) {
            if (ndx > 0 && switchArgs.empty()) {
                for (size_t i = 0; i < ndx; ++i) {
                    switchArgs.push_back(
                        Build<TCoAtomList>(ctx, pos)
                            .Add()
                                .Value(i)
                            .Build()
                        .Done());
                    switchArgs.push_back(Build<TCoLambda>(ctx, pos).Args({"stream"}).Body("stream").Done());
                }
            }
            switchArgs.push_back(
                Build<TCoAtomList>(ctx, pos)
                    .Add()
                        .Value(ndx)
                    .Build()
                .Done());
            switchArgs.push_back(TExprBase(remapper));
        } else if (!switchArgs.empty()) {
            switchArgs.push_back(
                Build<TCoAtomList>(ctx, pos)
                    .Add()
                        .Value(ndx)
                    .Build()
                .Done());
            switchArgs.push_back(Build<TCoLambda>(ctx, pos).Args({"stream"}).Body("stream").Done());
        }
        if (itemConstraints)
            outTable.RowSpec->SetConstraints(*itemConstraints);
        outTables.push_back(outTable
            .SetUnique(itemConstraints ? itemConstraints->GetConstraint<TDistinctConstraintNode>() : nullptr, pos, ctx)
            .ToExprNode(ctx, pos).Cast<TYtOutTable>()
        );
        ++ndx;
    }
    if (!switchArgs.empty()) {
        lambda = Build<TCoLambda>(ctx, pos)
            .Args({"stream"})
            .Body<TCoSwitch>()
                .Input<TExprApplier>()
                    .Apply(TCoLambda(lambda))
                    .With(0, "stream")
                .Build()
                .BufferBytes()
                    .Value(state->Configuration->SwitchLimit.Get().GetOrElse(DEFAULT_SWITCH_MEMORY_LIMIT))
                .Build()
                .FreeArgs()
                    .Add(switchArgs)
                .Build()
            .Build()
            .Done().Ptr();
    }
    return outTables;
}

TYtOutTable ConvertSingleOutTableWithSortAware(TExprNode::TPtr& lambda, bool& ordered, TPositionHandle pos,
    const TTypeAnnotationNode* outItemType, TExprContext& ctx, const TYtState::TPtr& state, const TConstraintSet& constraints) {

    const ui64 nativeTypeFlags = state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE;
    const bool useNativeDescSort = state->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
    const auto outStructType = outItemType->Cast<TStructExprType>();

    ordered = false;
    TYtOutTableInfo outTable(outStructType, nativeTypeFlags);
    if (auto sorted = constraints.GetConstraint<TSortedConstraintNode>()) {
        TKeySelectorBuilder builder(pos, ctx, useNativeDescSort, outStructType);
        builder.ProcessConstraint(*sorted);
        builder.FillRowSpecSort(*outTable.RowSpec);

        if (builder.NeedMap()) {
            lambda = ctx.Builder(pos)
                .Lambda()
                    .Param("stream")
                    .Apply(builder.MakeRemapLambda(true))
                        .With(0)
                            .Apply(*lambda)
                                .With(0, "stream")
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
                .Build();
        }
        ordered = true;
    }
    outTable.RowSpec->SetConstraints(constraints);
    outTable.SetUnique(constraints.GetConstraint<TDistinctConstraintNode>(), pos, ctx);
    return outTable.ToExprNode(ctx, pos).Cast<TYtOutTable>();
}

TVector<TYtOutTable> ConvertOutTablesWithSortAware(TExprNode::TPtr& lambda, bool& ordered, TPositionHandle pos,
    const TTypeAnnotationNode* outItemType, TExprContext& ctx, const TYtState::TPtr& state, const TConstraintSet& constraints) {
    TVector<TYtOutTable> outTables;
    if (outItemType->GetKind() == ETypeAnnotationKind::Variant) {
        return ConvertMultiOutTablesWithSortAware(lambda, ordered, pos, outItemType, ctx, state, constraints);
    }

    return TVector<TYtOutTable>{ConvertSingleOutTableWithSortAware(lambda, ordered, pos, outItemType, ctx, state, constraints)};
}

}  // namespace NYql 
