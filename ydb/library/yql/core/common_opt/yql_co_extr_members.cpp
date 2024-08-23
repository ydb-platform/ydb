#include "yql_co_extr_members.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_opt_window.h>
#include <ydb/library/yql/core/yql_type_helpers.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

TExprNode::TPtr ApplyExtractMembersToTake(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    TCoTake take(node);
    YQL_CLOG(DEBUG, Core) << "Move ExtractMembers over " << node->Content() << logSuffix;
    return Build<TCoTake>(ctx, node->Pos())
        .Input<TCoExtractMembers>()
            .Input(take.Input())
            .Members(members)
        .Build()
        .Count(take.Count())
        .Done().Ptr();
}

TExprNode::TPtr ApplyExtractMembersToSkip(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    TCoSkip skip(node);
    YQL_CLOG(DEBUG, Core) << "Move ExtractMembers over " << node->Content() << logSuffix;
    return Build<TCoSkip>(ctx, node->Pos())
        .Input<TCoExtractMembers>()
            .Input(skip.Input())
            .Members(members)
        .Build()
        .Count(skip.Count())
        .Done().Ptr();
}

TExprNode::TPtr ApplyExtractMembersToExtend(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    YQL_CLOG(DEBUG, Core) << "Move ExtractMembers over " << node->Content() << logSuffix;
    TExprNode::TListType inputs;
    for (auto& child: node->Children()) {
        inputs.emplace_back(ctx.Builder(child->Pos())
            .Callable(TCoExtractMembers::CallableName())
                .Add(0, child)
                .Add(1, members)
            .Seal()
            .Build());
    }

    return ctx.NewCallable(node->Pos(), node->Content(), std::move(inputs));
}

TExprNode::TPtr ApplyExtractMembersToSkipNullMembers(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    TCoSkipNullMembers skipNullMembers(node);
    const auto& filtered = skipNullMembers.Members();
    if (!filtered) {
        return {};
    }
    TExprNode::TListType filteredMembers;
    for (const auto& x : filtered.Cast()) {
        auto member = x.Value();
        bool hasMember = false;
        for (const auto& y : members->ChildrenList()) {
            if (member == y->Content()) {
                hasMember = true;
                break;
            }
        }

        if (hasMember) {
            filteredMembers.push_back(x.Ptr());
        } else {
            return nullptr;
        }
    }

    YQL_CLOG(DEBUG, Core) << "Move ExtractMembers over " << node->Content() << logSuffix;
    return Build<TCoSkipNullMembers>(ctx, skipNullMembers.Pos())
        .Input<TCoExtractMembers>()
            .Input(skipNullMembers.Input())
            .Members(members)
        .Build()
        .Members(ctx.NewList(skipNullMembers.Pos(), std::move(filteredMembers)))
        .Done().Ptr();
}

TExprNode::TPtr ApplyExtractMembersToFilterNullMembers(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    TCoFilterNullMembers filterNullMembers(node);
    if (!filterNullMembers.Input().Maybe<TCoAssumeAllMembersNullableAtOnce>()) {
        return {};
    }
    auto input = filterNullMembers.Input().Cast<TCoAssumeAllMembersNullableAtOnce>().Input();

    const auto originalStructType = GetSeqItemType(*filterNullMembers.Input().Ref().GetTypeAnn()).Cast<TStructExprType>();

    TExprNode::TPtr extendedMembers;
    TMaybeNode<TCoAtomList> filteredMembers;
    if (const auto& filtered = filterNullMembers.Members()) {
        TExprNode::TListType updatedMembers;
        for (const auto& x : filtered.Cast()) {
            auto member = x.Value();
            bool hasMember = false;
            for (const auto& y : members->ChildrenList()) {
                if (member == y->Content()) {
                    hasMember = true;
                    break;
                }
            }

            if (hasMember) {
                updatedMembers.push_back(x.Ptr());
            }
        }
        if ((members->ChildrenList().size() + updatedMembers.empty()) == originalStructType->GetSize()) {
            return {};
        }
        if (updatedMembers.empty()) {
            // Keep at least one optional field in input
            const auto extra = filtered.Cast().Item(0).Ptr();
            updatedMembers.push_back(extra);
            auto list = members->ChildrenList();
            list.push_back(extra);
            extendedMembers = ctx.NewList(members->Pos(), std::move(list));
        }
        filteredMembers = TCoAtomList(ctx.NewList(filtered.Cast().Pos(), std::move(updatedMembers)));
    } else {

        bool hasOptional = false;
        for (const auto& y : members->ChildrenList()) {
            if (auto type = originalStructType->FindItemType(y->Content()); type->GetKind() == ETypeAnnotationKind::Optional) {
                hasOptional = true;
                break;
            }
        }

        if ((members->ChildrenList().size() + !hasOptional) == originalStructType->GetSize()) {
            return {};
        }

        if (!hasOptional) {
            // Keep at least one optional field in input (use first any optional field)
            for (const auto& x : originalStructType->GetItems()) {
                if (x->GetItemType()->GetKind() == ETypeAnnotationKind::Optional) {
                    auto list = members->ChildrenList();
                    list.push_back(ctx.NewAtom(members->Pos(), x->GetName()));
                    extendedMembers = ctx.NewList(members->Pos(), std::move(list));
                    break;
                }
            }
            YQL_ENSURE(extendedMembers);
        }
    }

    YQL_CLOG(DEBUG, Core) << "Move ExtractMembers over " << node->Content() << logSuffix;

    if (extendedMembers) {
        return Build<TCoExtractMembers>(ctx, filterNullMembers.Pos())
            .Input<TCoFilterNullMembers>()
                .Input<TCoExtractMembers>()
                    .Input(input)
                    .Members(extendedMembers)
                .Build()
                .Members(filteredMembers)
            .Build()
            .Members(members)
            .Done().Ptr();
    }

    return Build<TCoFilterNullMembers>(ctx, filterNullMembers.Pos())
        .Input<TCoExtractMembers>()
            .Input(input)
            .Members(members)
        .Build()
        .Members(filteredMembers)
        .Done().Ptr();
}

TExprNode::TPtr ApplyExtractMembersToSort(const TExprNode::TPtr& node, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx, TStringBuf logSuffix) {
    TCoSortBase sort(node);
    TSet<TStringBuf> extractFields;
    for (const auto& x : members->ChildrenList()) {
        extractFields.emplace(x->Content());
    }
    TSet<TStringBuf> sortKeys;
    bool fieldSubset = HaveFieldsSubset(sort.KeySelectorLambda().Body().Ptr(), sort.KeySelectorLambda().Args().Arg(0).Ref(), sortKeys, parentsMap);
    bool allExist = true;
    if (!sortKeys.empty()) {
        for (const auto& key : sortKeys) {
            auto ret = extractFields.emplace(key);
            if (ret.second) {
                allExist = false;
            }
        }
    }
    if (allExist && sortKeys.size() == extractFields.size()) {
        YQL_CLOG(DEBUG, Core) << "Force `fieldSubset` for ExtractMembers over " << node->Content();
        fieldSubset = true;
    }
    if (fieldSubset && allExist) {
        YQL_CLOG(DEBUG, Core) << "Move ExtractMembers over " << node->Content() << logSuffix;
        return ctx.Builder(sort.Pos())
            .Callable(node->Content())
                .Callable(0, TCoExtractMembers::CallableName())
                    .Add(0, sort.Input().Ptr())
                    .Add(1, members)
                .Seal()
                .Add(1, sort.SortDirections().Ptr())
                .Add(2, ctx.DeepCopyLambda(sort.KeySelectorLambda().Ref()))
            .Seal()
            .Build();
    }
    else if (fieldSubset) {
        const auto structType = GetSeqItemType(*sort.Ref().GetTypeAnn()).Cast<TStructExprType>();
        if (structType->GetSize() <= extractFields.size()) {
            return {};
        }
        YQL_CLOG(DEBUG, Core) << "Inject ExtractMembers into " << node->Content() << logSuffix;
        TExprNode::TListType totalExtracted;
        for (const auto& field : extractFields) {
            totalExtracted.emplace_back(ctx.NewAtom(members->Pos(), field));
        }

        return ctx.Builder(sort.Pos())
            .Callable(TCoExtractMembers::CallableName())
                .Callable(0, node->Content())
                    .Callable(0, TCoExtractMembers::CallableName())
                        .Add(0, sort.Input().Ptr())
                        .Add(1, ctx.NewList(members->Pos(), std::move(totalExtracted)))
                    .Seal()
                    .Add(1, sort.SortDirections().Ptr())
                    .Add(2, ctx.DeepCopyLambda(sort.KeySelectorLambda().Ref()))
                .Seal()
                .Add(1, members)
            .Seal()
            .Build();
    }
    return {};
}

TExprNode::TPtr ApplyExtractMembersToAssumeUnique(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    std::unordered_set<std::string_view> extractFields(members->ChildrenSize());
    for (const auto& x : members->ChildrenList()) {
        extractFields.emplace(x->Content());
    }

    auto children = node->ChildrenList();
    children.front() = ctx.NewCallable(node->Pos(), TCoExtractMembers::CallableName(), {std::move(children.front()), members});
    auto it = children.begin();
    it = std::remove_if(++it, children.end(), [&extractFields](const TExprNode::TPtr& list) { return AnyOf(list->Children(), [&extractFields] (const TExprNode::TPtr& u) { return !extractFields.contains(u->Content()); }); });
    children.erase(it, children.cend());

    if (children.size() > 1U) {
        YQL_CLOG(DEBUG, Core) << "Move ExtractMembers over " << node->Content() << logSuffix;
        return ctx.ChangeChildren(*node, std::move(children));
    } else {
        YQL_CLOG(DEBUG, Core) << "Drop " << node->Content() << " after ExtractMembers" << logSuffix;
        return std::move(children.front());
    }
}

TExprNode::TPtr ApplyExtractMembersToTop(const TExprNode::TPtr& node, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx, TStringBuf logSuffix) {
    TCoTopBase top(node);
    TSet<TStringBuf> extractFields;
    for (const auto& x : members->ChildrenList()) {
        extractFields.emplace(x->Content());
    }
    TSet<TStringBuf> sortKeys;
    const bool fieldSubset = HaveFieldsSubset(top.KeySelectorLambda().Body().Ptr(), top.KeySelectorLambda().Args().Arg(0).Ref(), sortKeys, parentsMap);
    bool allExist = true;
    if (!sortKeys.empty()) {
        for (const auto& key : sortKeys) {
            if (!extractFields.contains(key)) {
                allExist = false;
                extractFields.emplace(key);
            }
        }
    }
    if (fieldSubset && allExist) {
        YQL_CLOG(DEBUG, Core) << "Move ExtractMembers over " << node->Content() << logSuffix;
        auto children = node->ChildrenList();
        children[TCoTopBase::idx_Input] = Build<TCoExtractMembers>(ctx, top.Pos())
            .Input(top.Input())
            .Members(members)
            .Done().Ptr();
        children[TCoTopBase::idx_KeySelectorLambda] = ctx.DeepCopyLambda(top.KeySelectorLambda().Ref());
        return ctx.ChangeChildren(*node, std::move(children));
    }
    else if (fieldSubset) {
        const auto structType = GetSeqItemType(*top.Ref().GetTypeAnn()).Cast<TStructExprType>();
        if (structType->GetSize() <= extractFields.size()) {
            return {};
        }
        YQL_CLOG(DEBUG, Core) << "Inject ExtractMembers into " << node->Content() << logSuffix;
        TExprNode::TListType totalExtracted;
        for (const auto& field : extractFields) {
            totalExtracted.emplace_back(ctx.NewAtom(members->Pos(), field));
        }

        auto children = node->ChildrenList();
        children[TCoTopBase::idx_Input] = Build<TCoExtractMembers>(ctx, top.Pos())
            .Input(top.Input())
            .Members(ctx.NewList(members->Pos(), std::move(totalExtracted)))
            .Done().Ptr();
        children[TCoTopBase::idx_KeySelectorLambda] = ctx.DeepCopyLambda(top.KeySelectorLambda().Ref());
        auto updatedTop = ctx.ChangeChildren(*node, std::move(children));

        return Build<TCoExtractMembers>(ctx, top.Pos())
            .Input(updatedTop)
            .Members(members)
            .Done().Ptr();
    }
    return {};
}

TExprNode::TPtr ApplyExtractMembersToEquiJoin(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    TCoEquiJoin join(node);
    const auto structType = GetSeqItemType(*join.Ref().GetTypeAnn()).Cast<TStructExprType>();
    if (structType->GetSize() == 0) {
        return {};
    }

    YQL_CLOG(DEBUG, Core) << "Move ExtractMembers over " << node->Content() << logSuffix;
    auto joinSettings = join.Arg(join.ArgCount() - 1).Ptr();
    auto renameMap = LoadJoinRenameMap(*joinSettings);
    joinSettings = RemoveSetting(*joinSettings, "rename", ctx);
    TSet<TStringBuf> usedFields;
    for (const auto& x : members->ChildrenList()) {
        usedFields.emplace(x->Content());
    }

    auto newRenameMap = UpdateUsedFieldsInRenameMap(renameMap, usedFields, structType);
    TExprNode::TListType joinSettingsNodes = joinSettings->ChildrenList();
    AppendEquiJoinRenameMap(join.Pos(), newRenameMap, joinSettingsNodes, ctx);
    joinSettings = ctx.ChangeChildren(*joinSettings, std::move(joinSettingsNodes));
    auto updatedEquiJoin = ctx.ShallowCopy(join.Ref());
    updatedEquiJoin->ChildRef(updatedEquiJoin->ChildrenSize() - 1) = joinSettings;
    return updatedEquiJoin;
}

TExprNode::TPtr ApplyExtractMembersToFlatMap(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    TCoFlatMapBase flatmap(node);
    YQL_CLOG(DEBUG, Core) << "Apply ExtractMembers to " << node->Content() << logSuffix;

    auto body = flatmap.Lambda().Body();
    TMaybeNode<TExprBase> newBody;

    if (auto maybeConditional = body.Maybe<TCoConditionalValueBase>()) {
        auto conditional = maybeConditional.Cast();
        TMaybeNode<TExprBase> extracted;

        if (body.Maybe<TCoListIf>() || body.Maybe<TCoOptionalIf>()) {
            TVector<TExprBase> tuples;
            for (const auto& member : members->ChildrenList()) {
                auto tuple = Build<TCoNameValueTuple>(ctx, flatmap.Pos())
                    .Name(member)
                    .Value<TCoMember>()
                        .Struct(conditional.Value())
                        .Name(member)
                        .Build()
                    .Done();

                tuples.push_back(tuple);
            }

            extracted = Build<TCoAsStruct>(ctx, flatmap.Pos())
                .Add(tuples)
                .Done();
        } else {
            extracted = Build<TCoExtractMembers>(ctx, flatmap.Pos())
                .Input(conditional.Value())
                .Members(members)
                .Done();
        }

        newBody = ctx.ChangeChild(conditional.Ref(), TCoConditionalValueBase::idx_Value, extracted.Cast().Ptr());
    } else {
        newBody = Build<TCoExtractMembers>(ctx, flatmap.Pos())
            .Input(flatmap.Lambda().Body())
            .Members(members)
            .Done();
    }

    if (flatmap.Maybe<TCoOrderedFlatMap>()) {
        return Build<TCoOrderedFlatMap>(ctx, flatmap.Pos())
            .Input(flatmap.Input())
            .Lambda()
                .Args({"item"})
                .template Body<TExprApplier>()
                    .Apply(newBody.Cast())
                    .With(flatmap.Lambda().Args().Arg(0), "item")
                    .Build()
                .Build()
            .Done()
            .Ptr();
    } else {
        return Build<TCoFlatMap>(ctx, flatmap.Pos())
            .Input(flatmap.Input())
            .Lambda()
                .Args({"item"})
                .template Body<TExprApplier>()
                    .Apply(newBody.Cast())
                    .With(flatmap.Lambda().Args().Arg(0), "item")
                    .Build()
                .Build()
            .Done()
            .Ptr();
    }
}

TExprNode::TPtr ApplyExtractMembersToPartitionByKey(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    TCoPartitionByKey part(node);
    YQL_CLOG(DEBUG, Core) << "Apply ExtractMembers to " << node->Content() << logSuffix;
    auto newBody = Build<TCoExtractMembers>(ctx, part.Pos())
        .Input(part.ListHandlerLambda().Body())
        .Members(members)
        .Done();

    return Build<TCoPartitionByKey>(ctx, part.Pos())
        .Input(part.Input())
        .KeySelectorLambda(part.KeySelectorLambda())
        .ListHandlerLambda()
            .Args({"groups"})
            .Body<TExprApplier>()
                .Apply(newBody)
                .With(part.ListHandlerLambda().Args().Arg(0), "groups")
            .Build()
        .Build()
        .SortDirections(part.SortDirections())
        .SortKeySelectorLambda(part.SortKeySelectorLambda())
        .Done()
        .Ptr();
}

TExprNode::TPtr ApplyExtractMembersToChopper(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    const TCoChopper chopper(node);
    YQL_CLOG(DEBUG, Core) << "Apply ExtractMembers to " << node->Content() << logSuffix;
    auto newBody = Build<TCoExtractMembers>(ctx, chopper.Handler().Pos())
        .Input(chopper.Handler().Body())
        .Members(members)
        .Done();

    return Build<TCoChopper>(ctx, chopper.Pos())
        .Input(chopper.Input())
        .KeyExtractor(chopper.KeyExtractor())
        .GroupSwitch(chopper.GroupSwitch())
        .Handler()
            .Args({"key", "group"})
            .Body<TExprApplier>()
                .Apply(newBody)
                .With(chopper.Handler().Args().Arg(0), "key")
                .With(chopper.Handler().Args().Arg(1), "group")
            .Build()
        .Build()
        .Done()
        .Ptr();
}

TExprNode::TPtr ApplyExtractMembersToMapJoinCore(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    const TCoMapJoinCore mapJoin(node);
    YQL_CLOG(DEBUG, Core) << "Apply ExtractMembers to " << node->Content() << logSuffix;
    TNodeSet used(members->ChildrenSize());
    members->ForEachChild([&used](const TExprNode& member) { used.emplace(&member); });

    auto right = mapJoin.RightRenames().Ref().ChildrenList();
    for (auto it = right.cbegin(); it < right.cend();) {
        if (used.contains((++it)->Get()))
            ++it;
        else {
            auto to = it;
            it = right.erase(--it, ++to);
        }
    }

    auto left = mapJoin.LeftRenames().Ref().ChildrenList();
    auto input = mapJoin.LeftKeysColumns().Ref().ChildrenList();
    const auto leftColumsEstimate = input.size() + (left.size() >> 1U);
    input.reserve(leftColumsEstimate);
    TNodeSet set(leftColumsEstimate);
    for (auto it = input.cbegin(); input.cend() != it;) {
        if (set.emplace(it->Get()).second)
            ++it;
        else
            it = input.erase(it);
    }

    for (auto it = left.cbegin(); it < left.cend();) {
        if (set.emplace(it->Get()).second)
            input.emplace_back(*it);
        if (used.contains((++it)->Get()))
            ++it;
        else {
            auto to = it;
            it = left.erase(--it, ++to);
        }
    }

    return Build<TCoMapJoinCore>(ctx, mapJoin.Pos())
        .LeftInput<TCoExtractMembers>()
            .Input(mapJoin.LeftInput())
            .Members(ctx.NewList(mapJoin.Pos(), std::move(input)))
            .Build()
        .RightDict(mapJoin.RightDict())
        .JoinKind(mapJoin.JoinKind())
        .LeftKeysColumns(mapJoin.LeftKeysColumns())
        .RightKeysColumns(mapJoin.RightKeysColumns())
        .LeftRenames(ctx.NewList(mapJoin.LeftInput().Pos(), std::move(left)))
        .RightRenames(ctx.NewList(mapJoin.RightRenames().Pos(), std::move(right)))
        .LeftKeysColumnNames(mapJoin.LeftKeysColumnNames())
        .RightKeysColumnNames(mapJoin.RightKeysColumnNames())
        .Done().Ptr();
}

TExprNode::TPtr ApplyExtractMembersToCalcOverWindow(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    YQL_ENSURE(node->IsCallable({"CalcOverWindow", "CalcOverSessionWindow", "CalcOverWindowGroup"}));

    auto input = node->ChildPtr(0);

    // window output = input fields + payload fields
    TSet<TStringBuf> outMembers;
    for (const auto& x : members->ChildrenList()) {
        outMembers.insert(x->Content());
    }

    auto inputStructType = input->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto outputStructType = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    TSet<TStringBuf> toDrop;
    for (const auto& out : outputStructType->GetItems()) {
        if (!outMembers.contains(out->GetName())) {
            toDrop.insert(out->GetName());
        }
    }

    TSet<TStringBuf> usedFields;
    TSet<TStringBuf> payloadFields;
    TExprNodeList newCalcs;
    auto calcs = ExtractCalcsOverWindow(node, ctx);
    bool dropped = false;
    for (auto& calcNode : calcs) {
        TCoCalcOverWindowTuple calc(calcNode);

        // all partition keys will be used
        for (const auto& key : calc.Keys()) {
            usedFields.insert(key.Value());
        }

        auto processListType = [&](TExprBase typeNode) {
            auto structType = typeNode.Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->
                Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            for (const auto& item : structType->GetItems()) {
                usedFields.insert(item->GetName());
            }
        };

        auto processSortTraits = [&](TExprBase node) {
            if (auto maybeSortTraits = node.Maybe<TCoSortTraits>()) {
                processListType(maybeSortTraits.Cast().ListType());
            } else {
                YQL_ENSURE(node.Maybe<TCoVoid>());
            }
        };

        // all sort keys will be used
        processSortTraits(calc.SortSpec());

        // all session keys + session sort keys will be used
        if (auto maybeSwt = calc.SessionSpec().Maybe<TCoSessionWindowTraits>()) {
            processListType(maybeSwt.Cast().ListType());
            processSortTraits(maybeSwt.Cast().SortSpec());
        } else {
            YQL_ENSURE(calc.SessionSpec().Maybe<TCoVoid>());
        }

        TExprNodeList newSessionColumns;
        for (auto& sessionColumn : calc.SessionColumns().Ref().ChildrenList()) {
            TStringBuf columnName = sessionColumn->Content();
            if (toDrop.contains(columnName)) {
                dropped = true;
                continue;
            }
            payloadFields.insert(columnName);
            newSessionColumns.push_back(sessionColumn);
        }

        TExprNodeList newFrames;
        for (const auto& winOnRows : calc.Frames().Ref().ChildrenList()) {
            YQL_ENSURE(TCoWinOnBase::Match(winOnRows.Get()));

            TExprNodeList newFrameItems;
            newFrameItems.push_back(winOnRows->ChildPtr(0));

            for (ui32 i = 1; i < winOnRows->ChildrenSize(); ++i) {
                auto field = winOnRows->Child(i)->Child(0)->Content();
                if (toDrop.contains(field)) {
                    dropped = true;
                    continue;
                }

                payloadFields.insert(field);
                newFrameItems.push_back(winOnRows->ChildPtr(i));
                auto payload = winOnRows->Child(i)->Child(1);
                const TStructExprType* structType;
                if (payload->IsCallable("WindowTraits")) {
                    structType = payload->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                }
                else if (payload->IsCallable({"Lead", "Lag", "Rank", "DenseRank", "PercentRank"})) {
                    structType = payload->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TListExprType>()
                        ->GetItemType()->Cast<TStructExprType>();
                } else {
                    continue;
                }

                for (const auto& item : structType->GetItems()) {
                    usedFields.insert(item->GetName());
                }
            }

            if (newFrameItems.size() == 1) {
                continue;
            }

            newFrames.push_back(ctx.ChangeChildren(*winOnRows, std::move(newFrameItems)));
        }

        newCalcs.emplace_back(
            Build<TCoCalcOverWindowTuple>(ctx, calc.Pos())
                .Keys(calc.Keys())
                .SortSpec(calc.SortSpec())
                .Frames(ctx.NewList(calc.Frames().Pos(), std::move(newFrames)))
                .SessionSpec(calc.SessionSpec())
                .SessionColumns(ctx.NewList(calc.SessionColumns().Pos(), std::move(newSessionColumns)))
                .Done().Ptr()
        );
    }

    // keep input fields
    for (const auto& in : inputStructType->GetItems()) {
        if (outMembers.contains(in->GetName()) && !payloadFields.contains(in->GetName())) {
            usedFields.insert(in->GetName());
        }
    }

    if (usedFields.size() == inputStructType->GetSize() && !dropped) {
        return {};
    }

    TExprNode::TListType usedExprList;
    for (const auto& x : usedFields) {
        usedExprList.push_back(ctx.NewAtom(node->Pos(), x));
    }

    auto newInput = Build<TCoExtractMembers>(ctx, node->Pos())
        .Input(input)
        .Members(ctx.NewList(node->Pos(), std::move(usedExprList)))
        .Done()
        .Ptr();

    auto calcOverWindow = Build<TCoCalcOverWindowGroup>(ctx, node->Pos())
        .Input(newInput)
        .Calcs(ctx.NewList(node->Pos(), std::move(newCalcs)))
        .Done().Ptr();

    YQL_CLOG(DEBUG, Core) << "Apply ExtractMembers to " << node->Content() << logSuffix;
    return Build<TCoExtractMembers>(ctx, node->Pos())
        .Input(calcOverWindow)
        .Members(members)
        .Done()
        .Ptr();
}

TExprNode::TPtr ApplyExtractMembersToAggregate(const TExprNode::TPtr& node, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx, TStringBuf logSuffix) {
    TCoAggregate aggr(node);
    TSet<TStringBuf> outMembers;
    for (const auto& x : members->ChildrenList()) {
        outMembers.insert(x->Content());
    }

    // TODOD: remove ExtractMembers pushdown to inputs when FieldSubsetEnableMultiusage is enabled
    TMaybe<TStringBuf> sessionColumn;
    const auto sessionSetting = GetSetting(aggr.Settings().Ref(), "session");
    if (sessionSetting) {
        YQL_ENSURE(sessionSetting->Child(1)->Child(0)->IsAtom());
        sessionColumn = sessionSetting->Child(1)->Child(0)->Content();
    }

    TMaybe<TStringBuf> hoppingColumn = "_yql_time";
    const auto hoppingSetting = GetSetting(aggr.Settings().Ref(), "hopping");
    if (hoppingSetting) {
        auto hoppingSettingValue = hoppingSetting->Child(1);
        bool isLegacyHopping = !hoppingSettingValue->IsList();
        if (!isLegacyHopping) {
            YQL_ENSURE(hoppingSettingValue->Child(0)->IsAtom());
            hoppingColumn = hoppingSettingValue->Child(0)->Content();
        }
    }

    TSet<TStringBuf> usedFields;
    // all actual (non-session/non-hopping) keys will be used
    for (const auto& key : aggr.Keys()) {
        if (key.Value() != sessionColumn && key.Value() != hoppingColumn) {
            usedFields.insert(key.Value());
        }
    }

    for (const auto& handler : aggr.Handlers()) {
        if (handler.DistinctName()) {
            usedFields.insert(handler.DistinctName().Cast().Value());
        } else {
            const auto& trait = handler.Trait().Ref();
            ui32 index;
            if (trait.IsCallable("AggregationTraits")) {
                index = 0;
            } else if (trait.IsCallable("AggApply")) {
                index = 1;
            } else {
                return {};
            }

            auto structType = trait.Child(index)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
            for (const auto& item : structType->GetItems()) {
                usedFields.insert(item->GetName());
            }
        }
    }

    if (hoppingSetting) {
        auto traitsNode = hoppingSetting->ChildPtr(1);
        if (traitsNode->IsList()) {
            traitsNode = traitsNode->ChildPtr(1);
        }
        auto traits = TCoHoppingTraits(traitsNode);
        auto timeExtractor = traits.TimeExtractor();

        auto usedType = traits.ItemType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
        for (const auto& usedField : usedType->GetItems()) {
            usedFields.insert(usedField->GetName());
        }

        TSet<TStringBuf> lambdaSubset;
        if (HaveFieldsSubset(timeExtractor.Body().Ptr(), *timeExtractor.Args().Arg(0).Raw(), lambdaSubset, parentsMap)) {
            usedFields.insert(lambdaSubset.cbegin(), lambdaSubset.cend());
        }
    }

    if (sessionSetting) {
        TCoSessionWindowTraits traits(sessionSetting->Child(1)->ChildPtr(1));

        auto usedType = traits.ListType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TListExprType>()->
            GetItemType()->Cast<TStructExprType>();
        for (const auto& item : usedType->GetItems()) {
            usedFields.insert(item->GetName());
        }
    }

    auto inputStructType = aggr.Input().Ptr()->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    YQL_ENSURE(usedFields.size() <= inputStructType->GetSize());
    auto newInput = aggr.Input();
    if (usedFields.size() < inputStructType->GetSize()) {
        TExprNode::TListType usedExprList;
        for (const auto& x : usedFields) {
            usedExprList.push_back(ctx.NewAtom(aggr.Pos(), x));
        }
        newInput = Build<TCoExtractMembers>(ctx, aggr.Pos())
            .Input(aggr.Input())
            .Members(ctx.NewList(aggr.Pos(), std::move(usedExprList)))
            .Done();
    }

    YQL_CLOG(DEBUG, Core) << "Apply ExtractMembers to " << node->Content() << logSuffix;
    return Build<TCoAggregate>(ctx, aggr.Pos())
        .InitFrom(aggr)
        .Input(newInput)
        .Settings(ReplaceSetting(aggr.Settings().Ref(), members->Pos(), "output_columns", members, ctx))
        .Done()
        .Ptr();
}

TExprNode::TPtr ApplyExtractMembersToCollect(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    TCoCollect collect(node);
    YQL_CLOG(DEBUG, Core) << "Move ExtractMembers over " << node->Content() << logSuffix;
    return Build<TCoCollect>(ctx, node->Pos())
        .Input<TCoExtractMembers>()
            .Input(collect.Input())
            .Members(members)
        .Build()
        .Done().Ptr();
}

TExprNode::TPtr ApplyExtractMembersToMapNext(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    const TCoMapNext mapNext(node);
    YQL_CLOG(DEBUG, Core) << "Apply ExtractMembers to " << node->Content() << logSuffix;

    return Build<TCoMapNext>(ctx, mapNext.Pos())
        .Input(mapNext.Input())
        .Lambda()
            .Args({"current", "next"})
            .Body<TCoFilterMembers>()
                .Input<TExprApplier>()
                    .Apply(mapNext.Lambda())
                    .With(0, "current")
                    .With(1, "next")
                    .Build()
                .Members(members)
                .Build()
            .Build()
        .Done()
        .Ptr();
}

TExprNode::TPtr ApplyExtractMembersToChain1Map(const TExprNode::TPtr& node, TExprNode::TPtr members, const TParentsMap& parentsMap, TExprContext& ctx, TStringBuf logSuffix) {
    const TCoChain1Map chain1Map(node);
    const auto allMembers = AddMembersUsedInside(chain1Map.UpdateHandler().Body().Ptr(), chain1Map.UpdateHandler().Args().Arg(1).Ref(), TExprNode::TPtr(members), parentsMap, ctx);
    if (!allMembers || GetSeqItemType(*node->GetTypeAnn()).Cast<TStructExprType>()->GetSize() <= allMembers->ChildrenSize())
        return {};

    YQL_CLOG(DEBUG, Core) << "Apply ExtractMembers to " << node->Content() << logSuffix;
    auto output = Build<TCoChain1Map>(ctx, chain1Map.Pos())
        .Input(chain1Map.Input())
        .InitHandler()
            .Args({"item"})
            .Body<TCoFilterMembers>()
                .Input<TExprApplier>()
                    .Apply(chain1Map.InitHandler())
                    .With(0, "item")
                    .Build()
                .Members(allMembers)
                .Build()
            .Build()
        .UpdateHandler()
            .Args({"prev","next"})
            .Body<TCoFilterMembers>()
                .Input<TExprApplier>()
                    .Apply(chain1Map.UpdateHandler())
                    .With(0, "prev")
                    .With(1, "next")
                    .Build()
                .Members(allMembers)
                .Build()
            .Build()
        .Done().Ptr();

    if (allMembers != members) {
        output = Build<TCoExtractMembers>(ctx, chain1Map.Pos())
            .Input(std::move(output))
            .Members(std::move(members))
            .Done().Ptr();
    }

    return output;
}

TExprNode::TPtr ApplyExtractMembersToCondense1(const TExprNode::TPtr& node, TExprNode::TPtr members, const TParentsMap& parentsMap, TExprContext& ctx, TStringBuf logSuffix) {
    const TCoCondense1 condense1(node);
    auto allMembers = members;
    allMembers = AddMembersUsedInside(condense1.UpdateHandler().Body().Ptr(), condense1.UpdateHandler().Args().Arg(1).Ref(), std::move(allMembers), parentsMap, ctx);
    allMembers = AddMembersUsedInside(condense1.SwitchHandler().Body().Ptr(), condense1.SwitchHandler().Args().Arg(1).Ref(), std::move(allMembers), parentsMap, ctx);

    if (!allMembers || GetSeqItemType(*node->GetTypeAnn()).Cast<TStructExprType>()->GetSize() <= allMembers->ChildrenSize())
        return {};

    YQL_CLOG(DEBUG, Core) << "Apply ExtractMembers to " << node->Content() << logSuffix;
    auto output = Build<TCoCondense1>(ctx, condense1.Pos())
        .Input(condense1.Input())
        .InitHandler()
            .Args({"item"})
            .Body<TCoFilterMembers>()
                .Input<TExprApplier>()
                    .Apply(condense1.InitHandler())
                    .With(0, "item")
                    .Build()
                .Members(allMembers)
                .Build()
            .Build()
        .SwitchHandler(condense1.SwitchHandler())
        .UpdateHandler()
            .Args({"prev","next"})
            .Body<TCoFilterMembers>()
                .Input<TExprApplier>()
                    .Apply(condense1.UpdateHandler())
                    .With(0, "prev")
                    .With(1, "next")
                    .Build()
                .Members(allMembers)
                .Build()
            .Build()
        .Done().Ptr();

    if (allMembers != members) {
        output = Build<TCoExtractMembers>(ctx, condense1.Pos())
            .Input(std::move(output))
            .Members(std::move(members))
            .Done().Ptr();
    }

    return output;
}

TExprNode::TPtr ApplyExtractMembersToCombineCore(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix) {
    const TCoCombineCore core(node);
    YQL_CLOG(DEBUG, Core) << "Apply ExtractMembers to " << node->Content() << logSuffix;

    return Build<TCoCombineCore>(ctx, core.Pos())
        .InitFrom(core)
        .FinishHandler()
            .Args({"key", "state"})
            .Body<TCoExtractMembers>()
                .Input<TExprApplier>()
                    .Apply(core.FinishHandler())
                    .With(0, "key")
                    .With(1, "state")
                    .Build()
                .Members(members)
                .Build()
            .Build()
        .Done()
        .Ptr();
}

TExprNode::TPtr ApplyExtractMembersToNarrowMap(const TExprNode::TPtr& node, const TExprNode::TPtr& members, bool isFlat, TExprContext& ctx, TStringBuf logSuffix) {
    YQL_CLOG(DEBUG, Core) << "Apply ExtractMembers to " << node->Content() << logSuffix;
    auto body = GetLambdaBody(node->Tail());
    std::for_each(body.begin(), body.end(), [&](TExprNode::TPtr& root) {
        const auto pos = root->Pos();
        root = ctx.NewCallable(pos, isFlat ? TCoExtractMembers::CallableName() : TCoFilterMembers::CallableName(), {std::move(root), members});
    });
    return ctx.ChangeChild(*node, TCoMapBase::idx_Lambda, ctx.DeepCopyLambda(node->Tail(), std::move(body)));
}

} // NYql
