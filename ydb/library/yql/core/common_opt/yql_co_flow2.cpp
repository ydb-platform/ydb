#include "yql_co_extr_members.h"
#include "yql_flatmap_over_join.h"
#include "yql_co.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_opt_window.h>
#include <ydb/library/yql/core/yql_type_helpers.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {
namespace {

using namespace NNodes;

bool AllowSubsetFieldsForNode(const TExprNode& node, const TOptimizeContext& optCtx) {
    YQL_ENSURE(optCtx.Types);
    static const TString multiUsageFlags = to_lower(TString("FieldSubsetEnableMultiusage"));
    return optCtx.IsSingleUsage(node) || optCtx.Types->OptimizerFlags.contains(multiUsageFlags);
}

bool AllowComplexFiltersOverAggregatePushdown(const TOptimizeContext& optCtx) {
    YQL_ENSURE(optCtx.Types);
    static const TString pushdown = to_lower(TString("PushdownComplexFiltersOverAggregate"));
    return optCtx.Types->OptimizerFlags.contains(pushdown);
}

TExprNode::TPtr AggregateSubsetFieldsAnalyzer(const TCoAggregate& node, TExprContext& ctx, const TParentsMap& parentsMap) {
    auto inputType = node.Input().Ref().GetTypeAnn();
    auto structType = inputType->GetKind() == ETypeAnnotationKind::List
        ? inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()
        : inputType->Cast<TStreamExprType>()->GetItemType()->Cast<TStructExprType>();

    if (structType->GetSize() == 0) {
        return node.Ptr();
    }

    TMaybe<TStringBuf> sessionColumn;
    const auto sessionSetting = GetSetting(node.Settings().Ref(), "session");
    if (sessionSetting) {
        YQL_ENSURE(sessionSetting->Child(1)->Child(0)->IsAtom());
        sessionColumn = sessionSetting->Child(1)->Child(0)->Content();
    }

    TMaybe<TStringBuf> hoppingColumn;
    auto hoppingSetting = GetSetting(node.Settings().Ref(), "hopping");
    if (hoppingSetting) {
        auto traitsNode = hoppingSetting->ChildPtr(1);
        if (traitsNode->IsList()) {
            YQL_ENSURE(traitsNode->Child(0)->IsAtom());
            hoppingColumn = traitsNode->Child(0)->Content();
        }
    }

    TSet<TStringBuf> usedFields;
    for (const auto& x : node.Keys()) {
        if (x.Value() != sessionColumn && x.Value() != hoppingColumn) {
            usedFields.insert(x.Value());
        }
    }

    if (usedFields.size() == structType->GetSize()) {
        return node.Ptr();
    }

    for (const auto& x : node.Handlers()) {
        if (x.Ref().ChildrenSize() == 3) {
            // distinct field
            usedFields.insert(x.Ref().Child(2)->Content());
        }
        else {
            auto traits = x.Ref().Child(1);
            ui32 index;
            if (traits->IsCallable("AggregationTraits")) {
                index = 0;
            } else if (traits->IsCallable("AggApply")) {
                index = 1;
            } else {
                return node.Ptr();
            }

            auto structType = traits->Child(index)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
            for (const auto& item : structType->GetItems()) {
                usedFields.insert(item->GetName());
            }
        }

        if (usedFields.size() == structType->GetSize()) {
            return node.Ptr();
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

        if (usedFields.size() == structType->GetSize()) {
            return node.Ptr();
        }
    }

    if (sessionSetting) {
        TCoSessionWindowTraits traits(sessionSetting->Child(1)->ChildPtr(1));

        auto usedType = traits.ListType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TListExprType>()->
            GetItemType()->Cast<TStructExprType>();
        for (const auto& item : usedType->GetItems()) {
            usedFields.insert(item->GetName());
        }

        if (usedFields.size() == structType->GetSize()) {
            return node.Ptr();
        }
    }

    TExprNode::TListType keepMembersList;
    for (const auto& x : usedFields) {
        keepMembersList.push_back(ctx.NewAtom(node.Pos(), x));
    }

    auto newInput = ctx.Builder(node.Pos())
        .Callable("ExtractMembers")
            .Add(0, node.Input().Ptr())
            .Add(1, ctx.NewList(node.Pos(), std::move(keepMembersList)))
        .Seal()
        .Build();

    auto ret = ctx.ChangeChild(node.Ref(), 0, std::move(newInput));
    return ret;
}

TExprNode::TPtr FlatMapSubsetFields(const TCoFlatMapBase& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    if (!AllowSubsetFieldsForNode(node.Input().Ref(), optCtx)) {
        return node.Ptr();
    }
    auto itemArg = node.Lambda().Args().Arg(0);
    auto itemType = itemArg.Ref().GetTypeAnn();
    if (itemType->GetKind() != ETypeAnnotationKind::Struct) {
        return node.Ptr();
    }

    auto itemStructType = itemType->Cast<TStructExprType>();
    if (itemStructType->GetSize() == 0) {
        return node.Ptr();
    }

    TSet<TStringBuf> usedFields;
    if (!HaveFieldsSubset(node.Lambda().Body().Ptr(), itemArg.Ref(), usedFields, *optCtx.ParentsMap)) {
        return node.Ptr();
    }

    TExprNode::TListType fieldNodes;
    for (auto& item : itemStructType->GetItems()) {
        if (usedFields.contains(item->GetName())) {
            fieldNodes.push_back(ctx.NewAtom(node.Pos(), item->GetName()));
        }
    }

    return Build<TCoFlatMapBase>(ctx, node.Pos())
        .CallableName(node.Ref().Content())
        .Input<TCoExtractMembers>()
            .Input(node.Input())
            .Members()
                .Add(fieldNodes)
                .Build()
            .Build()
        .Lambda()
            .Args({"item"})
            .Body<TExprApplier>()
                .Apply(node.Lambda())
                .With(0, "item")
                .Build()
            .Build()
        .Done()
        .Ptr();
}

TExprNode::TPtr RenameJoinTable(TPositionHandle pos, TExprNode::TPtr table,
    const THashMap<TString, TString>& upstreamTablesRename, TExprContext& ctx)
{
    if (auto renamed = upstreamTablesRename.FindPtr(table->Content())) {
        return ctx.NewAtom(pos, *renamed);
    }

    return table;
}

TExprNode::TPtr RenameEqualityTables(TPositionHandle pos, TExprNode::TPtr columns,
    const THashMap<TString, TString>& upstreamTablesRename, TExprContext& ctx)
{
    TExprNode::TListType newChildren(columns->ChildrenList());
    for (ui32 i = 0; i < newChildren.size(); i += 2) {
        newChildren[i] = RenameJoinTable(pos, newChildren[i], upstreamTablesRename, ctx);
    }

    auto ret = ctx.ChangeChildren(*columns, std::move(newChildren));
    return ret;
}

TExprNode::TPtr RenameJoinTree(TExprNode::TPtr joinTree, const THashMap<TString, TString>& upstreamTablesRename,
    TExprContext& ctx)
{
    TExprNode::TPtr left;
    if (joinTree->Child(1)->IsAtom()) {
        left = RenameJoinTable(joinTree->Pos(), joinTree->Child(1), upstreamTablesRename, ctx);
    }
    else {
        left = RenameJoinTree(joinTree->Child(1), upstreamTablesRename, ctx);
    }

    TExprNode::TPtr right;
    if (joinTree->Child(2)->IsAtom()) {
        right = RenameJoinTable(joinTree->Pos(), joinTree->Child(2), upstreamTablesRename, ctx);
    }
    else {
        right = RenameJoinTree(joinTree->Child(2), upstreamTablesRename, ctx);
    }

    TExprNode::TListType newChildren(joinTree->ChildrenList());
    newChildren[1] = left;
    newChildren[2] = right;
    newChildren[3] = RenameEqualityTables(joinTree->Pos(), joinTree->Child(3), upstreamTablesRename, ctx);
    newChildren[4] = RenameEqualityTables(joinTree->Pos(), joinTree->Child(4), upstreamTablesRename, ctx);

    auto ret = ctx.ChangeChildren(*joinTree, std::move(newChildren));
    return ret;
}

TExprNode::TPtr ReassembleJoinEquality(TExprNode::TPtr columns, const TStringBuf& upstreamLabel,
    const THashMap<TString, TString>& upstreamTablesRename,
    const THashMap<TString, TString>& upstreamColumnsBackRename, TExprContext& ctx)
{
    TExprNode::TListType newChildren(columns->ChildrenList());
    for (ui32 i = 0; i < columns->ChildrenSize(); i += 2) {
        if (columns->Child(i)->Content() != upstreamLabel) {
            continue;
        }

        auto column = columns->Child(i + 1);
        if (auto originalColumn = upstreamColumnsBackRename.FindPtr(column->Content())) {
            TStringBuf part1;
            TStringBuf part2;
            SplitTableName(*originalColumn, part1, part2);
            newChildren[i] = RenameJoinTable(columns->Pos(), ctx.NewAtom(columns->Pos(), part1),
                upstreamTablesRename, ctx);
            newChildren[i + 1] = ctx.NewAtom(columns->Pos(), part2);
        } else {
            TStringBuf part1;
            TStringBuf part2;
            SplitTableName(column->Content(), part1, part2);
            newChildren[i] = RenameJoinTable(columns->Pos(), ctx.NewAtom(columns->Pos(), part1),
                upstreamTablesRename, ctx);
            newChildren[i + 1] = ctx.NewAtom(columns->Pos(), part2);

            return nullptr;
        }
    }

    auto ret = ctx.ChangeChildren(*columns, std::move(newChildren));
    return ret;
}

TExprNode::TPtr FuseJoinTree(TExprNode::TPtr downstreamJoinTree, TExprNode::TPtr upstreamJoinTree, const TStringBuf& upstreamLabel,
    const THashMap<TString, TString>& upstreamTablesRename, const THashMap<TString, TString>& upstreamColumnsBackRename,
    TExprContext& ctx)
{
    TExprNode::TPtr left;
    if (downstreamJoinTree->Child(1)->IsAtom()) {
        if (downstreamJoinTree->Child(1)->Content() != upstreamLabel) {
            left = downstreamJoinTree->Child(1);
        }
        else {
            left = RenameJoinTree(upstreamJoinTree, upstreamTablesRename, ctx);
        }
    }
    else {
        left = FuseJoinTree(downstreamJoinTree->Child(1), upstreamJoinTree, upstreamLabel, upstreamTablesRename,
            upstreamColumnsBackRename, ctx);
        if (!left) {
            return nullptr;
        }
    }

    TExprNode::TPtr right;
    if (downstreamJoinTree->Child(2)->IsAtom()) {
        if (downstreamJoinTree->Child(2)->Content() != upstreamLabel) {
            right = downstreamJoinTree->Child(2);
        }
        else {
            right = RenameJoinTree(upstreamJoinTree, upstreamTablesRename, ctx);
        }
    } else {
        right = FuseJoinTree(downstreamJoinTree->Child(2), upstreamJoinTree, upstreamLabel, upstreamTablesRename,
            upstreamColumnsBackRename, ctx);
        if (!right) {
            return nullptr;
        }
    }

    TExprNode::TListType newChildren(downstreamJoinTree->ChildrenList());
    newChildren[1] = left;
    newChildren[2] = right;
    newChildren[3] = ReassembleJoinEquality(downstreamJoinTree->Child(3), upstreamLabel, upstreamTablesRename,
        upstreamColumnsBackRename, ctx);
    newChildren[4] = ReassembleJoinEquality(downstreamJoinTree->Child(4), upstreamLabel, upstreamTablesRename,
        upstreamColumnsBackRename, ctx);
    if (!newChildren[3] || !newChildren[4]) {
        return nullptr;
    }

    auto ret = ctx.ChangeChildren(*downstreamJoinTree, std::move(newChildren));
    return ret;
}

TExprNode::TPtr FuseEquiJoins(const TExprNode::TPtr& node, ui32 upstreamIndex, TExprContext& ctx) {
    ui32 downstreamInputs = node->ChildrenSize() - 2;
    auto upstreamList = node->Child(upstreamIndex)->Child(0);
    auto upstreamLabel = node->Child(upstreamIndex)->Child(1);
    THashSet<TStringBuf> downstreamLabels;
    for (ui32 i = 0; i < downstreamInputs; ++i) {
        auto label = node->Child(i)->Child(1);
        if (!label->IsAtom()) {
            return node;
        }

        downstreamLabels.insert(label->Content());
    }

    THashMap<TString, TString> upstreamTablesRename; // rename of conflicted upstream tables
    THashMap<TString, TString> upstreamColumnsBackRename; // renamed of columns under upstreamLabel to full name inside upstream
    TMap<TString, TVector<TString>> upstreamColumnsRename;
    ui32 upstreamInputs = upstreamList->ChildrenSize() - 2;
    THashSet<TStringBuf> upstreamLabels;
    for (ui32 i = 0; i < upstreamInputs; ++i) {
        auto label = upstreamList->Child(i)->Child(1);
        if (!label->IsAtom()) {
            return node;
        }

        upstreamLabels.insert(label->Content());
    }

    for (ui32 i = 0; i < upstreamInputs; ++i) {
        auto label = upstreamList->Child(i)->Child(1);
        if (!label->IsAtom()) {
            return node;
        }

        if (downstreamLabels.contains(label->Content())) {
            // fix conflict for labels
            for (ui32 suffix = 1;; ++suffix) {
                auto newName = TString::Join(label->Content(), "_", ToString(suffix));
                if (!downstreamLabels.contains(newName) && !upstreamLabels.contains(newName)) {
                    upstreamTablesRename.insert({ TString(label->Content()) , newName });
                    break;
                }
            }
        }
    }

    TExprNode::TListType equiJoinChildren;
    for (ui32 i = 0; i < downstreamInputs; ++i) {
        if (i != upstreamIndex) {
            equiJoinChildren.push_back(node->Child(i));
        } else {
            // insert the whole upstream inputs
            for (ui32 j = 0; j < upstreamInputs; ++j) {
                auto renamed = upstreamTablesRename.FindPtr(upstreamList->Child(j)->Child(1)->Content());
                if (!renamed) {
                    equiJoinChildren.push_back(upstreamList->Child(j));
                } else {
                    auto pair = ctx.ChangeChild(*upstreamList->Child(j), 1, ctx.NewAtom(node->Pos(), *renamed));
                    equiJoinChildren.push_back(pair);
                }
            }
        }
    }

    auto downstreamJoinTree = node->Child(downstreamInputs);
    auto downstreamSettings = node->Children().back();
    auto upstreamJoinTree = upstreamList->Child(upstreamInputs);
    TExprNode::TListType settingsChildren;

    for (auto& setting : upstreamList->Children().back()->Children()) {
        if (setting->Child(0)->Content() != "rename") {
            // unsupported option to fuse
            return node;
        }

        if (setting->Child(2)->Content().empty()) {
            auto drop = setting->Child(1)->Content();
            TStringBuf part1;
            TStringBuf part2;
            SplitTableName(drop, part1, part2);
            if (auto renamed = upstreamTablesRename.FindPtr(part1)) {
                part1 = *renamed;
            }

            auto newSetting = ctx.ChangeChild(*setting, 1,
                ctx.NewAtom(node->Pos(), TString::Join(part1, ".", part2)));
            settingsChildren.push_back(newSetting);
            continue;
        }

        upstreamColumnsBackRename[TString(setting->Child(2)->Content())] = TString(setting->Child(1)->Content());
        upstreamColumnsRename[TString(setting->Child(1)->Content())].push_back(TString(setting->Child(2)->Content()));
    }

    // fill remaining upstream columns
    for (const auto& item : upstreamList->GetTypeAnn()->Cast<TListExprType>()
        ->GetItemType()->Cast<TStructExprType>()->GetItems()) {
        auto columnName = TString(item->GetName());
        if (upstreamColumnsBackRename.count(columnName)) {
            continue;
        }

        upstreamColumnsRename[columnName].push_back(columnName);
        upstreamColumnsBackRename[columnName] = columnName;
    }

    for (auto& setting : downstreamSettings->Children()) {
        if (setting->Child(0)->Content() != "rename") {
            // unsupported option to fuse
            return node;
        }

        TStringBuf part1;
        TStringBuf part2;
        SplitTableName(setting->Child(1)->Content(), part1, part2);
        if (part1 != upstreamLabel->Content()) {
            settingsChildren.push_back(setting);
            continue;
        }

        if (auto originalName = upstreamColumnsBackRename.FindPtr(part2)) {
            SplitTableName(*originalName, part1, part2);
            if (auto renamed = upstreamTablesRename.FindPtr(part1)) {
                part1 = *renamed;
            }

            upstreamColumnsRename.erase(*originalName);
            auto newSetting = ctx.ChangeChild(*setting, 1, ctx.NewAtom(node->Pos(), TString::Join(part1, '.', part2)));
            settingsChildren.push_back(newSetting);
        } else {
            return node;
        }
    }

    for (auto& x : upstreamColumnsRename) {
        for (auto& y : x.second) {
            TStringBuf part1;
            TStringBuf part2;
            SplitTableName(x.first, part1, part2);
            if (auto renamed = upstreamTablesRename.FindPtr(part1)) {
                part1 = *renamed;
            }

            settingsChildren.push_back(ctx.Builder(node->Pos())
                .List()
                .Atom(0, "rename")
                .Atom(1, TString::Join(part1, ".", part2))
                .Atom(2, TString::Join(upstreamLabel->Content(), ".", y))
                .Seal()
                .Build());
        }
    }

    auto joinTree = FuseJoinTree(downstreamJoinTree, upstreamJoinTree, upstreamLabel->Content(),
        upstreamTablesRename, upstreamColumnsBackRename, ctx);
    if (!joinTree) {
        return node;
    }

    auto newSettings = ctx.NewList(node->Pos(), std::move(settingsChildren));

    equiJoinChildren.push_back(joinTree);
    equiJoinChildren.push_back(newSettings);
    auto ret = ctx.NewCallable(node->Pos(), "EquiJoin", std::move(equiJoinChildren));
    return ret;
}

bool IsRenamingOrPassthroughFlatMap(const TCoFlatMapBase& flatMap, THashMap<TStringBuf, TStringBuf>& renames,
    THashSet<TStringBuf>& outputMembers, bool& isIdentity)
{
    renames.clear();
    outputMembers.clear();
    isIdentity = false;

    auto body = flatMap.Lambda().Body();
    auto arg = flatMap.Lambda().Args().Arg(0);

    if (!IsJustOrSingleAsList(body.Ref())) {
        return false;
    }

    TExprBase outItem(body.Ref().ChildPtr(0));
    if (outItem.Raw() == arg.Raw()) {
        isIdentity = true;
        return true;
    }

    if (auto maybeStruct = outItem.Maybe<TCoAsStruct>()) {
        for (auto child : maybeStruct.Cast()) {
            auto tuple = child.Cast<TCoNameValueTuple>();
            auto value = tuple.Value();
            YQL_ENSURE(outputMembers.insert(tuple.Name().Value()).second);

            if (auto maybeMember = value.Maybe<TCoMember>()) {
                auto member = maybeMember.Cast();
                if (member.Struct().Raw() == arg.Raw()) {
                    TStringBuf oldName = member.Name().Value();
                    TStringBuf newName = tuple.Name().Value();

                    YQL_ENSURE(renames.insert({newName, oldName}).second);
                }
            }
        }
        return true;
    }

    return false;
}

bool IsInputSuitableForPullingOverEquiJoin(const TCoEquiJoinInput& input,
    const THashMap<TStringBuf, THashSet<TStringBuf>>& joinKeysByLabel,
    THashMap<TStringBuf, TStringBuf>& renames, TOptimizeContext& optCtx)
{
    renames.clear();
    YQL_ENSURE(input.Scope().Ref().IsAtom());

    auto maybeFlatMap = TMaybeNode<TCoFlatMapBase>(input.List().Ptr());
    if (!maybeFlatMap) {
        return false;
    }

    auto flatMap = maybeFlatMap.Cast();
    if (flatMap.Lambda().Args().Arg(0).Ref().IsUsedInDependsOn()) {
        return false;
    }

    if (!SilentGetSequenceItemType(flatMap.Input().Ref(), false)) {
        return false;
    }

    if (!optCtx.IsSingleUsage(input) || !optCtx.IsSingleUsage(flatMap)) {
        return false;
    }

    bool isIdentity = false;
    THashSet<TStringBuf> outputMembers;
    if (!IsRenamingOrPassthroughFlatMap(flatMap, renames, outputMembers, isIdentity)) {
        return false;
    }

    if (isIdentity) {
        // all fields are passthrough
        YQL_ENSURE(renames.empty());
        // do not bother pulling identity FlatMap
        return false;
    }

    if (IsTablePropsDependent(flatMap.Lambda().Body().Ref())) {
        renames.clear();
        return false;
    }

    auto keysIt = joinKeysByLabel.find(input.Scope().Ref().Content());
    const auto& joinKeys = (keysIt == joinKeysByLabel.end()) ? THashSet<TStringBuf>() : keysIt->second;

    size_t joinKeysFound = 0;
    bool hasRename = false;
    for (auto it = renames.begin(); it != renames.end();) {
        auto inputName = it->second;
        auto outputName = it->first;
        if (inputName != outputName) {
            hasRename = true;
        }
        YQL_ENSURE(outputMembers.erase(outputName) == 1);
        if (joinKeys.contains(outputName)) {
            joinKeysFound++;
            if (inputName != outputName) {
                it++;
                continue;
            }
        }
        renames.erase(it++);
    }

    if (joinKeysFound != joinKeys.size()) {
        // FlatMap is not renaming/passthrough for some join keys
        renames.clear();
        return false;
    }

    if (!hasRename && outputMembers.empty()) {
        // FlatMap _only_ passes through some subset of input columns
        // do not bother pulling such Flatmap - it will be optimized away later
        renames.clear();
        return false;
    }

    return true;
}

TExprNode::TPtr ApplyRenames(const TExprNode::TPtr& input, const TMap<TStringBuf, TVector<TStringBuf>>& renames,
    const TStructExprType& noRenamesResultType, TStringBuf canaryBaseName, TExprContext& ctx)
{
    TExprNode::TListType asStructArgs;
    for (auto& item : noRenamesResultType.GetItems()) {
        auto memberName = item->GetName();

        TStringBuf tableName;
        TStringBuf columnName;
        SplitTableName(memberName, tableName, columnName);

        if (columnName.find(canaryBaseName, 0) == 0) {
            continue;
        }

        auto it = renames.find(memberName);
        TVector<TStringBuf> passAsIs = { memberName };
        const TVector<TStringBuf>& targets = (it == renames.end()) ? passAsIs : it->second;
        if (targets.empty()) {
            continue;
        }

        auto member = ctx.Builder(input->Pos())
            .Callable("Member")
                .Add(0, input)
                .Atom(1, memberName)
            .Seal()
            .Build();

        for (auto& to : targets) {
            asStructArgs.push_back(
                ctx.Builder(input->Pos())
                    .List()
                        .Atom(0, to)
                        .Add(1, member)
                    .Seal()
                    .Build()
            );
        }
    }

    return ctx.NewCallable(input->Pos(), "AsStruct", std::move(asStructArgs));
}

TExprNode::TPtr ApplyRenamesToJoinKeys(const TExprNode::TPtr& joinKeys,
    const THashMap<TStringBuf, THashMap<TStringBuf, TStringBuf>>& inputJoinKeyRenamesByLabel, TExprContext& ctx)
{
    YQL_ENSURE(joinKeys->ChildrenSize() % 2 == 0);

    TExprNode::TListType newKeys;
    newKeys.reserve(joinKeys->ChildrenSize());

    for (ui32 i = 0; i < joinKeys->ChildrenSize(); i += 2) {
        auto table = joinKeys->ChildPtr(i);
        auto column = joinKeys->ChildPtr(i + 1);

        YQL_ENSURE(table->IsAtom());
        YQL_ENSURE(column->IsAtom());

        auto it = inputJoinKeyRenamesByLabel.find(table->Content());
        if (it != inputJoinKeyRenamesByLabel.end()) {
            auto renameIt = it->second.find(column->Content());
            if (renameIt != it->second.end()) {
                column = ctx.NewAtom(column->Pos(), renameIt->second);
            }
        }

        newKeys.push_back(table);
        newKeys.push_back(column);
    }

    return ctx.NewList(joinKeys->Pos(), std::move(newKeys));
}


TExprNode::TPtr ApplyRenamesToJoinTree(const TExprNode::TPtr& joinTree,
    const THashMap<TStringBuf, THashMap<TStringBuf, TStringBuf>>& inputJoinKeyRenamesByLabel, TExprContext& ctx)
{
    if (joinTree->IsAtom()) {
        return joinTree;
    }

    return ctx.Builder(joinTree->Pos())
        .List()
            .Add(0, joinTree->ChildPtr(0))
            .Add(1, ApplyRenamesToJoinTree(joinTree->ChildPtr(1), inputJoinKeyRenamesByLabel, ctx))
            .Add(2, ApplyRenamesToJoinTree(joinTree->ChildPtr(2), inputJoinKeyRenamesByLabel, ctx))
            .Add(3, ApplyRenamesToJoinKeys(joinTree->ChildPtr(3), inputJoinKeyRenamesByLabel, ctx))
            .Add(4, ApplyRenamesToJoinKeys(joinTree->ChildPtr(4), inputJoinKeyRenamesByLabel, ctx))
            .Add(5, joinTree->ChildPtr(5))
        .Seal()
        .Build();
}

const TTypeAnnotationNode* GetCanaryOutputType(const TStructExprType& outputType, TStringBuf fullCanaryName) {
    auto maybeIndex = outputType.FindItem(fullCanaryName);
    if (!maybeIndex) {
        return nullptr;
    }
    return outputType.GetItems()[*maybeIndex]->GetItemType();
}

TExprNode::TPtr BuildOutputFlattenMembersArg(const TCoEquiJoinInput& input, const TExprNode::TPtr& inputArg,
    const TString& canaryName, const TStructExprType& canaryResultTypeWithoutRenames, TExprContext& ctx)
{
    YQL_ENSURE(input.Scope().Ref().IsAtom());
    TStringBuf label = input.Scope().Ref().Content();

    auto flatMap = input.List().Cast<TCoFlatMapBase>();
    auto lambda = flatMap.Lambda();
    YQL_ENSURE(IsJustOrSingleAsList(lambda.Body().Ref()));
    auto strippedLambdaBody = lambda.Body().Ref().HeadPtr();

    const TString labelPrefix = TString::Join(label, ".");
    const TString fullCanaryName = FullColumnName(label, canaryName);

    const TTypeAnnotationNode* canaryOutType = GetCanaryOutputType(canaryResultTypeWithoutRenames, fullCanaryName);
    if (!canaryOutType) {
        // canary didn't survive join
        return {};
    }

    auto flatMapInputItem = GetSequenceItemType(flatMap.Input(), false);

    auto myStruct = ctx.Builder(input.Pos())
        .Callable("DivePrefixMembers")
            .Add(0, inputArg)
            .List(1)
                .Atom(0, labelPrefix)
            .Seal()
        .Seal()
        .Build();

    if (canaryOutType->GetKind() == ETypeAnnotationKind::Data) {
        YQL_ENSURE(canaryOutType->Cast<TDataExprType>()->GetSlot() == EDataSlot::Bool);
        // our input passed as-is
        return ctx.Builder(input.Pos())
            .List()
                .Atom(0, labelPrefix)
                .ApplyPartial(1, lambda.Args().Ptr(), std::move(strippedLambdaBody))
                    .With(0, std::move(myStruct))
                .Seal()
            .Seal()
            .Build();
    }

    YQL_ENSURE(canaryOutType->GetKind() == ETypeAnnotationKind::Optional);

    TExprNode::TListType membersForCheck;
    auto flatMapInputItems = flatMapInputItem->Cast<TStructExprType>()->GetItems();

    flatMapInputItems.push_back(ctx.MakeType<TItemExprType>(canaryName, ctx.MakeType<TDataExprType>(EDataSlot::Bool)));
    for (auto& item : flatMapInputItems) {
        if (item->GetItemType()->GetKind() != ETypeAnnotationKind::Optional) {
            membersForCheck.emplace_back(ctx.NewAtom(input.Pos(), item->GetName()));
        }
    }

    return ctx.Builder(input.Pos())
        .List()
            .Atom(0, labelPrefix)
            .Callable(1, "FlattenMembers")
                .List(0)
                    .Atom(0, "")
                    .Callable(1, flatMap.CallableName())
                        .Callable(0, "FilterNullMembers")
                            .Callable(0, "AssumeAllMembersNullableAtOnce")
                                .Callable(0, "Just")
                                    .Add(0, std::move(myStruct))
                                .Seal()
                            .Seal()
                            .List(1)
                                .Add(std::move(membersForCheck))
                            .Seal()
                        .Seal()
                        .Lambda(1)
                            .Param("canaryInput")
                            .Callable("Just")
                                .ApplyPartial(0, lambda.Args().Ptr(), std::move(strippedLambdaBody))
                                    .With(0)
                                        .Callable("RemoveMember")
                                            .Arg(0, "canaryInput")
                                            .Atom(1, canaryName)
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr PullUpFlatMapOverEquiJoin(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    if (!optCtx.Types->PullUpFlatMapOverJoin) {
        return node;
    }

    YQL_ENSURE(node->ChildrenSize() >= 4);
    auto inputsCount = ui32(node->ChildrenSize() - 2);

    auto joinTree = node->ChildPtr(inputsCount);
    if (HasOnlyOneJoinType(*joinTree, "Cross")) {
        return node;
    }

    auto settings = node->ChildPtr(inputsCount + 1);
    for (auto& child : settings->Children()) {
        if (child->Head().IsAtom("flatten")) {
            return node;
        }
    }

    static const TStringBuf canaryBaseName = "_yql_canary_";

    THashMap<TStringBuf, THashSet<TStringBuf>> joinKeysByLabel = CollectEquiJoinKeyColumnsByLabel(*joinTree);
    const auto renames = LoadJoinRenameMap(*settings);

    TVector<ui32> toPull;
    TJoinLabels canaryLabels;
    TJoinLabels actualLabels;
    THashMap<TStringBuf, THashMap<TStringBuf, TStringBuf>> inputJoinKeyRenamesByLabel;
    for (ui32 i = 0; i < inputsCount; ++i) {
        TCoEquiJoinInput input(node->ChildPtr(i));

        if (!input.Scope().Ref().IsAtom()) {
            return node;
        }

        const TTypeAnnotationNode* itemType = input.List().Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        auto structType = itemType->Cast<TStructExprType>();
        for (auto& si : structType->GetItems()) {
            if (si->GetName().find(canaryBaseName, 0) == 0) {
                // EquiJoin already processed
                return node;
            }
        }

        auto err = actualLabels.Add(ctx, *input.Scope().Ptr(), structType);
        YQL_ENSURE(!err);

        auto label = input.Scope().Ref().Content();


        if (IsInputSuitableForPullingOverEquiJoin(input, joinKeysByLabel, inputJoinKeyRenamesByLabel[label], optCtx)) {
            auto flatMap = input.List().Cast<TCoFlatMapBase>();

            auto flatMapInputItem = GetSequenceItemType(flatMap.Input(), false);
            auto structItems = flatMapInputItem->Cast<TStructExprType>()->GetItems();

            TString canaryName = TStringBuilder() << canaryBaseName << i;
            structItems.push_back(ctx.MakeType<TItemExprType>(canaryName, ctx.MakeType<TDataExprType>(EDataSlot::Bool)));
            structType = ctx.MakeType<TStructExprType>(structItems);

            YQL_CLOG(DEBUG, Core) << "Will pull up EquiJoin input #" << i;
            toPull.push_back(i);
        }

        err = canaryLabels.Add(ctx, *input.Scope().Ptr(), structType);
        YQL_ENSURE(!err);
    }

    if (toPull.empty()) {
        return node;
    }

    const TStructExprType* canaryResultType = nullptr;
    const TStructExprType* noRenamesResultType = nullptr;
    const auto settingsWithoutRenames = RemoveSetting(*settings, "rename", ctx);
    const auto joinTreeWithInputRenames = ApplyRenamesToJoinTree(joinTree, inputJoinKeyRenamesByLabel, ctx);


    {
        TJoinOptions options;
        auto status = ValidateEquiJoinOptions(node->Pos(), *settingsWithoutRenames, options, ctx);
        YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);

        status = EquiJoinAnnotation(node->Pos(), canaryResultType, canaryLabels,
                                         *joinTreeWithInputRenames, options, ctx);
        YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);

        status = EquiJoinAnnotation(node->Pos(), noRenamesResultType, actualLabels,
                                    *joinTree, options, ctx);
        YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);
    }



    TExprNode::TListType newEquiJoinArgs;
    newEquiJoinArgs.reserve(node->ChildrenSize());

    TExprNode::TListType flattenMembersArgs;

    auto afterJoinArg = ctx.NewArgument(node->Pos(), "joinOut");

    for (ui32 i = 0, j = 0; i < inputsCount; ++i) {
        TCoEquiJoinInput input(node->ChildPtr(i));

        TStringBuf label = input.Scope().Ref().Content();
        TString labelPrefix = TString::Join(label, ".");

        if (j < toPull.size() && i == toPull[j]) {
            j++;


            const TString canaryName = TStringBuilder() << canaryBaseName << i;
            const TString fullCanaryName = FullColumnName(label, canaryName);

            TCoFlatMapBase flatMap = input.List().Cast<TCoFlatMapBase>();

            const TTypeAnnotationNode* canaryOutType = GetCanaryOutputType(*canaryResultType, fullCanaryName);
            if (canaryOutType && canaryOutType->GetKind() == ETypeAnnotationKind::Optional) {
                // remove leading flatmap from input and launch canary
                newEquiJoinArgs.push_back(
                    ctx.Builder(input.Pos())
                        .List()
                            .Callable(0, flatMap.CallableName())
                                .Add(0, flatMap.Input().Ptr())
                                .Lambda(1)
                                    .Param("item")
                                    .Callable("Just")
                                        .Callable(0, "AddMember")
                                            .Arg(0, "item")
                                            .Atom(1, canaryName)
                                            .Callable(2, "Bool")
                                                .Atom(0, "true")
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Add(1, input.Scope().Ptr())
                        .Seal()
                        .Build()
                );
            } else {
                // just remove leading flatmap from input
                newEquiJoinArgs.push_back(
                    ctx.Builder(input.Pos())
                        .List()
                            .Add(0, flatMap.Input().Ptr())
                            .Add(1, input.Scope().Ptr())
                        .Seal()
                        .Build()
                );
            }

            auto flattenMembersArg = BuildOutputFlattenMembersArg(input, afterJoinArg, canaryName, *canaryResultType, ctx);
            if (flattenMembersArg) {
                flattenMembersArgs.push_back(flattenMembersArg);
            }
        } else {
            flattenMembersArgs.push_back(ctx.Builder(input.Pos())
                .List()
                    .Atom(0, labelPrefix)
                    .Callable(1, "DivePrefixMembers")
                        .Add(0, afterJoinArg)
                        .List(1)
                            .Atom(0, labelPrefix)
                        .Seal()
                    .Seal()
                .Seal()
                .Build());
            newEquiJoinArgs.push_back(input.Ptr());
        }
    }

    newEquiJoinArgs.push_back(joinTreeWithInputRenames);
    newEquiJoinArgs.push_back(settingsWithoutRenames);

    auto newEquiJoin = ctx.NewCallable(node->Pos(), "EquiJoin", std::move(newEquiJoinArgs));

    auto flattenMembers = flattenMembersArgs.empty() ? afterJoinArg :
                          ctx.NewCallable(node->Pos(), "FlattenMembers", std::move(flattenMembersArgs));

    auto newLambdaBody = ctx.Builder(node->Pos())
        .Callable("Just")
            .Add(0, ApplyRenames(flattenMembers, renames, *noRenamesResultType, canaryBaseName, ctx))
        .Seal()
        .Build();

    auto newLambda = ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), { afterJoinArg }), std::move(newLambdaBody));

    return ctx.NewCallable(node->Pos(), "OrderedFlatMap", { newEquiJoin, newLambda });
}

TExprNode::TPtr OptimizeFromFlow(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    if (!optCtx.IsSingleUsage(node->Head())) {
        return node;
    }

    if (node->Head().IsCallable("ToFlow") &&
        node->Head().Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream) {
        YQL_CLOG(DEBUG, Core) << "Drop " << node->Content() << " with " << node->Head().Content();
        return node->Head().HeadPtr();
    }

    if (node->Head().IsCallable("ToFlow") &&
        node->Head().Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List) {
        YQL_CLOG(DEBUG, Core) << "Replace  " << node->Content() << " with Iterator";

        return Build<TCoIterator>(ctx, node->Pos())
            .List(node->HeadPtr()->HeadPtr())
            .Done()
            .Ptr();
    }

    return node;
}

TExprNode::TPtr OptimizeCollect(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    if (!optCtx.IsSingleUsage(node->Head())) {
        return node;
    }

    if (node->Head().IsCallable({"ToFlow", "FromFlow"}) &&
        node->Head().Head().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional) {
        YQL_CLOG(DEBUG, Core) << "Drop " << node->Head().Content() <<  " under " << node->Content();
        return ctx.ChangeChildren(*node, node->Head().ChildrenList());
    }

    return node;
}

enum ESubgraphType {
    EXPR_CONST,
    EXPR_KEYS,
    EXPR_PAYLOADS,
    EXPR_MIXED,
};

TNodeMap<ESubgraphType> MarkSubgraphForAggregate(const TExprNode::TPtr& root, const TCoArgument& row, const THashSet<TStringBuf>& keys) {
    TNodeMap<ESubgraphType> result;
    size_t insideDependsOn = 0;
    VisitExpr(root, [&](const TExprNode::TPtr& node) {
        if (node->IsComplete()) {
            result[node.Get()] = EXPR_CONST;
            return false;
        }
        if (node->IsCallable("DependsOn")) {
            ++insideDependsOn;
            return true;
        }

        if (!insideDependsOn && node->IsCallable("Member") && &node->Head() == row.Raw()) {
            result[node.Get()] = keys.contains(node->Child(1)->Content()) ? EXPR_KEYS : EXPR_PAYLOADS;
            return false;
        }

        if (node->IsArgument()) {
            result[node.Get()] = node.Get() == row.Raw() ? EXPR_MIXED : EXPR_CONST;
            return false;
        }

        return true;
    }, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("DependsOn")) {
            YQL_ENSURE(insideDependsOn);
            --insideDependsOn;
        }
        if (result.contains(node.Get())) {
            return true;
        }
        ESubgraphType derivedType = EXPR_CONST;
        for (auto& child : node->ChildrenList()) {
            auto it = result.find(child.Get());
            YQL_ENSURE(it != result.end());
            switch (it->second) {
            case EXPR_CONST:
                break;
            case EXPR_KEYS: {
                if (derivedType == EXPR_CONST) {
                    derivedType = EXPR_KEYS;
                } else if (derivedType == EXPR_PAYLOADS) {
                    derivedType = EXPR_MIXED;
                }
                break;
            }
            case EXPR_PAYLOADS: {
                if (derivedType == EXPR_CONST) {
                    derivedType = EXPR_PAYLOADS;
                } else if (derivedType == EXPR_KEYS) {
                    derivedType = EXPR_MIXED;
                }
                break;
            }
            case EXPR_MIXED:
                derivedType = EXPR_MIXED;
                break;
            }
        }
        YQL_ENSURE(result.insert({node.Get(), derivedType}).second);
        return true;
    });

    return result;
}

class ICalcualtor : public TThrRefBase {
public:
    TMaybe<bool> Calculate() const {
        if (!Cached_.Defined()) {
            Cached_ = DoCalculate();
        }
        return *Cached_;
    }

    void DropCache() {
        Cached_ = {};
        DropChildCaches();
    }

    using TPtr = TIntrusivePtr<ICalcualtor>;
protected:
    virtual TMaybe<bool> DoCalculate() const = 0;
    virtual void DropChildCaches() = 0;
private:
    mutable TMaybe<TMaybe<bool>> Cached_;
};

class TUnknownValue : public ICalcualtor {
public:
    TUnknownValue() = default;
private:
    TMaybe<bool> DoCalculate() const override {
        return {};
    }
    void DropChildCaches() override {
    }
};

class TImmediateValue : public ICalcualtor {
public:
    TImmediateValue(ui64& input, size_t index)
        : Input_(&input)
        , Index_(index)
    {
        YQL_ENSURE(index < 64);
    }
private:
    TMaybe<bool> DoCalculate() const override {
        return ((*Input_) & (ui64(1) << Index_)) != 0;
    }
    void DropChildCaches() override {
    }

    const ui64* const Input_;
    const size_t Index_;
};

class TAndValue : public ICalcualtor {
public:
    explicit TAndValue(TVector<ICalcualtor::TPtr>&& children)
        : Children_(std::move(children))
    {
        YQL_ENSURE(!Children_.empty());
    }
private:
    TMaybe<bool> DoCalculate() const override {
        bool allTrue = true;
        for (auto& child : Children_) {
            YQL_ENSURE(child);
            auto val = child->Calculate();
            if (!val.Defined()) {
                allTrue = false;
            } else if (!*val) {
                return false;
            }
        }
        if (allTrue) {
            return true;
        }
        return {};
    }
    void DropChildCaches() override {
        for (auto& child : Children_) {
            child->DropCache();
        }
    }

    const TVector<ICalcualtor::TPtr> Children_;
};

class TOrValue : public ICalcualtor {
public:
    explicit TOrValue(TVector<ICalcualtor::TPtr>&& children)
        : Children_(std::move(children))
    {
        YQL_ENSURE(!Children_.empty());
    }
private:
    TMaybe<bool> DoCalculate() const override {
        bool allFalse = true;
        for (auto& child : Children_) {
            YQL_ENSURE(child);
            auto val = child->Calculate();
            if (!val.Defined()) {
                allFalse = false;
            } else if (*val) {
                return true;
            }
        }
        if (allFalse) {
            return false;
        }
        return {};
    }

    void DropChildCaches() override {
        for (auto& child : Children_) {
            child->DropCache();
        }
    }

    const TVector<ICalcualtor::TPtr> Children_;
};

class TNotValue : public ICalcualtor {
public:
    explicit TNotValue(ICalcualtor::TPtr child)
        : Child_(std::move(child))
    {
        YQL_ENSURE(Child_);
    }
private:
    TMaybe<bool> DoCalculate() const override {
        auto val = Child_->Calculate();
        if (!val.Defined()) {
            return val;
        }
        return !*val;
    }

    void DropChildCaches() override {
        Child_->DropCache();
    }
    const ICalcualtor::TPtr Child_;
};

ICalcualtor::TPtr BuildProgram(const TExprNode::TPtr& node, const TNodeMap<ESubgraphType>& markedGraph,
    TNodeMap<ICalcualtor::TPtr>& calcCache, TExprNodeList& keyPredicates, ui64& inputs)
{
    auto cached = calcCache.find(node.Get());
    if (cached != calcCache.end()) {
        return cached->second;
    }

    if (node->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data || node->GetTypeAnn()->Cast<TDataExprType>()->GetSlot() != EDataSlot::Bool) {
        return nullptr;
    }

    auto it = markedGraph.find(node.Get());
    YQL_ENSURE(it != markedGraph.end());
    ESubgraphType type = it->second;

    ICalcualtor::TPtr result;
    if (type == EXPR_CONST || type == EXPR_PAYLOADS) {
        result = new TUnknownValue();
    } else if (type == EXPR_KEYS) {
        size_t index = keyPredicates.size();
        if (index >= 64) {
            return nullptr;
        }
        result = new TImmediateValue(inputs, index);
        keyPredicates.push_back(node);
    } else if (node->IsCallable({"And", "Or", "Not"})) {
        YQL_ENSURE(type == EXPR_MIXED);
        YQL_ENSURE(node->ChildrenSize());
        TVector<ICalcualtor::TPtr> childCalcs;
        childCalcs.reserve(node->ChildrenSize());
        for (auto& childNode : node->ChildrenList()) {
            childCalcs.emplace_back(BuildProgram(childNode, markedGraph, calcCache, keyPredicates, inputs));
            if (!childCalcs.back()) {
                return nullptr;
            }
        }
        if (node->IsCallable("And")) {
            result = new TAndValue(std::move(childCalcs));
        } else if (node->IsCallable("Or")) {
            result = new TOrValue(std::move(childCalcs));
        } else {
            result = new TNotValue(childCalcs.front());
        }
    }

    if (result) {
        calcCache[node.Get()] = result;
    }
    return result;
}

TExprBase FilterOverAggregate(const TCoFlatMapBase& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    YQL_ENSURE(optCtx.ParentsMap);
    if (!TCoConditionalValueBase::Match(node.Lambda().Body().Raw())) {
        return node;
    }

    const TCoArgument arg = node.Lambda().Args().Arg(0);
    TCoConditionalValueBase body = node.Lambda().Body().Cast<TCoConditionalValueBase>();

    const TCoAggregate agg = node.Input().Cast<TCoAggregate>();
    THashSet<TStringBuf> keyColumns;
    for (auto key : agg.Keys()) {
        keyColumns.insert(key.Value());
    }

    TExprNodeList andComponents;
    if (auto maybeAnd = body.Predicate().Maybe<TCoAnd>()) {
        andComponents = maybeAnd.Cast().Ref().ChildrenList();
    } else {
        andComponents.push_back(body.Predicate().Ptr());
    }

    TExprNodeList pushComponents;
    TExprNodeList restComponents;
    size_t separableComponents = 0;
    for (auto& p : andComponents) {
        TSet<TStringBuf> usedFields;
        if (p->IsCallable("Likely") ||
            HasDependsOn(p, arg.Ptr()) ||
            !HaveFieldsSubset(p, arg.Ref(), usedFields, *optCtx.ParentsMap) ||
            !AllOf(usedFields, [&](TStringBuf field) { return keyColumns.contains(field); }) ||
            !p->IsComplete() && !IsStrict(p))
        {
            restComponents.push_back(p);
        } else {
            pushComponents.push_back(p);
            ++separableComponents;
        }
    }

    size_t nonSeparableComponents = 0;
    size_t maxKeyPredicates = 0;
    if (AllowComplexFiltersOverAggregatePushdown(optCtx)) {
        for (auto& p : restComponents) {
            if (p->IsCallable("Likely")) {
                continue;
            }
            const TNodeMap<ESubgraphType> marked = MarkSubgraphForAggregate(p, arg, keyColumns);
            auto rootIt = marked.find(p.Get());
            YQL_ENSURE(rootIt != marked.end());
            YQL_ENSURE(rootIt->second == EXPR_MIXED, "Key-only or const predicates should be handled earlier");

            TNodeMap<ICalcualtor::TPtr> calcCache;
            TExprNodeList keyPredicates;
            ui64 inputs = 0;

            auto calculator = BuildProgram(p, marked, calcCache, keyPredicates, inputs);
            if (!calculator || keyPredicates.empty() || keyPredicates.size() > 6) {
                continue;
            }

            ui64 maxInputs = ui64(1) << keyPredicates.size();
            maxKeyPredicates = std::max(maxKeyPredicates, keyPredicates.size());
            bool canPush = false;
            for (inputs = 0; inputs < maxInputs; ++inputs) {
                // the goal is to find all keyPredicate values for which p yields False value irrespective of all constants and payloads
                auto pResult = calculator->Calculate();
                if (pResult.Defined() && !*pResult) {
                    canPush = true;
                    TExprNodeList orItems;
                    for (size_t i = 0; i < keyPredicates.size(); ++i) {
                        // not (P1 == X and P2 == Y) ->   (P1 != X or P2 != Y)
                        // P1 != X: (X is true -> not P1, X is false -> P1)
                        bool value = (inputs & (ui64(1) << i)) != 0;
                        orItems.emplace_back(ctx.WrapByCallableIf(value, "Not", TExprNode::TPtr(keyPredicates[i])));
                    }
                    pushComponents.push_back(ctx.NewCallable(p->Pos(), "Or", std::move(orItems)));
                }
                calculator->DropCache();
            }
            nonSeparableComponents += canPush;
            p = ctx.WrapByCallableIf(canPush, "Likely", std::move(p));
        }
    }

    if (pushComponents.empty()) {
        return node;
    }

    YQL_CLOG(DEBUG, Core) << "Filter over Aggregate : " << separableComponents << " separable, "
                          << nonSeparableComponents << " non-separable predicates out of " << andComponents.size()
                          << ". Pushed " << pushComponents.size() << " components. Maximum analyzed key predicates " << maxKeyPredicates;

    TExprNode::TPtr pushPred = ctx.NewCallable(body.Predicate().Pos(), "And", std::move(pushComponents));
    TExprNode::TPtr restPred = restComponents.empty() ?
        MakeBool<true>(body.Predicate().Pos(), ctx) :
        ctx.NewCallable(body.Predicate().Pos(), "And", std::move(restComponents));

    auto pushBody = ctx.NewCallable(body.Pos(), "OptionalIf", { pushPred, arg.Ptr() });
    auto pushLambda = ctx.DeepCopyLambda(*ctx.ChangeChild(node.Lambda().Ref(), TCoLambda::idx_Body, std::move(pushBody)));

    auto restBody = ctx.ChangeChild(body.Ref(), TCoConditionalValueBase::idx_Predicate, std::move(restPred));
    auto restLambda = ctx.DeepCopyLambda(*ctx.ChangeChild(node.Lambda().Ref(), TCoLambda::idx_Body, std::move(restBody)));

    auto newAggInput = ctx.NewCallable(agg.Input().Pos(), "FlatMap", { agg.Input().Ptr(), pushLambda });
    auto newAgg = ctx.ChangeChild(agg.Ref(), TCoAggregate::idx_Input, std::move(newAggInput));
    return TExprBase(ctx.NewCallable(node.Pos(), node.Ref().Content(), { newAgg, restLambda }));
}

} // namespace

void RegisterCoFlowCallables2(TCallableOptimizerMap& map) {
    using namespace std::placeholders;

    map["FromFlow"] = std::bind(&OptimizeFromFlow, _1, _2, _3);
    map["Collect"] = std::bind(&OptimizeCollect, _1, _2, _3);

    map["FlatMap"] = map["OrderedFlatMap"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) -> TExprNode::TPtr {
        TCoFlatMapBase self(node);
        if (optCtx.IsSingleUsage(self.Input().Ref())) {
            if (self.Input().Ref().IsCallable("EquiJoin")) {
                auto ret = FlatMapOverEquiJoin(self, ctx, *optCtx.ParentsMap, false, optCtx.Types);
                if (!ret.Raw()) {
                    return nullptr;
                }

                if (ret.Raw() != self.Raw()) {
                    YQL_CLOG(DEBUG, Core) << node->Content() << "OverEquiJoin";
                    return ret.Ptr();
                }
            }

            if (self.Input().Ref().IsCallable("Aggregate")) {
                auto ret = FilterOverAggregate(self, ctx, optCtx);
                if (!ret.Raw()) {
                    return nullptr;
                }

                if (ret.Raw() != self.Raw()) {
                    return ret.Ptr();
                }
            }

            if (self.Input().Ref().IsCallable(TCoGroupingCore::CallableName())) {
                auto groupingCore = self.Input().Cast<TCoGroupingCore>();
                const TExprNode* extract = nullptr;
                // Find pattern: (FlatMap (GroupingCore ...) (lambda (x) ( ... (ExtractMembers (Nth x '1) ...))))
                const auto arg = self.Lambda().Args().Arg(0).Raw();
                if (const auto parents = optCtx.ParentsMap->find(arg); parents != optCtx.ParentsMap->cend()) {
                    for (const auto& parent : parents->second) {
                        if (parent->IsCallable(TCoNth::CallableName()) && &parent->Head() == arg && parent->Tail().Content() == "1") {
                            if (const auto nthParents = optCtx.ParentsMap->find(parent); nthParents != optCtx.ParentsMap->cend()) {
                                if (nthParents->second.size() == 1 && (*nthParents->second.begin())->IsCallable(TCoExtractMembers::CallableName())) {
                                    extract = *nthParents->second.begin();
                                    break;
                                }
                            }
                        }
                    }
                }
                if (extract) {
                    if (const auto handler = groupingCore.ConvertHandler()) {
                        auto newBody = Build<TCoCastStruct>(ctx, handler.Cast().Body().Pos())
                            .Struct(handler.Cast().Body())
                            .Type(ExpandType(handler.Cast().Body().Pos(), GetSeqItemType(*extract->GetTypeAnn()), ctx))
                            .Done();

                        groupingCore = Build<TCoGroupingCore>(ctx, groupingCore.Pos())
                            .InitFrom(groupingCore)
                            .ConvertHandler()
                                .Args({"item"})
                                .Body<TExprApplier>()
                                    .Apply(newBody)
                                    .With(handler.Cast().Args().Arg(0), "item")
                                .Build()
                            .Build()
                            .Done();

                        YQL_CLOG(DEBUG, Core) << "Pull out " << extract->Content() << " from " << node->Content() << " to " << groupingCore.Ref().Content() << " handler";
                        return Build<TCoFlatMapBase>(ctx, node->Pos())
                            .CallableName(node->Content())
                            .Input(groupingCore)
                            .Lambda(ctx.DeepCopyLambda(self.Lambda().Ref()))
                            .Done().Ptr();
                    }

                    std::map<std::string_view, TExprNode::TPtr> usedFields;
                    auto fields = extract->Tail().ChildrenList();
                    std::for_each(fields.cbegin(), fields.cend(), [&](const TExprNode::TPtr& field) { usedFields.emplace(field->Content(), field); });

                    if (HaveFieldsSubset(groupingCore.KeyExtractor().Body().Ptr(), groupingCore.KeyExtractor().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
                        && !usedFields.empty()
                        && HaveFieldsSubset(groupingCore.GroupSwitch().Body().Ptr(), groupingCore.GroupSwitch().Args().Arg(1).Ref(), usedFields, *optCtx.ParentsMap, false)
                        && !usedFields.empty()
                        && (GetSeqItemType(*groupingCore.Input().Ref().GetTypeAnn()).GetKind() == ETypeAnnotationKind::Struct)
                        && usedFields.size() < GetSeqItemType(*groupingCore.Input().Ref().GetTypeAnn()).Cast<TStructExprType>()->GetSize()) {
                        if (usedFields.size() != fields.size()) {
                            fields.reserve(usedFields.size());
                            fields.clear();
                            std::transform(usedFields.begin(), usedFields.end(), std::back_inserter(fields),
                                [](std::pair<const std::string_view, TExprNode::TPtr>& item){ return std::move(item.second); });
                        }

                        YQL_CLOG(DEBUG, Core) << "Pull out " << extract->Content() << " from " << node->Content() << " to " << groupingCore.Ref().Content() << " input";
                        return Build<TCoFlatMapBase>(ctx, node->Pos())
                            .CallableName(node->Content())
                            .Input<TCoGroupingCore>()
                                .Input<TCoExtractMembers>()
                                    .Input(groupingCore.Input())
                                    .Members()
                                        .Add(std::move(fields))
                                    .Build()
                                .Build()
                                .GroupSwitch(ctx.DeepCopyLambda(groupingCore.GroupSwitch().Ref()))
                                .KeyExtractor(ctx.DeepCopyLambda(groupingCore.KeyExtractor().Ref()))
                            .Build()
                            .Lambda(ctx.DeepCopyLambda(self.Lambda().Ref()))
                            .Done().Ptr();
                    }
                }
            }

            if (self.Input().Ref().IsCallable("Take") || self.Input().Ref().IsCallable("Skip")
                || self.Input().Maybe<TCoExtendBase>()) {

                auto& arg = self.Lambda().Args().Arg(0).Ref();
                auto body = self.Lambda().Body().Ptr();
                TSet<TStringBuf> usedFields;
                if (HaveFieldsSubset(body, arg, usedFields, *optCtx.ParentsMap)) {
                    YQL_CLOG(DEBUG, Core) << "FieldsSubset in " << node->Content() << " over " << self.Input().Ref().Content();

                    TExprNode::TListType filteredInputs;
                    filteredInputs.reserve(self.Input().Ref().ChildrenSize());
                    for (ui32 index = 0; index < self.Input().Ref().ChildrenSize(); ++index) {
                        auto x = self.Input().Ref().ChildPtr(index);
                        if (!self.Input().Maybe<TCoExtendBase>() && index > 0) {
                            filteredInputs.push_back(x);
                            continue;
                        }

                        filteredInputs.push_back(FilterByFields(node->Pos(), x, usedFields, ctx, false));
                    }

                    auto newInput = ctx.ChangeChildren(self.Input().Ref(), std::move(filteredInputs));
                    return ctx.Builder(node->Pos())
                        .Callable(node->Content())
                            .Add(0, newInput)
                            .Lambda(1)
                                .Param("item")
                                .Apply(self.Lambda().Ptr()).With(0, "item").Seal()
                            .Seal()
                        .Seal()
                        .Build();
                }
            }
        }

        auto ret = FlatMapSubsetFields(self, ctx, optCtx);
        if (ret != node) {
            YQL_CLOG(DEBUG, Core) << node->Content() << "SubsetFields";
            return ret;
        }

        return node;
    };

    map[TCoGroupingCore::CallableName()] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        TCoGroupingCore self(node);
        if (!AllowSubsetFieldsForNode(self.Input().Ref(), optCtx)) {
            return node;
        }

        if (!self.ConvertHandler()) {
            return node;
        }

        std::map<std::string_view, TExprNode::TPtr> usedFields;
        if (HaveFieldsSubset(self.ConvertHandler().Cast().Body().Ptr(), self.ConvertHandler().Cast().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && HaveFieldsSubset(self.KeyExtractor().Body().Ptr(), self.KeyExtractor().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && HaveFieldsSubset(self.GroupSwitch().Body().Ptr(), self.GroupSwitch().Args().Arg(1).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && (GetSeqItemType(*self.Input().Ref().GetTypeAnn()).GetKind() == ETypeAnnotationKind::Struct)
            && usedFields.size() < GetSeqItemType(*self.Input().Ref().GetTypeAnn()).Cast<TStructExprType>()->GetSize())
        {
            TExprNode::TListType fields;
            fields.reserve(usedFields.size());
            std::transform(usedFields.begin(), usedFields.end(), std::back_inserter(fields),
                [](std::pair<const std::string_view, TExprNode::TPtr>& item){ return std::move(item.second); });

            YQL_CLOG(DEBUG, Core) << node->Content() << "SubsetFields";
            return Build<TCoGroupingCore>(ctx, node->Pos())
                .Input<TCoExtractMembers>()
                    .Input(self.Input())
                    .Members()
                        .Add(std::move(fields))
                    .Build()
                .Build()
                .GroupSwitch(ctx.DeepCopyLambda(self.GroupSwitch().Ref()))
                .KeyExtractor(ctx.DeepCopyLambda(self.KeyExtractor().Ref()))
                .ConvertHandler(ctx.DeepCopyLambda(self.ConvertHandler().Ref()))
                .Done().Ptr();
        }
        return node;
    };

    map["CombineByKey"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        TCoCombineByKey self(node);
        if (!AllowSubsetFieldsForNode(self.Input().Ref(), optCtx)) {
            return node;
        }

        auto itemArg = self.PreMapLambda().Args().Arg(0);
        auto itemType = itemArg.Ref().GetTypeAnn();
        if (itemType->GetKind() != ETypeAnnotationKind::Struct) {
            return node;
        }

        auto itemStructType = itemType->Cast<TStructExprType>();
        if (itemStructType->GetSize() == 0) {
            return node;
        }

        TSet<TStringBuf> usedFields;
        if (!HaveFieldsSubset(self.PreMapLambda().Body().Ptr(), itemArg.Ref(), usedFields, *optCtx.ParentsMap)) {
            return node;
        }

        TExprNode::TPtr newInput;
        if (self.Input().Ref().IsCallable("Take") || self.Input().Ref().IsCallable("Skip") || self.Input().Maybe<TCoExtendBase>()) {
            TExprNode::TListType filteredInputs;
            filteredInputs.reserve(self.Input().Ref().ChildrenSize());
            for (ui32 index = 0; index < self.Input().Ref().ChildrenSize(); ++index) {
                auto x = self.Input().Ref().ChildPtr(index);
                if (!self.Input().Maybe<TCoExtendBase>() && index > 0) {
                    filteredInputs.push_back(x);
                    continue;
                }

                filteredInputs.push_back(FilterByFields(node->Pos(), x, usedFields, ctx, false));
            }

            YQL_CLOG(DEBUG, Core) << "FieldsSubset in " << node->Content() << " over " << self.Input().Ref().Content();
            newInput = ctx.ChangeChildren(self.Input().Ref(), std::move(filteredInputs));
        }
        else {
            TExprNode::TListType fieldNodes;
            for (auto& item : itemStructType->GetItems()) {
                if (usedFields.contains(item->GetName())) {
                    fieldNodes.push_back(ctx.NewAtom(self.Pos(), item->GetName()));
                }
            }

            YQL_CLOG(DEBUG, Core) << node->Content() << "SubsetFields";
            newInput = Build<TCoExtractMembers>(ctx, self.Input().Pos())
                .Input(self.Input())
                .Members()
                    .Add(fieldNodes)
                .Build()
                .Done()
                .Ptr();
        }

        return Build<TCoCombineByKey>(ctx, self.Pos())
            .Input(newInput)
            .PreMapLambda(ctx.DeepCopyLambda(self.PreMapLambda().Ref()))
            .KeySelectorLambda(ctx.DeepCopyLambda(self.KeySelectorLambda().Ref()))
            .InitHandlerLambda(ctx.DeepCopyLambda(self.InitHandlerLambda().Ref()))
            .UpdateHandlerLambda(ctx.DeepCopyLambda(self.UpdateHandlerLambda().Ref()))
            .FinishHandlerLambda(ctx.DeepCopyLambda(self.FinishHandlerLambda().Ref()))
            .Done()
            .Ptr();
    };

    map["EquiJoin"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        ui32 inputsCount = node->ChildrenSize() - 2;
        for (ui32 i = 0; i < inputsCount; ++i) {
            if (node->Child(i)->Child(0)->IsCallable("EquiJoin") &&
                optCtx.IsSingleUsage(*node->Child(i)) &&
                optCtx.IsSingleUsage(*node->Child(i)->Child(0))) {
                auto ret = FuseEquiJoins(node, i, ctx);
                if (ret != node) {
                    YQL_CLOG(DEBUG, Core) << "FuseEquiJoins";
                    return ret;
                }
            }
        }

        auto ret = PullUpFlatMapOverEquiJoin(node, ctx, optCtx);
        if (ret != node) {
            YQL_CLOG(DEBUG, Core) << "PullUpFlatMapOverEquiJoin";
            return ret;
        }

        return node;
    };

    map["ExtractMembers"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        TCoExtractMembers self(node);
        if (!optCtx.IsSingleUsage(self.Input())) {
            return node;
        }

        if (self.Input().Maybe<TCoTake>()) {
            if (auto res = ApplyExtractMembersToTake(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoSkip>()) {
            if (auto res = ApplyExtractMembersToSkip(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoSkipNullMembers>()) {
            if (auto res = ApplyExtractMembersToSkipNullMembers(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoFilterNullMembers>()) {
            if (auto res = ApplyExtractMembersToFilterNullMembers(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoSortBase>()) {
            if (auto res = ApplyExtractMembersToSort(self.Input().Ptr(), self.Members().Ptr(), *optCtx.ParentsMap, ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoAssumeUnique>() || self.Input().Maybe<TCoAssumeDistinct>()) {
            if (auto res = ApplyExtractMembersToAssumeUnique(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoTopBase>()) {
            if (auto res = ApplyExtractMembersToTop(self.Input().Ptr(), self.Members().Ptr(), *optCtx.ParentsMap, ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoExtendBase>()) {
            if (auto res = ApplyExtractMembersToExtend(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoEquiJoin>()) {
            if (auto res = ApplyExtractMembersToEquiJoin(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoFlatMapBase>()) {
            if (auto res = ApplyExtractMembersToFlatMap(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoPartitionByKey>()) {
            if (auto res = ApplyExtractMembersToPartitionByKey(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoCalcOverWindowBase>() || self.Input().Maybe<TCoCalcOverWindowGroup>()) {
            if (auto res = ApplyExtractMembersToCalcOverWindow(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoAggregate>()) {
            if (auto res = ApplyExtractMembersToAggregate(self.Input().Ptr(), self.Members().Ptr(), *optCtx.ParentsMap, ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoChopper>()) {
            if (auto res = ApplyExtractMembersToChopper(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoCollect>()) {
            if (auto res = ApplyExtractMembersToCollect(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoMapJoinCore>()) {
            if (auto res = ApplyExtractMembersToMapJoinCore(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoMapNext>()) {
            if (auto res = ApplyExtractMembersToMapNext(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoChain1Map>()) {
            if (auto res = ApplyExtractMembersToChain1Map(self.Input().Ptr(), self.Members().Ptr(), *optCtx.ParentsMap, ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoCondense1>()) {
            if (auto res = ApplyExtractMembersToCondense1(self.Input().Ptr(), self.Members().Ptr(), *optCtx.ParentsMap, ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoCombineCore>()) {
            if (auto res = ApplyExtractMembersToCombineCore(self.Input().Ptr(), self.Members().Ptr(), ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoNarrowMap>()) {
            if (auto res = ApplyExtractMembersToNarrowMap(self.Input().Ptr(), self.Members().Ptr(), false, ctx, {})) {
                return res;
            }
            return node;
        }

        if (self.Input().Maybe<TCoNarrowMultiMap>()) {
            if (auto res = ApplyExtractMembersToNarrowMap(self.Input().Ptr(), self.Members().Ptr(), false, ctx, {})) {
                return res;
            }
            return node;
        }

        if (const auto narrow = self.Input().Maybe<TCoNarrowFlatMap>()) {
            if (auto res = ApplyExtractMembersToNarrowMap(self.Input().Ptr(), self.Members().Ptr(), ETypeAnnotationKind::Optional != narrow.Cast().Lambda().Body().Ref().GetTypeAnn()->GetKind(), ctx, {})) {
                return res;
            }
            return node;
        }

        return node;
    };

    map[TCoChopper::CallableName()] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        const TCoChopper chopper(node);
        const auto arg = chopper.Handler().Args().Arg(1).Raw();
        if (const auto parents = optCtx.ParentsMap->find(arg); parents != optCtx.ParentsMap->cend()
            && parents->second.size() == 1
            && (*parents->second.begin())->IsCallable(TCoExtractMembers::CallableName())
            && arg == &(*parents->second.begin())->Head())
        {
            const auto extract = *parents->second.begin();
            std::map<std::string_view, TExprNode::TPtr> usedFields;
            auto fields = extract->Tail().ChildrenList();
            std::for_each(fields.cbegin(), fields.cend(), [&](const TExprNode::TPtr& field){ usedFields.emplace(field->Content(), field); });

            if (HaveFieldsSubset(chopper.KeyExtractor().Body().Ptr(), chopper.KeyExtractor().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
                && !usedFields.empty()
                && HaveFieldsSubset(chopper.GroupSwitch().Body().Ptr(), chopper.GroupSwitch().Args().Arg(1).Ref(), usedFields, *optCtx.ParentsMap, false)
                && !usedFields.empty()
                && (GetSeqItemType(*chopper.Input().Ref().GetTypeAnn()).GetKind() == ETypeAnnotationKind::Struct)
                && usedFields.size() < GetSeqItemType(*chopper.Input().Ref().GetTypeAnn()).Cast<TStructExprType>()->GetSize()) {
                if (usedFields.size() != fields.size()) {
                    fields.reserve(usedFields.size());
                    fields.clear();
                    std::transform(usedFields.begin(), usedFields.end(), std::back_inserter(fields),
                        [](std::pair<const std::string_view, TExprNode::TPtr>& item){ return std::move(item.second); });
                }

                YQL_CLOG(DEBUG, Core) << "Pull out " << extract->Content() << " from " << node->Content();
                return Build<TCoChopper>(ctx, chopper.Pos())
                    .Input<TCoExtractMembers>()
                        .Input(chopper.Input())
                        .Members().Add(std::move(fields)).Build()
                        .Build()
                    .KeyExtractor(ctx.DeepCopyLambda(chopper.KeyExtractor().Ref()))
                    .GroupSwitch(ctx.DeepCopyLambda(chopper.GroupSwitch().Ref()))
                    .Handler(ctx.DeepCopyLambda(chopper.Handler().Ref()))
                    .Done().Ptr();
            }
        }
        return node;
    };

    map["WindowTraits"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        auto structType = node->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
        TSet<TStringBuf> usedFields;
        auto initLambda = node->Child(1);
        auto updateLambda = node->Child(2);
        TSet<TStringBuf> lambdaSubset;
        if (!HaveFieldsSubset(initLambda->ChildPtr(1), *initLambda->Child(0)->Child(0), lambdaSubset, *optCtx.ParentsMap)) {
            return node;
        }

        usedFields.insert(lambdaSubset.cbegin(), lambdaSubset.cend());
        if (!HaveFieldsSubset(updateLambda->ChildPtr(1), *updateLambda->Child(0)->Child(0), lambdaSubset, *optCtx.ParentsMap)) {
            return node;
        }

        usedFields.insert(lambdaSubset.cbegin(), lambdaSubset.cend());
        if (usedFields.size() == structType->GetSize()) {
            return node;
        }

        TVector<const TItemExprType*> subsetItems;
        for (const auto& item : structType->GetItems()) {
            if (usedFields.contains(item->GetName())) {
                subsetItems.push_back(item);
            }
        }

        auto subsetType = ctx.MakeType<TStructExprType>(subsetItems);
        YQL_CLOG(DEBUG, Core) << "FieldSubset for WindowTraits";
        return ctx.Builder(node->Pos())
            .Callable("WindowTraits")
                .Add(0, ExpandType(node->Pos(), *subsetType, ctx))
                .Add(1, ctx.DeepCopyLambda(*node->Child(1)))
                .Add(2, ctx.DeepCopyLambda(*node->Child(2)))
                .Add(3, ctx.DeepCopyLambda(*node->Child(3)))
                .Add(4, ctx.DeepCopyLambda(*node->Child(4)))
                .Add(5, node->Child(5)->IsLambda() ? ctx.DeepCopyLambda(*node->Child(5)) : node->ChildPtr(5))
            .Seal()
            .Build();
    };

    map[TCoHoppingTraits::CallableName()] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        TCoHoppingTraits self(node);

        auto structType = node->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();

        const auto lambdaBody = self.TimeExtractor().Body().Ptr();
        const auto& arg = self.TimeExtractor().Args().Arg(0).Ref();

        TSet<TStringBuf> usedFields;
        if (!HaveFieldsSubset(lambdaBody, arg, usedFields, *optCtx.ParentsMap)) {
            return node;
        }

        if (usedFields.size() == structType->GetSize()) {
            return node;
        }

        TVector<const TItemExprType*> subsetItems;
        for (const auto& item : structType->GetItems()) {
            if (usedFields.contains(item->GetName())) {
                subsetItems.push_back(item);
            }
        }

        auto subsetType = ctx.MakeType<TStructExprType>(subsetItems);
        YQL_CLOG(DEBUG, Core) << "FieldSubset for HoppingTraits";
        return Build<TCoHoppingTraits>(ctx, node->Pos())
            .ItemType(ExpandType(node->Pos(), *subsetType, ctx))
            .TimeExtractor(ctx.DeepCopyLambda(self.TimeExtractor().Ref()))
            .Hop(self.Hop())
            .Interval(self.Interval())
            .Delay(self.Delay())
            .DataWatermarks(self.DataWatermarks())
            .Version(self.Version())
            .Done().Ptr();
    };

    map["AggregationTraits"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        auto type = node->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Struct) {
            // usually distinct, type of column is used instead
            return node;
        }

        auto structType = type->Cast<TStructExprType>();
        TSet<TStringBuf> usedFields;
        auto initLambda = node->Child(1);
        auto updateLambda = node->Child(2);
        TSet<TStringBuf> lambdaSubset;
        if (!HaveFieldsSubset(initLambda->ChildPtr(1), *initLambda->Child(0)->Child(0), lambdaSubset, *optCtx.ParentsMap)) {
            return node;
        }

        usedFields.insert(lambdaSubset.cbegin(), lambdaSubset.cend());
        if (!HaveFieldsSubset(updateLambda->ChildPtr(1), *updateLambda->Child(0)->Child(0), lambdaSubset, *optCtx.ParentsMap)) {
            return node;
        }

        usedFields.insert(lambdaSubset.cbegin(), lambdaSubset.cend());
        if (usedFields.size() == structType->GetSize()) {
            return node;
        }

        TVector<const TItemExprType*> subsetItems;
        for (const auto& item : structType->GetItems()) {
            if (usedFields.contains(item->GetName())) {
                subsetItems.push_back(item);
            }
        }

        auto subsetType = ctx.MakeType<TStructExprType>(subsetItems);
        YQL_CLOG(DEBUG, Core) << "FieldSubset for AggregationTraits";
        return ctx.Builder(node->Pos())
            .Callable("AggregationTraits")
                .Add(0, ExpandType(node->Pos(), *subsetType, ctx))
                .Add(1, ctx.DeepCopyLambda(*node->Child(1)))
                .Add(2, ctx.DeepCopyLambda(*node->Child(2)))
                .Add(3, ctx.DeepCopyLambda(*node->Child(3)))
                .Add(4, ctx.DeepCopyLambda(*node->Child(4)))
                .Add(5, ctx.DeepCopyLambda(*node->Child(5)))
                .Add(6, ctx.DeepCopyLambda(*node->Child(6)))
                .Add(7, node->Child(7)->IsLambda() ? ctx.DeepCopyLambda(*node->Child(7)) : node->ChildPtr(7))
            .Seal()
            .Build();
    };

    map["AggApply"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        auto type = node->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Struct) {
            // usually distinct, type of column is used instead
            return node;
        }

        auto structType = type->Cast<TStructExprType>();
        TSet<TStringBuf> usedFields;
        auto extractor = node->Child(2);
        for (ui32 i = 1; i < extractor->ChildrenSize(); ++i) {
            TSet<TStringBuf> lambdaSubset;
            if (!HaveFieldsSubset(extractor->ChildPtr(i), *extractor->Child(0)->Child(0), lambdaSubset, *optCtx.ParentsMap)) {
                return node;
            }

            usedFields.insert(lambdaSubset.cbegin(), lambdaSubset.cend());
        }

        if (usedFields.size() == structType->GetSize()) {
            return node;
        }

        TVector<const TItemExprType*> subsetItems;
        for (const auto& item : structType->GetItems()) {
            if (usedFields.contains(item->GetName())) {
                subsetItems.push_back(item);
            }
        }

        auto subsetType = ctx.MakeType<TStructExprType>(subsetItems);
        YQL_CLOG(DEBUG, Core) << "FieldSubset for AggApply";
        return ctx.ChangeChild(*node, 1, ExpandType(node->Pos(), *subsetType, ctx));
    };

    map["SessionWindowTraits"] = map["SortTraits"] = map["Lag"] = map["Lead"] = map["RowNumber"] = map["Rank"] = map["DenseRank"] =
        map["CumeDist"] = map["PercentRank"] = map["NTile"] =
        [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx)
    {
        auto structType = node->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()
            ->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        if (node->IsCallable({"RowNumber", "CumeDist", "NTile"})) {
            if (structType->GetSize() == 0) {
                return node;
            }

            auto subsetType = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>()));
            YQL_CLOG(DEBUG, Core) << "FieldSubset for " << node->Content();
            if (node->IsCallable({"NTile","CumeDist"})) {
                return ctx.Builder(node->Pos())
                    .Callable(node->Content())
                        .Add(0, ExpandType(node->Pos(), *subsetType, ctx))
                        .Add(1, node->TailPtr())
                    .Seal()
                    .Build();
            } else {
                return ctx.Builder(node->Pos())
                    .Callable(node->Content())
                        .Add(0, ExpandType(node->Pos(), *subsetType, ctx))
                    .Seal()
                    .Build();
            }
        }

        TSet<ui32> lambdaIndexes;
        TSet<TStringBuf> lambdaSubset;
        if (node->IsCallable("SessionWindowTraits")) {
            lambdaIndexes = { 2, 3, 4 };
            TCoSessionWindowTraits self(node);
            if (auto maybeSort = self.SortSpec().Maybe<TCoSortTraits>()) {
                const TTypeAnnotationNode* itemType =
                    maybeSort.Cast().ListType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TListExprType>()->GetItemType();
                if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
                    for (auto& col : itemType->Cast<TStructExprType>()->GetItems()) {
                        lambdaSubset.insert(col->GetName());
                    }
                }
            }
        } else {
            lambdaIndexes = { node->IsCallable("SortTraits") ? 2u : 1u };
        }

        for (ui32 idx : lambdaIndexes) {
            auto lambda = node->Child(idx);
            if (!HaveFieldsSubset(lambda->ChildPtr(1), *lambda->Child(0)->Child(0), lambdaSubset, *optCtx.ParentsMap)) {
                return node;
            }
        }

        if (lambdaSubset.size() == structType->GetSize()) {
            return node;
        }

        TVector<const TItemExprType*> subsetItems;
        for (const auto& item : structType->GetItems()) {
            if (lambdaSubset.contains(item->GetName())) {
                subsetItems.push_back(item);
            }
        }

        auto subsetType = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(subsetItems));
        YQL_CLOG(DEBUG, Core) << "FieldSubset for " << node->Content();
        if (node->IsCallable("SortTraits")) {
            return ctx.Builder(node->Pos())
                .Callable("SortTraits")
                    .Add(0, ExpandType(node->Pos(), *subsetType, ctx))
                    .Add(1, node->ChildPtr(1))
                    .Add(2, ctx.DeepCopyLambda(*node->ChildPtr(2)))
                .Seal()
                .Build();
        } else if (node->IsCallable("SessionWindowTraits")) {
            return ctx.Builder(node->Pos())
                .Callable("SessionWindowTraits")
                    .Add(0, ExpandType(node->Pos(), *subsetType, ctx))
                    .Add(1, node->ChildPtr(1))
                    .Add(2, ctx.DeepCopyLambda(*node->ChildPtr(2)))
                    .Add(3, ctx.DeepCopyLambda(*node->ChildPtr(3)))
                    .Add(4, ctx.DeepCopyLambda(*node->ChildPtr(4)))
                .Seal()
            .Build();
        } else {
            if (node->ChildrenSize() == 2) {
                return ctx.Builder(node->Pos())
                    .Callable(node->Content())
                        .Add(0, ExpandType(node->Pos(), *subsetType, ctx))
                        .Add(1, ctx.DeepCopyLambda(*node->ChildPtr(1)))
                    .Seal()
                    .Build();
            } else {
                return ctx.Builder(node->Pos())
                    .Callable(node->Content())
                        .Add(0, ExpandType(node->Pos(), *subsetType, ctx))
                        .Add(1, ctx.DeepCopyLambda(*node->ChildPtr(1)))
                        .Add(2, node->ChildPtr(2))
                    .Seal()
                    .Build();
            }
        }
    };

    map["Aggregate"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        TCoAggregate self(node);
        if (!AllowSubsetFieldsForNode(self.Input().Ref(), optCtx) && !optCtx.IsPersistentNode(self.Input())) {
            return node;
        }

        auto ret = AggregateSubsetFieldsAnalyzer(self, ctx, *optCtx.ParentsMap);
        if (ret != node) {
            YQL_CLOG(DEBUG, Core) << node->Content() << "SubsetFieldsAnalyzer";
            return ret;
        }

        return node;
    };

    map["CalcOverWindow"] = map["CalcOverSessionWindow"] = map["CalcOverWindowGroup"] =
        [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx)
    {
        if (!optCtx.IsSingleUsage(node->Head())) {
            return node;
        }

        if (!node->Head().IsCallable({"CalcOverWindow", "CalcOverSessionWindow", "CalcOverWindowGroup"})) {
            return node;
        }

        TExprNodeList parentCalcs = ExtractCalcsOverWindow(node, ctx);
        TExprNodeList calcs = ExtractCalcsOverWindow(node->HeadPtr(), ctx);
        calcs.insert(calcs.end(), parentCalcs.begin(), parentCalcs.end());

        YQL_CLOG(DEBUG, Core) << "Fuse nested " << node->Content() << " and " << node->Head().Content();

        return RebuildCalcOverWindowGroup(node->Head().Pos(), node->Head().HeadPtr(), calcs, ctx);
    };

    map[TCoCondense::CallableName()] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        const TCoCondense self(node);
        if (!AllowSubsetFieldsForNode(self.Input().Ref(), optCtx)) {
            return node;
        }

        std::map<std::string_view, TExprNode::TPtr> usedFields;
        if (HaveFieldsSubset(self.SwitchHandler().Body().Ptr(), self.SwitchHandler().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && HaveFieldsSubset(self.UpdateHandler().Body().Ptr(), self.UpdateHandler().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && (GetSeqItemType(*self.Input().Ref().GetTypeAnn()).GetKind() == ETypeAnnotationKind::Struct)
            && usedFields.size() < GetSeqItemType(*self.Input().Ref().GetTypeAnn()).Cast<TStructExprType>()->GetSize())
        {
            TExprNode::TListType fields;
            fields.reserve(usedFields.size());
            std::transform(usedFields.begin(), usedFields.end(), std::back_inserter(fields),
                [](std::pair<const std::string_view, TExprNode::TPtr>& item){ return std::move(item.second); });

            YQL_CLOG(DEBUG, Core) << node->Content() << "SubsetFields";
            return Build<TCoCondense>(ctx, node->Pos())
                .Input<TCoExtractMembers>()
                    .Input(self.Input())
                    .Members()
                        .Add(std::move(fields))
                    .Build()
                .Build()
                .State(self.State())
                .SwitchHandler(ctx.DeepCopyLambda(self.SwitchHandler().Ref()))
                .UpdateHandler(ctx.DeepCopyLambda(self.UpdateHandler().Ref()))
                .Done().Ptr();
        }
        return node;
    };

    map[TCoCondense1::CallableName()] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        const TCoCondense1 self(node);
        if (!AllowSubsetFieldsForNode(self.Input().Ref(), optCtx)) {
            return node;
        }

        std::map<std::string_view, TExprNode::TPtr> usedFields;
        if (HaveFieldsSubset(self.InitHandler().Body().Ptr(), self.InitHandler().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && HaveFieldsSubset(self.SwitchHandler().Body().Ptr(), self.SwitchHandler().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && HaveFieldsSubset(self.UpdateHandler().Body().Ptr(), self.UpdateHandler().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && (GetSeqItemType(*self.Input().Ref().GetTypeAnn()).GetKind() == ETypeAnnotationKind::Struct)
            && usedFields.size() < GetSeqItemType(*self.Input().Ref().GetTypeAnn()).Cast<TStructExprType>()->GetSize())
        {
            TExprNode::TListType fields;
            fields.reserve(usedFields.size());
            std::transform(usedFields.begin(), usedFields.end(), std::back_inserter(fields),
                [](std::pair<const std::string_view, TExprNode::TPtr>& item){ return std::move(item.second); });

            YQL_CLOG(DEBUG, Core) << node->Content() << "SubsetFields";
            return Build<TCoCondense1>(ctx, node->Pos())
                .Input<TCoExtractMembers>()
                    .Input(self.Input())
                    .Members()
                        .Add(std::move(fields))
                    .Build()
                .Build()
                .InitHandler(ctx.DeepCopyLambda(self.InitHandler().Ref()))
                .SwitchHandler(ctx.DeepCopyLambda(self.SwitchHandler().Ref()))
                .UpdateHandler(ctx.DeepCopyLambda(self.UpdateHandler().Ref()))
                .Done().Ptr();
        }
        return node;
    };

    map[TCoChain1Map::CallableName()] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        const TCoChain1Map self(node);
        if (!AllowSubsetFieldsForNode(self.Input().Ref(), optCtx)) {
            return node;
        }

        std::map<std::string_view, TExprNode::TPtr> usedFields;
        if (HaveFieldsSubset(self.InitHandler().Body().Ptr(), self.InitHandler().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && HaveFieldsSubset(self.UpdateHandler().Body().Ptr(), self.UpdateHandler().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && (GetSeqItemType(*self.Input().Ref().GetTypeAnn()).GetKind() == ETypeAnnotationKind::Struct)
            && usedFields.size() < GetSeqItemType(*self.Input().Ref().GetTypeAnn()).Cast<TStructExprType>()->GetSize())
        {
            TExprNode::TListType fields;
            fields.reserve(usedFields.size());
            std::transform(usedFields.begin(), usedFields.end(), std::back_inserter(fields),
                [](std::pair<const std::string_view, TExprNode::TPtr>& item){ return std::move(item.second); });

            YQL_CLOG(DEBUG, Core) << node->Content() << "SubsetFields";
            return Build<TCoChain1Map>(ctx, node->Pos())
                .Input<TCoExtractMembers>()
                    .Input(self.Input())
                    .Members()
                        .Add(std::move(fields))
                    .Build()
                .Build()
                .InitHandler(ctx.DeepCopyLambda(self.InitHandler().Ref()))
                .UpdateHandler(ctx.DeepCopyLambda(self.UpdateHandler().Ref()))
                .Done().Ptr();
        }
        return node;
    };

    map[TCoMapNext::CallableName()] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        TCoMapNext self(node);
        if (!AllowSubsetFieldsForNode(self.Input().Ref(), optCtx)) {
            return node;
        }

        std::map<std::string_view, TExprNode::TPtr> usedFields;
        if ((
             HaveFieldsSubset(self.Lambda().Body().Ptr(), self.Lambda().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false) &&
             HaveFieldsSubset(self.Lambda().Body().Ptr(), self.Lambda().Args().Arg(1).Ref(), usedFields, *optCtx.ParentsMap, false)
            )
                && (GetSeqItemType(*self.Input().Ref().GetTypeAnn()).GetKind() == ETypeAnnotationKind::Struct)
                && usedFields.size() < GetSeqItemType(*self.Input().Ref().GetTypeAnn()).Cast<TStructExprType>()->GetSize())
        {
            TExprNode::TListType fields;
            fields.reserve(usedFields.size());
            std::transform(usedFields.begin(), usedFields.end(), std::back_inserter(fields),
                [](std::pair<const std::string_view, TExprNode::TPtr>& item){ return std::move(item.second); });

            YQL_CLOG(DEBUG, Core) << node->Content() << "SubsetFields";
            return Build<TCoMapNext>(ctx, node->Pos())
                .Input<TCoExtractMembers>()
                    .Input(self.Input())
                    .Members()
                        .Add(std::move(fields))
                    .Build()
                .Build()
                .Lambda(ctx.DeepCopyLambda(self.Lambda().Ref()))
                .Done().Ptr();
        }
        return node;
    };

    map[TCoSqueezeToDict::CallableName()] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        const TCoSqueezeToDict self(node);
        if (!AllowSubsetFieldsForNode(self.Stream().Ref(), optCtx)) {
            return node;
        }

        std::map<std::string_view, TExprNode::TPtr> usedFields;
        if (HaveFieldsSubset(self.KeySelector().Body().Ptr(), self.KeySelector().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && HaveFieldsSubset(self.PayloadSelector().Body().Ptr(), self.PayloadSelector().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && (GetSeqItemType(*self.Stream().Ref().GetTypeAnn()).GetKind() == ETypeAnnotationKind::Struct)
            && usedFields.size() < GetSeqItemType(*self.Stream().Ref().GetTypeAnn()).Cast<TStructExprType>()->GetSize())
        {
            TExprNode::TListType fields;
            fields.reserve(usedFields.size());
            std::transform(usedFields.begin(), usedFields.end(), std::back_inserter(fields),
                [](std::pair<const std::string_view, TExprNode::TPtr>& item){ return std::move(item.second); });

            YQL_CLOG(DEBUG, Core) << node->Content() << "SubsetFields";
            return Build<TCoSqueezeToDict>(ctx, node->Pos())
                .Stream<TCoExtractMembers>()
                    .Input(self.Stream())
                    .Members()
                        .Add(std::move(fields))
                    .Build()
                .Build()
                .KeySelector(ctx.DeepCopyLambda(self.KeySelector().Ref()))
                .PayloadSelector(ctx.DeepCopyLambda(self.PayloadSelector().Ref()))
                .Settings(self.Settings())
                .Done().Ptr();
        }
        return node;
    };

    map[TCoCombineCore::CallableName()] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        const TCoCombineCore self(node);
        if (!AllowSubsetFieldsForNode(self.Input().Ref(), optCtx)) {
            return node;
        }

        std::map<std::string_view, TExprNode::TPtr> usedFields;
        if (HaveFieldsSubset(self.KeyExtractor().Body().Ptr(), self.KeyExtractor().Args().Arg(0).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && HaveFieldsSubset(self.InitHandler().Body().Ptr(), self.InitHandler().Args().Arg(1).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && HaveFieldsSubset(self.UpdateHandler().Body().Ptr(), self.UpdateHandler().Args().Arg(1).Ref(), usedFields, *optCtx.ParentsMap, false)
            && !usedFields.empty()
            && (GetSeqItemType(*self.Input().Ref().GetTypeAnn()).GetKind() == ETypeAnnotationKind::Struct)
            && usedFields.size() < GetSeqItemType(*self.Input().Ref().GetTypeAnn()).Cast<TStructExprType>()->GetSize())
        {
            TExprNode::TListType fields;
            fields.reserve(usedFields.size());
            std::transform(usedFields.begin(), usedFields.end(), std::back_inserter(fields),
                [](std::pair<const std::string_view, TExprNode::TPtr>& item){ return std::move(item.second); });

            YQL_CLOG(DEBUG, Core) << node->Content() << "SubsetFields";
            return Build<TCoCombineCore>(ctx, node->Pos())
                .Input<TCoExtractMembers>()
                    .Input(self.Input())
                    .Members()
                        .Add(std::move(fields))
                    .Build()
                .Build()
                .KeyExtractor(ctx.DeepCopyLambda(self.KeyExtractor().Ref()))
                .InitHandler(ctx.DeepCopyLambda(self.InitHandler().Ref()))
                .UpdateHandler(ctx.DeepCopyLambda(self.UpdateHandler().Ref()))
                .FinishHandler(ctx.DeepCopyLambda(self.FinishHandler().Ref()))
                .MemLimit(self.MemLimit())
                .Done().Ptr();
        }
        return node;
    };

    map[TCoMapJoinCore::CallableName()] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        const TCoMapJoinCore self(node);
        if (!AllowSubsetFieldsForNode(self.LeftInput().Ref(), optCtx)) {
            return node;
        }

        const auto& leftItemType = GetSeqItemType(*self.LeftInput().Ref().GetTypeAnn());
        if (ETypeAnnotationKind::Struct != leftItemType.GetKind()) {
            return node;
        }

        std::unordered_set<std::string_view> leftFileldsSet(self.LeftKeysColumns().Size() + (self.LeftRenames().Size() >> 1U));
        self.LeftKeysColumns().Ref().ForEachChild([&leftFileldsSet](const TExprNode& field) { leftFileldsSet.emplace(field.Content()); });
        TExprNode::TListType renamed;
        renamed.reserve(self.LeftRenames().Size() >> 1U);
        for (auto i = 0U; i < self.LeftRenames().Size(); ++++i) {
            if (leftFileldsSet.emplace(self.LeftRenames().Item(i).Value()).second)
                renamed.emplace_back(self.LeftRenames().Item(i).Ptr());
        }

        if (leftFileldsSet.size() < leftItemType.Cast<TStructExprType>()->GetSize()) {
            auto fields = self.LeftKeysColumns().Ptr();
            if (!renamed.empty()) {
                auto children = fields->ChildrenList();
                std::move(renamed.begin(), renamed.end(), std::back_inserter(children));
                fields = ctx.ChangeChildren(*fields, std::move(children));
            }

            YQL_CLOG(DEBUG, Core) << node->Content() << "SubsetFields";
            return Build<TCoMapJoinCore>(ctx, node->Pos())
                .InitFrom(self)
                .LeftInput<TCoExtractMembers>()
                    .Input(self.LeftInput())
                    .Members(std::move(fields))
                    .Build()
                .Done().Ptr();
        }

        return node;
    };

    map[TCoUnordered::CallableName()] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head())) {
            return node;
        }

        if (node->Head().IsCallable({"Merge", "OrderedExtend"})) {
            YQL_CLOG(DEBUG, Core) << "Swap " << node->Content() << " with " << node->Head().Content();
            auto children = node->Head().ChildrenList();
            std::transform(children.begin(), children.end(), children.begin(), [&](TExprNode::TPtr& child) { return ctx.ChangeChild(*node, 0U, std::move(child)); });
            return ctx.NewCallable(node->Head().Pos(), "Extend", std::move(children));
        }

        if (node->Head().IsCallable() && node->Head().Content().starts_with("Ordered")) {
            YQL_CLOG(DEBUG, Core) << "Swap " << node->Content() << " with " << node->Head().Content();
            auto children = node->Head().ChildrenList();
            children.front() = ctx.ChangeChild(*node, 0U, std::move(children.front()));
            return ctx.NewCallable(node->Head().Pos(), node->Head().Content().substr(7U), std::move(children));
        }

        if (node->Head().IsCallable("Mux")) {
            YQL_CLOG(DEBUG, Core) << "Swap " << node->Content() << " with " << node->Head().Content();
            auto children = node->Head().ChildrenList();
            std::transform(children.begin(), children.end(), children.begin(), [&](TExprNode::TPtr& child) { return ctx.ChangeChild(*node, 0U, std::move(child)); });
            return ctx.ChangeChildren(node->Head(), std::move(children));
        }

        if (node->Head().IsCallable({
            "Collect", "LazyList", "ForwardList","Iterator","FromFlow","ToFlow","AssumeUnique","AssumeDistinct",
            "FilterNullMembers","SkipNullMembers","FilterNullElements","SkipNullElements",
            "ExpandMap","WideMap","WideFilter","NarrowMap","NarrowFlatMap","NarrowMultiMap",
            "MapJoinCore"
        })) {
            YQL_CLOG(DEBUG, Core) << "Swap " << node->Content() << " with " << node->Head().Content();
            return ctx.SwapWithHead(*node);
        }

        if (node->Head().IsCallable("TopSort")) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
            return ctx.RenameNode(node->Head(), "Top");
        }

        if (node->Head().IsCallable({"Sort", "AssumeSorted"})) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " absorbs " << node->Head().Content();
            return ctx.ChangeChild(*node, 0U, node->Head().HeadPtr());
        }

        return node;
    };
}

}
