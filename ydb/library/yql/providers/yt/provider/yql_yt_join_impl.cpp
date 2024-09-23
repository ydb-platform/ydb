#include "yql_yt_join_impl.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_helpers.h"
#include "yql_yt_table.h"

#include <ydb/library/yql/providers/yt/opt/yql_yt_join.h>
#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/string/join.h>
#include <util/string/cast.h>
#include <util/string/builder.h>
#include <util/generic/xrange.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>
#include <util/generic/size_literals.h>
#include <util/generic/strbuf.h>

namespace NYql {

using TStatus = IGraphTransformer::TStatus;

namespace {

TYtJoinNode::TPtr FindYtEquiJoinLeaf(const std::vector<TYtJoinNodeLeaf::TPtr>& leaves, TStringBuf table) {
    for (auto& leaf : leaves) {
        if (!leaf) {
            continue;
        }

        if (Find(leaf->Scope.begin(), leaf->Scope.end(), table) != leaf->Scope.end()) {
            return leaf;
        }
    }

    ythrow yexception() << "Table " << TString{table}.Quote() << " not found";
}

void GatherEquiJoinKeyColumns(const TExprNode::TPtr& columns, THashSet<TString>& keyColumns) {
    for (ui32 i = 0; i < columns->ChildrenSize(); i += 2) {
        auto table = columns->Child(i)->Content();
        auto column = columns->Child(i + 1)->Content();
        keyColumns.insert({ FullColumnName(table, column) });
    }
}

TYtJoinNodeOp::TPtr ImportYtEquiJoinRecursive(const TVector<TYtJoinNodeLeaf::TPtr>& leaves, const TYtJoinNodeOp* parent,
    THashSet<TString>& drops, const TExprNode& joinTree, TExprContext& ctx)
{
    TYtJoinNodeOp::TPtr result(new TYtJoinNodeOp());
    result->Parent = parent;
    result->JoinKind = joinTree.HeadPtr();
    result->LeftLabel = joinTree.ChildPtr(3);
    result->RightLabel = joinTree.ChildPtr(4);
    result->LinkSettings = GetEquiJoinLinkSettings(*joinTree.Child(5));

    THashSet<TString> leftKeys, rightKeys;
    GatherEquiJoinKeyColumns(result->LeftLabel, leftKeys);
    GatherEquiJoinKeyColumns(result->RightLabel, rightKeys);
    if (!result->JoinKind->IsAtom({"RightOnly", "RightSemi"})) {
        for (const auto& key : leftKeys) {
            if (drops.contains(key)) {
                result->OutputRemoveColumns.insert(key);
            }
        }
    }

    if (!result->JoinKind->IsAtom({"LeftOnly", "LeftSemi"})) {
        for (const auto& key : rightKeys) {
            if (drops.contains(key)) {
                result->OutputRemoveColumns.insert(key);
            }
        }
    }

    std::vector<std::string_view> lCheck, rCheck;

    lCheck.reserve(leftKeys.size());
    for (const auto& key : leftKeys) {
        drops.erase(key);
        lCheck.emplace_back(key);
    }

    rCheck.reserve(rightKeys.size());
    for (const auto& key : rightKeys) {
        drops.erase(key);
        rCheck.emplace_back(key);
    }

    result->Left = joinTree.Child(1)->IsAtom() ?
        FindYtEquiJoinLeaf(leaves, joinTree.Child(1)->Content()):
        ImportYtEquiJoinRecursive(leaves, result.Get(), drops, *joinTree.Child(1), ctx);

    result->Right = joinTree.Child(2)->IsAtom() ?
        FindYtEquiJoinLeaf(leaves, joinTree.Child(2)->Content()):
        ImportYtEquiJoinRecursive(leaves, result.Get(), drops, *joinTree.Child(2), ctx);

    result->Scope.insert(result->Scope.end(), result->Left->Scope.cbegin(), result->Left->Scope.cend());
    result->Scope.insert(result->Scope.end(), result->Right->Scope.cbegin(), result->Right->Scope.cend());

    const std::string_view joinKind = result->JoinKind->Content();
    const bool singleSide = joinKind.ends_with("Only") || joinKind.ends_with("Semi");
    const bool rightSide = joinKind.starts_with("Right");
    const bool leftSide = joinKind.starts_with("Left");

    const auto lUnique = result->Left->Constraints.GetConstraint<TUniqueConstraintNode>();
    const auto rUnique = result->Right->Constraints.GetConstraint<TUniqueConstraintNode>();

    const bool lAny = result->LinkSettings.LeftHints.contains("unique") || result->LinkSettings.LeftHints.contains("any");
    const bool rAny = result->LinkSettings.RightHints.contains("unique") || result->LinkSettings.RightHints.contains("any");

    const bool lOneRow = lAny || lUnique && lUnique->ContainsCompleteSet(lCheck);
    const bool rOneRow = rAny || rUnique && rUnique->ContainsCompleteSet(rCheck);

    if (singleSide) {
        if (leftSide)
            result->Constraints.AddConstraint(lUnique);
        else if (rightSide)
            result->Constraints.AddConstraint(rUnique);
    } else if (!result->JoinKind->IsAtom("Cross")) {
        const bool exclusion = result->JoinKind->IsAtom("Exclusion");
        const bool useLeft = lUnique && (rOneRow || exclusion);
        const bool useRight = rUnique && (lOneRow || exclusion);

        if (useLeft && !useRight)
            result->Constraints.AddConstraint(lUnique);
        else if (useRight && !useLeft)
            result->Constraints.AddConstraint(rUnique);
        else if (useLeft && useRight)
            result->Constraints.AddConstraint(TUniqueConstraintNode::Merge(lUnique, rUnique, ctx));
    }

    const auto lDistinct = result->Left->Constraints.GetConstraint<TDistinctConstraintNode>();
    const auto rDistinct = result->Right->Constraints.GetConstraint<TDistinctConstraintNode>();

    if (singleSide) {
        if (leftSide)
            result->Constraints.AddConstraint(lDistinct);
        else if (rightSide)
            result->Constraints.AddConstraint(rDistinct);
    } else if (!result->JoinKind->IsAtom("Cross")) {
        const bool inner = result->JoinKind->IsAtom("Inner");
        const bool useLeft = lDistinct && rOneRow && (inner || leftSide);
        const bool useRight = rDistinct && lOneRow && (inner || rightSide);

        if (useLeft && !useRight)
            result->Constraints.AddConstraint(lDistinct);
        else if (useRight && !useLeft)
            result->Constraints.AddConstraint(rDistinct);
        else if (useLeft && useRight)
            result->Constraints.AddConstraint(TDistinctConstraintNode::Merge(lDistinct, rDistinct, ctx));
    }

    const auto lEmpty = result->Left->Constraints.GetConstraint<TEmptyConstraintNode>();
    const auto rEmpty = result->Right->Constraints.GetConstraint<TEmptyConstraintNode>();

    if (lEmpty || rEmpty) {
        if (result->JoinKind->IsAtom({"Inner", "LeftSemi", "RightSemi", "Cross"}))
            result->Constraints.AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        else if (leftSide && lEmpty)
            result->Constraints.AddConstraint(lEmpty);
        else if (rightSide && rEmpty)
            result->Constraints.AddConstraint(rEmpty);
        else if (lEmpty && rEmpty)
            result->Constraints.AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
    }

    return result;
}

bool IsEffectivelyUnique(const TEquiJoinLinkSettings& linkSettings, const TMapJoinSettings& settings, bool isLeft) {
    auto& hints = isLeft ? linkSettings.LeftHints : linkSettings.RightHints;
    return hints.contains("unique") || hints.contains("any") || (isLeft ? settings.LeftUnique : settings.RightUnique);
}

bool HasNonTrivialAny(const TEquiJoinLinkSettings& linkSettings, const TMapJoinSettings& settings, TChoice side) {
    YQL_ENSURE(IsLeftOrRight(side));
    auto& hints = (side == TChoice::Left) ? linkSettings.LeftHints : linkSettings.RightHints;
    bool unique = ((side == TChoice::Left) ? settings.LeftUnique : settings.RightUnique) || hints.contains("unique");
    return hints.contains("any") && !unique;
}

ui64 CalcInMemorySize(const TJoinLabels& labels, const TYtJoinNodeOp& op,
    TExprContext& ctx, const TMapJoinSettings& settings, bool isLeft, bool needPayload, ui64 size, bool mapJoinUseFlow)
{
    const TJoinLabel& label = labels.Inputs[isLeft ? 0 : 1];

    const ui64 rows = isLeft ? settings.LeftRows : settings.RightRows;

    if (op.JoinKind->IsAtom("Cross")) {
        if (mapJoinUseFlow) {
            return size + rows * (1ULL + label.InputType->GetSize()) * sizeof(NKikimr::NUdf::TUnboxedValuePod); // Table content after Collect
        } else {
            ui64 avgOtherSideWeight = (isLeft ? settings.RightSize : settings.LeftSize) / (isLeft ? settings.RightRows : settings.LeftRows);

            ui64 rowFactor = (1 + label.InputType->GetSize()) * sizeof(NKikimr::NUdf::TUnboxedValuePod); // Table content after Collect
            rowFactor += (1 + label.InputType->GetSize() + labels.Inputs[isLeft ? 1 : 0].InputType->GetSize()) * sizeof(NKikimr::NUdf::TUnboxedValuePod); // Table content after Map with added left side
            rowFactor += avgOtherSideWeight; // Average added left side for each row after Map

            return 2 * size + rows * rowFactor;
        }
    }

    const bool isUniqueKey = IsEffectivelyUnique(op.LinkSettings, settings, isLeft);

    bool many = needPayload && !isUniqueKey;

    auto inputKeyType = BuildJoinKeyType(label, *(isLeft ? op.LeftLabel : op.RightLabel));
    auto keyType = AsDictKeyType(inputKeyType, ctx);
    const TTypeAnnotationNode* payloadType = needPayload ? label.InputType : (const TTypeAnnotationNode*)ctx.MakeType<TVoidExprType>();

    double sizeFactor = 1.;
    ui64 rowFactor = 0;
    CalcToDictFactors(keyType, payloadType, EDictType::Hashed, many, true, sizeFactor, rowFactor);

    return size * sizeFactor + rows * rowFactor;
}

IGraphTransformer::TStatus TryEstimateDataSizeChecked(TVector<ui64>& result, TYtSection& inputSection, const TString& cluster,
    const TVector<TYtPathInfo::TPtr>& paths, const TMaybe<TVector<TString>>& columns, const TYtState& state, TExprContext& ctx)
{
    if (GetJoinCollectColumnarStatisticsMode(*state.Configuration) == EJoinCollectColumnarStatisticsMode::Sync) {
        auto syncResult = EstimateDataSize(cluster, paths, columns, state, ctx);
        if (!syncResult) {
            return IGraphTransformer::TStatus::Error;
        }
        result = std::move(*syncResult);
        return IGraphTransformer::TStatus::Ok;
    }

    TSet<TString> requestedColumns;
    auto status = TryEstimateDataSize(result, requestedColumns, cluster, paths, columns, state, ctx);
    if (status == TStatus::Repeat) {
        bool hasStatColumns = NYql::HasSetting(inputSection.Settings().Ref(), EYtSettingType::StatColumns);
        if (hasStatColumns) {
            auto oldColumns = NYql::GetSettingAsColumnList(inputSection.Settings().Ref(), EYtSettingType::StatColumns);
            TSet<TString> oldColumnSet(oldColumns.begin(), oldColumns.end());

            bool alreadyRequested = AllOf(requestedColumns, [&](const auto& c) {
                return oldColumnSet.contains(c);
            });

            YQL_ENSURE(!alreadyRequested);
        }

        YQL_CLOG(INFO, ProviderYt) << "Stat missing for columns: " << JoinSeq(", ", requestedColumns) << ", rebuilding section";
        TVector<TString> requestedColumnList(requestedColumns.begin(), requestedColumns.end());

        inputSection = Build<TYtSection>(ctx, inputSection.Ref().Pos())
            .InitFrom(inputSection)
            .Settings(NYql::AddSettingAsColumnList(inputSection.Settings().Ref(), EYtSettingType::StatColumns, requestedColumnList, ctx))
            .Done();
    }
    return status;
}


TStatus UpdateInMemorySizeSetting(TMapJoinSettings& settings, TYtSection& inputSection, const TJoinLabels& labels,
    const TYtJoinNodeOp& op, TExprContext& ctx, bool isLeft,
    const TStructExprType* itemType, const TVector<TString>& joinKeyList, const TYtState::TPtr& state, const TString& cluster,
    const TVector<TYtPathInfo::TPtr>& tables, bool mapJoinUseFlow)
{
    ui64 size = isLeft ? settings.LeftSize : settings.RightSize;
    const bool needPayload = op.JoinKind->IsAtom("Inner") || op.JoinKind->IsAtom(isLeft ? "Right" : "Left");

    if (!needPayload && !op.JoinKind->IsAtom("Cross")) {
        if (joinKeyList.size() < itemType->GetSize()) {
            TVector<ui64> dataSizes;
            auto status = TryEstimateDataSizeChecked(dataSizes, inputSection, cluster, tables, joinKeyList, *state, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
            size = Accumulate(dataSizes.begin(), dataSizes.end(), 0ull, [](ui64 sum, ui64 v) { return sum + v; });;
        }
    }

    (isLeft ? settings.LeftMemSize : settings.RightMemSize) =
        CalcInMemorySize(labels, op, ctx, settings, isLeft, needPayload, size, mapJoinUseFlow);
    return TStatus::Ok;
}

TYtJoinNodeLeaf::TPtr ConvertYtEquiJoinToLeaf(const TYtJoinNodeOp& op, TPositionHandle pos, TExprContext& ctx) {
    auto joinLabelBuilder = Build<TCoAtomList>(ctx, pos);
    for (auto& x : op.Scope) {
        joinLabelBuilder.Add().Value(x).Build();
    }
    auto label = joinLabelBuilder.Done().Ptr();
    auto section = Build<TYtSection>(ctx, pos)
        .Paths()
            .Add()
                .Table<TYtOutput>()
                    .Operation(op.Output.Cast())
                    .OutIndex().Value("0").Build()
                .Build()
                .Columns<TCoVoid>().Build()
                .Ranges<TCoVoid>().Build()
                .Stat<TCoVoid>().Build()
            .Build()
        .Build()
        .Settings()
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::JoinLabel))
                .Build()
                .Value(label)
            .Build()
        .Build()
        .Done();

    TYtJoinNodeLeaf::TPtr leaf = MakeIntrusive<TYtJoinNodeLeaf>(section, TMaybeNode<TCoLambda>());
    leaf->Scope = op.Scope;
    leaf->Label = label;
    return leaf;
}

void UpdateSortPrefix(bool initial, TVector<TString>& sortedKeys, const TYqlRowSpecInfo::TPtr& rowSpec,
    bool& areUniqueKeys, const THashSet<TString>& keySet, TMaybeNode<TCoLambda> premap)
{
    if (!rowSpec) {
        areUniqueKeys = false;
        sortedKeys.clear();
        return;
    }
    if (initial) {
        sortedKeys = rowSpec->SortedBy;
        TMaybe<THashSet<TStringBuf>> passthroughFields;
        bool isPassthrough = true;

        if (premap) {
            isPassthrough = IsPassthroughLambda(premap.Cast(), &passthroughFields);
        }

        for (size_t pos = 0; isPassthrough && pos < rowSpec->SortDirections.size(); ++pos) {
            if (!rowSpec->SortDirections[pos] ||
                (premap && passthroughFields && !(*passthroughFields).contains(rowSpec->SortedBy[pos])))
            {
                sortedKeys.resize(pos);
                break;
            }
        }
        areUniqueKeys = areUniqueKeys && (rowSpec->SortedBy.size() == sortedKeys.size());
    }
    else {
        size_t newPrefixLength = Min(sortedKeys.size(), rowSpec->SortedBy.size());
        for (size_t pos = 0; pos < newPrefixLength; ++pos) {
            if (sortedKeys[pos] != rowSpec->SortedBy[pos] || !rowSpec->SortDirections[pos]) {
                newPrefixLength = pos;
                break;
            }
        }

        areUniqueKeys = areUniqueKeys && (newPrefixLength == sortedKeys.size());
        sortedKeys.resize(newPrefixLength);
    }

    areUniqueKeys = areUniqueKeys && rowSpec->UniqueKeys && sortedKeys.size() == keySet.size();
    ui32 foundKeys = 0;
    for (auto key : sortedKeys) {
        if (keySet.contains(key)) {
            ++foundKeys;
        } else {
            break;
        }
    }

    if (foundKeys != keySet.size()) {
        areUniqueKeys = false;
        sortedKeys.clear();
    }
}

TVector<TString> BuildCompatibleSortWith(const TVector<TString>& otherSortedKeys, const TVector<TString>& otherJoinKeyList,
                                         const TVector<TString>& myJoinKeyList)
{
    YQL_ENSURE(myJoinKeyList.size() == otherJoinKeyList.size());
    YQL_ENSURE(otherSortedKeys.size() >= otherJoinKeyList.size());

    THashMap<TString, size_t> otherJoinListPos;
    for (size_t i = 0; i < otherJoinKeyList.size(); ++i) {
        otherJoinListPos[otherJoinKeyList[i]] = i;
    }

    YQL_ENSURE(otherJoinListPos.size() == otherJoinKeyList.size());


    TVector<TString> mySortedKeys;
    mySortedKeys.reserve(myJoinKeyList.size());

    for (size_t i = 0; i < Min(myJoinKeyList.size(), otherSortedKeys.size()); ++i) {
        auto otherKey = otherSortedKeys[i];
        auto joinPos = otherJoinListPos.find(otherKey);
        YQL_ENSURE(joinPos != otherJoinListPos.end());

        mySortedKeys.push_back(myJoinKeyList[joinPos->second]);
    }


    return mySortedKeys;
}


TVector<TString> BuildCommonSortPrefix(const TVector<TString>& leftSortedKeys, const TVector<TString>& rightSortedKeys,
                                       const TVector<TString>& leftJoinKeyList, const TVector<TString>& rightJoinKeyList,
                                       THashMap<TString, TString>& leftKeyRenames, THashMap<TString, TString>& rightKeyRenames,
                                       bool allowColumnRenames)
{
    TVector<TString> result;

    YQL_ENSURE(leftJoinKeyList.size() == rightJoinKeyList.size());
    size_t joinSize = leftJoinKeyList.size();

    THashMap<TString, size_t> leftJoinListPos;
    THashMap<TString, size_t> rightJoinListPos;
    if (allowColumnRenames) {
        for (size_t i = 0; i < joinSize; ++i) {
            leftJoinListPos[leftJoinKeyList[i]] = i;
            rightJoinListPos[rightJoinKeyList[i]] = i;
        }
    }

    for (size_t pos = 0; pos < Min(leftSortedKeys.size(), rightSortedKeys.size(), joinSize); ++pos) {
        auto left = leftSortedKeys[pos];
        auto right = rightSortedKeys[pos];
        if (left == right) {
            result.push_back(left);
            continue;
        }

        if (!allowColumnRenames) {
            break;
        }

        auto l = leftJoinListPos.find(left);
        YQL_ENSURE(l != leftJoinListPos.end());
        auto leftPos = l->second;

        auto r = rightJoinListPos.find(right);
        YQL_ENSURE(r != rightJoinListPos.end());
        auto rightPos = r->second;

        if (leftPos != rightPos) {
            break;
        }

        TString rename = TStringBuilder() << "_yql_join_renamed_column_" << leftPos;
        leftKeyRenames[left] = rename;
        rightKeyRenames[right] = rename;

        result.push_back(rename);
    }

    return result;
}

TYtSection SectionApplyRenames(const TYtSection& section, const THashMap<TString, TString>& renames, TExprContext& ctx) {
    if (!renames) {
        return section;
    }

    TVector<TYtPath> updatedPaths;
    for (size_t p = 0; p < section.Paths().Size(); ++p) {
        auto path = section.Paths().Item(p);

        TYtColumnsInfo columnsInfo(path.Columns());
        columnsInfo.SetRenames(renames);

        updatedPaths.push_back(Build<TYtPath>(ctx, path.Pos())
            .InitFrom(path)
            .Columns(columnsInfo.ToExprNode(ctx, path.Columns().Pos()))
            .Done()
        );
    }

    return Build<TYtSection>(ctx, section.Pos())
        .InitFrom(section)
        .Paths()
            .Add(updatedPaths)
        .Build()
        .Done();
}

TVector<TString> KeysApplyInputRenames(const TVector<TString>& keys, const THashMap<TString, TString>& renames) {
    auto result = keys;
    for (auto& i : result) {
        auto r = renames.find(i);
        if (r != renames.end()) {
            i = r->second;
        }
    }
    return result;
}

TExprNode::TPtr BuildReverseRenamingLambda(TPositionHandle pos, const THashMap<TString, TString>& renames, TExprContext& ctx) {
    auto arg = ctx.NewArgument(pos, "item");
    auto body = arg;

    for (auto& r : renames) {
        auto from = r.second;
        auto to = r.first;
        body = ctx.Builder(body->Pos())
            .Callable("RemoveMember")
                .Callable(0, "AddMember")
                    .Add(0, body)
                    .Atom(1, to)
                    .Callable(2, "Member")
                        .Add(0, body)
                        .Atom(1, from)
                    .Seal()
                .Seal()
                .Atom(1, from)
            .Seal()
            .Build();
    }

    return ctx.NewLambda(pos, ctx.NewArguments(pos, { arg }), std::move(body));
}

TMaybeNode<TCoLambda> GetPremapLambda(const TYtJoinNodeLeaf& leaf) {
    return leaf.Premap;
}

ETypeAnnotationKind DeriveCommonSequenceKind(ETypeAnnotationKind one, ETypeAnnotationKind two) {
    if (one == ETypeAnnotationKind::Stream || two == ETypeAnnotationKind::Stream) {
        return ETypeAnnotationKind::Stream;
    }

    if (one == ETypeAnnotationKind::List || two == ETypeAnnotationKind::List) {
        return ETypeAnnotationKind::List;
    }

    return ETypeAnnotationKind::Optional;
}

void ApplyInputPremap(TExprNode::TPtr& lambda, const TMaybeNode<TCoLambda>& premap, ETypeAnnotationKind commonKind,
                      const THashMap<TString, TString>& renames, TExprContext& ctx)
{
    TStringBuf converter;
    if (commonKind == ETypeAnnotationKind::Stream) {
        converter = "ToStream";
    } else if (commonKind == ETypeAnnotationKind::List) {
        converter = "ToList";
    } else if (commonKind == ETypeAnnotationKind::Optional) {
        converter = "ToSequence";
    } else {
        YQL_ENSURE(false, "unexpected common kind");
    }

    auto reverseRenamingLambda = BuildReverseRenamingLambda(lambda->Pos(), renames, ctx);

    if (premap) {
        lambda = ctx.Builder(lambda->Pos())
            .Lambda()
                .Param("item")
                .Callable(converter)
                    .Callable(0, "FlatMap")
                        .Apply(0, premap.Cast().Ptr())
                            .With(0)
                                .Apply(reverseRenamingLambda)
                                    .With(0, "item")
                                .Seal()
                            .Done()
                        .Seal()
                        .Add(1, lambda)
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    } else {
        lambda = ctx.Builder(lambda->Pos())
            .Lambda()
                .Param("item")
                .Callable(converter)
                    .Apply(0, lambda)
                        .With(0)
                            .Apply(reverseRenamingLambda)
                                .With(0, "item")
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }
}

void ApplyInputPremap(TExprNode::TPtr& lambda, const TYtJoinNodeLeaf& leaf,
                      const TYtJoinNodeLeaf& otherLeaf, TExprContext& ctx,
                      const THashMap<TString, TString>& renames = THashMap<TString, TString>())
{
    auto premap = GetPremapLambda(leaf);
    auto otherPremap = GetPremapLambda(otherLeaf);

    auto commonKind = ETypeAnnotationKind::Optional;
    if (premap) {
        YQL_ENSURE(EnsureSeqOrOptionalType(premap.Cast().Ref(), ctx));
        commonKind = DeriveCommonSequenceKind(commonKind, premap.Cast().Ref().GetTypeAnn()->GetKind());
    }
    if (otherPremap) {
        YQL_ENSURE(EnsureSeqOrOptionalType(otherPremap.Cast().Ref(), ctx));
        commonKind = DeriveCommonSequenceKind(commonKind, otherPremap.Cast().Ref().GetTypeAnn()->GetKind());
    }

    ApplyInputPremap(lambda, premap, commonKind, renames, ctx);
}



TString RenamesToString(const THashMap<TString, TString>& renames) {
    TVector<TString> renamesStr;
    renamesStr.reserve(renames.size());

    for (auto& r : renames) {
        renamesStr.emplace_back(r.first + " -> " + r.second);
    }

    return JoinSeq(", ", renamesStr);
}

struct TEquivKeys {
    TSet<TString> Keys;
    const TTypeAnnotationNode* Type = nullptr;
};

TTypeAnnotationNode::TListType GetTypes(const TVector<TEquivKeys>& input) {
    TTypeAnnotationNode::TListType result;
    result.reserve(input.size());
    for (auto& i : input) {
        result.push_back(i.Type);
    }
    return result;
}

void FlattenKeys(TVector<TString>& keys, TTypeAnnotationNode::TListType& types, const TVector<TEquivKeys>& input) {
    for (auto& i : input) {
        auto type = i.Type;
        for (auto& key : i.Keys) {
            keys.push_back(key);
            types.push_back(type);
        }
    }
}

void LogOutputSideSort(const TVector<TEquivKeys>& outSort, bool isLeft) {
    TVector<TString> strs;
    for (auto& s : outSort) {
        strs.push_back(TString::Join("{", JoinSeq(", ", s.Keys), "}"));
    }
    YQL_CLOG(INFO, ProviderYt) << "Output sort for " << (isLeft ? "left" : "right")
                               << " side: [" << JoinSeq(", ", strs) << "]";
}

void BuildOutputSideSort(TVector<TEquivKeys>& outSort, bool isLeft, TStringBuf joinType,
    const TStructExprType& outType, const TMap<TStringBuf, TVector<TStringBuf>>& renameMap,
    const TJoinLabel& label, const TVector<TString>& inputSort)
{
    YQL_ENSURE(joinType != "Cross");

    if (joinType == "Exclusion" || joinType == "Full") {
        // output is not sorted for either side
        return;
    }

    if (isLeft && (joinType == "Right" || joinType == "RightOnly" || joinType == "RightSemi")) {
        // no data or not sorted
        return;
    }

    if (!isLeft && (joinType == "Left" || joinType == "LeftOnly" || joinType == "LeftSemi")) {
        // no data or not sorted
        return;
    }

    for (auto& key : inputSort) {
        auto name = label.FullName(key);

        TVector<TStringBuf> newNames;
        newNames.push_back(name);
        if (auto renamed = renameMap.FindPtr(name)) {
            newNames = *renamed;
        }

        if (newNames.empty()) {
            // stop on first deleted sort key
            return;
        }

        TEquivKeys keys;

        for (auto n : newNames) {
            auto maybeIdx = outType.FindItem(n);
            if (!maybeIdx.Defined()) {
                return;
            }

            bool inserted = keys.Keys.insert(ToString(n)).second;
            YQL_ENSURE(inserted, "Duplicate key: " << n);

            if (!keys.Type) {
                keys.Type = outType.GetItems()[*maybeIdx]->GetItemType();
            } else {
                YQL_ENSURE(keys.Type == outType.GetItems()[*maybeIdx]->GetItemType());
            }
        }

        outSort.emplace_back(std::move(keys));
    }
}

TVector<TString> MatchSort(const THashSet<TString>& desiredKeys, const TVector<TEquivKeys>& sideSort) {
    TVector<TString> result;

    THashSet<TString> matchedKeys;
    for (auto& k : sideSort) {
        // only single alternative
        YQL_ENSURE(k.Keys.size() == 1);
        auto key = *k.Keys.begin();
        if (desiredKeys.contains(key)) {
            matchedKeys.insert(key);
            result.push_back(key);
        } else {
            break;
        }
    }

    if (matchedKeys.size() != desiredKeys.size()) {
        result.clear();
    }

    return result;
}

TVector<TStringBuf> MatchSort(TTypeAnnotationNode::TListType& types, const TVector<TStringBuf>& desiredSort, const TVector<TEquivKeys>& sideSort) {
    TVector<TStringBuf> result;
    types.clear();
    for (size_t i = 0, j = 0; i < desiredSort.size() && j < sideSort.size(); ++i) {
        auto key = desiredSort[i];
        if (sideSort[j].Keys.contains(key) ||
            (j + 1 < sideSort.size() && sideSort[++j].Keys.contains(key)))
        {
            result.push_back(key);
            types.push_back(sideSort[j].Type);
        } else {
            break;
        }
    }

    if (result.size() != desiredSort.size()) {
        result.clear();
        types.clear();
    }

    return result;
}

size_t GetSortWeight(TMap<TVector<TStringBuf>, size_t>& weights, const TVector<TStringBuf>& sort, const TSet<TVector<TStringBuf>>& sorts) {
    if (sort.empty() || !sorts.contains(sort)) {
        return 0;
    }

    if (auto w = weights.FindPtr(sort)) {
        return *w;
    }

    size_t weight = 1;
    auto currSort = sort;
    for (size_t i = currSort.size() - 1; i > 0; --i) {
        currSort.resize(i);
        auto currWeight = GetSortWeight(weights, currSort, sorts);
        if (currWeight) {
            weight += currWeight;
            break;
        }
    }

    weights[sort] = weight;
    return weight;
}


void BuildOutputSort(const TYtOutTableInfo& outTableInfo, const TYtJoinNodeOp& op, TStringBuf joinType, bool setTopLevelFullSort,
    const TStructExprType& outItemType, const TMap<TStringBuf, TVector<TStringBuf>>& renameMap,
    const TSet<TVector<TStringBuf>>& topLevelSorts,
    const TVector<TString>& leftSortedKeys, const TVector<TString>& rightSortedKeys,
    const TJoinLabel& leftLabel, const TJoinLabel& rightLabel)
{
    {
        TVector<TString> strs;
        for (auto& r : renameMap) {
            strs.push_back(TString::Join(r.first,  " -> {", JoinSeq(", ", r.second), "}"));
        }
        YQL_CLOG(INFO, ProviderYt) << "Join renames: [" << JoinSeq(", ", strs) << "]";
    }

    {
        TVector<TString> strs;
        for (auto& s: topLevelSorts) {
            strs.push_back(TString::Join("{", JoinSeq(", ", s), "}"));
        }
        YQL_CLOG(INFO, ProviderYt) << "Top level sorts: [" << JoinSeq(", ", strs) << "]";
    }

    YQL_CLOG(INFO, ProviderYt) << "Output Type: " << static_cast<const TTypeAnnotationNode&>(outItemType);

    YQL_CLOG(INFO, ProviderYt) << "Deriving output sort order for " << (op.Parent ? "intermediate " : "final ")
                               << joinType << " join. Input left side sort: [" << JoinSeq(", ", leftSortedKeys)
                               << "], input right side sort: [" << JoinSeq(", ", rightSortedKeys) << "]";

    TVector<TEquivKeys> outLeftSorted;
    BuildOutputSideSort(outLeftSorted, true, joinType, outItemType, renameMap, leftLabel, leftSortedKeys);
    LogOutputSideSort(outLeftSorted, true);

    TVector<TEquivKeys> outRightSorted;
    BuildOutputSideSort(outRightSorted, false, joinType, outItemType, renameMap, rightLabel, rightSortedKeys);
    LogOutputSideSort(outRightSorted, false);

    TVector<TString> outSorted;
    TTypeAnnotationNode::TListType outSortTypes;

    if (op.Parent) {
        if (op.Parent->JoinKind->Content() != "Cross") {
            THashSet<TString> desiredOutputSortKeys;
            YQL_ENSURE(&op == op.Parent->Left.Get() || &op == op.Parent->Right.Get());
            auto parentLabel = (&op == op.Parent->Left.Get()) ? op.Parent->LeftLabel : op.Parent->RightLabel;
            for (ui32 i = 0; i < parentLabel->ChildrenSize(); i += 2) {
                auto table  = parentLabel->Child(i)->Content();
                auto column = parentLabel->Child(i + 1)->Content();
                desiredOutputSortKeys.insert(FullColumnName(table, column));
            }

            YQL_ENSURE(!desiredOutputSortKeys.empty());

            YQL_CLOG(INFO, ProviderYt) << "Desired output sort keys for next join: [" << JoinSeq(", ", desiredOutputSortKeys) << "]";

            THashSet<TString> matchedSortKeys;

            auto leftMatch = MatchSort(desiredOutputSortKeys, outLeftSorted);
            auto rightMatch = MatchSort(desiredOutputSortKeys, outRightSorted);

            // choose longest sort
            if (leftMatch.size() > rightMatch.size() || (leftMatch.size() == rightMatch.size() && leftMatch <= rightMatch)) {
                outSorted = leftMatch;
                outSortTypes = GetTypes(outLeftSorted);
            } else {
                outSorted = rightMatch;
                outSortTypes = GetTypes(outRightSorted);
            }
        }
    } else if (setTopLevelFullSort) {
        FlattenKeys(outSorted, outSortTypes, outLeftSorted);
        FlattenKeys(outSorted, outSortTypes, outRightSorted);
    } else {
        TMap<TVector<TStringBuf>, size_t> weights;

        size_t bestWeight = 0;
        TVector<TStringBuf> bestSort;

        for (auto& requestedSort : topLevelSorts) {
            TTypeAnnotationNode::TListType leftMatchTypes;
            auto leftMatch = MatchSort(leftMatchTypes, requestedSort, outLeftSorted);
            auto weight = GetSortWeight(weights, leftMatch, topLevelSorts);
            if (weight > bestWeight) {
                bestWeight = weight;
                bestSort = leftMatch;
                outSortTypes = leftMatchTypes;
            }

            TTypeAnnotationNode::TListType rightMatchTypes;
            auto rightMatch = MatchSort(rightMatchTypes, requestedSort, outRightSorted);
            weight = GetSortWeight(weights, rightMatch, topLevelSorts);
            if (weight > bestWeight) {
                bestWeight = weight;
                bestSort = rightMatch;
                outSortTypes = rightMatchTypes;
            }
        }

        outSorted.clear();
        outSorted.reserve(bestSort.size());
        for (auto& k : bestSort) {
            outSorted.push_back(ToString(k));
        }
    }

    YQL_ENSURE(outSortTypes.size() >= outSorted.size());
    outSortTypes.resize(outSorted.size());
    if (!outSorted.empty()) {
        outTableInfo.RowSpec->SortMembers = outSorted;
        outTableInfo.RowSpec->SortedBy = outSorted;
        outTableInfo.RowSpec->SortedByTypes = outSortTypes;
        outTableInfo.RowSpec->SortDirections = TVector<bool>(outSorted.size(), true);
    }

    YQL_CLOG(INFO, ProviderYt) << "Resulting output sort: [" << JoinSeq(", ", outTableInfo.RowSpec->SortedBy) << "]";
}

const TStructExprType* MakeIntermediateEquiJoinTableType(TPositionHandle pos, const TExprNode& joinTree,
    const TJoinLabels& labels, const THashSet<TString>& outputRemoveColumns, TExprContext& ctx)
{
    auto reduceColumnTypes = GetJoinColumnTypes(joinTree, labels, ctx);
    TVector<const TItemExprType*> structItems;
    for (auto& x : reduceColumnTypes) {
        if (!outputRemoveColumns.contains(x.first)) {
            structItems.push_back(ctx.MakeType<TItemExprType>(x.first, x.second));
        }
    }

    auto reduceResultType = ctx.MakeType<TStructExprType>(structItems);
    if (!reduceResultType->Validate(pos, ctx)) {
        return nullptr;
    }

    return reduceResultType;
}

void AddAnyJoinOptionsToCommonJoinCore(TExprNode::TListType& options, bool swapTables, const TEquiJoinLinkSettings& linkSettings, TPositionHandle pos, TExprContext& ctx) {
    TExprNode::TListType anySettings;

    const auto& leftHints  = swapTables ? linkSettings.RightHints : linkSettings.LeftHints;
    const auto& rightHints = swapTables ? linkSettings.LeftHints : linkSettings.RightHints;

    if (leftHints.contains("any")) {
        anySettings.push_back(ctx.NewAtom(pos, "left", TNodeFlags::Default));
    }

    if (rightHints.contains("any")) {
        anySettings.push_back(ctx.NewAtom(pos, "right", TNodeFlags::Default));
    }

    if (!anySettings.empty()) {
        options.push_back(
            ctx.Builder(pos)
                .List()
                    .Atom(0, "any", TNodeFlags::Default)
                    .Add(1, ctx.NewList(pos, std::move(anySettings)))
                .Seal()
                .Build());
    }
}

TExprNode::TPtr BuildYtReduceLambda(TPositionHandle pos, const TExprNode::TPtr& groupArg, TExprNode::TPtr&& flatMapLambdaBody, const bool sysColumns, TExprContext& ctx)
{
    TExprNode::TPtr chopperHandler = ctx.NewLambda(pos, ctx.NewArguments(pos, {ctx.NewArgument(pos, "stup"), groupArg }), std::move(flatMapLambdaBody));
    TExprNode::TPtr chopperSwitch;

    if (sysColumns) {
        chopperSwitch = ctx.Builder(pos)
            .Lambda()
                .Param("key")
                .Param("item")
                .Callable("SqlExtractKey")
                    .Arg(0, "item")
                    .Lambda(1)
                        .Param("row")
                        .Callable("Member")
                            .Arg(0, "row")
                            .Atom(1, YqlSysColumnKeySwitch, TNodeFlags::Default)
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        chopperHandler = ctx.Builder(pos)
            .Lambda()
                .Param("key")
                .Param("group")
                .Apply(chopperHandler)
                    .With(0, "key")
                    .With(1)
                        .Callable("RemovePrefixMembers")
                            .Arg(0, "group")
                            .List(1)
                                .Atom(0, YqlSysColumnKeySwitch, TNodeFlags::Default)
                            .Seal()
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
            .Build();
    }
    else {
        chopperSwitch = ctx.Builder(pos)
            .Lambda()
                .Param("key")
                .Param("item")
                .Callable("YtIsKeySwitch")
                    .Callable(0, "DependsOn")
                        .Arg(0, "item")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    return ctx.Builder(pos)
        .Lambda()
            .Param("flow")
            .Callable("Chopper")
                .Arg(0, "flow")
                .Lambda(1)
                    .Param("item")
                    .Callable("Uint64") // Fake const key
                        .Atom(0, "0", TNodeFlags::Default)
                    .Seal()
                .Seal()
                .Add(2, chopperSwitch)
                .Add(3, chopperHandler)
            .Seal()
        .Seal()
        .Build();
}

struct TMergeJoinSortInfo {
    TChoice AdditionalSort = TChoice::None;
    TChoice NeedRemapBeforeSort = TChoice::None;

    const TStructExprType* LeftBeforePremap = nullptr;
    const TStructExprType* RightBeforePremap = nullptr;

    TVector<TString> LeftSortedKeys;
    TVector<TString> RightSortedKeys;

    TVector<TString> CommonSortedKeys;

    THashMap<TString, TString> LeftKeyRenames;
    THashMap<TString, TString> RightKeyRenames;
};

TMergeJoinSortInfo Invert(const TMergeJoinSortInfo& info) {
    auto result = info;
    result.AdditionalSort = Invert(result.AdditionalSort);
    result.NeedRemapBeforeSort = Invert(result.NeedRemapBeforeSort);
    std::swap(result.LeftBeforePremap, result.RightBeforePremap);
    std::swap(result.LeftSortedKeys, result.RightSortedKeys);
    std::swap(result.LeftKeyRenames, result.RightKeyRenames);
    return result;
}

TYtSection SectionApplyAdditionalSort(const TYtSection& section, const TYtEquiJoin& equiJoin, const TVector<TString>& sortTableOrder, const TStructExprType* sortTableType,
    bool needRemapBeforeSort, const TYtState& state, TExprContext& ctx)
{
    auto pos = equiJoin.Pos();
    auto inputWorld = equiJoin.World();
    auto inputSection = section;

    TTypeAnnotationNode::TListType sortedByTypes;
    for (auto& column: sortTableOrder) {
        auto ndx = sortTableType->FindItem(column);
        YQL_ENSURE(ndx.Defined(), "Missing column " << column);
        sortedByTypes.push_back(sortTableType->GetItems()[*ndx]->GetItemType());
    }

    TVector<bool> sortDirections(sortTableOrder.size(), true);
    ui64 nativeTypeFlags = state.Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE;
    TMaybe<NYT::TNode> nativeType;

    if (needRemapBeforeSort) {
        inputSection = Build<TYtSection>(ctx, pos)
            .Paths()
                .Add()
                    .Table<TYtOutput>()
                        .Operation<TYtMap>()
                            .World(inputWorld)
                            .DataSink(equiJoin.DataSink())
                            .Input()
                                .Add(inputSection)
                            .Build()
                            .Output()
                                .Add(TYtOutTableInfo(sortTableType, state.Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE)
                                     .ToExprNode(ctx, pos).Cast<TYtOutTable>())
                            .Build()
                            .Settings(GetFlowSettings(pos, state, ctx))
                            .Mapper()
                                .Args({"list"})
                                .Body("list")
                            .Build()
                        .Build()
                        .OutIndex().Value("0").Build()
                    .Build()
                    .Columns<TCoVoid>().Build()
                    .Ranges<TCoVoid>().Build()
                    .Stat<TCoVoid>().Build()
                .Build()
            .Build()
            .Settings().Build()
            .Done();

        inputWorld = Build<TCoWorld>(ctx, pos).Done();
    } else {
        auto inputRowSpec = TYtTableBaseInfo::GetRowSpec(section.Paths().Item(0).Table());
        // Use types from first input only, because all of them shoud be equal (otherwise remap is required)
        nativeTypeFlags = inputRowSpec->GetNativeYtTypeFlags();
        nativeType = inputRowSpec->GetNativeYtType();
    }

    TYtOutTableInfo sortOut(sortTableType, nativeTypeFlags);
    sortOut.RowSpec->SortMembers = sortTableOrder;
    sortOut.RowSpec->SortedBy = sortTableOrder;
    sortOut.RowSpec->SortedByTypes = sortedByTypes;
    sortOut.RowSpec->SortDirections = sortDirections;

    if (nativeType) {
        sortOut.RowSpec->CopyTypeOrders(*nativeType);
    }

    return Build<TYtSection>(ctx, pos)
        .Paths()
            .Add()
                .Table<TYtOutput>()
                    .Operation<TYtSort>()
                        .World(inputWorld)
                        .DataSink(equiJoin.DataSink())
                        .Input()
                            .Add(inputSection)
                        .Build()
                        .Output()
                            .Add(sortOut.ToExprNode(ctx, pos).Cast<TYtOutTable>())
                        .Build()
                        .Settings().Build()
                    .Build()
                    .OutIndex().Value("0").Build()
                .Build()
                .Columns<TCoVoid>().Build()
                .Ranges<TCoVoid>().Build()
                .Stat<TCoVoid>().Build()
            .Build()
        .Build()
        .Settings().Build()
        .Done();
}


bool RewriteYtMergeJoin(TYtEquiJoin equiJoin, const TJoinLabels& labels, TYtJoinNodeOp& op,
    const TYtJoinNodeLeaf& leftLeaf, const TYtJoinNodeLeaf& rightLeaf, const TYtState::TPtr& state, TExprContext& ctx,
    bool swapTables, bool joinReduce, bool tryFirstAsPrimary, bool joinReduceForSecond,
    const TMergeJoinSortInfo sortInfo, bool& skipped)
{
    skipped = false;
    auto pos = equiJoin.Pos();

    auto leftKeyColumns = op.LeftLabel;
    auto rightKeyColumns = op.RightLabel;
    if (swapTables) {
        DoSwap(leftKeyColumns, rightKeyColumns);
    }

    auto joinType = op.JoinKind;
    if (swapTables) {
        SwapJoinType(pos, joinType, ctx);
    }

    auto& mainLabel = labels.Inputs[swapTables ? 1 : 0];
    auto& smallLabel = labels.Inputs[swapTables ? 0 : 1];

    auto joinTree = ctx.NewList(pos, {
        joinType,
        ctx.NewAtom(pos, leftLeaf.Scope[0]),
        ctx.NewAtom(pos, rightLeaf.Scope[0]),
        leftKeyColumns,
        rightKeyColumns,
        ctx.NewList(pos, {})
    });

    auto columnTypes = GetJoinColumnTypes(*joinTree, labels, "Inner", ctx);
    auto finalColumnTypes = GetJoinColumnTypes(*joinTree, labels, ctx);

    auto outputLeftSchemeType = MakeOutputJoinColumns(columnTypes, mainLabel, ctx);
    auto outputRightSchemeType = MakeOutputJoinColumns(columnTypes, smallLabel, ctx);

    auto inputKeyTypeLeft = BuildJoinKeyType(mainLabel, *leftKeyColumns);
    auto inputKeyTypeRight = BuildJoinKeyType(smallLabel, *rightKeyColumns);
    auto filteredKeyTypeLeft = AsDictKeyType(RemoveNullsFromJoinKeyType(inputKeyTypeLeft), ctx);
    auto filteredKeyTypeRight = AsDictKeyType(RemoveNullsFromJoinKeyType(inputKeyTypeRight), ctx);
    if (!IsSameAnnotation(*filteredKeyTypeLeft, *filteredKeyTypeRight)) {
        YQL_CLOG(INFO, ProviderYt) << "Mismatch key types, left: " << *AsDictKeyType(inputKeyTypeLeft, ctx) << ", right: " << *AsDictKeyType(inputKeyTypeRight, ctx);
        skipped = true;
        return true;
    }

    auto outputKeyType = UnifyJoinKeyType(pos, inputKeyTypeLeft, inputKeyTypeRight, ctx);

    TExprNode::TListType leftMembersNodes;
    TExprNode::TListType rightMembersNodes;
    TExprNode::TListType requiredMembersNodes;
    bool hasData[2] = { false, false };
    TMap<TStringBuf, TVector<TStringBuf>> renameMap;
    TSet<TVector<TStringBuf>> topLevelSorts;
    if (!op.Parent) {
        renameMap = LoadJoinRenameMap(equiJoin.JoinOptions().Ref());
        topLevelSorts = LoadJoinSortSets(equiJoin.JoinOptions().Ref());
    }

    if (joinType->Content() != "RightSemi" && joinType->Content() != "RightOnly") {
        hasData[0] = true;
        for (auto x : outputLeftSchemeType->GetItems()) {
            auto name = ctx.NewAtom(pos, x->GetName());
            if (auto renamed = renameMap.FindPtr(x->GetName())) {
                if (renamed->empty()) {
                    continue;
                }
            }

            auto finalColumnType = finalColumnTypes[x->GetName()];
            if (!finalColumnType->IsOptionalOrNull()) {
                requiredMembersNodes.push_back(name);
            }

            leftMembersNodes.push_back(name);
        }
    }

    if (joinType->Content() != "LeftSemi" && joinType->Content() != "LeftOnly") {
        hasData[1] = true;
        for (auto x : outputRightSchemeType->GetItems()) {
            auto name = ctx.NewAtom(pos, x->GetName());
            if (auto renamed = renameMap.FindPtr(x->GetName())) {
                if (renamed->empty()) {
                    continue;
                }
            }

            auto finalColumnType = finalColumnTypes[x->GetName()];
            if (!finalColumnType->IsOptionalOrNull()) {
                requiredMembersNodes.push_back(name);
            }

            rightMembersNodes.push_back(name);
        }
    }

    TCommonJoinCoreLambdas cjcLambdas[2];
    TVector<TString> keys = KeysApplyInputRenames(BuildJoinKeyList(mainLabel, *leftKeyColumns), sortInfo.LeftKeyRenames);
    for (ui32 index = 0; index < 2; ++index) {
        auto& label =      (index == 0) ? mainLabel : smallLabel;

        auto keyColumnsNode = (index == 0) ? leftKeyColumns : rightKeyColumns;

        auto myOutputSchemeType =    (index == 0) ? outputLeftSchemeType : outputRightSchemeType;
        auto otherOutputSchemeType = (index == 1) ? outputLeftSchemeType : outputRightSchemeType;

        cjcLambdas[index] = MakeCommonJoinCoreReduceLambda(pos, ctx, label, outputKeyType, *keyColumnsNode,
            joinType->Content(), myOutputSchemeType, otherOutputSchemeType, index, false,
            0, renameMap, hasData[index], hasData[1 - index], keys);

        auto& leaf =       (index == 0) ? leftLeaf : rightLeaf;
        auto& otherLeaf  = (index == 1) ? leftLeaf : rightLeaf;
        auto& keyRenames = (index == 0) ? sortInfo.LeftKeyRenames : sortInfo.RightKeyRenames;
        ApplyInputPremap(cjcLambdas[index].ReduceLambda, leaf, otherLeaf, ctx, keyRenames);
    }
    YQL_ENSURE(cjcLambdas[0].CommonJoinCoreInputType == cjcLambdas[1].CommonJoinCoreInputType, "Must be same type from both side of join.");

    auto groupArg = ctx.NewArgument(pos, "group");
    auto convertedList = ctx.Builder(pos)
        .Callable("FlatMap")
            .Add(0, groupArg)
            .Lambda(1)
                .Param("item")
                .Callable("Visit")
                    .Arg(0, "item")
                    .Atom(1, 0U)
                    .Add(2, cjcLambdas[0].ReduceLambda)
                    .Atom(3, 1U)
                    .Add(4, cjcLambdas[1].ReduceLambda)
                .Seal()
            .Seal()
        .Seal()
        .Build();

    TExprNode::TListType optionNodes;
    AddAnyJoinOptionsToCommonJoinCore(optionNodes, swapTables, op.LinkSettings, pos, ctx);
    if (auto memLimit = state->Configuration->CommonJoinCoreLimit.Get()) {
        optionNodes.push_back(ctx.Builder(pos)
            .List()
                .Atom(0, "memLimit", TNodeFlags::Default)
                .Atom(1, ToString(*memLimit), TNodeFlags::Default)
            .Seal()
            .Build());
    }
    optionNodes.push_back(ctx.Builder(pos)
        .List()
            .Atom(0, "sorted", TNodeFlags::Default)
            .Atom(1, "left", TNodeFlags::Default)
        .Seal()
        .Build());

    TExprNode::TListType keyMembersNodes;
    YQL_ENSURE(sortInfo.CommonSortedKeys.size() == outputKeyType.size());
    for (auto& x : sortInfo.CommonSortedKeys) {
        keyMembersNodes.push_back(ctx.NewAtom(pos, x));
    }

    auto joinedRawStream = ctx.NewCallable(pos, "CommonJoinCore", { convertedList, joinType,
        ctx.NewList(pos, std::move(leftMembersNodes)), ctx.NewList(pos, std::move(rightMembersNodes)),
        ctx.NewList(pos, std::move(requiredMembersNodes)), ctx.NewList(pos, std::move(keyMembersNodes)),
        ctx.NewList(pos, std::move(optionNodes)), ctx.NewAtom(pos, "_yql_table_index", TNodeFlags::Default) });
    auto joinedRaw = joinedRawStream;

    const TStructExprType* outItemType = nullptr;
    if (!op.Parent) {
        if (auto type = GetSequenceItemType(equiJoin.Pos(),
                                            equiJoin.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1],
                                            false, ctx)) {
            outItemType = type->Cast<TStructExprType>();
        } else {
            return false;
        }
    } else {
        outItemType = MakeIntermediateEquiJoinTableType(pos, *joinTree, labels, op.OutputRemoveColumns, ctx);
        if (!outItemType) {
            return false;
        }
    }

    auto joined = ctx.Builder(pos)
        .Callable("Map")
            .Add(0, joinedRaw)
            .Add(1, BuildJoinRenameLambda(pos, renameMap, *outItemType, ctx).Ptr())
        .Seal()
        .Build();

    TYtOutTableInfo outTableInfo(outItemType, state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
    outTableInfo.RowSpec->SetConstraints(op.Constraints);
    outTableInfo.SetUnique(op.Constraints.GetConstraint<TDistinctConstraintNode>(), pos, ctx);
    const bool setTopLevelFullSort = state->Configuration->JoinMergeSetTopLevelFullSort.Get().GetOrElse(false);

    BuildOutputSort(outTableInfo, op, joinType->Content(), setTopLevelFullSort, *outItemType, renameMap, topLevelSorts,
                    sortInfo.LeftSortedKeys, sortInfo.RightSortedKeys, mainLabel, smallLabel);

    auto reduceWorld = equiJoin.World();
    auto leftSection = Build<TYtSection>(ctx, leftLeaf.Section.Pos())
        .InitFrom(leftLeaf.Section)
        .Settings(NYql::RemoveSettings(leftLeaf.Section.Settings().Ref(), EYtSettingType::JoinLabel | EYtSettingType::StatColumns, ctx))
        .Done();
    auto rightSection = Build<TYtSection>(ctx, rightLeaf.Section.Pos())
        .InitFrom(rightLeaf.Section)
        .Settings(NYql::RemoveSettings(rightLeaf.Section.Settings().Ref(), EYtSettingType::JoinLabel | EYtSettingType::StatColumns, ctx))
        .Done();


    if (sortInfo.AdditionalSort != TChoice::None) {
        if (sortInfo.AdditionalSort == TChoice::Left || sortInfo.AdditionalSort == TChoice::Both) {
            bool needRemapBeforeSort = sortInfo.NeedRemapBeforeSort == TChoice::Left || sortInfo.NeedRemapBeforeSort == TChoice::Both;
            leftSection = SectionApplyAdditionalSort(leftSection, equiJoin, sortInfo.LeftSortedKeys, sortInfo.LeftBeforePremap, needRemapBeforeSort, *state, ctx);
        }

        if (sortInfo.AdditionalSort == TChoice::Right || sortInfo.AdditionalSort == TChoice::Both) {
            bool needRemapBeforeSort = sortInfo.NeedRemapBeforeSort == TChoice::Right || sortInfo.NeedRemapBeforeSort == TChoice::Both;
            rightSection = SectionApplyAdditionalSort(rightSection, equiJoin, sortInfo.RightSortedKeys, sortInfo.RightBeforePremap, needRemapBeforeSort, *state, ctx);
        }

        reduceWorld = Build<TCoWorld>(ctx, pos).Done();
    }

    leftSection = SectionApplyRenames(leftSection, sortInfo.LeftKeyRenames, ctx);
    rightSection = SectionApplyRenames(rightSection, sortInfo.RightKeyRenames, ctx);

    auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, pos)
        .Add()
            .Name()
                .Value(ToString(EYtSettingType::ReduceBy), TNodeFlags::Default)
            .Build()
            .Value(ToAtomList(sortInfo.CommonSortedKeys, pos, ctx))
        .Build();

    if (joinReduce) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::JoinReduce), TNodeFlags::Default)
                .Build()
            .Build();
    }

    if (tryFirstAsPrimary) {
        const ui64 maxJobSize = state->Configuration->JoinMergeReduceJobMaxSize.Get().GetOrElse(8_GB);
        auto subSettingsBuilder = Build<TCoNameValueTupleList>(ctx, pos)
            .Add()
                .Name()
                    .Value(MaxJobSizeForFirstAsPrimaryName, TNodeFlags::Default)
                .Build()
                .Value<TCoAtom>()
                    .Value(ToString(maxJobSize), TNodeFlags::Default)
                .Build()
            .Build();
        if (joinReduceForSecond) {
            subSettingsBuilder
                .Add()
                    .Name()
                        .Value(JoinReduceForSecondAsPrimaryName, TNodeFlags::Default)
                    .Build()
                .Build();
        }

        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::FirstAsPrimary), TNodeFlags::Default)
                .Build()
                .Value(subSettingsBuilder.Done())
            .Build();
    }

    if (state->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::Flow), TNodeFlags::Default)
                .Build()
            .Build();
    }

    const auto useSystemColumns = state->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS);
    if (useSystemColumns) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::KeySwitch), TNodeFlags::Default)
                .Build()
            .Build();
    }

    op.Output = Build<TYtReduce>(ctx, pos)
        .World(reduceWorld)
        .DataSink(equiJoin.DataSink())
        .Input()
            .Add(leftSection)
            .Add(rightSection)
        .Build()
        .Output()
            .Add(outTableInfo.ToExprNode(ctx, pos).Cast<TYtOutTable>())
        .Build()
        .Settings(settingsBuilder.Done())
        .Reducer(BuildYtReduceLambda(pos, groupArg, std::move(joined), useSystemColumns, ctx))
        .Done();

    return true;
}

bool RewriteYtMapJoin(TYtEquiJoin equiJoin, const TJoinLabels& labels, bool isLookupJoin,
    TYtJoinNodeOp& op, const TYtJoinNodeLeaf& leftLeaf, const TYtJoinNodeLeaf& rightLeaf,
    TExprContext& ctx, const TMapJoinSettings& settings, bool useShards, const TYtState::TPtr& state)
{
    auto pos = equiJoin.Pos();
    auto joinType = op.JoinKind;
    if (settings.SwapTables) {
        SwapJoinType(pos, joinType, ctx);
    }

    auto& mainLabel = labels.Inputs[settings.SwapTables ? 1 : 0];
    auto& smallLabel = labels.Inputs[settings.SwapTables ? 0 : 1];

    TStringBuf strategyName = isLookupJoin ? "LookupJoin" : "MapJoin";

    auto const& leftHints  = settings.SwapTables ? op.LinkSettings.RightHints : op.LinkSettings.LeftHints;
    auto const& rightHints = settings.SwapTables ? op.LinkSettings.LeftHints : op.LinkSettings.RightHints;

    YQL_ENSURE(!leftHints.contains("any"));

    const bool isUniqueKey = rightHints.contains("unique") || settings.RightUnique || rightHints.contains("any");
    if (isUniqueKey) {
        YQL_CLOG(INFO, ProviderYt) << strategyName << " assumes unique keys for the small table";
    }

    ui64 partCount = 1;
    ui64 partRows = settings.RightRows;
    if ((settings.RightSize > 0) && useShards) {
        partCount = (settings.RightMemSize + settings.MapJoinLimit - 1) / settings.MapJoinLimit;
        partRows = (settings.RightRows + partCount - 1) / partCount;
    }

    if (partCount > 1) {
        YQL_ENSURE(!isLookupJoin);
        YQL_CLOG(INFO, ProviderYt) << strategyName << " sharded into " << partCount << " parts, each " << partRows << " rows";
    }

    auto leftKeyColumns = settings.SwapTables ? op.RightLabel : op.LeftLabel;
    auto rightKeyColumns = settings.SwapTables ? op.LeftLabel : op.RightLabel;
    auto joinTree = ctx.NewList(pos, {
        joinType,
        ctx.NewAtom(pos, leftLeaf.Scope[0]),
        ctx.NewAtom(pos, rightLeaf.Scope[0]),
        leftKeyColumns,
        rightKeyColumns,
        ctx.NewList(pos, {})
    });

    auto columnTypes = GetJoinColumnTypes(*joinTree, labels, ctx);
    auto outputLeftSchemeType = MakeOutputJoinColumns(columnTypes, mainLabel, ctx);
    auto outputRightSchemeType = MakeOutputJoinColumns(columnTypes, smallLabel, ctx);

    auto inputKeyTypeLeft = BuildJoinKeyType(mainLabel, *leftKeyColumns);
    auto inputKeyTypeRight = BuildJoinKeyType(smallLabel, *rightKeyColumns);
    auto outputKeyType = UnifyJoinKeyType(pos, inputKeyTypeLeft, inputKeyTypeRight, ctx);

    TMap<TStringBuf, TVector<TStringBuf>> renameMap;
    if (!op.Parent) {
        renameMap = LoadJoinRenameMap(equiJoin.JoinOptions().Ref());
    }

    const TStructExprType* outItemType = nullptr;
    if (!op.Parent) {
        if (auto type = GetSequenceItemType(equiJoin.Pos(),
                                            equiJoin.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1],
                                            false, ctx)) {
            outItemType = type->Cast<TStructExprType>();
        } else {
            return false;
        }
    } else {
        outItemType = MakeIntermediateEquiJoinTableType(pos, *joinTree, labels, op.OutputRemoveColumns, ctx);
        if (!outItemType) {
            return false;
        }
    }

    auto mainPaths = MakeUnorderedSection(leftLeaf.Section, ctx).Paths();
    auto mainSettings = NYql::RemoveSettings(leftLeaf.Section.Settings().Ref(), EYtSettingType::JoinLabel | EYtSettingType::StatColumns, ctx);
    auto smallPaths = MakeUnorderedSection(rightLeaf.Section, ctx).Paths();
    auto smallSettings = NYql::RemoveSettings(rightLeaf.Section.Settings().Ref(), EYtSettingType::JoinLabel | EYtSettingType::StatColumns, ctx);
    if (!NYql::HasSetting(*smallSettings, EYtSettingType::Unordered)) {
        smallSettings = NYql::AddSetting(*smallSettings, EYtSettingType::Unordered, {}, ctx);
    }
    auto smallKeyColumns = rightKeyColumns;

    TSyncMap syncList;
    for (auto path: smallPaths) {
        if (auto out = path.Table().Maybe<TYtOutput>()) {
            syncList.emplace(GetOutputOp(out.Cast()).Ptr(), syncList.size());
        }
    }
    auto mapWorld = ApplySyncListToWorld(equiJoin.World().Ptr(), syncList, ctx);

    auto mapSettingsBuilder = Build<TCoNameValueTupleList>(ctx, pos);
    if (partCount > 1) {
        mapSettingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::Sharded))
                .Build()
            .Build();
    }

    const bool isCross = joinType->IsAtom("Cross");

    auto tableContentSettings = ctx.NewList(pos, {});
    if (isCross) {
        ui64 rowFactor = (1 + smallLabel.InputType->GetSize()) * sizeof(NKikimr::NUdf::TUnboxedValuePod); // Table content after Collect
        rowFactor += (1 + smallLabel.InputType->GetSize() + mainLabel.InputType->GetSize()) * sizeof(NKikimr::NUdf::TUnboxedValuePod); // Table content after Map with added left side
        rowFactor += settings.LeftSize / settings.LeftRows; // Average added left side for each row after Map

        tableContentSettings = NYql::AddSetting(*tableContentSettings, EYtSettingType::RowFactor, ctx.NewAtom(pos, ToString(rowFactor), TNodeFlags::Default), ctx);
    }

    auto mapJoinUseFlow = state->Configuration->MapJoinUseFlow.Get().GetOrElse(DEFAULT_MAP_JOIN_USE_FLOW);

    TYtOutTableInfo outTableInfo(outItemType, state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
    outTableInfo.RowSpec->SetConstraints(op.Constraints);
    outTableInfo.SetUnique(op.Constraints.GetConstraint<TDistinctConstraintNode>(), pos, ctx);

    TVector<TYtMap> maps;
    for (ui64 partNo = 0; partNo < partCount; ++partNo) {
        auto listArg = ctx.NewArgument(pos, "list");

        ui64 dictItemsCount = settings.RightRows;
        auto readSettings = smallSettings;

        if (partCount > 1) {
            ui64 start = partRows * partNo;
            ui64 finish = Min(start + partRows, settings.RightRows);
            readSettings = NYql::AddSetting(*readSettings, EYtSettingType::Skip,
                ctx.Builder(pos)
                    .Callable("Uint64")
                        .Atom(0, ToString(start), TNodeFlags::Default)
                    .Seal()
                    .Build(), ctx);

            readSettings = NYql::AddSetting(*readSettings, EYtSettingType::Take,
                ctx.Builder(pos)
                    .Callable("Uint64")
                        .Atom(0, ToString(finish - start), TNodeFlags::Default)
                    .Seal()
                    .Build(), ctx);

            readSettings = NYql::AddSetting(*readSettings, EYtSettingType::Unordered, {}, ctx);

            dictItemsCount = finish - start;
        }

        auto tableContent = Build<TYtTableContent>(ctx, pos)
            .Input<TYtReadTable>()
                .World<TCoWorld>().Build()
                .DataSource(ctx.RenameNode(equiJoin.DataSink().Ref(), TYtDSource::CallableName()))
                .Input()
                    .Add()
                        .Paths(smallPaths)
                        .Settings(readSettings)
                    .Build()
                .Build()
            .Build()
            .Settings(tableContentSettings)
            .Done().Ptr();

        TExprNode::TPtr lookupJoinFilterLambda;
        if (isLookupJoin) {
            TExprNode::TPtr tableContentAsJoinKeysTupleList;
            YQL_ENSURE(leftKeyColumns->ChildrenSize() == rightKeyColumns->ChildrenSize());

            TVector<TString> mainJoinMembers;
            TVector<TString> smallJoinMembers;

            for (ui32 i = 0; i < leftKeyColumns->ChildrenSize(); i += 2) {
                TString mainMemberName = mainLabel.MemberName(leftKeyColumns->Child(i)->Content(),
                                                              leftKeyColumns->Child(i + 1)->Content());

                TString smallMemberName = smallLabel.MemberName(rightKeyColumns->Child(i)->Content(),
                                                                rightKeyColumns->Child(i + 1)->Content());

                mainJoinMembers.push_back(mainMemberName);
                smallJoinMembers.push_back(smallMemberName);
            }

            auto status = SubstTables(tableContent, state, false, ctx);
            if (status.Level == IGraphTransformer::TStatus::Error) {
                return false;
            }

            ctx.Step.Repeat(TExprStep::ExprEval);

            tableContent = ctx.Builder(pos)
                .Callable("EvaluateExpr")
                    .Add(0, tableContent)
                .Seal()
                .Build();

            lookupJoinFilterLambda = ctx.Builder(pos)
                .Lambda()
                    .Param("item")
                    .Callable("Coalesce")
                        .Callable(0, "SqlIn")
                            .Callable(0, "EvaluateExpr")
                                .Callable(0, "FlatMap")
                                    .Add(0, tableContent)
                                    .Lambda(1)
                                        .Param("input")
                                        .Callable("Just")
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                auto builder = parent.List(0);
                                                for (ui32 i = 0; i < smallJoinMembers.size(); ++i) {
                                                    builder
                                                        .Callable(i, "Member")
                                                            .Arg(0, "input")
                                                            .Atom(1, smallJoinMembers[i])
                                                        .Seal();
                                                }
                                                return builder.Seal();
                                            })
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                auto builder = parent.List(1);
                                for (ui32 i = 0; i < mainJoinMembers.size(); ++i) {
                                    builder
                                        .Callable(i, "Member")
                                            .Arg(0, "item")
                                            .Atom(1, mainJoinMembers[i])
                                        .Seal();
                                }
                                return builder.Seal();
                            })
                            .List(2)
                            .Seal()
                        .Seal()
                        .Callable(1, "Bool")
                            .Atom(0, "false", TNodeFlags::Default)
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        if (auto premap = GetPremapLambda(rightLeaf)) {
            tableContent = ctx.Builder(tableContent->Pos())
                .Callable("FlatMap")
                    .Add(0, tableContent)
                    .Add(1, premap.Cast().Ptr())
                .Seal()
                .Build();
        }

        const bool needPayload = joinType->IsAtom({"Inner", "Left"});

        // don't produce nulls
        TExprNode::TListType remappedMembers;
        TExprNode::TListType remappedMembersToSkipNull;

        TExprNode::TPtr smallKeySelector;
        if (!isCross) {
            tableContent = RemapNonConvertibleItems(tableContent, smallLabel, *rightKeyColumns, outputKeyType, remappedMembers, remappedMembersToSkipNull, ctx);
            if (!remappedMembersToSkipNull.empty()) {
                tableContent = ctx.NewCallable(pos, "SkipNullMembers", { tableContent, ctx.NewList(pos, std::move(remappedMembersToSkipNull)) });
            }

            auto arg = ctx.NewArgument(pos, "item");
            TExprNode::TListType tupleItems;
            YQL_ENSURE(2 * remappedMembers.size() == rightKeyColumns->ChildrenSize());
            for (auto memberNameNode : remappedMembers) {
                auto member = ctx.Builder(pos)
                    .Callable("Member")
                        .Add(0, arg)
                        .Add(1, memberNameNode)
                    .Seal()
                    .Build();

                tupleItems.push_back(member);
            }

            TExprNode::TPtr lambdaBody;
            if (tupleItems.size() == 1) {
                lambdaBody = tupleItems.front();
            } else {
                lambdaBody = ctx.NewList(pos, std::move(tupleItems));
            }

            smallKeySelector = ctx.NewLambda(pos, ctx.NewArguments(pos, { arg }), std::move(lambdaBody));
        } else {
            tableContent = Build<TCoCollect>(ctx, pos)
                .Input(tableContent)
                .Done().Ptr();
        }

        // may produce null in keys
        TExprNode::TPtr smallPayloadSelector;
        if (!isCross) {
            if (needPayload) {
                smallPayloadSelector = ctx.Builder(pos)
                    .Lambda()
                    .Param("item")
                    .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent)->TExprNodeBuilder& {
                        ui32 index = 0;
                        for (auto x : smallLabel.EnumerateAllMembers()) {
                            parent.List(index++)
                                .Atom(0, x)
                                .Callable(1, "Member")
                                    .Arg(0, "item")
                                .Atom(1, x)
                                .Seal();
                        }

                        return parent;
                    })
                    .Seal()
                    .Seal()
                    .Build();
            }
            else {
                smallPayloadSelector = ctx.Builder(pos)
                    .Lambda()
                        .Param("item")
                        .Callable("Void")
                        .Seal()
                    .Seal()
                    .Build();
            }
        }

        TExprNode::TPtr dict;
        if (!isCross && !mapJoinUseFlow) {
            dict = ctx.Builder(pos)
                .Callable("ToDict")
                    .Add(0, tableContent)
                    .Add(1, smallKeySelector)
                    .Add(2, smallPayloadSelector)
                    .List(3)
                        .Atom(0, "Hashed")
                        .Atom(1, needPayload && !isUniqueKey ? "Many" : "One")
                        .Atom(2, "Compact")
                        .List(3)
                            .Atom(0, "ItemsCount")
                            .Atom(1, ToString(dictItemsCount))
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        auto mainArg = ctx.NewArgument(pos, "mainRow");
        auto lookupArg = ctx.NewArgument(pos, "lookupRow");
        TExprNode::TListType joinedOutNodes;
        TExprNode::TListType rightRenameNodes;
        TExprNode::TListType leftRenameNodes;
        for (ui32 index = 0; index < outputLeftSchemeType->GetSize(); ++index) {
            auto item = outputLeftSchemeType->GetItems()[index];
            TVector<TStringBuf> newNames;
            newNames.push_back(item->GetName());
            TStringBuf part1;
            TStringBuf part2;
            SplitTableName(item->GetName(), part1, part2);
            TString memberName = mainLabel.MemberName(part1, part2);
            if (!op.Parent) {
                if (auto renamed = renameMap.FindPtr(item->GetName())) {
                    newNames = *renamed;
                }
            } else if (op.OutputRemoveColumns.contains(item->GetName())) {
                newNames = {};
            }

            for (auto newName : newNames) {
                leftRenameNodes.push_back(ctx.NewAtom(pos, memberName));
                leftRenameNodes.push_back(ctx.NewAtom(pos, newName));
                AddJoinRemappedColumn(pos, mainArg, joinedOutNodes, memberName, newName, ctx);
            }
        }

        if (needPayload || isCross) {
            for (ui32 index = 0; index < outputRightSchemeType->GetSize(); ++index) {
                auto item = outputRightSchemeType->GetItems()[index];
                TVector<TStringBuf> newNames;
                newNames.push_back(item->GetName());
                TStringBuf part1;
                TStringBuf part2;
                SplitTableName(item->GetName(), part1, part2);
                TString memberName = smallLabel.MemberName(part1, part2);
                if (!op.Parent) {
                    if (auto renamed = renameMap.FindPtr(item->GetName())) {
                        newNames = *renamed;
                    }
                } else if (op.OutputRemoveColumns.contains(item->GetName())) {
                    newNames = {};
                }

                for (auto newName : newNames) {
                    rightRenameNodes.push_back(ctx.NewAtom(pos, memberName));
                    rightRenameNodes.push_back(ctx.NewAtom(pos, newName));
                    AddJoinRemappedColumn(pos, lookupArg, joinedOutNodes, memberName, newName, ctx);
                }
            }
        }

        TExprNode::TPtr joined;
        if (!isCross) {
            TExprNode::TListType leftKeyColumnNodes;
            TExprNode::TListType leftKeyColumnNodesNullable;
            auto mapInput = RemapNonConvertibleItems(listArg, mainLabel, *leftKeyColumns, outputKeyType, leftKeyColumnNodes, leftKeyColumnNodesNullable, ctx);
            if (mapJoinUseFlow) {
                joined = ctx.Builder(pos)
                    .Callable("FlatMap")
                        .Callable(0, "SqueezeToDict")
                            .Callable(0, "ToFlow")
                                .Add(0, std::move(tableContent))
                                .Callable(1, "DependsOn")
                                    .Add(0, listArg)
                                .Seal()
                            .Seal()
                            .Add(1, std::move(smallKeySelector))
                            .Add(2, std::move(smallPayloadSelector))
                            .List(3)
                                .Atom(0, "Hashed", TNodeFlags::Default)
                                .Atom(1, needPayload && !isUniqueKey ? "Many" : "One", TNodeFlags::Default)
                                .Atom(2, "Compact", TNodeFlags::Default)
                                .List(3)
                                    .Atom(0, "ItemsCount", TNodeFlags::Default)
                                    .Atom(1, ToString(dictItemsCount), TNodeFlags::Default)
                                .Seal()
                            .Seal()
                        .Seal()
                        .Lambda(1)
                            .Param("dict")
                            .Callable("MapJoinCore")
                                .Add(0, std::move(mapInput))
                                .Arg(1, "dict")
                                .Add(2, joinType)
                                .Add(3, ctx.NewList(pos, std::move(leftKeyColumnNodes)))
                                .Add(4, ctx.NewList(pos, std::move(remappedMembers)))
                                .Add(5, ctx.NewList(pos, std::move(leftRenameNodes)))
                                .Add(6, ctx.NewList(pos, std::move(rightRenameNodes)))
                                .Add(7, leftKeyColumns)
                                .Add(8, rightKeyColumns)
                            .Seal()
                        .Seal()
                    .Seal()
                    .Build();
            } else {
                joined = ctx.Builder(pos)
                    .Callable("MapJoinCore")
                        .Add(0, mapInput)
                        .Add(1, dict)
                        .Add(2, joinType)
                        .Add(3, ctx.NewList(pos, std::move(leftKeyColumnNodes)))
                        .Add(4, ctx.NewList(pos, std::move(remappedMembers)))
                        .Add(5, ctx.NewList(pos, std::move(leftRenameNodes)))
                        .Add(6, ctx.NewList(pos, std::move(rightRenameNodes)))
                        .Add(7, leftKeyColumns)
                        .Add(8, rightKeyColumns)
                    .Seal()
                    .Build();
            }
        }
        else {
            auto joinedOut = ctx.NewCallable(pos, "AsStruct", std::move(joinedOutNodes));
            auto joinedBody = ctx.Builder(pos)
                .Callable("Map")
                    .Callable(0, "ToFlow")
                        .Add(0, std::move(tableContent))
                        .Callable(1, "DependsOn")
                            .Add(0, listArg)
                        .Seal()
                    .Seal()
                    .Lambda(1)
                        .Param("smallRow")
                        .ApplyPartial(nullptr, std::move(joinedOut)).WithNode(*lookupArg, "smallRow").Seal()
                    .Seal()
                .Seal()
                .Build();

            auto joinedLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { mainArg }), std::move(joinedBody));
            joined = ctx.Builder(pos)
                .Callable("FlatMap")
                .Add(0, listArg)
                .Add(1, std::move(joinedLambda))
                .Seal()
                .Build();
        }

        auto mapLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {std::move(listArg)}), std::move(joined));
        if (const auto premap = GetPremapLambda(leftLeaf)) {
            TExprNode::TPtr placeHolder;
            std::tie(placeHolder, mapLambda) = ReplaceDependsOn(mapLambda, ctx, state->Types);

            mapLambda = ctx.Builder(mapLambda->Pos())
                .Lambda()
                    .Param("list")
                    .Apply(mapLambda)
                        .With(0)
                            .Callable("FlatMap")
                                .Arg(0, "list")
                                .Add(1, premap.Cast().Ptr())
                            .Seal()
                        .Done()
                        .WithNode(*placeHolder, "list")
                    .Seal()
                .Seal()
                .Build();
        }

        // since premap doesn't affect key columns we can apply lookup join filter before premap
        if (lookupJoinFilterLambda) {
            TExprNode::TPtr placeHolder;
            std::tie(placeHolder, mapLambda) = ReplaceDependsOn(mapLambda, ctx, state->Types);

            mapLambda = ctx.Builder(mapLambda->Pos())
                .Lambda()
                    .Param("list")
                    .Apply(mapLambda)
                        .With(0)
                            .Callable("Filter")
                                .Arg(0, "list")
                                .Add(1, std::move(lookupJoinFilterLambda))
                            .Seal()
                        .Done()
                        .WithNode(*placeHolder, "list")
                    .Seal()
                .Seal()
                .Build();
        }

        if (state->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
            mapSettingsBuilder
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::Flow), TNodeFlags::Default)
                    .Build()
                .Build();
        }

        auto map = Build<TYtMap>(ctx, pos)
            .World(mapWorld)
            .DataSink(equiJoin.DataSink())
            .Input()
                .Add()
                    .Paths(mainPaths)
                    .Settings(mainSettings)
                .Build()
            .Build()
            .Output()
                .Add(outTableInfo.ToExprNode(ctx, pos).Cast<TYtOutTable>())
            .Build()
            .Settings(mapSettingsBuilder.Done())
            .Mapper(mapLambda)
            .Done();

        maps.push_back(map);
    }

    if (maps.size() == 1) {
        op.Output = maps.front();
    }
    else {
        TVector<TYtPath> paths;
        for (auto map: maps) {
            paths.push_back(Build<TYtPath>(ctx, pos)
                .Table<TYtOutput>()
                    .Operation(map)
                    .OutIndex().Value(0U).Build()
                .Build()
                .Columns<TCoVoid>().Build()
                .Ranges<TCoVoid>().Build()
                .Stat<TCoVoid>().Build()
                .Done()
            );
        }
        op.Output = Build<TYtMerge>(ctx, pos)
            .World<TCoWorld>().Build()
            .DataSink(equiJoin.DataSink())
            .Input()
                .Add()
                    .Paths()
                        .Add(paths)
                    .Build()
                    .Settings()
                    .Build()
                .Build()
            .Build()
            .Output()
                .Add(outTableInfo.ToExprNode(ctx, pos).Cast<TYtOutTable>())
            .Build()
            .Settings()
            .Build()
            .Done();
    }

    return true;
}

TCoLambda BuildIdentityLambda(TPositionHandle pos, TExprContext& ctx) {
    return Build<TCoLambda>(ctx, pos)
        .Args({"item"})
        .Body("item")
        .Done();
}

TCoLambda BuildMapCombinerSideLambda(const TYtState::TPtr& state, const TMap<TString, const TTypeAnnotationNode*>& keys, TPositionHandle pos, TExprContext& ctx) {

    auto pickleLambda = ctx.Builder(pos)
        .Lambda()
            .Param("item")
            .Callable("StablePickle")
                .Arg(0, "item")
            .Seal()
        .Seal()
        .Build();

    auto identityLambda = BuildIdentityLambda(pos, ctx).Ptr();

    TCoLambda keyExtractor(ctx.Builder(pos)
        .Lambda()
            .Param("item")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                auto builder = parent.List();
                size_t i = 0;
                for (const auto& [name, type] : keys) {
                    bool needPickle = RemoveOptionalType(type)->GetKind() != ETypeAnnotationKind::Data;
                    builder
                        .Apply(i++, needPickle ? pickleLambda : identityLambda)
                            .With(0)
                                .Callable("Member")
                                    .Arg(0, "item")
                                    .Atom(1, name)
                                .Seal()
                            .Done()
                        .Seal();
                }
                return builder.Seal();
            })
        .Seal()
        .Build());


    return Build<TCoLambda>(ctx, pos)
        .Args({"stream"})
        .Body<TCoCombineCore>()
            .Input("stream")
            .KeyExtractor(keyExtractor)
            .InitHandler()
                .Args({"key", "item"})
                .Body("item")
            .Build()
            .UpdateHandler()
                .Args({"key", "item", "state"})
                .Body("state")
            .Build()
            .FinishHandler()
                .Args({"key", "state"})
                .Body<TCoJust>()
                    .Input("state")
                .Build()
            .Build()
            .MemLimit<TCoAtom>()
                .Value(ToString(state->Configuration->CombineCoreLimit.Get().GetOrElse(0)))
            .Build()
        .Build()
        .Done();
}

TCoLambda BuildMapCombinerLambda(const TYtState::TPtr& state, const TJoinLabels& labels, const TYtJoinNodeOp& op, TPositionHandle pos, TExprContext& ctx)
{
    TCoLambda identityLambda = BuildIdentityLambda(pos, ctx);

    bool leftAny = op.LinkSettings.LeftHints.contains("any");
    bool rightAny = op.LinkSettings.RightHints.contains("any");

    if (!leftAny && !rightAny) {
        return identityLambda;
    }

    auto leftKeyColumns = op.LeftLabel;
    auto rightKeyColumns = op.RightLabel;

    YQL_ENSURE(leftKeyColumns);
    YQL_ENSURE(rightKeyColumns);

    auto leftKeyTypes = BuildJoinKeyTypeMap(labels.Inputs[0], *leftKeyColumns);
    auto rightKeyTypes = BuildJoinKeyTypeMap(labels.Inputs[1], *rightKeyColumns);

    return Build<TCoLambda>(ctx, pos)
        .Args({"stream"})
        .Body<TCoSwitch>()
            .Input("stream")
            .BufferBytes()
                .Value(ToString(state->Configuration->SwitchLimit.Get().GetOrElse(DEFAULT_SWITCH_MEMORY_LIMIT)))
            .Build()
            .FreeArgs()
                .Add<TCoAtomList>()
                    .Add()
                        .Value(0U)
                    .Build()
                .Build()
                .Add(leftAny ? BuildMapCombinerSideLambda(state, leftKeyTypes, pos, ctx) : identityLambda)
                .Add<TCoAtomList>()
                    .Add()
                        .Value(1U)
                    .Build()
                .Build()
                .Add(rightAny ? BuildMapCombinerSideLambda(state, rightKeyTypes, pos, ctx) : identityLambda)
            .Build()
        .Build()
        .Done();
}

bool JoinKeysMayHaveNulls(const TVector<const TTypeAnnotationNode*>& inputKeyTypes, const TVector<const TTypeAnnotationNode*>& unifiedKeyTypes) {
    YQL_ENSURE(inputKeyTypes.size() == unifiedKeyTypes.size());
    for (size_t i = 0; i < inputKeyTypes.size(); ++i) {
        if (inputKeyTypes[i]->HasOptionalOrNull()) {
            return true;
        }

        NUdf::TCastResultOptions options = CastResult<true>(inputKeyTypes[i], unifiedKeyTypes[i]);
        YQL_ENSURE(!(options & NKikimr::NUdf::ECastOptions::Impossible));
        if (options & NKikimr::NUdf::ECastOptions::MayFail) {
            return true;
        }
    }
    return false;
}

TExprNode::TPtr BuildSideSplitNullsLambda(TPositionHandle pos, bool mayHaveNulls, const TExprNode::TPtr& inputItem,
    const TVector<TString>& keyColumns, const TString& sidePrefix,
    const TCoLambda& joinRenamingLambda, const TStructExprType& joinOutputType,
    const TExprNode::TPtr& outputVariantType, size_t outputVariantIndex, TExprContext& ctx)
{
    if (!mayHaveNulls) {
        return ctx.Builder(pos)
            .Lambda()
                .Param("side")
                .Callable("Variant")
                    .Add(0, inputItem)
                    .Atom(1, 0U)
                    .Add(2, outputVariantType)
                .Seal()
            .Seal()
            .Build();
    }

    return ctx.Builder(pos)
        .Lambda()
            .Param("side")
            .Callable("If")
                .Callable(0, "HasNull")
                    .List(0)
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (ui32 i = 0; i < keyColumns.size(); ++i) {
                                parent
                                    .Callable(i, "Member")
                                        .Add(0, inputItem)
                                        .Atom(1, keyColumns[i])
                                    .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
                .Callable(1, "Variant")
                    .Apply(0, joinRenamingLambda.Ptr())
                        .With(0)
                            .Callable("StrictCast")
                                .Callable(0, "FlattenMembers")
                                    .List(0)
                                        .Atom(0, sidePrefix)
                                        .Arg(1, "side")
                                    .Seal()
                                .Seal()
                                .Add(1, ExpandType(pos, joinOutputType, ctx))
                            .Seal()
                        .Done()
                    .Seal()
                    .Atom(1, ToString(outputVariantIndex), TNodeFlags::Default)
                    .Add(2, outputVariantType)
                .Seal()
                .Callable(2, "Variant")
                    .Add(0, inputItem)
                    .Atom(1, 0U)
                    .Add(2, outputVariantType)
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

bool RewriteYtCommonJoin(TYtEquiJoin equiJoin, const TJoinLabels& labels, TYtJoinNodeOp& op,
    const TYtJoinNodeLeaf& leftLeaf, const TYtJoinNodeLeaf& rightLeaf, const TYtState::TPtr& state, TExprContext& ctx,
    bool leftUnique, bool rightUnique, ui64 leftSize, ui64 rightSize)
{
    const auto pos = equiJoin.Pos();

    const auto leftNotFat = leftUnique || op.LinkSettings.LeftHints.contains("unique") || op.LinkSettings.LeftHints.contains("small");
    const auto rightNotFat = rightUnique || op.LinkSettings.RightHints.contains("unique") || op.LinkSettings.RightHints.contains("small");
    bool leftFirst = false;
    if (leftNotFat != rightNotFat) {
        // non-fat will be first
        leftFirst = leftNotFat;
    } else {
        // small table will be first
        leftFirst = leftSize < rightSize;
    }

    YQL_CLOG(INFO, ProviderYt) << "CommonJoin, << " << (leftFirst ? "left" : "right") << " table moved first";

    auto leftKeyColumns = op.LeftLabel;
    auto rightKeyColumns = op.RightLabel;

    auto joinType = op.JoinKind;
    auto joinTree = ctx.NewList(pos, {
        joinType,
        ctx.NewAtom(pos, leftLeaf.Scope[0]),
        ctx.NewAtom(pos, rightLeaf.Scope[0]),
        leftKeyColumns,
        rightKeyColumns,
        ctx.NewList(pos, {})
    });

    auto columnTypes = GetJoinColumnTypes(*joinTree, labels, "Inner", ctx);
    auto finalColumnTypes = GetJoinColumnTypes(*joinTree, labels, ctx);
    auto outputLeftSchemeType = MakeOutputJoinColumns(columnTypes, labels.Inputs[0], ctx);
    auto outputRightSchemeType = MakeOutputJoinColumns(columnTypes, labels.Inputs[1], ctx);

    TVector<const TTypeAnnotationNode*> outputKeyType;
    TVector<const TTypeAnnotationNode*> inputKeyTypeLeft;
    TVector<const TTypeAnnotationNode*> inputKeyTypeRight;
    const bool isCrossJoin = joinType->IsAtom("Cross");
    if (isCrossJoin) {
        outputKeyType = TVector<const TTypeAnnotationNode*>(1, ctx.MakeType<TDataExprType>(EDataSlot::Uint32));
    } else {
        inputKeyTypeLeft = BuildJoinKeyType(labels.Inputs[0], *leftKeyColumns);
        inputKeyTypeRight = BuildJoinKeyType(labels.Inputs[1], *rightKeyColumns);
        outputKeyType = UnifyJoinKeyType(pos, inputKeyTypeLeft, inputKeyTypeRight, ctx);
    }

    TVector<TString> ytReduceByColumns;
    for (size_t i = 0; i < outputKeyType.size(); ++i) {
        ytReduceByColumns.push_back(TStringBuilder() << "_yql_join_column_" << i);
    }

    TExprNode::TListType leftMembersNodes;
    TExprNode::TListType rightMembersNodes;
    TExprNode::TListType requiredMembersNodes;
    bool hasData[2] = { false, false };
    TMap<TStringBuf, TVector<TStringBuf>> renameMap;
    if (!op.Parent) {
        renameMap = LoadJoinRenameMap(equiJoin.JoinOptions().Ref());
    }

    if (!joinType->IsAtom({"RightSemi", "RightOnly"})) {
        hasData[0] = true;
        for (auto x : outputLeftSchemeType->GetItems()) {
            auto name = ctx.NewAtom(pos, x->GetName());
            if (auto renamed = renameMap.FindPtr(x->GetName())) {
                if (renamed->empty()) {
                    continue;
                }
            }

            auto finalColumnType = finalColumnTypes[x->GetName()];
            if (!finalColumnType->IsOptionalOrNull()) {
                requiredMembersNodes.push_back(name);
            }

            leftMembersNodes.push_back(name);
        }
    }

    if (!joinType->IsAtom({"LeftSemi", "LeftOnly"})) {
        hasData[1] = true;
        for (auto x : outputRightSchemeType->GetItems()) {
            auto name = ctx.NewAtom(pos, x->GetName());
            if (auto renamed = renameMap.FindPtr(x->GetName())) {
                if (renamed->empty()) {
                    continue;
                }
            }

            auto finalColumnType = finalColumnTypes[x->GetName()];
            if (!finalColumnType->IsOptionalOrNull()) {
                requiredMembersNodes.push_back(name);
            }

            rightMembersNodes.push_back(name);
        }
    }

    TCommonJoinCoreLambdas cjcLambdas[2];
    for (ui32 index = 0; index < 2; ++index) {
        auto keyColumnsNode = (index == 0) ? leftKeyColumns : rightKeyColumns;

        auto& label = labels.Inputs[index];
        auto& otherLabel = labels.Inputs[1 - index];

        auto myOutputSchemeType = (index == 0) ? outputLeftSchemeType : outputRightSchemeType;
        auto otherOutputSchemeType = (index == 1) ? outputLeftSchemeType : outputRightSchemeType;

        cjcLambdas[index] = MakeCommonJoinCoreLambdas(pos, ctx, label, otherLabel, outputKeyType, *keyColumnsNode,
            joinType->Content(), myOutputSchemeType, otherOutputSchemeType, index, true,
            leftFirst ? index : 1 - index, renameMap, hasData[index], hasData[1 - index], ytReduceByColumns);

        auto& leaf =      (index == 0) ? leftLeaf : rightLeaf;
        auto& otherLeaf = (index == 1) ? leftLeaf : rightLeaf;
        ApplyInputPremap(cjcLambdas[index].MapLambda, leaf, otherLeaf, ctx);
    }
    YQL_ENSURE(cjcLambdas[0].CommonJoinCoreInputType == cjcLambdas[1].CommonJoinCoreInputType, "Must be same type from both side of join.");

    auto groupArg = ctx.NewArgument(pos, "group");

    TExprNode::TListType optionNodes;
    AddAnyJoinOptionsToCommonJoinCore(optionNodes, false, op.LinkSettings, pos, ctx);
    if (auto memLimit = state->Configuration->CommonJoinCoreLimit.Get()) {
        optionNodes.push_back(ctx.Builder(pos)
            .List()
                .Atom(0, "memLimit", TNodeFlags::Default)
                .Atom(1, ToString(*memLimit), TNodeFlags::Default)
            .Seal()
            .Build());
    }
    optionNodes.push_back(ctx.Builder(pos)
        .List()
            .Atom(0, "sorted", TNodeFlags::Default)
            .Atom(1, leftFirst ? "left" : "right", TNodeFlags::Default)
        .Seal()
        .Build());

    TExprNode::TListType keyMembersNodes;
    if (!isCrossJoin) {
        for (auto& x : ytReduceByColumns) {
            keyMembersNodes.push_back(ctx.NewAtom(pos, x));
        }
    }

    auto convertedList = PrepareForCommonJoinCore(pos, ctx, groupArg, cjcLambdas[0].ReduceLambda,
                                                  cjcLambdas[1].ReduceLambda);
    auto joinedRawStream = ctx.NewCallable(pos, "CommonJoinCore", { convertedList, joinType,
        ctx.NewList(pos, std::move(leftMembersNodes)), ctx.NewList(pos, std::move(rightMembersNodes)),
        ctx.NewList(pos, std::move(requiredMembersNodes)), ctx.NewList(pos, std::move(keyMembersNodes)),
        ctx.NewList(pos, std::move(optionNodes)), ctx.NewAtom(pos, "_yql_table_index", TNodeFlags::Default) });
    auto joinedRaw = joinedRawStream;

    const TStructExprType* outItemTypeBeforeRename = MakeIntermediateEquiJoinTableType(pos, *joinTree, labels, {}, ctx);
    if (!outItemTypeBeforeRename) {
        return false;
    }
    const TStructExprType* outItemType = nullptr;
    if (!op.Parent) {
        if (auto type = GetSequenceItemType(equiJoin.Pos(),
                                            equiJoin.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1],
                                            false, ctx)) {
            outItemType = type->Cast<TStructExprType>();
        } else {
            return false;
        }
    } else {
        outItemType = MakeIntermediateEquiJoinTableType(pos, *joinTree, labels, op.OutputRemoveColumns, ctx);
        if (!outItemType) {
            return false;
        }
    }

    const TCoLambda joinRenamingLambda = BuildJoinRenameLambda(pos, renameMap, *outItemType, ctx);

    auto joined = ctx.Builder(pos)
        .Callable("Map")
            .Add(0, joinedRaw)
            .Add(1, joinRenamingLambda.Ptr())
        .Seal()
        .Build();

    TVector<TString> ytSortByColumns;
    ytSortByColumns = ytReduceByColumns;
    ytSortByColumns.push_back("_yql_sort");

    auto mapCombinerLambda = BuildMapCombinerLambda(state, labels, op, pos, ctx);

    TExprNode::TPtr chopperHandler = ctx.NewLambda(pos, ctx.NewArguments(pos, {ctx.NewArgument(pos, "stup"), groupArg }), std::move(joined));
    TExprNode::TPtr chopperSwitch;
    auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, pos);

    if (state->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS)) {
        chopperSwitch = ctx.Builder(pos)
            .Lambda()
                .Param("key")
                .Param("item")
                .Callable("SqlExtractKey")
                    .Arg(0, "item")
                    .Lambda(1)
                        .Param("row")
                        .Callable("Member")
                            .Arg(0, "row")
                            .Atom(1, YqlSysColumnKeySwitch, TNodeFlags::Default)
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        chopperHandler = ctx.Builder(pos)
            .Lambda()
                .Param("key")
                .Param("group")
                .Apply(chopperHandler)
                    .With(0, "key")
                    .With(1)
                        .Callable("RemovePrefixMembers")
                            .Arg(0, "group")
                            .List(1)
                                .Atom(0, YqlSysColumnPrefix, TNodeFlags::Default)
                            .Seal()
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
            .Build();

        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::KeySwitch), TNodeFlags::Default)
                .Build()
            .Build();
    }
    else {
        chopperSwitch = Build<TCoLambda>(ctx, pos)
            .Args({"key", "item"})
            .Body<TYtIsKeySwitch>()
                .DependsOn()
                    .Input("item")
                .Build()
            .Build()
            .Done().Ptr();
    }

    auto reducer = Build<TCoLambda>(ctx, pos)
        .Args({"stream"})
        .Body<TCoChopper>()
            .Input("stream")
            .KeyExtractor()
                .Args({"item"})
                .Body<TCoUint64>()
                    .Literal()
                        .Value(0U)
                    .Build()
                .Build()
            .Build()
            .GroupSwitch(chopperSwitch)
            .Handler(chopperHandler)
        .Build()
        .Done();

    settingsBuilder
        .Add()
            .Name()
                .Value(ToString(EYtSettingType::ReduceBy), TNodeFlags::Default)
            .Build()
            .Value(ToAtomList(ytReduceByColumns, pos, ctx))
        .Build()
        .Add()
            .Name()
                .Value(ToString(EYtSettingType::SortBy), TNodeFlags::Default)
            .Build()
            .Value(ToAtomList(ytSortByColumns, pos, ctx))
        .Build();

    auto mapLambda = Build<TCoLambda>(ctx, pos)
            .Args({"flow"})
            .Body<TCoOrderedFlatMap>()
                .Input<TExprApplier>()
                    .Apply(mapCombinerLambda)
                    .With(0, "flow")
                .Build()
                .Lambda()
                    .Args({"item"})
                    .Body<TCoVisit>()
                        .Input("item")
                        .FreeArgs()
                            .Add<TCoAtom>()
                                .Value(0U)
                            .Build()
                            .Add(cjcLambdas[0].MapLambda)
                            .Add<TCoAtom>()
                                .Value(1U)
                            .Build()
                            .Add(cjcLambdas[1].MapLambda)
                        .Build()
                    .Build()
                .Build()
            .Build().Done().Ptr();

    TYtOutTableInfo outInfo(outItemType, state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
    outInfo.RowSpec->SetConstraints(op.Constraints);
    outInfo.SetUnique(op.Constraints.GetConstraint<TDistinctConstraintNode>(), pos, ctx);
    const auto outTableInfo = outInfo.ToExprNode(ctx, pos).Ptr();

    TExprNode::TListType mapReduceOutputs;
    TVector<bool> mapReduceOutputsSingleValue;

    mapReduceOutputs.push_back(outTableInfo);
    if (!isCrossJoin && state->Configuration->JoinCommonUseMapMultiOut.Get().GetOrElse(DEFAULT_JOIN_COMMON_USE_MULTI_OUT)) {
        bool leftNulls = false;
        if (!leftNotFat && joinType->IsAtom({"Left", "Full", "Exclusion"})) {
            leftNulls = JoinKeysMayHaveNulls(inputKeyTypeLeft, outputKeyType);
            if (leftNulls) {
                mapReduceOutputs.push_back(outTableInfo);
                mapReduceOutputsSingleValue.push_back(op.LinkSettings.LeftHints.contains("any"));
            }
        }

        bool rightNulls = false;
        if (!rightNotFat && joinType->IsAtom({"Right", "Full", "Exclusion"})) {
            rightNulls = JoinKeysMayHaveNulls(inputKeyTypeRight, outputKeyType);
            if (rightNulls) {
                mapReduceOutputs.push_back(outTableInfo);
                mapReduceOutputsSingleValue.push_back(op.LinkSettings.RightHints.contains("any"));
            }
        }

        if (leftNulls || rightNulls) {
            TExprNode::TPtr itemArg = ctx.NewArgument(pos, "item");
            TExprNode::TListType outputVarTypeItems;

            // output to reducer
            outputVarTypeItems.push_back(ctx.NewCallable(pos, "TypeOf", { itemArg }));

            // direct outputs
            size_t leftOutputIndex = 0;
            if (leftNulls) {
                leftOutputIndex = outputVarTypeItems.size();
                outputVarTypeItems.push_back(ExpandType(pos, *outItemType, ctx));
            }
            size_t rightOutputIndex = 0;
            if (rightNulls) {
                rightOutputIndex = outputVarTypeItems.size();
                outputVarTypeItems.push_back(ExpandType(pos, *outItemType, ctx));
            }

            auto variantType = ctx.Builder(pos)
                .Callable("VariantType")
                    .Add(0, ctx.NewCallable(pos, "TupleType", std::move(outputVarTypeItems)))
                .Seal()
                .Build();

            const TString leftSidePrefix = labels.Inputs[0].AddLabel ? (TStringBuilder() << labels.Inputs[0].Tables[0] << ".") : TString();
            TExprNode::TPtr leftVisitLambda = BuildSideSplitNullsLambda(pos, leftNulls, itemArg, ytReduceByColumns, leftSidePrefix,
                joinRenamingLambda, *outItemTypeBeforeRename, variantType, leftOutputIndex, ctx);

            const TString rightSidePrefix = labels.Inputs[1].AddLabel ? (TStringBuilder() << labels.Inputs[1].Tables[0] << ".") : TString();
            TExprNode::TPtr rightVisitLambda = BuildSideSplitNullsLambda(pos, rightNulls, itemArg, ytReduceByColumns, rightSidePrefix,
                joinRenamingLambda, *outItemTypeBeforeRename, variantType, rightOutputIndex, ctx);

            auto splitLambdaBody = ctx.Builder(pos)
                .Callable("Visit")
                    .Callable(0, "Member")
                        .Add(0, itemArg)
                        .Atom(1, "_yql_join_payload", TNodeFlags::Default)
                    .Seal()
                    .Atom(1, 0U)
                    .Add(2, leftVisitLambda)
                    .Atom(3, 1U)
                    .Add(4, rightVisitLambda)
                .Seal()
                .Build();

            mapLambda = ctx.Builder(pos)
                .Lambda()
                    .Param("stream")
                    .Callable("OrderedMap")
                        .Apply(0, mapLambda)
                            .With(0, "stream")
                        .Seal()
                        .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, { itemArg }), std::move(splitLambdaBody)))
                    .Seal()
                .Seal()
                .Build();
        }
    }

    if (state->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::Flow), TNodeFlags::Default)
                .Build()
            .Build();
    }

    const auto mapreduce = Build<TYtMapReduce>(ctx, pos)
        .World(equiJoin.World())
        .DataSink(equiJoin.DataSink())
        .Input()
            .Add()
                .Paths(MakeUnorderedSection(leftLeaf.Section, ctx).Paths())
                .Settings(NYql::RemoveSettings(leftLeaf.Section.Settings().Ref(), EYtSettingType::JoinLabel | EYtSettingType::StatColumns, ctx))
            .Build()
            .Add()
                .Paths(MakeUnorderedSection(rightLeaf.Section, ctx).Paths())
                .Settings(NYql::RemoveSettings(rightLeaf.Section.Settings().Ref(), EYtSettingType::JoinLabel | EYtSettingType::StatColumns, ctx))
            .Build()
        .Build()
        .Output(ctx.NewList(pos, std::move(mapReduceOutputs)))
        .Settings(settingsBuilder.Done())
        .Mapper(mapLambda)
        .Reducer(reducer)
        .Done();

    if (mapReduceOutputsSingleValue.empty()) {
        op.Output = mapreduce;
    } else {
        TVector<TYtPath> paths;
        ui32 idx = 0U;
        for (bool single: mapReduceOutputsSingleValue) {
            TExprBase ranges = Build<TCoVoid>(ctx, pos).Done();
            if (single) {
                ranges = Build<TExprList>(ctx, pos)
                    .Add<TYtRow>()
                        .Index<TCoUint64>()
                            .Literal()
                                .Value(0U)
                            .Build()
                        .Build()
                    .Build()
                    .Done();
            }

            paths.push_back(Build<TYtPath>(ctx, pos)
                .Table<TYtOutput>()
                    .Operation(mapreduce)
                    .OutIndex().Value(idx++).Build()
                .Build()
                .Columns<TCoVoid>().Build()
                .Ranges(ranges)
                .Stat<TCoVoid>().Build()
                .Done()
            );
        }

        paths.push_back(Build<TYtPath>(ctx, pos)
            .Table<TYtOutput>()
                .Operation(mapreduce)
                .OutIndex().Value(idx++).Build()
            .Build()
            .Columns<TCoVoid>().Build()
            .Ranges<TCoVoid>().Build()
            .Stat<TCoVoid>().Build()
            .Done()
        );

        op.Output = Build<TYtMerge>(ctx, pos)
            .World(equiJoin.World())
            .DataSink(equiJoin.DataSink())
            .Input()
                .Add()
                    .Paths()
                        .Add(paths)
                    .Build()
                    .Settings()
                    .Build()
                .Build()
            .Build()
            .Output()
                .Add(outTableInfo)
            .Build()
            .Settings()
            .Build()
            .Done();
    }

    return true;
}

bool RewriteYtEmptyJoin(TYtEquiJoin equiJoin, const TJoinLabels& labels, TYtJoinNodeOp& op,
    const TYtJoinNodeLeaf& leftLeaf, const TYtJoinNodeLeaf& rightLeaf, const TYtState::TPtr& state, TExprContext& ctx)
{
    auto pos = equiJoin.Pos();

    YQL_CLOG(INFO, ProviderYt) << "EmptyJoin";

    const TStructExprType* outItemType = nullptr;
    if (!op.Parent) {
        if (auto type = GetSequenceItemType(pos, equiJoin.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1], false, ctx)) {
            outItemType = type->Cast<TStructExprType>();
        } else {
            return false;
        }
    } else {
        auto joinTree = ctx.NewList(pos, {
            op.JoinKind,
            ctx.NewAtom(pos, leftLeaf.Scope[0]),
            ctx.NewAtom(pos, rightLeaf.Scope[0]),
            op.LeftLabel,
            op.RightLabel,
            ctx.NewList(pos, {})
        });

        outItemType = MakeIntermediateEquiJoinTableType(pos, *joinTree, labels, op.OutputRemoveColumns, ctx);
        if (!outItemType) {
            return false;
        }
    }

    TSyncMap syncList;
    for (auto path: leftLeaf.Section.Paths()) {
        if (auto out = path.Table().Maybe<TYtOutput>()) {
            syncList.emplace(out.Cast().Operation().Ptr(), syncList.size());
        }
    }
    for (auto path: rightLeaf.Section.Paths()) {
        if (auto out = path.Table().Maybe<TYtOutput>()) {
            syncList.emplace(out.Cast().Operation().Ptr(), syncList.size());
        }
    }

    TYtOutTableInfo outTableInfo(outItemType, state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
    outTableInfo.RowSpec->SetConstraints(op.Constraints);
    outTableInfo.SetUnique(op.Constraints.GetConstraint<TDistinctConstraintNode>(), pos, ctx);

    op.Output = Build<TYtTouch>(ctx, pos)
        .World(ApplySyncListToWorld(equiJoin.World().Ptr(), syncList, ctx))
        .DataSink(equiJoin.DataSink())
        .Output()
            .Add(outTableInfo.ToExprNode(ctx, pos).Cast<TYtOutTable>())
        .Build()
        .Done();

    return true;
}

struct TJoinSideStats {
    TString TableNames;
    bool HasUniqueKeys = false;
    bool IsDynamic = false;
    bool NeedsRemap = false;

    TVector<TString> SortedKeys;

    ui64 RowsCount = 0;
    ui64 Size = 0;
};

enum class ESizeStatCollectMode {
    NoSize,
    RawSize,
    ColumnarSize,
};

TStatus CollectJoinSideStats(ESizeStatCollectMode sizeMode, TJoinSideStats& stats, TYtSection& inputSection,
    const TYtState& state, const TString& cluster,
    const TVector<TYtPathInfo::TPtr>& tableInfo, const THashSet<TString>& joinKeys,
    bool isCross, TMaybeNode<TCoLambda> premap, TExprContext& ctx)
{
    stats = {};

    stats.HasUniqueKeys = !isCross;
    stats.IsDynamic = AnyOf(tableInfo, [](const TYtPathInfo::TPtr& path) {
        return path->Table->Meta->IsDynamic;
    });
    const ui64 nativeTypeFlags = state.Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) && inputSection.Ref().GetTypeAnn()
         ? GetNativeYtTypeFlags(*inputSection.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>())
         : 0ul;
    TMaybe<NYT::TNode> firstNativeType;
    if (!tableInfo.empty()) {
        firstNativeType = tableInfo.front()->GetNativeYtType();
    }
    stats.NeedsRemap = NYql::HasSetting(inputSection.Settings().Ref(), EYtSettingType::SysColumns)
        || AnyOf(tableInfo, [nativeTypeFlags, firstNativeType](const TYtPathInfo::TPtr& path) {
            return path->RequiresRemap()
                || path->Table->RowSpec->HasAuxColumns() // TODO: remove
                || nativeTypeFlags != path->GetNativeYtTypeFlags()
                || firstNativeType != path->GetNativeYtType();
        });

    bool first = true;
    for (auto& path: tableInfo) {
        if (sizeMode != ESizeStatCollectMode::NoSize) {
            YQL_ENSURE(path->Table->Stat);
            auto tableRecords = path->Table->Stat->RecordsCount;
            if (path->Ranges) {
                tableRecords = path->Ranges->GetUsedRows(tableRecords).GetOrElse(tableRecords);
            }
            stats.RowsCount += tableRecords;
            stats.Size += path->Table->Stat->DataSize;
        }

        if (!stats.TableNames.empty()) {
            stats.TableNames += " | ";
        }

        stats.TableNames += path->Table->Name;
        if (!isCross) {
            UpdateSortPrefix(first, stats.SortedKeys, path->Table->RowSpec, stats.HasUniqueKeys, joinKeys, premap);
        }
        first = false;
    }

    if (sizeMode != ESizeStatCollectMode::ColumnarSize) {
        return TStatus::Ok;
    }

    TVector<ui64> dataSizes;
    auto status = TryEstimateDataSizeChecked(dataSizes, inputSection, cluster, tableInfo, {}, state, ctx);
    if (status.Level != TStatus::Ok) {
        return status;
    }

    stats.Size = Accumulate(dataSizes.begin(), dataSizes.end(), 0ull, [](ui64 sum, ui64 v) { return sum + v; });
    return TStatus::Ok;
}

TStatus CollectPathsAndLabels(TVector<TYtPathInfo::TPtr>& tables, TJoinLabels& labels,
    const TStructExprType*& itemType, const TStructExprType*& itemTypeBeforePremap,
    const TYtJoinNodeLeaf& leaf, TExprContext& ctx)
{
    tables = {};
    itemType = nullptr;
    itemTypeBeforePremap = nullptr;

    TExprBase input = leaf.Section;
    if (auto type = GetSequenceItemType(input, false, ctx)) {
        itemTypeBeforePremap = type->Cast<TStructExprType>();
    } else {
        return TStatus::Error;
    }

    if (leaf.Premap) {
        input = leaf.Premap.Cast();
    }

    if (auto type = GetSequenceItemType(input, false, ctx)) {
        itemType = type->Cast<TStructExprType>();
    } else {
        return TStatus::Error;
    }

    if (auto err = labels.Add(ctx, *leaf.Label, itemType)) {
        ctx.AddError(*err);
        return TStatus::Error;
    }

    for (auto path: leaf.Section.Paths()) {
        auto pathInfo = MakeIntrusive<TYtPathInfo>(path);
        tables.push_back(pathInfo);
    }

    return TStatus::Ok;
}

TStatus CollectPathsAndLabelsReady(bool& ready, TVector<TYtPathInfo::TPtr>& tables, TJoinLabels& labels,
    const TStructExprType*& itemType, const TStructExprType*& itemTypeBeforePremap,
    const TYtJoinNodeLeaf& leaf, TExprContext& ctx)
{
    ready = false;
    TStatus result = CollectPathsAndLabels(tables, labels, itemType, itemTypeBeforePremap, leaf, ctx);
    if (result != TStatus::Ok) {
        return result;
    }

    ready = AllOf(tables, [](const auto& pathInfo) { return bool(pathInfo->Table->Stat); });
    return TStatus::Ok;
}

TStatus CollectStatsAndMapJoinSettings(ESizeStatCollectMode sizeMode, TMapJoinSettings& mapSettings,
    TJoinSideStats& leftStats, TJoinSideStats& rightStats,
    bool leftTablesReady, const TVector<TYtPathInfo::TPtr>& leftTables, const THashSet<TString>& leftJoinKeys,
    bool rightTablesReady, const TVector<TYtPathInfo::TPtr>& rightTables, const THashSet<TString>& rightJoinKeys,
    TYtJoinNodeLeaf* leftLeaf, TYtJoinNodeLeaf* rightLeaf, const TYtState& state, bool isCross,
    TString cluster, TExprContext& ctx)
{
    mapSettings = {};
    leftStats = {};
    rightStats = {};

    if (leftLeaf) {
        auto premap = GetPremapLambda(*leftLeaf);
        auto joinSideStatus = CollectJoinSideStats(leftTablesReady ? sizeMode : ESizeStatCollectMode::NoSize, leftStats, leftLeaf->Section, state, cluster,
                                                   leftTables, leftJoinKeys, isCross, premap, ctx);
        if (joinSideStatus.Level != TStatus::Ok) {
            return joinSideStatus;
        }

        if (leftTablesReady) {
            mapSettings.LeftRows = leftStats.RowsCount;
            mapSettings.LeftSize = leftStats.Size;
            mapSettings.LeftCount = leftTables.size();
            mapSettings.LeftUnique = leftStats.HasUniqueKeys && mapSettings.LeftCount == 1;
        }
    }

    if (rightLeaf) {
        auto premap = GetPremapLambda(*rightLeaf);
        auto joinSideStatus = CollectJoinSideStats(rightTablesReady ? sizeMode : ESizeStatCollectMode::NoSize, rightStats, rightLeaf->Section, state, cluster,
                                                   rightTables, rightJoinKeys, isCross, premap, ctx);
        if (joinSideStatus.Level != TStatus::Ok) {
            return joinSideStatus;
        }

        if (rightTablesReady) {
            mapSettings.RightRows = rightStats.RowsCount;
            mapSettings.RightSize = rightStats.Size;
            mapSettings.RightCount = rightTables.size();
            mapSettings.RightUnique = rightStats.HasUniqueKeys && mapSettings.RightCount == 1;
        }
    }

    if (sizeMode == ESizeStatCollectMode::RawSize) {
        mapSettings.LeftMemSize = mapSettings.LeftSize;
        mapSettings.RightMemSize = mapSettings.RightSize;
    }

    return TStatus::Ok;
}

TStatus RewriteYtEquiJoinLeaf(TYtEquiJoin equiJoin, TYtJoinNodeOp& op, TYtJoinNodeLeaf& leftLeaf,
                              TYtJoinNodeLeaf& rightLeaf, const TYtState::TPtr& state, TExprContext& ctx)
{
    TJoinLabels labels;

    bool leftTablesReady = false;
    TVector<TYtPathInfo::TPtr> leftTables;
    bool rightTablesReady = false;
    TVector<TYtPathInfo::TPtr> rightTables;
    const TStructExprType* leftItemType = nullptr;
    const TStructExprType* leftItemTypeBeforePremap = nullptr;
    const TStructExprType* rightItemType = nullptr;
    const TStructExprType* rightItemTypeBeforePremap = nullptr;

    {
        auto status = CollectPathsAndLabelsReady(leftTablesReady, leftTables, labels, leftItemType, leftItemTypeBeforePremap, leftLeaf, ctx);
        if (status != TStatus::Ok) {
            YQL_ENSURE(status.Level == TStatus::Error);
            return status;
        }

        status = CollectPathsAndLabelsReady(rightTablesReady, rightTables, labels, rightItemType, rightItemTypeBeforePremap, rightLeaf, ctx);
        if (status != TStatus::Ok) {
            YQL_ENSURE(status.Level == TStatus::Error);
            return status;
        }
    }

    const auto joinType = op.JoinKind->Content();
    const auto disableOptimizers = state->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>());

    bool empty = false;
    if (leftLeaf.Section.Ref().GetConstraint<TEmptyConstraintNode>() != nullptr
        && AllOf(leftTables, [](const TYtPathInfo::TPtr& p) { return !p->Table->Meta->IsDynamic; })
    ) {
        if (joinType == "Inner" || joinType == "Left" || joinType == "LeftOnly" || joinType == "LeftSemi" || joinType == "RightSemi" || joinType == "Cross") {
            empty = true;
            YQL_CLOG(INFO, ProviderYt) << "Left join side is empty";
        }
    }
    if (!empty
        && rightLeaf.Section.Ref().GetConstraint<TEmptyConstraintNode>() != nullptr
        && AllOf(rightTables, [](const TYtPathInfo::TPtr& p) { return !p->Table->Meta->IsDynamic; })
    ) {
        if (joinType == "Inner" || joinType == "Right" || joinType == "RightOnly" || joinType == "RightSemi" || joinType == "LeftSemi" || joinType == "Cross") {
            empty = true;
            YQL_CLOG(INFO, ProviderYt) << "Right join side is empty";
        }
    }
    if (empty) {
        return RewriteYtEmptyJoin(equiJoin, labels, op, leftLeaf, rightLeaf, state, ctx)
            ? TStatus::Ok
            : TStatus::Error;
    }

    const bool isCross = joinType == "Cross";
    const unsigned readyCount = unsigned(leftTablesReady) + rightTablesReady;
    if (isCross && readyCount < 2) {
        return TStatus::Repeat;
    }

    auto leftJoinKeys = BuildJoinKeys(labels.Inputs[0], *op.LeftLabel);
    auto rightJoinKeys = BuildJoinKeys(labels.Inputs[1], *op.RightLabel);
    auto leftJoinKeyList = BuildJoinKeyList(labels.Inputs[0], *op.LeftLabel);
    auto rightJoinKeyList = BuildJoinKeyList(labels.Inputs[1], *op.RightLabel);
    YQL_ENSURE(leftJoinKeys.size() <= leftJoinKeyList.size());
    YQL_ENSURE(rightJoinKeys.size() <= rightJoinKeyList.size());
    YQL_ENSURE(leftJoinKeyList.size() == rightJoinKeyList.size());
    if (!isCross) {
        YQL_CLOG(INFO, ProviderYt) << "leftJoinKeys: " << JoinSeq(",", leftJoinKeyList)
            << ", rightJoinKeys: " << JoinSeq(",", rightJoinKeyList);
    }

    auto const& linkSettings = op.LinkSettings;
    ui64 mergeTablesLimit = 0;
    TMaybe<bool> mergeUseSmallAsPrimary;
    double mergeUnsortedFactor = 0.2;
    bool forceMergeJoin = false;
    if (auto limit = state->Configuration->JoinMergeTablesLimit.Get()) {
        YQL_CLOG(INFO, ProviderYt) << "JoinMergeTablesLimit: " << *limit;
        mergeTablesLimit = *limit;

        if (mergeUseSmallAsPrimary = state->Configuration->JoinMergeUseSmallAsPrimary.Get()) {
            YQL_CLOG(INFO, ProviderYt) << "JoinMergeUseSmallAsPrimary: " << *mergeUseSmallAsPrimary;
        }

        if (auto factor = state->Configuration->JoinMergeUnsortedFactor.Get()) {
            YQL_CLOG(INFO, ProviderYt) << "JoinMergeUnsortedFactor: " << *factor;
            mergeUnsortedFactor = *factor;
        }

        if (auto force = state->Configuration->JoinMergeForce.Get()) {
            YQL_CLOG(INFO, ProviderYt) << "JoinMergeForce: " << *force;
            forceMergeJoin = *force;
        } else if (linkSettings.ForceSortedMerge) {
            YQL_CLOG(INFO, ProviderYt) << "Got forceSortedMerge from link settings";
            forceMergeJoin = true;
        }
    }
    if (!readyCount && !forceMergeJoin) {
        return TStatus::Repeat;
    }

    auto cluster = TString{equiJoin.DataSink().Cluster().Value()};

    TMapJoinSettings mapSettings;
    TJoinSideStats leftStats;
    TJoinSideStats rightStats;

    const bool allowLookupJoin = !isCross && leftTablesReady && rightTablesReady && !forceMergeJoin;
    if (allowLookupJoin) {
        auto status = CollectStatsAndMapJoinSettings(ESizeStatCollectMode::RawSize, mapSettings, leftStats, rightStats,
                                                     leftTablesReady, leftTables, leftJoinKeys, rightTablesReady, rightTables, rightJoinKeys,
                                                     &leftLeaf, &rightLeaf, *state, isCross, cluster, ctx);
        if (status.Level != TStatus::Ok) {
            return (status.Level == TStatus::Repeat) ? TStatus::Ok : status;
        }

        YQL_CLOG(INFO, ProviderYt) << "Considering LookupJoin: left table(s): "
            << leftStats.TableNames << " with size: " << mapSettings.LeftSize << ", rows: "
            << mapSettings.LeftRows << ", dynamic : " << leftStats.IsDynamic << ", right table(s): "
            << rightStats.TableNames << " with size: " << mapSettings.RightSize << ", rows: "
            << mapSettings.RightRows << ", dynamic : " << rightStats.IsDynamic;

        YQL_CLOG(INFO, ProviderYt) << "Join kind: " << op.JoinKind->Content() << ", left unique: " << leftStats.HasUniqueKeys
                                   << ", right unique: " << rightStats.HasUniqueKeys << ", left sorted prefix: ["
                                   << JoinSeq(",", leftStats.SortedKeys) << "], right sorted prefix: ["
                                   << JoinSeq(",", rightStats.SortedKeys) << "]";

        auto lookupJoinLimit = Min(state->Configuration->LookupJoinLimit.Get().GetOrElse(0),
                                   state->Configuration->EvaluationTableSizeLimit.Get().GetOrElse(Max<ui64>()));
        auto lookupJoinMaxRows = state->Configuration->LookupJoinMaxRows.Get().GetOrElse(0);

        bool isLeftSorted = leftStats.SortedKeys.size() >= leftJoinKeys.size();
        bool isRightSorted = rightStats.SortedKeys.size() >= rightJoinKeys.size();

        // TODO: support ANY for left side via Reduce with PartitionByKey
        bool leftAny = linkSettings.LeftHints.contains("any");
        bool rightAny = linkSettings.RightHints.contains("any");

        bool isLeftAllowLookupJoin = !leftAny && isLeftSorted && (mapSettings.RightRows < lookupJoinMaxRows) &&
                                     (mapSettings.RightMemSize < lookupJoinLimit) &&
                                     (joinType == "Inner" || joinType == "LeftSemi") && !rightStats.IsDynamic;

        bool isRightAllowLookupJoin = !rightAny && isRightSorted && (mapSettings.LeftRows < lookupJoinMaxRows) &&
                                      (mapSettings.LeftMemSize < lookupJoinLimit) &&
                                      (joinType == "Inner" || joinType == "RightSemi") && !leftStats.IsDynamic;

        YQL_CLOG(INFO, ProviderYt) << "LookupJoin: isLeftAllowLookupJoin: " << isLeftAllowLookupJoin
                                   << ", isRightAllowLookupJoin: " << isRightAllowLookupJoin;

        if (isLeftAllowLookupJoin || isRightAllowLookupJoin) {
            bool swapTables = false;
            if (isLeftAllowLookupJoin && isRightAllowLookupJoin) {
                swapTables = (mapSettings.LeftSize < mapSettings.RightSize);
            }
            else if (!isLeftAllowLookupJoin) {
                swapTables = true;
            }

            mapSettings.SwapTables = swapTables;

            if (swapTables) {
                DoSwap(mapSettings.LeftRows, mapSettings.RightRows);
                DoSwap(mapSettings.LeftSize, mapSettings.RightSize);
                DoSwap(mapSettings.LeftMemSize, mapSettings.RightMemSize);
                DoSwap(mapSettings.LeftUnique, mapSettings.RightUnique);
                YQL_CLOG(INFO, ProviderYt) << "Selected LookupJoin: filter over the right table, use content of the left one";
                return RewriteYtMapJoin(equiJoin, labels, true, op, rightLeaf, leftLeaf, ctx, mapSettings, false, state) ?
                       TStatus::Ok : TStatus::Error;
            } else {
                YQL_CLOG(INFO, ProviderYt) << "Selected LookupJoin: filter over the left table, use content of the right one";
                return RewriteYtMapJoin(equiJoin, labels, true, op, leftLeaf, rightLeaf, ctx, mapSettings, false, state) ?
                       TStatus::Ok : TStatus::Error;
            }
        }
    }

    {
        auto status = CollectStatsAndMapJoinSettings(ESizeStatCollectMode::ColumnarSize, mapSettings, leftStats, rightStats,
                                                    leftTablesReady, leftTables, leftJoinKeys, rightTablesReady, rightTables, rightJoinKeys,
                                                    &leftLeaf, &rightLeaf, *state, isCross, cluster, ctx);
        if (status.Level != TStatus::Ok) {
            return (status.Level == TStatus::Repeat) ? TStatus::Ok : status;
        }
    }

    YQL_CLOG(INFO, ProviderYt) << "Left table(s): "
        << (leftTablesReady ? leftStats.TableNames : "(not ready)") << " with size: " << mapSettings.LeftSize << ", rows: "
        << mapSettings.LeftRows << ", dynamic : " << leftStats.IsDynamic << ", right table(s): "
        << (rightTablesReady ? rightStats.TableNames : "(not ready)") << " with size: " << mapSettings.RightSize << ", rows: "
        << mapSettings.RightRows << ", dynamic : " << rightStats.IsDynamic;

    YQL_CLOG(INFO, ProviderYt) << "Join kind: " << op.JoinKind->Content() << ", left hints: " <<
        (linkSettings.LeftHints ? JoinSeq(",", linkSettings.LeftHints) :  "none") << ", right hints: " <<
        (linkSettings.RightHints ? JoinSeq(",", linkSettings.RightHints) :  "none") << ", left unique: " << leftStats.HasUniqueKeys
        << ", right unique: " << rightStats.HasUniqueKeys << ", left sorted prefix: ["
        << JoinSeq(",", leftStats.SortedKeys) << "], right sorted prefix: ["
        << JoinSeq(",", rightStats.SortedKeys) << "]";

    bool allowOrderedJoin = !isCross && ((leftTablesReady && rightTablesReady) || forceMergeJoin);

    TMergeJoinSortInfo sortInfo;
    sortInfo.LeftSortedKeys = leftStats.SortedKeys;
    sortInfo.RightSortedKeys = rightStats.SortedKeys;

    sortInfo.LeftBeforePremap = leftItemTypeBeforePremap;
    sortInfo.RightBeforePremap = rightItemTypeBeforePremap;

    if (allowOrderedJoin) {
        bool isLeftSorted = sortInfo.LeftSortedKeys.size() >= leftJoinKeys.size();
        bool isRightSorted = sortInfo.RightSortedKeys.size() >= rightJoinKeys.size();

        if (allowOrderedJoin && !isLeftSorted && !isRightSorted && !forceMergeJoin) {
            YQL_CLOG(INFO, ProviderYt) << "Skipped OrderedJoin, because both sides are unsorted";
            allowOrderedJoin = false;
        }

        if (allowOrderedJoin && (leftJoinKeys.size() != leftJoinKeyList.size() ||
                                 rightJoinKeys.size() != rightJoinKeyList.size()))
        {
            YQL_CLOG(INFO, ProviderYt) << "Skipped OrderedJoin, because side(s) contain duplicate join keys";
            allowOrderedJoin = false;
        }

        if (allowOrderedJoin && !forceMergeJoin && (mapSettings.LeftCount + mapSettings.RightCount > mergeTablesLimit)) {
            YQL_CLOG(INFO, ProviderYt) << "Skipped OrderedJoin, because there are too many tables, mergeTablesLimit: " << mergeTablesLimit;
            allowOrderedJoin = false;
        }

        if (allowOrderedJoin && (!isLeftSorted || !isRightSorted)) {
            if (!isLeftSorted && !isRightSorted) {
                YQL_ENSURE(forceMergeJoin);
                sortInfo.AdditionalSort = TChoice::Both;
                sortInfo.LeftSortedKeys = leftJoinKeyList;
                sortInfo.RightSortedKeys = BuildCompatibleSortWith(sortInfo.LeftSortedKeys, leftJoinKeyList, rightJoinKeyList);
            } else if (!isRightSorted) {
                if (!forceMergeJoin && mapSettings.RightSize > mapSettings.LeftSize * mergeUnsortedFactor) {
                    YQL_CLOG(INFO, ProviderYt) << "Skipped OrderedJoin, because unsorted right table is too big";
                    allowOrderedJoin = false;
                } else {
                    sortInfo.AdditionalSort = TChoice::Right;
                    sortInfo.RightSortedKeys = BuildCompatibleSortWith(sortInfo.LeftSortedKeys, leftJoinKeyList, rightJoinKeyList);
                }
            } else {
                if (!forceMergeJoin && mapSettings.LeftSize > mapSettings.RightSize * mergeUnsortedFactor) {
                    YQL_CLOG(INFO, ProviderYt) << "Skipped OrderedJoin, because unsorted left table is too big";
                    allowOrderedJoin = false;
                } else {
                    sortInfo.AdditionalSort = TChoice::Left;
                    sortInfo.LeftSortedKeys = BuildCompatibleSortWith(sortInfo.RightSortedKeys, rightJoinKeyList, leftJoinKeyList);
                }
            }

            if (allowOrderedJoin && !isLeftSorted) {
                isLeftSorted = true;
                YQL_ENSURE(sortInfo.LeftSortedKeys.size() == leftJoinKeys.size());
                if (leftStats.NeedsRemap) {
                    sortInfo.NeedRemapBeforeSort = Merge(sortInfo.NeedRemapBeforeSort, TChoice::Left);
                }
                YQL_CLOG(INFO, ProviderYt) << "Added sort of the left table"
                                           << (leftStats.NeedsRemap ? " with additional remapping" : "");
            }

            if (allowOrderedJoin && !isRightSorted) {
                isRightSorted = true;
                YQL_ENSURE(sortInfo.RightSortedKeys.size() == rightJoinKeys.size());
                if (rightStats.NeedsRemap) {
                    sortInfo.NeedRemapBeforeSort = Merge(sortInfo.NeedRemapBeforeSort, TChoice::Right);
                }
                YQL_CLOG(INFO, ProviderYt) << "Added sort of the right table"
                                           << (rightStats.NeedsRemap ? " with additional remapping" : "");
            }
        }

        if (allowOrderedJoin) {
            YQL_ENSURE(isLeftSorted);
            YQL_ENSURE(isRightSorted);

            YQL_ENSURE(sortInfo.LeftSortedKeys.size() >= leftJoinKeys.size());
            YQL_ENSURE(sortInfo.RightSortedKeys.size() >= rightJoinKeys.size());

            // TODO: in some cases we can allow output to be sorted more strictly
            sortInfo.LeftSortedKeys.resize(leftJoinKeys.size());
            sortInfo.RightSortedKeys.resize(rightJoinKeys.size());

            const bool allowColumnRenames = state->Configuration->JoinAllowColumnRenames.Get().GetOrElse(true)
                // TODO: remove next line after https://st.yandex-team.ru/YT-12738 is fixed
                && !leftStats.IsDynamic && !rightStats.IsDynamic;

            sortInfo.CommonSortedKeys = BuildCommonSortPrefix(sortInfo.LeftSortedKeys, sortInfo.RightSortedKeys,
                leftJoinKeyList, rightJoinKeyList, sortInfo.LeftKeyRenames, sortInfo.RightKeyRenames, allowColumnRenames);

            YQL_CLOG(INFO, ProviderYt) << "Common sorted prefix (with JoinAllowColumnRenames: " << allowColumnRenames << ") is ["
                << JoinSeq(", ", sortInfo.CommonSortedKeys) << "], left sorted prefix: ["
                << JoinSeq(", ", sortInfo.LeftSortedKeys) << "], right sorted prefix: ["
                << JoinSeq(", ", sortInfo.RightSortedKeys) << "], left renames: ["
                << RenamesToString(sortInfo.LeftKeyRenames) << "], right renames: ["
                << RenamesToString(sortInfo.RightKeyRenames) << "]";

            if (sortInfo.CommonSortedKeys.size() < leftJoinKeys.size()) {
                YQL_CLOG(INFO, ProviderYt) << "Skipped OrderedJoin, because table sort prefixes are incompatible with "
                                              "join keys";
                allowOrderedJoin = false;
            }
        }
    }

    if (allowOrderedJoin) {
        if (joinType.EndsWith("Semi")) {
            const bool leftSemi = joinType == "LeftSemi";
            const auto& dictLabel = leftSemi ? labels.Inputs[1] : labels.Inputs[0];
            auto& dictHints = leftSemi ? op.LinkSettings.RightHints : op.LinkSettings.LeftHints;

            op.JoinKind = ctx.NewAtom(op.JoinKind->Pos(), "Inner");
            dictHints.insert("any");

            auto columnsToRemove = dictLabel.EnumerateAllColumns();
            op.OutputRemoveColumns.insert(columnsToRemove.begin(), columnsToRemove.end());

            YQL_CLOG(INFO, ProviderYt) << "Will rewrite ordered " << joinType << " to ANY Inner + remove columns ["
                                       << JoinSeq(", ", columnsToRemove) << "]";
            return TStatus::Ok;
        }

        bool allowPrimaryLeft = (joinType == "Inner" || joinType == "Left" || joinType == "LeftOnly");
        bool allowPrimaryRight = (joinType == "Inner" || joinType == "Right" || joinType == "RightOnly");

        bool swapTables = false;
        bool useJoinReduce = false;
        bool useJoinReduceForSecond = false;
        bool tryFirstAsPrimary = false;

        if (allowPrimaryLeft != allowPrimaryRight) {
            swapTables = allowPrimaryLeft;
            auto primary = swapTables ? TChoice::Left : TChoice::Right;
            useJoinReduce = !HasNonTrivialAny(linkSettings, mapSettings, primary);
        } else if (allowPrimaryLeft) {
            YQL_ENSURE(allowPrimaryRight);
            // both tables can be chosen as primary
            bool biggerHasUniqueKeys = mapSettings.RightSize > mapSettings.LeftSize ?
                                       mapSettings.RightUnique : mapSettings.LeftUnique;

            if (biggerHasUniqueKeys) {
                // it is safe to use smaller table as primary
                swapTables = mapSettings.RightSize > mapSettings.LeftSize;
            } else if (mergeUseSmallAsPrimary) {
                // explicit setting
                if (*mergeUseSmallAsPrimary) {
                    // use smaller table as primary
                    swapTables = mapSettings.RightSize > mapSettings.LeftSize;
                } else {
                    // use bigger table as primary
                    swapTables = mapSettings.LeftSize > mapSettings.RightSize;
                }
            } else {
                // make bigger table last one, and try first (smaller) as primary
                swapTables = mapSettings.LeftSize > mapSettings.RightSize;
                tryFirstAsPrimary = true;
            }

            auto primary = swapTables ? TChoice::Left : TChoice::Right;
            if (tryFirstAsPrimary) {
                useJoinReduceForSecond = !HasNonTrivialAny(linkSettings, mapSettings, primary);
                useJoinReduce = !HasNonTrivialAny(linkSettings, mapSettings, Invert(primary));
            } else {
                useJoinReduce = !HasNonTrivialAny(linkSettings, mapSettings, primary);
            }
        } else {
            // try to move non-fat table to the left, otherwise keep them as is
            if (mapSettings.LeftUnique != mapSettings.RightUnique) {
                swapTables = mapSettings.RightUnique;
            } else {
                swapTables = mapSettings.LeftSize > mapSettings.RightSize;
            }
        }

        YQL_CLOG(INFO, ProviderYt) << "OrderedJoin allowPrimaryLeft : " << allowPrimaryLeft
            << ", allowPrimaryRight " << allowPrimaryRight;

        YQL_CLOG(INFO, ProviderYt) << "Selected OrderedJoin over the " << (swapTables ? "left" : "right")
            << " table as primary, will use join reduce: " << useJoinReduce << ", will try other primary: " << tryFirstAsPrimary
            << ", will use join reduce for other primary: " << useJoinReduceForSecond;

        bool skipped = false;
        if (!RewriteYtMergeJoin(equiJoin, labels, op,
                                swapTables ? rightLeaf : leftLeaf,
                                swapTables ? leftLeaf : rightLeaf,
                                state, ctx, swapTables, useJoinReduce, tryFirstAsPrimary, useJoinReduceForSecond,
                                swapTables ? Invert(sortInfo) : sortInfo,
                                skipped))
        {
            return TStatus::Error;
        }

        if (!skipped) {
            return TStatus::Ok;
        }
    }

    if (auto mapJoinLimit = state->Configuration->MapJoinLimit.Get(); mapJoinLimit && !forceMergeJoin) {
        YQL_CLOG(INFO, ProviderYt) << "MapJoinLimit: " << *mapJoinLimit;
        mapSettings.MapJoinLimit = *mapJoinLimit;

        if (mapSettings.MapJoinLimit) {
            mapSettings.MapJoinShardMinRows = state->Configuration->MapJoinShardMinRows.Get().GetOrElse(1);
            mapSettings.MapJoinShardCount = state->Configuration->MapJoinShardCount.Get().GetOrElse(1);

            ui64 rightLimit = mapSettings.MapJoinLimit;
            ui64 leftLimit = mapSettings.MapJoinLimit;
            auto leftPartSize = mapSettings.CalculatePartSize(mapSettings.LeftRows);
            auto rightPartSize = mapSettings.CalculatePartSize(mapSettings.RightRows);
            TMaybe<ui64> leftPartCount;
            TMaybe<ui64> rightPartCount;
            if (leftPartSize) {
                YQL_ENSURE(leftTablesReady);
                leftPartCount = (mapSettings.LeftRows + *leftPartSize - 1) / *leftPartSize;
            }

            if (rightPartSize) {
                YQL_ENSURE(rightTablesReady);
                rightPartCount = (mapSettings.RightRows + *rightPartSize - 1) / *rightPartSize;
            }

            bool allowShardRight = false;
            const bool rightUnique = linkSettings.RightHints.contains("unique") || mapSettings.RightUnique;
            const bool denyShardRight = linkSettings.RightHints.contains("any") && !rightUnique;
            // TODO: currently we disable sharding when other side is not ready
            if (leftTablesReady && rightPartCount && !denyShardRight && ((joinType == "Inner") || (joinType == "Cross") ||
                (joinType == "LeftSemi" && rightUnique))) {
                allowShardRight = true;
                rightLimit *= *rightPartCount;
            }

            bool allowShardLeft = false;
            const bool leftUnique = linkSettings.LeftHints.contains("unique") || mapSettings.LeftUnique;
            const bool denyShardLeft = linkSettings.LeftHints.contains("any") && !leftUnique;
            // TODO: currently we disable sharding when other side is not ready
            if (rightTablesReady && leftPartCount && !denyShardLeft && ((joinType == "Inner") || (joinType == "Cross") ||
                (joinType == "RightSemi" && leftUnique))) {
                allowShardLeft = true;
                leftLimit *= *leftPartCount;
            }

            auto mapJoinUseFlow = state->Configuration->MapJoinUseFlow.Get().GetOrElse(DEFAULT_MAP_JOIN_USE_FLOW);
            if (leftTablesReady) {
                auto status = UpdateInMemorySizeSetting(mapSettings, leftLeaf.Section, labels, op, ctx, true, leftItemType, leftJoinKeyList, state, cluster, leftTables, mapJoinUseFlow);
                if (status.Level != TStatus::Ok) {
                    return (status.Level == TStatus::Repeat) ? TStatus::Ok : status;
                }
            }

            if (rightTablesReady) {
                auto status = UpdateInMemorySizeSetting(mapSettings, rightLeaf.Section, labels, op, ctx, false, rightItemType, rightJoinKeyList, state, cluster, rightTables, mapJoinUseFlow);
                if (status.Level != TStatus::Ok) {
                    return (status.Level == TStatus::Repeat) ? TStatus::Ok : status;
                }
            }

            YQL_CLOG(INFO, ProviderYt) << "MapJoinShardMinRows: " << mapSettings.MapJoinShardMinRows
                << ", MapJoinShardCount: " << mapSettings.MapJoinShardCount
                << ", left is present: " << leftTablesReady
                << ", left size limit: " << leftLimit << ", left mem size:" << mapSettings.LeftMemSize
                << ", right is present: " << rightTablesReady
                << ", right size limit: " << rightLimit << ", right mem size: " << mapSettings.RightMemSize;

            bool leftAny = linkSettings.LeftHints.contains("any");
            bool rightAny = linkSettings.RightHints.contains("any");

            const bool isLeftAllowMapJoin = !leftAny && rightTablesReady && (mapSettings.RightMemSize <= rightLimit) &&
                (joinType == "Inner" || joinType == "Left" || joinType == "LeftOnly" || joinType == "LeftSemi" || joinType == "Cross")
                && !rightStats.IsDynamic;
            const bool isRightAllowMapJoin = !rightAny && leftTablesReady && (mapSettings.LeftMemSize <= leftLimit) &&
                (joinType == "Inner" || joinType == "Right" || joinType == "RightOnly" || joinType == "RightSemi" || joinType == "Cross")
                && !leftStats.IsDynamic;
            YQL_CLOG(INFO, ProviderYt) << "MapJoin: isLeftAllowMapJoin: " << isLeftAllowMapJoin
                << ", isRightAllowMapJoin: " << isRightAllowMapJoin;

            if (isLeftAllowMapJoin || isRightAllowMapJoin) {
                bool swapTables = false;
                if (isLeftAllowMapJoin && isRightAllowMapJoin) {
                    swapTables = (mapSettings.LeftSize < mapSettings.RightSize);
                }
                else if (!isLeftAllowMapJoin) {
                    swapTables = true;
                }

                mapSettings.SwapTables = swapTables;

                if (swapTables) {
                    DoSwap(mapSettings.LeftRows, mapSettings.RightRows);
                    DoSwap(mapSettings.LeftSize, mapSettings.RightSize);
                    DoSwap(mapSettings.LeftMemSize, mapSettings.RightMemSize);
                    DoSwap(mapSettings.LeftUnique, mapSettings.RightUnique);
                    YQL_CLOG(INFO, ProviderYt) << "Selected MapJoin: map over the right table, use content of the left one";
                    return RewriteYtMapJoin(equiJoin, labels, false, op, rightLeaf, leftLeaf, ctx, mapSettings, allowShardLeft, state) ?
                           TStatus::Ok : TStatus::Error;
                } else {
                    YQL_CLOG(INFO, ProviderYt) << "Selected MapJoin: map over the left table, use content of the right one";
                    return RewriteYtMapJoin(equiJoin, labels, false, op, leftLeaf, rightLeaf, ctx, mapSettings, allowShardRight, state) ?
                           TStatus::Ok : TStatus::Error;
                }
            }
        }
    }

    if (leftTablesReady && rightTablesReady) {
        YQL_CLOG(INFO, ProviderYt) << "Selected CommonJoin";
        return RewriteYtCommonJoin(equiJoin, labels, op, leftLeaf, rightLeaf, state, ctx, leftStats.HasUniqueKeys,
                                   rightStats.HasUniqueKeys, mapSettings.LeftSize, mapSettings.RightSize) ?
               TStatus::Ok : TStatus::Error;
    } else {
        return TStatus::Repeat;
    }
}

TExprBase MakePremap(const TYtJoinNodeLeaf& leaf, TExprContext& ctx) {
    if (leaf.Premap) {
        return leaf.Premap.Cast();
    }
    return Build<TCoVoid>(ctx, leaf.Section.Pos()).Done();
}

TExprNode::TPtr ExportJoinTree(const TYtJoinNodeOp& op, TPositionHandle pos, TVector<TYtSection>& inputSections,
    TVector<TExprBase>& premaps, THashSet<TString>& outputRemoveColumns, TExprContext& ctx)
{
    TExprNode::TPtr left;
    if (auto leaf = dynamic_cast<const TYtJoinNodeLeaf*>(op.Left.Get())) {
        left = ctx.NewAtom(pos, op.Left->Scope[0]);
        inputSections.push_back(leaf->Section);
        premaps.push_back(MakePremap(*leaf, ctx));
    } else {
        left = ExportJoinTree(*dynamic_cast<const TYtJoinNodeOp*>(op.Left.Get()), pos, inputSections, premaps,
            outputRemoveColumns, ctx);
    }

    TExprNode::TPtr right;
    if (auto leaf = dynamic_cast<TYtJoinNodeLeaf*>(op.Right.Get())) {
        right = ctx.NewAtom(pos, op.Right->Scope[0]);
        inputSections.push_back(leaf->Section);
        premaps.push_back(MakePremap(*leaf, ctx));
    }
    else {
        right = ExportJoinTree(*dynamic_cast<const TYtJoinNodeOp*>(op.Right.Get()), pos, inputSections, premaps,
            outputRemoveColumns, ctx);
    }

    outputRemoveColumns.insert(op.OutputRemoveColumns.begin(), op.OutputRemoveColumns.end());

    return ctx.NewList(pos, {
        op.JoinKind,
        left,
        right,
        op.LeftLabel,
        op.RightLabel,
        BuildEquiJoinLinkSettings(op.LinkSettings, ctx),
    });
}

void CombineJoinStatus(TStatus& status, TStatus other) {
    YQL_ENSURE(status == TStatus::Repeat || status == TStatus::Ok || status == TStatus::Error);

    switch (other.Level) {
    case TStatus::Error:
        status = TStatus::Error;
        break;
    case TStatus::Ok:
        if (status != TStatus::Error) {
            status = TStatus::Ok;
        }
        break;
    case TStatus::Repeat:
        break;
    default:
        YQL_ENSURE(false, "Unexpected join status");
    }
}

enum class EStarRewriteStatus {
    WaitInput,
    None,
    Ok,
    Error,
};

bool IsJoinKindCompatibleWithStar(TStringBuf kind) {
    return kind == "Inner" ||
           kind == "Left"  || kind == "LeftSemi"  || kind == "LeftOnly" ||
           kind == "Right" || kind == "RightSemi" || kind == "RightOnly";
}

bool IsSideSuitableForStarJoin(TStringBuf joinKind, const TEquiJoinLinkSettings& linkSettings, const TMapJoinSettings& mapJoinSettings, bool isLeft)
{
    YQL_ENSURE(IsJoinKindCompatibleWithStar(joinKind));

    if (joinKind == (isLeft ? "Left" : "Right") || joinKind == "Inner")
    {
        // other side should be unique
        return IsEffectivelyUnique(linkSettings, mapJoinSettings, !isLeft);
    } else if (joinKind.StartsWith(isLeft ? "Left" : "Right")) {
        return true;
    }

    return false;
}

bool ExtractJoinKeysForStarJoin(const TExprNode& labelNode, TString& label, TVector<TString>& keyList) {
    YQL_ENSURE(labelNode.ChildrenSize() > 0);
    YQL_ENSURE(labelNode.ChildrenSize() % 2 == 0);
    label = {};
    keyList = {};
    keyList.reserve(labelNode.ChildrenSize() / 2);

    for (size_t i = 0; i < labelNode.ChildrenSize(); i += 2) {
        YQL_ENSURE(labelNode.Child(i)->IsAtom());
        YQL_ENSURE(labelNode.Child(i + 1)->IsAtom());
        auto table = labelNode.Child(i)->Content();
        auto key = labelNode.Child(i + 1)->Content();

        if (!label) {
            label = table;
        } else if (label != table) {
            return false;
        }

        keyList.emplace_back(key);
    }
    return true;
}

TVector<const TTypeAnnotationNode*> BuildJoinKeyType(const TStructExprType& input, const TVector<TString>& keys) {
    TVector<const TTypeAnnotationNode*> result;
    result.reserve(keys.size());
    for (auto& key : keys) {
        auto maybeIndex = input.FindItem(key);
        YQL_ENSURE(maybeIndex);
        result.push_back(input.GetItems()[*maybeIndex]->GetItemType());
    }
    return result;
}

constexpr auto joinFixedArgsCount = 7U;

const TStructExprType* GetJoinInputType(TYtEquiJoin equiJoin, size_t inputIndex, TExprContext& ctx) {
    const auto premap = TMaybeNode<TCoLambda>(equiJoin.Ref().ChildPtr(joinFixedArgsCount + inputIndex));
    TExprBase input = premap ? TExprBase(premap.Cast()) : TExprBase(equiJoin.Input().Item(inputIndex));
    if (auto type = GetSequenceItemType(input, false, ctx)) {
        return type->Cast<TStructExprType>();
    }
    return nullptr;
}

void CollectPossibleStarJoins(const TYtEquiJoin& equiJoin, TYtJoinNodeOp& op, const TYtState::TPtr& state, EStarRewriteStatus& collectStatus, TExprContext& ctx) {
    YQL_ENSURE(!op.StarOptions);
    if (collectStatus != EStarRewriteStatus::Ok) {
        return;
    }

    auto leftLeaf  = dynamic_cast<TYtJoinNodeLeaf*>(op.Left.Get());
    auto rightLeaf = dynamic_cast<TYtJoinNodeLeaf*>(op.Right.Get());

    auto leftOp    = dynamic_cast<TYtJoinNodeOp*>(op.Left.Get());
    auto rightOp   = dynamic_cast<TYtJoinNodeOp*>(op.Right.Get());

    if (!leftLeaf) {
        YQL_ENSURE(leftOp);
        CollectPossibleStarJoins(equiJoin, *leftOp, state, collectStatus, ctx);
    }

    if (!rightLeaf) {
        YQL_ENSURE(rightOp);
        CollectPossibleStarJoins(equiJoin, *rightOp, state, collectStatus, ctx);
    }

    if (collectStatus != EStarRewriteStatus::Ok) {
        return;
    }

    if (!leftLeaf && !rightLeaf) {
        // join of two joins is never a star join
        return;
    }

    auto joinKind = op.JoinKind->Content();
    if (!IsJoinKindCompatibleWithStar(joinKind) ||
        (leftLeaf && leftLeaf->Scope.size() != 1) ||
        (rightLeaf && rightLeaf->Scope.size() != 1))
    {
        return;
    }

    YQL_ENSURE(!leftLeaf || leftLeaf->Label->IsAtom());
    YQL_ENSURE(!rightLeaf || rightLeaf->Label->IsAtom());

    TJoinLabels labels;

    bool leftTablesReady = false;
    TVector<TYtPathInfo::TPtr> leftTables;
    bool rightTablesReady = false;
    TVector<TYtPathInfo::TPtr> rightTables;
    const TStructExprType* leftItemType = nullptr;
    const TStructExprType* leftItemTypeBeforePremap = nullptr;
    const TStructExprType* rightItemType = nullptr;
    const TStructExprType* rightItemTypeBeforePremap = nullptr;

    THashSet<TString> leftJoinKeys;
    THashSet<TString> rightJoinKeys;

    TVector<TString> leftJoinKeyList;
    TVector<TString> rightJoinKeyList;

    if (leftLeaf) {
        leftTablesReady = true;
        auto status = CollectPathsAndLabels(leftTables, labels, leftItemType, leftItemTypeBeforePremap, *leftLeaf, ctx);
        if (status != TStatus::Ok) {
            YQL_ENSURE(status.Level == TStatus::Error);
            collectStatus = EStarRewriteStatus::Error;
            return;
        }

        leftJoinKeys = BuildJoinKeys(labels.Inputs[0], *op.LeftLabel);
        leftJoinKeyList = BuildJoinKeyList(labels.Inputs[0], *op.LeftLabel);
        YQL_ENSURE(leftJoinKeys.size() <= leftJoinKeyList.size());
    }

    if (rightLeaf) {
        rightTablesReady = true;
        auto status = CollectPathsAndLabels(rightTables, labels, rightItemType, rightItemTypeBeforePremap, *rightLeaf, ctx);
        if (status != TStatus::Ok) {
            YQL_ENSURE(status.Level == TStatus::Error);
            collectStatus = EStarRewriteStatus::Error;
            return;
        }

        rightJoinKeys = BuildJoinKeys(labels.Inputs[leftLeaf ? 1 : 0], *op.RightLabel);
        rightJoinKeyList = BuildJoinKeyList(labels.Inputs[leftLeaf ? 1 : 0], *op.RightLabel);
    }


    auto cluster = TString{equiJoin.DataSink().Cluster().Value()};

    TMapJoinSettings mapSettings;
    TJoinSideStats leftStats;
    TJoinSideStats rightStats;

    {
        bool isCross = false;
        auto status = CollectStatsAndMapJoinSettings(ESizeStatCollectMode::NoSize, mapSettings, leftStats, rightStats,
                                                     leftTablesReady, leftTables, leftJoinKeys, rightTablesReady, rightTables, rightJoinKeys,
                                                     leftLeaf, rightLeaf, *state, isCross, cluster, ctx);

        switch (status.Level) {
        case TStatus::Error:
            collectStatus = EStarRewriteStatus::Error;
            break;
        case TStatus::Ok:
            break;
        default:
            YQL_ENSURE(false, "Unexpected collect stats status");
        }

        if (collectStatus != EStarRewriteStatus::Ok) {
            return;
        }
    }

    if (leftLeaf) {
        if (leftStats.SortedKeys.size() < leftJoinKeys.size()) {
            // left is not sorted
            return;
        }

        if (leftJoinKeyList.size() != leftJoinKeys.size()) {
            // right side contains duplicate join keys
            return;
        }
    }

    if (rightLeaf) {
        if (rightStats.SortedKeys.size() < rightJoinKeys.size()) {
            // right is not sorted
            return;
        }

        if (rightJoinKeyList.size() != rightJoinKeys.size()) {
            // right side contains duplicate join keys
            return;
        }
    }

    auto addStarOption = [&](bool isLeft) {

        const auto& joinKeys = isLeft ? leftJoinKeys : rightJoinKeys;

        TYtStarJoinOption starJoinOption;
        starJoinOption.StarKeys.insert(joinKeys.begin(), joinKeys.end());
        starJoinOption.StarInputIndex = isLeft ? leftLeaf->Index : rightLeaf->Index;
        starJoinOption.StarLabel = isLeft ? leftLeaf->Label->Content() : rightLeaf->Label->Content();
        starJoinOption.StarSortedKeys = isLeft ? leftStats.SortedKeys : rightStats.SortedKeys;

        YQL_CLOG(INFO, ProviderYt) << "Adding " << (isLeft ? rightLeaf->Label->Content() : leftLeaf->Label->Content())
                                   << " [" << JoinSeq(", ", isLeft ? rightJoinKeyList : leftJoinKeyList)
                                   << "] to star " << starJoinOption.StarLabel << " [" << JoinSeq(", ", starJoinOption.StarKeys)
                                   << "]";

        op.StarOptions.emplace_back(std::move(starJoinOption));
    };

    if (leftLeaf && rightLeaf) {
        YQL_ENSURE(leftLeaf->Label->Content() != rightLeaf->Label->Content());

        YQL_ENSURE(leftItemType && rightItemType);
        auto inputKeyTypeLeft = BuildJoinKeyType(*leftItemType, leftJoinKeyList);
        auto inputKeyTypeRight = BuildJoinKeyType(*rightItemType, rightJoinKeyList);
        if (!IsSameAnnotation(*AsDictKeyType(RemoveNullsFromJoinKeyType(inputKeyTypeLeft), ctx),
                              *AsDictKeyType(RemoveNullsFromJoinKeyType(inputKeyTypeRight), ctx)))
        {
            // key types should match for merge star join to work
            return;
        }

        if (IsSideSuitableForStarJoin(joinKind, op.LinkSettings, mapSettings, true)) {
            addStarOption(true);
        }

        if (IsSideSuitableForStarJoin(joinKind, op.LinkSettings, mapSettings, false)) {
            addStarOption(false);
        }
    } else {

        auto childOp = leftLeaf ? rightOp : leftOp;

        bool allowNonUnique = joinKind.EndsWith("Semi") || joinKind.EndsWith("Only");
        bool allowKind = true;
        if (joinKind.StartsWith("Left")) {
            allowKind = leftLeaf == nullptr;
        } else if (joinKind.StartsWith("Right")) {
            allowKind = leftLeaf != nullptr;
        }

        if (childOp->StarOptions && allowKind && (allowNonUnique || IsEffectivelyUnique(op.LinkSettings, mapSettings, leftLeaf != nullptr))) {

            const auto& leafJoinKeyList = leftLeaf ? leftJoinKeyList : rightJoinKeyList;
            TString leafLabel = leftLeaf ? TString{leftLeaf->Label->Content()} : TString{rightLeaf->Label->Content()};

            auto inputKeyTypeLeaf = BuildJoinKeyType(leftLeaf ? *leftItemType : *rightItemType, leafJoinKeyList);

            TString childLabel;
            TVector<TString> childKeyList;

            if (ExtractJoinKeysForStarJoin(leftLeaf ? *op.RightLabel : *op.LeftLabel, childLabel, childKeyList)) {
                TSet<TString> childKeys(childKeyList.begin(), childKeyList.end());
                for (const auto& childOption : childOp->StarOptions) {
                    if (leafLabel == childOption.StarLabel || childLabel != childOption.StarLabel ||
                        childKeys != childOption.StarKeys)
                    {
                        continue;
                    }

                    auto starInputType = GetJoinInputType(equiJoin, childOption.StarInputIndex, ctx);
                    YQL_ENSURE(starInputType);
                    auto inputKeyTypeChild = BuildJoinKeyType(*starInputType, childKeyList);

                    if (!IsSameAnnotation(*AsDictKeyType(RemoveNullsFromJoinKeyType(inputKeyTypeChild), ctx),
                                          *AsDictKeyType(RemoveNullsFromJoinKeyType(inputKeyTypeLeaf), ctx)))
                    {
                        // key types should match for merge star join to work
                        return;
                    }


                    TYtStarJoinOption option = childOption;
                    YQL_CLOG(INFO, ProviderYt) << "Adding " << leafLabel << " [" << JoinSeq(", ", leafJoinKeyList)
                                               << "] to star " << option.StarLabel << " [" << JoinSeq(", ", option.StarKeys)
                                               << "]";

                    op.StarOptions.emplace_back(option);
                }
                YQL_ENSURE(op.StarOptions.size() <= 1);
            }
        }
    }
}

const TStructExprType* AddLabelToStructType(const TStructExprType& input, TStringBuf label, TExprContext& ctx) {
    TVector<const TItemExprType*> structItems;
    for (auto& i : input.GetItems()) {
        structItems.push_back(ctx.MakeType<TItemExprType>(FullColumnName(label, i->GetName()), i->GetItemType()));
    }
    return ctx.MakeType<TStructExprType>(structItems);
}

const TStructExprType* MakeStructMembersOptional(const TStructExprType& input, TExprContext& ctx) {
    TVector<const TItemExprType*> structItems;
    for (auto& i : input.GetItems()) {
        if (i->GetItemType()->GetKind() == ETypeAnnotationKind::Optional) {
            structItems.push_back(i);
        } else {
            structItems.push_back(ctx.MakeType<TItemExprType>(i->GetName(), ctx.MakeType<TOptionalExprType>(i->GetItemType())));
        }
    }
    return ctx.MakeType<TStructExprType>(structItems);
}

EStarRewriteStatus RewriteYtEquiJoinStarSingleChain(TYtEquiJoin equiJoin, TYtJoinNodeOp& op, const TYtState::TPtr& state, TExprContext& ctx) {

    YQL_ENSURE(op.StarOptions.size() == 1);
    const auto& starOption = op.StarOptions.front();

    auto starLabel = starOption.StarLabel;
    const auto& starKeys = starOption.StarKeys;
    const auto& starSortedKeys = starOption.StarSortedKeys;
    const auto starInputIndex = starOption.StarInputIndex;
    const auto starPremap = TMaybeNode<TCoLambda>(equiJoin.Ref().ChildPtr(joinFixedArgsCount + starInputIndex));

    auto starInputType = GetJoinInputType(equiJoin, starInputIndex, ctx);
    if (!starInputType) {
        return EStarRewriteStatus::Error;
    }

    THashMap<TString, TString> starRenames;
    TSet<TString> renamedStarKeys;
    TVector<TString> renamedStarSortedKeys;
    {
        size_t idx = 0;
        for (auto& c : starOption.StarKeys) {
            auto renamed = TStringBuilder() << "_yql_star_join_column_" << idx++;
            starRenames[c] = renamed;
            renamedStarKeys.insert(renamed);
        }

        for (auto& c : starOption.StarSortedKeys) {
            if (!starKeys.contains(c)) {
                break;
            }
            YQL_ENSURE(starRenames.contains(c));
            renamedStarSortedKeys.push_back(starRenames[c]);
        }
    }

    TYtJoinNodeOp* currOp = &op;

    struct TStarJoinStep {
        TStringBuf JoinKind;
        TString Label;

        size_t ReduceIndex = Max<size_t>();
        TMaybeNode<TCoLambda> Premap;

        const TStructExprType* InputType = nullptr;

        TVector<TString> StarKeyList;
        TVector<TString> KeyList;

        THashMap<TString, TString> Renames;
    };

    TVector<TStarJoinStep> starChain;
    TVector<TYtSection> reduceSections;
    ETypeAnnotationKind commonPremapKind = ETypeAnnotationKind::Optional;
    if (starPremap) {
        commonPremapKind = DeriveCommonSequenceKind(commonPremapKind, starPremap.Cast().Ref().GetTypeAnn()->GetKind());
    }

    THashSet<TString> unusedKeys;
    while (currOp) {
        const TYtStarJoinOption* option = nullptr;
        for (auto& opt : currOp->StarOptions) {
            if (opt.StarLabel == starLabel) {
                option = &opt;
            }
        }
        YQL_ENSURE(option);
        YQL_ENSURE(starLabel == option->StarLabel);
        YQL_ENSURE(starKeys == option->StarKeys);
        YQL_ENSURE(starInputIndex == option->StarInputIndex);

        auto leftLeaf = dynamic_cast<TYtJoinNodeLeaf*>(currOp->Left.Get());
        auto rightLeaf = dynamic_cast<TYtJoinNodeLeaf*>(currOp->Right.Get());
        TYtJoinNodeLeaf* leaf = nullptr;
        const TExprNode* leafLabelNode = nullptr;
        const TExprNode* starLabelNode = nullptr;

        TYtJoinNodeOp* nextOp = nullptr;
        if (leftLeaf && rightLeaf) {
            leaf = (option->StarInputIndex == leftLeaf->Index) ? rightLeaf : leftLeaf;
            leafLabelNode = (option->StarInputIndex == leftLeaf->Index) ? currOp->RightLabel.Get() : currOp->LeftLabel.Get();
            starLabelNode = (option->StarInputIndex == leftLeaf->Index) ? currOp->LeftLabel.Get() : currOp->RightLabel.Get();
        } else {
            leaf = leftLeaf ? leftLeaf : rightLeaf;
            leafLabelNode = leftLeaf ? currOp->LeftLabel.Get() : currOp->RightLabel.Get();
            starLabelNode = leftLeaf ? currOp->RightLabel.Get() : currOp->LeftLabel.Get();

            nextOp = leftLeaf ? dynamic_cast<TYtJoinNodeOp*>(currOp->Right.Get()) :
                                dynamic_cast<TYtJoinNodeOp*>(currOp->Left.Get());
            YQL_ENSURE(nextOp);
        }
        YQL_ENSURE(leaf);
        YQL_ENSURE(leafLabelNode);
        YQL_ENSURE(starLabelNode);

        TStarJoinStep starJoinStep;
        starJoinStep.JoinKind = currOp->JoinKind->Content();
        starJoinStep.InputType = GetJoinInputType(equiJoin, leaf->Index, ctx);
        if (!starJoinStep.InputType) {
            return EStarRewriteStatus::Error;
        }

        YQL_ENSURE(ExtractJoinKeysForStarJoin(*leafLabelNode, starJoinStep.Label, starJoinStep.KeyList));
        TString sl;
        YQL_ENSURE(ExtractJoinKeysForStarJoin(*starLabelNode, sl, starJoinStep.StarKeyList));
        YQL_ENSURE(sl == starLabel);
        YQL_ENSURE(starJoinStep.KeyList.size() == starJoinStep.StarKeyList.size());

        for (auto i : xrange(starJoinStep.KeyList.size())) {
            auto starColumn = starJoinStep.StarKeyList[i];
            auto otherColumn = starJoinStep.KeyList[i];

            YQL_ENSURE(starRenames.contains(starColumn));
            auto starRenamed = starRenames[starColumn];
            YQL_ENSURE(starRenamed);

            auto& renamed = starJoinStep.Renames[otherColumn];
            YQL_ENSURE(!renamed || renamed == starRenamed);
            renamed = starRenamed;
        }

        starJoinStep.ReduceIndex = leaf->Index; // will be updated
        starJoinStep.Premap = TMaybeNode<TCoLambda>(equiJoin.Ref().ChildPtr(joinFixedArgsCount + leaf->Index));
        if (starJoinStep.Premap) {
            YQL_ENSURE(EnsureSeqOrOptionalType(starJoinStep.Premap.Cast().Ref(), ctx));
            commonPremapKind = DeriveCommonSequenceKind(commonPremapKind, starJoinStep.Premap.Cast().Ref().GetTypeAnn()->GetKind());
        }
        starChain.emplace_back(std::move(starJoinStep));

        unusedKeys.insert(currOp->OutputRemoveColumns.begin(), currOp->OutputRemoveColumns.end());

        currOp = nextOp;
    }

    // we start doing joins from leafs
    std::reverse(starChain.begin(), starChain.end());

    const TStructExprType* chainOutputType = AddLabelToStructType(*starInputType, starLabel, ctx);
    reduceSections.reserve(starChain.size() + 1);

    TVector<size_t> innerIndexes;
    TVector<size_t> semiIndexes;
    TVector<size_t> onlyIndexes;
    TVector<size_t> leftIndexes;
    for (auto& item : starChain) {
        auto section = equiJoin.Input().Item(item.ReduceIndex);
        section = Build<TYtSection>(ctx, section.Pos())
            .InitFrom(section)
            .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::JoinLabel | EYtSettingType::StatColumns, ctx))
            .Done();

        section = SectionApplyRenames(section, item.Renames, ctx);

        reduceSections.push_back(section);
        item.ReduceIndex = reduceSections.size() - 1;

        if (item.JoinKind == "Inner") {
            innerIndexes.push_back(item.ReduceIndex);
        } else if (item.JoinKind.EndsWith("Semi")) {
            semiIndexes.push_back(item.ReduceIndex);
        } else if (item.JoinKind.EndsWith("Only")) {
            onlyIndexes.push_back(item.ReduceIndex);
        } else if (item.JoinKind == "Left" || item.JoinKind == "Right") {
            leftIndexes.push_back(item.ReduceIndex);
        } else {
            YQL_ENSURE(false, "Unexpected join type in Star JOIN");
        }

        if (item.JoinKind.EndsWith("Semi") || item.JoinKind.EndsWith("Only")) {
            // output type remains the same as input
            continue;
        }

        const TStructExprType* inputType = AddLabelToStructType(*item.InputType, item.Label, ctx);
        if (item.JoinKind != "Inner") {
            inputType = MakeStructMembersOptional(*inputType, ctx);
        }

        TVector<const TItemExprType*> outputTypeItems = chainOutputType->GetItems();
        const auto& inputTypeItems = inputType->GetItems();
        outputTypeItems.insert(outputTypeItems.end(), inputTypeItems.begin(), inputTypeItems.end());

        chainOutputType = ctx.MakeType<TStructExprType>(outputTypeItems);
    }

    {
        auto section = equiJoin.Input().Item(starInputIndex);
        section = Build<TYtSection>(ctx, section.Pos())
            .InitFrom(section)
            .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::JoinLabel | EYtSettingType::StatColumns, ctx))
            .Done();

        section = SectionApplyRenames(section, starRenames, ctx);
        reduceSections.push_back(section);
    }

    YQL_ENSURE(reduceSections.size() == starChain.size() + 1);
    YQL_ENSURE(innerIndexes.size() + semiIndexes.size() + onlyIndexes.size() + leftIndexes.size() == starChain.size());

    const auto pos = equiJoin.Pos();

    const TStructExprType* outItemType = nullptr;
    if (!op.Parent) {
        if (auto type = GetSequenceItemType(pos,
                                            equiJoin.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1],
                                            false, ctx))
        {
            outItemType = type->Cast<TStructExprType>();
        } else {
            return EStarRewriteStatus::Error;
        }
    } else {
        TVector<const TItemExprType*> structItems = chainOutputType->GetItems();
        EraseIf(structItems, [&](const TItemExprType* item) { return unusedKeys.contains(item->GetName()); });
        outItemType = ctx.MakeType<TStructExprType>(structItems);
    }

    const TVariantExprType* inputVariant = nullptr;
    {
        TTypeAnnotationNode::TListType items;
        items.reserve(reduceSections.size());
        for (const auto& item: starChain) {
            items.push_back(item.InputType);
        }
        items.push_back(starInputType);
        inputVariant = ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(items));
    }

    TYtOutTableInfo outTableInfo(outItemType, state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
    outTableInfo.RowSpec->SetConstraints(equiJoin.Ref().GetConstraintSet());
    outTableInfo.SetUnique(equiJoin.Ref().GetConstraint<TDistinctConstraintNode>(), pos, ctx);
    // TODO: mark output sorted
    Y_UNUSED(starSortedKeys);


    YQL_CLOG(INFO, ProviderYt) << "Processing star join " << starLabel << ":[" << JoinSeq(",", starKeys) << "] of length " << starChain.size();
    for (auto& item : starChain) {
        YQL_CLOG(INFO, ProviderYt) << "Join: " << item.JoinKind << ", " << item.Label << ":[" << JoinSeq(",", item.KeyList)
                                   << "] -> " << starLabel << ":[" << JoinSeq(",", item.StarKeyList) << "]";
    }
    YQL_CLOG(INFO, ProviderYt) << "StarJoin result type is " << *(const TTypeAnnotationNode*)chainOutputType;

    TExprNode::TPtr groupArg = ctx.NewArgument(pos, "group");
    TExprNode::TPtr nullFilteredRenamedAndPremappedStream = ctx.Builder(pos)
        .Callable("FlatMap")
            .Add(0, groupArg)
            .Lambda(1)
                .Param("item")
                .Callable("Visit")
                    .Arg(0, "item")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i = 0; i < reduceSections.size(); ++i) {
                            bool isStarInput = (i == reduceSections.size() - 1);
                            TSet<TString> skipNullMemberColumns;
                            if (!isStarInput || innerIndexes) {
                                // skip null for all inputs except for star input when there are no inner joins
                                skipNullMemberColumns = renamedStarKeys;
                            }

                            auto premapLambda = ctx.Builder(pos)
                                .Lambda()
                                    .Param("item")
                                    .Callable("Just")
                                        .Arg(0, "item")
                                    .Seal()
                                .Seal()
                                .Build();

                            ApplyInputPremap(premapLambda, isStarInput ? starPremap : starChain[i].Premap,
                                             commonPremapKind, isStarInput ? starRenames : starChain[i].Renames, ctx);

                            parent
                                .Atom(2 * i + 1, i)
                                .Lambda(2 * i + 2)
                                    .Param("unpackedVariant")
                                    .Callable("FlatMap")
                                        .Callable(0, "FlatMap")
                                            .Callable(0, "SkipNullMembers")
                                                .Callable(0, "Just")
                                                    .Arg(0, "unpackedVariant")
                                                .Seal()
                                                .Add(1, ToAtomList(skipNullMemberColumns, pos, ctx))
                                            .Seal()
                                            .Lambda(1)
                                                .Param("filtered")
                                                .Apply(premapLambda)
                                                    .With(0, "filtered")
                                                .Seal()
                                            .Seal()
                                        .Seal()
                                        .Lambda(1)
                                            .Param("filteredRenamedAndPremapped")
                                            .Callable("Just")
                                                .Callable(0, "Variant")
                                                    .Arg(0, "filteredRenamedAndPremapped")
                                                    .Atom(1, i)
                                                    .Add(2, ExpandType(pos, *inputVariant, ctx))
                                                .Seal()
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                .Seal();
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto buildOptionalInnersFromState = ctx.Builder(pos)
        .Lambda()
            .Param("state")
            .Callable("TryRemoveAllOptionals")
                .List(0)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i = 0; i < innerIndexes.size(); ++i) {
                            parent
                                .Callable(i, "Nth")
                                    .Arg(0, "state")
                                    .Atom(1, ToString(innerIndexes[i]), TNodeFlags::Default)
                                .Seal();
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
        .Seal()
        .Build();

    TExprNode::TPtr buildResultRow = ctx.Builder(pos)
        .Lambda()
            .Param("starItem")
            .Param("inners")
            .Param("state")
            .Callable("FlattenMembers")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    ui32 seqno = 0;
                    parent
                        .List(seqno++)
                            .Atom(0, starLabel + ".")
                            .Arg(1, "starItem")
                        .Seal();

                    for (ui32 i = 0; i < innerIndexes.size(); ++i) {
                        parent
                            .List(seqno++)
                                .Atom(0, starChain[innerIndexes[i]].Label + ".")
                                .Callable(1, "Nth")
                                    .Arg(0, "inners")
                                    .Atom(1, i)
                                .Seal()
                            .Seal();
                    }

                    for (ui32 i = 0; i < leftIndexes.size(); ++i) {
                        parent
                            .List(seqno++)
                                .Atom(0, starChain[leftIndexes[i]].Label + ".")
                                .Callable(1, "Nth")
                                    .Arg(0, "state")
                                    .Atom(1, ToString(leftIndexes[i]), TNodeFlags::Default)
                                .Seal()
                            .Seal();
                    }

                    return parent;
                })
            .Seal()
        .Seal()
        .Build();

    TExprNode::TPtr performStarChainLambda = ctx.Builder(pos)
        .Lambda()
            .Param("starItem")
            .Param("state")
            .Callable("FlatMap")
                .Apply(0, buildOptionalInnersFromState)
                    .With(0, "state")
                .Seal()
                .Lambda(1)
                    .Param("inners")
                    .Callable("Filter")
                        .Callable(0, "Just")
                            .Apply(0, buildResultRow)
                                .With(0, "starItem")
                                .With(1, "inners")
                                .With(2, "state")
                            .Seal()
                        .Seal()
                        .Lambda(1)
                            .Param("unused")
                            .Callable("And")
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    ui32 seqno = 0;
                                    for (auto i : onlyIndexes) {
                                        parent
                                            .Callable(seqno++, "Not")
                                                .Callable(0, "Exists")
                                                    .Callable(0, "Nth")
                                                        .Arg(0, "state")
                                                        .Atom(1, i)
                                                    .Seal()
                                                .Seal()
                                            .Seal();
                                    }

                                    for (auto i : semiIndexes) {
                                        parent
                                            .Callable(seqno++, "Exists")
                                                .Callable(0, "Nth")
                                                    .Arg(0, "state")
                                                    .Atom(1, i)
                                                .Seal()
                                            .Seal();
                                    }

                                    if (seqno == 0) {
                                        parent
                                            .Callable(seqno, "Bool")
                                                .Atom(0, "true", TNodeFlags::Default)
                                            .Seal();
                                    }
                                    return parent;
                                })
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto foldMapStateNode = ctx.Builder(pos)
        .List()
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                for (ui32 i = 0; i < starChain.size(); ++i) {
                    auto joinKind = starChain[i].JoinKind;
                    bool isEmptyStructInState = joinKind.EndsWith("Semi") || joinKind.EndsWith("Only");
                    TVector<const TItemExprType*> noItems;
                    auto stateItemType = isEmptyStructInState ? ctx.MakeType<TStructExprType>(noItems) : starChain[i].InputType;
                    parent
                        .Callable(i, "Nothing")
                            .Add(0, ExpandType(pos, *ctx.MakeType<TOptionalExprType>(stateItemType), ctx))
                        .Seal();
                }
                return parent;
            })
        .Seal()
        .Build();

    TExprNode::TPtr emptyResultNode = ctx.Builder(pos)
        .Callable("Nothing")
            .Add(0, ExpandType(pos, *ctx.MakeType<TOptionalExprType>(chainOutputType), ctx))
        .Seal()
        .Build();

    auto updateStateLambdaBuilder = [&](size_t inputIndex, size_t tupleSize, bool useEmptyStruct) {
        YQL_ENSURE(inputIndex < tupleSize);
        return ctx.Builder(pos)
            .Lambda()
                .Param("item")
                .Param("state")
                .List()
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i = 0; i < tupleSize; ++i) {
                            if (i != inputIndex) {
                                parent
                                    .Callable(i, "Nth")
                                        .Arg(0, "state")
                                        .Atom(1, ToString(i), TNodeFlags::Default)
                                    .Seal();
                            } else if (useEmptyStruct) {
                                parent
                                    .Callable(i, "Just")
                                        .Callable(0, "AsStruct")
                                        .Seal()
                                    .Seal();
                            } else {
                                parent
                                    .Callable(i, "Just")
                                        .Arg(0, "item")
                                    .Seal();
                            }
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
            .Build();
    };

    TExprNode::TPtr foldMapVisitLambda = ctx.Builder(pos)
        .Lambda()
            .Param("item")
            .Param("state")
            .Callable("Visit")
                .Arg(0, "item")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < starChain.size(); ++i) {
                        auto joinKind = starChain[i].JoinKind;
                        bool isEmptyStructInState = joinKind.EndsWith("Semi") || joinKind.EndsWith("Only");
                        parent
                            .Atom(2 * i + 1, i)
                            .Lambda(2 * i + 2)
                                .Param("var")
                                .List()
                                    .Add(0, emptyResultNode)
                                    .Callable(1, "If")
                                        .Callable(0, "Exists")
                                            .Callable(0, "Nth")
                                                .Arg(0, "state")
                                                .Atom(1, i)
                                            .Seal()
                                        .Seal()
                                        .Arg(1, "state")
                                        .Apply(2, updateStateLambdaBuilder(i, starChain.size(), isEmptyStructInState))
                                            .With(0, "var")
                                            .With(1, "state")
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal();
                    }
                    return parent;
                })
                .Atom(2 * starChain.size() + 1, ToString(starChain.size()))
                .Lambda(2 * starChain.size() + 2)
                    .Param("var")
                    .List()
                        .Apply(0, performStarChainLambda)
                            .With(0, "var")
                            .With(1, "state")
                        .Seal()
                        .Arg(1, "state")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    TExprNode::TPtr foldedStream = ctx.Builder(pos)
        .Callable("FoldMap")
            .Add(0, nullFilteredRenamedAndPremappedStream)
            .Add(1, foldMapStateNode)
            .Lambda(2)
                .Param("item")
                .Param("state")
                .List()
                    .Callable(0, "Nth")
                        .Apply(0, foldMapVisitLambda)
                            .With(0, "item")
                            .With(1, "state")
                        .Seal()
                        .Atom(1, 0U)
                    .Seal()
                    .Callable(1, "Nth")
                        .Apply(0, foldMapVisitLambda)
                            .With(0, "item")
                            .With(1, "state")
                        .Seal()
                        .Atom(1, 1U)
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    TMap<TStringBuf, TVector<TStringBuf>> renameMap;
    if (!op.Parent) {
        renameMap = LoadJoinRenameMap(equiJoin.JoinOptions().Ref());
    }
    TExprNode::TPtr finalRenamingLambda = BuildJoinRenameLambda(pos, renameMap, *outItemType, ctx).Ptr();

    TExprNode::TPtr finalRenamedStream = ctx.Builder(pos)
        .Callable("FlatMap")
            .Add(0, foldedStream)
            .Lambda(1)
                .Param("optItem")
                .Callable("Map")
                    .Arg(0, "optItem")
                    .Lambda(1)
                        .Param("item")
                        .Apply(finalRenamingLambda)
                            .With(0, "item")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, pos)
        .Add()
            .Name()
                .Value(ToString(EYtSettingType::ReduceBy))
            .Build()
            .Value(ToAtomList(renamedStarSortedKeys, pos, ctx))
        .Build()
        .Add()
            .Name()
                .Value(ToString(EYtSettingType::JoinReduce))
            .Build()
        .Build();

    if (state->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::Flow))
                .Build()
            .Build();
    }

    const auto useSystemColumns = state->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS);
    if (useSystemColumns) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::KeySwitch))
                .Build()
            .Build();
    }

    auto reduceOp = Build<TYtReduce>(ctx, pos)
        .World(equiJoin.World())
        .DataSink(equiJoin.DataSink())
        .Input()
            .Add(reduceSections)
        .Build()
        .Output()
            .Add(outTableInfo.ToExprNode(ctx, pos).Cast<TYtOutTable>())
        .Build()
        .Settings(settingsBuilder.Done())
        .Reducer(BuildYtReduceLambda(pos, groupArg, std::move(finalRenamedStream), useSystemColumns, ctx))
        .Done();

    op.Output = reduceOp;
    return EStarRewriteStatus::Ok;
}

void CombineStarStatus(EStarRewriteStatus& status, EStarRewriteStatus other) {
    YQL_ENSURE(status == EStarRewriteStatus::Error || status == EStarRewriteStatus::None ||
               status == EStarRewriteStatus::Ok);
    switch (other) {
        case EStarRewriteStatus::Error:
            status = other;
            break;
        case EStarRewriteStatus::Ok:
            if (status != EStarRewriteStatus::Error) {
                status = other;
            }
            break;
        case EStarRewriteStatus::None:
            break;
        default:
            YQL_ENSURE(false, "Unexpected star join status");
    }
}

EStarRewriteStatus RewriteYtEquiJoinStarChains(TYtEquiJoin equiJoin, TYtJoinNodeOp& op, const TYtState::TPtr& state, TExprContext& ctx) {
    TYtJoinNodeLeaf* leftLeaf = dynamic_cast<TYtJoinNodeLeaf*>(op.Left.Get());
    TYtJoinNodeLeaf* rightLeaf = dynamic_cast<TYtJoinNodeLeaf*>(op.Right.Get());

    EStarRewriteStatus result = EStarRewriteStatus::None;
    if (op.StarOptions) {
        if (leftLeaf && rightLeaf) {
            // too trivial star join - let RewriteYtEquiJoinLeaves() to handle it
            return result;
        }
        return RewriteYtEquiJoinStarSingleChain(equiJoin, op, state, ctx);
    }

    if (!leftLeaf) {
        auto leftOp = dynamic_cast<TYtJoinNodeOp*>(op.Left.Get());
        YQL_ENSURE(leftOp);
        CombineStarStatus(result, RewriteYtEquiJoinStarChains(equiJoin, *leftOp, state, ctx));
        if (result == EStarRewriteStatus::Error) {
            return result;
        }
        if (result == EStarRewriteStatus::Ok && leftOp->Output) {
            op.Left = ConvertYtEquiJoinToLeaf(*leftOp, equiJoin.Pos(), ctx);
        }
    }

    if (!rightLeaf) {
        auto rightOp = dynamic_cast<TYtJoinNodeOp*>(op.Right.Get());
        YQL_ENSURE(rightOp);
        CombineStarStatus(result, RewriteYtEquiJoinStarChains(equiJoin, *rightOp, state, ctx));
        if (result == EStarRewriteStatus::Error) {
            return result;
        }
        if (result == EStarRewriteStatus::Ok && rightOp->Output) {
            op.Right = ConvertYtEquiJoinToLeaf(*rightOp, equiJoin.Pos(), ctx);
        }
    }

    return result;
}

EStarRewriteStatus RewriteYtEquiJoinStar(TYtEquiJoin equiJoin, TYtJoinNodeOp& op, const TYtState::TPtr& state, TExprContext& ctx) {
    const bool enableStarJoin = state->Configuration->JoinEnableStarJoin.Get().GetOrElse(false);
    if (!enableStarJoin) {
        return EStarRewriteStatus::None;
    }

    const bool allowColumnRenames = state->Configuration->JoinAllowColumnRenames.Get().GetOrElse(true);
    if (!allowColumnRenames) {
        return EStarRewriteStatus::None;
    }

    EStarRewriteStatus collectStatus = EStarRewriteStatus::Ok;
    CollectPossibleStarJoins(equiJoin, op, state, collectStatus, ctx);
    if (collectStatus != EStarRewriteStatus::Ok) {
        return collectStatus;
    }

    return RewriteYtEquiJoinStarChains(equiJoin, op, state, ctx);
}

} // namespace

TYtJoinNodeOp::TPtr ImportYtEquiJoin(TYtEquiJoin equiJoin, TExprContext& ctx) {
    TVector<TYtJoinNodeLeaf::TPtr> leaves;
    TMaybeNode<TExprBase> world;
    for (const size_t i: xrange(equiJoin.Input().Size())) {
        auto leaf = MakeIntrusive<TYtJoinNodeLeaf>(equiJoin.Input().Item(i), TMaybeNode<TCoLambda>(equiJoin.Ref().ChildPtr(joinFixedArgsCount + i)));
        leaf->Label = NYql::GetSetting(leaf->Section.Settings().Ref(), EYtSettingType::JoinLabel)->Child(1);
        leaf->Index = i;
        TPartOfConstraintBase::TPathReduce rename;
        if (leaf->Label->IsAtom()) {
            leaf->Scope.emplace_back(leaf->Label->Content());
            rename = [&](const TPartOfConstraintBase::TPathType& path) -> std::vector<TPartOfConstraintBase::TPathType> {
                auto result = path;
                TStringBuilder sb;
                sb << leaf->Scope.front() << '.' << result.front();
                result.front() = ctx.AppendString(sb);
                return {result};
            };
        } else {
            for (const auto& x : leaf->Label->Children()) {
                leaf->Scope.emplace_back(x->Content());
            }
        }

        const auto sourceConstraints = leaf->Section.Ref().GetConstraintSet();
        if (const auto distinct = sourceConstraints.GetConstraint<TDistinctConstraintNode>())
            if (const auto complete = leaf->Premap ? TPartOfDistinctConstraintNode::MakeComplete(ctx, leaf->Premap.Cast().Body().Ref().GetConstraint<TPartOfDistinctConstraintNode>(), distinct) : distinct)
                if (const auto d = complete->RenameFields(ctx, rename))
                    leaf->Constraints.AddConstraint(d);
        if (const auto unique = sourceConstraints.GetConstraint<TUniqueConstraintNode>())
            if (const auto complete = leaf->Premap ? TPartOfUniqueConstraintNode::MakeComplete(ctx, leaf->Premap.Cast().Body().Ref().GetConstraint<TPartOfUniqueConstraintNode>(), unique) : unique)
                if (const auto u = complete->RenameFields(ctx, rename))
                    leaf->Constraints.AddConstraint(u);
        if (const auto empty = sourceConstraints.GetConstraint<TEmptyConstraintNode>())
            leaf->Constraints.AddConstraint(empty);

        leaves.push_back(std::move(leaf));
    }

    const auto& renameMap = LoadJoinRenameMap(equiJoin.JoinOptions().Ref());
    THashSet<TString> drops;
    for (const auto& [column, renames] : renameMap) {
        if (renames.empty()) {
            drops.insert(ToString(column));
        }
    }

    const auto root = ImportYtEquiJoinRecursive(leaves, nullptr, drops, equiJoin.Joins().Ref(), ctx);
    if (const auto renames = LoadJoinRenameMap(equiJoin.JoinOptions().Ref()); !renames.empty()) {
        const auto rename = [&renames](const TPartOfConstraintBase::TPathType& path) -> std::vector<TPartOfConstraintBase::TPathType> {
            if (path.empty())
                return {};

            const auto it = renames.find(path.front());
            if (renames.cend() == it)
                return {path};
            if (it->second.empty())
                return {};

            std::vector<TPartOfConstraintBase::TPathType> res(it->second.size());
            std::transform(it->second.cbegin(), it->second.cend(), res.begin(), [&path](const std::string_view& newName) {
                auto newPath = path;
                newPath.front() = newName;
                return newPath;
            });
            return res;
        };

        TConstraintSet set;
        if (const auto unique = root->Constraints.GetConstraint<TUniqueConstraintNode>())
            if (const auto u = unique->RenameFields(ctx, rename))
                set.AddConstraint(u);
        if (const auto distinct = root->Constraints.GetConstraint<TDistinctConstraintNode>())
            if (const auto d = distinct->RenameFields(ctx, rename))
                set.AddConstraint(d);
        if (const auto empty = root->Constraints.GetConstraint<TEmptyConstraintNode>())
            set.AddConstraint(empty);
        root->Constraints = set;
    }

    root->CostBasedOptPassed = HasSetting(equiJoin.JoinOptions().Ref(), "cbo_passed");
    return root;
}

IGraphTransformer::TStatus RewriteYtEquiJoin(TYtEquiJoin equiJoin, TYtJoinNodeOp& op, const TYtState::TPtr& state, TExprContext& ctx) {
    switch (RewriteYtEquiJoinStar(equiJoin, op, state, ctx)) {
    case EStarRewriteStatus::Error:
        return IGraphTransformer::TStatus::Error;
    case EStarRewriteStatus::Ok:
        return IGraphTransformer::TStatus::Ok;
    case EStarRewriteStatus::WaitInput:
        return IGraphTransformer::TStatus::Repeat;
    case EStarRewriteStatus::None:
        break;
    }

    return RewriteYtEquiJoinLeaves(equiJoin, op, state, ctx);
}

TStatus RewriteYtEquiJoinLeaves(TYtEquiJoin equiJoin, TYtJoinNodeOp& op, const TYtState::TPtr& state, TExprContext& ctx) {
    TYtJoinNodeLeaf* leftLeaf = dynamic_cast<TYtJoinNodeLeaf*>(op.Left.Get());
    TYtJoinNodeLeaf* rightLeaf = dynamic_cast<TYtJoinNodeLeaf*>(op.Right.Get());

    TStatus result = TStatus::Repeat;
    if (!leftLeaf) {
        auto& leftOp = *dynamic_cast<TYtJoinNodeOp*>(op.Left.Get());
        CombineJoinStatus(result, RewriteYtEquiJoinLeaves(equiJoin, leftOp, state, ctx));
        if (result.Level == TStatus::Error) {
            return result;
        }
        if (result.Level == TStatus::Ok && leftOp.Output) {
            // convert to leaf
            op.Left = ConvertYtEquiJoinToLeaf(leftOp, equiJoin.Pos(), ctx);
        }
    }

    if (!rightLeaf) {
        auto& rightOp = *dynamic_cast<TYtJoinNodeOp*>(op.Right.Get());
        CombineJoinStatus(result, RewriteYtEquiJoinLeaves(equiJoin, rightOp, state, ctx));
        if (result.Level == TStatus::Error) {
            return result;
        }
        if (result.Level == TStatus::Ok && rightOp.Output) {
            // convert to leaf
            op.Right = ConvertYtEquiJoinToLeaf(rightOp, equiJoin.Pos(), ctx);
        }
    }

    if (leftLeaf && rightLeaf) {
        CombineJoinStatus(result, RewriteYtEquiJoinLeaf(equiJoin, op, *leftLeaf, *rightLeaf, state, ctx));
    }

    return result;
}

TMaybeNode<TExprBase> ExportYtEquiJoin(TYtEquiJoin equiJoin, const TYtJoinNodeOp& op, TExprContext& ctx,
    const TYtState::TPtr& state) {
    if (op.Output) {
        return op.Output.Cast();
    }

    TVector<TYtSection> sections;
    TVector<TExprBase> premaps;
    THashSet<TString> outputRemoveColumns;
    auto joinTree = ExportJoinTree(op, equiJoin.Pos(), sections, premaps, outputRemoveColumns, ctx);

    TExprNode::TPtr joinSettings = equiJoin.JoinOptions().Ptr();
    if (outputRemoveColumns) {
        auto renameMap = LoadJoinRenameMap(*joinSettings);
        for (auto& c : outputRemoveColumns) {
            YQL_ENSURE(renameMap[c].empty(), "Rename map contains non-trivial renames for column " << c);
        }

        joinSettings = RemoveSetting(*joinSettings, "rename", ctx);
        TExprNode::TListType joinSettingsNodes = joinSettings->ChildrenList();

        AppendEquiJoinRenameMap(joinSettings->Pos(), renameMap, joinSettingsNodes, ctx);
        joinSettings = ctx.ChangeChildren(*joinSettings, std::move(joinSettingsNodes));
    }

    if (!HasSetting(*joinSettings, "cbo_passed") && op.CostBasedOptPassed) {
        joinSettings = AddSetting(*joinSettings, joinSettings->Pos(), "cbo_passed", {}, ctx);
    }

    auto outItemType = GetSequenceItemType(equiJoin.Pos(),
                                           equiJoin.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1],
                                           false, ctx);
    if (!outItemType) {
        return {};
    }

    const auto join = Build<TYtEquiJoin>(ctx, equiJoin.Pos())
        .World(equiJoin.World())
        .DataSink(equiJoin.DataSink())
        .Input()
            .Add(sections)
        .Build()
        .Output()
            .Add(TYtOutTableInfo(outItemType->Cast<TStructExprType>(),
                state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE).ToExprNode(ctx, equiJoin.Pos()).Cast<TYtOutTable>())
        .Build()
        .Settings()
        .Build()
        .Joins(joinTree)
        .JoinOptions(joinSettings)
        .Done();
    auto children = join.Ref().ChildrenList();
    children.reserve(children.size() + premaps.size());
    std::transform(premaps.cbegin(), premaps.cend(), std::back_inserter(children), std::bind(&TExprBase::Ptr, std::placeholders::_1));
    return TExprBase(ctx.ChangeChildren(join.Ref(), std::move(children)));
}

}
