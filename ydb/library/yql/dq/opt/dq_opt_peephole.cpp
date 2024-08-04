#include "dq_opt_peephole.h"

#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_type_helpers.h>

#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/size_literals.h>
#include <util/generic/bitmap.h>

namespace NYql::NDq {

using namespace NYql::NNodes;

namespace {

inline std::string_view GetTableLabel(const TExprBase& node) {
    static const std::string_view empty;

    if (node.Maybe<TCoAtom>()) {
        return node.Cast<TCoAtom>().Value();
    }

    return empty;
}

inline TString GetColumnName(std::string_view label, const TItemExprType *key) {
    if (!label.empty()) {
        return FullColumnName(label, key->GetName());
    }

    return ToString(key->GetName());
}

std::pair<TExprNode::TListType, TExprNode::TListType> JoinKeysToAtoms(TExprContext& ctx, const TDqJoinBase& join,
    std::string_view leftTableLabel, std::string_view rightTableLabel)
{
    TExprNode::TListType leftNodes;
    TExprNode::TListType rightNodes;

    for (const auto& joinOn : join.JoinKeys()) {
        TExprNode::TPtr leftValue, rightValue;

        if (leftTableLabel.empty()) {
            leftValue = ctx.NewAtom(
                join.Pos(),
                FullColumnName(joinOn.LeftLabel().Value(), joinOn.LeftColumn().Value())
            );
        } else {
            leftValue = joinOn.LeftColumn().Ptr();
        }

        if (rightTableLabel.empty()) {
            rightValue = ctx.NewAtom(
                join.Pos(),
                FullColumnName(joinOn.RightLabel().Value(), joinOn.RightColumn().Value())
            );
        } else {
            rightValue = joinOn.RightColumn().Ptr();
        }

        leftNodes.emplace_back(leftValue);
        rightNodes.emplace_back(rightValue);
    }

    return {std::move(leftNodes), std::move(rightNodes)};
}

TExprNode::TPtr AddConvertedKeys(TExprNode::TPtr list, TExprContext& ctx, TExprNode::TListType& leftKeyColumnNodes, const TTypeAnnotationNode::TListType& keyTypes, const TStructExprType* origItemType) {
    std::vector<std::pair<TString, std::pair<TString, const TTypeAnnotationNode*>>> columnsToConvert;
    for (auto i = 0U; i < leftKeyColumnNodes.size(); i++) {
        const auto origName = TString(leftKeyColumnNodes[i]->Content());
        auto itemType= origItemType->FindItemType(origName);
        if (itemType->Equals(*keyTypes[i])) {
            continue;
        }
        const auto newName = TStringBuilder() << origName << "_map_join_core_key_converted_" << i << "_";
        columnsToConvert.emplace_back(origName, std::pair<TString, const TTypeAnnotationNode*>{newName, keyTypes[i]});
        leftKeyColumnNodes[i] = ctx.NewAtom(leftKeyColumnNodes[i]->Pos(), newName);
    }
    const auto pos = list->Pos();
    return ctx.Builder(pos)
        .Callable("Map")
            .Add(0, std::move(list))
            .Lambda(1)
                .Param("dict")
                .Callable("FlattenMembers")
                    .List(0)
                        .Atom(0, "")
                        .Arg(1, "dict")
                    .Seal()
                    .List(1)
                        .Atom(0, "")
                        .Callable(1, "AsStruct")
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                auto i = 0U;
                                for (const auto& [oldName, newCol]: columnsToConvert) {
                                    parent.List(i)
                                        .Atom(0, newCol.first)
                                        .Callable(1, "StrictCast")
                                            .Callable(0, "Member")
                                                .Arg(0, "dict")
                                                .Atom(1, oldName)
                                            .Seal()
                                            .Add(1, ExpandType(pos, *newCol.second, ctx))
                                        .Seal()
                                    .Seal();
                                    i++;
                                }
                                return parent;
                            })
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TListType OriginalJoinOutputMembers(const TDqPhyMapJoin& mapJoin, TExprContext& ctx) {
    const auto origItemType = mapJoin.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List ?
        mapJoin.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>() :
        mapJoin.Ref().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TStructExprType>();
    TExprNode::TListType structMembers;
    structMembers.reserve(origItemType->GetItems().size());
    for (const auto& item: origItemType->GetItems()) {
        structMembers.push_back(ctx.NewAtom(mapJoin.Pos(), item->GetName()));
    }
    return structMembers;
}

TExprNode::TPtr ExpandJoinInput(const TStructExprType& type, TExprNode::TPtr&& arg, TExprContext& ctx) {
    return ctx.Builder(arg->Pos())
            .Callable("ExpandMap")
                .Add(0, std::move(arg))
                .Lambda(1)
                    .Param("item")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        auto i = 0U;
                        for (const auto& item : type.GetItems()) {
                            parent.Callable(i++, "Member")
                                .Arg(0, "item")
                                .Atom(1, item->GetName())
                                .Seal();
                        }
                        return parent;
                    })
                .Seal()
            .Seal().Build();
}

} // anonymous namespace end

TExprBase DqPeepholeRewriteMapJoinWithGraceCore(const TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TDqPhyGraceJoin>()) {
        return node;
    }
    const auto graceJoin = node.Cast<TDqPhyGraceJoin>();
    const auto pos = graceJoin.Pos();

    const TString leftTableLabel(GetTableLabel(graceJoin.LeftLabel()));
    const TString rightTableLabel(GetTableLabel(graceJoin.RightLabel()));

    auto [leftKeyColumnNodes, rightKeyColumnNodes] = JoinKeysToAtoms(ctx, graceJoin, leftTableLabel, rightTableLabel);
    const auto keyWidth = leftKeyColumnNodes.size();

    ui32 outputIndex = 0;
    const auto makeRenames = [&ctx, &outputIndex, pos](TStringBuf, const TStructExprType& type) {
        TExprNode::TListType renames;
        for (auto i = 0u; i < type.GetSize(); i++) {
            renames.emplace_back(ctx.NewAtom(pos, ctx.GetIndexAsString(i)));
            renames.emplace_back(ctx.NewAtom(pos, ctx.GetIndexAsString(outputIndex++)));
        }
        return renames;
    };

    const auto itemTypeLeft = GetSequenceItemType(graceJoin.LeftInput(), false, ctx)->Cast<TStructExprType>();
    const auto itemTypeRight = GetSequenceItemType(graceJoin.RightInput(), false, ctx)->Cast<TStructExprType>();

    std::vector<TString> fullColNames;

    for (auto i = 0u; i < itemTypeLeft->GetSize(); i++) {
        TString name(itemTypeLeft->GetItems()[i]->GetName());
        if (leftTableLabel) {
            name = leftTableLabel + "." + name;
        }
        fullColNames.push_back(name);
    }
    for (auto i = 0u; i < itemTypeRight->GetSize(); i++) {
        TString name(itemTypeRight->GetItems()[i]->GetName());
        if (rightTableLabel) {
            name = rightTableLabel + "." + name;
        }
        fullColNames.push_back(name);
    }

    TExprNode::TListType leftRenames = makeRenames(leftTableLabel, *itemTypeLeft);
    TExprNode::TListType rightRenames, rightPayloads;
    const bool withRightSide = graceJoin.JoinType().Value() != "LeftOnly" && graceJoin.JoinType().Value() != "LeftSemi";
    if (withRightSide) {
        rightRenames = makeRenames(rightTableLabel, *itemTypeRight);
        rightPayloads.reserve(rightRenames.size() >> 1U);
        for (auto it = rightRenames.cbegin(); rightRenames.cend() != it; ++++it)
            rightPayloads.emplace_back(*it);
    }

    TTypeAnnotationNode::TListType keyTypesLeft(keyWidth);
    TTypeAnnotationNode::TListType keyTypesRight(keyWidth);
    TTypeAnnotationNode::TListType keyTypes(keyWidth);
    for (auto i = 0U; i < keyTypes.size(); ++i) {
        const auto keyTypeLeft = itemTypeLeft->FindItemType(leftKeyColumnNodes[i]->Content());
        const auto keyTypeRight = itemTypeRight->FindItemType(rightKeyColumnNodes[i]->Content());
        bool optKey = false;
        keyTypes[i] = JoinDryKeyType(keyTypeLeft, keyTypeRight, optKey, ctx);
        if (!keyTypes[i]) {
            keyTypes.clear();
            keyTypesLeft.clear();
            keyTypesRight.clear();
            break;
        }
        keyTypesLeft[i] = optKey ? ctx.MakeType<TOptionalExprType>(keyTypes[i]) : keyTypes[i];
        keyTypesRight[i] = optKey ? ctx.MakeType<TOptionalExprType>(keyTypes[i]) : keyTypes[i];
    }

    auto leftInput = ExpandJoinInput(*itemTypeLeft, ctx.NewCallable(graceJoin.LeftInput().Pos(), "ToFlow", {graceJoin.LeftInput().Ptr()}), ctx);
    auto rightInput = ExpandJoinInput(*itemTypeRight, ctx.NewCallable(graceJoin.RightInput().Pos(), "ToFlow", {graceJoin.RightInput().Ptr()}), ctx);
    YQL_ENSURE(!keyTypes.empty());

    for (auto i = 0U; i < leftKeyColumnNodes.size(); i++) {
        const auto origName = TString(leftKeyColumnNodes[i]->Content());
        auto index = itemTypeLeft->FindItem(origName);
        YQL_ENSURE(index);
        leftKeyColumnNodes[i] = ctx.NewAtom(leftKeyColumnNodes[i]->Pos(), ctx.GetIndexAsString(*index));
    }
    for (auto i = 0U; i < rightKeyColumnNodes.size(); i++) {
        const auto origName = TString(rightKeyColumnNodes[i]->Content());
        auto index = itemTypeRight->FindItem(origName);
        YQL_ENSURE(index);
        rightKeyColumnNodes[i] = ctx.NewAtom(rightKeyColumnNodes[i]->Pos(), ctx.GetIndexAsString(*index));
    }

    auto [leftKeyColumnNodesCopy, rightKeyColumnNodesCopy] = JoinKeysToAtoms(ctx, graceJoin, leftTableLabel, rightTableLabel);

    auto graceJoinCore = Build<TCoGraceJoinCore>(ctx, pos)
            .LeftInput(std::move(leftInput))
            .RightInput(std::move(rightInput))
            .JoinKind(graceJoin.JoinType())
            .LeftKeysColumns(ctx.NewList(pos, std::move(leftKeyColumnNodes)))
            .RightKeysColumns(ctx.NewList(pos,  std::move(rightKeyColumnNodes)))
            .LeftRenames(ctx.NewList(pos, std::move(leftRenames)))
            .RightRenames(ctx.NewList(pos, std::move(rightRenames)))
            .LeftKeysColumnNames(ctx.NewList(pos,  std::move(leftKeyColumnNodesCopy)))
            .RightKeysColumnNames(ctx.NewList(pos,  std::move(rightKeyColumnNodesCopy)))
            .Flags()
            .Build()
        .Done();

    auto graceNode = ctx.Builder(pos)
        .Callable("NarrowMap")
            .Add(0, graceJoinCore.Ptr())
            .Lambda(1)
                .Params("output", fullColNames.size())
                .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 i = 0U;
                        for (const auto& colName : fullColNames) {
                            parent.List(i)
                                .Atom(0, colName)
                                .Arg(1, "output", i)
                            .Seal();
                            i++;
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return TExprBase(graceNode);
}

/**
 * Rewrites a `KqpMapJoin` to the `MapJoinCore`.
 *
 * Restrictions:
 *  - Don't select join strategy, always use `MapJoin`
 *  - Explicitly convert right input to the dict
 *  - Use quite pretty trick: do `MapJoinCore` in `FlatMap`-lambda
 *    (rely on the fact that there will be only one element in the `FlatMap`-stream)
 *  - Align key types using `StrictCast`, use internal columns to store converted left keys
 */
TExprBase DqPeepholeRewriteMapJoinWithMapCore(const TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TDqPhyMapJoin>()) {
        return node;
    }

    const auto mapJoin = node.Cast<TDqPhyMapJoin>();
    const auto pos = mapJoin.Pos();

    const auto leftTableLabel = GetTableLabel(mapJoin.LeftLabel());
    const auto rightTableLabel = GetTableLabel(mapJoin.RightLabel());

    auto [leftKeyColumnNodes, rightKeyColumnNodes] = JoinKeysToAtoms(ctx, mapJoin, leftTableLabel, rightTableLabel);
    const auto keyWidth = leftKeyColumnNodes.size();

    const auto makeRenames = [&ctx, pos](TStringBuf label, const TStructExprType& type) {
        TExprNode::TListType renames;
        for (const auto& member : type.GetItems()) {
            renames.emplace_back(ctx.NewAtom(pos, member->GetName()));
            renames.emplace_back(ctx.NewAtom(pos, GetColumnName(label, member)));
        }
        return renames;
    };

    const auto itemTypeLeft = GetSeqItemType(*mapJoin.LeftInput().Ref().GetTypeAnn()).Cast<TStructExprType>();
    const auto itemTypeRight = GetSeqItemType(*mapJoin.RightInput().Ref().GetTypeAnn()).Cast<TStructExprType>();

    TExprNode::TListType leftRenames = makeRenames(leftTableLabel, *itemTypeLeft);
    TExprNode::TListType rightRenames, rightPayloads;
    const bool withRightSide = mapJoin.JoinType().Value() != "LeftOnly" && mapJoin.JoinType().Value() != "LeftSemi";
    if (withRightSide) {
        rightRenames = makeRenames(rightTableLabel, *itemTypeRight);
        rightPayloads.reserve(rightRenames.size() >> 1U);
        for (auto it = rightRenames.cbegin(); rightRenames.cend() != it; ++++it)
            rightPayloads.emplace_back(*it);
    }

    TTypeAnnotationNode::TListType keyTypesLeft(keyWidth);
    TTypeAnnotationNode::TListType keyTypes(keyWidth);
    for (auto i = 0U; i < keyTypes.size(); ++i) {
        const auto keyTypeLeft = itemTypeLeft->FindItemType(leftKeyColumnNodes[i]->Content());
        const auto keyTypeRight = itemTypeRight->FindItemType(rightKeyColumnNodes[i]->Content());
        bool optKey = false;
        keyTypes[i] = JoinDryKeyType(keyTypeLeft, keyTypeRight, optKey, ctx);
        if (!keyTypes[i]) {
            keyTypes.clear();
            keyTypesLeft.clear();
            break;
        }
        keyTypesLeft[i] = optKey ? ctx.MakeType<TOptionalExprType>(keyTypes[i]) : keyTypes[i];
    }

    auto leftInput = ctx.NewCallable(mapJoin.LeftInput().Pos(), "ToFlow", {mapJoin.LeftInput().Ptr()});
    auto rightInput = ctx.NewCallable(mapJoin.RightInput().Pos(), "ToFlow", {mapJoin.RightInput().Ptr()});

    if (keyTypes.empty()) {
        const auto type = mapJoin.Ref().GetTypeAnn();
        if (mapJoin.JoinType().Value() == "Inner" || mapJoin.JoinType().Value() == "LeftSemi")
            return TExprBase(ctx.NewCallable(pos, "EmptyIterator", {ExpandType(pos, *type, ctx)}));

        const auto structType = GetSeqItemType(*type).Cast<TStructExprType>();
        return TExprBase(ctx.Builder(pos)
            .Callable("Map")
                .Add(0, std::move(leftInput))
                .Lambda(1)
                    .Param("row")
                    .Callable("AsStruct")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            ui32 idx = 0U;
                            for (auto i = 0U; i < leftRenames.size(); ++++i) {
                                parent.List(idx++)
                                    .Add(0, std::move(leftRenames[i + 1U]))
                                    .Callable(1, "Member")
                                        .Arg(0, "row")
                                        .Add(1, std::move(leftRenames[i]))
                                    .Seal()
                                .Seal();
                            }
                            for (auto i = 0U; i < rightRenames.size(); ++i) {
                                const auto memberType = structType->FindItemType(rightRenames[++i]->Content());
                                parent.List(idx++)
                                    .Add(0, std::move(rightRenames[i]))
                                    .Callable(1, "Nothing")
                                        .Add(0, ExpandType(pos, *memberType, ctx))
                                    .Seal()
                                .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal().Build()
        );
    }

    const bool payloads = !rightPayloads.empty();
    rightInput = MakeDictForJoin<true>(PrepareListForJoin(std::move(rightInput), keyTypes, rightKeyColumnNodes, std::move(rightPayloads), payloads, false, true, ctx), payloads, withRightSide, ctx);
    leftInput = AddConvertedKeys(std::move(leftInput), ctx, leftKeyColumnNodes, keyTypesLeft, itemTypeLeft);
    auto [leftKeyColumnNodesCopy, rightKeyColumnNodesCopy] = JoinKeysToAtoms(ctx, mapJoin, leftTableLabel, rightTableLabel);
    auto [_, rightKeyColumnNodesAnotherCopy] = JoinKeysToAtoms(ctx, mapJoin, leftTableLabel, rightTableLabel);

    return Build<TCoExtractMembers>(ctx, pos)
        .Input<TCoFlatMap>()
            .Input(std::move(rightInput))
            .Lambda()
                .Args({"dict"})
                .Body<TCoMapJoinCore>()
                    .LeftInput(std::move(leftInput))
                    .RightDict("dict")
                    .JoinKind(mapJoin.JoinType())
                    .LeftKeysColumns(ctx.NewList(pos, std::move(leftKeyColumnNodes)))
                    .RightKeysColumns(ctx.NewList(pos,  std::move(rightKeyColumnNodesCopy)))
                    .LeftRenames(ctx.NewList(pos, std::move(leftRenames)))
                    .RightRenames(ctx.NewList(pos, std::move(rightRenames)))
                    .LeftKeysColumnNames(ctx.NewList(pos,  std::move(leftKeyColumnNodesCopy)))
                    .RightKeysColumnNames(ctx.NewList(pos,  std::move(rightKeyColumnNodesAnotherCopy)))
                .Build()
            .Build()
        .Build()
        .Members()
            .Add(OriginalJoinOutputMembers(mapJoin, ctx))
        .Build()
        .Done();
}

TExprBase DqPeepholeRewriteCrossJoin(const TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TDqPhyCrossJoin>()) {
        return node;
    }
    auto crossJoin = node.Cast<TDqPhyCrossJoin>();

    auto leftTableLabel = GetTableLabel(crossJoin.LeftLabel());
    auto rightTableLabel = GetTableLabel(crossJoin.RightLabel());

    TCoArgument leftArg{ctx.NewArgument(crossJoin.Pos(), "_kqp_left")};
    TCoArgument rightArg{ctx.NewArgument(crossJoin.Pos(), "_kqp_right")};

    TExprNodeList keys;
    auto collectKeys = [&ctx, &keys](const TExprBase& input, TStringBuf label, const TCoArgument& arg) {
        for (auto key : GetSeqItemType(*input.Ref().GetTypeAnn()).Cast<TStructExprType>()->GetItems()) {
            auto fqColumnName = GetColumnName(label, key);
            keys.emplace_back(
                Build<TCoNameValueTuple>(ctx, input.Pos())
                    .Name().Build(fqColumnName)
                    .Value<TCoMember>()
                        .Struct(arg)
                        .Name().Build(ToString(key->GetName()))
                        .Build()
                    .Done().Ptr());
        }
    };
    collectKeys(crossJoin.LeftInput(), leftTableLabel, leftArg);
    collectKeys(crossJoin.RightInput(), rightTableLabel, rightArg);

    // we have to `Condense` right input as single-element stream of lists (single list of all elements from the right),
    // because stream supports single iteration only
    auto itemArg = Build<TCoArgument>(ctx, crossJoin.Pos()).Name("item").Done();
    auto rightAsStreamOfLists = Build<TCoCondense1>(ctx, crossJoin.Pos())
        .Input<TCoToFlow>()
            .Input(crossJoin.RightInput())
            .Build()
        .InitHandler()
            .Args({itemArg})
            .Body<TCoAsList>()
                .Add(itemArg)
                .Build()
            .Build()
        .SwitchHandler()
            .Args({"item", "state"})
            .Body<TCoBool>()
                .Literal().Build("false")
                .Build()
            .Build()
        .UpdateHandler()
            .Args({"item", "state"})
            .Body<TCoAppend>()
                .List("state")
                .Item("item")
                .Build()
            .Build()
        .Done();

    return Build<TCoFlatMap>(ctx, crossJoin.Pos())
        .Input(rightAsStreamOfLists)
        .Lambda()
            .Args({"rightAsList"})
            .Body<TCoFlatMap>()
                .Input(crossJoin.LeftInput())
                .Lambda()
                    .Args({leftArg})
                    .Body<TCoMap>()
                        // here we have `List`, so we can iterate over it many times (for every `leftArg`)
                        .Input("rightAsList")
                        .Lambda()
                            .Args({rightArg})
                            .Body<TCoAsStruct>()
                                .Add(keys)
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();
}

namespace {

TExprNode::TPtr UnpackJoinedData(const TStructExprType* leftRowType, const TStructExprType* rightRowType,
    std::string_view leftLabel, std::string_view rightLabel, TPositionHandle pos, TExprContext& ctx)
{
    auto arg = Build<TCoArgument>(ctx, pos)
            .Name("packedItem")
            .Done();

    const auto& leftScheme = leftRowType->GetItems();
    const auto& rightScheme = rightRowType->GetItems();

    TExprNode::TListType outValueItems;
    outValueItems.reserve(leftScheme.size() + rightScheme.size());

    for (int tableIndex = 0; tableIndex < 2; tableIndex++) {
        const auto& scheme = tableIndex ? rightScheme : leftScheme;
        const auto label = tableIndex ? rightLabel : leftLabel;

        for (const auto& item : scheme) {
            auto nameAtom = ctx.NewAtom(pos, item->GetName());

            auto pair = ctx.Builder(pos)
                .List()
                    .Atom(0, GetColumnName(label, item))
                    .Callable(1, "Member")
                        .Callable(0, "Nth")
                            .Add(0, arg.Ptr())
                            .Atom(1, ToString(tableIndex), TNodeFlags::Default)
                            .Seal()
                        .Atom(1, item->GetName())
                        .Seal()
                    .Seal()
                .Build();

            outValueItems.push_back(pair);
        }
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({arg})
        .Body<TCoAsStruct>()
            .Add(outValueItems)
            .Build()
        .Done().Ptr();
}

} //anonymous namespace end

NNodes::TExprBase DqPeepholeRewriteJoinDict(const NNodes::TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TDqPhyJoinDict>()) {
        return node;
    }

    const auto joinDict = node.Cast<TDqPhyJoinDict>();
    const auto joinKind = joinDict.JoinType().Value();

    YQL_ENSURE(joinKind != "Cross"sv);

    const auto leftTableLabel = GetTableLabel(joinDict.LeftLabel());
    const auto rightTableLabel = GetTableLabel(joinDict.RightLabel());

    auto [leftKeys, rightKeys] = JoinKeysToAtoms(ctx, joinDict, leftTableLabel, rightTableLabel);

    YQL_CLOG(TRACE, CoreDq) << "[DqPeepholeRewriteJoinDict] join types"
        << ", left: " << *joinDict.LeftInput().Ref().GetTypeAnn()
        << ", right: " << *joinDict.RightInput().Ref().GetTypeAnn();

    const auto* leftRowType = GetSeqItemType(*joinDict.LeftInput().Ref().GetTypeAnn()).Cast<TStructExprType>();
    const auto* rightRowType = GetSeqItemType(*joinDict.RightInput().Ref().GetTypeAnn()).Cast<TStructExprType>();

    bool optKey = false, badKey = false;
    const bool filter = joinKind == "Inner" || joinKind.ends_with("Semi");
    const bool leftKind = joinKind.starts_with("Left");
    const bool rightKind = joinKind.starts_with("Right");
    TTypeAnnotationNode::TListType keyTypeItems;
    keyTypeItems.reserve(rightKeys.size());
    std::vector<std::string_view> lKeys(leftKeys.size()), rKeys(rightKeys.size());
    for (auto i = 0U; i < rightKeys.size() && !badKey; ++i) {
        const auto keyType1 = leftRowType->FindItemType(lKeys[i] = leftKeys[i]->Content());
        const auto keyType2 = rightRowType->FindItemType(rKeys[i] = rightKeys[i]->Content());
        if (leftKind) {
            keyTypeItems.emplace_back(JoinDryKeyType(keyType1, keyType2, optKey, ctx));
        } else if (rightKind){
            keyTypeItems.emplace_back(JoinDryKeyType(keyType2, keyType1, optKey, ctx));
        } else {
            keyTypeItems.emplace_back(CommonType<true>(node.Pos(), DryType(keyType1, optKey, ctx), DryType(keyType2, optKey, ctx), ctx));
            optKey = optKey && !filter;
        }
        badKey = !keyTypeItems.back();
    }

    const bool payload1 = joinKind != "RightOnly" && joinKind != "RightSemi";
    const bool payload2 = joinKind != "LeftOnly" && joinKind != "LeftSemi";

    const bool filter1 = filter || rightKind;
    const bool filter2 = filter || leftKind;

    auto list1 = joinDict.LeftInput().Ptr();
    auto list2 = joinDict.RightInput().Ptr();

    if (list1->IsCallable(TCoIterator::CallableName()))
        list1 = list1->HeadPtr();

    if (list2->IsCallable(TCoIterator::CallableName()))
        list2 = list2->HeadPtr();

    const auto lUnique = list1->GetConstraint<TUniqueConstraintNode>();
    const auto rUnique = list2->GetConstraint<TUniqueConstraintNode>();

    const bool uniqueLeft  = lUnique && lUnique->ContainsCompleteSet(lKeys);
    const bool uniqueRight = rUnique && rUnique->ContainsCompleteSet(rKeys);

    const bool multi1 = payload1 && !uniqueLeft;
    const bool multi2 = payload2 && !uniqueRight;

    TExprNode::TListType flags;
    if (uniqueLeft)
        flags.emplace_back(ctx.NewAtom(node.Pos(), "LeftUnique", TNodeFlags::Default));
    if (uniqueRight)
        flags.emplace_back(ctx.NewAtom(node.Pos(), "RightUnique", TNodeFlags::Default));

    if (badKey) {
        if (filter)
            return TExprBase(ctx.NewCallable(node.Pos(), "EmptyIterator", {ExpandType(node.Pos(), *node.Ref().GetTypeAnn(), ctx)}));

        lKeys.clear();
        rKeys.clear();
        keyTypeItems.clear();
        leftKeys.clear();
        rightKeys.clear();
        leftKeys.emplace_back(MakeBool<true>(node.Pos(), ctx));
        rightKeys.emplace_back(MakeBool<false>(node.Pos(), ctx));
    }

    list1 = ctx.WrapByCallableIf(ETypeAnnotationKind::Flow != list1->GetTypeAnn()->GetKind(), TCoToFlow::CallableName(), std::move(list1));
    list2 = ctx.WrapByCallableIf(ETypeAnnotationKind::Flow != list2->GetTypeAnn()->GetKind(), TCoToFlow::CallableName(), std::move(list2));

    list1 = PrepareListForJoin(std::move(list1), keyTypeItems, leftKeys, {}, payload1, optKey, filter1, ctx);
    list2 = PrepareListForJoin(std::move(list2), keyTypeItems, rightKeys, {}, payload2, optKey, filter2, ctx);

    list1 = MakeDictForJoin<true>(std::move(list1), payload1, multi1, ctx);
    list2 = MakeDictForJoin<true>(std::move(list2), payload2, multi2, ctx);

    // Join return list of tuple of structs. I.e. if you have tables t1 and t2 with values t1.a, t1.b and t2.c, t2.d,
    // you will receive List<Tuple<Struct<t1.a, t1.b>, Struct<t2.c, t2.d>>> and this data should be unpacked to
    // List<Struct<t1.a, t1.b, t2.c, t2.d>>
    const auto unpackData = UnpackJoinedData(leftRowType, rightRowType, leftTableLabel, rightTableLabel, joinDict.Pos(), ctx);

    return Build<TCoFlatMap>(ctx, joinDict.Pos())
        .Input(std::move(list1))
        .Lambda()
            .Args({"left"})
            .Body<TCoFlatMap>()
                .Input(std::move(list2))
                .Lambda()
                    .Args({"right"})
                    .Body<TCoMap>()
                        .Input<TCoJoinDict>()
                            .LeftInput("left")
                            .RightInput("right")
                            .JoinKind(joinDict.JoinType())
                            .Flags().Add(std::move(flags)).Build()
                            .Build()
                        .Lambda(unpackData)
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();
}

NNodes::TExprBase DqPeepholeRewritePureJoin(const NNodes::TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TDqJoin>()) {
        return node;
    }

    auto join = node.Cast<TDqJoin>();

    if (join.JoinType().Value() == "Cross") {
        return Build<TCoCollect>(ctx, join.Pos())
            .Input<TDqPhyCrossJoin>()
                .LeftInput<TCoIterator>()
                    .List(join.LeftInput())
                    .Build()
                .LeftLabel(join.LeftLabel())
                .RightInput<TCoIterator>()
                    .List(join.RightInput())
                    .Build()
                .RightLabel(join.RightLabel())
                .JoinType(join.JoinType())
                .JoinKeys(join.JoinKeys())
                .LeftJoinKeyNames(join.LeftJoinKeyNames())
                .RightJoinKeyNames(join.RightJoinKeyNames())
                .Build()
            .Done();
    } else {
        return Build<TCoCollect>(ctx, join.Pos())
            .Input<TDqPhyJoinDict>()
                .LeftInput<TCoIterator>()
                    .List(join.LeftInput())
                    .Build()
                .LeftLabel(join.LeftLabel())
                .RightInput<TCoIterator>()
                    .List(join.RightInput())
                    .Build()
                .RightLabel(join.RightLabel())
                .JoinType(join.JoinType())
                .JoinKeys(join.JoinKeys())
                .LeftJoinKeyNames(join.LeftJoinKeyNames())
                .RightJoinKeyNames(join.RightJoinKeyNames())
                .Build()
            .Done();
    }
}

NNodes::TExprBase DqPeepholeRewriteReplicate(const NNodes::TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TDqReplicate>()) {
        return node;
    }
    auto dqReplicate = node.Cast<TDqReplicate>();

    TVector<TExprBase> branches;
    branches.reserve(dqReplicate.Args().Count() - 1);
    auto inputIndex = NDq::BuildAtomList("0", dqReplicate.Pos(), ctx);
    for (size_t i = 1; i < dqReplicate.Args().Count(); ++i) {
        branches.emplace_back(inputIndex);
        branches.emplace_back(ctx.DeepCopyLambda(dqReplicate.Arg(i).Ref()));
    }

    return Build<TCoSwitch>(ctx, dqReplicate.Pos())
        .Input(dqReplicate.Input())
        .BufferBytes()
            .Value(ToString(128_MB))
            .Build()
        .FreeArgs()
            .Add(branches)
            .Build()
        .Done();
}

NNodes::TExprBase DqPeepholeDropUnusedInputs(const NNodes::TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TDqStageBase>()) {
        return node;
    }

    auto stage = node.Cast<TDqStageBase>();

    auto isArgumentUsed = [](const TExprNode::TPtr& node, const TExprNode* argument) {
        return !!FindNode(node,
            [](const TExprNode::TPtr& node) {
                return !TDqStageBase::Match(node.Get()) && !TDqPhyPrecompute::Match(node.Get());
            },
            [argument](const TExprNode::TPtr& node) {
                return node.Get() == argument;
            });
    };

    TDynBitMap unusedInputs;
    for (ui64 i = 0; i < stage.Inputs().Size(); ++i) {
        if (!isArgumentUsed(stage.Program().Body().Ptr(), stage.Program().Args().Arg(i).Raw())) {
            unusedInputs.Set(i);
        }
    }

    if (unusedInputs.Empty()) {
        return node;
    }

    TExprNode::TListType newInputs;
    TExprNode::TListType newArgs;
    TNodeOnNodeOwnedMap replaces;

    for (ui64 i = 0; i < stage.Inputs().Size(); ++i) {
        if (!unusedInputs.Test(i)) {
            newInputs.push_back(stage.Inputs().Item(i).Ptr());
            auto arg = stage.Program().Args().Arg(i).Raw();
            newArgs.push_back(ctx.NewArgument(arg->Pos(), arg->Content()));
            replaces[arg] = newArgs.back();
        }
    }

    auto children = node.Ref().ChildrenList();
    children[TDqStageBase::idx_Inputs] = ctx.NewList(stage.Inputs().Pos(), std::move(newInputs));
    children[TDqStageBase::idx_Program] = ctx.NewLambda(stage.Program().Pos(),
        ctx.NewArguments(stage.Program().Args().Pos(), std::move(newArgs)),
        ctx.ReplaceNodes(stage.Program().Body().Ptr(), replaces));

    return NNodes::TExprBase(ctx.ChangeChildren(node.Ref(), std::move(children)));
}

NNodes::TExprBase DqPeepholeRewriteLength(const NNodes::TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    if (!node.Maybe<TDqPhyLength>()) {
        return node;
    }

    auto dqPhyLength = node.Cast<TDqPhyLength>();
    if (typesCtx.IsBlockEngineEnabled()) {
        return NNodes::TExprBase(ctx.Builder(node.Pos())
            .Callable("NarrowMap")
                .Callable(0, "BlockCombineAll")
                    .Callable(0, "WideToBlocks")
                        .Add(0, MakeExpandMap(node.Pos(), {}, dqPhyLength.Input().Ptr(), ctx))
                    .Seal()
                    .Callable(1, "Void")
                    .Seal()
                    .List(2)
                        .List(0)
                            .Callable(0, "AggBlockApply")
                                .Atom(0, "count_all")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Lambda(1)
                    .Param("value")
                    .Callable("AsStruct")
                        .List(0)
                            .Atom(0, dqPhyLength.Name())
                            .Arg(1, "value")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build());
    }

    return Build<TCoCondense>(ctx, node.Pos())
        .Input(dqPhyLength.Input())
        .State<TCoAsStruct>()
            .Add<TCoNameValueTuple>()
                .Name(dqPhyLength.Name())
                .Value<TCoUint64>()
                    .Literal().Build("0")
                    .Build()
                .Build()
            .Build()
        .SwitchHandler()
            .Args({"item", "state"})
            .Body(MakeBool<false>(node.Pos(), ctx))
            .Build()
        .UpdateHandler()
            .Args({"item", "state"})
            .Body<TCoAsStruct>()
                .Add<TCoNameValueTuple>()
                    .Name(dqPhyLength.Name())
                    .Value<TCoAggrAdd>()
                        .Left<TCoMember>()
                            .Struct("state")
                            .Name(dqPhyLength.Name())
                            .Build()
                        .Right<TCoUint64>()
                            .Literal().Build("1")
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();
}

} // namespace NYql::NDq
