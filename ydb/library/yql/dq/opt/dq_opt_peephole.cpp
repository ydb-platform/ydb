#include "dq_opt_peephole.h"

#include <ydb/library/yql/core/yql_join.h> 
#include <ydb/library/yql/core/yql_opt_utils.h> 
#include <ydb/library/yql/core/yql_expr_type_annotation.h> 

#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/size_literals.h>

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

TExprNode::TPtr BuildDictKeySelector(TExprContext& ctx, TPositionHandle pos, const TExprNode::TListType& keyAtoms,
    const TTypeAnnotationNode::TListType& keyDryTypes, bool optional)
{
    YQL_ENSURE(keyAtoms.size() == keyDryTypes.size());

    TExprNode::TListType keysTuple;

    auto keySelectorArg = Build<TCoArgument>(ctx, pos)
        .Name("keyArg")
        .Done();

    for (const auto& atom: keyAtoms) {
        auto member = Build<TCoMember>(ctx, pos)
            .Struct(keySelectorArg)
            .Name(atom)
            .Done();

        keysTuple.emplace_back(member.Ptr());
    }

    if (keysTuple.size() == 1) {
        return optional
            ? Build<TCoLambda>(ctx, pos)
                .Args({keySelectorArg})
                .Body<TCoStrictCast>()
                    .Value(keysTuple[0])
                    .Type(ExpandType(pos, *keyDryTypes[0], ctx))
                    .Build()
                .Done().Ptr()
            : Build<TCoLambda>(ctx, pos)
                .Args({keySelectorArg})
                .Body<TCoJust>()
                    .Input(keysTuple[0])
                    .Build()
                .Done().Ptr();
    }

    auto type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TTupleExprType>(keyDryTypes));
    return optional
        ? Build<TCoLambda>(ctx, pos)
            .Args({keySelectorArg})
            .Body<TCoStrictCast>()
                .Value(ctx.NewList(pos, std::move(keysTuple)))
                .Type(ExpandType(pos, *type, ctx))
                .Build()
            .Done().Ptr()
        : Build<TCoLambda>(ctx, pos)
            .Args({keySelectorArg})
            .Body<TCoJust>()
                .Input(ctx.NewList(pos, std::move(keysTuple)))
                .Build()
            .Done().Ptr();
}

} // anonymous namespace end

/**
 * Rewrites a `KqpMapJoin` to the `MapJoinCore`.
 *
 * Restrictions:
 *  - Don't select join strategy, always use `MapJoin`
 *  - Explicitly convert right input to the dict
 *  - Use quite pretty trick: do `MapJoinCore` in `FlatMap`-lambda
 *    (rely on the fact that there will be only one element in the `FlatMap`-stream)
 */
TExprBase DqPeepholeRewriteMapJoin(const TExprBase& node, TExprContext& ctx) {
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

    const auto itemTypeLeft = GetSeqItemType(mapJoin.LeftInput().Ref().GetTypeAnn())->Cast<TStructExprType>();
    const auto itemTypeRight = GetSeqItemType(mapJoin.RightInput().Ref().GetTypeAnn())->Cast<TStructExprType>();

    TExprNode::TListType leftRenames = makeRenames(leftTableLabel, *itemTypeLeft);
    TExprNode::TListType rightRenames, rightPayloads;
    const bool withRightSide = mapJoin.JoinType().Value() != "LeftOnly" && mapJoin.JoinType().Value() != "LeftSemi";
    if (withRightSide) {
        rightRenames = makeRenames(rightTableLabel, *itemTypeRight);
        rightPayloads.reserve(rightRenames.size() >> 1U);
        for (auto it = rightRenames.cbegin(); rightRenames.cend() != it; ++++it)
            rightPayloads.emplace_back(*it);
    }

    TTypeAnnotationNode::TListType keyTypes(keyWidth);
    for (auto i = 0U; i < keyTypes.size(); ++i) {
        const auto keyTypeLeft = itemTypeLeft->FindItemType(leftKeyColumnNodes[i]->Content());
        const auto keyTypeRight = itemTypeRight->FindItemType(rightKeyColumnNodes[i]->Content());
        bool optKey = false;
        keyTypes[i] = JoinDryKeyType(keyTypeLeft, keyTypeRight, optKey, ctx);
        if (!keyTypes[i])
            keyTypes.clear();
    }

    auto leftInput = ctx.NewCallable(mapJoin.LeftInput().Pos(), "ToFlow", {mapJoin.LeftInput().Ptr()});
    auto rightInput = ctx.NewCallable(mapJoin.RightInput().Pos(), "ToFlow", {mapJoin.RightInput().Ptr()});

    if (keyTypes.empty()) {
        const auto type = mapJoin.Ref().GetTypeAnn();
        if (mapJoin.JoinType().Value() == "Inner" || mapJoin.JoinType().Value() == "LeftSemi")
            return TExprBase(ctx.NewCallable(pos, "EmptyIterator", {ExpandType(pos, *type, ctx)}));

        const auto structType = GetSeqItemType(type)->Cast<TStructExprType>();
        return TExprBase(ctx.Builder(pos)
            .Callable("Map")
                .Add(0, std::move(leftInput))
                .Lambda(0)
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
    rightInput = MakeDictForJoin<true>(PrepareListForJoin(std::move(rightInput), keyTypes, rightKeyColumnNodes, rightPayloads, payloads, false, true, ctx), payloads, withRightSide, ctx);

    return Build<TCoFlatMap>(ctx, pos)
        .Input(std::move(rightInput))
        .Lambda()
            .Args({"dict"})
            .Body<TCoMapJoinCore>()
                .LeftInput(std::move(leftInput))
                .RightDict("dict")
                .JoinKind(mapJoin.JoinType())
                .LeftKeysColumns(ctx.NewList(pos, std::move(leftKeyColumnNodes)))
                .LeftRenames(ctx.NewList(pos, std::move(leftRenames)))
                .RightRenames(ctx.NewList(pos, std::move(rightRenames)))
                .Build()
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
        for (auto key : GetSeqItemType(input.Ref().GetTypeAnn())->Cast<TStructExprType>()->GetItems()) {
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

    const auto* leftRowType = GetSeqItemType(joinDict.LeftInput().Ref().GetTypeAnn())->Cast<TStructExprType>();
    const auto* rightRowType = GetSeqItemType(joinDict.RightInput().Ref().GetTypeAnn())->Cast<TStructExprType>();

    bool optKeyLeft = false, optKeyRight = false, badKey = false;
    TTypeAnnotationNode::TListType keyTypeItems;
    keyTypeItems.reserve(leftKeys.size());
    for (auto i = 0U; i < leftKeys.size(); ++i) {
        auto leftKeyType = leftRowType->FindItemType(leftKeys[i]->Content());
        auto rightKeyType = rightRowType->FindItemType(rightKeys[i]->Content());
        keyTypeItems.emplace_back(CommonType<true>(node.Pos(), DryType(leftKeyType, optKeyLeft, ctx), DryType(rightKeyType, optKeyRight, ctx), ctx));
        badKey = !keyTypeItems.back();
        if (badKey) {
            YQL_CLOG(DEBUG, CoreDq) << "Not comparable keys in join: " << leftKeys[i]->Content()
                << "(" << *leftKeyType << ") vs " << rightKeys[i]->Content() << "(" << *rightKeyType << ")";
            break;
        }
    }

    TExprNode::TPtr leftKeySelector;
    TExprNode::TPtr rightKeySelector;

    if (badKey) {
        leftKeySelector = Build<TCoLambda>(ctx, node.Pos())
            .Args({"row"})
            .Body<TCoBool>()
                .Literal().Build("true")
                .Build()
            .Done().Ptr();

        rightKeySelector = Build<TCoLambda>(ctx, node.Pos())
            .Args({"item"})
            .Body<TCoBool>()
                .Literal().Build("false")
                .Build()
            .Done().Ptr();
    } else {
        leftKeySelector = BuildDictKeySelector(ctx, joinDict.Pos(), leftKeys, keyTypeItems, optKeyLeft);
        rightKeySelector = BuildDictKeySelector(ctx, joinDict.Pos(), rightKeys, keyTypeItems, optKeyRight);
    }

    auto streamToDict = [&ctx](const TExprBase& input, const TExprNode::TPtr& keySelector) {
        return Build<TCoSqueezeToDict>(ctx, input.Pos())
            .Stream(input)
            .KeySelector(keySelector)
            .PayloadSelector()
                .Args({"item"})
                .Body("item")
                .Build()
            .Settings()
                .Add<TCoAtom>().Build("Hashed")
                .Add<TCoAtom>().Build("Many")
                .Add<TCoAtom>().Build("Compact")
                .Build()
            .Done();
    };

    auto leftDict = streamToDict(joinDict.LeftInput(), leftKeySelector);
    auto rightDict = streamToDict(joinDict.RightInput(), rightKeySelector);

    auto join = Build<TCoFlatMap>(ctx, joinDict.Pos())
        .Input(leftDict) // only 1 element with dict
        .Lambda()
            .Args({"left"})
            .Body<TCoFlatMap>()
                .Input(rightDict) // only 1 element with dict
                .Lambda()
                    .Args({"right"})
                    .Body<TCoJoinDict>()
                        .LeftInput("left")
                        .RightInput("right")
                        .JoinKind(joinDict.JoinType())
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();

    // Join return list of tuple of structs. I.e. if you have tables t1 and t2 with values t1.a, t1.b and t2.c, t2.d,
    // you will receive List<Tuple<Struct<t1.a, t1.b>, Struct<t2.c, t2.d>>> and this data should be unpacked to
    // List<Struct<t1.a, t1.b, t2.c, t2.d>>
    auto unpackData = UnpackJoinedData(leftRowType, rightRowType, leftTableLabel, rightTableLabel, join.Pos(), ctx);

    return Build<TCoMap>(ctx, joinDict.Pos())
        .Input(join)
        .Lambda(unpackData)
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
        branches.emplace_back(ctx.DeepCopyLambda(dqReplicate.Args().Get(i).Ref()));
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

} // namespace NYql::NDq
