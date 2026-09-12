#include "yql_sql_combine_expander.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_join.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

namespace {

using namespace NNodes;

struct TPresortTraits {
    bool HasPresort;
    TExprNode::TListType Keys;
    TExprNode::TListType Dirs;
};

TString MakeListJoinCoreKeyName(size_t index) {
    return TStringBuilder() << YqlListJoinCoreKeyPrefix << index;
}

TExprNode::TPtr BuildStrictCastWrapper(const TPositionHandle& pos, const TTypeAnnotationNode* targetType, const TExprNode::TPtr& keyComponent, TExprContext& ctx) {
    // clang-format off
    return ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .Callable("StrictCast")
                .Callable(0, "Just")
                    .Apply(0, keyComponent)
                        .With(0, "row")
                    .Seal()
                .Seal()
                .Add(1, ExpandType(pos, *ctx.MakeType<TOptionalExprType>(targetType), ctx))
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr BuildNthKeyComponentWrapper(const TPositionHandle& pos, const TExprNode::TPtr& keyExtractor, size_t index, TExprContext& ctx) {
    // clang-format off
    return ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .Callable("Nth")
                .Apply(0, keyExtractor)
                    .With(0, "row")
                .Seal()
                .Atom(1, ToString(index))
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNodeBuilder& AppendKeyComponent(TExprNodeBuilder& parent, size_t index, const TStringBuf keyName, const TExprNode::TPtr& keyExtractor) {
    // clang-format off
    return parent.List(index)
        .Atom(0, keyName)
        .Apply(1, keyExtractor)
            .With(0, "row")
        .Seal()
    .Seal();
    // clang-format on
}

TExprNode::TPtr PrepareSqlCombineInputSource(const TExprNode& combineInput, ui32 tableIndex, TStringBuf inputPrefix, const TTypeAnnotationNode* commonKeyType, TPositionHandle pos, TExprContext& ctx) {
    const auto keyExtract = combineInput.Child(TCoSqlCombineInput::idx_KeyExtractLambda);
    const auto sourceKeyType = keyExtract->GetTypeAnn();
    const auto presortUsedColumns = combineInput.Child(TCoSqlCombineInput::idx_PresortUsedColumns)->ChildrenList();
    const auto argMapUsedColumns = combineInput.Child(TCoSqlCombineInput::idx_ArgMapUsedColumns)->ChildrenList();
    TSet<TStringBuf> usedColumnNames;
    for (const auto& atom : presortUsedColumns) {
        usedColumnNames.insert(atom->Content());
    }
    for (const auto& atom : argMapUsedColumns) {
        usedColumnNames.insert(atom->Content());
    }
    TExprNode::TListType inputUsedColumns;
    inputUsedColumns.reserve(usedColumnNames.size());
    for (const auto& name : usedColumnNames) {
        inputUsedColumns.push_back(ctx.NewAtom(pos, name));
    }
    TExprNode::TListType keyComponents;
    TExprNode::TListType filterNullMemberNames;
    TVector<TString> replaceMemberNames;
    if (commonKeyType->GetKind() != ETypeAnnotationKind::Tuple) {
        TExprNode::TPtr keyComponent = keyExtract;
        if (sourceKeyType != commonKeyType) {
            keyComponent = BuildStrictCastWrapper(pos, commonKeyType, keyComponent, ctx);
            replaceMemberNames.emplace_back(MakeListJoinCoreKeyName(0));
            filterNullMemberNames.push_back(ctx.NewAtom(pos, replaceMemberNames.back()));
        }
        keyComponents.push_back(std::move(keyComponent));
    } else {
        const auto targetTypeItems = commonKeyType->Cast<TTupleExprType>()->GetItems();
        const auto sourceTypeItems = sourceKeyType->Cast<TTupleExprType>()->GetItems();
        for (ui32 i = 0; i < commonKeyType->Cast<TTupleExprType>()->GetSize(); i++) {
            TExprNode::TPtr keyComponent = BuildNthKeyComponentWrapper(pos, keyExtract, i, ctx);
            if (sourceTypeItems[i] != targetTypeItems[i]) {
                keyComponent = BuildStrictCastWrapper(pos, targetTypeItems[i], keyComponent, ctx);
                replaceMemberNames.push_back(MakeListJoinCoreKeyName(i));
                filterNullMemberNames.push_back(ctx.NewAtom(pos, replaceMemberNames.back()));
            }
            keyComponents.push_back(std::move(keyComponent));
        }
    }

    // clang-format off
    return ctx.Builder(pos)
        .Callable("FlatMap")
            .Add(0, combineInput.HeadPtr())
            .Lambda(1)
                .Param("row")
                .Callable("FilterNullMembers")
                    .Callable(0, "Just")
                        .Callable(0, "FlattenMembers")
                            .List(0)
                                .Atom(0, inputPrefix)
                                .Callable(1, "FilterMembers")
                                    .Arg(0, "row")
                                    .Add(1, ctx.NewList(pos, std::move(inputUsedColumns)))
                                .Seal()

                            .Seal()
                            .List(1)
                                .Atom(0, "")
                                .Callable(1, "AsStruct")
                                    .List(0)
                                        .Atom(0, "_yql_table_index")
                                        .Callable(1, "Uint32")
                                            .Atom(0, ToString(tableIndex))
                                        .Seal()
                                    .Seal()
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        for (ui32 i = 0; i < keyComponents.size(); i++) {
                                            AppendKeyComponent(parent, i + 1, MakeListJoinCoreKeyName(i), keyComponents[i]);
                                        }
                                        return parent;
                                    })
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                    .List(1)
                        .Add(std::move(filterNullMemberNames))
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr BuildPrefixedKeyExtractor(const TPositionHandle& pos, TStringBuf inputPrefix, const TExprNode::TPtr& keyExtract, TExprContext& ctx) {
    // clang-format off
    return ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .Apply(keyExtract)
                .With(0)
                    .Callable("DivePrefixMembers")
                        .Arg(0, "row")
                        .List(1)
                            .Atom(0, inputPrefix)
                        .Seal()
                    .Seal()
                .Done()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TPresortTraits BuildSqlCombinePresortTraits(const TExprNode& combineInput, TStringBuf inputPrefix, TPositionHandle pos, TExprContext& ctx) {
    const auto& presortKey = combineInput.Child(TCoSqlCombineInput::idx_PresortKeyLambda);
    const auto& presortDir = combineInput.Child(TCoSqlCombineInput::idx_PresortDirectionNode);
    const auto keyType = presortKey->GetTypeAnn();
    const bool isMulti = keyType->GetKind() == ETypeAnnotationKind::Tuple;
    const ui32 presortSize = isMulti ? keyType->Cast<TTupleExprType>()->GetSize() : 1;

    TPresortTraits traits;
    traits.HasPresort = !presortKey->IsCallable("Void");
    traits.Keys.reserve(presortSize);
    traits.Dirs.reserve(presortSize);

    if (!traits.HasPresort) {
        return traits;
    }

    if (!isMulti) {
        // clang-format off
        traits.Keys.push_back(ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    return parent.Apply(BuildPrefixedKeyExtractor(pos, inputPrefix, presortKey, ctx))
                        .With(0, "row")
                    .Seal();
                })
            .Seal().Build());
        // clang-format on
        traits.Dirs.emplace_back(presortDir);
        return traits;
    }

    for (ui32 i = 0; i < presortSize; i++) {
        // clang-format off
        traits.Keys.push_back(ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    return parent.Apply(BuildNthKeyComponentWrapper(pos, BuildPrefixedKeyExtractor(pos, inputPrefix, presortKey, ctx), i, ctx))
                        .With(0, "row")
                    .Seal();
                })
            .Seal().Build());
        // clang-format on
        traits.Dirs.push_back(presortDir->ChildPtr(i));
    }
    return traits;
}

std::pair<TExprNode::TPtr, TExprNode::TPtr> BuildSortKeySelectorAndDirection(const TPositionHandle& pos, TPresortTraits&& leftPresort, TPresortTraits&& rightPresort, TExprContext& ctx) {
    if (!leftPresort.HasPresort && !rightPresort.HasPresort) {
        const auto noPresort = ctx.NewCallable(pos, "Void", {});
        return {noPresort, noPresort};
    }
    // clang-format off
    const auto presortKeySelector = ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .Do([&leftPresort, &rightPresort](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                // clang-format on
                auto list = parent.List();
                ui32 idx = 0;
                // clang-format off
                list.Callable(idx++, "Member")
                    .Arg(0, "row")
                    .Atom(1, "_yql_table_index")
                .Seal();
                // clang-format on
                for (auto& k : leftPresort.Keys) {
                    // clang-format off
                    list.Apply(idx++, *k)
                        .With(0, "row")
                    .Seal();
                    // clang-format on
                }
                for (auto& k : rightPresort.Keys) {
                    // clang-format off
                    list.Apply(idx++, *k)
                        .With(0, "row")
                    .Seal();
                    // clang-format on
                }
                return list.Seal();
            // clang-format off
            })
        .Seal().Build();
    // clang-format on
    TExprNode::TListType dirList;
    dirList.push_back(ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, "true")}));
    for (auto& d : leftPresort.Dirs) {
        dirList.push_back(std::move(d));
    }
    for (auto& d : rightPresort.Dirs) {
        dirList.push_back(std::move(d));
    }
    const auto presortDirection = ctx.NewList(pos, std::move(dirList));
    return {presortKeySelector, presortDirection};
}

TExprNode::TPtr BuildPartitionKeySelector(const TPositionHandle& pos, const TTypeAnnotationNode* keyType, TExprContext& ctx) {
    if (keyType->GetKind() != ETypeAnnotationKind::Tuple) {
        // clang-format off
        return ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Callable("Member")
                    .Arg(0, "row")
                    .Atom(1, MakeListJoinCoreKeyName(0))
                .Seal()
            .Seal().Build();
        // clang-format on
    }
    // clang-format off
    return ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .List()
                .Do([&keyType](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    // clang-format on
                    for (ui32 i = 0; i < keyType->Cast<TTupleExprType>()->GetSize(); i++) {
                        // clang-format off
                        parent.Callable(i, "Member")
                            .Arg(0, "row")
                            .Atom(1, MakeListJoinCoreKeyName(i))
                        .Seal();
                        // clang-format on
                    }
                    return parent;
                // clang-format off
                })
            .Seal()
        .Seal().Build();
    // clang-format on
}

} // namespace

TExprNode::TPtr ExpandSqlCombine(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    Y_UNUSED(typesCtx);
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();

    const auto& leftCombineInput = *node->Child(TCoSqlCombine::idx_LeftInput);
    const auto& rightCombineInput = *node->Child(TCoSqlCombine::idx_RightInput);
    const auto& usingLambda = *node->Child(TCoSqlCombine::idx_UsingLambda);
    const auto pos = node->Pos();

    const auto commonKeyType = usingLambda.Head().Head().GetTypeAnn();

    const auto leftTagged = PrepareSqlCombineInputSource(leftCombineInput, 0, YqlListJoinCoreLeftInputPrefix,  commonKeyType, pos, ctx);
    const auto rightTagged = PrepareSqlCombineInputSource(rightCombineInput, 1, YqlListJoinCoreRightInputPrefix, commonKeyType, pos, ctx);

    // clang-format off
    auto merged = ctx.Builder(pos)
        .Callable("UnionAll")
            .Add(0, leftTagged)
            .Add(1, rightTagged)
        .Seal().Build();
    // clang-format on

    auto leftPresort = BuildSqlCombinePresortTraits(leftCombineInput,  YqlListJoinCoreLeftInputPrefix,  pos, ctx);
    auto rightPresort = BuildSqlCombinePresortTraits(rightCombineInput, YqlListJoinCoreRightInputPrefix, pos, ctx);
    auto [presortKeySelector, presortDirection] = BuildSortKeySelectorAndDirection(pos, std::move(leftPresort), std::move(rightPresort), ctx);

    auto partitionKeySelector = BuildPartitionKeySelector(pos, commonKeyType, ctx);

    // clang-format off
    auto groupSwitch = ctx.Builder(pos)
        .Lambda()
            .Param("key")
            .Param("item")
            .Callable("IsKeySwitch")
                .Arg(0, "key")
                .Arg(1, "item")
                .Lambda(2)
                    .Param("k")
                    .Arg("k")
                .Seal()
                .Add(3, partitionKeySelector)
            .Seal()
        .Seal().Build();
    // clang-format on

    const auto& leftArgMapLambda = leftCombineInput.ChildPtr(TCoSqlCombineInput::idx_ArgMapLambda);
    const auto& rightArgMapLambda = rightCombineInput.ChildPtr(TCoSqlCombineInput::idx_ArgMapLambda);
    const auto& leftInputItemType = *leftArgMapLambda->Head().Head().GetTypeAnn();
    const auto& rightInputItemType = *rightArgMapLambda->Head().Head().GetTypeAnn();
    // clang-format off
    auto chopperHandler = ctx.Builder(pos)
        .Lambda()
            .Param("key")
            .Param("groupStream")
            .Callable("ListJoinCore")
                .Arg(0, "groupStream")
                .Add(1, ExpandType(pos, *commonKeyType, ctx))
                .Add(2, leftArgMapLambda)
                .Add(3, ExpandType(pos, leftInputItemType, ctx))
                .Add(4, rightArgMapLambda)
                .Add(5, ExpandType(pos, rightInputItemType, ctx))
                .Lambda(6)
                    .Param("keyArg")
                    .Param("leftList")
                    .Param("rightList")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        parent.Callable("ToStream")
                            .Callable(0, "ToSequence")
                                .Apply(0, usingLambda)
                                    .With(0, "keyArg")
                                    .With(1, "leftList")
                                    .With(2, "rightList")
                                .Seal()
                            .Seal()
                        .Seal();
                        // clang-format on
                        return parent;
                    // clang-format off
                    })
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on

    // clang-format off
    return ctx.Builder(pos)
        .Callable("LPartitionsByKeys")
            .Add(0, std::move(merged))
            .Add(1, partitionKeySelector)
            .Add(2, std::move(presortDirection))
            .Add(3, std::move(presortKeySelector))
            .Lambda(4)
                .Param("partitionStream")
                .Callable("Chopper")
                    .Arg(0, "partitionStream")
                    .Add(1, partitionKeySelector)
                    .Add(2, std::move(groupSwitch))
                    .Add(3, std::move(chopperHandler))
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

} // namespace NYql
