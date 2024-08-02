#include "type_ann_core.h"
#include "type_ann_impl.h"
#include "type_ann_list.h"

#include <ydb/library/yql/core/sql_types/time_order_recover.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_opt_window.h>
#include <ydb/library/yql/core/yql_type_helpers.h>

#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <util/generic/algorithm.h>
#include <util/string/join.h>

namespace NYql {
namespace NTypeAnnImpl {

using namespace NNodes;

namespace {
    bool IsEmptyList(const TExprNode::TPtr& x) {
        return x->GetTypeAnn() && x->GetTypeAnn()->GetKind() == ETypeAnnotationKind::EmptyList;
    };

    TExprNode::TPtr RewriteMultiAggregate(const TExprNode& node, TExprContext& ctx) {
        auto exprLambda = node.Child(1);
        const TStructExprType* structType = nullptr;
        const TTupleExprType* tupleType = nullptr;
        const TListExprType* listType = nullptr;
        ui32 elemsCount = 0;
        const auto& type = *exprLambda->GetTypeAnn();
        if (type.GetKind() == ETypeAnnotationKind::Struct) {
            structType = type.Cast<TStructExprType>();
            elemsCount = structType->GetSize();
        } else if (type.GetKind() == ETypeAnnotationKind::Tuple) {
            tupleType = type.Cast<TTupleExprType>();
            elemsCount = tupleType->GetSize();
        } else {
            listType = type.Cast<TListExprType>();
            elemsCount = 1;
        }

        auto atom0 = ctx.NewAtom(node.Pos(), 0U);
        auto atom1 = ctx.NewAtom(node.Pos(), 1U);
        TExprNode::TPtr initArg1 = ctx.NewArgument(node.Pos(), "item");
        TExprNode::TPtr initArg2 = ctx.NewArgument(node.Pos(), "parent");
        TExprNode::TListType initBodyArgs;

        TExprNode::TPtr updateArg1 = ctx.NewArgument(node.Pos(), "item");
        TExprNode::TPtr updateArg2 = ctx.NewArgument(node.Pos(), "state");
        TExprNode::TPtr updateArg3 = ctx.NewArgument(node.Pos(), "parent");
        TExprNode::TListType updateBodyArgs;

        TExprNode::TPtr shiftArg1 = ctx.NewArgument(node.Pos(), "item");
        TExprNode::TPtr shiftArg2 = ctx.NewArgument(node.Pos(), "state");
        TExprNode::TPtr shiftArg3 = ctx.NewArgument(node.Pos(), "parent");
        TExprNode::TListType shiftBodyArgs;

        TExprNode::TPtr saveArg1 = ctx.NewArgument(node.Pos(), "state");
        TExprNode::TListType saveBodyArgs;

        TExprNode::TPtr loadArg1 = ctx.NewArgument(node.Pos(), "state");
        TExprNode::TListType loadBodyArgs;

        TExprNode::TPtr mergeArg1 = ctx.NewArgument(node.Pos(), "state1");
        TExprNode::TPtr mergeArg2 = ctx.NewArgument(node.Pos(), "state2");
        TExprNode::TListType mergeBodyArgs;

        TExprNode::TPtr finishArg1 = ctx.NewArgument(node.Pos(), "state");
        TExprNode::TListType finishBodyArgs;

        TExprNode::TListType defValueArgs;

        auto traitsFactory = node.ChildPtr(2);
        auto traitsFactoryBody = traitsFactory->ChildPtr(1);
        if (traitsFactoryBody->IsCallable("ToWindowTraits")) {
            if (!traitsFactoryBody->Head().IsCallable("AggregationTraits")) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Expected AggregationTraits inside ToWindowTraits"));
                return nullptr;
            }

            traitsFactoryBody = ExpandToWindowTraits(traitsFactoryBody->Head(), ctx);
        }

        if (!traitsFactoryBody->IsCallable("AggregationTraits") && !traitsFactoryBody->IsCallable("WindowTraits")) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Expected AggregationTraits or WindowTraits"));
            return nullptr;
        }

        bool onWindow = traitsFactoryBody->IsCallable("WindowTraits");

        bool isNullDefValue = true;
        bool isLambdaDefValue = false;
        for (ui32 elem = 0; elem < elemsCount; ++elem) {
            TExprNode::TPtr extractor;
            TExprNode::TPtr memberAtom;
            TExprNode::TPtr nthAtom;
            if (structType) {
                auto member = structType->GetItems()[elem];
                auto memberName = member->GetName();
                memberAtom = ctx.NewAtom(node.Pos(), memberName);
                extractor = ctx.Builder(node.Pos())
                    .Lambda()
                        .Param("row")
                        .Callable("Member")
                            .Apply(0, exprLambda).With(0, "row").Seal()
                            .Add(1, memberAtom)
                        .Seal()
                    .Seal()
                    .Build();
            } else if (tupleType) {
                nthAtom = ctx.NewAtom(node.Pos(), ToString(elem));
                extractor = ctx.Builder(node.Pos())
                    .Lambda()
                        .Param("row")
                        .Callable("Nth")
                            .Apply(0, exprLambda).With(0, "row").Seal()
                            .Add(1, nthAtom)
                        .Seal()
                    .Seal()
                    .Build();
            } else {
                extractor = ctx.Builder(node.Pos())
                    .Lambda()
                        .Param("row")
                        .Arg("row")
                    .Seal()
                    .Build();
            }

            TNodeOnNodeOwnedMap factoryReplaces;
            factoryReplaces[traitsFactory->Head().Child(0)] = listType ? ExpandType(node.Pos(), *listType->GetItemType(), ctx) : node.HeadPtr();
            factoryReplaces[traitsFactory->Head().Child(1)] = extractor;

            auto traits = ctx.ReplaceNodes(TExprNode::TPtr(traitsFactoryBody), factoryReplaces);
            ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
            auto status = ExpandApply(traits, traits, ctx);
            if (status == IGraphTransformer::TStatus::Error) {
                return nullptr;
            }

            {
                TExprNode::TPtr listItemArg = ctx.NewArgument(node.Pos(), "listitem");
                auto initLambda = traits->Child(1);
                TNodeOnNodeOwnedMap replaces;
                replaces[initLambda->Head().Child(0)] = listType ? listItemArg : initArg1;
                if (initLambda->Head().ChildrenSize() == 2) {
                    replaces[initLambda->Head().Child(1)] = initArg2;
                }

                auto replaced = ctx.ReplaceNodes(initLambda->TailPtr(), replaces);
                if (structType) {
                    initBodyArgs.push_back(ctx.NewList(node.Pos(), { memberAtom, replaced }));
                } else if (tupleType) {
                    initBodyArgs.push_back(replaced);
                } else {
                    initBodyArgs.push_back(ctx.ReplaceNode(exprLambda->TailPtr(), exprLambda->Head().Head(), initArg1));
                    initBodyArgs.push_back(ctx.NewLambda(node.Pos(),
                        ctx.NewArguments(node.Pos(), { listItemArg }),
                        std::move(replaced)));
                }
            }

            {
                TExprNode::TPtr listItemArg = ctx.NewArgument(node.Pos(), "listitem");
                auto updateLambda = traits->Child(2);
                TNodeOnNodeOwnedMap replaces;
                replaces[updateLambda->Head().Child(0)] = listType ?
                    ctx.NewCallable(node.Pos(), "Nth", { listItemArg, atom0 }) :
                    updateArg1;
                replaces[updateLambda->Head().Child(1)] = structType ?
                    ctx.NewCallable(node.Pos(), "Member", { updateArg2, memberAtom }) :
                    ctx.NewCallable(node.Pos(), "Nth", { listType ? listItemArg : updateArg2, listType ? atom1 : nthAtom });
                if (updateLambda->Head().ChildrenSize() == 3) {
                    replaces[updateLambda->Head().Child(2)] = updateArg3;
                }

                auto replaced = ctx.ReplaceNodes(updateLambda->TailPtr(), replaces);
                if (structType) {
                    updateBodyArgs.push_back(ctx.NewList(node.Pos(), { memberAtom, replaced }));
                } else if (tupleType) {
                    updateBodyArgs.push_back(replaced);
                } else {
                    updateBodyArgs.push_back(ctx.NewCallable(node.Pos(), "Collect", { ctx.NewCallable(node.Pos(), "Zip", {
                        ctx.ReplaceNode(exprLambda->TailPtr(), exprLambda->Head().Head(), updateArg1),
                        updateArg2 }) }));
                    updateBodyArgs.push_back(ctx.NewLambda(node.Pos(),
                        ctx.NewArguments(node.Pos(), { listItemArg }),
                        std::move(replaced)));
                }
            }

            if (onWindow) {
                {
                    TExprNode::TPtr listItemArg = ctx.NewArgument(node.Pos(), "listitem");
                    auto shiftLambda = traits->Child(3);
                    TNodeOnNodeOwnedMap replaces;
                    replaces[shiftLambda->Head().Child(0)] = listType ?
                        ctx.NewCallable(node.Pos(), "Nth", { listItemArg, atom0 }) :
                        shiftArg1;
                    replaces[shiftLambda->Head().Child(1)] = structType ?
                        ctx.NewCallable(node.Pos(), "Member", { shiftArg2, memberAtom }) :
                        ctx.NewCallable(node.Pos(), "Nth", { listType ? listItemArg : shiftArg2, listType ? atom1 : nthAtom });
                    if (shiftLambda->Head().ChildrenSize() == 3) {
                        replaces[shiftLambda->Head().Child(2)] = shiftArg3;
                    }

                    auto replaced = ctx.ReplaceNodes(shiftLambda->TailPtr(), replaces);
                    if (structType) {
                        shiftBodyArgs.push_back(ctx.NewList(node.Pos(), { memberAtom, replaced }));
                    } else if (tupleType) {
                        shiftBodyArgs.push_back(replaced);
                    } else {
                        shiftBodyArgs.push_back(ctx.NewCallable(node.Pos(), "Collect",
                            { ctx.NewCallable(node.Pos(), "Zip", { exprLambda, shiftArg2 }) }));
                        shiftBodyArgs.push_back(ctx.NewLambda(node.Pos(),
                            ctx.NewArguments(node.Pos(), { listItemArg }),
                            std::move(replaced)));
                    }
                }
            }

            if (!onWindow) {
                {
                    TExprNode::TPtr listItemArg = ctx.NewArgument(node.Pos(), "listitem");
                    auto saveLambda = traits->Child(3);
                    TNodeOnNodeOwnedMap replaces;
                    replaces[saveLambda->Head().Child(0)] = listType ? listItemArg : (structType ?
                        ctx.NewCallable(node.Pos(), "Member", { saveArg1, memberAtom }) :
                        ctx.NewCallable(node.Pos(), "Nth", { saveArg1, nthAtom }));
                    auto replaced = ctx.ReplaceNodes(saveLambda->TailPtr(), replaces);
                    if (structType) {
                        saveBodyArgs.push_back(ctx.NewList(node.Pos(), { memberAtom, replaced }));
                    } else if (tupleType) {
                        saveBodyArgs.push_back(replaced);
                    } else {
                        saveBodyArgs.push_back(saveArg1);
                        saveBodyArgs.push_back(ctx.NewLambda(node.Pos(),
                            ctx.NewArguments(node.Pos(), { listItemArg }),
                            std::move(replaced)));
                    }
                }

                {
                    TExprNode::TPtr listItemArg = ctx.NewArgument(node.Pos(), "listitem");
                    auto loadLambda = traits->Child(4);
                    TNodeOnNodeOwnedMap replaces;
                    replaces[loadLambda->Head().Child(0)] = listType ? listItemArg : (structType ?
                        ctx.NewCallable(node.Pos(), "Member", { loadArg1, memberAtom }) :
                        ctx.NewCallable(node.Pos(), "Nth", { loadArg1, nthAtom }));
                    auto replaced = ctx.ReplaceNodes(loadLambda->TailPtr(), replaces);
                    if (structType) {
                        loadBodyArgs.push_back(ctx.NewList(node.Pos(), { memberAtom, replaced }));
                    } else if (tupleType) {
                        loadBodyArgs.push_back(replaced);
                    } else {
                        loadBodyArgs.push_back(loadArg1);
                        loadBodyArgs.push_back(ctx.NewLambda(node.Pos(),
                            ctx.NewArguments(node.Pos(), { listItemArg }),
                            std::move(replaced)));
                    }
                }

                {
                    TExprNode::TPtr listItemArg = ctx.NewArgument(node.Pos(), "listitem");
                    auto mergeLambda = traits->Child(5);
                    TNodeOnNodeOwnedMap replaces;
                    replaces[mergeLambda->Head().Child(0)] = structType ?
                        ctx.NewCallable(node.Pos(), "Member", { mergeArg1, memberAtom }) :
                        ctx.NewCallable(node.Pos(), "Nth", { listType ? listItemArg : mergeArg1, listType ? atom0 : nthAtom });
                    replaces[mergeLambda->Head().Child(1)] = structType ?
                        ctx.NewCallable(node.Pos(), "Member", { mergeArg2, memberAtom }) :
                        ctx.NewCallable(node.Pos(), "Nth", { listType ? listItemArg : mergeArg2, listType ? atom1 : nthAtom });
                    auto replaced = ctx.ReplaceNodes(mergeLambda->TailPtr(), replaces);
                    if (structType) {
                        mergeBodyArgs.push_back(ctx.NewList(node.Pos(), { memberAtom, replaced }));
                    } else if (tupleType) {
                        mergeBodyArgs.push_back(replaced);
                    } else {
                        mergeBodyArgs.push_back(ctx.NewCallable(node.Pos(), "Collect", {
                            ctx.NewCallable(node.Pos(), "Zip", { mergeArg1, mergeArg2 }) }));
                        mergeBodyArgs.push_back(ctx.NewLambda(node.Pos(),
                            ctx.NewArguments(node.Pos(), { listItemArg }),
                            std::move(replaced)));
                    }
                }
            }

            {
                TExprNode::TPtr listItemArg = ctx.NewArgument(node.Pos(), "listitem");
                auto finishLambda = traits->Child(onWindow ? 4 : 6);
                TNodeOnNodeOwnedMap replaces;
                replaces[finishLambda->Head().Child(0)] = listType ? listItemArg : (structType ?
                    ctx.NewCallable(node.Pos(), "Member", { finishArg1, memberAtom }) :
                    ctx.NewCallable(node.Pos(), "Nth", { finishArg1, nthAtom }));
                auto replaced = ctx.ReplaceNodes(finishLambda->TailPtr(), replaces);
                if (structType) {
                    finishBodyArgs.push_back(ctx.NewList(node.Pos(), { memberAtom, replaced }));
                } else if (tupleType) {
                    finishBodyArgs.push_back(replaced);
                } else {
                    finishBodyArgs.push_back(finishArg1);
                    finishBodyArgs.push_back(ctx.NewLambda(node.Pos(),
                        ctx.NewArguments(node.Pos(), { listItemArg }),
                        std::move(replaced)));
                }
            }

            if (!listType) {
                ui32 defValueIndex = onWindow ? 5 : 7;
                if (!traits->Child(defValueIndex)->IsCallable("Null")) {
                    isNullDefValue = false;
                    isLambdaDefValue = isLambdaDefValue || traits->ChildPtr(defValueIndex)->IsLambda();
                    if (structType) {
                        defValueArgs.push_back(ctx.NewList(node.Pos(), { memberAtom, traits->ChildPtr(defValueIndex) }));
                    } else {
                        defValueArgs.push_back(traits->ChildPtr(defValueIndex));
                    }
                }
            }
        }

        TExprNode::TPtr defValue;
        if (listType || isNullDefValue) {
            defValue = ctx.NewCallable(node.Pos(), "Null", {});
        } else if (!isLambdaDefValue) {
            defValue = structType ?
                ctx.NewCallable(node.Pos(), "AsStruct", std::move(defValueArgs)) :
                ctx.NewList(node.Pos(), std::move(defValueArgs));
        } else {
            if (structType) {
                defValue = ctx.Builder(node.Pos())
                    .Lambda()
                        .Param("type")
                        .Callable("AsStruct")
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                for (ui32 i = 0; i < defValueArgs.size(); ++i) {
                                    const auto& x = defValueArgs[i];
                                    auto list = parent.List(i);
                                    list.Add(0, x->HeadPtr());
                                    auto value = x->ChildPtr(1);
                                    if (value->IsLambda()) {
                                        list.Apply(1, value)
                                            .With(0)
                                            .Callable("StructMemberType")
                                                .Arg(0, "type")
                                                .Add(1, x->HeadPtr())
                                            .Seal()
                                            .Done().Seal();
                                    } else {
                                        list.Add(1, value);
                                    }

                                    list.Seal();
                                }

                                return parent;
                            })
                        .Seal()
                    .Seal()
                    .Build();
            } else {
                defValue = ctx.Builder(node.Pos())
                    .Lambda()
                        .Param("type")
                        .List()
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                for (ui32 i = 0; i < defValueArgs.size(); ++i) {
                                    const auto& value = defValueArgs[i];
                                    if (value->IsLambda()) {
                                        parent.Apply(i, value)
                                            .With(0)
                                            .Callable("TupleElementType")
                                                .Arg(0, "type")
                                                .Atom(1, ToString(i))
                                            .Seal()
                                            .Done().Seal();
                                    } else {
                                        parent.Add(i, value);
                                    }
                                }

                                return parent;
                            })
                        .Seal()
                    .Seal()
                    .Build();
            }
        }

        if (onWindow) {
            return ctx.NewCallable(node.Pos(), "WindowTraits", {
                node.HeadPtr(),
                ctx.NewLambda(node.Pos(),
                    ctx.NewArguments(node.Pos(), { initArg1, initArg2 }),
                    !tupleType ?
                        ctx.NewCallable(node.Pos(), structType ? "AsStruct" : "Map", std::move(initBodyArgs)) :
                        ctx.NewList(node.Pos(), std::move(initBodyArgs))),
                ctx.NewLambda(node.Pos(),
                    ctx.NewArguments(node.Pos(), { updateArg1, updateArg2, updateArg3 }),
                    !tupleType ?
                        ctx.NewCallable(node.Pos(), structType ? "AsStruct" : "Map", std::move(updateBodyArgs)) :
                        ctx.NewList(node.Pos(), std::move(updateBodyArgs))),
                ctx.NewLambda(node.Pos(),
                    ctx.NewArguments(node.Pos(), { shiftArg1, shiftArg2, shiftArg3 }),
                    !tupleType ?
                        ctx.NewCallable(node.Pos(), structType ? "AsStruct" : "Map", std::move(shiftBodyArgs)) :
                        ctx.NewList(node.Pos(), std::move(shiftBodyArgs))),
                ctx.NewLambda(node.Pos(),
                    ctx.NewArguments(node.Pos(), { finishArg1 }),
                    !tupleType ?
                        ctx.NewCallable(node.Pos(), structType ? "AsStruct" : "Map", std::move(finishBodyArgs)) :
                        ctx.NewList(node.Pos(), std::move(finishBodyArgs))),
                defValue });
        } else {
            return ctx.NewCallable(node.Pos(), "AggregationTraits", {
                node.HeadPtr(),
                ctx.NewLambda(node.Pos(),
                    ctx.NewArguments(node.Pos(), { initArg1, initArg2 }),
                    !tupleType ?
                        ctx.NewCallable(node.Pos(), structType ? "AsStruct" : "Map", std::move(initBodyArgs)) :
                        ctx.NewList(node.Pos(), std::move(initBodyArgs))),
                ctx.NewLambda(node.Pos(),
                    ctx.NewArguments(node.Pos(), { updateArg1, updateArg2, updateArg3 }),
                    !tupleType ?
                        ctx.NewCallable(node.Pos(), structType ? "AsStruct" : "Map", std::move(updateBodyArgs)) :
                        ctx.NewList(node.Pos(), std::move(updateBodyArgs))
                    ),
                ctx.NewLambda(node.Pos(),
                    ctx.NewArguments(node.Pos(), { saveArg1 }),
                    !tupleType ?
                        ctx.NewCallable(node.Pos(), structType ? "AsStruct" : "Map", std::move(saveBodyArgs)) :
                        ctx.NewList(node.Pos(), std::move(saveBodyArgs))
                ),
                ctx.NewLambda(node.Pos(),
                    ctx.NewArguments(node.Pos(), { loadArg1 }),
                    !tupleType ?
                        ctx.NewCallable(node.Pos(), structType ? "AsStruct" : "Map", std::move(loadBodyArgs)) :
                        ctx.NewList(node.Pos(), std::move(loadBodyArgs))
                ),
                ctx.NewLambda(node.Pos(),
                    ctx.NewArguments(node.Pos(), { mergeArg1, mergeArg2 }),
                    !tupleType ?
                        ctx.NewCallable(node.Pos(), structType ? "AsStruct" : "Map", std::move(mergeBodyArgs)) :
                        ctx.NewList(node.Pos(), std::move(mergeBodyArgs))
                ),
                ctx.NewLambda(node.Pos(),
                    ctx.NewArguments(node.Pos(), { finishArg1 }),
                    !tupleType ?
                        ctx.NewCallable(node.Pos(), structType ? "AsStruct" : "Map", std::move(finishBodyArgs)) :
                        ctx.NewList(node.Pos(), std::move(finishBodyArgs))
                ),
                defValue });
        }
    }

    IGraphTransformer::TStatus ValidateCalcOverWindowArgs(TVector<const TItemExprType*>& outputStructType,
        const TStructExprType& inputStructType, TExprNode& partitionBy, const TExprNode& sortSpec, TExprNode& winList,
        const TExprNode::TPtr& sessionSpec, const TExprNode::TPtr& sessionColumns, TExprContext& ctx)
    {
        YQL_ENSURE(sessionSpec ? bool(sessionColumns) : !sessionColumns);
        if (!EnsureTuple(partitionBy, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (auto& child : partitionBy.Children()) {
            if (!EnsureAtom(*child, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto item = inputStructType.FindItem(child->Content());
            if (!item) {
                ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Unknown key column: " <<
                    child->Content() << ", type: " << static_cast<const TTypeAnnotationNode&>(inputStructType)));
                return IGraphTransformer::TStatus::Error;
            }

            auto columnType = inputStructType.GetItems()[*item]->GetItemType();
            if (!columnType->IsHashable() || !columnType->IsEquatable()) {
                ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Expected hashable and equatable type for key column: " <<
                    child->Content() << ", but got: " << *columnType));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!sortSpec.IsCallable({"Void", "SortTraits"})) {
            ctx.AddError(TIssue(ctx.GetPosition(sortSpec.Pos()), "Expected sort traits or Void"));
            return IGraphTransformer::TStatus::Error;
        }

        if (sessionSpec && !sessionSpec->IsCallable({"Void", "SessionWindowTraits"})) {
            ctx.AddError(TIssue(ctx.GetPosition(sessionSpec->Pos()), "Expected SessionWindowTraits or Void"));
            return IGraphTransformer::TStatus::Error;
        }

        if (sessionColumns) {
            if (!EnsureTuple(*sessionColumns, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (sessionColumns->ChildrenSize()) {
                if (!sessionSpec->IsCallable("SessionWindowTraits")) {
                    ctx.AddError(TIssue(ctx.GetPosition(sessionSpec->Pos()), "Got non-empty session columns list without session traits"));
                    return IGraphTransformer::TStatus::Error;
                }

                TCoSessionWindowTraits traits(sessionSpec);

                TVector<const TItemExprType*> sessionStructItems;
                sessionStructItems.push_back(ctx.MakeType<TItemExprType>("start", traits.Calculate().Ref().GetTypeAnn()));
                sessionStructItems.push_back(ctx.MakeType<TItemExprType>("state", traits.InitState().Ref().GetTypeAnn()));
                auto sessionStructType = ctx.MakeType<TStructExprType>(sessionStructItems);

                for (auto& column : sessionColumns->ChildrenList()) {
                    if (!EnsureAtom(*column, ctx)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    outputStructType.push_back(ctx.MakeType<TItemExprType>(column->Content(), sessionStructType));
                }
            }
        }

        if (!EnsureTuple(winList, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        for (auto winOn: winList.Children()) {
            if (!TCoWinOnBase::Match(winOn.Get())) {
                ctx.AddError(TIssue(ctx.GetPosition(winOn->Pos()), "Expected WinOnRows/WinOnGroups/WinOnRange"));
                return IGraphTransformer::TStatus::Error;
            }

            bool frameCanBeEmpty = !TWindowFrameSettings::Parse(*winOn, ctx).IsNonEmpty();

            for (auto iterFunc = winOn->Children().begin() + 1; iterFunc != winOn->Children().end(); ++iterFunc) {
                auto func = *iterFunc;
                YQL_ENSURE(func->IsList());
                YQL_ENSURE(func->Child(0)->IsAtom());

                const auto paramName = func->Child(0)->Content();
                const auto calcSpec = func->Child(1);
                YQL_ENSURE(calcSpec->IsCallable({"Lag", "Lead", "RowNumber", "Rank", "DenseRank", "WindowTraits", "PercentRank", "CumeDist", "NTile"}));

                auto traitsInputTypeNode = calcSpec->Child(0);
                YQL_ENSURE(traitsInputTypeNode->GetTypeAnn());
                const TTypeAnnotationNode* traitsInputItemType = traitsInputTypeNode->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                if (!calcSpec->IsCallable("WindowTraits")) {
                    traitsInputItemType = traitsInputItemType->Cast<TListExprType>()->GetItemType();
                }

                const TStructExprType* traitsInputStruct = traitsInputItemType->Cast<TStructExprType>();
                // traitsInputStruct should be subset of inputStruct
                for (auto& item : traitsInputStruct->GetItems()) {
                    auto name = item->GetName();
                    auto type = item->GetItemType();
                    if (auto idx = inputStructType.FindItem(name)) {
                        if (inputStructType.GetItems()[*idx]->GetItemType() == type) {
                            continue;
                        }
                    }
                    ctx.AddError(TIssue(ctx.GetPosition(traitsInputTypeNode->Pos()), TStringBuilder() << "Invalid " <<
                        calcSpec->Content() << " traits input type: " << *traitsInputItemType << ", expecting subset of " <<
                        static_cast<const TTypeAnnotationNode&>(inputStructType)));
                    return IGraphTransformer::TStatus::Error;
                }

                if (calcSpec->IsCallable("WindowTraits")) {
                    auto finishType = calcSpec->Child(4)->GetTypeAnn();
                    if (frameCanBeEmpty) {
                        auto defVal = calcSpec->Child(5);
                        if (!defVal->IsCallable("Null")) {
                            finishType = defVal->GetTypeAnn();
                        } else if (!finishType->IsOptionalOrNull()) {
                            finishType = ctx.MakeType<TOptionalExprType>(finishType);
                        }
                    }
                    outputStructType.push_back(ctx.MakeType<TItemExprType>(paramName, finishType));
                } else {
                    outputStructType.push_back(ctx.MakeType<TItemExprType>(paramName, calcSpec->GetTypeAnn()));
                }
            }
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ValidateTraitsDefaultValue(const TExprNode::TPtr& input, ui32 defaultValueIndex,
        const TTypeAnnotationNode& finishType, TStringBuf finishName, TExprNode::TPtr& output, TContext& ctx)
    {
        auto defaultValue = input->Child(defaultValueIndex);
        if (!defaultValue->IsCallable("Null")) {
            if (defaultValue->IsLambda()) {
                if (!EnsureMaxArgsCount(defaultValue->Head(), 1, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto newDefault = defaultValue->ChildPtr(1);
                if (defaultValue->Head().ChildrenSize() == 1) {
                    newDefault = ctx.Expr.ReplaceNode(std::move(newDefault), defaultValue->Head().Head(),
                        ExpandType(input->Pos(), finishType, ctx.Expr));
                }

                output = ctx.Expr.ChangeChild(*input, defaultValueIndex, std::move(newDefault));
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureComputable(*defaultValue, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            // defaultValue should have such type that expression (<value-of-finishType> ?? defaultValue) is the correct one
            const auto leftType = &finishType;
            const auto rightType = defaultValue->GetTypeAnn();

            auto leftItemType = leftType;
            if (leftType->GetKind() == ETypeAnnotationKind::Optional) {
                leftItemType = leftType->Cast<TOptionalExprType>()->GetItemType();
            }

            auto rightItemType = rightType;
            if (leftType->GetKind() != ETypeAnnotationKind::Optional &&
                rightType->GetKind() == ETypeAnnotationKind::Optional) {
                rightItemType = rightType->Cast<TOptionalExprType>()->GetItemType();
            }

            TExprNode::TPtr arg1 = ctx.Expr.NewArgument(input->Pos(), "arg1");
            TExprNode::TPtr arg2 = defaultValue;
            auto convertedArg2 = arg2;
            const TTypeAnnotationNode* commonItemType = nullptr;
            auto status = SilentInferCommonType(convertedArg2, *rightItemType, arg1, *leftItemType, ctx.Expr, commonItemType,
                TConvertFlags().Set(NConvertFlags::AllowUnsafeConvert));
            if (status == IGraphTransformer::TStatus::Error) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(defaultValue->Pos()),
                    TStringBuilder() << "Uncompatible types of default value and " << finishName
                                     << " : " << *defaultValue->GetTypeAnn() << " vs " << finishType));
                return status;
            }

            if (arg2 != convertedArg2) {
                output = ctx.Expr.ChangeChild(*input, defaultValueIndex, std::move(convertedArg2));
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        return IGraphTransformer::TStatus::Ok;
    }
} // namespace

    IGraphTransformer::TStatus FilterWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 2U, 3U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->ChildrenSize() > 2U) {
            const auto expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
            const auto convertStatus = TryConvertTo(input->TailRef(), *expectedType, ctx.Expr);
            if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()),
                    TStringBuilder() << "Mismatch 'limit' type. Expected Uint64, got: " << *input->Tail().GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambda = input->ChildRef(1);
        const auto status = ConvertToLambda(lambda, ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureSpecificDataType(*lambda, EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    template<bool InverseCondition>
    IGraphTransformer::TStatus InclusiveFilterWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto status = ConvertToLambda(input->ChildRef(1), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            output = InverseCondition ?
                ctx.Expr.Builder(input->Pos())
                    .Callable("Nothing")
                        .Add(0, ExpandType(input->Pos(), *input->Head().GetTypeAnn(), ctx.Expr))
                    .Seal().Build():
                input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        auto& lambda = input->TailRef();
        if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureSpecificDataType(*lambda, EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    template
    IGraphTransformer::TStatus InclusiveFilterWrapper<false>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    template
    IGraphTransformer::TStatus InclusiveFilterWrapper<true>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    IGraphTransformer::TStatus MapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsIdentityLambda(input->Tail())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto status = ConvertToLambda(input->TailRef(), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambda = input->TailRef();
        if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (&lambda->Head().Head() == &lambda->Tail()) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureComputableType(lambda->Pos(), *lambda->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *lambda->GetTypeAnn(), ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus MapNextWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false, false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto status = ConvertToLambda(input->TailRef(), ctx.Expr, 2);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambda = input->TailRef();
        const TTypeAnnotationNode* nextItemType = ctx.Expr.MakeType<TOptionalExprType>(itemType);
        if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType, nextItemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureComputableType(lambda->Pos(), *lambda->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *lambda->GetTypeAnn(), ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus LMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto status = ConvertToLambda(input->TailRef(), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambda = input->TailRef();
        const TTypeAnnotationNode* itemType =
            input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        auto handlerStreamType = ctx.Expr.MakeType<TStreamExprType>(itemType);

        if (!UpdateLambdaAllArgumentsTypes(lambda, { handlerStreamType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureSeqOrOptionalType(lambda->Pos(), *lambda->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto retKind = lambda->GetTypeAnn()->GetKind();
        const TTypeAnnotationNode* retItemType;
        if (retKind == ETypeAnnotationKind::List) {
            retItemType = lambda->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        } else if (retKind == ETypeAnnotationKind::Optional) {
            retItemType = lambda->GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
        } else {
            retItemType = lambda->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(retItemType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ShuffleByKeysWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto itemType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        auto& lambdaKeySelector = input->ChildRef(1);
        auto status = ConvertToLambda(lambdaKeySelector, ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaKeySelector, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaKeySelector->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto keyType = lambdaKeySelector->GetTypeAnn();
        if (!EnsureHashableKey(lambdaKeySelector->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureEquatableKey(lambdaKeySelector->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambdaHandler = input->ChildRef(2);
        status = ConvertToLambda(lambdaHandler, ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto handlerStreamType = ctx.Expr.MakeType<TStreamExprType>(itemType);

        if (!UpdateLambdaAllArgumentsTypes(lambdaHandler, { handlerStreamType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureSeqOrOptionalType(lambdaHandler->Pos(), *lambdaHandler->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto retKind = lambdaHandler->GetTypeAnn()->GetKind();
        const TTypeAnnotationNode* retItemType;
        if (retKind == ETypeAnnotationKind::List) {
            retItemType = lambdaHandler->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        } else if (retKind == ETypeAnnotationKind::Optional) {
            retItemType = lambdaHandler->GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
        } else {
            retItemType = lambdaHandler->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(retItemType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FoldMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        auto& initState = *input->ChildRef(1);
        auto& lambda = input->ChildRef(2);

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(initState, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(lambda, ctx.Expr, 2);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType, initState.GetTypeAnn()}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto lambdaType = lambda->GetTypeAnn();
        if (!lambdaType) {
            return IGraphTransformer::TStatus::Repeat;
        }
        if (!EnsureTupleTypeSize(*lambda, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureComputableType(lambda->Pos(), *lambdaType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambda->Child(1)->IsList()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder() <<
                "Expected literal tuple as result for update lambda for FoldMap"));
            return IGraphTransformer::TStatus::Error;
        }
        auto tupleExprType = lambdaType->Cast<TTupleExprType>();
        auto updatedListType = tupleExprType->GetItems()[0];
        auto stateType = tupleExprType->GetItems()[1];
        if (!IsSameAnnotation(*initState.GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder() <<
                "Mismatch of update lambda state and init state type: " <<
                *stateType << " != " << *initState.GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *updatedListType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus Fold1MapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        auto& initLambda = input->ChildRef(1);
        auto& updateLambda = input->ChildRef(2);

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(initLambda, ctx.Expr, 1);
        status = status.Combine(ConvertToLambda(updateLambda, ctx.Expr, 2));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(initLambda, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto lambdaType = initLambda->GetTypeAnn();
        if (!lambdaType) {
            return IGraphTransformer::TStatus::Repeat;
        }
        if (!EnsureTupleTypeSize(*initLambda, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!initLambda->Child(1)->IsList()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(initLambda->Pos()), TStringBuilder() <<
                "Expected literal tuple as result for init lambda for Fold1Map"));
            return IGraphTransformer::TStatus::Error;
        }
        auto tupleExprType = lambdaType->Cast<TTupleExprType>();
        auto updatedListType = tupleExprType->GetItems()[0];
        auto stateType = tupleExprType->GetItems()[1];

        if (!UpdateLambdaAllArgumentsTypes(updateLambda, {itemType, stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto updateLambdaType = updateLambda->GetTypeAnn();
        if (!updateLambdaType) {
            return IGraphTransformer::TStatus::Repeat;
        }
        if (!IsSameAnnotation(*lambdaType, *updateLambdaType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(updateLambda->Pos()), TStringBuilder() <<
                "Mismatch of update and init lambda types: " <<
                *updateLambdaType << " != " << *lambdaType));
            return IGraphTransformer::TStatus::Error;
        }
        if (!updateLambda->Child(1)->IsList()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(updateLambda->Pos()), TStringBuilder() <<
                "Expected literal tuple as result for update lambda for Fold1Map"));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *updatedListType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus Chain1MapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsIdentityLambda(input->Tail()) && IsIdentityLambda(*input->Child(1))) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& initLambda = input->ChildRef(1);
        auto& updateLambda = input->ChildRef(2);

        auto status = ConvertToLambda(initLambda, ctx.Expr, 1);
        status = status.Combine(ConvertToLambda(updateLambda, ctx.Expr, 2));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(initLambda, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto lambdaType = initLambda->GetTypeAnn();
        if (!lambdaType) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!UpdateLambdaAllArgumentsTypes(updateLambda, {itemType, lambdaType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto updateLambdaType = updateLambda->GetTypeAnn();
        if (!updateLambdaType) {
            return IGraphTransformer::TStatus::Repeat;
        }
        if (!IsSameAnnotation(*lambdaType, *updateLambdaType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(updateLambda->Pos()), TStringBuilder() <<
                "Mismatch of update and init lambda types: " <<
                *updateLambdaType << " != " << *lambdaType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *updateLambdaType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <bool Warn>
    IGraphTransformer::TStatus FlatMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto status = ConvertToLambda(input->TailRef(), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambda = input->TailRef();

        if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        const auto retKind = lambda->GetTypeAnn()->GetKind();
        // input    F L S O
        // lambda L F L S L
        // lambda S F L S S
        // lambda O F L S O
        // lambda F F F - F

        bool warn = false;
        auto resultKind = input->Head().GetTypeAnn()->GetKind();
        const TTypeAnnotationNode* lambdaItemType = nullptr;
        switch (retKind) {
            case ETypeAnnotationKind::List:
                lambdaItemType = lambda->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                break;
            case ETypeAnnotationKind::Stream:
                lambdaItemType = lambda->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
                break;
            case ETypeAnnotationKind::Optional:
                if (input->Content().EndsWith("Warn")) {
                    warn = true;
                }

                lambdaItemType = lambda->GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
                break;
            case ETypeAnnotationKind::Pg:
                if (input->Content().EndsWith("Warn")) {
                    warn = true;
                }

                lambdaItemType = lambda->GetTypeAnn();
                break;
            case ETypeAnnotationKind::Null: {
                if (input->Content().EndsWith("Warn")) {
                    warn = true;
                }

                if (resultKind == ETypeAnnotationKind::List) {
                    output = ctx.Expr.NewCallable(input->Pos(), "EmptyList", {});
                } else if (resultKind == ETypeAnnotationKind::Optional) {
                    output = ctx.Expr.NewCallable(input->Pos(), "Null", {});
                } else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder()
                        << "Only list or optional are supported as input if FlatMap lambda return type is null, but input type is: " << *input->Head().GetTypeAnn()));
                    return IGraphTransformer::TStatus::Error;
                }

                return IGraphTransformer::TStatus::Repeat;
            }
            case ETypeAnnotationKind::Flow:
                if (ETypeAnnotationKind::Stream != input->Head().GetTypeAnn()->GetKind()) {
                    lambdaItemType = lambda->GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
                    resultKind = ETypeAnnotationKind::Flow;
                    break;
                }
                [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
            default:
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder()
                    << "Expected list, stream or optional as FlatMap lambda return type, but got: " << *lambda->GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
        }

        if (ETypeAnnotationKind::Optional == resultKind) {
            resultKind = retKind;
        }

        if (warn) {
            auto issue = TIssue(
                ctx.Expr.GetPosition(input->Pos()),
                "ListFlatMap with optional result is deprecated, please use ListNotNull and ListMap instead"
            );
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YQL_DEPRECATED_LIST_FLATMAP_OPTIONAL, issue);
            if (!ctx.Expr.AddWarning(issue)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (input->Content().EndsWith("Warn")) {
            output = ctx.Expr.RenameNode(*input, input->Content().SubString(0, input->Content().length() - 4));
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(MakeSequenceType(resultKind, *lambdaItemType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template
    IGraphTransformer::TStatus FlatMapWrapper<true>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    template
    IGraphTransformer::TStatus FlatMapWrapper<false>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);


    template<bool Ordered>
    IGraphTransformer::TStatus MultiMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false, true, false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto status = ConvertToLambda(input->TailRef(), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (input->Tail().ChildrenSize() < 3U) {
            output = ctx.Expr.RenameNode(*input, Ordered ? "OrderedMap" : "Map");
            return IGraphTransformer::TStatus::Repeat;
        }

        auto& lambda = input->TailRef();
        if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        const auto lambdaItemType = lambda->Tail().GetTypeAnn();
        for (ui32 i = 1U; i < lambda->ChildrenSize() - 1U; ++i) {
            if (!IsSameAnnotation(*lambdaItemType, *lambda->Child(i)->GetTypeAnn())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder()
                    << input->Content() << " lambda returns types is not same."));
                return IGraphTransformer::TStatus::Error;
            }
        }

        const auto resultKind = input->Head().GetTypeAnn()->GetKind();
        input->SetTypeAnn(MakeSequenceType(resultKind, *lambdaItemType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template IGraphTransformer::TStatus MultiMapWrapper<true>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    template IGraphTransformer::TStatus MultiMapWrapper<false>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    TExprNodeBuilder& AddChildren(TExprNodeBuilder& builder, ui32 index, const TExprNode::TPtr& input) {
        const auto i = index;
        return i >= input->ChildrenSize() ? builder : AddChildren(builder.Add(i, input->ChildPtr(i)), ++index, input);
    }

    template<ui32 MinArgsCount = 2U, ui32 MaxArgsCount = MinArgsCount, bool UseFlatMap = false>
    IGraphTransformer::TStatus OptListWrapperImpl(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx,
        TStringBuf name) {
        if (MinArgsCount == MaxArgsCount) {
            if (!EnsureArgsCount(*input, MinArgsCount, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!(EnsureMinArgsCount(*input, MinArgsCount, ctx.Expr) && EnsureMaxArgsCount(*input, MaxArgsCount, ctx.Expr))) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional = false;
        auto type = input->Head().GetTypeAnn();
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
            isOptional = true;
        }

        if (type->GetKind() != ETypeAnnotationKind::List && type->GetKind() != ETypeAnnotationKind::EmptyList) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected (empty) list or optional of (empty) list, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (isOptional) {
            output = AddChildren(ctx.Expr.Builder(input->Pos())
                .Callable(UseFlatMap ? "FlatMap" : "Map")
                    .Add(0, input->HeadPtr())
                    .Lambda(1)
                        .Param("x")
                        .Callable(name)
                            .Arg(0, "x"), 1U, input)
                        .Seal()
                    .Seal()
                .Seal().Build();
        } else {
            output = ctx.Expr.RenameNode(*input, name);
        }

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus ListFilterWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl(input, output, ctx, "OrderedFilter");
    }

    IGraphTransformer::TStatus ListMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl(input, output, ctx, "OrderedMap");
    }

    IGraphTransformer::TStatus ListFlatMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl(input, output, ctx, "OrderedFlatMapWarn");
    }

    IGraphTransformer::TStatus ListSkipWhileInclusiveWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl(input, output, ctx, "SkipWhileInclusive");
    }

    IGraphTransformer::TStatus ListTakeWhileInclusiveWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl(input, output, ctx, "TakeWhileInclusive");
    }

    IGraphTransformer::TStatus ListSkipWhileWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl(input, output, ctx, "SkipWhile");
    }

    IGraphTransformer::TStatus ListTakeWhileWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl(input, output, ctx, "TakeWhile");
    }

    IGraphTransformer::TStatus ListSkipWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl(input, output, ctx, "Skip");
    }

    IGraphTransformer::TStatus ListHeadWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<1U, 1U, true>(input, output, ctx, "Head");
    }

    IGraphTransformer::TStatus ListLastWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<1U, 1U, true>(input, output, ctx, "Last");
    }

    IGraphTransformer::TStatus ListTakeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl(input, output, ctx, "Take");
    }

    IGraphTransformer::TStatus ListEnumerateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<1U, 3U>(input, output, ctx, "Enumerate");
    }

    IGraphTransformer::TStatus ListReverseWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<1U>(input, output, ctx, "Reverse");
    }

    IGraphTransformer::TStatus ListSortWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<3U>(input, output, ctx, "Sort");
    }

    IGraphTransformer::TStatus ListTopSortWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 2, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TStringBuf newName = input->Content();
        newName.Skip(4);
        bool desc = false;
        if (newName.EndsWith("Asc")) {
            newName.Chop(3);
        } else if (newName.EndsWith("Desc")) {
            newName.Chop(4);
            desc = true;
        }

        TExprNode::TPtr sortLambda = nullptr;
        if (input->ChildrenSize() == 3) {
            sortLambda = input->ChildPtr(2);
        } else {
            sortLambda = ctx.Expr.Builder(input->Pos())
                .Lambda()
                    .Param("item")
                    .Arg("item")
                .Seal()
            .Build();
        }

        return OptListWrapperImpl<4U, 4U>(ctx.Expr.Builder(input->Pos())
            .Callable(newName)
                .Add(0, input->ChildPtr(0))
                .Add(1, input->ChildPtr(1))
                .Callable(2, "Bool")
                    .Atom(0, desc ? "false" : "true")
                .Seal()
                .Add(3, sortLambda)
            .Seal()
        .Build(), output, ctx, newName);
    }

    IGraphTransformer::TStatus ListExtractWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<2U>(input, output, ctx, "OrderedExtract");
    }

    IGraphTransformer::TStatus ListCollectWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<1U>(input, output, ctx, "Collect");
    }

    IGraphTransformer::TStatus OptListFold1WrapperImpl(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx, TExprNode::TPtr&& updateLambda) {
        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->Head().GetTypeAnn() && IsEmptyList(*RemoveOptionalType(input->Head().GetTypeAnn()))) {
            output = ctx.Expr.NewCallable(input->Pos(), "Null", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListOrOptionalListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto type = input->Head().GetTypeAnn();
        TExprNode::TPtr fold1Input = input->HeadPtr();
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
            fold1Input = ctx.Expr.Builder(input->Head().Pos())
                .Callable("Coalesce")
                    .Add(0, input->HeadPtr())
                    .Callable(1, "List")
                        .Add(0, ExpandType(input->Head().Pos(), *type, ctx.Expr))
                    .Seal()
                .Seal()
                .Build();
        }

        output = ctx.Expr.Builder(input->Pos())
            .Callable("Fold1")
                .Add(0, fold1Input)
                .Lambda(1)
                    .Param("item")
                    .Arg("item")
                .Seal()
                .Add(2, std::move(updateLambda))
            .Seal()
            .Build();

        const auto itemType = type->Cast<TListExprType>()->GetItemType();
        if (itemType->GetKind() == ETypeAnnotationKind::Optional) {
            // remove extra optional level created by Fold1
            output = ctx.Expr.Builder(input->Pos())
                .Callable("FlatMap")
                    .Add(0, output)
                    .Lambda(1)
                        .Param("item")
                        .Arg("item")
                    .Seal()
                .Seal()
                .Build();
        }

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus OptListFold1WrapperImpl(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx, TStringBuf name) {
        if (!EnsureArgsCount(*input, 1U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto lambda = ctx.Expr.Builder(input->Pos())
            .Lambda()
                .Param("item")
                .Param("state")
                .Callable(name)
                    .Arg(0, "state")
                    .Arg(1, "item")
                .Seal()
            .Seal().Build();

        return OptListFold1WrapperImpl(input, output, ctx, std::move(lambda));
    }

    IGraphTransformer::TStatus ListFoldWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<3U>(input, output, ctx, "Fold");
    }

    IGraphTransformer::TStatus ListFold1Wrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<3U, 3U, true>(input, output, ctx, "Fold1");
    }

    IGraphTransformer::TStatus ListFoldMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<3U>(input, output, ctx, "FoldMap");
    }

    IGraphTransformer::TStatus ListFold1MapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<3U>(input, output, ctx, "Fold1Map");
    }

    IGraphTransformer::TStatus ListMinWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListFold1WrapperImpl(input, output, ctx, "AggrMin");
    }

    IGraphTransformer::TStatus ListMaxWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListFold1WrapperImpl(input, output, ctx, "AggrMax");
    }

    IGraphTransformer::TStatus ListSumWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListFold1WrapperImpl(input, output, ctx, "AggrAdd");
    }

    IGraphTransformer::TStatus ListConcatWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMaxArgsCount(*input, 2U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (2U > input->ChildrenSize()) {
            return OptListFold1WrapperImpl(input, output, ctx, "AggrConcat");
        }

        if (IsNull(input->Tail())) {
            output = ctx.Expr.ChangeChildren(*input, {input->HeadPtr()});
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isOptional;
        if (const TDataExprType* data; !EnsureDataOrOptionalOfData(input->Tail(), isOptional, data, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isOptional) {
            output = ctx.Expr.Builder(input->Pos())
                .Callable("IfPresent")
                    .Add(0, input->TailPtr())
                    .Lambda(1)
                        .Param("separator")
                        .Callable(input->Content())
                            .Add(0, input->HeadPtr())
                            .Arg(1, "separator")
                        .Seal()
                    .Seal()
                    .Callable(2, input->Content())
                        .Add(0, input->HeadPtr())
                    .Seal()
                .Seal().Build();
            return IGraphTransformer::TStatus::Repeat;
        }

        auto lambda = ctx.Expr.Builder(input->Pos())
            .Lambda()
                .Param("item")
                .Param("state")
                .Callable("AggrConcat")
                    .Callable(0, "Concat")
                        .Arg(0, "state")
                        .Add(1, input->TailPtr())
                    .Seal()
                    .Arg(1, "item")
                .Seal()
            .Seal().Build();
        return OptListFold1WrapperImpl(input, output, ctx, std::move(lambda));
    }

    IGraphTransformer::TStatus ListAvgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->Head().GetTypeAnn() && IsEmptyList(*RemoveOptionalType(input->Head().GetTypeAnn()))) {
            output = ctx.Expr.NewCallable(input->Pos(), "Null", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListOrOptionalListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            output = ctx.Expr.Builder(input->Pos())
                .Callable("FlatMap")
                    .Add(0, input->HeadPtr())
                    .Lambda(1)
                        .Param("list")
                        .Callable(input->Content())
                            .Arg(0, "list")
                        .Seal()
                    .Seal()
                .Seal().Build();
            return IGraphTransformer::TStatus::Repeat;
        }

        const auto itemType = RemoveOptionalType(input->Head().GetTypeAnn())->Cast<TListExprType>()->GetItemType();

        bool isOptionalItem;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(input->Head().Pos(), itemType, isOptionalItem, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto targetSlot = EDataSlot::Float == dataType->GetSlot() || GetDataTypeInfo(dataType->GetSlot()).FixedSize < 4U ?
            EDataSlot::Float : EDataSlot::Double;

        if (dataType->GetSlot() != targetSlot || isOptionalItem) {
            auto cast = ctx.Expr.Builder(input->Head().Pos())
                .Callable("SafeCast")
                    .Add(0, input->HeadPtr())
                    .Callable(1, "ListType")
                        .Callable(0, "DataType")
                            .Atom(0, GetDataTypeInfo(targetSlot).Name, TNodeFlags::Default)
                        .Seal()
                    .Seal()
                .Seal().Build();
            output = ctx.Expr.ChangeChild(*input, 0U, std::move(cast));
        } else {
            const auto list = ctx.Expr.Builder(input->Pos())
                .Callable("ListCollect")
                    .Add(0, input->HeadPtr())
                .Seal().Build();

            output = ctx.Expr.Builder(input->Pos())
                .Callable("Div")
                    .Callable(0, "ListSum")
                        .Add(0, list)
                    .Seal()
                    .Callable(1, "Length")
                        .Add(0, list)
                    .Seal()
                .Seal().Build();
        }

        return IGraphTransformer::TStatus::Repeat;
    }

    template <bool AllOrAny>
    IGraphTransformer::TStatus ListAllAnyWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto stub = MakeBool<AllOrAny>(input->Pos(), ctx.Expr);
        if (IsNull(input->Head())) {
            output = std::move(stub);
            return IGraphTransformer::TStatus::Repeat;
        }

        auto listType = input->Head().GetTypeAnn();
        if (HasError(listType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!listType) {
            YQL_ENSURE(input->Head().IsLambda());
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected (optional) list type, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        if (listType->GetKind() == ETypeAnnotationKind::Optional) {
            listType = listType->Cast<TOptionalExprType>()->GetItemType();
        }

        if (IsEmptyList(*listType)) {
            output = stub;
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(input->Head().Pos(), *listType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto itemType = listType->Cast<TListExprType>()->GetItemType();

        bool isOptionalItem;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(input->Head().Pos(), itemType, isOptionalItem, dataType, ctx.Expr) || !EnsureSpecificDataType(input->Head().Pos(), *dataType, EDataSlot::Bool, ctx.Expr)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "List of " << *itemType << " instead of boolean."));
            return IGraphTransformer::TStatus::Error;
        }

        const auto coalesce = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            return isOptionalItem ?
                parent
                    .Callable("Coalesce")
                        .Arg(0, "item")
                        .Callable(1, "Bool")
                            .Atom(0, "false", TNodeFlags::Default)
                        .Seal()
                    .Seal():
                parent.Arg("item");
        };

        const auto filter = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            return AllOrAny ? coalesce(parent):
                isOptionalItem ?
                    parent
                        .Callable("Not")
                            .Callable(0, "Coalesce")
                                .Arg(0, "item")
                                .Callable(1, "Bool")
                                    .Atom(0, "false", TNodeFlags::Default)
                                .Seal()
                            .Seal()
                        .Seal():
                    parent.Callable("Not").Arg(0, "item").Seal();
        };

        output = ctx.Expr.Builder(input->Pos())
            .Callable("IfPresent")
                .Callable(0, "ListLast")
                    .Callable(0, "ListTakeWhileInclusive")
                        .Add(0, input->HeadPtr())
                        .Lambda(1)
                            .Param("item")
                            .Do(filter)
                        .Seal()
                    .Seal()
                .Seal()
                .Lambda(1)
                    .Param("item")
                    .Do(coalesce)
                .Seal()
                .Add(2, std::move(stub))
            .Seal().Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    template
    IGraphTransformer::TStatus ListAllAnyWrapper<true>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    template
    IGraphTransformer::TStatus ListAllAnyWrapper<false>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    IGraphTransformer::TStatus PrependWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListOrEmptyType(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Tail().GetTypeAnn()->GetKind() == ETypeAnnotationKind::EmptyList) {
            output = ctx.Expr.NewCallable(input->Pos(), "AsList", { input->HeadPtr() });
            return IGraphTransformer::TStatus::Repeat;
        }

        auto expectedType = input->Tail().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        auto convertStatus = TryConvertTo(input->HeadRef(), *expectedType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Mismatch type of item being prepended and list"));
            return IGraphTransformer::TStatus::Error;
        } else if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        input->SetTypeAnn(input->Tail().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AppendWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListOrEmptyType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::EmptyList) {
            output = ctx.Expr.NewCallable(input->Pos(), "AsList", { input->TailPtr() });
            return IGraphTransformer::TStatus::Repeat;
        }

        auto expectedType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        auto convertStatus = TryConvertTo(input->TailRef(), *expectedType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()), "Mismatch type of item being appended and list"));
            return IGraphTransformer::TStatus::Error;
        } else if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus LengthWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = MakeNothingData(ctx.Expr, input->Head().Pos(), "Uint64");
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!input->Head().GetTypeAnn()) {
            YQL_ENSURE(input->Head().Type() == TExprNode::Lambda);
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected (optional) list or dict type, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        auto originalType = input->Head().GetTypeAnn();
        auto type = originalType;
        bool isOptional = false;
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            isOptional = true;
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }

        if (type->GetKind() == ETypeAnnotationKind::EmptyList || type->GetKind() == ETypeAnnotationKind::EmptyDict) {
            output = ctx.Expr.Builder(input->Head().Pos())
                .Callable("Uint64")
                    .Atom(0, "0")
                .Seal()
                .Build();

            if (isOptional) {
                output = MakeConstMap(input->Pos(), input->HeadPtr(), output, ctx.Expr);
            }

            return IGraphTransformer::TStatus::Repeat;
        }

        if (type->GetKind() != ETypeAnnotationKind::List && type->GetKind() != ETypeAnnotationKind::Dict) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected (optional) list or dict type, but got: " << *originalType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64));
        if (isOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus IteratorWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Head().GetTypeAnn()) {
            YQL_ENSURE(input->Type() == TExprNode::Lambda);
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected (optional) list, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
            if (!EnsureDependsOn(*input->Child(i), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        auto originalType = input->Head().GetTypeAnn();
        auto type = originalType;
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }

        if (type->GetKind() != ETypeAnnotationKind::List) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected (optional) list, but got: " << *originalType));
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = type->Cast<TListExprType>()->GetItemType();
        input->SetTypeAnn(ctx.Expr.MakeType<TStreamExprType>(itemType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus EmptyIteratorWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
            if (!EnsureDependsOn(*input->Child(i), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureNewSeqType<false, false>(input->Head().Pos(), *type, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(type);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ForwardListWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false, false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(itemType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ToStreamWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List) {
            output = ctx.Expr.RenameNode(*input, "Iterator");
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow) {
            output = ctx.Expr.RenameNode(*input, "FromFlow");
            return IGraphTransformer::TStatus::Repeat;
        }

        for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
            if (!EnsureDependsOn(*input->Child(i), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TStreamExprType>(itemType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ToSequenceWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        switch (input->Head().GetTypeAnn()->GetKind()) {
            case ETypeAnnotationKind::Stream:
            case ETypeAnnotationKind::List:
            case ETypeAnnotationKind::Optional:
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            default:
                output = ctx.Expr.RenameNode(*input, "Just");
                return IGraphTransformer::TStatus::Repeat;
        }
    }

    IGraphTransformer::TStatus LazyListWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListOrOptionalListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CollectWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow) {
            itemType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
        } else if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List) {
            itemType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        } else if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream) {
            itemType = input->Head().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected list or stream type, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(itemType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ListFromRangeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 2U, 3U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TVector<ui32> nonNullNodes;
        for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
            if (!input->Child(i)->GetTypeAnn()) {
                YQL_ENSURE(input->Child(i)->IsLambda());
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i)->Pos()), TStringBuilder() << "Lambda is not expected as argument of " << input->Content()));
                return IGraphTransformer::TStatus::Error;
            }
            if (input->Child(i)->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Null) {
                nonNullNodes.push_back(i);
            }
        }
        switch (nonNullNodes.size()) {
        case 0U:
            output = ctx.Expr.Builder(input->Pos()).Callable("Null").Seal().Build();
            return IGraphTransformer::TStatus::Repeat;
        case 1U: {
            bool _itemIsOpt = false;
            const TDataExprType* itemType = nullptr;
            if (!EnsureDataOrOptionalOfData(*input->Child(nonNullNodes[0]),
                                            _itemIsOpt, itemType, ctx.Expr))
            {
                return IGraphTransformer::TStatus::Error;
            }
            output = ctx.Expr.Builder(input->Pos())
                .Callable("Nothing")
                    .Callable(0U, "OptionalType")
                        .Callable(0U, "ListType")
                            .Add(0U, ExpandType(input->Pos(), *itemType, ctx.Expr))
                        .Seal()
                    .Seal()
                .Seal().Build();
            return IGraphTransformer::TStatus::Repeat;
        }
        case 2U: {
            if (input->ChildrenSize() == 2U) {
                break;
            }
            bool _itemIsOpt = false;
            const TDataExprType* _itemType = nullptr;
            const auto idx1 = nonNullNodes[0], idx2 = nonNullNodes[1];
            if (!EnsureDataOrOptionalOfData(*input->Child(idx1), _itemIsOpt, _itemType, ctx.Expr))
            {
                return IGraphTransformer::TStatus::Error;
            }
            if (!EnsureDataOrOptionalOfData(*input->Child(idx2), _itemIsOpt, _itemType, ctx.Expr))
            {
                return IGraphTransformer::TStatus::Error;
            }
            auto commonType = CommonType<false>(input->Pos(), input->Child(idx1)->GetTypeAnn(), input->Child(idx2)->GetTypeAnn(), ctx.Expr);
            if (!commonType)
                return IGraphTransformer::TStatus::Error;
            if (ETypeAnnotationKind::Optional == commonType->GetKind()) {
                commonType = commonType->Cast<TOptionalExprType>()->GetItemType();
            }
            output = ctx.Expr.Builder(input->Pos())
                .Callable("Nothing")
                    .Callable(0U, "OptionalType")
                        .Callable(0U, "ListType")
                            .Add(0U, ExpandType(input->Pos(), *commonType, ctx.Expr))
                        .Seal()
                    .Seal()
                .Seal().Build();
            return IGraphTransformer::TStatus::Repeat;
        }
        default:
            break;
        }

        const auto stepType = input->ChildrenSize() == 2U ? nullptr : input->Tail().GetTypeAnn();
        bool beginIsOpt = false, endIsOpt = false, stepIsOpt = false;
        const TDataExprType* _itemType = nullptr;
        const TDataExprType* stepItemType = nullptr;

        if (!EnsureDataOrOptionalOfData(*input->Child(0U), beginIsOpt, _itemType, ctx.Expr)
            || !EnsureDataOrOptionalOfData(*input->Child(1U), endIsOpt, _itemType, ctx.Expr))
        {
            return IGraphTransformer::TStatus::Error;
        }
        if (stepType && !EnsureDataOrOptionalOfData(*input->Child(2U), stepIsOpt,
                                                    stepItemType, ctx.Expr))
        {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* commonType = nullptr;
        if (stepType && IsDataTypeFloat(stepItemType->GetSlot())) {
            commonType = ((beginIsOpt || endIsOpt) && !stepIsOpt)
                ? ctx.Expr.MakeType<TOptionalExprType>(stepType) : stepType;
            if (const auto status = TrySilentConvertTo(input->ChildRef(0U), *commonType, ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                if (status == IGraphTransformer::TStatus::Error) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(0U)->Pos()), TStringBuilder() << "Impossible silent convert bound of type " <<
                        *input->Child(0U)->GetTypeAnn() << " into " << *commonType));
                }
                return status;
            }
            if (const auto status = TrySilentConvertTo(input->ChildRef(1U), *commonType, ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                if (status == IGraphTransformer::TStatus::Error) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1U)->Pos()), TStringBuilder() << "Impossible silent convert bound of type " <<
                        *input->Child(1U)->GetTypeAnn() << " into " << *commonType));
                }
                return status;
            }
        } else {
            commonType = CommonType<false>(input->Pos(), input->Child(0U)->GetTypeAnn(), input->Child(1U)->GetTypeAnn(), ctx.Expr);
            if (!commonType)
                return IGraphTransformer::TStatus::Error;

            if (const auto status = TryConvertTo(input->ChildRef(0U), *commonType, ctx.Expr).Combine(TryConvertTo(input->ChildRef(1U), *commonType, ctx.Expr)); status != IGraphTransformer::TStatus::Ok)
                return status;

            if (stepIsOpt && ETypeAnnotationKind::Optional != commonType->GetKind()) {
                commonType = ctx.Expr.MakeType<TOptionalExprType>(commonType);
            }
        }

        const bool commonIsOpt = ETypeAnnotationKind::Optional == commonType->GetKind();
        const auto commonItemType = commonIsOpt
            ? commonType->Cast<TOptionalExprType>()->GetItemType() : commonType;
        const auto slot = commonItemType->Cast<TDataExprType>()->GetSlot();
        if (!(IsDataTypeDateOrTzDateOrInterval(slot) || IsDataTypeNumeric(slot))) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected type of bounds is numeric or datetime, but got " << *commonType));
            return IGraphTransformer::TStatus::Error;
        }

        const auto stepSlot = IsDataTypeDateOrTzDateOrInterval(slot)
            ? (IsDataTypeBigDate(slot) ? EDataSlot::Interval64 : EDataSlot::Interval)
            : MakeSigned(slot);
        if (stepItemType) {
            if (const auto requredStepType = slot == stepSlot ? commonItemType : ctx.Expr.MakeType<TDataExprType>(stepSlot); !IsSameAnnotation(*stepItemType, *requredStepType)) {
                if (const auto status = TrySilentConvertTo(input->ChildRef(2U), *requredStepType, ctx.Expr); status == IGraphTransformer::TStatus::Repeat)
                    return status;
                else if (status == IGraphTransformer::TStatus::Error && !EnsureSpecificDataType(input->Tail().Pos(), *stepItemType, stepSlot, ctx.Expr))
                    return status;
            }
        } else {
            TExprNode::TPtr value;
            switch (slot) {
                case EDataSlot::Date:
                case EDataSlot::TzDate:
                case EDataSlot::Date32:
                    value = ctx.Expr.NewAtom(input->Pos(), "86400000000", TNodeFlags::Default);
                    break;
                case EDataSlot::Datetime:
                case EDataSlot::TzDatetime:
                case EDataSlot::Datetime64:
                    value = ctx.Expr.NewAtom(input->Pos(), "1000000", TNodeFlags::Default);
                    break;
                default:
                    value = ctx.Expr.NewAtom(input->Pos(), "1", TNodeFlags::Default);
                    break;
            }

            auto newChildren = input->ChildrenList();
            newChildren.emplace_back(ctx.Expr.NewCallable(input->Pos(), NKikimr::NUdf::GetDataTypeInfo(stepSlot).Name, {std::move(value)}));
            output = ctx.Expr.ChangeChildren(*input, std::move(newChildren));
            return IGraphTransformer::TStatus::Repeat;
        }

        if (commonIsOpt) {
            const auto defVal = ctx.Expr.Builder(input->Pos())
                .Callable("Nothing")
                    .Callable(0U, "OptionalType")
                        .Callable(0U, "ListType")
                            .Add(0U, ExpandType(input->Pos(), *commonItemType, ctx.Expr))
                        .Seal()
                    .Seal()
                .Seal().Build();
            const auto lambda = ctx.Expr.Builder(input->Pos())
                .Lambda()
                    .Do([&] (TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        if (beginIsOpt) {
                            parent.Param("begin");
                        }
                        if (endIsOpt) {
                            parent.Param("end");
                        }
                        if (stepIsOpt) {
                            parent.Param("step");
                        }
                        return parent;
                    })
                    .Callable("Just")
                        .Callable(0U, input->Content())
                            .Do([&] (TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                if (beginIsOpt) {
                                    parent.Arg(0U, "begin");
                                } else {
                                    parent.Add(0U, input->ChildPtr(0U));
                                }
                                if (endIsOpt) {
                                    parent.Arg(1U, "end");
                                } else {
                                    parent.Add(1U, input->ChildPtr(1U));
                                }
                                if (stepIsOpt) {
                                    return parent.Arg(2U, "step");
                                } else {
                                    return parent.Add(2U, input->ChildPtr(2U));
                                }
                            })
                        .Seal()
                    .Seal()
                .Seal().Build();
            output = ctx.Expr.Builder(input->Pos())
                .Callable("IfPresent")
                    .Do([&] (TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 cnt = 0;
                        if (beginIsOpt) {
                            parent.Add(cnt++, input->ChildPtr(0U));
                        }
                        if (endIsOpt) {
                            parent.Add(cnt++, input->ChildPtr(1U));
                        }
                        if (stepIsOpt) {
                            parent.Add(cnt++, input->ChildPtr(2U));
                        }
                        parent.Add(cnt++, lambda);
                        return parent.Add(cnt++, defVal);
                    })
                .Seal().Build();
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(commonType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ReplicateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
        auto convertStatus = TryConvertTo(input->TailRef(), *expectedType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), "Mismatch argument types"));
            return IGraphTransformer::TStatus::Error;
        }

        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(input->Head().GetTypeAnn()));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SwitchWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* inputItemType = nullptr;
        if (!EnsureNewSeqType<false, false>(input->Head(), ctx.Expr, &inputItemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsurePersistableType(input->Head().Pos(), *inputItemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        ui64 memoryLimitBytes = 0;
        if (!TryFromString(input->Child(1)->Content(), memoryLimitBytes)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to convert to integer: " << input->Child(1)->Content()));
            return IGraphTransformer::TStatus::Error;
        }

        const TTupleExprType* inputStreams = nullptr;
        ui32 inputStreamsCount = 1;
        if (inputItemType->GetKind() == ETypeAnnotationKind::Variant) {
            auto underlyingType = inputItemType->Cast<TVariantExprType>()->GetUnderlyingType();
            if (!EnsureTupleType(input->Head().Pos(), *underlyingType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            inputStreams = underlyingType->Cast<TTupleExprType>();
            if (inputStreams->GetSize() < 2) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Expected at least 2 input streams"));
                return IGraphTransformer::TStatus::Error;
            }

            inputStreamsCount = inputStreams->GetSize();
        }

        TVector<const TTypeAnnotationNode*> outputStreamItems;
        for (ui32 i = 2; i < input->ChildrenSize(); i += 2) {
            if (!EnsureTuple(*input->Child(i), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            TVector<const TTypeAnnotationNode*> lambdaInputItemTypes;
            std::unordered_set<ui32> indxs;
            for (const auto& child : input->Child(i)->Children()) {
                if (!EnsureAtom(*child, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                ui32 index = 0;
                if (!TryFromString(child->Content(), index)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder()
                        << "Failed to convert to integer: " << child->Content()));
                    return IGraphTransformer::TStatus::Error;
                }

                if (!indxs.emplace(index).second) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder()
                        << "Duplicate stream index: " << index));
                    return IGraphTransformer::TStatus::Error;
                }

                if (index >= inputStreamsCount) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder()
                        << "Wrong stream index: "
                        << index << ", count: " << inputStreamsCount));
                    return IGraphTransformer::TStatus::Error;
                }

                lambdaInputItemTypes.push_back(inputStreams ? inputStreams->GetItems()[index] : inputItemType);
            }

            if (lambdaInputItemTypes.empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i)->Pos()), "At least one stream index must be specified"));
            }

            if (i + 1 == input->ChildrenSize()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i)->Pos()), "Expected handler"));
                return IGraphTransformer::TStatus::Error;
            }

            auto status = ConvertToLambda(input->ChildRef(i + 1), ctx.Expr, 1);
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            const auto lambdaInputItemType = 1U == lambdaInputItemTypes.size() ?
                lambdaInputItemTypes.front(): ctx.Expr.MakeType<TVariantExprType>(ctx.Expr.MakeType<TTupleExprType>(lambdaInputItemTypes));

            const auto lambdaInputType = MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *lambdaInputItemType, ctx.Expr);

            auto& lambda = input->ChildRef(i + 1);
            if (!UpdateLambdaAllArgumentsTypes(lambda, { lambdaInputType }, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!lambda->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            const TTypeAnnotationNode* lambdaItemType = nullptr;
            if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream) {
                if (!EnsureStreamType(lambda->Pos(), *lambda->GetTypeAnn(), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                lambdaItemType = lambda->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
            } else {
                YQL_ENSURE(input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow);
                if (!EnsureFlowType(lambda->Pos(), *lambda->GetTypeAnn(), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                lambdaItemType = lambda->GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
            }

            if (!EnsurePersistableType(lambda->Pos(), *lambdaItemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (lambdaItemType->GetKind() != ETypeAnnotationKind::Variant) {
                outputStreamItems.push_back(lambdaItemType);
                continue;
            }

            const auto underlyingType = lambdaItemType->Cast<TVariantExprType>()->GetUnderlyingType();
            if (!EnsureTupleType(lambda->Pos(), *underlyingType, ctx.Expr)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), "Lambda output variant must be tuple based"));
                return IGraphTransformer::TStatus::Error;
            }

            const auto lambdaTupleType = underlyingType->Cast<TTupleExprType>();
            for (auto& x : lambdaTupleType->GetItems()) {
                outputStreamItems.push_back(x);
            }
        }

        const auto resultItemType = 1U == outputStreamItems.size() ?
            outputStreamItems.front() : ctx.Expr.MakeType<TVariantExprType>(ctx.Expr.MakeType<TTupleExprType>(outputStreamItems));

        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *resultItemType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus HasItemsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Head().GetTypeAnn()) {
            YQL_ENSURE(input->Head().Type() == TExprNode::Lambda);
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected (optional) (empty) list or dict type, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        auto originalType = input->Head().GetTypeAnn();
        auto type = originalType;
        bool isOptional = false;
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
            isOptional = true;
        }

        if (type->GetKind() == ETypeAnnotationKind::EmptyList || type->GetKind() == ETypeAnnotationKind::EmptyDict) {
            output = MakeBool(input->Pos(), false, ctx.Expr);
            if (isOptional) {
                output = MakeConstMap(input->Pos(), input->HeadPtr(), output, ctx.Expr);
            }

            return IGraphTransformer::TStatus::Repeat;
        }

        if (type->GetKind() != ETypeAnnotationKind::List && type->GetKind() != ETypeAnnotationKind::Dict) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected (optional) (empty) list or dict type, but got: " << *originalType));
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* resType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool);
        if (isOptional) {
            resType = ctx.Expr.MakeType<TOptionalExprType>(resType);
        }

        input->SetTypeAnn(resType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ExtendWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        switch (input->ChildrenSize()) {
            case 0U:
                output = ctx.Expr.NewCallable(input->Pos(), "EmptyList", {});
                return IGraphTransformer::TStatus::Repeat;
            case 1U:
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            default:
                break;
        }

        const bool hasEmptyLists = AnyOf(input->Children(), IsEmptyList);
        if (hasEmptyLists) {
            auto children = input->ChildrenList();
            EraseIf(children, IsEmptyList);
            output = ctx.Expr.ChangeChildren(*input, std::move(children));
            return IGraphTransformer::TStatus::Repeat;
        }

        auto listType = input->Head().GetTypeAnn();
        for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
            if (!EnsureNewSeqType<false>(*input->Child(i), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (i && !IsSameAnnotation(*listType, *input->Child(i)->GetTypeAnn())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder()
                    << "Mismatch type of sequences being extended: "
                    << GetTypeDiff(*listType, *input->Child(i)->GetTypeAnn())));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (listType->GetKind() == ETypeAnnotationKind::Flow) {
            auto flowItem = listType->Cast<TFlowExprType>()->GetItemType();
            if (flowItem->GetKind() == ETypeAnnotationKind::Multi) {
                auto items = flowItem->Cast<TMultiExprType>()->GetItems();
                if (!items.empty()) {
                    items.pop_back();
                    if (AnyOf(items, [](const auto& item) { return item->GetKind() == ETypeAnnotationKind::Scalar; })) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder()
                            << "Scalars are not supported in Extend inputs:  " << *listType));
                        return IGraphTransformer::TStatus::Error;
                    }
                }
            }
        }

        input->SetTypeAnn(listType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus UnionAllWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        switch (input->ChildrenSize()) {
            case 0U:
                output = ctx.Expr.NewCallable(input->Pos(), "EmptyList", {});
                return IGraphTransformer::TStatus::Repeat;
            case 1U:
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            default:
                break;
        }

        const bool hasEmptyLists = AnyOf(input->Children(), IsEmptyList);
        if (hasEmptyLists) {
            auto children = input->ChildrenList();
            EraseIf(children, IsEmptyList);
            output = ctx.Expr.ChangeChildren(*input, std::move(children));
            return IGraphTransformer::TStatus::Repeat;
        }

        std::unordered_map<std::string_view, std::pair<const TTypeAnnotationNode*, ui32>> members;
        const auto inputsCount = input->ChildrenSize();
        const auto checkStructType = [&members, &ctx, inputsCount, pos = input->Pos()](TExprNode& input) -> const TStructExprType* {
            if (!EnsureListType(input, ctx.Expr)) {
                return nullptr;
            }
            const auto itemType = input.GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            if (!EnsureStructType(input.Pos(), *itemType, ctx.Expr)) {
                return nullptr;
            }
            const auto structType = itemType->Cast<TStructExprType>();
            for (const auto& item: structType->GetItems()) {
                if (const auto res = members.insert({ item->GetName(), { item->GetItemType(), 1U } }); !res.second) {
                    if (item->GetItemType()->GetKind() == ETypeAnnotationKind::Error) {
                        continue;
                    }

                    auto& p = res.first->second;
                    if (p.first->GetKind() == ETypeAnnotationKind::Error) {
                        continue;
                    }

                    if (const auto commonType = CommonType<false, true>(input.Pos(), p.first, item->GetItemType(), ctx.Expr)) {
                        p.first = commonType;
                        ++p.second;
                        continue;
                    }

                    const TIssue err(
                        ctx.Expr.GetPosition(pos),
                        TStringBuilder()
                            << "Uncompatible member " << item->GetName() << " types: "
                            << *p.first << " and " << *item->GetItemType()
                    );

                    p = { ctx.Expr.MakeType<TErrorExprType>(err), inputsCount };
                }
            }
            return structType;
        };

        TVector<const TStructExprType*> structTypes;
        for (auto child : input->Children()) {
            auto structType = checkStructType(*child);
            if (!structType) {
                return IGraphTransformer::TStatus::Error;
            }

            structTypes.push_back(structType);
        }

        TVector<const TItemExprType*> resultItems;
        THashSet<TStringBuf> addedMembers;
        auto addResultItems = [&resultItems, &members, &ctx, &addedMembers, inputsCount](const TStructExprType* structType) {
            for (auto item: structType->GetItems()) {
                const TTypeAnnotationNode* memberType = nullptr;
                auto it = members.find(item->GetName());
                YQL_ENSURE(it != members.end());
                memberType = it->second.first;
                if (it->second.first->GetKind() != ETypeAnnotationKind::Error && it->second.second < inputsCount) {
                    if (!memberType->IsOptionalOrNull()) {
                        memberType = ctx.Expr.MakeType<TOptionalExprType>(memberType);
                    }
                }

                if (addedMembers.insert(item->GetName()).second) {
                    resultItems.push_back(ctx.Expr.MakeType<TItemExprType>(
                        item->GetName(),
                        memberType
                    ));
                }
            }
        };

        for (auto structType : structTypes) {
            addResultItems(structType);
        }

        auto structType = ctx.Expr.MakeType<TStructExprType>(resultItems);
        if (!structType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(structType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus UnionAllPositionalWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!ctx.Types.OrderedColumns) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder()
                << "Unable to handle positional UNION ALL with column order disabled"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TColumnOrder resultColumnOrder;
        const TStructExprType* resultStructType = nullptr;
        auto status = InferPositionalUnionType(input->Pos(), input->ChildrenList(), resultColumnOrder, resultStructType, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(resultStructType));
        return ctx.Types.SetColumnOrder(*input, resultColumnOrder, ctx.Expr);
    }

    IGraphTransformer::TStatus InferPositionalUnionType(TPositionHandle pos, const TExprNode::TListType& children,
        TColumnOrder& resultColumnOrder, const TStructExprType*& resultStructType, TExtContext& ctx) {
        TTypeAnnotationNode::TListType resultTypes;
        size_t idx = 0;
        for (const auto& child : children) {
            if (!EnsureListType(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            auto itemType = child->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            YQL_ENSURE(itemType);
            if (!EnsureStructType(child->Pos(), *itemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto childColumnOrder = ctx.Types.LookupColumnOrder(*child);
            if (!childColumnOrder) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder()
                    << "Input #" << idx << " does not have ordered columns. "
                    << "Consider making column order explicit by using SELECT with column names"));
                return IGraphTransformer::TStatus::Error;
            }

            TTypeAnnotationNode::TListType childTypes;
            const auto structType = itemType->Cast<TStructExprType>();
            for (const auto& [col, gen_col] : *childColumnOrder) {
                auto itemIdx = structType->FindItem(gen_col);
                YQL_ENSURE(itemIdx);
                childTypes.push_back(structType->GetItems()[*itemIdx]->GetItemType());
            }

            if (idx == 0) {
                resultColumnOrder = *childColumnOrder;
                resultTypes = childTypes;
            } else {
                if (childTypes.size() != resultTypes.size()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder()
                        << "Different column counts in UNION ALL inputs: input #0 has " << resultTypes.size()
                        << " column, input #" << idx << " has " << childTypes.size() << " columns"));
                    return IGraphTransformer::TStatus::Error;
                }
                for (size_t i = 0; i < childTypes.size(); ++i) {
                    if (const auto commonType = CommonType<false>(child->Pos(), resultTypes[i], childTypes[i], ctx.Expr))
                        resultTypes[i] = commonType;
                    else
                        return IGraphTransformer::TStatus::Error;
                }
            }
            idx++;
        }

        YQL_ENSURE(resultColumnOrder.Size() == resultTypes.size());
        TVector<const TItemExprType*> structItems;
        for (size_t i = 0; i < resultTypes.size(); ++i) {
            structItems.push_back(ctx.Expr.MakeType<TItemExprType>(resultColumnOrder[i].PhysicalName, resultTypes[i]));
        }

        resultStructType = ctx.Expr.MakeType<TStructExprType>(structItems);
        if (!resultStructType->Validate(pos, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ListAutomapArgs(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx,
        TStringBuf name)
    {
        bool isSomeOptional = false;
        for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
            if (!input->Child(i)->GetTypeAnn()) {
                YQL_ENSURE(input->Child(i)->Type() == TExprNode::Lambda);
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i)->Pos()), TStringBuilder()
                    << "Expected (optional) list type, but got lambda"));
                return IGraphTransformer::TStatus::Error;
            }

            if (IsNull(*input->Child(i))) {
                output = input->ChildPtr(i);
                return IGraphTransformer::TStatus::Repeat;
            }

            bool isOptional = false;
            auto inputType = input->Child(i)->GetTypeAnn();
            if (inputType->GetKind() == ETypeAnnotationKind::Optional) {
                isOptional = true;
                inputType = inputType->Cast<TOptionalExprType>()->GetItemType();
            }

            if (inputType->GetKind() != ETypeAnnotationKind::List &&
                inputType->GetKind() != ETypeAnnotationKind::EmptyList) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i)->Pos()), TStringBuilder()
                    << "Expected (optional) (empty) list type, but got: " << *input->Child(i)->GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }

            isSomeOptional = isSomeOptional || isOptional;
        }

        if (!isSomeOptional) {
            output = ctx.Expr.RenameNode(*input, name);
            return IGraphTransformer::TStatus::Repeat;
        }

        // pack all nodes to tuple
        output = ctx.Expr.Builder(input->Pos())
            .Callable("Map")
                .Callable(0, "FilterNullElements")
                    .Callable(0, "Just")
                        .List(0)
                            .Add(input->ChildrenList())
                        .Seal()
                    .Seal()
                .Seal()
                .Lambda(1)
                    .Param("item")
                    .Callable(name)
                    .Do([&](TExprNodeBuilder& builder) -> TExprNodeBuilder& {
                        for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
                            builder.Callable(i, "Nth")
                                    .Arg(0, "item")
                                    .Atom(1, ToString(i), TNodeFlags::Default)
                                .Seal();
                        }

                        return builder;
                    })

                    .Seal()
                .Seal()
            .Seal()
            .Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    template<bool IsStrict>
    IGraphTransformer::TStatus ListExtendWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!input->ChildrenSize()) {
            output = ctx.Expr.NewCallable(input->Pos(), "EmptyList", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        bool hasEmptyList = false;
        for (const auto& child : input->Children()) {
            if (!child->GetTypeAnn()) {
                YQL_ENSURE(child->Type() == TExprNode::Lambda);
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder()
                    << "Expected (optional) list type, but got lambda"));
                return IGraphTransformer::TStatus::Error;
            }

            if (IsNull(*child)) {
                output = child;
                return IGraphTransformer::TStatus::Repeat;
            }
            hasEmptyList = hasEmptyList || ETypeAnnotationKind::EmptyList == child->GetTypeAnn()->GetKind();
        }

        if (hasEmptyList) {
            auto children = input->ChildrenList();
            children.erase(std::remove_if(children.begin(), children.end(),
                [](const TExprNode::TPtr& child) { return ETypeAnnotationKind::EmptyList == child->GetTypeAnn()->GetKind(); }), children.end());
            output = ctx.Expr.ChangeChildren(*input, std::move(children));
            return IGraphTransformer::TStatus::Repeat;
        }

        if constexpr (!IsStrict) {
            if (const auto commonType = CommonTypeForChildren(*input, ctx.Expr)) {
                if (const auto status = ConvertChildrenToType(input, commonType, ctx.Expr); status != IGraphTransformer::TStatus::Ok)
                    return status;
            } else
                return IGraphTransformer::TStatus::Error;
        }

        return ListAutomapArgs(input, output, ctx, "OrderedExtend");
    }

    template IGraphTransformer::TStatus ListExtendWrapper<true>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    template IGraphTransformer::TStatus ListExtendWrapper<false>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    IGraphTransformer::TStatus ListUnionAllWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return ListAutomapArgs(input, output, ctx, "UnionAll");
    }

    IGraphTransformer::TStatus ListZipWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return ListAutomapArgs(input, output, ctx, "Zip");
    }

    IGraphTransformer::TStatus ListZipAllWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return ListAutomapArgs(input, output, ctx, "ZipAll");
    }

    bool ValidateSortDirections(TExprNode& direction, TExprContext& ctx, bool& isTuple) {
        bool isOkAscending = false;

        if (direction.GetTypeAnn()) {
            if (direction.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Data &&
                direction.GetTypeAnn()->Cast<TDataExprType>()->GetSlot() == EDataSlot::Bool) {
                isOkAscending = true;
            }
            else if (direction.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
                isTuple = true;
                auto& items = direction.GetTypeAnn()->Cast<TTupleExprType>()->GetItems();
                isOkAscending = true;
                for (auto& child : items) {
                    if (child->GetKind() != ETypeAnnotationKind::Data || child->Cast<TDataExprType>()->GetSlot() != EDataSlot::Bool) {
                        isOkAscending = false;
                        break;
                    }
                }
            }
        }

        if (!isOkAscending) {
            ctx.AddError(TIssue(ctx.GetPosition(direction.Pos()), "Expected either Bool or tuple of Bool"));
            return false;
        }

        return true;
    }

    IGraphTransformer::TStatus ValidateSortTraits(const TTypeAnnotationNode* itemType, const TExprNode::TPtr& direction, TExprNode::TPtr& lambda, TExprContext& ctx) {
        bool isTuple = false;
        if (!ValidateSortDirections(*direction, ctx, isTuple)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(lambda, ctx, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (isTuple) {
            const auto directionTupleSize = direction->GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
            if (directionTupleSize == 0 || directionTupleSize >= 2) {
                if (lambda->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Tuple) {
                    ctx.AddError(TIssue(ctx.GetPosition(lambda->Pos()), TStringBuilder() << "expected tuple type as lambda result, but got: " << *lambda->GetTypeAnn()));
                    return IGraphTransformer::TStatus::Error;
                }

                const auto lambdaTupleSize = lambda->GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
                if (lambdaTupleSize != directionTupleSize) {
                    ctx.AddError(TIssue(ctx.GetPosition(lambda->Pos()), TStringBuilder() <<
                        "Mismatch of lambda result and ascending parameters sizes: " <<
                        lambdaTupleSize << " != " << directionTupleSize));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        if (!lambda->GetTypeAnn()->IsComparable()) {
            ctx.AddError(TIssue(ctx.GetPosition(lambda->Pos()), TStringBuilder() << "Expected comparable type, but got: " << *lambda->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SortWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ValidateSortTraits(itemType, input->Child(1), input->TailRef(), ctx.Expr);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus TopWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
        const auto convertStatus = TryConvertTo(input->ChildRef(1), *expectedType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), "Mismatch argument types"));
            return IGraphTransformer::TStatus::Error;
        }

        const auto status = ValidateSortTraits(itemType, input->Child(2), input->TailRef(), ctx.Expr);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus KeepTopWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(*input->Child(1))) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(input->Child(1)->Pos(), *input->Child(1)->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
        if (const auto convertStatus = TryConvertTo(input->ChildRef(0), *expectedType, ctx.Expr); convertStatus.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Mismatch argument types"));
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = input->Child(1)->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (!IsSameAnnotation(*itemType, *input->Child(2)->GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), TStringBuilder() << "Mismatch list items and new item types, "
                << *itemType << " != " << *input->Child(2)->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        const auto status = ValidateSortTraits(itemType, input->Child(3), input->TailRef(), ctx.Expr);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(input->Child(1)->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus UnorderedWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SortTraitsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        const auto& listTypeNode = input->Head();
        auto& listType = *listTypeNode.GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (listType.GetKind() == ETypeAnnotationKind::EmptyList) {
            output = ctx.Expr.NewCallable(input->Pos(), "Void", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(listTypeNode.Pos(), listType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ValidateSortTraits(listType.Cast<TListExprType>()->GetItemType(), input->Child(1), input->TailRef(), ctx.Expr);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SessionWindowTraitsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 4, 5, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        const auto listTypeNode = input->HeadPtr();
        const auto sortTraits = input->ChildPtr(1);

        auto& listType = *listTypeNode->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (listType.GetKind() == ETypeAnnotationKind::EmptyList) {
            output = ctx.Expr.NewCallable(input->Pos(), "Void", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(listTypeNode->Pos(), listType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!sortTraits->IsCallable({"Void", "SortTraits"})) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(sortTraits->Pos()),
                TStringBuilder() << "Expecting SortTraits or Void as second argument"));
            return IGraphTransformer::TStatus::Error;
        }

        auto itemType = listType.Cast<TListExprType>()->GetItemType();

        if (input->ChildrenSize() == 4) {
            // legacy SessionWindowTraits
            auto& sessionKey = input->ChildRef(2);
            auto& sessionPred = input->TailRef();

            auto status = ConvertToLambda(sessionKey, ctx.Expr, 1);
            status = status.Combine(ConvertToLambda(sessionPred, ctx.Expr, 2));
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            if (!UpdateLambdaAllArgumentsTypes(sessionKey, { itemType }, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto sessionKeyType = sessionKey->GetTypeAnn();
            if (!sessionKeyType) {
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureComputableType(sessionKey->Pos(), *sessionKeyType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!UpdateLambdaAllArgumentsTypes(sessionPred, { sessionKeyType, sessionKeyType }, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto sessionPredType = sessionPred->GetTypeAnn();
            if (!sessionPredType) {
                return IGraphTransformer::TStatus::Repeat;
            }

            if (sessionPredType->GetKind() != ETypeAnnotationKind::Data ||
                sessionPredType->Cast<TDataExprType>()->GetSlot() != EDataSlot::Bool)
            {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(sessionPred->Pos()),
                    TStringBuilder() << "Expected Bool as return type of session timeout predicate, but got: " << *sessionPredType));
                return IGraphTransformer::TStatus::Error;
            }

            // rewrite in new format
            auto initLambda = ctx.Expr.Builder(input->Pos())
                .Lambda()
                    .Param("row")
                    .Apply(sessionKey)
                        .With(0,  "row")
                    .Seal()
                .Seal()
                .Build();

            auto calculateLambda = ctx.Expr.Builder(input->Pos())
                .Lambda()
                    .Param("row")
                    .Param("state")
                    .Apply(sessionKey)
                        .With(0,  "row")
                    .Seal()
                .Seal()
                .Build();

            auto updateLambda = ctx.Expr.Builder(input->Pos())
                .Lambda()
                    .Param("row")
                    .Param("state")
                    .List()
                        .Apply(0, sessionPred)
                            .With(0, "state")
                            .With(1)
                                .Apply(sessionKey)
                                    .With(0, "row")
                                .Seal()
                            .Done()
                        .Seal()
                        .Apply(1, sessionKey)
                            .With(0, "row")
                        .Seal()
                    .Seal()
                .Seal()
                .Build();

            output = ctx.Expr.ChangeChildren(*input, { listTypeNode, sortTraits, initLambda, updateLambda, calculateLambda });
            return IGraphTransformer::TStatus::Repeat;
        }

        auto& initLambda = input->ChildRef(2);
        auto& updateLambda = input->ChildRef(3);
        auto& calculateLambda = input->ChildRef(4);

        auto status = ConvertToLambda(initLambda, ctx.Expr, 1);
        status = status.Combine(ConvertToLambda(updateLambda, ctx.Expr, 2));
        status = status.Combine(ConvertToLambda(calculateLambda, ctx.Expr, 2));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        // initLambda
        if (!UpdateLambdaAllArgumentsTypes(initLambda, { itemType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto sessionStateType = initLambda->GetTypeAnn();
        if (!sessionStateType) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureComputableType(initLambda->Pos(), *sessionStateType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // calculateLambda
        if (!UpdateLambdaAllArgumentsTypes(calculateLambda, { itemType, sessionStateType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto sessionValueType = calculateLambda->GetTypeAnn();
        if (!sessionValueType) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureComputableType(initLambda->Pos(), *sessionValueType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // updateLambda
        if (!UpdateLambdaAllArgumentsTypes(updateLambda, { itemType, sessionStateType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto updateLambdaType = updateLambda->GetTypeAnn();
        if (!updateLambdaType) {
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* expectedType =
            ctx.Expr.MakeType<TTupleExprType>(
                TTypeAnnotationNode::TListType{ ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool), sessionStateType });

        if (!IsSameAnnotation(*expectedType, *updateLambdaType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(updateLambda->Pos()),
                TStringBuilder() << "Expected " << *expectedType << " as return type of update lambda, but got: " << *updateLambdaType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus GroupByKeyWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(input->ChildRef(1), ctx.Expr, 1);
        status = status.Combine(ConvertToLambda(input->TailRef(), ctx.Expr, 2));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambdaKeySelector = input->ChildRef(1);
        auto itemType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (!UpdateLambdaAllArgumentsTypes(lambdaKeySelector, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaKeySelector->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureOneOrTupleOfDataOrOptionalOfData(*lambdaKeySelector, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (lambdaKeySelector->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple &&
            lambdaKeySelector->GetTypeAnn()->Cast<TTupleExprType>()->GetSize() < 2) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaKeySelector->Pos()), "Tuple must contain at least 2 items"));
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambdaListHandler = input->TailRef();
        if (!UpdateLambdaAllArgumentsTypes(lambdaListHandler, {lambdaKeySelector->GetTypeAnn(), input->Head().GetTypeAnn()}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaListHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto retKind = lambdaListHandler->GetTypeAnn()->GetKind();
        if (retKind != ETypeAnnotationKind::List && retKind != ETypeAnnotationKind::Optional
            && retKind != ETypeAnnotationKind::Stream) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaListHandler->Pos()), TStringBuilder() <<
                "Expected either list, stream or optional, but got: " << *lambdaListHandler->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (retKind == ETypeAnnotationKind::List) {
            input->SetTypeAnn(lambdaListHandler->GetTypeAnn());
        } else if (retKind == ETypeAnnotationKind::Optional) {
            auto itemType = lambdaListHandler->GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
            input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(itemType));
        } else {
            auto itemType = lambdaListHandler->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
            input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(itemType));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PartitionByKeyWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(input->ChildRef(1), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (input->Child(2)->IsCallable("Void") != input->Child(3)->IsCallable("Void")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(3)->Pos()), TStringBuilder() <<
                "Direction and sort key extractor should be specified at the same time"));
            return IGraphTransformer::TStatus::Error;
        }

        bool hasSort = !input->Child(2)->IsCallable("Void");
        if (hasSort) {
            auto status = ValidateSortTraits(itemType, input->Child(2), input->ChildRef(3), ctx.Expr);
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        status = ConvertToLambda(input->ChildRef(4), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambdaKeySelector = input->ChildRef(1);
        if (!UpdateLambdaAllArgumentsTypes(lambdaKeySelector, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaKeySelector->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto keyType = lambdaKeySelector->GetTypeAnn();
        if (!EnsureHashableKey(lambdaKeySelector->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureEquatableKey(lambdaKeySelector->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambdaListHandler = input->ChildRef(4);
        TTypeAnnotationNode::TListType tupleItems = {
            lambdaKeySelector->GetTypeAnn(),
            ctx.Expr.MakeType<TStreamExprType>(itemType)
        };

        const auto groupType = ctx.Expr.MakeType<TTupleExprType>(tupleItems);
        const auto listOfGroupsType = ctx.Expr.MakeType<TStreamExprType>(groupType);
        if (!UpdateLambdaAllArgumentsTypes(lambdaListHandler, {listOfGroupsType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaListHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureSeqOrOptionalType(lambdaListHandler->Pos(), *lambdaListHandler->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto retKind = lambdaListHandler->GetTypeAnn()->GetKind();
        if (retKind != ETypeAnnotationKind::List && retKind != ETypeAnnotationKind::Optional &&
            retKind != ETypeAnnotationKind::Stream)
        {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaListHandler->Pos()), TStringBuilder() <<
                "Expected either list, stream or optional, but got: " << *lambdaListHandler->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }


        const auto& retItemType = GetSeqItemType(*lambdaListHandler->GetTypeAnn());
        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), retItemType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PartitionsByKeysWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambdaKeySelector = input->ChildRef(1);
        const auto status1 = ConvertToLambda(lambdaKeySelector, ctx.Expr, 1);
        if (status1.Level != IGraphTransformer::TStatus::Ok) {
            return status1;
        }

        auto& lambdaFinalHandler = input->TailRef();
        const auto status = ConvertToLambda(lambdaFinalHandler, ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (input->Child(2)->IsCallable("Void") != input->Child(3)->IsCallable("Void")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(3)->Pos()), TStringBuilder() <<
                "Direction and sort key extractor should be specified at the same time"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Child(2)->IsCallable("Void")) {
            const auto status = ValidateSortTraits(itemType, input->Child(2), input->ChildRef(3), ctx.Expr);
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaKeySelector, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaKeySelector->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto keyType = lambdaKeySelector->GetTypeAnn();
        if (!EnsureHashableKey(lambdaKeySelector->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureEquatableKey(lambdaKeySelector->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaFinalHandler, { input->Head().GetTypeAnn() }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaFinalHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureNewSeqType<false>(input->Tail().Pos(), *lambdaFinalHandler->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(lambdaFinalHandler->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ReverseWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus TakeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
        auto convertStatus = TryConvertTo(input->ChildRef(1), *expectedType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), "Mismatch argument types"));
            return IGraphTransformer::TStatus::Error;
        }

        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FoldWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->ChildPtr(1);
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsEmptyList(input->Head())) {
            output = input->ChildPtr(1);
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListOrOptionalType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto status = ConvertToLambda(input->TailRef(), ctx.Expr, 2);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            output = ctx.Expr.Builder(input->Pos())
                .Callable("IfPresent")
                    .Add(0, input->HeadPtr())
                    .Lambda(1)
                        .Param("item")
                        .Apply(input->TailPtr())
                            .With(0, "item")
                            .With(1, input->ChildPtr(1))
                        .Seal()
                    .Seal()
                    .Add(2, input->ChildPtr(1))
                .Seal().Build();
            return IGraphTransformer::TStatus::Repeat;
        }

        auto& lambda = input->TailRef();
        const auto itemType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();

        if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType, input->Child(1)->GetTypeAnn()}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*lambda->GetTypeAnn(), *input->Child(1)->GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder() << "Mismatch of lambda return type and state type, "
                << *lambda->GetTypeAnn() << " != " << *input->Child(1)->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Child(1)->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus Fold1Wrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsEmptyList(input->Head())) {
            output = ctx.Expr.NewCallable(input->Pos(), "Null", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListOrOptionalType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto status1 = ConvertToLambda(input->ChildRef(1), ctx.Expr, 1);
        if (status1.Level != IGraphTransformer::TStatus::Ok) {
            return status1;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            output = ctx.Expr.NewCallable(input->Pos(), "Map", {input->HeadPtr(), input->ChildPtr(1)});
            return IGraphTransformer::TStatus::Repeat;
        }

        const auto status2 = ConvertToLambda(input->TailRef(), ctx.Expr, 2);
        if (status2.Level != IGraphTransformer::TStatus::Ok) {
            return status2;
        }

        auto& initLambda = input->ChildRef(1);
        const auto itemType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();

        if (!UpdateLambdaAllArgumentsTypes(initLambda, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!initLambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        const auto stateType = initLambda->GetTypeAnn();
        auto& updateLambda = input->TailRef();
        if (!UpdateLambdaAllArgumentsTypes(updateLambda, {itemType, stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!updateLambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*updateLambda->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(updateLambda->Pos()), TStringBuilder() << "Mismatch of lambda return type and state type, "
                << *updateLambda->GetTypeAnn() << "!= " << *stateType));
            return IGraphTransformer::TStatus::Error;
        }

        const auto optAnn = ctx.Expr.MakeType<TOptionalExprType>(stateType);
        input->SetTypeAnn(optAnn);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CondenseWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
        if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto stateType = input->Child(1)->GetTypeAnn();

        auto& switchLambda = input->ChildRef(2);
        auto& updateLambda = input->ChildRef(3);

        const auto status = ConvertToLambda(switchLambda, ctx.Expr, 2).Combine(ConvertToLambda(updateLambda, ctx.Expr, 2));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(switchLambda, { itemType, stateType }, ctx.Expr) ||
            !UpdateLambdaAllArgumentsTypes(updateLambda, { itemType, stateType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!switchLambda->GetTypeAnn() || !updateLambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(switchLambda->Pos(), switchLambda->GetTypeAnn(), isOptional, dataType, ctx.Expr) || !EnsureSpecificDataType(switchLambda->Pos(), *dataType, EDataSlot::Bool, ctx.Expr)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(switchLambda->Pos()), TStringBuilder() << "Switch lambda returns " << *switchLambda->GetTypeAnn() << " instead of boolean."));
            return IGraphTransformer::TStatus::Error;
        }

        if (!IsSameAnnotation(*updateLambda->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(updateLambda->Pos()), TStringBuilder() << "Mismatch of lambda return type and state type, "
                << *updateLambda->GetTypeAnn() << " != " << *stateType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *stateType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus Condense1Wrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
        if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& initLambda = input->ChildRef(1);
        auto& switchLambda = input->ChildRef(2);
        auto& updateLambda = input->ChildRef(3);

        const auto status = ConvertToLambda(initLambda, ctx.Expr, 1)
            .Combine(ConvertToLambda(switchLambda, ctx.Expr, 2))
            .Combine(ConvertToLambda(updateLambda, ctx.Expr, 2));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(initLambda, { itemType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!initLambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        const auto stateType = initLambda->GetTypeAnn();

        if (!UpdateLambdaAllArgumentsTypes(switchLambda, { itemType, stateType }, ctx.Expr) ||
            !UpdateLambdaAllArgumentsTypes(updateLambda, { itemType, stateType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!switchLambda->GetTypeAnn() || !updateLambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(switchLambda->Pos(), switchLambda->GetTypeAnn(), isOptional, dataType, ctx.Expr) || !EnsureSpecificDataType(switchLambda->Pos(), *dataType, EDataSlot::Bool, ctx.Expr)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(switchLambda->Pos()), TStringBuilder() << "Switch lambda returns " << *switchLambda->GetTypeAnn() << " instead of boolean."));
            return IGraphTransformer::TStatus::Error;
        }

        if (!IsSameAnnotation(*updateLambda->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(updateLambda->Pos()), TStringBuilder() << "Mismatch of lambda return type and state type, "
                << *updateLambda->GetTypeAnn() << "!= " << *stateType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *stateType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SqueezeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStreamType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambda = input->ChildRef(2);

        auto status = ConvertToLambda(lambda, ctx.Expr, 2);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto stateType = input->Child(1)->GetTypeAnn();
        auto itemType = input->Head().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
        if (!UpdateLambdaAllArgumentsTypes(lambda, { itemType, stateType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*lambda->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder() << "Mismatch of lambda return type and state type, "
                << *lambda->GetTypeAnn() << " != " << *stateType));
            return IGraphTransformer::TStatus::Error;
        }

        auto& saveLambda = input->ChildRef(3);
        auto& loadLambda = input->ChildRef(4);

        if (saveLambda->IsCallable("Void") != loadLambda->IsCallable("Void")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(saveLambda->Pos()), TStringBuilder() <<
                "Save and load lambdas must be specified at the same time"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!saveLambda->IsCallable("Void")) {
            auto status = ConvertToLambda(saveLambda, ctx.Expr, 1);
            status = status.Combine(ConvertToLambda(loadLambda, ctx.Expr, 1));
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            if (!UpdateLambdaAllArgumentsTypes(saveLambda, {stateType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!saveLambda->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            auto savedStateType = saveLambda->GetTypeAnn();
            if (!EnsurePersistableType(saveLambda->Pos(), *savedStateType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!UpdateLambdaAllArgumentsTypes(loadLambda, {savedStateType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!loadLambda->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!IsSameAnnotation(*loadLambda->GetTypeAnn(), *stateType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(loadLambda->Pos()), TStringBuilder() << "Mismatch of load lambda return type and state type, "
                    << *loadLambda->GetTypeAnn() << " != " << *stateType));
                return IGraphTransformer::TStatus::Error;
            }
        }

        auto issue = TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Deprecated Squeeze, use Condense instead.");
        SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YQL_DEPRECATED_FUNCTION_OR_SIGNATURE, issue);
        if (!ctx.Expr.AddWarning(issue)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TStreamExprType>(stateType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus Squeeze1Wrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStreamType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& initLambda = input->ChildRef(1);
        auto& updateLambda = input->ChildRef(2);

        auto status = ConvertToLambda(initLambda, ctx.Expr, 1);
        status = status.Combine(ConvertToLambda(updateLambda, ctx.Expr, 2));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto itemType = input->Head().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
        if (!UpdateLambdaAllArgumentsTypes(initLambda, { itemType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!initLambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto stateType = initLambda->GetTypeAnn();
        if (!UpdateLambdaAllArgumentsTypes(updateLambda, { itemType, stateType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!updateLambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*updateLambda->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(updateLambda->Pos()), TStringBuilder() << "Mismatch of lambda return type and state type, "
                << *updateLambda->GetTypeAnn() << "!= " << *stateType));
            return IGraphTransformer::TStatus::Error;
        }

        auto& saveLambda = input->ChildRef(3);
        auto& loadLambda = input->ChildRef(4);

        if (saveLambda->IsCallable("Void") != loadLambda->IsCallable("Void")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(saveLambda->Pos()), TStringBuilder() <<
                "Save and load lambdas must be specified at the same time"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!saveLambda->IsCallable("Void")) {
            auto status = ConvertToLambda(saveLambda, ctx.Expr, 1);
            status = status.Combine(ConvertToLambda(loadLambda, ctx.Expr, 1));
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            if (!UpdateLambdaAllArgumentsTypes(saveLambda, {stateType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!saveLambda->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            auto savedStateType = saveLambda->GetTypeAnn();
            if (!EnsurePersistableType(saveLambda->Pos(), *savedStateType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!UpdateLambdaAllArgumentsTypes(loadLambda, {savedStateType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!loadLambda->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!IsSameAnnotation(*loadLambda->GetTypeAnn(), *stateType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(loadLambda->Pos()), TStringBuilder() << "Mismatch of load lambda return type and state type, "
                    << *loadLambda->GetTypeAnn() << " != " << *stateType));
                return IGraphTransformer::TStatus::Error;
            }
        }

        auto issue = TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Deprecated Squeeze1, use Condense1 instead.");
        SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YQL_DEPRECATED_FUNCTION_OR_SIGNATURE, issue);
        if (!ctx.Expr.AddWarning(issue)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto optAnn = ctx.Expr.MakeType<TStreamExprType>(stateType);
        input->SetTypeAnn(optAnn);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus DiscardWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureNewSeqType<false, false>(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ZipWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!input->ChildrenSize()) {
            output = ctx.Expr.NewCallable(input->Pos(), "List", {ExpandType(input->Pos(), *ctx.Expr.MakeType<TListExprType>(ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType())), ctx.Expr)});
            return IGraphTransformer::TStatus::Repeat;
        }

        for (auto& child : input->Children()) {
            if (!EnsureListOrEmptyType(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        TTypeAnnotationNode::TListType tupleItems;
        bool hasEmpty = false;
        for (auto& child : input->Children()) {
            hasEmpty = hasEmpty || child->GetTypeAnn()->GetKind() != ETypeAnnotationKind::List;
            auto itemType = child->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List ?
                child->GetTypeAnn()->Cast<TListExprType>()->GetItemType() :
                ctx.Expr.MakeType<TVoidExprType>();
            tupleItems.push_back(itemType);
        }
        const auto returnType = ctx.Expr.MakeType<TListExprType>(ctx.Expr.MakeType<TTupleExprType>(tupleItems));
        if (hasEmpty) {
            output = ctx.Expr.NewCallable(input->Pos(), "List", {ExpandType(input->Pos(), *returnType, ctx.Expr)});
            return IGraphTransformer::TStatus::Repeat;
        }
        input->SetTypeAnn(returnType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ZipAllWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!input->ChildrenSize()) {
            output = ctx.Expr.NewCallable(input->Pos(), "List", {ExpandType(input->Pos(), *ctx.Expr.MakeType<TListExprType>(ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType())), ctx.Expr)});
            return IGraphTransformer::TStatus::Repeat;
        }

        for (auto& child : input->Children()) {
            if (!EnsureListOrEmptyType(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        TTypeAnnotationNode::TListType tupleItems;
        for (auto& child : input->Children()) {
            auto itemType = child->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List ?
                child->GetTypeAnn()->Cast<TListExprType>()->GetItemType() :
                ctx.Expr.MakeType<TVoidExprType>();

            auto optType = ctx.Expr.MakeType<TOptionalExprType>(itemType);
            tupleItems.push_back(optType);
        }

        auto retTuple = ctx.Expr.MakeType<TTupleExprType>(tupleItems);
        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(retTuple));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus EnumerateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureMaxArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
        if (input->ChildrenSize() > 1) {
            auto convertStatus = TryConvertTo(input->ChildRef(1), *expectedType, ctx.Expr);
            if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), "Mismatch argument types"));
                return IGraphTransformer::TStatus::Error;
            }

            if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
                return convertStatus;
            }
        }

        if (input->ChildrenSize() > 2) {
            auto convertStatus = TryConvertTo(input->ChildRef(2), *expectedType, ctx.Expr);
            if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), "Mismatch argument types"));
                return IGraphTransformer::TStatus::Error;
            }

            if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
                return convertStatus;
            }
        }

        TTypeAnnotationNode::TListType tupleItems(2);
        tupleItems[0] = expectedType;
        tupleItems[1] = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        auto retTuple = ctx.Expr.MakeType<TTupleExprType>(tupleItems);
        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(retTuple));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ListWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() == ETypeAnnotationKind::EmptyList) {
            output = ctx.Expr.NewCallable(input->Pos(), "EmptyList", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(input->Head().Pos(), *type, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto listType = type->Cast<TListExprType>();
        auto itemType = listType->GetItemType();
        for (size_t i = 1; i < input->ChildrenSize(); ++i) {
            if (!IsSameAnnotation(*itemType, *input->Child(i)->GetTypeAnn())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i)->Pos()), TStringBuilder() << "Mismatch type of list item, "
                    << *itemType << " != " << *input->Child(i)->GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(listType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CombineByKeyWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        ui32 expectedArgsCount = 6;
        bool isWithSpilling = false;
        if (input->Content().find("WithSpilling") != std::string::npos) {
            expectedArgsCount = 7;
            isWithSpilling = true;
        }
        bool isFinalize = false;
        if (input->Content().find("FinalizeByKey") != std::string::npos) {
            isFinalize = true;
        }
        if (!EnsureArgsCount(*input, expectedArgsCount, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* rawItemType = nullptr;
        if (!EnsureNewSeqType<false, true>(input->Head(), ctx.Expr, &rawItemType)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto inputTypeKind = input->Head().GetTypeAnn()->GetKind();

        auto status = ConvertToLambda(input->ChildRef(1), ctx.Expr, 1);
        status = status.Combine(ConvertToLambda(input->ChildRef(2), ctx.Expr, 1));
        status = status.Combine(ConvertToLambda(input->ChildRef(3), ctx.Expr, 2));
        status = status.Combine(ConvertToLambda(input->ChildRef(4), ctx.Expr, 3));
        status = status.Combine(ConvertToLambda(input->ChildRef(5), ctx.Expr, 2));
        if (isWithSpilling) {
            status = status.Combine(ConvertToLambda(input->ChildRef(6), ctx.Expr, 2));
        }
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambdaPreMap = input->ChildRef(1);
        if (!UpdateLambdaAllArgumentsTypes(lambdaPreMap, {rawItemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaPreMap->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<true, true>(*lambdaPreMap, ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambdaKeySelector = input->ChildRef(2);
        if (!UpdateLambdaAllArgumentsTypes(lambdaKeySelector, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaKeySelector->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto keyType = lambdaKeySelector->GetTypeAnn();
        if (!EnsureHashableKey(lambdaKeySelector->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureEquatableKey(lambdaKeySelector->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambdaInitHandler = input->ChildRef(3);
        if (!UpdateLambdaAllArgumentsTypes(lambdaInitHandler, {lambdaKeySelector->GetTypeAnn(), itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaInitHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureComputableType(lambdaInitHandler->Pos(), *lambdaInitHandler->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto stateType = lambdaInitHandler->GetTypeAnn();
        auto& lambdaUpdateHandler = input->ChildRef(4);
        if (!UpdateLambdaAllArgumentsTypes(lambdaUpdateHandler, {lambdaKeySelector->GetTypeAnn(), itemType, stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaUpdateHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*lambdaUpdateHandler->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaUpdateHandler->Pos()), TStringBuilder() <<
                "Mismatch of update lambda return type and state type, " << *lambdaUpdateHandler->GetTypeAnn() << "!= "
                << *stateType));
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambdaFinishHandler = input->ChildRef(5);
        if (!UpdateLambdaAllArgumentsTypes(lambdaFinishHandler, {lambdaKeySelector->GetTypeAnn(), stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaFinishHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureComputableType(lambdaFinishHandler->Pos(), *lambdaFinishHandler->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* retItemType = nullptr;
        if (isFinalize) {
            retItemType = lambdaFinishHandler->GetTypeAnn();
        } else {
            if (!EnsureNewSeqType<true, true>(*lambdaFinishHandler, ctx.Expr, &retItemType)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (isWithSpilling) {
            auto& lambdaSerDeHandler = input->ChildRef(6);
            if (isFinalize) {
                if (!UpdateLambdaAllArgumentsTypes(lambdaSerDeHandler, {lambdaKeySelector->GetTypeAnn(), stateType}, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else {
                if (!UpdateLambdaAllArgumentsTypes(lambdaSerDeHandler, {lambdaKeySelector->GetTypeAnn(), retItemType}, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            }

            if (!lambdaSerDeHandler->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureComputableType(lambdaSerDeHandler->Pos(), *lambdaSerDeHandler->GetTypeAnn(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(MakeSequenceType(inputTypeKind, *retItemType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ExtractWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListOrOptionalType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto firstArgKind = input->Head().GetTypeAnn()->GetKind();
        if (!EnsureAtom(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (firstArgKind == ETypeAnnotationKind::List) {
            itemType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        } else {
            itemType = input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
        }

        const TTypeAnnotationNode* extractedType = nullptr;
        if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
            auto structType = itemType->Cast<TStructExprType>();
            auto pos = structType->FindItem(input->Tail().Content());
            if (!pos) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() <<
                    "Member with name " << input->Tail().Content() << " is not found in " << *input->Head().GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }

            extractedType = structType->GetItems()[*pos]->GetItemType();
        }
        else if (itemType->GetKind() == ETypeAnnotationKind::Tuple) {
            ui32 index = 0;
            if (!TryFromString(input->Tail().Content(), index)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to convert to integer: " << input->Child(1)->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            auto tupleType = itemType->Cast<TTupleExprType>();
            if (index >= tupleType->GetSize()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Index out of range. Index: " <<
                    index << ", size: " << tupleType->GetSize() << ", type: " << *input->Head().GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }

            extractedType = tupleType->GetItems()[index];
        }
        else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() <<
                "Expected either list of struct or list tuples, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (firstArgKind == ETypeAnnotationKind::List) {
            input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(extractedType));
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(extractedType));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ExtractMembersWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto status = NormalizeTupleOfAtoms(input, 1, output, ctx.Expr);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        const TTypeAnnotationNode* inputItemType = nullptr;
        if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &inputItemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStructType(input->Head().Pos(), *inputItemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto structType = inputItemType->Cast<TStructExprType>();
        TVector<const TItemExprType*> resItems;
        for (auto& x : input->Tail().Children()) {
            YQL_ENSURE(x->IsAtom());
            auto pos = FindOrReportMissingMember(x->Content(), input->Head().Pos(), *structType, ctx.Expr);
            if (!pos) {
                return IGraphTransformer::TStatus::Error;
            }

            resItems.push_back(structType->GetItems()[*pos]);
        }

        const auto resItemType = ctx.Expr.MakeType<TStructExprType>(resItems);
        if (!resItemType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsSameAnnotation(*resItemType, *structType)) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *resItemType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template<bool Chopped>
    IGraphTransformer::TStatus AssumeConstraintWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, Chopped ? 2U : 1U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAnySeqType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->ChildrenSize() > 1U) {
            for (auto i = 1U; i < input->ChildrenSize(); ++i) {
                if (const auto status = NormalizeTupleOfAtoms<true, Chopped ? 1U : 2U>(input, i, output, ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                    return status;
                }
            }
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    template IGraphTransformer::TStatus AssumeConstraintWrapper<true>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    template IGraphTransformer::TStatus AssumeConstraintWrapper<false>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    IGraphTransformer::TStatus AssumeColumnOrderWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleOfAtoms(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (HasError(input->Head().GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Head().GetTypeAnn()) {
            YQL_ENSURE(input->Head().Type() == TExprNode::Lambda);
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()),
                TStringBuilder() << "Expected either struct or optional/list/stream/flow of struct, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::EmptyList) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Struct) {
            itemType = input->Head().GetTypeAnn();
        } else if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }


        if (!EnsureStructType(input->Head().Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TStructExprType* structType = itemType->Cast<TStructExprType>();

        auto inputColumns = GetColumnsOfStructOrSequenceOfStruct(*input->Head().GetTypeAnn());
        auto columnOrder = input->Tail().ChildrenList();

        TColumnOrder order;

        for (auto& column : columnOrder) {
            YQL_ENSURE(column->IsAtom());
            auto colName = TString(column->Content());
            auto pos = FindOrReportMissingMember(colName, input->Head().Pos(), *structType, ctx.Expr);
            if (!pos) {
                return IGraphTransformer::TStatus::Error;
            }
            inputColumns.erase(inputColumns.find(order.AddColumn(colName)));
        }

        if (input->IsCallable("AssumeColumnOrderPartial")) {
            for (auto& c : inputColumns) {
                columnOrder.push_back(ctx.Expr.NewAtom(input->Pos(), c));
            }

            output = ctx.Expr.Builder(input->Pos())
                .Callable("AssumeColumnOrder")
                    .Add(0, input->HeadPtr())
                    .Add(1, ctx.Expr.NewList(input->Tail().Pos(), std::move(columnOrder)))
                .Seal()
                .Build();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!inputColumns.empty()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()),
                TStringBuilder() << "Unordered columns " << JoinSeq(", ", inputColumns)
                                 << " in input of type " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (!ctx.Types.OrderedColumns) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AggregationTraitsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 8, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto itemType = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (itemType->GetKind() == ETypeAnnotationKind::Unit) {
            output = ctx.Expr.NewCallable(input->Pos(), "Void", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        IGraphTransformer::TStatus status = IGraphTransformer::TStatus::Ok;
        status = status.Combine(ConvertToLambda(input->ChildRef(1), ctx.Expr, 1, 2)); // init
        status = status.Combine(ConvertToLambda(input->ChildRef(2), ctx.Expr, 2, 3)); // update
        status = status.Combine(ConvertToLambda(input->ChildRef(3), ctx.Expr, 1)); // save
        status = status.Combine(ConvertToLambda(input->ChildRef(4), ctx.Expr, 1)); // load
        status = status.Combine(ConvertToLambda(input->ChildRef(5), ctx.Expr, 2)); // merge
        status = status.Combine(ConvertToLambda(input->ChildRef(6), ctx.Expr, 1)); // finish
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!EnsureComputableType(input->Head().Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambdaUpdate = input->ChildRef(2);
        const bool overState = lambdaUpdate->Tail().IsCallable("Void");

        auto& lambdaInit = input->ChildRef(1);

        auto ui32Type = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint32);
        if (lambdaInit->Head().ChildrenSize() == 1U) {
            if (!UpdateLambdaAllArgumentsTypes(lambdaInit, { itemType }, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!UpdateLambdaAllArgumentsTypes(lambdaInit, { itemType, ui32Type }, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!lambdaInit->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto combineStateType = lambdaInit->GetTypeAnn();
        if (!EnsureComputableType(lambdaInit->Pos(), *combineStateType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (lambdaUpdate->Head().ChildrenSize() == 2U) {
            if (!UpdateLambdaAllArgumentsTypes(lambdaUpdate, { itemType, combineStateType }, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!UpdateLambdaAllArgumentsTypes(lambdaUpdate, { itemType, combineStateType, ui32Type }, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!lambdaUpdate->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!overState) {
            if (!IsSameAnnotation(*lambdaUpdate->GetTypeAnn(), *combineStateType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaUpdate->Pos()), TStringBuilder() << "Mismatch update lambda result type, expected: "
                    << *combineStateType << ", but got: " << *lambdaUpdate->GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        auto& lambdaMerge = input->ChildRef(5);
        const bool noSaveLoad = lambdaMerge->Tail().IsCallable("Void");
        if (overState && noSaveLoad) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaMerge->Pos()), "Merge handler should be specified because of aggregation over states"));
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* reduceStateType = nullptr;
        if (!overState) {
            auto& lambdaSave = input->ChildRef(3);
            if (!UpdateLambdaAllArgumentsTypes(lambdaSave, { combineStateType }, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!lambdaSave->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            auto savedType = lambdaSave->GetTypeAnn();
            if (!noSaveLoad && !EnsurePersistableType(lambdaSave->Pos(), *savedType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto& lambdaLoad = input->ChildRef(4);
            if (!UpdateLambdaAllArgumentsTypes(lambdaLoad, { savedType }, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!lambdaLoad->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            auto& lambdaUpdate = input->ChildRef(2);
            if (!IsSameAnnotation(*lambdaUpdate->GetTypeAnn(), *lambdaLoad->GetTypeAnn())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaUpdate->Pos()), TStringBuilder() << "Mismatch state type after load, expected: "
                    << *lambdaUpdate->GetTypeAnn() << ", but got: " << *lambdaLoad->GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }

            reduceStateType = lambdaLoad->GetTypeAnn();
        } else {
            auto& lambdaLoad = input->ChildRef(4);
            if (!UpdateLambdaAllArgumentsTypes(lambdaLoad, { combineStateType }, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!lambdaLoad->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            reduceStateType = lambdaLoad->GetTypeAnn();

            auto& lambdaSave = input->ChildRef(3);
            if (!UpdateLambdaAllArgumentsTypes(lambdaSave, { reduceStateType }, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!lambdaSave->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!IsSameAnnotation(*lambdaSave->GetTypeAnn(), *combineStateType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaSave->Pos()), TStringBuilder() << "Mismatch serialized state type after save, expected: "
                    << *combineStateType << ", but got: " << *lambdaSave->GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaMerge, { reduceStateType, reduceStateType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaMerge->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!noSaveLoad && !IsSameAnnotation(*lambdaMerge->GetTypeAnn(), *reduceStateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaMerge->Pos()), TStringBuilder() << "Mismatch merge lambda result type, expected: "
                << *reduceStateType << ", but got: " << *lambdaMerge->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambdaFinish = input->ChildRef(6);
        if (!UpdateLambdaAllArgumentsTypes(lambdaFinish, { reduceStateType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaFinish->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto finishType = lambdaFinish->GetTypeAnn();
        if (!EnsureComputableType(lambdaFinish->Pos(), *finishType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        status = ValidateTraitsDefaultValue(input, 7, *finishType, "finish", output, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus MultiAggregateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto rowType = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();

        auto status = ConvertToLambda(input->ChildRef(1), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        status = ConvertToLambda(input->ChildRef(2), ctx.Expr, 2);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& exprLambda = input->ChildRef(1);
        if (!UpdateLambdaAllArgumentsTypes(exprLambda, { rowType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!exprLambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (exprLambda->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Struct &&
            exprLambda->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Tuple &&
            exprLambda->GetTypeAnn()->GetKind() != ETypeAnnotationKind::List) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(exprLambda->Pos()),
                TStringBuilder() << "Expected struct, tuple or list, but got: " << *exprLambda->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        output = RewriteMultiAggregate(*input, ctx.Expr);
        return output ? IGraphTransformer::TStatus::Repeat : IGraphTransformer::TStatus::Error;
    }

    IGraphTransformer::TStatus AggregateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        TStringBuf suffix = input->Content();
        YQL_ENSURE(suffix.SkipPrefix("Aggregate"));
        const bool isMany = suffix == "MergeManyFinalize";
        if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureMaxArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* inputItemType = nullptr;
        if (!EnsureNewSeqType<false, true>(input->Head(), ctx.Expr, &inputItemType)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto inputTypeKind = input->Head().GetTypeAnn()->GetKind();

        if (!EnsureStructType(input->Head().Pos(), *inputItemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto inputStructType = inputItemType->Cast<TStructExprType>();
        if (input->Child(1)->IsCallable("Void")) {
            TExprNodeList names;
            names.reserve(inputStructType->GetSize());
            for (const auto& item : inputStructType->GetItems()) {
                names.emplace_back(ctx.Expr.NewAtom(input->Child(1)->Pos(), item->GetName()));
            }
            output = ctx.Expr.ChangeChild(*input, 1, ctx.Expr.NewList(input->Child(1)->Pos(), std::move(names)));
            return IGraphTransformer::TStatus::Repeat;
        }

        if (isMany && ctx.Types.IsBlockEngineEnabled()) {
            auto streamIndex = inputStructType->FindItem("_yql_group_stream_index");
            if (streamIndex) {
                const TTypeAnnotationNode* streamIndexType = inputStructType->GetItems()[*streamIndex]->GetItemType();
                if (streamIndexType->GetKind() != ETypeAnnotationKind::Data || streamIndexType->Cast<TDataExprType>()->GetSlot() != EDataSlot::Uint32) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Invalid type for service column _yql_group_stream_index: expecting Uint32, got: " << *streamIndexType));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        auto status = NormalizeTupleOfAtoms(input, 1, output, ctx.Expr);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!EnsureTuple(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->ChildrenSize() < 4U) {
            auto children = input->ChildrenList();
            children.push_back(ctx.Expr.NewList(input->Pos(), {}));
            output = ctx.Expr.ChangeChildren(*input, std::move(children));
            return IGraphTransformer::TStatus::Repeat;
        } else {
            TExprNode::TPtr normalized;
            status = NormalizeKeyValueTuples(input->ChildPtr(3), 0, normalized, ctx.Expr);
            if (status.Level == IGraphTransformer::TStatus::Repeat) {
                output = ctx.Expr.ChangeChild(*input, 3, std::move(normalized));
                return status;
            }

            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        bool isHopping = false;
        const auto settings = input->Child(3);
        if (!EnsureTuple(*settings, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TVector<const TItemExprType*> rowColumns;
        TMaybe<TStringBuf> sessionColumnName;
        TMaybe<TStringBuf> hoppingColumnName;
        TExprNode::TPtr outputColumns;
        for (ui32 i = 0; i < settings->ChildrenSize(); ++i) {
            const auto& setting = settings->ChildPtr(i);
            if (!EnsureTupleMinSize(*setting, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(setting->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            const auto settingName = setting->Head().Content();
            if (settingName == "hopping") {
                isHopping = true;
                if (!EnsureTupleSize(*setting, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto value = setting->ChildPtr(1);
                if (value->Type() == TExprNode::List) {
                    if (!EnsureTupleSize(*value, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto hoppingOutputColumn = value->Child(0);
                    if (!EnsureAtom(*hoppingOutputColumn, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (hoppingOutputColumn->Content().Empty()) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(hoppingOutputColumn->Pos()),
                            TStringBuilder() << "Hopping output column name can not be empty"));
                        return IGraphTransformer::TStatus::Error;
                    }

                    hoppingColumnName = hoppingOutputColumn->Content();

                    auto traits = value->Child(1);
                    if (!traits->IsCallable("HoppingTraits")) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(setting->Child(1)->Pos()),
                            TStringBuilder() << "Expected HoppingTraits callable"));
                        return IGraphTransformer::TStatus::Error;
                    }

                    bool seenAsKeyColumn = AnyOf(input->Child(1)->ChildrenList(), [&](const auto& keyColum) {
                        return hoppingColumnName == keyColum->Content();
                    });

                    if (!seenAsKeyColumn) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(setting->Child(1)->Pos()),
                            TStringBuilder() << "Hopping column " << *hoppingColumnName << " is not listed in key columns"));
                        return IGraphTransformer::TStatus::Error;
                    }
                } else if (setting->Child(1)->IsCallable("HoppingTraits")) {
                    hoppingColumnName = "_yql_time"; // legacy hopping
                } else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(setting->Child(1)->Pos()),
                        TStringBuilder() << "Expected HoppingTraits callable"));
                    return IGraphTransformer::TStatus::Error;
                }

                rowColumns.push_back(ctx.Expr.MakeType<TItemExprType>(
                        *hoppingColumnName,
                        ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Timestamp))));
            } else if (settingName == "session") {
                if (!EnsureTupleSize(*setting, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto value = setting->ChildPtr(1);
                if (!EnsureTupleSize(*value, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto sessionOutputColumn = value->Child(0);
                if (!EnsureAtom(*sessionOutputColumn, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (sessionOutputColumn->Content().Empty()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(sessionOutputColumn->Pos()),
                        TStringBuilder() << "Session output column name can not be empty"));
                    return IGraphTransformer::TStatus::Error;
                }

                sessionColumnName = sessionOutputColumn->Content();

                auto traits = value->Child(1);
                if (!traits->IsCallable("SessionWindowTraits")) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(setting->Child(1)->Pos()),
                        TStringBuilder() << "Expected SessionWindowTraits callable"));
                    return IGraphTransformer::TStatus::Error;
                }

                auto sessionKeyType = traits->Child(TCoSessionWindowTraits::idx_Calculate)->GetTypeAnn();
                rowColumns.push_back(ctx.Expr.MakeType<TItemExprType>(*sessionColumnName, sessionKeyType));

                bool seenAsKeyColumn = AnyOf(input->Child(1)->ChildrenList(), [&](const auto& keyColum) {
                    return sessionColumnName == keyColum->Content();
                });

                if (!seenAsKeyColumn) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(setting->Child(1)->Pos()),
                        TStringBuilder() << "Session column " << *sessionColumnName << " is not listed in key columns"));
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (settingName == "compact") {
                if (!EnsureTupleSize(*setting, 1, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (settingName == "many_streams" && isMany) {
                if (!EnsureTupleSize(*setting, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto value = setting->ChildPtr(1);
                if (!EnsureTuple(*value, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!ValidateAggManyStreams(*value, input->Child(2)->ChildrenSize(), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (settingName == "output_columns" && suffix.empty()) {
                if (!EnsureTupleSize(*setting, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                TExprNode::TPtr newSetting;
                auto status = NormalizeTupleOfAtoms(setting, 1, newSetting, ctx.Expr);
                if (status != IGraphTransformer::TStatus::Ok) {
                    if (status == IGraphTransformer::TStatus::Repeat) {
                        auto newSettings = ctx.Expr.ChangeChild(*settings, i, std::move(newSetting));
                        output = ctx.Expr.ChangeChild(*input, TCoAggregateBase::idx_Settings, std::move(newSettings));
                    }
                    return status;
                }
                outputColumns = setting->ChildPtr(1);
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(setting->Head().Pos()),
                    TStringBuilder() << "Unexpected setting: " << settingName));
                return IGraphTransformer::TStatus::Error;
            }
        }

        for (auto& child : input->Child(1)->Children()) {
            if (!EnsureAtom(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (sessionColumnName == child->Content() || hoppingColumnName == child->Content()) {
                continue;
            }

            auto item = FindOrReportMissingMember(child->Content(), child->Pos(), *inputStructType, ctx.Expr);
            if (!item) {
                return IGraphTransformer::TStatus::Error;
            }

            auto columnType = inputStructType->GetItems()[*item]->GetItemType();
            if (!columnType->IsHashable() || !columnType->IsEquatable()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Expected hashable and equatable type for key column: " <<
                    child->Content() << ", but got: " << *columnType));
                return IGraphTransformer::TStatus::Error;
            }

            rowColumns.push_back(ctx.Expr.MakeType<TItemExprType>(child->Content(), columnType));
        }

        for (ui32 childIndex = 0; childIndex < input->Child(2)->ChildrenSize(); ++childIndex) {
            const auto& child = input->Child(2)->ChildRef(childIndex);
            if (!EnsureTupleMinSize(*child, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureTupleMaxSize(*child, 3, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!child->Head().IsAtom() && !child->Head().IsList()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Head().Pos()), TStringBuilder()
                    << "Expected atom or tuple, but got: " << child->Head().Type()));
                return IGraphTransformer::TStatus::Error;
            }

            if (child->Head().IsList()) {
                if (!EnsureTupleMinSize(child->Head(), 1, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                for (auto& outField : child->Head().Children()) {
                    if (!EnsureAtom(*outField, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }
            }

            const bool isAggApply = child->Child(1)->IsCallable({ "AggApply", "AggApplyState", "AggApplyManyState" });
            const bool isTraits = child->Child(1)->IsCallable("AggregationTraits");
            if (!isAggApply && !isTraits) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Child(1)->Pos()), "Expected aggregation traits"));
                return IGraphTransformer::TStatus::Error;
            }

            const TTypeAnnotationNode* distinctColumnType = nullptr;
            if (child->ChildrenSize() == 3) {
                if (suffix != "" && suffix != "Finalize") {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "DISTINCT aggregation is not supported for mode: " << suffix));
                    return IGraphTransformer::TStatus::Error;
                }

                if (!EnsureAtom(*child->Child(2), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto item = inputStructType->FindItem(child->Child(2)->Content());
                if (!item) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Unknown key column: " <<
                        child->Child(2)->Content() << ", type: " << *input->Head().GetTypeAnn()));
                    return IGraphTransformer::TStatus::Error;
                }

                distinctColumnType = inputStructType->GetItems()[*item]->GetItemType();
                if (!distinctColumnType->IsHashable() || !distinctColumnType->IsEquatable()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Expected hashable and equatable type for distinct column: " <<
                        child->Child(2)->Content() << ", but got: " << *distinctColumnType));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            if (suffix == "" || suffix.EndsWith("Finalize")) {
                auto finishType = isAggApply ? child->Child(1)->GetTypeAnn() : child->Child(1)->Child(6)->GetTypeAnn();
                bool isOptional = finishType->GetKind() == ETypeAnnotationKind::Optional;
                if (child->Head().IsList()) {
                    if (isOptional) {
                        finishType = finishType->Cast<TOptionalExprType>()->GetItemType();
                    }

                    const auto tupleType = finishType->Cast<TTupleExprType>();
                    if (tupleType->GetSize() != child->Head().ChildrenSize()) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Child(1)->Child(6)->Pos()),
                            TStringBuilder() << "Expected tuple type of size: " << child->Head().ChildrenSize() << ", but got: " << tupleType->GetSize()));
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (ui32 index = 0; index < tupleType->GetSize(); ++index) {
                        const auto item = tupleType->GetItems()[index];
                        rowColumns.push_back(ctx.Expr.MakeType<TItemExprType>(
                            child->Head().Child(index)->Content(),
                            (isOptional || (input->Child(1)->ChildrenSize() == 0 && !isHopping)) &&
                                !item->IsOptionalOrNull() ? ctx.Expr.MakeType<TOptionalExprType>(item) : item));
                    }
                } else {
                    if (input->Child(1)->ChildrenSize() == 0 && !isHopping) {
                        if (isTraits) {
                            // need to apply default value
                            auto defVal = child->Child(1)->ChildPtr(7);
                            if (!defVal->IsCallable("Null")) {
                                finishType = defVal->GetTypeAnn();
                            } else if (!finishType->IsOptionalOrNull()) {
                                finishType = ctx.Expr.MakeType<TOptionalExprType>(finishType);
                            }
                        } else {
                            auto name = child->Child(1)->Child(0)->Content();
                            if ((name == "count" || name == "count_all")) {
                                if (isOptional) {
                                    finishType = finishType->Cast<TOptionalExprType>()->GetItemType();
                                }
                            } else if (!finishType->IsOptionalOrNull()) {
                                finishType = ctx.Expr.MakeType<TOptionalExprType>(finishType);
                            }
                        }
                    }
                    rowColumns.push_back(ctx.Expr.MakeType<TItemExprType>(child->Head().Content(), finishType));
                }
            } else if (suffix == "Combine" || suffix == "CombineState" || suffix == "MergeState") {
                auto stateType = isAggApply ? AggApplySerializedStateType(child->ChildPtr(1), ctx.Expr) : child->Child(1)->Child(3)->GetTypeAnn();
                if (child->Head().IsList()) {
                    for (const auto& x : child->Head().Children()) {
                        rowColumns.push_back(ctx.Expr.MakeType<TItemExprType>(x->Content(), stateType));
                    }
                } else {
                    rowColumns.push_back(ctx.Expr.MakeType<TItemExprType>(child->Head().Content(), stateType));
                }
            } else {
                YQL_ENSURE(false, "Unknown aggregation mode: " << suffix);
            }
        }

        auto rowType = ctx.Expr.MakeType<TStructExprType>(rowColumns);
        if (!rowType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (outputColumns) {
            THashSet<TStringBuf> originalColumns;
            for (auto& item : rowColumns) {
                originalColumns.insert(item->GetName());
            }
            THashSet<TStringBuf> outputColumnNames;
            for (auto& col : outputColumns->ChildrenList()) {
                if (!originalColumns.contains(col->Content())) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(col->Pos()), TStringBuilder() << "Unknown output column " << col->Content()));
                    return IGraphTransformer::TStatus::Error;
                }
                outputColumnNames.insert(col->Content());
            }
            EraseIf(rowColumns, [&](const auto& item) { return !outputColumnNames.contains(item->GetName()); });
            if (rowColumns.size() == originalColumns.size()) {
                auto newSettings = RemoveSetting(*settings, "output_columns", ctx.Expr);
                output = ctx.Expr.ChangeChild(*input, TCoAggregateBase::idx_Settings, std::move(newSettings));
                return IGraphTransformer::TStatus::Repeat;
            }
            rowType = ctx.Expr.MakeType<TStructExprType>(rowColumns);
        }

        input->SetTypeAnn(MakeSequenceType(inputTypeKind, *rowType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AggOverStateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambda1 = input->ChildRef(0);
        auto status = ConvertToLambda(lambda1, ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambda2 = input->ChildRef(1);
        status = ConvertToLambda(lambda2, ctx.Expr, 0);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto root = lambda2->TailPtr();
        if (root->IsCallable("AggApply")) {
            if (!EnsureArgsCount(*root, 3, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            // extractor for state, not initial value itself
            output = ctx.Expr.Builder(input->Pos())
                .Callable("AggApplyState")
                    .Add(0, root->ChildPtr(0))
                    .Add(1, root->ChildPtr(1))
                    .Add(2, input->ChildPtr(0))
                    .Callable(3, "Void")
                    .Seal()
                .Seal()
                .Build();

            return IGraphTransformer::TStatus::Repeat;
        } else if (root->IsCallable("AggregationTraits")) {
            // make Void update handler
            if (!EnsureArgsCount(*root, 8, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            output = ctx.Expr.Builder(input->Pos())
                .Callable("AggregationTraits")
                    .Add(0, root->ChildPtr(0))
                    .Add(1, input->ChildPtr(0)) // extractor for state, not initial value itself
                    .Lambda(2)
                        .Param("item")
                        .Param("state")
                        .Callable("Void")
                        .Seal()
                    .Seal()
                    .Add(3, root->ChildPtr(3))
                    .Add(4, root->ChildPtr(4))
                    .Add(5, root->ChildPtr(5))
                    .Add(6, root->ChildPtr(6))
                    .Add(7, root->ChildPtr(7))
                .Seal()
                .Build();

            return IGraphTransformer::TStatus::Repeat;
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(root->Pos()), "Expected aggregation traits"));
            return IGraphTransformer::TStatus::Error;
        }
    }

    IGraphTransformer::TStatus SqlAggregateAllWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isStream;
        if (!EnsureSeqType(input->Head(), ctx.Expr, &isStream)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto inputItemType = isStream
            ? input->Head().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()
            : input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();

        if (!EnsureStructType(input->Head().Pos(), *inputItemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto inputStructType = inputItemType->Cast<TStructExprType>();
        for (auto& item : inputStructType->GetItems()) {
            auto columnName = item->GetName();
            auto columnType = item->GetItemType();
            if (!columnType->IsHashable() || !columnType->IsEquatable()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Expected hashable and equatable type for key column: " << columnName << ", but got: " << *columnType));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CountedAggregateAllWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isStream;
        if (!EnsureSeqType(input->Head(), ctx.Expr, &isStream)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto inputItemType = isStream
            ? input->Head().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()
            : input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();

        if (!EnsureStructType(input->Head().Pos(), *inputItemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        THashSet<TStringBuf> countedColumns;
        auto inputStructType = inputItemType->Cast<TStructExprType>();
        if (!EnsureTuple(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (auto child : input->Tail().Children()) {
            if (!EnsureAtom(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!inputStructType->FindItem(child->Content())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Unknown counted member: " << child->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            if (countedColumns.contains(child->Content())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Duplicated counted member: " << child->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            countedColumns.insert(child->Content());
        }

        TVector<const TItemExprType*> retItems;
        for (auto& item : inputStructType->GetItems()) {
            auto columnName = item->GetName();
            auto columnType = item->GetItemType();
            if (countedColumns.contains(columnName)) {
                if (!EnsureComputableType(input->Pos(), *columnType, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                retItems.push_back(ctx.Expr.MakeType<TItemExprType>(columnName, ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64)));
                continue;
            }

            if (!columnType->IsHashable() || !columnType->IsEquatable()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Expected hashable and equatable type for key column: " << columnName << ", but got: " << *columnType));
                return IGraphTransformer::TStatus::Error;
            }

            retItems.push_back(item);
        }

        auto retStruct = ctx.Expr.MakeType<TStructExprType>(retItems);
        if (isStream) {
            input->SetTypeAnn(ctx.Expr.MakeType<TStreamExprType>(retStruct));
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(retStruct));
        }
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AggApplyWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        const bool overState = input->Content().EndsWith("State");
        const bool isMany = input->Content().EndsWith("ManyState");
        if (overState) {
            if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto name = input->Child(0)->Content();
        if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        bool hasOriginalType = false;
        if (overState && !input->Child(3)->IsCallable("Void")) {
            if (auto status = EnsureTypeRewrite(input->ChildRef(3), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }

           hasOriginalType = true;
        }

        auto itemType = input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (itemType->GetKind() == ETypeAnnotationKind::Unit) {
            output = ctx.Expr.NewCallable(input->Pos(), "Void", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        auto& lambda = input->ChildRef(2);
        const auto status = ConvertToLambda(lambda, ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambda, { itemType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (name == "count" || name == "count_all") {
            const TTypeAnnotationNode* retType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
            if (overState) {
                if (!IsSameAnnotation(*lambda->GetTypeAnn(), *retType)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Mismatch count type, expected: " << *lambda->GetTypeAnn() << ", but got: " << *retType));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            input->SetTypeAnn(retType);
        } else if (name == "sum") {
            const TTypeAnnotationNode* retType;
            if (!GetSumResultType(input->Pos(), *lambda->GetTypeAnn(), retType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (overState) {
                if (!IsSameAnnotation(*lambda->GetTypeAnn(), *retType)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Mismatch sum type, expected: " << *lambda->GetTypeAnn() << ", but got: " << *retType));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            if (hasOriginalType) {
                auto originalExtractorType = input->Child(3)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                if (!ApplyOriginalType(input, isMany, originalExtractorType, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else {
                input->SetTypeAnn(retType);
            }
        } else if (name == "avg") {
            const TTypeAnnotationNode* retType;
            if (!overState) {
                if (!GetAvgResultType(input->Pos(), *lambda->GetTypeAnn(), retType, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else {
                if (!GetAvgResultTypeOverState(input->Pos(), *lambda->GetTypeAnn(), retType, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            }

            if (hasOriginalType) {
                auto originalExtractorType = input->Child(3)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                if (!ApplyOriginalType(input, isMany, originalExtractorType, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else {
                input->SetTypeAnn(retType);
            }
        } else if (name == "min" || name == "max") {
            const TTypeAnnotationNode* retType;
            if (!GetMinMaxResultType(input->Pos(), *lambda->GetTypeAnn(), retType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (overState) {
                if (!IsSameAnnotation(*lambda->GetTypeAnn(), *retType)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Mismatch min/max type, expected: " << *lambda->GetTypeAnn() << ", but got: " << *retType));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            input->SetTypeAnn(retType);
        } else if (name == "some") {
            input->SetTypeAnn(lambda->GetTypeAnn());
        } else if (name.StartsWith("pg_")) {
            if (lambda->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
                lambda = ConvertToMultiLambda(lambda, ctx.Expr);
                return IGraphTransformer::TStatus::Repeat;
            }

            auto func = name;
            func.SkipPrefix("pg_");
            TVector<ui32> argTypes;
            bool needRetype = false;
            if (auto status = ExtractPgTypesFromMultiLambda(lambda, argTypes, needRetype, ctx.Expr);
                status != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            if (overState && !hasOriginalType) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Partial aggregation of " << name << " is not supported"));
                return IGraphTransformer::TStatus::Error;
            }

            const TTypeAnnotationNode* originalResultType = nullptr;
            if (hasOriginalType) {
                auto originalExtractorType = input->Child(3)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                originalResultType = GetOriginalResultType(input->Pos(), isMany, originalExtractorType, ctx.Expr);
                if (!originalResultType) {
                    return IGraphTransformer::TStatus::Error;
                }
            }

            const NPg::TAggregateDesc* aggDescPtr;
            try {
                if (overState) {
                    if (argTypes.size() != 1) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                            "Expected only one argument"));
                        return IGraphTransformer::TStatus::Error;
                    }

                    aggDescPtr = &NPg::LookupAggregation(TString(func), argTypes[0], originalResultType->Cast<TPgExprType>()->GetId());
                } else {
                    aggDescPtr = &NPg::LookupAggregation(TString(func), argTypes);
                }
                if (aggDescPtr->Kind != NPg::EAggKind::Normal) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        "Only normal aggregation supported"));
                    return IGraphTransformer::TStatus::Error;
                }
            } catch (const yexception& e) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), e.what()));
                return IGraphTransformer::TStatus::Error;
            }

            if (overState) {
                input->SetTypeAnn(originalResultType);
            } else {
                const NPg::TAggregateDesc& aggDesc = *aggDescPtr;
                const auto& finalDesc = NPg::LookupProc(aggDesc.FinalFuncId ? aggDesc.FinalFuncId : aggDesc.TransFuncId);
                auto resultType = finalDesc.ResultType;
                AdjustReturnType(resultType, aggDesc.ArgTypes, 0, argTypes);
                input->SetTypeAnn(ctx.Expr.MakeType<TPgExprType>(resultType));
            }
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Unsupported agg name: " << name));
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AggBlockApplyWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        const bool overState = input->Content().EndsWith("State");
        if (!EnsureMinArgsCount(*input, overState ? 2 : 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* originalType = nullptr;
        if (overState && !input->Child(1)->IsCallable("Void")) {
            if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            originalType = input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        }

        auto name = input->Child(0)->Content();
        if (!name.StartsWith("pg_")) {
            ui32 expectedArgs;
            if (name == "count_all") {
                expectedArgs = overState ? 2 : 1;
            } else if (name == "count" || name == "sum" || name == "avg" || name == "min" || name == "max" || name == "some") {
                expectedArgs = 2;
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Unsupported agg name: " << name));
                return IGraphTransformer::TStatus::Error;
            }

            if (overState) {
                ++expectedArgs;
            }

            if (!EnsureArgsCount(*input, expectedArgs, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        for (ui32 i = overState ? 2 : 1; i < input->ChildrenSize(); ++i) {
            if (auto status = EnsureTypeRewrite(input->ChildRef(i), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        if (name == "count_all" || name == "count") {
            const TTypeAnnotationNode* retType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
            if (overState) {
                auto itemType = input->Child(2)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                if (!IsSameAnnotation(*itemType, *retType)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Mismatch count type, expected: " << *itemType << ", but got: " << *retType));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            input->SetTypeAnn(retType);
        } else if (name == "sum") {
            auto itemType = input->Child(overState ? 2 : 1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            const TTypeAnnotationNode* retType;
            if (!GetSumResultType(input->Pos(), *itemType, retType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (overState) {
                if (!IsSameAnnotation(*itemType, *retType)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Mismatch sum type, expected: " << *itemType << ", but got: " << *retType));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            input->SetTypeAnn(retType);
        } else if (name == "avg") {
            auto itemType = input->Child(overState ? 2 : 1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            const TTypeAnnotationNode* retType;
            if (!overState) {
                if (!GetAvgResultType(input->Pos(), *itemType, retType, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else {
                if (!GetAvgResultTypeOverState(input->Pos(), *itemType, retType, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            }

            input->SetTypeAnn(retType);
        } else if (name == "min" || name == "max") {
            auto itemType = input->Child(overState ? 2 : 1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            const TTypeAnnotationNode* retType;
            if (!GetMinMaxResultType(input->Pos(), *itemType, retType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (overState) {
                if (!IsSameAnnotation(*itemType, *retType)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Mismatch min/max type, expected: " << *itemType << ", but got: " << *retType));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            input->SetTypeAnn(retType);
        } else if (name == "some") {
            auto itemType = input->Child(overState ? 2 : 1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            const TTypeAnnotationNode* retType = itemType;
            input->SetTypeAnn(retType);
        } else if (name.StartsWith("pg_")) {
            auto func = name.SubStr(3);
            TVector<ui32> argTypes;
            for (ui32 i = 1 + (overState ? 1 : 0); i < input->ChildrenSize(); ++i) {
                argTypes.push_back(input->Child(i)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TPgExprType>()->GetId());
            }

            const NPg::TAggregateDesc* aggDescPtr;
            if (overState) {
                YQL_ENSURE(argTypes.size() == 1);
                if (!originalType) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Partial aggreation is not supported for: " << name));
                    return IGraphTransformer::TStatus::Error;
                }

                auto resultType = originalType->Cast<TPgExprType>()->GetId();
                aggDescPtr = &NPg::LookupAggregation(TString(func), argTypes[0], resultType);
            } else {
                aggDescPtr = &NPg::LookupAggregation(TString(func), argTypes);
            }

            if (overState) {
                input->SetTypeAnn(originalType);
            } else {
                auto stateType = NPg::LookupProc(aggDescPtr->SerializeFuncId ? aggDescPtr->SerializeFuncId : aggDescPtr->TransFuncId).ResultType;
                input->SetTypeAnn(ctx.Expr.MakeType<TPgExprType>(stateType));
            }
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Unsupported agg name: " << name));
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FilterNullMembersWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 1, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStructType(input->Head().Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto originalStructType = itemType->Cast<TStructExprType>();

        std::unordered_map<std::string_view, const TTypeAnnotationNode*> columnTypes;
        columnTypes.reserve(originalStructType->GetSize());
        for (const auto& x : originalStructType->GetItems()) {
            columnTypes.emplace(x->GetName(), x->GetItemType());
        }

        if (input->ChildrenSize() < 2U) {
            bool hasOptional = false;
            for (auto& column : columnTypes) {
                if (column.second->GetKind() == ETypeAnnotationKind::Optional) {
                    hasOptional = true;
                    column.second = column.second->Cast<TOptionalExprType>()->GetItemType();
                }
            }

            if (!hasOptional) {
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            }
        } else {
            const auto status = NormalizeTupleOfAtoms(input, 1, output, ctx.Expr);
            if (status != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            TExprNode::TListType reducedMembers;
            const auto& members = input->Tail();
            reducedMembers.reserve(members.ChildrenSize());
            for (const auto& child : members.Children()) {
                const auto& name = child->Content();
                const auto it = columnTypes.find(name);
                if (columnTypes.cend() == it) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(members.Pos()), TStringBuilder() << "Unknown column: " << name));
                    return IGraphTransformer::TStatus::Error;
                }

                auto& columnType = it->second;
                if (columnType->GetKind() == ETypeAnnotationKind::Optional) {
                    reducedMembers.emplace_back(child);
                    columnType = columnType->Cast<TOptionalExprType>()->GetItemType();
                }
            }

            if (reducedMembers.empty()) {
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            } else if (reducedMembers.size() != members.ChildrenSize()) {
                output = ctx.Expr.ChangeChild(*input, 1, ctx.Expr.NewList(members.Pos(), std::move(reducedMembers)));
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        TVector<const TItemExprType*> items;
        for (auto& pair : columnTypes) {
            items.push_back(ctx.Expr.MakeType<TItemExprType>(pair.first, pair.second));
        }

        const auto newStructType = ctx.Expr.MakeType<TStructExprType>(items);
        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *newStructType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SkipNullMembersWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 1, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStructType(input->Head().Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto originalStructType = itemType->Cast<TStructExprType>();

        std::unordered_map<std::string_view, const TTypeAnnotationNode*> columnTypes;
        columnTypes.reserve(originalStructType->GetSize());
        for (const auto& x : originalStructType->GetItems()) {
            columnTypes.emplace(x->GetName(), x->GetItemType());
        }

        if (input->ChildrenSize() < 2U) {
            bool hasOptional = false;
            for (const auto& column : columnTypes) {
                if (column.second->GetKind() == ETypeAnnotationKind::Optional) {
                    hasOptional = true;
                    break;
                }
            }

            if (!hasOptional) {
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            }
        } else {
            const auto status = NormalizeTupleOfAtoms(input, 1, output, ctx.Expr);
            if (status != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            TExprNode::TListType reducedMembers;
            const auto members = input->Child(1);
            reducedMembers.reserve(members->ChildrenSize());
            for (const auto& child : members->Children()) {
                const auto& name = child->Content();

                const auto it = columnTypes.find(name);
                if (columnTypes.cend() == it) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(members->Pos()), TStringBuilder() << "Unknown column: " << name));
                    return IGraphTransformer::TStatus::Error;
                }

                if (it->second->GetKind() == ETypeAnnotationKind::Optional) {
                    reducedMembers.emplace_back(child);
                }
            }

            if (reducedMembers.empty()) {
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            } else if (reducedMembers.size() != members->ChildrenSize()) {
                output = ctx.Expr.ChangeChild(*input, 1, ctx.Expr.NewList(members->Pos(), std::move(reducedMembers)));
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FilterNullElementsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 1, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleType(input->Head().Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto originalTupleType = itemType->Cast<TTupleExprType>();
        auto columnTypes = originalTupleType->GetItems();

        if (input->ChildrenSize() < 2U) {
            bool hasOptional = false;
            for (auto& column : columnTypes) {
                if (column->GetKind() == ETypeAnnotationKind::Optional) {
                    hasOptional = true;
                    column = column->Cast<TOptionalExprType>()->GetItemType();
                }
            }

            if (!hasOptional) {
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            }
        } else {
            const auto status = NormalizeTupleOfAtoms(input, 1, output, ctx.Expr);
            if (status != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            TExprNode::TListType reducedElements;
            const auto elements = input->Child(1);
            reducedElements.reserve(elements->ChildrenSize());
            for (const auto& child : elements->Children()) {
                ui32 index = 0U;
                if (!TryFromString(child->Content(), index)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Failed to convert to integer: " << child->Content()));
                    return IGraphTransformer::TStatus::Error;
                }

                if (index >= columnTypes.size()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Index out of range. Index: " <<
                        index << ", size: " << columnTypes.size()));
                    return IGraphTransformer::TStatus::Error;
                }

                auto& columnType = columnTypes[index];
                if (columnType->GetKind() == ETypeAnnotationKind::Optional) {
                    reducedElements.emplace_back(child);
                    columnType = columnType->Cast<TOptionalExprType>()->GetItemType();
                }
            }

            if (reducedElements.empty()) {
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            } else if (reducedElements.size() != elements->ChildrenSize()) {
                output = ctx.Expr.ChangeChild(*input, 1, ctx.Expr.NewList(elements->Pos(), std::move(reducedElements)));
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        const auto newTupleType = ctx.Expr.MakeType<TTupleExprType>(columnTypes);
        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *newTupleType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SkipNullElementsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 1, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleType(input->Head().Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto originalTupleType = itemType->Cast<TTupleExprType>();
        const auto& columnTypes = originalTupleType->GetItems();

        if (input->ChildrenSize() < 2U) {
            bool hasOptional = false;
            for (const auto& column : columnTypes) {
                if (column->GetKind() == ETypeAnnotationKind::Optional) {
                    hasOptional = true;
                    break;
                }
            }

            if (!hasOptional) {
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            }
        } else {
            const auto status = NormalizeTupleOfAtoms(input, 1, output, ctx.Expr);
            if (status != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            TExprNode::TListType reducedElements;
            const auto elements = input->Child(1);
            reducedElements.reserve(elements->ChildrenSize());
            for (const auto& child : elements->Children()) {
                ui32 index = 0U;
                if (!TryFromString(child->Content(), index)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Failed to convert to integer: " << child->Content()));
                    return IGraphTransformer::TStatus::Error;
                }

                if (index >= columnTypes.size()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Index out of range. Index: " <<
                        index << ", size: " << columnTypes.size()));
                    return IGraphTransformer::TStatus::Error;
                }

                if (columnTypes[index]->GetKind() == ETypeAnnotationKind::Optional) {
                    reducedElements.emplace_back(child);
                }
            }

            if (reducedElements.empty()) {
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            } else if (reducedElements.size() != elements->ChildrenSize()) {
                output = ctx.Expr.ChangeChild(*input, 1, ctx.Expr.NewList(elements->Pos(), std::move(reducedElements)));
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus WinOnWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto frameSettingsNode = input->ChildPtr(0);
        if (frameSettingsNode->IsList()) {
            TExprNode::TPtr normalizedFrameSettings;
            auto status = NormalizeKeyValueTuples(frameSettingsNode, 0, normalizedFrameSettings, ctx.Expr);
            if (status.Level == IGraphTransformer::TStatus::Repeat) {
                output = ctx.Expr.ChangeChild(*input, 0, std::move(normalizedFrameSettings));
                return status;
            }

            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        auto frameSettings = TWindowFrameSettings::TryParse(*input, ctx.Expr);
        if (!frameSettings) {
            return IGraphTransformer::TStatus::Error;
        }

        auto frameType = frameSettings->GetFrameType();
        if (frameType == EFrameType::FrameByGroups) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "GROUPS in frame specification are not supported yet"));
            return IGraphTransformer::TStatus::Error;
        }

        if (frameType == EFrameType::FrameByRange) {
            // only UNBOUNDED PRECEDING -> CURRENT ROW is currently supported
            if (!(IsUnbounded(frameSettings->GetFirst()) && IsCurrentRow(frameSettings->GetLast()))) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "RANGE in frame specification is not supported yet"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        auto status = NormalizeKeyValueTuples(input, 1, output, ctx.Expr);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
            if (!EnsureTupleSize(*input->Child(i), 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            auto currColumn = input->Child(i)->Child(0)->Content();
            auto calcSpec = input->Child(i)->Child(1);
            if (!calcSpec->IsCallable({"WindowTraits", "Lag", "Lead", "RowNumber", "Rank", "DenseRank", "PercentRank", "CumeDist", "NTile", "Void"})) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(calcSpec->Pos()),
                                         "Invalid traits or special function for calculation on window"));
                return IGraphTransformer::TStatus::Error;
            }

            if (i + 1 < input->ChildrenSize()) {
                auto nextColumn = input->Child(i + 1)->Child(0)->Content();
                if (currColumn == nextColumn) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i + 1)->Child(0)->Pos()),
                                             TStringBuilder() << "Duplicate column '" << nextColumn << "'"));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ToWindowTraitsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Head().IsCallable("AggregationTraits")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                "Expected AggregationTraits"));
            return IGraphTransformer::TStatus::Error;
        }

        const auto& traits = input->Head();
        output = ExpandToWindowTraits(traits, ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus WindowTraitsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 6, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto& item = input->ChildRef(0);
        auto& lambdaInit = input->ChildRef(1);
        auto& lambdaUpdate = input->ChildRef(2);
        auto& lambdaShift = input->ChildRef(3);
        auto& lambdaCurrent = input->ChildRef(4);

        if (auto status = EnsureTypeRewrite(item, ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto itemType = item->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (itemType->GetKind() == ETypeAnnotationKind::Unit) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
            output = ctx.Expr.NewCallable(input->Pos(), "Void", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        auto status = ConvertToLambda(lambdaInit, ctx.Expr, 1, 2);
        status = status.Combine(ConvertToLambda(lambdaUpdate, ctx.Expr, 2, 3));
        status = status.Combine(ConvertToLambda(lambdaShift, ctx.Expr, 2, 3));
        status = status.Combine(ConvertToLambda(lambdaCurrent, ctx.Expr, 1));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!EnsureComputableType(item->Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto ui32Type = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint32);
        if (lambdaInit->Head().ChildrenSize() == 1U) {
            if (!UpdateLambdaAllArgumentsTypes(lambdaInit, {itemType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!UpdateLambdaAllArgumentsTypes(lambdaInit, {itemType, ui32Type}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!lambdaInit->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto stateType = lambdaInit->GetTypeAnn();
        if (!EnsureComputableType(lambdaInit->Pos(), *stateType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (lambdaUpdate->Head().ChildrenSize() == 2U) {
            if (!UpdateLambdaAllArgumentsTypes(lambdaUpdate, {itemType, stateType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!UpdateLambdaAllArgumentsTypes(lambdaUpdate, {itemType, stateType, ui32Type}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!lambdaUpdate->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*lambdaUpdate->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaUpdate->Pos()), TStringBuilder() << "Mismatch update lambda result type, expected: "
                << *stateType << ", but got: " << *lambdaUpdate->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (lambdaShift->Head().ChildrenSize() == 2U) {
            if (!UpdateLambdaAllArgumentsTypes(lambdaShift, {itemType, stateType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!UpdateLambdaAllArgumentsTypes(lambdaShift, {itemType, stateType, ui32Type}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!lambdaShift->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (lambdaShift->Child(1)->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Void && !IsSameAnnotation(*lambdaShift->GetTypeAnn(), *stateType)) {
            bool error = false;
            if (stateType->GetKind() == ETypeAnnotationKind::Struct && lambdaShift->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Struct) {
                auto stateStruct = stateType->Cast<TStructExprType>();
                auto shiftStruct = lambdaShift->GetTypeAnn()->Cast<TStructExprType>();
                if (stateStruct->GetSize() == shiftStruct->GetSize()) {
                    for (ui32 i = 0; i < stateStruct->GetSize(); ++i) {
                        auto stateItem = stateStruct->GetItems()[i];
                        auto shiftItem = shiftStruct->GetItems()[i];
                        if (stateItem->GetName() != shiftItem->GetName()) {
                            error = true;
                            break;
                        }

                        if (shiftItem->GetItemType()->GetKind() != ETypeAnnotationKind::Void &&
                            !IsSameAnnotation(*shiftItem->GetItemType(), *stateItem->GetItemType())) {
                            error = true;
                            break;
                        }
                    }
                } else {
                    error = true;
                }
            }

            if (error) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaShift->Pos()), TStringBuilder() << "Mismatch shift lambda result type, expected void or : "
                    << *stateType << ", but got: " << *lambdaShift->GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaCurrent, {stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaCurrent->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto currentType = lambdaCurrent->GetTypeAnn();
        if (!EnsureComputableType(lambdaCurrent->Pos(), *currentType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        status = ValidateTraitsDefaultValue(input, 5, *currentType, "current", output, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CalcOverWindowWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        const bool isSession = input->IsCallable("CalcOverSessionWindow");
        if (isSession && input->ChildrenSize() == 5) {
            auto children = input->ChildrenList();
            children.push_back(ctx.Expr.NewList(input->Pos(), {}));
            output = ctx.Expr.ChangeChildren(*input, std::move(children));
            return IGraphTransformer::TStatus::Repeat;
        }
        if (!EnsureArgsCount(*input, isSession ? 6 : 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto inputListType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (!EnsureStructType(input->Head().Pos(), *inputListType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = NormalizeTupleOfAtoms(input, 1, output, ctx.Expr);
        if (isSession) {
            status = status.Combine(NormalizeTupleOfAtoms<false>(input, 5, output, ctx.Expr));
        }

        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto inputStructType = inputListType->Cast<TStructExprType>();
        TVector<const TItemExprType*> outputColumns = inputStructType->GetItems();

        status = ValidateCalcOverWindowArgs(outputColumns, *inputStructType, *input->Child(1), *input->Child(2), *input->Child(3),
            isSession ? input->ChildPtr(4) : TExprNode::TPtr(), isSession ? input->ChildPtr(5) : TExprNode::TPtr(), ctx.Expr);

        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto rowType = ctx.Expr.MakeType<TStructExprType>(outputColumns);
        if (!rowType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(rowType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CalcOverWindowGroupWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto inputListType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (!EnsureStructType(input->Head().Pos(), *inputListType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& calcs = input->ChildRef(1);
        if (!EnsureTuple(*calcs, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        IGraphTransformer::TStatus status = IGraphTransformer::TStatus::Ok;
        TExprNodeList calcsList = calcs->ChildrenList();
        for (auto& calc : calcsList) {
            if (!EnsureTupleMinSize(*calc, 3, ctx.Expr) || !EnsureTupleMaxSize(*calc, 5, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (calc->ChildrenSize() < 5) {
                auto calcItems = calc->ChildrenList();
                if (calc->ChildrenSize() == 3) {
                    calcItems.push_back(ctx.Expr.NewCallable(calc->Pos(), "Void", {}));
                }
                if (calc->ChildrenSize() == 4) {
                    calcItems.push_back(ctx.Expr.NewList(calc->Pos(), {}));
                }

                calc = ctx.Expr.ChangeChildren(*calc, std::move(calcItems));
                status = status.Combine(IGraphTransformer::TStatus::Repeat);
            } else {
                status = status.Combine(NormalizeTupleOfAtoms<false>(calc, TCoCalcOverWindowTuple::idx_SessionColumns, calc, ctx.Expr));
            }

            if (status.Level == IGraphTransformer::TStatus::Error) {
                return status;
            }
        }

        if (status.Level != IGraphTransformer::TStatus::Ok) {
            if (status.Level == IGraphTransformer::TStatus::Repeat) {
                calcs = ctx.Expr.NewList(calcs->Pos(), std::move(calcsList));
            }
            return status;
        }

        auto inputStructType = inputListType->Cast<TStructExprType>();
        TVector<const TItemExprType*> outputColumns = inputStructType->GetItems();

        for (auto& calc : calcs->Children()) {
            status = ValidateCalcOverWindowArgs(outputColumns, *inputStructType, *calc->Child(0), *calc->Child(1), *calc->Child(2),
                calc->ChildPtr(3), calc->ChildPtr(4), ctx.Expr);

            if (status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        auto rowType = ctx.Expr.MakeType<TStructExprType>(outputColumns);
        if (!rowType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(rowType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus WinLeadLagWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr) || !EnsureMaxArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        const auto argCount = input->ChildrenSize();
        auto& lambdaValue = input->ChildRef(1);
        auto winOffsetType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64);

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }
        auto rowListType = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (rowListType->GetKind() == ETypeAnnotationKind::EmptyList) {
            output = ctx.Expr.NewCallable(input->Pos(), "Void", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(input->Head().Pos(), *rowListType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto rowType = rowListType->Cast<TListExprType>()->GetItemType();
        if (!EnsureStructType(input->Head().Pos(), *rowType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(lambdaValue, ctx.Expr, 1);
        if (argCount > 2) {
            status = status.Combine(TryConvertTo(input->ChildRef(2), *winOffsetType, ctx.Expr));
        }
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }
        if (!UpdateLambdaAllArgumentsTypes(lambdaValue, {rowType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambdaValue->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        const bool isOptional = lambdaValue->GetTypeAnn()->IsOptionalOrNull();
        input->SetTypeAnn(isOptional ? lambdaValue->GetTypeAnn() : ctx.Expr.MakeType<TOptionalExprType>(lambdaValue->GetTypeAnn()));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus WinRowNumberWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }
        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus WinCumeDistWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto options = input->Child(1);
        if (!EnsureTuple(*options, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (const auto& option : options->Children()) {
            if (!EnsureTupleSize(*option, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(option->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            const auto optionName = option->Head().Content();
            static const THashSet<TStringBuf> supportedOptions =
                {"ansi"};
            if (!supportedOptions.contains(optionName)) {
                ctx.Expr.AddError(
                    TIssue(ctx.Expr.GetPosition(option->Pos()),
                        TStringBuilder() << "Unknown " << input->Content() << "option '" << optionName));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Double));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus WinNTileWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64);
        auto status = TryConvertTo(input->ChildRef(1), *expectedType, ctx.Expr);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus WinRankWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambdaValue = input->ChildRef(1);
        auto options = input->Child(2);

        if (!EnsureTuple(*options, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (const auto& option : options->Children()) {
            if (!EnsureTupleSize(*option, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(option->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            const auto optionName = option->Head().Content();
            static const THashSet<TStringBuf> supportedOptions =
                {"ansi", "warnNoAnsi"};
            if (!supportedOptions.contains(optionName)) {
                ctx.Expr.AddError(
                    TIssue(ctx.Expr.GetPosition(option->Pos()),
                        TStringBuilder() << "Unknown " << input->Content() << "option '" << optionName));
                return IGraphTransformer::TStatus::Error;
            }
        }

        auto rowListType = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (rowListType->GetKind() == ETypeAnnotationKind::EmptyList) {
            output = ctx.Expr.NewCallable(input->Pos(), "Void", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(input->Head().Pos(), *rowListType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto rowType = rowListType->Cast<TListExprType>()->GetItemType();
        if (!EnsureStructType(input->Head().Pos(), *rowType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(lambdaValue, ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaValue, {rowType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto keyType = lambdaValue->GetTypeAnn();
        if (!keyType) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureEquatableType(lambdaValue->Pos(), *keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isAnsi = HasSetting(*options, "ansi");
        if (!isAnsi && keyType->HasOptionalOrNull() && keyType->GetKind() != ETypeAnnotationKind::Optional) {
            // this case is just broken in legacy RANK()/DENSE_RANK()
            ctx.Expr.AddError(
                TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Legacy " << input->Content() << " does not support key type " << *keyType << ".\n"
                                     << "Please use PRAGMA AnsiRankForNullableKeys;"));
            return IGraphTransformer::TStatus::Error;
        }

        if (HasSetting(*options, "warnNoAnsi")) {
            if (!isAnsi && keyType->HasOptionalOrNull()) {
                auto issue = TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << input->Content() << " will not produce ANSI-compliant result here.\n"
                                     << "Consider adding 'PRAGMA AnsiRankForNullableKeys;' or \n"
                                     << "'PRAGMA DisableAnsiRankForNullableKeys;' to keep current behavior and silence this warning");
                SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_LEGACY_RANK_FOR_NULLABLE_KEYS, issue);
                if (!ctx.Expr.AddWarning(issue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            }

            output = ctx.Expr.ChangeChild(*input, 2, RemoveSetting(*options, "warnNoAnsi", ctx.Expr));
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* outputType = ctx.Expr.MakeType<TDataExprType>(input->IsCallable("PercentRank") ?
            EDataSlot::Double : EDataSlot::Uint64);
        if (!isAnsi && keyType->GetKind() == ETypeAnnotationKind::Optional) {
            outputType = ctx.Expr.MakeType<TOptionalExprType>(outputType);
        }

        input->SetTypeAnn(outputType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus HoppingTraitsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 7, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto itemType = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureComputableType(input->Head().Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambdaTimeExtractor = input->ChildRef(1);
        auto status = ConvertToLambda(lambdaTimeExtractor, ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaTimeExtractor, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambdaTimeExtractor->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureSpecificDataType(*lambdaTimeExtractor, EDataSlot::Timestamp, ctx.Expr, true)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* intervalType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Interval);

        auto checkWindowParam = [&] (TExprNode::TPtr& param) -> IGraphTransformer::TStatus {
            auto type = param->GetTypeAnn();
            if (type->GetKind() == ETypeAnnotationKind::Optional) {
                if (param->IsCallable("Nothing")) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(param->Pos()), "Hopping window parameter value cannot be evaluated"));
                    return IGraphTransformer::TStatus::Error;
                }
                type = type->Cast<TOptionalExprType>()->GetItemType();
            }
            if (!IsSameAnnotation(*type, *intervalType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(param->Pos()), TStringBuilder()
                    << "Mismatch hopping window parameter type, expected: "
                    << *intervalType << ", but got: " << *type));
                return IGraphTransformer::TStatus::Error;
            }
            if (!IsPureIsolatedLambda(*param)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(param->Pos()), "Parameter is not a pure expression"));
                return IGraphTransformer::TStatus::Error;
            }
            return IGraphTransformer::TStatus::Ok;
        };

        auto convertStatus = checkWindowParam(input->ChildRef(2));
        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }
        convertStatus = checkWindowParam(input->ChildRef(3));
        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }
        convertStatus = checkWindowParam(input->ChildRef(4));
        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        const auto& dataWatermarksNodePtr = input->ChildRef(5);
        if (dataWatermarksNodePtr->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Unit) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(dataWatermarksNodePtr->Pos()), TStringBuilder()
                << "Expected unit type, but got: "
                << *dataWatermarksNodePtr->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus HoppingCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 11, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& item = input->ChildRef(0);

        auto& lambdaTimeExtractor = input->ChildRef(1);
        auto* hop = input->Child(2);
        auto* interval = input->Child(3);
        auto* delay = input->Child(4);

        auto& lambdaInit = input->ChildRef(5);
        auto& lambdaUpdate = input->ChildRef(6);
        auto& saveLambda = input->ChildRef(7);
        auto& loadLambda = input->ChildRef(8);
        auto& lambdaMerge = input->ChildRef(9);
        auto& lambdaFinish = input->ChildRef(10);

        if (!EnsureStreamType(*item, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(lambdaTimeExtractor, ctx.Expr, 1);
        status = status.Combine(ConvertToLambda(lambdaInit, ctx.Expr, 1));
        status = status.Combine(ConvertToLambda(lambdaUpdate, ctx.Expr, 2));
        status = status.Combine(ConvertToLambda(lambdaMerge, ctx.Expr, 2));
        status = status.Combine(ConvertToLambda(lambdaFinish, ctx.Expr, 2));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto timeType = ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Timestamp));
        auto itemType = item->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();

        if (!EnsureStructType(input->Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaTimeExtractor, {itemType}, ctx.Expr))
        {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambdaTimeExtractor->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureSpecificDataType(*hop, EDataSlot::Interval, ctx.Expr, true)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureSpecificDataType(*interval, EDataSlot::Interval, ctx.Expr, true)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureSpecificDataType(*delay, EDataSlot::Interval, ctx.Expr, true)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaInit, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambdaInit->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto stateType = lambdaInit->GetTypeAnn();
        if (!EnsureComputableType(lambdaInit->Pos(), *stateType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaUpdate, {itemType, stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambdaUpdate->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*lambdaUpdate->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaUpdate->Pos()), TStringBuilder() << "Mismatch update lambda result type, expected: "
                << *stateType << ", but got: " << *lambdaUpdate->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaMerge, {stateType, stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambdaMerge->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*lambdaMerge->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaMerge->Pos()), TStringBuilder() << "Mismatch merge lambda result type, expected: "
                << *stateType << ", but got: " << *lambdaMerge->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaFinish, {stateType, timeType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambdaFinish->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        if (lambdaFinish->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Struct) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaFinish->Pos()), TStringBuilder() <<
                "Expected struct type as finish lambda result type, but got: " << *lambdaMerge->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (saveLambda->IsCallable("Void") != loadLambda->IsCallable("Void")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(saveLambda->Pos()), TStringBuilder() <<
                "Save and load lambdas must be specified at the same time"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!saveLambda->IsCallable("Void")) {
            auto status = ConvertToLambda(saveLambda, ctx.Expr, 1);
            status = status.Combine(ConvertToLambda(loadLambda, ctx.Expr, 1));
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            if (!UpdateLambdaAllArgumentsTypes(saveLambda, {stateType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!saveLambda->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            auto savedStateType = saveLambda->GetTypeAnn();
            if (!EnsurePersistableType(saveLambda->Pos(), *savedStateType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!UpdateLambdaAllArgumentsTypes(loadLambda, {savedStateType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!loadLambda->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!IsSameAnnotation(*loadLambda->GetTypeAnn(), *stateType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(loadLambda->Pos()), TStringBuilder() << "Mismatch of load lambda return type and state type, "
                    << *loadLambda->GetTypeAnn() << " != " << *stateType));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TStreamExprType>(lambdaFinish->GetTypeAnn()));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus MultiHoppingCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 14, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto& item = input->ChildRef(0);
        auto& lambdaKeyExtractor = input->ChildRef(1);

        auto& lambdaTimeExtractor = input->ChildRef(2);
        auto* hop = input->Child(3);
        auto* interval = input->Child(4);
        auto* delay = input->Child(5);

        auto& lambdaInit = input->ChildRef(7);
        auto& lambdaUpdate = input->ChildRef(8);
        auto& saveLambda = input->ChildRef(9);
        auto& loadLambda = input->ChildRef(10);
        auto& lambdaMerge = input->ChildRef(11);
        auto& lambdaFinish = input->ChildRef(12);

        if (!EnsureStreamType(*item, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(lambdaKeyExtractor, ctx.Expr, 1);
        status = status.Combine(ConvertToLambda(lambdaTimeExtractor, ctx.Expr, 1));
        status = status.Combine(ConvertToLambda(lambdaInit, ctx.Expr, 1));
        status = status.Combine(ConvertToLambda(lambdaUpdate, ctx.Expr, 2));
        status = status.Combine(ConvertToLambda(lambdaMerge, ctx.Expr, 2));
        status = status.Combine(ConvertToLambda(lambdaFinish, ctx.Expr, 3));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto timeType = ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Timestamp));
        auto itemType = item->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();

        if (!EnsureStructType(input->Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaKeyExtractor, {itemType}, ctx.Expr))
        {
            return IGraphTransformer::TStatus::Error;
        }
        auto keyType = lambdaKeyExtractor->GetTypeAnn();
        if (!keyType) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaTimeExtractor, {itemType}, ctx.Expr))
        {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambdaTimeExtractor->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureSpecificDataType(*hop, EDataSlot::Interval, ctx.Expr, true)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureSpecificDataType(*interval, EDataSlot::Interval, ctx.Expr, true)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureSpecificDataType(*delay, EDataSlot::Interval, ctx.Expr, true)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaInit, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambdaInit->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto stateType = lambdaInit->GetTypeAnn();
        if (!EnsureComputableType(lambdaInit->Pos(), *stateType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaUpdate, {itemType, stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambdaUpdate->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*lambdaUpdate->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaUpdate->Pos()), TStringBuilder() << "Mismatch update lambda result type, expected: "
                << *stateType << ", but got: " << *lambdaUpdate->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaMerge, {stateType, stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambdaMerge->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*lambdaMerge->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaMerge->Pos()), TStringBuilder() << "Mismatch merge lambda result type, expected: "
                << *stateType << ", but got: " << *lambdaMerge->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaFinish, {keyType, stateType, timeType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!lambdaFinish->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        if (lambdaFinish->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Struct) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambdaFinish->Pos()), TStringBuilder() <<
                "Expected struct type as finish lambda result type, but got: " << *lambdaMerge->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (saveLambda->IsCallable("Void") != loadLambda->IsCallable("Void")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(saveLambda->Pos()), TStringBuilder() <<
                "Save and load lambdas must be specified at the same time"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!saveLambda->IsCallable("Void")) {
            auto status = ConvertToLambda(saveLambda, ctx.Expr, 1);
            status = status.Combine(ConvertToLambda(loadLambda, ctx.Expr, 1));
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            if (!UpdateLambdaAllArgumentsTypes(saveLambda, {stateType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!saveLambda->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            auto savedStateType = saveLambda->GetTypeAnn();
            if (!EnsurePersistableType(saveLambda->Pos(), *savedStateType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!UpdateLambdaAllArgumentsTypes(loadLambda, {savedStateType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!loadLambda->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!IsSameAnnotation(*loadLambda->GetTypeAnn(), *stateType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(loadLambda->Pos()), TStringBuilder() << "Mismatch of load lambda return type and state type, "
                    << *loadLambda->GetTypeAnn() << " != " << *stateType));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TStreamExprType>(lambdaFinish->GetTypeAnn()));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CombineCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinMaxArgsCount(*input, 5U, 6U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false, false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (TCoCombineCore::idx_MemLimit < input->ChildrenSize()) {
            if (!EnsureAtom(*input->Child(TCoCombineCore::idx_MemLimit), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            ui64 memLimit = 0ULL;
            if (!TryFromString(input->Child(TCoCombineCore::idx_MemLimit)->Content(), memLimit)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(TCoCombineCore::idx_MemLimit)->Pos()), TStringBuilder() <<
                    "Bad memLimit value: " << input->Child(TCoCombineCore::idx_MemLimit)->Content()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        TExprNode::TPtr& keyExtractor = input->ChildRef(TCoCombineCore::idx_KeyExtractor);
        TExprNode::TPtr& initHandler = input->ChildRef(TCoCombineCore::idx_InitHandler);
        TExprNode::TPtr& updateHandler = input->ChildRef(TCoCombineCore::idx_UpdateHandler);
        TExprNode::TPtr& finishHandler = input->ChildRef(TCoCombineCore::idx_FinishHandler);

        auto status = ConvertToLambda(keyExtractor, ctx.Expr, 1);
        status = status.Combine(ConvertToLambda(initHandler, ctx.Expr, 2));
        status = status.Combine(ConvertToLambda(updateHandler, ctx.Expr, 3));
        status = status.Combine(ConvertToLambda(finishHandler, ctx.Expr, 2));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        // keyExtractor
        if (!UpdateLambdaAllArgumentsTypes(keyExtractor, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!keyExtractor->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        auto keyType = keyExtractor->GetTypeAnn();
        if (!EnsureHashableKey(keyExtractor->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureEquatableKey(keyExtractor->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // initHandler
        if (!UpdateLambdaAllArgumentsTypes(initHandler, {keyType, itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!initHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        auto stateType = initHandler->GetTypeAnn();
        if (!EnsureComputableType(initHandler->Pos(), *stateType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // updateHandler
        if (!UpdateLambdaAllArgumentsTypes(updateHandler, {keyType, itemType, stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!updateHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        if (!IsSameAnnotation(*updateHandler->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(updateHandler->Pos()), "Mismatch of update lambda return type and state type"));
            return IGraphTransformer::TStatus::Error;
        }

        // finishHandler
        if (!UpdateLambdaAllArgumentsTypes(finishHandler, {keyType, stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!finishHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        auto finishType = finishHandler->GetTypeAnn();
        if (!EnsureSeqOrOptionalType(finishHandler->Pos(), *finishType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto retKind = finishType->GetKind();
        const TTypeAnnotationNode* retItemType = nullptr;
        if (retKind == ETypeAnnotationKind::List) {
            retItemType = finishType->Cast<TListExprType>()->GetItemType();
        } else if (retKind == ETypeAnnotationKind::Optional) {
            retItemType = finishType->Cast<TOptionalExprType>()->GetItemType();
        } else {
            retItemType = finishType->Cast<TStreamExprType>()->GetItemType();
        }

        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *retItemType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CombineCoreWithSpillingWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinMaxArgsCount(*input, 6U, 7U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false, false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (TCoCombineCoreWithSpilling::idx_MemLimit < input->ChildrenSize()) {
            if (!EnsureAtom(*input->Child(TCoCombineCoreWithSpilling::idx_MemLimit), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            ui64 memLimit = 0ULL;
            if (!TryFromString(input->Child(TCoCombineCoreWithSpilling::idx_MemLimit)->Content(), memLimit)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(TCoCombineCoreWithSpilling::idx_MemLimit)->Pos()), TStringBuilder() <<
                    "Bad memLimit value: " << input->Child(TCoCombineCoreWithSpilling::idx_MemLimit)->Content()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        TExprNode::TPtr& keyExtractor = input->ChildRef(TCoCombineCoreWithSpilling::idx_KeyExtractor);
        TExprNode::TPtr& initHandler = input->ChildRef(TCoCombineCoreWithSpilling::idx_InitHandler);
        TExprNode::TPtr& updateHandler = input->ChildRef(TCoCombineCoreWithSpilling::idx_UpdateHandler);
        TExprNode::TPtr& finishHandler = input->ChildRef(TCoCombineCoreWithSpilling::idx_FinishHandler);
        TExprNode::TPtr& loadHandler = input->ChildRef(TCoCombineCoreWithSpilling::idx_LoadHandler);

        auto status = ConvertToLambda(keyExtractor, ctx.Expr, 1);
        status = status.Combine(ConvertToLambda(initHandler, ctx.Expr, 2));
        status = status.Combine(ConvertToLambda(updateHandler, ctx.Expr, 3));
        status = status.Combine(ConvertToLambda(finishHandler, ctx.Expr, 2));
        status = status.Combine(ConvertToLambda(loadHandler, ctx.Expr, 2));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        // keyExtractor
        if (!UpdateLambdaAllArgumentsTypes(keyExtractor, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!keyExtractor->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        auto keyType = keyExtractor->GetTypeAnn();
        if (!EnsureHashableKey(keyExtractor->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureEquatableKey(keyExtractor->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // initHandler
        if (!UpdateLambdaAllArgumentsTypes(initHandler, {keyType, itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!initHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        auto stateType = initHandler->GetTypeAnn();
        if (!EnsureComputableType(initHandler->Pos(), *stateType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // updateHandler
        if (!UpdateLambdaAllArgumentsTypes(updateHandler, {keyType, itemType, stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!updateHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        if (!IsSameAnnotation(*updateHandler->GetTypeAnn(), *stateType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(updateHandler->Pos()), "Mismatch of update lambda return type and state type"));
            return IGraphTransformer::TStatus::Error;
        }

        // finishHandler
        if (!UpdateLambdaAllArgumentsTypes(finishHandler, {keyType, stateType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!finishHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        auto finishType = finishHandler->GetTypeAnn();
        if (!EnsureSeqOrOptionalType(finishHandler->Pos(), *finishType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto retKind = finishType->GetKind();
        const TTypeAnnotationNode* retItemType = nullptr;
        if (retKind == ETypeAnnotationKind::List) {
            retItemType = finishType->Cast<TListExprType>()->GetItemType();
        } else if (retKind == ETypeAnnotationKind::Optional) {
            retItemType = finishType->Cast<TOptionalExprType>()->GetItemType();
        } else {
            retItemType = finishType->Cast<TStreamExprType>()->GetItemType();
        }

        // loadHandler
        if (!UpdateLambdaAllArgumentsTypes(loadHandler, {keyType, retItemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!loadHandler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        if (!EnsureComputableType(loadHandler->Pos(), *loadHandler->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *retItemType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus GroupingCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 3, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStreamType(*input->Child(TCoGroupingCore::idx_Input), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const bool hasHandler = 4 == input->ChildrenSize();

        const auto streamType = input->Child(TCoGroupingCore::idx_Input)->GetTypeAnn();
        auto itemType = streamType->Cast<TStreamExprType>()->GetItemType();

        auto& groupSwitch = input->ChildRef(TCoGroupingCore::idx_GroupSwitch);
        auto& keyExtractor = input->ChildRef(TCoGroupingCore::idx_KeyExtractor);

        auto status = ConvertToLambda(groupSwitch, ctx.Expr, 2);
        status = status.Combine(ConvertToLambda(keyExtractor, ctx.Expr, 1));
        if (hasHandler) {
            auto& handler = input->ChildRef(TCoGroupingCore::idx_ConvertHandler);
            status = status.Combine(ConvertToLambda(handler, ctx.Expr, 1));
        }
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        // keyExtractor
        if (!UpdateLambdaAllArgumentsTypes(keyExtractor, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!keyExtractor->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto keyType = keyExtractor->GetTypeAnn();
        if (!EnsureHashableKey(keyExtractor->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureEquatableKey(keyExtractor->Pos(), keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // groupSwitch
        if (!UpdateLambdaAllArgumentsTypes(groupSwitch, {keyType, itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!groupSwitch->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        if (!EnsureSpecificDataType(*groupSwitch, EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // handler
        if (hasHandler) {
            auto& handler = input->ChildRef(TCoGroupingCore::idx_ConvertHandler);
            if (!UpdateLambdaAllArgumentsTypes(handler, {itemType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            itemType = handler->GetTypeAnn();
            if (!itemType) {
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TStreamExprType>(
            ctx.Expr.MakeType<TTupleExprType>(
                TTypeAnnotationNode::TListType{
                    keyExtractor->GetTypeAnn(),
                    ctx.Expr.MakeType<TStreamExprType>(itemType)
                }
            )
        ));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ChopperWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 4U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false, false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& keyExtractor = input->ChildRef(1U);
        auto& groupSwitch = input->ChildRef(2U);
        auto& handler = input->TailRef();

        auto status = ConvertToLambda(keyExtractor, ctx.Expr, 1);
        status = status.Combine(ConvertToLambda(groupSwitch, ctx.Expr, 2));
        status = status.Combine(ConvertToLambda(handler, ctx.Expr, 2));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        // keyExtractor
        if (!UpdateLambdaAllArgumentsTypes(keyExtractor, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!keyExtractor->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        // groupSwitch
        const auto keyType = keyExtractor->GetTypeAnn();
        if (!UpdateLambdaAllArgumentsTypes(groupSwitch, {keyType, itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!groupSwitch->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
        if (!EnsureSpecificDataType(*groupSwitch, EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!UpdateLambdaAllArgumentsTypes(handler, {keyType, input->Head().GetTypeAnn()}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!handler->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (handler->GetTypeAnn()->GetKind() != input->Head().GetTypeAnn()->GetKind()) {
            switch (input->Head().GetTypeAnn()->GetKind()) {
                case ETypeAnnotationKind::Stream: {
                    auto lambda = ctx.Expr.Builder(input->Tail().Pos())
                        .Lambda()
                            .Param("key")
                            .Param("stream")
                            .Callable("FromFlow")
                                .Apply(0, input->Tail())
                                    .With(0, "key")
                                    .With(1, "stream")
                                .Seal()
                            .Seal()
                        .Seal().Build();
                    output = ctx.Expr.ChangeChild(*input, 3U, std::move(lambda));
                    return IGraphTransformer::TStatus::Repeat;
                }
                case ETypeAnnotationKind::Flow: {
                    auto lambda = ctx.Expr.Builder(input->Tail().Pos())
                        .Lambda()
                            .Param("key")
                            .Param("flow")
                            .Callable("ToFlow")
                                .Apply(0, input->Tail())
                                    .With(0, "key")
                                    .With(1, "flow")
                                .Seal()
                            .Seal()
                        .Seal().Build();
                    output = ctx.Expr.ChangeChild(*input, 3U, std::move(lambda));
                    return IGraphTransformer::TStatus::Repeat;
                }
                default:
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(keyExtractor->Pos()), TStringBuilder()
                        << "Wrong handler kind of output sequence " << *handler->GetTypeAnn() << ", must be " << input->Head().GetTypeAnn()->GetKind()));
                    return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(handler->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus IterableWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambda = input->ChildRef(0);
        const auto status = ConvertToLambda(lambda, ctx.Expr, 0);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaArgumentsType(*lambda, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (lambda->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Stream) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder()
                << "Expend Stream as output type of lambda, but got : " << *lambda->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(lambda->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ListNotNullWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isOptional = false;
        auto type = input->Head().GetTypeAnn();
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
            isOptional = true;
        }

        if (type->GetKind() != ETypeAnnotationKind::List && type->GetKind() != ETypeAnnotationKind::EmptyList) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected (empty) list or optional of (empty) list, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (type->GetKind() == ETypeAnnotationKind::EmptyList) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        auto originalItemType = type->Cast<TListExprType>()->GetItemType();
        if (originalItemType->GetKind() == ETypeAnnotationKind::Null) {
            output = ctx.Expr.Builder(input->Pos())
                .Callable("AsList")
                .Seal()
                .Build();
            return IGraphTransformer::TStatus::Repeat;
        }

        auto itemType = RemoveOptionalType(originalItemType);
        const TTypeAnnotationNode* newType = ctx.Expr.MakeType<TListExprType>(itemType);
        if (isOptional) {
            newType = ctx.Expr.MakeType<TOptionalExprType>(newType);
        }

        output = ctx.Expr.Builder(input->Pos())
            .Callable("SafeCast")
                .Add(0, input->HeadPtr())
                .Add(1, ExpandType(input->Pos(), *newType, ctx.Expr))
            .Seal()
            .Build();
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus ListUniqWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<1U>(input, output, ctx, "Uniq");
    }

    IGraphTransformer::TStatus ListUniqStableWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return OptListWrapperImpl<1U>(input, output, ctx, "UniqStable");
    }

    IGraphTransformer::TStatus UniqWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto type = input->Head().GetTypeAnn();

        if (type->GetKind() == ETypeAnnotationKind::EmptyList) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (type->GetKind() != ETypeAnnotationKind::List) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected (empty) list, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        auto itemType = type->Cast<TListExprType>()->GetItemType();

        if (!itemType->IsHashable() || !itemType->IsEquatable()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected hashable and equatable type, but got: " << *itemType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ListFlattenWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isOptional = false;
        auto type = input->Head().GetTypeAnn();
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
            isOptional = true;
        }

        if (type->GetKind() != ETypeAnnotationKind::List && type->GetKind() != ETypeAnnotationKind::EmptyList) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected (empty) list or optional of (empty) list as input, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        bool isItemOptional = false;
        const TTypeAnnotationNode* itemType = nullptr;
        if (type->GetKind() == ETypeAnnotationKind::List) {
            itemType = type->Cast<TListExprType>()->GetItemType();
            if (itemType->GetKind() != ETypeAnnotationKind::Null) {
                if (itemType->GetKind() == ETypeAnnotationKind::Optional) {
                    itemType = itemType->Cast<TOptionalExprType>()->GetItemType();
                    isItemOptional = true;
                }

                if (itemType->GetKind() != ETypeAnnotationKind::List && itemType->GetKind() != ETypeAnnotationKind::EmptyList) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                        << "Expected (empty) list or optional of (empty) list as input item, but got: " << *type->Cast<TListExprType>()->GetItemType()));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        bool retEmptyList = !itemType || itemType->GetKind() == ETypeAnnotationKind::Null ||
            itemType->GetKind() == ETypeAnnotationKind::EmptyList;

        if (retEmptyList) {
            auto empty = ctx.Expr.NewCallable(input->Pos(), "EmptyList", {});
            if (!isOptional) {
                output = empty;
            } else {
                output = ctx.Expr.Builder(input->Pos())
                    .Callable("OrderedMap")
                        .Add(0, input->HeadPtr())
                        .Lambda(1)
                            .Param("x")
                            .Set(empty)
                        .Seal()
                    .Seal()
                    .Build();
            }

            return IGraphTransformer::TStatus::Repeat;
        }

        auto idLambda = ctx.Expr.Builder(input->Pos())
            .Lambda()
                .Param("z")
                .Arg("z")
            .Seal()
            .Build();

        auto innerLambda = isItemOptional ?
            ctx.Expr.Builder(input->Pos())
            .Lambda()
                .Param("y")
                .Callable("OrderedFlatMap")
                    .Arg(0, "y")
                    .Add(1, idLambda)
                .Seal()
            .Seal()
            .Build()
            : idLambda;

        auto lambda = ctx.Expr.Builder(input->Pos())
            .Lambda()
                .Param("x")
                .Callable("OrderedFlatMap")
                    .Arg(0, "x")
                    .Add(1, innerLambda)
                .Seal()
            .Seal()
            .Build();

        if (!isOptional) {
            output = ctx.Expr.Builder(input->Pos())
                .Apply(lambda)
                    .With(0, input->HeadPtr())
                .Seal()
                .Build();
        } else {
            output = ctx.Expr.Builder(input->Pos())
                    .Callable("OrderedMap")
                        .Add(0, input->HeadPtr())
                        .Add(1, lambda)
                    .Seal()
                    .Build();
        }

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus SqueezeToListWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 1U, 2U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureFlowType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->ChildrenSize() > 1U) {
            if (!EnsureSpecificDataType(input->Tail(), EDataSlot::Uint64, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            auto children = input->ChildrenList();
            children.emplace_back(ctx.Expr.NewCallable(input->Pos(), "Uint64", {ctx.Expr.NewAtom(input->Pos(), 0U)}));
            output = ctx.Expr.ChangeChildren(*input, std::move(children));
            return IGraphTransformer::TStatus::Repeat;
        }

        const auto itemType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
        input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(ctx.Expr.MakeType<TListExprType>(itemType)));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus EmptyFromWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (const TTypeAnnotationNode* itemType = nullptr; !EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    TExprNode::TPtr ExpandToWindowTraits(const TExprNode& input, TExprContext& ctx) {
        YQL_ENSURE(input.IsCallable("AggregationTraits"));
        return ctx.Builder(input.Pos())
            .Callable("WindowTraits")
                .Add(0, input.ChildPtr(0))
                .Add(1, input.ChildPtr(1))
                .Add(2, input.ChildPtr(2))
                .Lambda(3)
                    .Param("value")
                    .Param("state")
                    .Callable("Void")
                    .Seal()
                .Seal()
                .Add(4, input.ChildPtr(6))
                .Add(5, input.ChildPtr(7))
            .Seal()
            .Build();
    }

    bool ValidateAggManyStreams(const TExprNode& value, ui32 aggCount, TExprContext& ctx) {
        THashSet<ui32> usedIdxs;
        for (const auto& child : value.Children()) {
            if (!EnsureTuple(*child, ctx)) {
                return false;
            }

            for (const auto& atom : child->Children()) {
                if (!EnsureAtom(*atom, ctx)) {
                    return false;
                }

                ui32 idx;
                if (!TryFromString(atom->Content(), idx) || idx >= aggCount) {
                    ctx.AddError(TIssue(ctx.GetPosition(atom->Pos()),
                        TStringBuilder() << "Invalid aggregation index: " << atom->Content()));
                    return false;
                }

                if (!usedIdxs.insert(idx).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(atom->Pos()),
                        TStringBuilder() << "Duplication of aggregation index: " << atom->Content()));
                    return false;
                }
            }
        }

        if (usedIdxs.size() != aggCount) {
            ctx.AddError(TIssue(ctx.GetPosition(value.Pos()),
                TStringBuilder() << "Mismatch of total aggregations count in streams, expected: " << aggCount << ", got: " << usedIdxs.size()));
            return false;
        }

        return true;
    }

    IGraphTransformer::TStatus TimeOrderRecoverWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        const auto& source = input->ChildRef(0);
        auto& timeExtractor = input->ChildRef(1);
        const auto& delay = input->ChildRef(2);
        const auto& ahead = input->ChildRef(3);
        const auto& rowLimit = input->ChildRef(4);

        if (!EnsureFlowType(*source, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        const auto& inputRowType = GetSeqItemType(source->GetTypeAnn());

        if (!EnsureStructType(source->Pos(), *inputRowType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        //add artificial boolean column to taint out of order rows in the result table
        auto outputRowColumns = inputRowType->Cast<TStructExprType>()->GetItems();
        outputRowColumns.push_back(ctx.Expr.MakeType<TItemExprType>(
            NYql::NTimeOrderRecover::OUT_OF_ORDER_MARKER,
            ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool)
        ));
        auto outputRowType = ctx.Expr.MakeType<TStructExprType>(outputRowColumns);
        if (!outputRowType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto status = ConvertToLambda(timeExtractor, ctx.Expr, 1, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }
        { //timeExtractor
            if (!UpdateLambdaAllArgumentsTypes(timeExtractor, {inputRowType}, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!timeExtractor->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }
            bool isOptional;
            const TDataExprType* type;
            if (!EnsureDataOrOptionalOfData(*timeExtractor, isOptional, type, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (type->GetSlot() != EDataSlot::Timestamp) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(timeExtractor->Pos()), TStringBuilder() << "Expected Timestamp, but got: " << type->GetSlot()));
                return IGraphTransformer::TStatus::Error;
            }
        }
        { //delay
            bool isOptional;
            const TDataExprType* type;
            if (!EnsureDataOrOptionalOfData(*delay, isOptional, type, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (type->GetSlot() != EDataSlot::Interval) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(delay->Pos()), TStringBuilder() << "Expected Interval, but got: " << type->GetSlot()));
                return IGraphTransformer::TStatus::Error;
            }
        }
        { //ahead
            bool isOptional;
            const TDataExprType* type;
            if (!EnsureDataOrOptionalOfData(*ahead, isOptional, type, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (type->GetSlot() != EDataSlot::Interval) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(ahead->Pos()), TStringBuilder() << "Expected Interval, but got: " << type->GetSlot()));
                return IGraphTransformer::TStatus::Error;
            }
        }
        { //rowLimit
            bool isOptional;
            const TDataExprType* type;
            if (!EnsureDataOrOptionalOfData(*rowLimit, isOptional, type, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (type->GetSlot() != EDataSlot::Uint32) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(rowLimit->Pos()), TStringBuilder() << "Expected Uint32, but got: " << type->GetSlot()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputRowType));
        return IGraphTransformer::TStatus::Ok;
    }
} // namespace NTypeAnnImpl
}
