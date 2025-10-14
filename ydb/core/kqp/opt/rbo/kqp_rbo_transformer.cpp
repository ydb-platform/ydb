#include "kqp_rbo_transformer.h"
#include "kqp_operator.h"
#include "kqp_plan_conversion_utils.h"

#include <yql/essentials/utils/log/log.h>

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;
using namespace NYql::NDq;

namespace {
struct TJoinTableAliases {
    THashSet<TString> LeftSideAliases;
    THashSet<TString> RightSideAliases;
};

TJoinTableAliases GatherJoinAliasesLeftSideMultiInputs(const TVector<TInfoUnit> &joinKeys, const THashSet<TString> &processedInputs) {
    TJoinTableAliases joinAliases;
    for (const auto &joinKey : joinKeys) {
        if (processedInputs.count(joinKey.Alias)) {
            joinAliases.LeftSideAliases.insert(joinKey.Alias);
        } else {
            joinAliases.RightSideAliases.insert(joinKey.Alias);
        }
    }
    Y_ENSURE(joinAliases.LeftSideAliases.size(), "Left side of the join inputs are empty");
    Y_ENSURE(joinAliases.RightSideAliases.size() == 1, "Right side of the join should have only one input");
    return joinAliases;
}

TJoinTableAliases GatherJoinAliasesTwoInputs(const TVector<TInfoUnit> &joinKeys) {
    TJoinTableAliases joinAliases;
    for (ui32 i = 0; i < joinKeys.size(); i += 2) {
        joinAliases.LeftSideAliases.insert(joinKeys[i].Alias);
        joinAliases.RightSideAliases.insert(joinKeys[i + 1].Alias);
    }

    Y_ENSURE(joinAliases.LeftSideAliases.size() == 1, "Left side of the join should have only one input");
    Y_ENSURE(joinAliases.RightSideAliases.size() == 1, "Right side of the join should have only one input");
    return joinAliases;
}

TExprNode::TPtr BuildJoinKeys(const TVector<TInfoUnit> &joinKeys, const TJoinTableAliases &joinAliases, THashSet<TString> &processedInputs,
                              TExprContext &ctx, TPositionHandle pos) {
    Y_ENSURE(joinKeys.size() >= 2 && !(joinKeys.size() & 1), "Invalid join key size");
    TVector<TDqJoinKeyTuple> keys;
    for (ui32 i = 0; i < joinKeys.size(); i += 2) {
        auto leftSideKey = joinKeys[i];
        auto rightSideKey = joinKeys[i + 1];
        if (joinAliases.LeftSideAliases.count(rightSideKey.Alias)) {
            std::swap(leftSideKey, rightSideKey);
        }
        // clang-format off
        keys.push_back(Build<TDqJoinKeyTuple>(ctx, pos)
                           .LeftLabel()
                               .Value(leftSideKey.Alias)
                           .Build()
                           .LeftColumn()
                               .Value(leftSideKey.ColumnName)
                           .Build()
                           .RightLabel()
                               .Value(rightSideKey.Alias)
                           .Build()
                           .RightColumn()
                               .Value(rightSideKey.ColumnName)
                           .Build()
                      .Done());
        // clang-format on
        processedInputs.insert(leftSideKey.Alias);
        processedInputs.insert(rightSideKey.Alias);
    }
    return Build<TDqJoinKeyTupleList>(ctx, pos).Add(keys).Done().Ptr();
}

void ToCamelCase(std::string &s) {
    char previous = ' ';
    auto f = [&](char current) {
        char result = (std::isblank(previous) && std::isalpha(current)) ? std::toupper(current) : std::tolower(current);
        previous = current;
        return result;
    };
    std::transform(s.begin(), s.end(), s.begin(), f);
}

TExprNode::TPtr ReplacePgOps(TExprNode::TPtr input, TExprContext &ctx) {
    if (input->IsLambda()) {
        auto lambda = TCoLambda(input);

        // clang-format off
            return Build<TCoLambda>(ctx, input->Pos())
                .Args(lambda.Args())
                .Body(ReplacePgOps(lambda.Body().Ptr(), ctx))
            .Done().Ptr();
        // clang-format on
    } else if (input->IsCallable("PgAnd")) {
        // clang-format off
            return ctx.Builder(input->Pos())
                .Callable("ToPg")
                    .Callable(0, "And")
                        .Callable(0, "FromPg")
                            .Add(0, ReplacePgOps(input->ChildPtr(0), ctx))
                        .Seal()
                        .Callable(1, "FromPg")
                            .Add(0, ReplacePgOps(input->ChildPtr(1), ctx))
                        .Seal()
                    .Seal()
                .Seal()
            .Build();
        // clang-format on

    } else if (input->IsCallable("PgOr")) {
        // clang-format off
            return ctx.Builder(input->Pos())
                .Callable("ToPg")
                    .Callable(0, "Or")
                        .Callable(0, "FromPg")
                            .Add(0, ReplacePgOps(input->ChildPtr(0), ctx))
                        .Seal()
                        .Callable(1, "FromPg")
                            .Add(0, ReplacePgOps(input->ChildPtr(1), ctx))
                        .Seal()
                    .Seal()
                .Seal()
            .Build();
            // clnag-format on
        }
        else if (input->IsCallable()){
            TVector<TExprNode::TPtr> newChildren;
            for (auto c : input->Children()) {
                newChildren.push_back(ReplacePgOps(c, ctx));
            }
            // clang-format off
            return ctx.Builder(input->Pos())
                .Callable(input->Content())
                    .Add(std::move(newChildren))
                .Seal()
            .Build();
        // clang-format on
    } else if (input->IsList()) {
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(ReplacePgOps(c, ctx));
        }
        // clang-format off
            return ctx.Builder(input->Pos())
                .List()
                    .Add(std::move(newChildren))
                .Seal()
            .Build();
        // clang-format on
    } else {
        return input;
    }
}

TExprNode::TPtr BuildSort(TExprNode::TPtr input, TExprNode::TPtr sort, TExprContext &ctx) {
    TVector<TExprNode::TPtr> sortElements;

    for (auto sortItem : sort->Child(1)->Children()) {
        auto sortLambda = sortItem->Child(1);
        auto direction = sortItem->Child(2);
        auto nullsFirst = sortItem->Child(3);

        // clang-format off
        sortElements.push_back(Build<TKqpOpSortElement>(ctx, input->Pos())
            .Input(input)
            .Direction(direction)
            .NullsFirst(nullsFirst)
            .Lambda(sortLambda)
            .Done().Ptr());
        // clang-format on
    }

    // clang-format off
    return Build<TKqpOpSort>(ctx, input->Pos())
        .Input(input)
        .SortExpressions().Add(sortElements).Build()
        .Done().Ptr();
    // clang-format off
}

TExprNode::TPtr RewritePgSelect(const TExprNode::TPtr &node, TExprContext &ctx, const TTypeAnnotationContext &typeCtx) {
    Y_UNUSED(typeCtx);

    auto setItems = GetSetting(node->Head(), "set_items")->TailPtr();
    TVector<TExprNode::TPtr> setItemsResults;
    for (ui32 i = 0; i < setItems->ChildrenSize(); ++i) {
        auto setItem = setItems->ChildPtr(i);

        TVector<TExprNode::TPtr> resultElements;
        // In pg syntax duplicate attributes are allowed in the results, but we need to rename them
        // We use the counters for this purpose
        THashMap<TString, int> resultElementCounters;

        TExprNode::TPtr joinExpr;
        TExprNode::TPtr filterExpr;
        TExprNode::TPtr lastAlias;

        auto from = GetSetting(setItem->Tail(), "from");
        THashMap<TString, TExprNode::TPtr> aliasToInputMap;
        TVector<TExprNode::TPtr> inputsInOrder;

        if (from) {
            for (auto fromItem : from->Child(1)->Children()) {
                // From item can be a table read with an alias or a subquery with an alias
                // In case of a subquery, we have already translated PgSelect of the nested subquery
                // so we just need to remove TKqpOpRoot and plug in the translated subquery

                auto childExpr = fromItem->ChildPtr(0);
                auto alias = fromItem->Child(1);
                TExprNode::TPtr fromExpr;

                if (TKqpOpRoot::Match(childExpr.Get())) {
                    auto opRoot = TKqpOpRoot(childExpr);

                    TVector<TExprNode::TPtr> subqueryElements;

                    // We need to rename all the IUs in the subquery to reflect the new alias
                    auto subqueryType = childExpr->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                    for (auto item : subqueryType->GetItems()) {
                        auto orig = TString(item->GetName());
                        auto unit = TInfoUnit(orig);
                        auto renamedUnit = TInfoUnit(TString(alias->Content()), unit.ColumnName);

                        // clang-format off
                        subqueryElements.push_back(Build<TKqpOpMapElementRename>(ctx, node->Pos())
                            .Input(opRoot.Input())
                            .Variable().Value(renamedUnit.GetFullName()).Build()
                            .From().Value(unit.GetFullName()).Build()
                        .Done().Ptr());
                        // clang-format on
                    }

                    // clang-format off
                    fromExpr = Build<TKqpOpMap>(ctx, node->Pos())
                        .Input(opRoot.Input())
                        .MapElements().Add(subqueryElements).Build()
                        .Project().Value("true").Build()
                    .Done().Ptr();
                    // clang-format on
                }

                else {
                    auto readExpr = TKqlReadTableRanges(childExpr);

                    // clang-format off
                    fromExpr = Build<TKqpOpRead>(ctx, node->Pos())
                        .Table(readExpr.Table())
                        .Alias(alias)
                        .Columns(readExpr.Columns())
                    .Done().Ptr();
                    // clang-format on
                }

                aliasToInputMap.insert({TString(alias->Content()), fromExpr});
                inputsInOrder.push_back(fromExpr);
                lastAlias = alias;
            }
        }

        THashSet<TString> processedInputs;
        auto joinOps = GetSetting(setItem->Tail(), "join_ops");
        if (joinOps) {
            for (ui32 i = 0; i < joinOps->Tail().ChildrenSize(); ++i) {
                ui32 tableInputsCount = 0;
                auto tuple = joinOps->Tail().Child(i);
                for (ui32 j = 0; j < tuple->ChildrenSize(); ++j) {
                    auto join = tuple->Child(j);
                    auto joinType = join->Child(0)->Content();
                    if (joinType == "push") {
                        ++tableInputsCount;
                        continue;
                    }

                    Y_ENSURE(join->ChildrenSize() > 1 && join->Child(1)->ChildrenSize() > 1);
                    auto pgResolvedOps = FindNodes(join->Child(1)->Child(1)->TailPtr(), [](const TExprNode::TPtr &node) {
                        if (node->IsCallable("PgResolvedOp")) {
                            return true;
                        } else {
                            return false;
                        }
                    });

                    TVector<TInfoUnit> joinKeys;
                    for (const auto &pgResolvedOp : pgResolvedOps) {
                        TVector<TInfoUnit> keys;
                        GetAllMembers(pgResolvedOp, keys);
                        joinKeys.insert(joinKeys.end(), keys.begin(), keys.end());
                    }

                    TJoinTableAliases joinAliases;
                    TExprNode::TPtr leftInput;
                    TExprNode::TPtr rightInput;

                    if (tableInputsCount == 2) {
                        joinAliases = GatherJoinAliasesTwoInputs(joinKeys);
                        const auto leftSideAlias = *joinAliases.LeftSideAliases.begin();
                        const auto rightSideAlias = *joinAliases.RightSideAliases.begin();
                        Y_ENSURE(aliasToInputMap.count(leftSideAlias), "Left side alias is not present in input tables");
                        Y_ENSURE(aliasToInputMap.count(rightSideAlias), "Right sided alias is not present input tables");
                        leftInput = aliasToInputMap[leftSideAlias];
                        rightInput = aliasToInputMap[rightSideAlias];
                    } else if (tableInputsCount == 1) {
                        joinAliases = GatherJoinAliasesLeftSideMultiInputs(joinKeys, processedInputs);
                        const auto rightSideAlias = *joinAliases.RightSideAliases.begin();
                        Y_ENSURE(aliasToInputMap.contains(rightSideAlias), "Right side alias is not present in input tables");
                        leftInput = joinExpr;
                        rightInput = aliasToInputMap[rightSideAlias];
                    }

                    auto joinKind = TString(joinType);
                    ToCamelCase(joinKind);

                    // clang-format off
                    joinExpr = Build<TKqpOpJoin>(ctx, node->Pos())
                        .LeftInput(leftInput)
                        .RightInput(rightInput)
                        .JoinKind()
                            .Value(joinKind)
                        .Build()
                        .JoinKeys(BuildJoinKeys(joinKeys, joinAliases, processedInputs, ctx, node->Pos()))
                    .Done().Ptr();
                    // clang-format on
                    tableInputsCount = 0;
                }
            }

            // Build in order
            if (!joinExpr) {
                ui32 inputIndex = 0;
                if (inputsInOrder.size() > 1) {
                    while (inputIndex < inputsInOrder.size()) {
                        auto leftTableInput = inputIndex == 0 ? inputsInOrder[inputIndex] : joinExpr;
                        auto rightTableInput = inputIndex == 0 ? inputsInOrder[inputIndex + 1] : inputsInOrder[inputIndex];
                        auto joinKeys = Build<TDqJoinKeyTupleList>(ctx, node->Pos()).Done();
                        // clang-format off
                        joinExpr = Build<TKqpOpJoin>(ctx, node->Pos())
                            .LeftInput(leftTableInput)
                            .RightInput(rightTableInput)
                            .JoinKind()
                                .Value("Cross")
                            .Build()
                            .JoinKeys(joinKeys)
                        .Done().Ptr();
                        // clang-format on
                        inputIndex += (inputIndex == 0 ? 2 : 1);
                    }
                } else {
                    joinExpr = inputsInOrder.front();
                }
            }
        }

        filterExpr = joinExpr;

        auto where = GetSetting(setItem->Tail(), "where");

        if (where) {
            TExprNode::TPtr lambda = where->Child(1)->Child(1);
            lambda = ReplacePgOps(lambda, ctx);
            // clang-format off
            filterExpr = Build<TKqpOpFilter>(ctx, node->Pos())
                .Input(filterExpr)
                .Lambda(lambda)
            .Done().Ptr();
            // clang-format on
        }

        if (!filterExpr) {
            filterExpr = Build<TKqpOpEmptySource>(ctx, node->Pos()).Done().Ptr();
        }

        auto result = GetSetting(setItem->Tail(), "result");
        auto finalType = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

        TExprNode::TPtr resultExpr = filterExpr;

        for (auto resultItem : result->Child(1)->Children()) {
            auto column = resultItem->Child(0);
            TString columnName = TString(column->Content());

            const auto expectedTypeNode = finalType->FindItemType(columnName);
            Y_ENSURE(expectedTypeNode);
            const auto expectedType = expectedTypeNode->Cast<TPgExprType>();
            const auto actualTypeNode = resultItem->GetTypeAnn();

            YQL_CLOG(TRACE, CoreDq) << "Actual type for column: " << columnName << " is: " << *actualTypeNode;

            ui32 actualPgTypeId;
            bool convertToPg;
            Y_ENSURE(ExtractPgType(actualTypeNode, actualPgTypeId, convertToPg, node->Pos(), ctx));

            auto needPgCast = (expectedType->GetId() != actualPgTypeId);
            auto lambda = TCoLambda(ctx.DeepCopyLambda(*(resultItem->Child(2))));

            if (convertToPg) {
                Y_ENSURE(!needPgCast, TStringBuilder() << "Conversion to PG type is different at typization (" << expectedType->GetId()
                                                       << ") and optimization (" << actualPgTypeId << ") stages.");

                TExprNode::TPtr lambdaBody = lambda.Body().Ptr();
                lambdaBody = ReplacePgOps(lambdaBody, ctx);
                auto toPg = ctx.NewCallable(node->Pos(), "ToPg", {lambdaBody});

                // clang-format off
                lambda = Build<TCoLambda>(ctx, node->Pos())
                    .Args(lambda.Args())
                    .Body(toPg)
                .Done();
                // clang-format on
            } else if (needPgCast) {
                auto pgType =
                    ctx.NewCallable(node->Pos(), "PgType", {ctx.NewAtom(node->Pos(), NPg::LookupType(expectedType->GetId()).Name)});
                TExprNode::TPtr lambdaBody = lambda.Body().Ptr();
                lambdaBody = ReplacePgOps(lambdaBody, ctx);
                auto pgCast = ctx.NewCallable(node->Pos(), "PgCast", {lambdaBody, pgType});

                // clang-format off
                lambda = Build<TCoLambda>(ctx, node->Pos())
                    .Args(lambda.Args())
                    .Body(pgCast)
                .Done();
                // clang-format on
            }

            if (resultElementCounters.contains(columnName)) {
                resultElementCounters[columnName] += 1;
                columnName = columnName + "_generated_" + std::to_string(resultElementCounters.at(columnName));
            } else {
                resultElementCounters[columnName] = 1;
            }

            auto variable = Build<TCoAtom>(ctx, node->Pos()).Value(columnName).Done();

            // clang-format off
            resultElements.push_back(Build<TKqpOpMapElementLambda>(ctx, node->Pos())
                .Input(resultExpr)
                .Variable(variable)
                .Lambda(lambda)
            .Done().Ptr());
            // clang-format on
        }

        // clang-format off
        auto setItemPtr = Build<TKqpOpMap>(ctx, node->Pos())
            .Input(resultExpr)
            .MapElements()
                .Add(resultElements)
            .Build()
            .Project()
                .Value("true")
            .Build()
        .Done().Ptr();
        // clang-format onto

        auto sort = GetSetting(setItem->Tail(), "sort");
        if (sort) {
            setItemPtr = BuildSort(setItemPtr, sort, ctx);
        }

        setItemsResults.push_back(setItemPtr);
    }

    auto setOps = GetSetting(node->Head(), "set_ops");
    Y_ENSURE(setOps && setItemsResults.size());

    auto setOpsList = setOps->TailPtr();
    TExprNode::TPtr opResult = setItemsResults.front();
    for (ui32 i = 0, end = setOpsList->ChildrenSize(), setItemsIndex = 0, opsInputCount = 0; i < end; ++i) {
        if (setOpsList->ChildPtr(i)->Content() == "push") {
            ++opsInputCount;
            continue;
        }
        Y_ENSURE(setOpsList->ChildPtr(i)->Content() == "union_all");
        Y_ENSURE(opsInputCount <= 2);

        TExprNode::TPtr leftInput;
        TExprNode::TPtr rightInput;
        if (opsInputCount == 2) {
            Y_ENSURE(setItemsIndex + 1 < end);
            leftInput = setItemsResults[setItemsIndex++];
            rightInput = setItemsResults[setItemsIndex++];
        } else {
            Y_ENSURE(setItemsIndex < end);
            leftInput = opResult;
            rightInput = setItemsResults[setItemsIndex++];
        }

        // clang-format off
        opResult = Build<TKqpOpUnionAll>(ctx, node->Pos())
            .LeftInput(leftInput)
            .RightInput(rightInput)
        .Done().Ptr();
        // clang-format on

        // Count again.
        opsInputCount = 0;
    }

    auto sort = GetSetting(node->Head(), "sort");
    if (sort) {
        opResult = BuildSort(opResult, sort, ctx);
    }

    // clang-format off
    return Build<TKqpOpRoot>(ctx, node->Pos())
        .Input(opResult)
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr PushTakeIntoPlan(const TExprNode::TPtr &node, TExprContext &ctx, const TTypeAnnotationContext &typeCtx) {
    Y_UNUSED(typeCtx);
    auto take = TCoTake(node);
    if (auto root = take.Input().Maybe<TKqpOpRoot>()) {
        // clang-format off
        return Build<TKqpOpRoot>(ctx, node->Pos())
            .Input<TKqpOpLimit>()
                .Input(root.Cast().Input())
                .Count(take.Count())
            .Build()
        .Done().Ptr();
        // clang-format on
    } else {
        return node;
    }
}
} // namespace

namespace NKikimr {
namespace NKqp {

IGraphTransformer::TStatus TKqpPgRewriteTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    auto status = OptimizeExpr(
        output, output,
        [this](const TExprNode::TPtr &node, TExprContext &ctx) -> TExprNode::TPtr {
            if (TCoPgSelect::Match(node.Get())) {
                return RewritePgSelect(node, ctx, TypeCtx);
            } else if (TCoTake::Match(node.Get())) {
                return PushTakeIntoPlan(node, ctx, TypeCtx);
            } else {
                return node;
            }
        },
        ctx, settings);

    return status;
}

void TKqpPgRewriteTransformer::Rewind() {}

IGraphTransformer::TStatus TKqpNewRBOTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    auto status = OptimizeExpr(
        output, output,
        [this](const TExprNode::TPtr &node, TExprContext &ctx) -> TExprNode::TPtr {
            if (TKqpOpRoot::Match(node.Get())) {
                auto root = PlanConverter().ConvertRoot(node);
                root.ComputeParents();
                return RBO.Optimize(root, ctx);
            } else {
                return node;
            }
        },
        ctx, settings);

    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    return IGraphTransformer::TStatus::Ok;
}

void TKqpNewRBOTransformer::Rewind() {}

IGraphTransformer::TStatus TKqpRBOCleanupTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    Y_UNUSED(ctx);

    YQL_CLOG(TRACE, CoreDq) << "Cleanup input plan: " << output->Dump();

    if (output->IsList() && output->ChildrenSize() >= 1) {
        auto child_level_1 = output->Child(0);
        YQL_CLOG(TRACE, CoreDq) << "Matched level 0";

        if (child_level_1->IsList() && child_level_1->ChildrenSize() >= 1) {
            auto child_level_2 = child_level_1->Child(0);
            YQL_CLOG(TRACE, CoreDq) << "Matched level 1";

            if (child_level_2->IsList() && child_level_2->ChildrenSize() >= 1) {
                auto child_level_3 = child_level_2->Child(0);
                YQL_CLOG(TRACE, CoreDq) << "Matched level 2";

                if (child_level_3->IsList() && child_level_2->ChildrenSize() >= 1) {
                    auto maybeQuery = child_level_3->Child(0);

                    if (TKqpPhysicalQuery::Match(maybeQuery)) {
                        YQL_CLOG(TRACE, CoreDq) << "Found query node";
                        output = maybeQuery;
                    }
                }
            }
        }
    }

    return IGraphTransformer::TStatus::Ok;
}

void TKqpRBOCleanupTransformer::Rewind() {}

TAutoPtr<IGraphTransformer> CreateKqpPgRewriteTransformer(const TIntrusivePtr<TKqpOptimizeContext> &kqpCtx,
                                                          TTypeAnnotationContext &typeCtx) {
    return new TKqpPgRewriteTransformer(kqpCtx, typeCtx);
}

TAutoPtr<IGraphTransformer> CreateKqpNewRBOTransformer(const TIntrusivePtr<TKqpOptimizeContext> &kqpCtx, TTypeAnnotationContext &typeCtx,
                                                       const TKikimrConfiguration::TPtr &config,
                                                       TAutoPtr<IGraphTransformer> typeAnnTransformer,
                                                       TAutoPtr<IGraphTransformer> peephole) {
    return new TKqpNewRBOTransformer(kqpCtx, typeCtx, config, typeAnnTransformer, peephole);
}

TAutoPtr<IGraphTransformer> CreateKqpRBOCleanupTransformer(TTypeAnnotationContext &typeCtx) {
    return new TKqpRBOCleanupTransformer(typeCtx);
}

} // namespace NKqp
} // namespace NKikimr