#include "yql_co.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/set.h>
#include <util/string/cast.h>

#include <utility>

namespace NYql {

namespace {

using namespace NNodes;

bool IsConstMapLambda(TCoLambda lambda) {
    const auto body = lambda.Body();
    return body.Ref().IsCallable("Just") && body.Ref().GetDependencyScope()->second != lambda.Raw();
}

TExprNode::TPtr ExtractMemberFromLiteral(const TExprNode& lambda, const std::string_view& member) {
    if (lambda.IsLambda() && lambda.Tail().IsCallable("AsStruct")) {
        for (const auto& pair : lambda.Tail().ChildrenList()) {
            if (pair->Head().IsAtom(member))
                return pair->TailPtr();
            else if (!pair->Tail().IsCallable("Member") || &pair->Tail().Tail() != &pair->Head() || &pair->Tail().Head() != &lambda.Head().Head())
                break;
        }
    }
    return {};
}

bool IsPasstroughtFields(std::map<std::string_view, TExprNode::TPtr> fields, const TExprNode& lambda) {
    if (&lambda.Tail() == &lambda.Head().Head() || &lambda.Tail() == &lambda.Head().Tail())
        return true;

    if (lambda.Tail().IsCallable("AsStruct")) {
        lambda.Tail().ForEachChild([&](const TExprNode& field) {
            if (field.Tail().IsCallable("Member") && &field.Tail().Tail() == &field.Head() && (&field.Tail().Head() == &lambda.Head().Head() || &field.Tail().Head() == &lambda.Head().Tail()))
                fields.erase(field.Head().Content());
        });
    }

    return fields.empty();
}

template <typename TResult>
TExprNode::TPtr FuseFlatmaps(TCoFlatMapBase outerMap, TExprContext& ctx, TTypeAnnotationContext* types) {
    auto innerMap = outerMap.Input().template Cast<TCoFlatMapBase>();
    auto innerBody = innerMap.Lambda().Body();
    auto outerBody = outerMap.Lambda().Body();
    auto outerLambda = outerMap.Lambda().Ptr();
    auto outerLambdaArg = outerMap.Lambda().Args().Arg(0).Raw();
    if (outerLambdaArg->IsUsedInDependsOn()) {
        return outerMap.Ptr();
    }

    if (outerBody.Ref().IsCallable({"Just", "AsList"}) && innerBody.Ref().IsCallable({"Just", "AsList"})) {
        const auto width = outerBody.Ref().ChildrenSize() * innerBody.Ref().ChildrenSize();
        YQL_CLOG(DEBUG, Core) << "Fuse " << outerMap.Ref().Content() << " with " << innerMap.Ref().Content() << " width " << width;
        auto flatMap = ctx.Builder(outerMap.Pos())
            .Callable(TResult::CallableName())
                .Add(0, innerMap.Input().Ptr())
                .Lambda(1)
                    .Param("item")
                    .Callable(width > 1U ? "AsList" : "Just")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            auto j = 0U;
                            for (auto i = 0U; i < innerBody.Ref().ChildrenSize(); ++i) {
                                for (auto o = 0U; o < outerBody.Ref().ChildrenSize(); ++o) {
                                    parent.ApplyPartial(j++, outerMap.Lambda().Args().Ptr(), outerBody.Ref().ChildPtr(o))
                                        .With(0)
                                            .ApplyPartial(innerMap.Lambda().Args().Ptr(), innerBody.Ref().ChildPtr(i))
                                                .With(0, "item")
                                            .Seal()
                                        .Done()
                                    .Seal();
                                }
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal().Build();
        return ctx.WrapByCallableIf(1U == width && outerMap.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List && innerMap.Input().Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional, "ToList", std::move(flatMap));
    }

    if (IsJustOrSingleAsList(innerBody.Ref())) {
        auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(outerLambda, ctx, types);
        if (!placeHolder) {
            return {};
        }

        auto clonedInnerLambda = TCoLambda(ctx.DeepCopyLambda(innerMap.Lambda().Ref()));
        auto outerArgValue = innerBody.template Maybe<TCoJust>()
            ? clonedInnerLambda.Body().template Cast<TCoJust>().Input()
            : clonedInnerLambda.Body().template Cast<TCoAsList>().Arg(0);

        YQL_CLOG(DEBUG, Core) << "FuseFlatmaps with inner " << innerBody.Ref().Content();

        if (outerMap.Input().Ref().GetTypeAnn()->GetKind() != innerMap.Input().Ref().GetTypeAnn()->GetKind()
            && innerMap.Input().Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {

            if (outerMap.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow &&
                outerBody.Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Flow) {

                return Build<TResult>(ctx, outerMap.Pos())
                    .Input(innerMap.Input())
                    .Lambda()
                        .Args({"item"})
                        .template Body<TCoToFlow>()
                            .template Input<TExprApplier>()
                                .Apply(TCoLambda(lambdaWithPlaceholder))
                                .With(0, outerArgValue)
                                .With(clonedInnerLambda.Args().Arg(0), "item")
                                .With(TExprBase(placeHolder), "item")
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();

            } else if (outerMap.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream &&
                outerBody.Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Stream) {

                return Build<TResult>(ctx, outerMap.Pos())
                    .Input(innerMap.Input())
                    .Lambda()
                        .Args({"item"})
                        .template Body<TCoToStream>()
                            .template Input<TExprApplier>()
                                .Apply(TCoLambda(outerLambda))
                                .With(0, outerArgValue)
                                .With(clonedInnerLambda.Args().Arg(0), "item")
                                .With(TExprBase(placeHolder), "item")
                            .Build()
                            .FreeArgs()
                                .template Add<TCoDependsOn>()
                                    .Input("item")
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();

            } else if (outerMap.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List &&
                outerBody.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {

                return Build<TResult>(ctx, outerMap.Pos())
                    .Input(innerMap.Input())
                    .Lambda()
                        .Args({"item"})
                        .template Body<TCoToList>()
                            .template Optional<TExprApplier>()
                                .Apply(TCoLambda(outerLambda))
                                .With(0, outerArgValue)
                                .With(clonedInnerLambda.Args().Arg(0), "item")
                                .With(TExprBase(placeHolder), "item")
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
            }
        }

        return Build<TResult>(ctx, outerMap.Pos())
            .Input(innerMap.Input())
            .Lambda()
                .Args({"item"})
                .template Body<TExprApplier>()
                    .Apply(TCoLambda(outerLambda))
                    .With(0, outerArgValue)
                    .With(clonedInnerLambda.Args().Arg(0), "item")
                    .With(TExprBase(placeHolder), "item")
                .Build()
            .Build()
            .Done().Ptr();
    }

    if (innerBody.template Maybe<TCoOptionalIf>() || innerBody.template Maybe<TCoListIf>()) {
        const auto clonedInnerLambda = TCoLambda(ctx.DeepCopyLambda(innerMap.Lambda().Ref()));
        const auto conditional = clonedInnerLambda.Body().template Cast<TCoConditionalValueBase>();

        auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(outerMap.Lambda().Ptr(), ctx, types);
        if (!placeHolder) {
            return {};
        }

        auto value = ctx.Builder(outerMap.Pos())
            .Apply(lambdaWithPlaceholder)
            .With(0, conditional.Value().Ptr())
            .Seal()
            .Build();

        if (outerBody.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream || outerBody.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow) {
            value = ctx.Builder(outerMap.Pos())
                .Callable(TCoForwardList::CallableName())
                    .Add(0, value)
                .Seal()
                .Build();
        }

        auto conditionName = outerBody.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional
            ? TCoFlatOptionalIf::CallableName()
            : TCoFlatListIf::CallableName();

        auto newBody = Build<TCoConditionalValueBase>(ctx, outerMap.Pos())
            .CallableName(conditionName)
            .Predicate(conditional.Predicate())
            .Value(value)
            .Done();

        YQL_CLOG(DEBUG, Core) << "FuseFlatmaps with inner " << innerBody.Ref().Content();

        if (outerMap.Input().Ref().GetTypeAnn()->GetKind() != innerMap.Input().Ref().GetTypeAnn()->GetKind()
            && innerMap.Input().Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {

            if (outerMap.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow &&
                outerBody.Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Flow) {

                return Build<TResult>(ctx, outerMap.Pos())
                    .Input(innerMap.Input())
                    .Lambda()
                        .Args({"item"})
                        .template Body<TCoToFlow>()
                            .template Input<TExprApplier>()
                                .Apply(newBody)
                                .With(clonedInnerLambda.Args().Arg(0), "item")
                                .With(TExprBase(placeHolder), "item")
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();

            } else if (outerMap.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream &&
                outerBody.Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Stream) {

                return Build<TResult>(ctx, outerMap.Pos())
                    .Input(innerMap.Input())
                    .Lambda()
                        .Args({"item"})
                        .template Body<TCoToStream>()
                            .template Input<TExprApplier>()
                                .Apply(newBody)
                                .With(clonedInnerLambda.Args().Arg(0), "item")
                                .With(TExprBase(placeHolder), "item")
                            .Build()
                            .FreeArgs()
                                .template Add<TCoDependsOn>()
                                    .Input("item")
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();

            } else if (outerMap.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List &&
                outerBody.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {

                return Build<TResult>(ctx, outerMap.Pos())
                    .Input(innerMap.Input())
                    .Lambda()
                        .Args({"item"})
                        .template Body<TCoToList>()
                            .template Optional<TExprApplier>()
                                .Apply(newBody)
                                .With(clonedInnerLambda.Args().Arg(0), "item")
                                .With(TExprBase(placeHolder), "item")
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
            }
        }

        return Build<TResult>(ctx, outerMap.Pos())
            .Input(innerMap.Input())
            .Lambda()
                .Args({"item"})
                .template Body<TExprApplier>()
                    .Apply(newBody)
                    .With(clonedInnerLambda.Args().Arg(0), "item")
                    .With(TExprBase(placeHolder), "item")
                .Build()
            .Build()
            .Done().Ptr();
    }

    if (innerBody.template Maybe<TCoVisit>() && outerBody.template Maybe<TCoJust>()) {
        auto outerLambda = outerMap.Lambda().Ptr();

        auto originalVisit = innerBody.Ptr();
        YQL_CLOG(DEBUG, Core) << "FuseFlatmaps with inner " << innerBody.Ref().Content();
        return ctx.Builder(outerMap.Pos())
            .Callable(TResult::CallableName())
                .Add(0, innerMap.Input().Ptr())
                .Lambda(1)
                    .Param("item")
                    .Callable("Visit")
                        .ApplyPartial(0, innerMap.Lambda().Args().Ptr(), originalVisit->HeadPtr())
                            .With(0, "item")
                        .Seal()
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (size_t i = 1; i < originalVisit->ChildrenSize(); ++i) {
                                auto child = originalVisit->ChildPtr(i);
                                if (child->IsAtom()) {
                                    auto lambda = originalVisit->Child(i + 1);
                                    parent
                                        .Add(i, std::move(child))
                                        .Lambda(i + 1)
                                            .Param("visitItem")
                                            .Callable(TResult::CallableName())
                                                .Apply(0, lambda)
                                                    .With(0, "visitItem")
                                                .Seal()
                                                .Lambda(1)
                                                    .Param("mapItem")
                                                    .Apply(outerLambda)
                                                        .With(0, "mapItem")
                                                    .Seal()
                                                .Seal()
                                            .Seal()
                                        .Seal();
                                    ++i;
                                }
                                else {
                                    parent.Callable(i, TResult::CallableName())
                                        .Add(0, std::move(child))
                                        .Lambda(1)
                                            .Param("mapItem")
                                            .Apply(outerLambda)
                                                .With(0, "mapItem")
                                            .Seal()
                                        .Seal()
                                    .Seal();
                                }
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    if (innerBody.template Maybe<TCoAsList>() && outerBody.template Maybe<TCoJust>()) {

        auto outerJustInput = outerBody.template Cast<TCoJust>().Input().Ptr();
        auto outerLambdaArgs = outerMap.Lambda().Args().Ptr();
        auto innerLambdaArg = innerMap.Lambda().Args().Arg(0).Raw();

        auto originalAsList = innerBody.Ptr();

        YQL_CLOG(DEBUG, Core) << "FuseFlatmaps with inner " << innerBody.Ref().Content();
        return ctx.Builder(outerMap.Pos())
            .Callable(TResult::CallableName())
                .Add(0, innerMap.Input().Ptr())
                .Lambda(1)
                    .Param("item")
                    .Callable("AsList")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (size_t i = 0; i < originalAsList->ChildrenSize(); ++i) {
                                parent.ApplyPartial(i, outerLambdaArgs, outerJustInput)
                                    .With(0, originalAsList->ChildPtr(i))
                                    .WithNode(*innerLambdaArg, "item")
                                .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    if (innerBody.template Maybe<TCoExtendBase>() && outerBody.template Maybe<TCoJust>()) {

        auto originalExtend = innerBody.Ptr();

        YQL_CLOG(DEBUG, Core) << "FuseFlatmaps with inner " << innerBody.Ref().Content();
        return ctx.Builder(outerMap.Pos())
            .Callable(TResult::CallableName())
                .Add(0, innerMap.Input().Ptr())
                .Lambda(1)
                    .Param("item")
                    .Callable(originalExtend->Content())
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (size_t i = 0; i < originalExtend->ChildrenSize(); ++i) {
                                parent.ApplyPartial(i, {}, outerMap.Ptr())
                                    .WithNode(outerMap.Input().Ref(), originalExtend->ChildPtr(i))
                                    .WithNode(innerMap.Lambda().Args().Arg(0).Ref(), "item")
                                .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    return outerMap.Ptr();
}

template <bool TakeOrSkip>
TExprNode::TPtr FusePart(const TExprNode& node, TExprContext& ctx) {
    auto children = node.Head().ChildrenList();
    children.back() = ctx.NewCallable(node.Pos(), TakeOrSkip ? "Min" : "Add", {node.TailPtr(), std::move(children.back())});
    return ctx.ChangeChildren(node.Head(), std::move(children));
}

TExprNode::TPtr SumLengthOverExtend(const TExprNode& node, TExprContext& ctx) {
    auto children = node.Head().ChildrenList();
    for (auto& child : children) {
        child = ctx.ChangeChild(node, 0U, std::move(child));
    }

    return ctx.Builder(node.Pos())
        .Callable("Fold")
            .Callable(0, "AsList")
                .Add(std::move(children))
            .Seal()
            .Callable(1, "Uint64")
                .Atom(0, 0U)
            .Seal()
            .Lambda(2)
                .Param("item")
                .Param("state")
                .Callable("AggrAdd")
                    .Arg(0, "item")
                    .Arg(1, "state")
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr OrHasItemsOverExtend(const TExprNode& node, TExprContext& ctx) {
    auto children = node.Head().ChildrenList();
    for (auto& child : children) {
        child = ctx.ChangeChild(node, 0U, std::move(child));
    }
    return ctx.NewCallable(node.Pos(), "Or", std::move(children));
}

TExprNode::TPtr FuseSkipAfterEnumerate(const TExprNode& node, TExprContext& ctx) {
    auto enumerateChildren = node.Head().ChildrenList(); // Enumerate
    enumerateChildren.front() = ctx.ChangeChild(node, 0U, std::move(enumerateChildren.front()));

    auto offset = enumerateChildren.size() > 2U ?
        ctx.NewCallable(node.Pos(), "*", {enumerateChildren[2], node.TailPtr()}):
        node.TailPtr();

    if (enumerateChildren.size() > 1U) {
        enumerateChildren[1] = ctx.NewCallable(node.Pos(), "+", {std::move(enumerateChildren[1]), std::move(offset)});
    } else {
        enumerateChildren.emplace_back(std::move(offset));
    }

    return ctx.ChangeChildren(node.Head(), std::move(enumerateChildren));
}

template <bool SingleArg>
TExprNode::TPtr FuseFlatMapOverByKey(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Fuse " << node.Content() << " over " << node.Head().Content();
    auto lambda = SingleArg ?
        ctx.Builder(node.Pos())
            .Lambda()
                .Param("list")
                .Callable(node.Content())
                    .Apply(0, node.Head().Tail())
                        .With(0, "list")
                    .Seal()
                    .Add(1, node.TailPtr())
                .Seal()
            .Seal().Build():
        ctx.Builder(node.Pos())
            .Lambda()
                .Param("key")
                .Param("state")
                .Callable(node.Content())
                    .Apply(0, node.Head().Tail())
                        .With(0, "key")
                        .With(1, "state")
                    .Seal()
                    .Add(1, node.TailPtr())
                .Seal()
            .Seal().Build();

    return ctx.ChangeChild(node.Head(), node.Head().ChildrenSize() - 1U, std::move(lambda));
}

TExprNode::TPtr ExtractOneItemStructFromFold(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Extract single item struct from " << node.Content();
    const auto structType = node.Child(1)->GetTypeAnn()->Cast<TStructExprType>();
    const auto memberName = structType->GetItems().front()->GetName();
    const auto memberNameAtom = ctx.NewAtom(node.Pos(), memberName);

    return ctx.Builder(node.Pos())
        .Callable("AsStruct")
            .List(0)
                .Add(0, memberNameAtom)
                .Callable(1, node.Content())
                    .Add(0, node.HeadPtr())
                    .Callable(1, "Member")
                        .Add(0, node.ChildPtr(1))
                        .Add(1, memberNameAtom)
                    .Seal()
                    .Lambda(2)
                        .Param("item")
                        .Param("state")
                        .Callable("Member")
                            .Apply(0, node.Tail())
                                .With(0, "item")
                                .With(1)
                                    .Callable("AsStruct")
                                        .List(0)
                                            .Add(0, memberNameAtom)
                                            .Arg(1, "state")
                                        .Seal()
                                    .Seal()
                                .Done()
                            .Seal()
                            .Add(1, memberNameAtom)
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr ExtractOneItemTupleFromFold(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Extract single item tuple from " << node.Content();
    return ctx.Builder(node.Pos())
        .List()
            .Callable(0, node.Content())
                .Add(0, node.HeadPtr())
                .Callable(1, "Nth")
                    .Add(0, node.ChildPtr(1))
                    .Atom(1, 0U)
                .Seal()
                .Lambda(2)
                    .Param("item")
                    .Param("state")
                    .Callable("Nth")
                        .Apply(0, node.Tail())
                            .With(0, "item")
                            .With(1)
                                .List()
                                    .Arg(0, "state")
                                .Seal()
                            .Done()
                        .Seal()
                        .Atom(1, 0U)
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr ExtractOneItemStructFromFold1(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Extract single item struct from " << node.Content();
    const auto structType = node.Child(1)->Child(1)->GetTypeAnn()->Cast<TStructExprType>();
    const auto memberName = structType->GetItems().front()->GetName();
    const auto memberNameAtom = ctx.NewAtom(node.Pos(), memberName);

    return ctx.Builder(node.Pos())
        .Callable("Map")
            .Callable(0, node.Content())
                .Add(0, node.HeadPtr())
                .Lambda(1)
                    .Param("item")
                    .Callable("Member")
                        .Apply(0, *node.Child(1))
                            .With(0, "item")
                        .Seal()
                        .Add(1, memberNameAtom)
                    .Seal()
                .Seal()
                .Lambda(2)
                    .Param("item")
                    .Param("state")
                    .Callable("Member")
                        .Apply(0, node.Tail())
                            .With(0, "item")
                            .With(1)
                                .Callable("AsStruct")
                                    .List(0)
                                        .Add(0, memberNameAtom)
                                        .Arg(1, "state")
                                    .Seal()
                                .Seal()
                            .Done()
                        .Seal()
                        .Add(1, memberNameAtom)
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(1)
                .Param("m")
                .Callable("AsStruct")
                    .List(0)
                        .Add(0, memberNameAtom)
                        .Arg(1, "m")
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr ExtractOneItemTupleFromFold1(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Extract single item tuple from " << node.Content();
    return ctx.Builder(node.Pos())
        .Callable("Map")
            .Callable(0, node.Content())
                .Add(0, node.HeadPtr())
                .Lambda(1)
                    .Param("item")
                    .Callable("Nth")
                        .Apply(0, *node.Child(1))
                            .With(0, "item")
                        .Seal()
                        .Atom(1, 0U)
                    .Seal()
                .Seal()
                .Lambda(2)
                    .Param("item")
                    .Param("state")
                    .Callable("Nth")
                        .Apply(0, node.Tail())
                            .With(0, "item")
                            .With(1)
                                .List()
                                    .Arg(0, "state")
                                .Seal()
                            .Done()
                        .Seal()
                        .Atom(1, 0U)
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(1)
                .Param("m")
                .List()
                    .Arg(0, "m")
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr ExtractOneItemStructFromCondense(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Extract single item struct from " << node.Content();
    const auto structType = GetSeqItemType(*node.GetTypeAnn()).Cast<TStructExprType>();
    const auto memberName = structType->GetItems().front()->GetName();
    const auto memberNameAtom = ctx.NewAtom(node.Pos(), memberName);

    return ctx.Builder(node.Pos())
        .Callable("Map")
            .Callable(0, node.Content())
                .Add(0, node.HeadPtr())
                .Callable(1, "Member")
                    .Add(0, node.ChildPtr(1))
                    .Add(1, memberNameAtom)
                .Seal()
                .Lambda(2)
                    .Param("item")
                    .Param("state")
                    .Apply(*node.Child(2))
                        .With(0, "item")
                        .With(1)
                            .Callable("AsStruct")
                                .List(0)
                                    .Add(0, memberNameAtom)
                                    .Arg(1, "state")
                                .Seal()
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
                .Lambda(3)
                    .Param("item")
                    .Param("state")
                    .Callable("Member")
                        .Apply(0, node.Tail())
                            .With(0, "item")
                            .With(1)
                                .Callable("AsStruct")
                                    .List(0)
                                        .Add(0, memberNameAtom)
                                        .Arg(1, "state")
                                    .Seal()
                                .Seal()
                            .Done()
                        .Seal()
                        .Add(1, memberNameAtom)
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(1)
                .Param("out")
                .Callable("AsStruct")
                    .List(0)
                        .Add(0, memberNameAtom)
                        .Arg(1, "out")
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr ExtractOneItemTupleFromCondense(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Extract single item tuple from " << node.Content();
    return ctx.Builder(node.Pos())
        .Callable("Map")
            .Callable(0, node.Content())
                .Add(0, node.HeadPtr())
                .Callable(1, "Nth")
                    .Add(0, node.ChildPtr(1))
                    .Atom(1, 0U)
                .Seal()
                .Lambda(2)
                    .Param("item")
                    .Param("state")
                    .Apply(*node.Child(2))
                        .With(0, "item")
                        .With(1)
                            .List()
                                .Arg(0, "state")
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
                .Lambda(3)
                    .Param("item")
                    .Param("state")
                    .Callable("Nth")
                        .Apply(0, node.Tail())
                            .With(0, "item")
                            .With(1)
                                .List()
                                    .Arg(0, "state")
                                .Seal()
                            .Done()
                        .Seal()
                        .Atom(1, 0U)
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(1)
                .Param("out")
                .List()
                    .Arg(0, "out")
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr ExtractOneItemStructFromCondense1(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Extract single item struct from " << node.Content();
    const auto structType = GetSeqItemType(*node.GetTypeAnn()).Cast<TStructExprType>();
    const auto memberName = structType->GetItems().front()->GetName();
    const auto memberNameAtom = ctx.NewAtom(node.Pos(), memberName);

    return ctx.Builder(node.Pos())
        .Callable("Map")
            .Callable(0, node.Content())
                .Add(0, node.HeadPtr())
                .Lambda(1)
                    .Param("item")
                    .Callable("Member")
                        .Apply(0, *node.Child(1))
                            .With(0, "item")
                        .Seal()
                        .Add(1, memberNameAtom)
                    .Seal()
                .Seal()
                .Lambda(2)
                    .Param("item")
                    .Param("state")
                    .Apply(*node.Child(2))
                        .With(0, "item")
                        .With(1)
                            .Callable("AsStruct")
                                .List(0)
                                    .Add(0, memberNameAtom)
                                    .Arg(1, "state")
                                .Seal()
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
                .Lambda(3)
                    .Param("item")
                    .Param("state")
                    .Callable("Member")
                        .Apply(0, node.Tail())
                            .With(0, "item")
                            .With(1)
                                .Callable("AsStruct")
                                    .List(0)
                                        .Add(0, memberNameAtom)
                                        .Arg(1, "state")
                                    .Seal()
                                .Seal()
                            .Done()
                        .Seal()
                        .Add(1, memberNameAtom)
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(1)
                .Param("out")
                .Callable("AsStruct")
                    .List(0)
                        .Add(0, memberNameAtom)
                        .Arg(1, "out")
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr ExtractOneItemTupleFromCondense1(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Extract single item tuple from " << node.Content();
    return ctx.Builder(node.Pos())
        .Callable("Map")
            .Callable(0, node.Content())
                .Add(0, node.HeadPtr())
                .Lambda(1)
                    .Param("item")
                    .Callable("Nth")
                        .Apply(0, *node.Child(1))
                            .With(0, "item")
                        .Seal()
                        .Atom(1, 0U)
                    .Seal()
                .Seal()
                .Lambda(2)
                    .Param("item")
                    .Param("state")
                    .Apply(*node.Child(2))
                        .With(0, "item")
                        .With(1)
                            .List()
                                .Arg(0, "state")
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
                .Lambda(3)
                    .Param("item")
                    .Param("state")
                    .Callable("Nth")
                        .Apply(0, node.Tail())
                            .With(0, "item")
                            .With(1)
                                .List()
                                    .Arg(0, "state")
                                .Seal()
                            .Done()
                        .Seal()
                        .Atom(1, 0U)
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(1)
                .Param("out")
                .List()
                    .Arg(0, "out")
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr ConvertFoldBySumToLength(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto& lambda = node->Tail();
    auto arg1 = lambda.Tail().Child(0);
    const bool isInc = lambda.Tail().IsCallable("Inc");
    auto arg2 = isInc ? nullptr : lambda.Tail().Child(1);
    if (arg1->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data ||
        (arg2 && arg2->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data)) {
        return node;
    }

    const auto stateArg = lambda.Head().Child(1);
    if (arg2 && (arg2 == stateArg)) {
        DoSwap(arg1, arg2);
    }

    if (arg1 != stateArg) {
        return node;
    }

    if (arg2 && arg2->GetDependencyScope()->second == &lambda) {
        return node;
    }

    YQL_CLOG(DEBUG, Core) << "Convert " << node->Content() <<  " by sum to length";

    auto convertedIncrementValue = arg2 ? arg2 : ctx.NewCallable(node->Pos(), "Uint64", { ctx.NewAtom(node->Pos(), "1", TNodeFlags::Default) });
    const bool integral = IsDataTypeIntegral(arg1->GetTypeAnn()->Cast<TDataExprType>()->GetSlot())
        && (!arg2 || IsDataTypeIntegral(arg2->GetTypeAnn()->Cast<TDataExprType>()->GetSlot()));

    auto type = ExpandType(arg1->Pos(), *arg1->GetTypeAnn(), ctx);
    return ctx.Builder(node->Pos())
        .Callable(integral ? "BitCast" : "SafeCast")
            .Callable(0, "+")
                .Add(0, node->ChildPtr(1))
                .Callable(1, "*")
                    .Add(0, std::move(convertedIncrementValue))
                    .Callable(1, "Length")
                        .Add(0, node->HeadPtr())
                    .Seal()
                .Seal()
            .Seal()
            .Add(1, std::move(type))
        .Seal().Build();
}

TExprNode::TPtr ConvertFold1BySumToLength(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto& updateLambda = node->Tail();
    auto arg1 = updateLambda.Tail().Child(0);
    const bool isInc = updateLambda.Tail().IsCallable("Inc");
    auto arg2 = isInc ? nullptr : updateLambda.Tail().Child(1);
    if (arg1->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data ||
        (arg2 && arg2->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data)) {
        return node;
    }

    const auto stateArg = updateLambda.Head().Child(1);
    if (arg2 && (arg2 == stateArg)) {
        DoSwap(arg1, arg2);
    }

    if (arg1 != stateArg) {
        return node;
    }

    if (arg2 && arg2->GetDependencyScope()->second == &updateLambda) {
        return node;
    }

    const auto& initLambda = *node->Child(1);
    if (initLambda.Tail().GetDependencyScope()->second == &initLambda) {
        return node;
    }

    YQL_CLOG(DEBUG, Core) << "Convert " << node->Content() <<  " by sum to length";

    auto convertedIncrementValue = arg2 ? arg2 : ctx.NewCallable(node->Pos(), "Uint64", { ctx.NewAtom(node->Pos(), "1", TNodeFlags::Default) });
    const bool integral = IsDataTypeIntegral(arg1->GetTypeAnn()->Cast<TDataExprType>()->GetSlot())
        && (!arg2 || IsDataTypeIntegral(arg2->GetTypeAnn()->Cast<TDataExprType>()->GetSlot()));

    auto type = ExpandType(arg1->Pos(), *arg1->GetTypeAnn(), ctx);
    return ctx.Builder(node->Pos())
        .Callable("OptionalIf")
            .Callable(0, "HasItems")
                .Add(0, node->HeadPtr())
            .Seal()
            .Callable(1, integral ? "BitCast" : "SafeCast")
                .Callable(0, "+")
                    .Add(0, initLambda.TailPtr())
                    .Callable(1, "*")
                        .Add(0, std::move(convertedIncrementValue))
                        .Callable(1, "Dec")
                            .Callable(0, "Length")
                                .Add(0, node->HeadPtr())
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Add(1, std::move(type))
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr ConvertFoldByConstMinMax(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto& lambda = node->Tail();
    auto arg1 = lambda.Tail().Child(0);
    auto arg2 = lambda.Tail().Child(1);
    if (arg1->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data ||
        arg2->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data) {
        return node;
    }

    const auto stateArg = lambda.Head().Child(1);
    if (arg2 == stateArg) {
        DoSwap(arg1, arg2);
    }

    if (arg1 != stateArg) {
        return node;
    }

    if (arg2->GetDependencyScope()->second == &lambda) {
        return node;
    }

    YQL_CLOG(DEBUG, Core) << "Convert " << node->Content() <<  " by const " << lambda.Tail().Content();

    return ctx.Builder(node->Pos())
        .Callable("If")
            .Callable(0, "HasItems")
                .Add(0, node->HeadPtr())
            .Seal()
            .Callable(1, lambda.Tail().Content())
                .Add(0, node->ChildPtr(1))
                .Add(1, arg2)
            .Seal()
            .Add(2, node->ChildPtr(1))
        .Seal().Build();
}

TExprNode::TPtr ConvertFold1ByConstMinMax(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto& updateLambda = node->Tail();
    auto arg1 = updateLambda.Tail().Child(0);
    auto arg2 = updateLambda.Tail().Child(1);
    if (arg1->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data ||
        arg2->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data) {
        return node;
    }

    const auto stateArg = updateLambda.Head().Child(1);
    if (arg2 == stateArg) {
        DoSwap(arg1, arg2);
    }

    if (arg1 != stateArg) {
        return node;
    }

    if (arg2->GetDependencyScope()->second == &updateLambda) {
        return node;
    }

    const auto& initLambda = *node->Child(1);
    if (initLambda.Tail().GetDependencyScope()->second == &initLambda) {
        return node;
    }

    YQL_CLOG(DEBUG, Core) << "Convert " << node->Content() <<  " by const " << updateLambda.Tail().Content();

    return ctx.Builder(node->Pos())
        .Callable("OptionalIf")
            .Callable(0, "HasItems")
                .Add(0, node->HeadPtr())
            .Seal()
            .Callable(1, "If")
                .Callable(0, "AggrEquals")
                    .Callable(0, "Length")
                        .Add(0, node->HeadPtr())
                    .Seal()
                    .Callable(1, "Uint64")
                        .Atom(0, 1U)
                    .Seal()
                .Seal()
                .Add(1, initLambda.TailPtr())
                .Callable(2, updateLambda.Tail().Content())
                    .Add(0, arg2)
                    .Add(1, initLambda.TailPtr())
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr PropagateMapToFold(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Propagate " << node.Head().Content() << " to " << node.Content();
    return ctx.Builder(node.Pos())
        .Callable(node.Content())
            .Add(0, node.Head().HeadPtr())
            .Add(1, node.ChildPtr(1))
            .Lambda(2)
                .Param("item")
                .Param("state")
                .Apply(node.Tail())
                    .With(0)
                        .ApplyPartial(node.Head().Tail().HeadPtr(), node.Head().Tail().Tail().HeadPtr())
                            .With(0, "item")
                        .Seal()
                    .Done()
                    .With(1, "state")
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr PropagateMapToFold1(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Propagate " << node.Head().Content() << " to " << node.Content();
    return ctx.Builder(node.Pos())
        .Callable(node.Content())
            .Add(0, node.Head().HeadPtr())
            .Lambda(1)
                .Param("item")
                .Apply(*node.Child(1))
                    .With(0)
                        .ApplyPartial(node.Head().Tail().HeadPtr(), node.Head().Tail().Tail().HeadPtr())
                            .With(0, "item")
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
            .Lambda(2)
                .Param("item")
                .Param("state")
                .Apply(node.Tail())
                    .With(0)
                        .ApplyPartial(node.Head().Tail().HeadPtr(), node.Head().Tail().Tail().HeadPtr())
                            .With(0, "item")
                        .Seal()
                    .Done()
                    .With(1, "state")
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr PropagateMapToCondense(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Propagate " << node.Head().Content() << " to " << node.Content();
    return ctx.Builder(node.Pos())
        .Callable(node.Content())
            .Add(0, node.Head().HeadPtr())
            .Add(1, node.ChildPtr(1))
            .Lambda(2)
                .Param("item")
                .Param("state")
                .Apply(*node.Child(2))
                    .With(0)
                        .ApplyPartial(node.Head().Tail().HeadPtr(), node.Head().Tail().Tail().HeadPtr())
                            .With(0, "item")
                        .Seal()
                    .Done()
                    .With(1, "state")
                .Seal()
            .Seal()
            .Lambda(3)
                .Param("item")
                .Param("state")
                .Apply(node.Tail())
                    .With(0)
                        .ApplyPartial(node.Head().Tail().HeadPtr(), node.Head().Tail().Tail().HeadPtr())
                            .With(0, "item")
                        .Seal()
                    .Done()
                    .With(1, "state")
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr PropagateMapToCondense1(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Propagate " << node.Head().Content() << " to " << node.Content();
    return ctx.Builder(node.Pos())
        .Callable(node.Content())
            .Add(0, node.Head().HeadPtr())
            .Lambda(1)
                .Param("item")
                .Apply(*node.Child(1))
                    .With(0)
                        .ApplyPartial(node.Head().Tail().HeadPtr(), node.Head().Tail().Tail().HeadPtr())
                            .With(0, "item")
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
            .Lambda(2)
                .Param("item")
                .Param("state")
                .Apply(*node.Child(2))
                    .With(0)
                        .ApplyPartial(node.Head().Tail().HeadPtr(), node.Head().Tail().Tail().HeadPtr())
                            .With(0, "item")
                        .Seal()
                    .Done()
                    .With(1, "state")
                .Seal()
            .Seal()
            .Lambda(3)
                .Param("item")
                .Param("state")
                .Apply(node.Tail())
                    .With(0)
                        .ApplyPartial(node.Head().Tail().HeadPtr(), node.Head().Tail().Tail().HeadPtr())
                            .With(0, "item")
                        .Seal()
                    .Done()
                    .With(1, "state")
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr PropagateConstPremapIntoCombineByKey(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, Core) << "Propagate const premap into " << node.Content();

    const auto constItem = node.Child(1)->Tail().HeadPtr();

    auto children = node.ChildrenList();

    // update lambda
    children[4] = ctx.Builder(children[4]->Pos())
        .Lambda()
            .Param("key")
            .Param("item")
            .Param("state")
            .Apply(*children[4])
                .With(0)
                    .Apply(*children[2])
                        .With(0, constItem)
                    .Seal()
                .Done()
                .With(1, constItem)
                .With(2, "state")
            .Seal()
        .Seal()
        .Build();

    // init lambda
    children[3] = ctx.Builder(children[3]->Pos())
        .Lambda()
            .Param("key")
            .Param("item")
            .Apply(*children[3])
                .With(0)
                    .Apply(*children[2])
                        .With(0, constItem)
                    .Seal()
                .Done()
                .With(1, constItem)
            .Seal()
        .Seal()
        .Build();

    // keyExtractorLambda
    children[2] = ctx.Builder(children[2]->Pos())
        .Lambda()
            .Param("item")
            .Apply(*children[2])
                .With(0, std::move(constItem))
            .Seal()
        .Seal()
        .Build();

    return ctx.ChangeChildren(node, std::move(children));
}

TExprNode::TPtr OptimizeReverse(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    if (optCtx.IsSingleUsage(node->Head()) && node->Head().IsCallable({"Sort", "AssumeSorted"})) {
        YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
        auto asc = node->Head().ChildPtr(1);
        if (asc->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
            TExprNode::TListType newAscChildren;
            if (asc->IsList()) {
                newAscChildren = asc->ChildrenList();
            } else {
                const auto size = asc->GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
                newAscChildren.reserve(size);
                for (ui32 i = 0U; i < size; ++i) {
                    newAscChildren.emplace_back(ctx.Builder(asc->Pos())
                        .Callable("Nth")
                            .Add(0, asc)
                            .Atom(1, ToString(i), TNodeFlags::Default)
                        .Seal().Build());
                }
            }
            for (auto& child : newAscChildren) {
                child = ctx.NewCallable(asc->Pos(), "Not", {std::move(child)});
            }

            asc = ctx.NewList(node->Pos(), std::move(newAscChildren));
        } else {
            asc = ctx.NewCallable(node->Pos(), "Not", {std::move(asc)});
        }

        auto children = node->Head().ChildrenList();
        if (node->Head().IsCallable("AssumeSorted")) {
            children.front() = ctx.ChangeChild(*node, 0U, std::move(children.front()));
        }
        children[1] = std::move(asc);
        return ctx.ChangeChildren(node->Head(), std::move(children));
    }

    return node;
}

TExprNode::TPtr OptimizeLookup(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
     if (optCtx.IsSingleUsage(node->Head()) && node->Head().IsCallable("ToIndexDict") && TMaybeNode<TCoIntegralCtor>(node->TailPtr())) {
        const auto& atom = node->Tail().Head();
        if (atom.Content() == "0" && !(TNodeFlags::BinaryContent & atom.Flags())) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " zero item over " << node->Head().Content();
            return ctx.RenameNode(node->Head(), "ListHead");
        }
    }
    return node;
}

template <bool Ordered>
TExprNode::TPtr OptimizeFlatMap(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    const std::conditional_t<Ordered, TCoOrderedFlatMap, TCoFlatMap> self(node);
    if (!optCtx.IsSingleUsage(self.Input().Ref())) {
        return node;
    }

    if constexpr (Ordered) {
        if (self.Input().template Maybe<TCoOrderedFlatMap>()) {
            if (const auto ret = FuseFlatmaps<TCoOrderedFlatMap>(self, ctx, optCtx.Types); ret != node) {
                return ret;
            }
        }
    }

    if (self.Input().template Maybe<TCoFlatMapBase>()) {
        if (const auto ret = FuseFlatmaps<TCoFlatMap>(self, ctx, optCtx.Types); ret != node) {
            return ret;
        }
    }

    if (self.Lambda().Ref().IsComplete()) {
        if (node->Head().IsCallable({"GroupByKey", "CombineByKey"})) {
            return FuseFlatMapOverByKey<false>(*node, ctx);
        }

        if (node->Head().IsCallable({"PartitionByKey", "PartitionsByKeys", "ShuffleByKeys"})) {
            return FuseFlatMapOverByKey<true>(*node, ctx);
        }
    }

    if (node->Head().IsCallable("ForwardList")) {
        if (ETypeAnnotationKind::List == node->GetTypeAnn()->GetKind()) {
            YQL_CLOG(DEBUG, Core) << "Swap " << node->Content() << " with " << node->Head().Content();
            return ctx.SwapWithHead(*node);
        } else {
            YQL_CLOG(DEBUG, Core) << "Drop " << node->Head().Content() << " under " << node->Content();
            return ctx.ChangeChild(*node, 0U, node->Head().HeadPtr());
        }
    }

    if (const auto& input = self.Input().Ref(); input.IsCallable("Switch") && IsJustOrSingleAsList(self.Lambda().Body().Ref())) {
        if (const auto item = optCtx.GetParentIfSingle(self.Lambda().Args().Arg(0).Ref()); item && item->IsCallable("VariantItem")) {
            const auto& inputItemType = GetSeqItemType(*input.Head().GetTypeAnn());
            const auto variants = ETypeAnnotationKind::Variant == inputItemType.GetKind() ? inputItemType.template Cast<TVariantExprType>()->GetUnderlyingType()->template Cast<TTupleExprType>()->GetSize() : 0U;
            TNodeSet atoms(variants);
            for (auto i = 2U; i < self.Input().Ref().ChildrenSize(); ++i) {
                const auto& ids = *input.Child(i);
                for (auto j = 0U; j < ids.ChildrenSize(); ++j)
                    if (!atoms.emplace(ids.Child(j)).second)
                        return node;

                if (input.Child(++i) != &input.Tail())
                    return node;
            }

            if (variants != atoms.size()) {
                return node;
            }

            YQL_CLOG(DEBUG, Core) << node->Content() << " over " << input.Content() << " with all " << variants << " identical lambdas.";
            auto lambda = ctx.DeepCopyLambda(node->Tail(), ctx.ReplaceNode(self.Lambda().Body().Ref().HeadPtr(), *item, item->HeadPtr()));
            constexpr auto mapType = Ordered ? "OrderedMap" : "Map";
            return ctx.Builder(node->Pos())
                .Callable(mapType)
                    .Apply(0, input.Tail())
                        .With(0)
                            .Callable(mapType)
                                .Add(0, input.HeadPtr())
                                .Lambda(1)
                                    .Param("var")
                                    .Callable(item->Content())
                                        .Arg(0, "var")
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Done()
                    .Seal()
                    .Add(1, std::move(lambda))
                .Seal().Build();
        }
    }

    if (node->Head().IsCallable(Ordered ? "OrderedExtend" : "Extend") &&
        // constraints below can not be derived for (Ordered)Extend
        !node->GetConstraint<TSortedConstraintNode>() &&
        !node->GetConstraint<TPartOfSortedConstraintNode>() &&
        !node->GetConstraint<TUniqueConstraintNode>() &&
        !node->GetConstraint<TPartOfUniqueConstraintNode>() &&
        !node->GetConstraint<TDistinctConstraintNode>() &&
        !node->GetConstraint<TPartOfDistinctConstraintNode>())
    {
        auto canPush = [&](const auto& child) {
            // we push FlatMap over Extend only if it can later be fused with child
            return child->IsCallable({Ordered ? "OrderedFlatMap" : "FlatMap", "GroupByKey", "CombineByKey", "PartitionByKey", "PartitionsByKeys", "ShuffleByKeys",
                                      "ListIf", "FlatListIf", "AsList", "ToList"}) && optCtx.IsSingleUsage(*child);
        };
        if (AllOf(node->Head().ChildrenList(), canPush)) {
            TExprNodeList newChildren;
            for (auto child : node->Head().ChildrenList()) {
                newChildren.push_back(ctx.ChangeChild(*node, TCoFlatMapBase::idx_Input, std::move(child)));
            }
            YQL_CLOG(DEBUG, Core) << "Swap " << node->Content() << " with " << node->Head().Content();
            return ctx.ChangeChildren(node->Head(), std::move(newChildren));
        }
    }

    return node;
}

}

void RegisterCoFlowCallables1(TCallableOptimizerMap& map) {
    using namespace std::placeholders;

    map["FlatMap"] = std::bind(&OptimizeFlatMap<false>, _1, _2, _3);
    map["OrderedFlatMap"] = std::bind(&OptimizeFlatMap<true>, _1, _2, _3);

    map["Lookup"] = std::bind(&OptimizeLookup, _1, _2, _3);

    map["Skip"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head()) && !optCtx.IsPersistentNode(node->Head())) {
            return node;
        }

        if (node->Head().IsCallable({"ForwardList", "FromFlow"})) {
            YQL_CLOG(DEBUG, Core) << "Swap " << node->Content() << " with " << node->Head().Content();
            return ctx.SwapWithHead(*node);
        }

        if (TCoSkip::Match(&node->Head())) {
            YQL_CLOG(DEBUG, Core) << "Fuse " << node->Content() << " over " << node->Head().Content();
            return FusePart<false>(*node, ctx);
        }

        if (TCoEnumerate::Match(&node->Head())) {
            YQL_CLOG(DEBUG, Core) << "Fuse " << node->Content() << " over " << node->Head().Content();
            return FuseSkipAfterEnumerate(*node, ctx);
        }

        if (TCoFlatMapBase::Match(&node->Head()) &&
            node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List &&
            IsJustOrSingleAsList(node->Head().Tail().Tail())) {
            YQL_CLOG(DEBUG, Core) << "Move " << node->Content() << " over " << node->Head().Content();
            return ctx.SwapWithHead(*node);
        }

        return node;
    };

    map["Take"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head()) && !optCtx.IsPersistentNode(node->Head())) {
            return node;
        }

        if (node->Head().IsCallable({"ForwardList", "FromFlow"})) {
            YQL_CLOG(DEBUG, Core) << "Swap " << node->Content() << " with " << node->Head().Content();
            return ctx.SwapWithHead(*node);
        }

        if (node->Head().IsCallable("Sort")) {
            YQL_CLOG(DEBUG, Core) << "Fuse " << node->Content() << " over " << node->Head().Content();
            auto children = node->Head().ChildrenList();
            auto it = children.cbegin();
            children.emplace(++it, node->TailPtr());
            return ctx.NewCallable(node->Pos(), "TopSort", std::move(children));
        }

        if (node->Head().IsCallable("ExtractMembers") && node->Head().Head().IsCallable("Sort") && optCtx.IsSingleUsage(node->Head().Head())) {
            YQL_CLOG(DEBUG, Core) << "Fuse " << node->Content() << " over " << node->Head().Content() << " over " <<  node->Head().Head().Content();
            auto children =  node->Head().Head().ChildrenList();
            auto it = children.cbegin();
            children.emplace(++it, node->TailPtr());
            return ctx.ChangeChild(node->Head(), 0U, ctx.NewCallable(node->Pos(), "TopSort", std::move(children)));
        }

        if (node->Head().IsCallable({"Top", "TopSort"})) {
            YQL_CLOG(DEBUG, Core) << "Fuse " << node->Content() << " over " << node->Head().Content();
            return ctx.ChangeChild(node->Head(), 1U, ctx.NewCallable(node->Pos(), "Min", {node->TailPtr(), node->Head().ChildPtr(1)}));
        }

        if (TCoTake::Match(&node->Head())) {
            YQL_CLOG(DEBUG, Core) << "Fuse " << node->Content() << " over " << node->Head().Content();
            return FusePart<true>(*node, ctx);
        }

        if (TCoEnumerate::Match(&node->Head())) {
            YQL_CLOG(DEBUG, Core) << "Fuse " << node->Content() << " over " << node->Head().Content();
            return ctx.SwapWithHead(*node);
        }

        if (TCoFlatMapBase::Match(&node->Head()) &&
            node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List &&
            IsJustOrSingleAsList(node->Head().Tail().Tail())) {
            YQL_CLOG(DEBUG, Core) << "Move " << node->Content() << " over " << node->Head().Content();
            return ctx.SwapWithHead(*node);
        }

        return node;
    };

    map["Length"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head()) && !optCtx.IsPersistentNode(node->Head())) {
            return node;
        }

        if (node->Head().IsCallable("Enumerate")) {
            YQL_CLOG(DEBUG, Core) << "Move " << node->Content() << " over " << node->Head().Content();
            return ctx.ChangeChild(*node, 0U, node->Head().HeadPtr());
        }

        if (TCoExtendBase::Match(&node->Head())) {
            YQL_CLOG(DEBUG, Core) << "Sum " << node->Content() << " over " << node->Head().Content();
            return SumLengthOverExtend(*node, ctx);
        }

        if (node->Head().IsCallable({"Append", "Insert"})) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
            return ctx.NewCallable(node->Head().Tail().Pos(), "Inc", {ctx.ChangeChild(*node, 0U, node->Head().HeadPtr())});
        }

        if (node->Head().IsCallable("Prepend")) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
            return ctx.NewCallable(node->Head().Head().Pos(), "Inc", {ctx.ChangeChild(*node, 0U, node->Head().TailPtr())});
        }

        if (node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List) {
            const auto itemType = node->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
                const auto structType = itemType->Cast<TStructExprType>();
                if (structType->GetSize() > 0) {
                    YQL_CLOG(DEBUG, Core) << node->Content() << " over non empty structs";
                    return ctx.Builder(node->Pos())
                        .Callable("Length")
                            .Callable(0, "ExtractMembers")
                                .Add(0, node->HeadPtr())
                                .List(1).Seal()
                            .Seal()
                        .Seal().Build();
                }
            }
        }

        return node;
    };

    map["HasItems"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head()) && !optCtx.IsPersistentNode(node->Head())) {
            return node;
        }

        if (node->Head().IsCallable("Enumerate")) {
            YQL_CLOG(DEBUG, Core) << "Move " << node->Content() << " over " << node->Head().Content();
            return ctx.ChangeChild(*node, 0U, node->Head().HeadPtr());
        }

        if (TCoExtendBase::Match(&node->Head())) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
            return OrHasItemsOverExtend(*node, ctx);
        }

        return node;
    };

    map["Fold"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head())) {
            return node;
        }

        if (node->Child(1)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Struct &&
            1U == node->Child(1)->GetTypeAnn()->Cast<TStructExprType>()->GetSize()) {
            return ExtractOneItemStructFromFold(*node, ctx);
        } else if (node->Child(1)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple &&
            1U == node->Child(1)->GetTypeAnn()->Cast<TTupleExprType>()->GetSize()) {
            return ExtractOneItemTupleFromFold(*node, ctx);
        }

        if (node->Head().IsCallable({"FlatMap", "OrderedFlatMap"}) && IsJustOrSingleAsList(node->Head().Tail().Tail())) {
            return PropagateMapToFold(*node, ctx);
        }

        if (node->Tail().Tail().IsCallable({"+", "Add", "Inc", "AggrAdd"})) {
            return ConvertFoldBySumToLength(node, ctx);
        }

        if (2U == node->Tail().Tail().ChildrenSize() && node->Tail().Tail().IsCallable({"Min", "Max", "AggrMin", "AggrMax"})) {
            return ConvertFoldByConstMinMax(node, ctx);
        }

        return node;
    };

    map["Fold1"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head())) {
            return node;
        }

        if (node->Child(1)->Child(1)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Struct &&
            1U == node->Child(1)->Child(1)->GetTypeAnn()->Cast<TStructExprType>()->GetSize()) {
            return ExtractOneItemStructFromFold1(*node, ctx);
        } else if (node->Child(1)->Child(1)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple &&
            1U == node->Child(1)->Child(1)->GetTypeAnn()->Cast<TTupleExprType>()->GetSize()) {
            return ExtractOneItemTupleFromFold1(*node, ctx);
        }

        if (node->Head().IsCallable({"FlatMap", "OrderedFlatMap"}) && IsJustOrSingleAsList(node->Head().Tail().Tail())) {
            return PropagateMapToFold1(*node, ctx);
        }

        if (node->Tail().Tail().IsCallable({"+", "Add", "Inc", "AggrAdd"})) {
            return ConvertFold1BySumToLength(node, ctx);
        }

        if (2U == node->Tail().Tail().ChildrenSize() && node->Tail().Tail().IsCallable({"Min", "Max", "AggrMin", "AggrMax"})) {
            return ConvertFold1ByConstMinMax(node, ctx);
        }

        return node;
    };

    map["Condense"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head())) {
            return node;
        }

        const auto& itemType = GetSeqItemType(*node->GetTypeAnn());
        if (itemType.GetKind() == ETypeAnnotationKind::Struct &&
            1U == itemType.Cast<TStructExprType>()->GetSize()) {
            return ExtractOneItemStructFromCondense(*node, ctx);
        } else if (itemType.GetKind() == ETypeAnnotationKind::Tuple &&
            1U == itemType.Cast<TTupleExprType>()->GetSize()) {
            return ExtractOneItemTupleFromCondense(*node, ctx);
        }

        if (node->Head().IsCallable({"FlatMap", "OrderedFlatMap"}) && IsJustOrSingleAsList(node->Head().Tail().Tail())) {
            return PropagateMapToCondense(*node, ctx);
        }

        if (node->Head().IsCallable({"ForwardList", "FromFlow"})) {
            YQL_CLOG(DEBUG, Core) << "Swap " << node->Content() << " with " << node->Head().Content();
            return ctx.SwapWithHead(*node);
        }

        return node;
    };

    map["Condense1"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head())) {
            return node;
        }

        const auto& itemType = GetSeqItemType(*node->GetTypeAnn());
        if (itemType.GetKind() == ETypeAnnotationKind::Struct &&
            1U == itemType.Cast<TStructExprType>()->GetSize()) {
            return ExtractOneItemStructFromCondense1(*node, ctx);
        } else if (itemType.GetKind() == ETypeAnnotationKind::Tuple &&
            1U == itemType.Cast<TTupleExprType>()->GetSize()) {
            return ExtractOneItemTupleFromCondense1(*node, ctx);
        }

        if (node->Head().IsCallable({"FlatMap", "OrderedFlatMap"}) && IsJustOrSingleAsList(node->Head().Tail().Tail())) {
            return PropagateMapToCondense1(*node, ctx);
        }

        if (node->Head().IsCallable({"ForwardList", "FromFlow"})) {
            YQL_CLOG(DEBUG, Core) << "Swap " << node->Content() << " with " << node->Head().Content();
            return ctx.SwapWithHead(*node);
        }

        // Very special optimizer for hybrid reduce. Drops Chain1Map whose only purpose is make key switch column.
        const TCoCondense1 self(node);
        if (const TMaybeNode<TCoChain1Map> maybeChain(&SkipCallables(self.Input().Ref(), {"ExtractMembers"})); maybeChain && self.SwitchHandler().Body().Ref().IsCallable("Member")
            && &self.SwitchHandler().Body().Ref().Head() == self.SwitchHandler().Args().Arg(0).Raw()) {
            const auto member = self.SwitchHandler().Body().Ref().Tail().Content();
            const auto& chain = maybeChain.Cast();
            if (const auto init = ExtractMemberFromLiteral(chain.InitHandler().Ref(), member), update = ExtractMemberFromLiteral(chain.UpdateHandler().Ref(), member);
                init && update && init->IsCallable("Bool") && !FromString<bool>(init->Tail().Content())) {
                if (std::map<std::string_view, TExprNode::TPtr> usedFields;
                    HaveFieldsSubset(update, chain.UpdateHandler().Args().Arg(1).Ref(), usedFields, *optCtx.ParentsMap, false) && !usedFields.empty()
                    && IsPasstroughtFields(usedFields, self.InitHandler().Ref()) && IsPasstroughtFields(usedFields, self.UpdateHandler().Ref())) {
                    YQL_CLOG(DEBUG, Core) << "Fuse " << node->Content() << " with " << node->Head().Content();
                    auto lambda = ctx.Builder(chain.Pos())
                        .Lambda()
                            .Param("item")
                            .Param("state")
                            .ApplyPartial(chain.UpdateHandler().Args().Ptr(), std::move(update))
                                .With(0, "item")
                                .With(1, "state")
                            .Seal()
                        .Seal().Build();

                    return Build<TCoCondense1>(ctx, self.Pos())
                        .Input(chain.Input())
                        .InitHandler(ctx.DeepCopyLambda(self.InitHandler().Ref()))
                        .SwitchHandler(std::move(lambda))
                        .UpdateHandler(ctx.DeepCopyLambda(self.UpdateHandler().Ref()))
                        .Done().Ptr();
                }
            }
        }

        return node;
    };

    map["FinalizeByKey"] = map["CombineByKey"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head())) {
            return node;
        }

        const TCoLambda preMap(node->ChildPtr(1));
        if (const TMaybeNode<TCoFlatMapBase> maybeFlatmap = node->HeadPtr()) {
            if (const auto flatMap = maybeFlatmap.Cast(); IsJustOrSingleAsList(flatMap.Lambda().Body().Ref()) && flatMap.Input().Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional) {
                YQL_CLOG(DEBUG, Core) << "Fuse " << node->Content() << " over " <<  node->Head().Content();
                auto children = node->ChildrenList();
                children.front() = flatMap.Input().Ptr();
                children[1] = Build<TCoLambda>(ctx, preMap.Pos())
                    .Args({"item"})
                    .Body<TExprApplier>()
                        .Apply(preMap.Body())
                        .With(preMap.Args().Arg(0), TExprBase(flatMap.Lambda().Body().Ref().HeadPtr()))
                        .With(flatMap.Lambda().Args().Arg(0), "item")
                        .Build()
                    .Done().Ptr();

                return ctx.ChangeChildren(*node,std::move(children));
            }
        }

        if (IsConstMapLambda(preMap) &&
            (
                IsDepended(node->Child(2)->Tail(), *node->Child(2)->Head().Child(0)) ||
                IsDepended(node->Child(3)->Tail(), *node->Child(3)->Head().Child(1)) ||
                IsDepended(node->Child(4)->Tail(), *node->Child(4)->Head().Child(1))
            )
        ) {
            return PropagateConstPremapIntoCombineByKey(*node, ctx);
        }

        if (preMap.Body().Ref().IsCallable("ToList")) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " premap ToList elimination.";
            auto newPreMapLambda = ctx.DeepCopyLambda(preMap.Ref());
            newPreMapLambda = ctx.ChangeChild(*newPreMapLambda, 1, newPreMapLambda->Tail().HeadPtr());
            return ctx.ChangeChild(*node, 1U, std::move(newPreMapLambda));
        }

        return node;
    };

    map["Reverse"] = std::bind(&OptimizeReverse, _1, _2, _3);

    map["Visit"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head())) {
            return node;
        }

        if (!TCoVisit::Match(&node->Head())) {
            return node;
        }

        // Outer variant index to inner index + lambda
        THashMap<TStringBuf, std::vector<std::pair<TStringBuf, TExprNode::TPtr>>> innerLambdas;
        TExprNode::TPtr defValue;
        TStringBuf defOutIndex;
        TSet<TString> defInnerIndicies;

        const auto& innerVisit = node->Head();

        if (innerVisit.ChildrenSize() % 2 == 0) {
            // Has default value
            auto innerVarType = innerVisit.GetTypeAnn()->Cast<TVariantExprType>();
            if (innerVarType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Struct) {
                for (auto item: innerVarType->GetUnderlyingType()->Cast<TStructExprType>()->GetItems()) {
                    defInnerIndicies.emplace(item->GetName());
                }
            }
            else {
                for (size_t i = 0; i < innerVarType->GetUnderlyingType()->Cast<TTupleExprType>()->GetSize(); ++i) {
                    defInnerIndicies.emplace(ToString(i));
                }
            }
        }

        for (ui32 index = 1; index < innerVisit.ChildrenSize(); ++index) {
            if (innerVisit.Child(index)->IsAtom()) {
                const auto itemIndex = innerVisit.Child(index)->Content();
                defInnerIndicies.erase(TString(itemIndex));
                ++index;
                auto lambda = innerVisit.ChildPtr(index);
                if (auto var = TMaybeNode<TCoVariant>(lambda->Child(1))) {
                    innerLambdas[var.Cast().Index().Value()].emplace_back(itemIndex, std::move(lambda));
                }
                else {
                    return node;
                }
            }
            else {
                if (auto var = TMaybeNode<TCoVariant>(innerVisit.Child(index))) {
                    defOutIndex = var.Cast().Index().Value();
                    defValue = var.Cast().Item().Ptr();
                }
                else {
                    return node;
                }
            }
        }

        if (innerLambdas.contains(defOutIndex)) {
            return node;
        }

        for (ui32 index = 1; index < node->ChildrenSize(); ++index) {
            if (node->Child(index)->IsAtom()) {
                const auto itemIndex = node->Child(index)->Content();
                if (!innerLambdas.contains(itemIndex) && defOutIndex != itemIndex) {
                    return node;
                }
            }
        }

        YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
        return ctx.Builder(node->Pos())
            .Callable("Visit")
                .Add(0, innerVisit.HeadPtr())
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    ui32 index = 0;
                    for (ui32 i = 1; i < node->ChildrenSize(); ++i) {
                        if (node->Child(i)->IsAtom()) {
                            const auto itemIndex = node->Child(i)->Content();
                            auto lambda = node->ChildPtr(i + 1);
                            if (auto p = innerLambdas.FindPtr(itemIndex)) {
                                for (auto pr: *p) {
                                    auto itemLambda = ctx.Builder(lambda->Pos())
                                        .Lambda()
                                            .Param("item")
                                            .Apply(*lambda)
                                                .With(0)
                                                    .ApplyPartial(pr.second->HeadPtr(), pr.second->Child(1)->ChildPtr(TCoVariant::idx_Item))
                                                        .With(0, "item")
                                                    .Seal()
                                                .Done()
                                            .Seal()
                                        .Seal()
                                        .Build();

                                    parent.Atom(++index, pr.first);
                                    parent.Add(++index, std::move(itemLambda));
                                }
                            }
                            else {
                                lambda = ctx.Builder(lambda->Pos())
                                    .Lambda()
                                        .Param("item")
                                        .Apply(*lambda)
                                            .With(0, defValue)
                                        .Seal()
                                    .Seal()
                                    .Build();
                                for (auto& newItemIndex: defInnerIndicies) {
                                    parent.Atom(++index, newItemIndex);
                                    parent.Add(++index, lambda);
                                }
                            }
                            ++i;
                        }
                        else {
                            parent.Add(++index, node->ChildPtr(i));
                        }
                    }
                    return parent;
                })
            .Seal()
            .Build();
    };

    map["VariantItem"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head())) {
            return node;
        }

        if (!TCoVisit::Match(&node->Head())) {
            return node;
        }

        const auto& visit = node->Head();
        for (ui32 index = 1; index < visit.ChildrenSize(); ++index) {
            if (visit.Child(index)->IsAtom()) {
                ++index;
                if (!TCoVariant::Match(visit.Child(index)->Child(1))) {
                    return node;
                }
            }
            else {
                if (!TCoVariant::Match(visit.Child(index))) {
                    return node;
                }
            }
        }

        YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
        return ctx.Builder(visit.Pos())
            .Callable("Visit")
                .Add(0, visit.HeadPtr())
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 1; i < visit.ChildrenSize(); ++i) {
                        if (visit.Child(i)->IsAtom()) {
                            parent.Add(i, visit.ChildPtr(i));
                            auto visitLambda = visit.Child(i + 1);
                            parent.Lambda(i + 1, visitLambda->Pos())
                                .Param("item")
                                .ApplyPartial(visitLambda->HeadPtr(), visitLambda->Child(1)->ChildPtr(TCoVariant::idx_Item))
                                    .With(0, "item")
                                .Seal()
                                .Seal();
                            ++i;
                        }
                        else {
                            parent.Add(i, visit.Child(i)->ChildPtr(TCoVariant::idx_Item));
                        }
                    }
                    return parent;
                })
            .Seal()
            .Build();
    };

    map["SkipNullMembers"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head())) {
            return node;
        }

        const auto skipNullMembers = TCoSkipNullMembers(node);
        if (!skipNullMembers.Members()) {
            return node;
        }

        if (auto maybeFlatmap = skipNullMembers.Input().Maybe<TCoFlatMapBase>()) {
            auto flatmap = maybeFlatmap.Cast();

            TMaybe<THashSet<TStringBuf>> passthroughFields;
            if (IsPassthroughFlatMap(flatmap, &passthroughFields)
                && !IsTablePropsDependent(flatmap.Lambda().Body().Ref())
                // SkipNullMembers doesn't support optional items
                && flatmap.Lambda().Args().Arg(0).Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional)
            {
                bool hasAllMembers = true;
                if (passthroughFields) {
                    for (const auto& member : skipNullMembers.Members().Cast()) {
                        if (!passthroughFields->contains(member)) {
                            hasAllMembers = false;
                            break;
                        }
                    }
                }

                if (hasAllMembers) {
                    YQL_CLOG(DEBUG, Core) << node->Content() << "OverFlatmap";
                    return ctx.Builder(flatmap.Pos())
                        .Callable(flatmap.CallableName())
                            .Callable(0, TCoSkipNullMembers::CallableName())
                                .Add(0, flatmap.Input().Ptr())
                                .Add(1, skipNullMembers.Members().Cast().Ptr())
                            .Seal()
                            .Add(1, flatmap.Lambda().Ptr())
                        .Seal()
                        .Build();
                }
            }
        }

        if (node->Head().IsCallable({"ForwardList", "FromFlow"})) {
            YQL_CLOG(DEBUG, Core) << "Swap " << node->Content() << " with " << node->Head().Content();
            return ctx.SwapWithHead(*node);
        }

        return node;
    };

    map["Exists"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (!optCtx.IsSingleUsage(node->Head())) {
            return node;
        }

        if (node->Head().IsCallable("Lookup")) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
            return ctx.RenameNode(node->Head(), "Contains");
        }

        return node;
    };

    map["Sort"] = map["Top"] = map["TopSort"] =
    map["TakeWhile"] = map["SkipWhile"] = map["TakeWhileInclusive"] = map["SkipWhileInclusive"] =
    map["SkipNullElements"] = map["FilterNullMembers"] = map["FilterNullElements"] = map["ExtractMembers"] =
    map["Chain1Map"] = map["Fold1Map"] = map["FoldMap"] =
    map["Map"] = map["OrderedMap"] = map["MultiMap"] = map["OrderedMultiMap"] =
        [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx)
    {
        if (!optCtx.IsSingleUsage(node->Head())) {
            return node;
        }

        if (node->Head().IsCallable({"ForwardList", "FromFlow"})) {
            YQL_CLOG(DEBUG, Core) << "Swap " << node->Content() << " with " << node->Head().Content();
            return ctx.SwapWithHead(*node);
        }

        return node;
    };


    map["ForwardList"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (optCtx.HasParent(*node) && !optCtx.IsSingleUsage(*node)) {
            YQL_ENSURE(optCtx.ParentsMap);
            auto parentsIt = optCtx.ParentsMap->find(node.Get());
            YQL_ENSURE(parentsIt != optCtx.ParentsMap->cend());
            if (AnyOf(parentsIt->second, [](const TExprNode* parent) { return !parent->IsCallable({"Length", "HasItems"}); })) {
                YQL_CLOG(DEBUG, Core) << "Collect list instead of " << node->Content();
                return ctx.RenameNode(*node, "Collect");
            }
        }

        return node;
    };

}

}
