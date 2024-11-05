#include "type_ann_core.h"
#include "type_ann_expr.h"
#include "type_ann_impl.h"
#include "type_ann_types.h"
#include "ydb/library/yql/core/yql_opt_utils.h"

namespace NYql {
namespace NTypeAnnImpl {
    const TTypeAnnotationNode* MakeItemDescriptorType(TExprContext& ctx) {
        TVector<const TItemExprType*> items = {
            ctx.MakeType<TItemExprType>("Name", ctx.MakeType<TDataExprType>(EDataSlot::String)),
            ctx.MakeType<TItemExprType>("Type", MakeTypeHandleResourceType(ctx))
        };

        return ctx.MakeType<TStructExprType>(items);
    }

    const TTypeAnnotationNode* MakeDictDescriptorType(TExprContext& ctx) {
        auto res = MakeTypeHandleResourceType(ctx);
        TVector<const TItemExprType*> items = {
            ctx.MakeType<TItemExprType>("Key", res),
            ctx.MakeType<TItemExprType>("Payload", res)
        };

        return ctx.MakeType<TStructExprType>(items);
    }

    const TTypeAnnotationNode* MakeTaggedDescriptorType(TExprContext& ctx) {
        TVector<const TItemExprType*> items = {
            ctx.MakeType<TItemExprType>("Base", MakeTypeHandleResourceType(ctx)),
            ctx.MakeType<TItemExprType>("Tag", ctx.MakeType<TDataExprType>(EDataSlot::String)),
        };

        return ctx.MakeType<TStructExprType>(items);
    }

    const TTypeAnnotationNode* MakeArgumentDescriptorType(TExprContext& ctx) {
        TVector<const TItemExprType*> items = {
            ctx.MakeType<TItemExprType>("Type", MakeTypeHandleResourceType(ctx)),
            ctx.MakeType<TItemExprType>("Name", ctx.MakeType<TDataExprType>(EDataSlot::String)),
            ctx.MakeType<TItemExprType>("Flags", ctx.MakeType<TListExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String))),
        };

        return ctx.MakeType<TStructExprType>(items);
    }

    const TTypeAnnotationNode* MakeCallabledDescriptorType(TExprContext& ctx) {
        TVector<const TItemExprType*> items = {
            ctx.MakeType<TItemExprType>("Result", MakeTypeHandleResourceType(ctx)),
            ctx.MakeType<TItemExprType>("Payload", ctx.MakeType<TDataExprType>(EDataSlot::String)),
            ctx.MakeType<TItemExprType>("OptionalArgumentsCount", ctx.MakeType<TDataExprType>(EDataSlot::Uint32)),
            ctx.MakeType<TItemExprType>("Arguments", ctx.MakeType<TListExprType>(MakeArgumentDescriptorType(ctx))),
        };

        return ctx.MakeType<TStructExprType>(items);
    }

    IGraphTransformer::TStatus EnsureCodeOrListOfCode(TExprNode::TPtr& node, TExprContext& ctx) {
        if (!node->GetTypeAnn()) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Expected code or list of code, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        auto resType = MakeCodeResourceType(ctx);
        if (IsSameAnnotation(*node->GetTypeAnn(), *resType)) {
            // wrap with AsList
            node = ctx.NewCallable(node->Pos(), "AsList", { node });
            return IGraphTransformer::TStatus::Repeat;
        }

        auto lstType = ctx.MakeType<TListExprType>(resType);
        if (IsSameAnnotation(*node->GetTypeAnn(), *lstType)) {
            return IGraphTransformer::TStatus::Ok;
        }

        if (node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::EmptyList) {
            node = ctx.NewCallable(node->Pos(), "List", { ExpandType(node->Pos(), *lstType, ctx) });
            return IGraphTransformer::TStatus::Repeat;
        }

        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Expected code or list of code, but got: " << *node->GetTypeAnn()));
        return IGraphTransformer::TStatus::Error;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Generic>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto genType = ctx.Expr.MakeType<TGenericExprType>();
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(genType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Resource>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto tag = input->Child(0)->Content();
        auto resType = ctx.Expr.MakeType<TResourceExprType>(tag);
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(resType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Tagged>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto underlyingType = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureInspectableType(input->Child(0)->Pos(), *underlyingType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto tag = input->Child(1)->Content();

        auto taggedType = ctx.Expr.MakeType<TTaggedExprType>(underlyingType, tag);
        if (!taggedType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(taggedType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Error>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(3), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        ui32 row = 0;
        if (!TryFromString(input->Child(0)->Content(), row)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to convert to integer: " << input->Child(0)->Content()));
            return IGraphTransformer::TStatus::Error;
        }

        ui32 column = 0;
        if (!TryFromString(input->Child(1)->Content(), column)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to convert to integer: " << input->Child(1)->Content()));
            return IGraphTransformer::TStatus::Error;
        }

        auto file = TString(input->Child(2)->Content());
        auto message = input->Child(3)->Content();
        auto resType = ctx.Expr.MakeType<TErrorExprType>(TIssue(TPosition(column, row, file), message));
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(resType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Data>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto name = input->Child(0)->Content();
        auto slot = NKikimr::NUdf::FindDataSlot(name);
        if (!slot) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Unknown data type: " << name));
            return IGraphTransformer::TStatus::Error;
        } else if (*slot == EDataSlot::Decimal) {
            if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto dataType = ctx.Expr.MakeType<TDataExprParamsType>(*slot , input->Child(1)->Content(), input->Child(2)->Content());
            if (!dataType->Validate(input->Pos(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(dataType));
        } else {
            if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto dataType = ctx.Expr.MakeType<TDataExprType>(*slot);
            input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(dataType));
        }
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::List>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto itemType = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureInspectableType(input->Child(0)->Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto listType = ctx.Expr.MakeType<TListExprType>(itemType);
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(listType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Stream>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto itemType = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureInspectableType(input->Child(0)->Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto listType = ctx.Expr.MakeType<TStreamExprType>(itemType);
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(listType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Flow>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto itemType = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureInspectableType(input->Child(0)->Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto listType = ctx.Expr.MakeType<TFlowExprType>(itemType);
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(listType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Block>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto itemType = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureInspectableType(input->Child(0)->Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto type = ctx.Expr.MakeType<TBlockExprType>(itemType);
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(type));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Scalar>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto itemType = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureInspectableType(input->Child(0)->Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto type = ctx.Expr.MakeType<TScalarExprType>(itemType);
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(type));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Optional>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto itemType = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureInspectableType(input->Child(0)->Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto optionalType = ctx.Expr.MakeType<TOptionalExprType>(itemType);
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(optionalType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Tuple>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        TTypeAnnotationNode::TListType items;
        for (size_t i = 0; i < input->ChildrenSize(); ++i) {
            auto& child = input->ChildRef(i);
            if (auto status = EnsureTypeRewrite(child, ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
            auto elemType = child->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            if (!EnsureInspectableType(child->Pos(), *elemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            items.push_back(elemType);
        }

        auto tupleType = ctx.Expr.MakeType<TTupleExprType>(items);
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(tupleType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Multi>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        TTypeAnnotationNode::TListType items;
        for (size_t i = 0; i < input->ChildrenSize(); ++i) {
            auto& child = input->ChildRef(i);
            if (auto status = EnsureTypeRewrite(child, ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            auto elemType = child->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            if (!EnsureInspectableType(child->Pos(), *elemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            items.push_back(elemType);
        }

        auto multiType = ctx.Expr.MakeType<TMultiExprType>(items);
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(multiType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Struct>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        TVector<const TItemExprType*> items;
        for (size_t i = 0; i < input->ChildrenSize(); ++i) {
            auto& child = input->ChildRef(i);
            if (!EnsureTupleSize(*child, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto nameNode = child->Child(0);
            if (!EnsureAtom(*nameNode, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto& typeNode = child->ChildRef(1);
            if (auto status = EnsureTypeRewrite(typeNode, ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                if (status == IGraphTransformer::TStatus::Repeat) {
                    child = ctx.Expr.ShallowCopy(*child);
                }
                return status;
            }

            auto memberType = typeNode->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            if (!EnsureInspectableType(typeNode->Pos(), *memberType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            items.push_back(ctx.Expr.MakeType<TItemExprType>(nameNode->Content(), memberType));
        }

        auto structType = ctx.Expr.MakeType<TStructExprType>(items);
        if (!structType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto less = [](const TExprNode::TPtr& left, const TExprNode::TPtr& right) {
            return left->Child(0)->Content() < right->Child(0)->Content();
        };

        if (!IsSorted(input->Children().begin(), input->Children().end(), less)) {
            auto list = input->ChildrenList();
            Sort(list.begin(), list.end(), less);
            output = ctx.Expr.ChangeChildren(*input, std::move(list));
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(structType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Dict>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->ChildRef(0), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto keyType = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        auto payloadType = input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureInspectableType(input->Child(1)->Pos(), *payloadType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto dictType = ctx.Expr.MakeType<TDictExprType>(keyType, payloadType);
        if (!dictType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(dictType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Void>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto voidType = ctx.Expr.MakeType<TVoidExprType>();
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(voidType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Null>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto voidType = ctx.Expr.MakeType<TNullExprType>();
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(voidType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Unit>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto unitType = ctx.Expr.MakeType<TUnitExprType>();
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(unitType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::EmptyList>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto emptyListType = ctx.Expr.MakeType<TEmptyListExprType>();
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(emptyListType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::EmptyDict>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto emptyDictType = ctx.Expr.MakeType<TEmptyDictExprType>();
        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(emptyDictType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::OptionalItem>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto resType = MakeTypeHandleResourceType(ctx.Expr);
        if (input->Child(0)->GetTypeAnn() == resType) {
            input->SetTypeAnn(resType);
            return IGraphTransformer::TStatus::Ok;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() == ETypeAnnotationKind::Null) {
            output = ExpandType(input->Pos(), *ctx.Expr.MakeType<TUnitExprType>(), ctx.Expr);
            return IGraphTransformer::TStatus::Repeat;
        }

        if (type->GetKind() != ETypeAnnotationKind::Optional) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(0)->Pos()),
                TStringBuilder() << "Expected optional type, but got: " << *type));
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *type->Cast<TOptionalExprType>()->GetItemType(), ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::ListItem>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto resType = MakeTypeHandleResourceType(ctx.Expr);
        if (input->Child(0)->GetTypeAnn() == resType) {
            input->SetTypeAnn(resType);
            return IGraphTransformer::TStatus::Ok;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() == ETypeAnnotationKind::EmptyList) {
            output = ExpandType(input->Pos(), *ctx.Expr.MakeType<TUnitExprType>(), ctx.Expr);
            return IGraphTransformer::TStatus::Repeat;
        }

        if (type->GetKind() != ETypeAnnotationKind::List) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(0)->Pos()),
                TStringBuilder() << "Expected list type, but got: " << *type));
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *type->Cast<TListExprType>()->GetItemType(), ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::StreamItem>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto resType = MakeTypeHandleResourceType(ctx.Expr);
        if (input->Child(0)->GetTypeAnn() == resType) {
            input->SetTypeAnn(resType);
            return IGraphTransformer::TStatus::Ok;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Stream) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(0)->Pos()),
                TStringBuilder() << "Expected stream type, but got: " << *type));
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *type->Cast<TStreamExprType>()->GetItemType(), ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::TupleElement>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto type = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Tuple) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(0)->Pos()),
                TStringBuilder() << "Expected tuple type, but got: " << *type));
            return IGraphTransformer::TStatus::Error;
        }

        ui32 index = 0;
        if (!TryFromString(input->Child(1)->Content(), index)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to convert to integer: " << input->Child(1)->Content()));
            return IGraphTransformer::TStatus::Error;
        }

        auto tupleType = type->Cast<TTupleExprType>();
        if (index >= tupleType->GetSize()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Incorrect tuple index: " << index
                << ", tuple size: " << tupleType->GetSize()));
            return IGraphTransformer::TStatus::Error;

        }

        output = ExpandType(input->Pos(), *tupleType->GetItems()[index], ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::StructMember>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto type = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Struct) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(0)->Pos()), TStringBuilder() << "Expected struct type, but got: "
                << *type));
            return IGraphTransformer::TStatus::Error;
        }

        auto structType = type->Cast<TStructExprType>();
        const TItemExprType* foundMember = nullptr;
        for (auto item : structType->GetItems()) {
            if (item->GetName() == input->Child(1)->Content()) {
                foundMember = item;
                break;
            }
        }

        if (!foundMember) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Incorrect member name: "
                << input->Child(1)->Content()));
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *foundMember->GetItemType(), ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::DictKey>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() == ETypeAnnotationKind::EmptyDict) {
            output = ExpandType(input->Pos(), *ctx.Expr.MakeType<TUnitExprType>(), ctx.Expr);
            return IGraphTransformer::TStatus::Repeat;
        }

        if (type->GetKind() != ETypeAnnotationKind::Dict) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(0)->Pos()), TStringBuilder() << "Expected dict type, but got: "
                << *type));
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *type->Cast<TDictExprType>()->GetKeyType(), ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::DictPayload>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() == ETypeAnnotationKind::EmptyDict) {
            output = ExpandType(input->Pos(), *ctx.Expr.MakeType<TUnitExprType>(), ctx.Expr);
            return IGraphTransformer::TStatus::Repeat;
        }

        if (type->GetKind() != ETypeAnnotationKind::Dict) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(0)->Pos()), TStringBuilder() << "Expected dict type, but got: "
                << *type));
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *type->Cast<TDictExprType>()->GetPayloadType(), ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Callable>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleMinSize(*input->Child(0), 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleMaxSize(*input->Child(0), 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        ui32 optionalArgsCount = 0;
        if (input->Child(0)->ChildrenSize() > 0) {
            if (!EnsureAtom(*input->Child(0)->Child(0), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!TryFromString(input->Child(0)->Child(0)->Content(), optionalArgsCount)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to convert to integer: "
                    << input->Child(0)->Child(0)->Content()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        TString payload;
        if (input->Child(0)->ChildrenSize() > 1) {
            if (!EnsureAtom(*input->Child(0)->Child(1), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            payload = input->Child(0)->Child(1)->Content();
        }

        const TTypeAnnotationNode* resultType = nullptr;
        TVector<TCallableExprType::TArgumentInfo> arguments;
        for (ui32 index = 1; index < input->ChildrenSize(); ++index) {
            auto child = input->Child(index);
            if (!EnsureTupleMinSize(*child, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureTupleMaxSize(*child, index == 1 ? 1 : 3, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto& typeChild = child->ChildRef(0);
            if (auto status = EnsureTypeRewrite(typeChild, ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                if (status == IGraphTransformer::TStatus::Repeat) {
                    input->ChildRef(index) = ctx.Expr.ShallowCopy(*child);
                }
                return status;
            }

            auto type = typeChild->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            if (!EnsureComputableType(child->Pos(), *type, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (index == 1) {
                resultType = type;
            } else {
                TCallableExprType::TArgumentInfo arg;
                arg.Type = type;
                if (child->ChildrenSize() > 1) {
                    auto nameChild = child->Child(1);
                    if (!EnsureAtom(*nameChild, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    arg.Name = nameChild->Content();
                }

                if (child->ChildrenSize() > 2) {
                    auto flagsChild = child->Child(2);
                    if (!EnsureAtom(*flagsChild, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (!TryFromString<ui64>(flagsChild->Content(), arg.Flags)) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(flagsChild->Pos()), TStringBuilder() << "Expected unsigned number, but got: "
                            << flagsChild->Content()));
                    }
                }

                arguments.push_back(arg);
            }
        }

        auto callableType = ctx.Expr.MakeType<TCallableExprType>(resultType, arguments, optionalArgsCount, payload);
        if (!callableType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(callableType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::CallableResult>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Callable) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(0)->Pos()), TStringBuilder() << "Expected callable type, but got: "
                << *type));
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *type->Cast<TCallableExprType>()->GetReturnType(), ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::CallableArgument>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto type = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Callable) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(0)->Pos()), TStringBuilder() << "Expected callable type, but got: "
                << *type));
            return IGraphTransformer::TStatus::Error;
        }

        ui32 index = 0;
        if (!TryFromString(input->Child(1)->Content(), index)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to convert to integer: " << input->Child(1)->Content()));
            return IGraphTransformer::TStatus::Error;
        }

        auto callableType = type->Cast<TCallableExprType>();
        if (index >= callableType->GetArgumentsSize()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Incorrect argument index: " << index
                << ", total arguments: " << callableType->GetArgumentsSize()));
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *callableType->GetArguments()[index].Type, ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus ParseTypeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TMemoryPool pool(4096);
        TIssues issues;
        auto parsedType = ParseType(input->Child(0)->Content(), pool, issues, ctx.Expr.GetPosition(input->Child(0)->Pos()));
        if (!parsedType) {
            ctx.Expr.IssueManager.AddIssues(issues);
            return IGraphTransformer::TStatus::Error;
        }

        auto inputPos = ctx.Expr.GetPosition(input->Pos());
        auto astRoot = TAstNode::NewList(inputPos, pool,
            TAstNode::NewList(inputPos, pool,
                TAstNode::NewLiteralAtom(inputPos, TStringBuf("return"), pool), parsedType));
        TExprNode::TPtr exprRoot;
        if (!CompileExpr(*astRoot, exprRoot, ctx.Expr, nullptr, nullptr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // TODO: Collect type annotation directly from AST.
        NYql::TTypeAnnotationContext cleanTypes;
        auto callableTransformer = CreateExtCallableTypeAnnotationTransformer(cleanTypes);
        auto typeTransformer = CreateTypeAnnotationTransformer(callableTransformer, cleanTypes);
        if (InstantTransform(*typeTransformer, exprRoot, ctx.Expr) != IGraphTransformer::TStatus::Ok) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(exprRoot->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FormatTypeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto resType = MakeTypeHandleResourceType(ctx.Expr);
        if (input->Child(0)->GetTypeAnn() != resType) {
            if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FormatTypeDiffWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto resType = MakeTypeHandleResourceType(ctx.Expr);
        if (input->Child(0)->GetTypeAnn() != resType) {
            if (auto status = EnsureTypeRewrite(input->ChildRef(0), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }
        if (input->Child(1)->GetTypeAnn() != resType) {
            if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }
        if (!EnsureAtom(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!TryFromString<bool>(input->Child(2)->Content())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->ChildPtr(2)->Pos()), TStringBuilder() << "Expected boolean value, but got: " << input->ChildPtr(2)->Content()));
            return IGraphTransformer::TStatus::Error;
        }
        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus TypeHandleWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SerializeTypeHandleWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Yson));
        return IGraphTransformer::TStatus::Ok;
    };

    IGraphTransformer::TStatus ParseTypeHandleWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(*input->Child(0), EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::AddMember>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->ChildRef(2), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureStructType(input->Child(0)->Pos(), *type, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto structType = type->Cast<TStructExprType>();
        TVector<const TItemExprType*> items = structType->GetItems();
        auto newItem = ctx.Expr.MakeType<TItemExprType>(input->Child(1)->Content(),
            input->Child(2)->GetTypeAnn()->Cast<TTypeExprType>()->GetType());
        if (!newItem->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        items.push_back(newItem);
        auto newStructType = ctx.Expr.MakeType<TStructExprType>(items);
        if (!newStructType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *newStructType, ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus RemoveMemberImpl(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx, bool force) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Child(1)->Content().empty()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), "Empty member name is not allowed"));
            return IGraphTransformer::TStatus::Error;
        }

        auto type = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureStructType(input->Child(0)->Pos(), *type, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto structType = type->Cast<TStructExprType>();
        TVector<const TItemExprType*> items;
        items.reserve(Max<ui64>(structType->GetSize(), 1) - 1);
        bool found = false;
        for (auto& item : structType->GetItems()) {
            if (item->GetName() == input->Child(1)->Content()) {
                found = true;
                continue;
            }

            items.push_back(item);
        }

        if (!found && !force) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() <<
                "Unknown member name: " << input->Child(1)->Content()));
            return IGraphTransformer::TStatus::Error;
        }

        auto newStructType = ctx.Expr.MakeType<TStructExprType>(items);
        if (!newStructType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *newStructType, ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::RemoveMember>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return RemoveMemberImpl(input, output, ctx, false);
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::ForceRemoveMember>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        return RemoveMemberImpl(input, output, ctx, true);
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::FlattenMembers>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        TVector<const TItemExprType*> allItems;
        for (auto& child : input->Children()) {
            if (!EnsureTupleSize(*child, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto prefix = child->Child(0);
            if (!EnsureAtom(*prefix, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (auto status = EnsureTypeRewrite(child->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            auto type = child->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            const TStructExprType* structType = nullptr;
            bool optional = false;
            if (!EnsureStructOrOptionalStructType(child->Child(1)->Pos(), *type, optional, structType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            for (auto& field : structType->GetItems()) {
                auto itemType = field->GetItemType();
                if (optional && itemType->GetKind() != ETypeAnnotationKind::Optional) {
                    itemType = ctx.Expr.MakeType<TOptionalExprType>(itemType);
                }
                auto newField = ctx.Expr.MakeType<TItemExprType>(
                    TString::Join(prefix->Content(), field->GetName()),
                    itemType
                    );
                allItems.push_back(newField);
            }
        }

        auto newStructType = ctx.Expr.MakeType<TStructExprType>(allItems);
        if (!newStructType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *newStructType, ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    template <>
    IGraphTransformer::TStatus TypeWrapper<ETypeAnnotationKind::Variant>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto underlyingType = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureComputableType(input->Child(0)->Pos(), *underlyingType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto varType = ctx.Expr.MakeType<TVariantExprType>(underlyingType);
        if (!varType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(varType));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus TypeArgWrapper<ETypeArgument::VariantUnderlying>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto resType = MakeTypeHandleResourceType(ctx.Expr);
        if (input->Child(0)->GetTypeAnn() == resType) {
            input->SetTypeAnn(resType);
            return IGraphTransformer::TStatus::Ok;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Variant) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(0)->Pos()), TStringBuilder() << "Expected variant type, but got: "
                << *type));
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *type->Cast<TVariantExprType>()->GetUnderlyingType(), ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus TypeKindWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus SplitTypeHandleWrapper<ETypeAnnotationKind::Data>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String)));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Data>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto itemType = input->Child(0)->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (!EnsureSpecificDataType(input->Child(0)->Pos(), *itemType, EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Optional>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::List>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Stream>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus SplitTypeHandleWrapper<ETypeAnnotationKind::Tuple>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(MakeTypeHandleResourceType(ctx.Expr)));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Tuple>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto resType = MakeTypeHandleResourceType(ctx.Expr);
        auto listType = ctx.Expr.MakeType<TListExprType>(resType);
        auto status = TryConvertTo(input->ChildRef(0), *listType, ctx.Expr);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(resType);
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus SplitTypeHandleWrapper<ETypeAnnotationKind::Struct>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(MakeItemDescriptorType(ctx.Expr)));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Struct>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto itemType = MakeItemDescriptorType(ctx.Expr);
        auto listType = ctx.Expr.MakeType<TListExprType>(itemType);
        auto status = TryConvertTo(input->ChildRef(0), *listType, ctx.Expr);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus SplitTypeHandleWrapper<ETypeAnnotationKind::Dict>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeDictDescriptorType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Dict>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus SplitTypeHandleWrapper<ETypeAnnotationKind::Resource>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Resource>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(*input->Child(0), EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus SplitTypeHandleWrapper<ETypeAnnotationKind::Tagged>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTaggedDescriptorType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Tagged>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(*input->Child(1), EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Variant>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Void>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Null>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::EmptyList>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::EmptyDict>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Pg>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(*input->Child(0), EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus SplitTypeHandleWrapper<ETypeAnnotationKind::Pg>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CallableArgumentWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureMaxArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->ChildrenSize() > 1) {
            auto optStrType = ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
            auto status = TryConvertTo(input->ChildRef(1), *optStrType, ctx.Expr);
            if (status == IGraphTransformer::TStatus::Error) {
                return status;
            }
        }

        if (input->ChildrenSize() > 2) {
            auto optListStrType = ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TListExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String)));
            auto status = TryConvertTo(input->ChildRef(2), *optListStrType, ctx.Expr);
            if (status == IGraphTransformer::TStatus::Error) {
                return status;
            }
        }

        output = ctx.Expr.Builder(input->Pos())
            .Callable("AsStruct")
                .List(0)
                    .Atom(0, "Type")
                    .Add(1, input->ChildPtr(0))
                .Seal()
                .List(1)
                    .Atom(0, "Name")
                    .Callable(1, "Coalesce")
                        .Add(0, input->ChildrenSize() > 1 ? input->ChildPtr(1) : ctx.Expr.NewCallable(TPosition(), "Null", {}))
                        .Callable(1, "String")
                            .Atom(0, "")
                        .Seal()
                    .Seal()
                .Seal()
                .List(2)
                    .Atom(0, "Flags")
                    .Callable(1, "Coalesce")
                        .Add(0, input->ChildrenSize() > 2 ? input->ChildPtr(2) : ctx.Expr.NewCallable(TPosition(), "Null", {}))
                        .Callable(1, "List")
                            .Callable(0, "ListType")
                                .Callable(0, "DataType")
                                    .Atom(0, "String")
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    template <>
    IGraphTransformer::TStatus SplitTypeHandleWrapper<ETypeAnnotationKind::Callable>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeCallabledDescriptorType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeTypeHandleWrapper<ETypeAnnotationKind::Callable>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureMaxArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTypeHandleResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto listType = ctx.Expr.MakeType<TListExprType>(MakeArgumentDescriptorType(ctx.Expr));
        auto status = TryConvertTo(input->ChildRef(1), *listType, ctx.Expr);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (input->ChildrenSize() > 2) {
            auto optUi32Type = ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint32));
            status = TryConvertTo(input->ChildRef(2), *optUi32Type, ctx.Expr);
            if (status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        if (input->ChildrenSize() > 3) {
            auto optStrType = ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
            status = TryConvertTo(input->ChildRef(3), *optStrType, ctx.Expr);
            if (status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        input->SetTypeAnn(MakeTypeHandleResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FormatCodeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureCodeResourceType(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ReprCodeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsurePersistable(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeCodeResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RestartEvaluationWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(input);
        Y_UNUSED(output);
        ctx.Expr.Step.Repeat(TExprStep::ExprEval);
        return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
    }

    IGraphTransformer::TStatus EvaluateExprIfPureWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Head().GetTypeAnn()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Lambda is unexpected here"));
            return IGraphTransformer::TStatus::Error;
        }

        output = IsPureIsolatedLambda(input->Head()) ? ctx.Expr.RenameNode(*input, "EvaluateExpr") : input->HeadPtr();
        return IGraphTransformer::TStatus::Repeat;
    }

    template <>
    IGraphTransformer::TStatus MakeCodeWrapper<TExprNode::World>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(MakeCodeResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeCodeWrapper<TExprNode::Atom>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::String);
        auto status = TryConvertTo(input->ChildRef(0), *expectedType, ctx.Expr);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(MakeCodeResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeCodeWrapper<TExprNode::List>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        IGraphTransformer::TStatus status = IGraphTransformer::TStatus::Ok;
        for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
            auto childStatus = EnsureCodeOrListOfCode(input->ChildRef(i), ctx.Expr);
            status = status.Combine(childStatus);
        }

        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(MakeCodeResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeCodeWrapper<TExprNode::Callable>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::String);
        auto status = TryConvertTo(input->ChildRef(0), *expectedType, ctx.Expr);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
            auto childStatus = EnsureCodeOrListOfCode(input->ChildRef(i), ctx.Expr);
            status = status.Combine(childStatus);
        }

        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(MakeCodeResourceType(ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    template <>
    IGraphTransformer::TStatus MakeCodeWrapper<TExprNode::Lambda>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureMaxArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->ChildrenSize() == 1) {
            auto status = ConvertToLambda(input->ChildRef(0), ctx.Expr, Max<ui32>());
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        } else {
            auto ui32Type = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint32);
            auto status = TryConvertTo(input->ChildRef(0), *ui32Type, ctx.Expr);
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            status = ConvertToLambda(input->ChildRef(1), ctx.Expr, 1);
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        auto& lambda = input->ChildRef(input->ChildrenSize() - 1);
        auto resType = MakeCodeResourceType(ctx.Expr);
        auto argType = input->ChildrenSize() == 1 ? resType : ctx.Expr.MakeType<TListExprType>(resType);
        auto argTypes = std::vector<const TTypeAnnotationNode*>(lambda->Child(0)->ChildrenSize(), argType);
        if (!UpdateLambdaAllArgumentsTypes(lambda, argTypes, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureCodeResourceType(*lambda, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(resType);
        return IGraphTransformer::TStatus::Ok;
    }
} // namespace NTypeAnnImpl
} // namespace NYql
