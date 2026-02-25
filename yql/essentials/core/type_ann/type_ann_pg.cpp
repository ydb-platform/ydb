#include "type_ann_pg.h"

#include "type_ann_list.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_expr_csee.h>

#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/utils.h>

#include <util/generic/set.h>
#include <util/generic/hash.h>

namespace NYql::NTypeAnnImpl {

const NPg::TTypeDesc& GetTypeDescOfNode(const TExprNodePtr& node)
{
    const auto typeId = node->GetTypeAnn()->Cast<TPgExprType>()->GetId();

    return NPg::LookupType(typeId);
}

bool IsCastRequired(ui32 fromTypeId, ui32 toTypeId) {
    if (toTypeId == fromTypeId) {
        return false;
    }
    if (toTypeId == NPg::AnyOid || toTypeId == NPg::AnyArrayOid || toTypeId == NPg::AnyNonArrayOid) {
        return false;
    }
    return true;
}

bool AdjustPgUnknownType(TVector<const TItemExprType*>& outputItems, TExprContext& ctx) {
    bool replaced = false;
    for (auto& item : outputItems) {
        if (item->GetItemType()->GetKind() == ETypeAnnotationKind::Pg &&
            item->GetItemType()->Cast<TPgExprType>()->GetName() == "unknown") {
                replaced = true;
                item = ctx.MakeType<TItemExprType>(
                    item->GetName(),
                    ctx.MakeType<TPgExprType>(NPg::LookupType("text").TypeId));
        }
    }

    return replaced;
}

TExprNodePtr WrapWithPgCast(TExprNodePtr&& node, ui32 targetTypeId, TExprContext& ctx) {
    return ctx.Builder(node->Pos())
        .Callable("PgCast")
            .Add(0, std::move(node))
            .Callable(1, "PgType")
                .Atom(0, NPg::LookupType(targetTypeId).Name)
                .Seal()
        .Seal()
        .Build();
};

TExprNodePtr WrapWithPgCast(TExprNodePtr& node, ui32 targetTypeId, TExprContext& ctx) {
    return ctx.Builder(node->Pos())
        .Callable("PgCast")
            .Add(0, node)
            .Callable(1, "PgType")
                .Atom(0, NPg::LookupType(targetTypeId).Name)
                .Seal()
        .Seal()
        .Build();
};

TExprNodePtr FindLeftCombinatorOfNthSetItem(const TExprNode* setItems, const TExprNode* setOps, ui32 n) {
    TVector<ui32> setItemsStack(setItems->ChildrenSize());
    i32 sp = -1;
    ui32 itemIdx = 0;
    for (const auto& op : setOps->Children()) {
        if (op->Content() == "push") {
            setItemsStack[++sp] = itemIdx++;
        } else {
            Y_ENSURE(0 <= sp);
            if (setItemsStack[sp] == n) {
                return op;
            }
            --sp;
            Y_ENSURE(0 <= sp);
        }
    }
    Y_UNREACHABLE();
}

IGraphTransformer::TStatus InferPgCommonType(
    TPositionHandle pos,
    const TExprNode* setItems,
    const TExprNode* setOps,
    TColumnOrder& resultColumnOrder,
    const TStructExprType*& resultStructType,
    TExtContext& ctx,
    bool& isUniversal)
{
    isUniversal = false;
    TVector<TVector<ui32>> pgTypes;
    size_t fieldsCnt = 0;

    for (size_t i = 0; i < setItems->ChildrenSize(); ++i) {
        const auto* child = setItems->Child(i);
        if (child->GetTypeAnn() && child->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
            isUniversal = true;
            return IGraphTransformer::TStatus::Ok;
        }

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
                << "Input #" << i << " does not have ordered columns. "
                << "Consider making column order explicit by using SELECT with column names"));
            return IGraphTransformer::TStatus::Error;
        }

        if (0 == i) {
            resultColumnOrder = *childColumnOrder;
            fieldsCnt = resultColumnOrder.Size();

            pgTypes.resize(fieldsCnt);
            for (size_t j = 0; j < fieldsCnt; ++j) {
                pgTypes[j].reserve(setItems->ChildrenSize());
            }
        } else {
            if (childColumnOrder->Size() != fieldsCnt) {
                TExprNodePtr combinator = FindLeftCombinatorOfNthSetItem(setItems, setOps, i);
                Y_ENSURE(combinator);

                TString op(combinator->Content());
                if (op.EndsWith("_all")) {
                    op.erase(op.length() - 4);
                }
                op.to_upper();

                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder()
                    << "each " << op << " query must have the same number of columns"));

                return IGraphTransformer::TStatus::Error;
            }
        }

        const auto structType = itemType->Cast<TStructExprType>();
        {
            size_t j = 0;
            for (const auto& [col, gen_col] : *childColumnOrder) {
                auto itemIdx = structType->FindItemI(gen_col, nullptr);
                YQL_ENSURE(itemIdx);

                const auto* type = structType->GetItems()[*itemIdx]->GetItemType();
                ui32 pgType;
                bool convertToPg;
                if (!ExtractPgType(type, pgType, convertToPg, child->Pos(), ctx.Expr, isUniversal)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (isUniversal) {
                    return IGraphTransformer::TStatus::Ok;
                }

                pgTypes[j].push_back(pgType);

                ++j;
            }
        }
    }

    TVector<const TItemExprType*> structItems;
    for (size_t j = 0; j < fieldsCnt; ++j) {
        const NPg::TTypeDesc* commonType;
        if (const auto issue = NPg::LookupCommonType(pgTypes[j],
            [j, &setItems, &ctx](size_t i) {
                return ctx.Expr.GetPosition(setItems->Child(i)->Child(j)->Pos());
            }, commonType))
        {
            ctx.Expr.AddError(*issue);
            return IGraphTransformer::TStatus::Error;
        }
        structItems.push_back(ctx.Expr.MakeType<TItemExprType>(resultColumnOrder[j].PhysicalName,
            ctx.Expr.MakeType<TPgExprType>(commonType->TypeId)));
    }

    resultStructType = ctx.Expr.MakeType<TStructExprType>(structItems);
    if (!resultStructType->Validate(pos, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgSelfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    bool isResolved = input->Content().StartsWith("PgResolvedCall");
    if (!EnsureMinArgsCount(*input, isResolved ? 3 : 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto name = input->Head().Content();

    if (isResolved) {
        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (!EnsureTuple(*input->Child(isResolved ? 2 : 1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool rangeFunction = false;
    ui32 refinedType = 0;
    for (const auto& setting : input->Child(isResolved ? 2 : 1)->Children()) {
        if (!EnsureTupleMinSize(*setting, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(setting->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto content = setting->Head().Content();
        if (content == "range") {
            rangeFunction = true;
        } else if (content == "type") {
            if (!EnsureTupleSize(*setting, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(setting->Tail(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            refinedType = NPg::LookupType(TString(setting->Tail().Content())).TypeId;
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Unexpected setting " << content << " in function " << name));
            return IGraphTransformer::TStatus::Error;
        }
    }

    TVector<ui32> argTypes;
    bool needRetype = false;
    for (ui32 i = isResolved ? 3 : 2; i < input->ChildrenSize(); ++i) {
        auto type = input->Child(i)->GetTypeAnn();
        ui32 argType;
        bool convertToPg;
        bool isUniversal;
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr, isUniversal)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (convertToPg) {
            input->ChildRef(i) = ctx.Expr.NewCallable(input->Child(i)->Pos(), "ToPg", { input->ChildPtr(i) });
            needRetype = true;
        }

        argTypes.push_back(argType);
    }

    if (needRetype) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (isResolved) {
        auto procId = FromString<ui32>(input->Child(1)->Content());
        const auto& proc = NPg::LookupProc(procId, argTypes);
        if (!AsciiEqualsIgnoreCase(proc.Name, name)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Mismatch of resolved function name, expected: " << name << ", but got: " << proc.Name));
            return IGraphTransformer::TStatus::Error;
        }

        if (proc.Kind == NPg::EProcKind::Window) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Window function " << name << " cannot be called directly"));
            return IGraphTransformer::TStatus::Error;
        }

        if (proc.Kind == NPg::EProcKind::Aggregate) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Aggregate function " << name << " cannot be called directly"));
            return IGraphTransformer::TStatus::Error;
        }

        auto resultType = proc.ResultType;
        AdjustReturnType(resultType, proc.ArgTypes, proc.VariadicType, argTypes);
        if (resultType == NPg::AnyArrayOid && refinedType) {
            const auto& refinedDesc = NPg::LookupType(refinedType);
            YQL_ENSURE(refinedDesc.ArrayTypeId == refinedDesc.TypeId);
            resultType = refinedDesc.TypeId;
        }

        const TTypeAnnotationNode* result = ctx.Expr.MakeType<TPgExprType>(resultType);
        TMaybe<TColumnOrder> resultColumnOrder;
        if (resultType == NPg::RecordOid && rangeFunction) {
            if (proc.OutputArgNames.empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Aggregate function " << name << " cannot be used in FROM"));
                return IGraphTransformer::TStatus::Error;
            }

            resultColumnOrder.ConstructInPlace();
            TVector<const TItemExprType*> items;
            for (size_t i = 0; i < proc.OutputArgTypes.size(); ++i) {
                items.push_back(ctx.Expr.MakeType<TItemExprType>(
                    resultColumnOrder->AddColumn(proc.OutputArgNames[i]),
                    ctx.Expr.MakeType<TPgExprType>(proc.OutputArgTypes[i])));
            }

            result = ctx.Expr.MakeType<TStructExprType>(items);
        }

        if (proc.ReturnSet) {
            result = ctx.Expr.MakeType<TListExprType>(result);
        }

        input->SetTypeAnn(result);
        if (resultColumnOrder) {
            return ctx.Types.SetColumnOrder(*input, *resultColumnOrder, ctx.Expr);
        } else {
            return IGraphTransformer::TStatus::Ok;
        }
    } else {
        try {
            const auto procOrType = NPg::LookupProcWithCasts(TString(name), argTypes);
            auto children = input->ChildrenList();
            if (const auto* procPtr = std::get_if<const NPg::TProcDesc*>(&procOrType)) {
                const auto& proc = *(*procPtr);
                auto idNode = ctx.Expr.NewAtom(input->Pos(), ToString(proc.ProcId));
                children.insert(children.begin() + 1, idNode);

                const auto& fargTypes = proc.ArgTypes;
                for (size_t i = 0; i < argTypes.size(); ++i) {
                    auto targetType = (i >= fargTypes.size()) ? proc.VariadicType : fargTypes[i];
                    if (IsCastRequired(argTypes[i], targetType)) {
                        children[i+3] = WrapWithPgCast(std::move(children[i+3]), targetType, ctx.Expr);
                    }
                }

                if (argTypes.size() < fargTypes.size()) {
                    YQL_ENSURE(fargTypes.size() - argTypes.size() <= proc.DefaultArgs.size());
                    for (size_t i = argTypes.size(); i < fargTypes.size(); ++i) {
                        const auto& value = proc.DefaultArgs[i + proc.DefaultArgs.size() - fargTypes.size()];
                        TExprNode::TPtr defNode;
                        if (!value) {
                            defNode = ctx.Expr.NewCallable(input->Pos(), "Null", {});
                        } else {
                            defNode = ctx.Expr.Builder(input->Pos())
                                .Callable("PgConst")
                                    .Atom(0, *value)
                                    .Callable(1, "PgType")
                                        .Atom(0, NPg::LookupType(fargTypes[i]).Name)
                                    .Seal()
                                .Seal()
                                .Build();
                        }
                        children.insert(children.end(), defNode);
                    }
                }

                if (proc.Lang == NPg::LangSQL) {
                    if (!proc.ExprNode) {
                        throw yexception() << "Function " << proc.Name << " has no implementation";
                    }

                    // substibute by lambda
                    YQL_ENSURE(proc.ExprNode->IsLambda());
                    YQL_ENSURE(proc.ExprNode->Head().ChildrenSize() == fargTypes.size());
                    TNodeOnNodeOwnedMap deepClones;
                    YQL_ENSURE(NPg::GetSqlLanguageParser());
                    auto lambda = ctx.Expr.DeepCopy(*proc.ExprNode, NPg::GetSqlLanguageParser()->GetContext(), deepClones, true, false);
                    TNodeOnNodeOwnedMap replaces;
                    for (ui32 i = 0; i < fargTypes.size(); ++i) {
                        replaces[lambda->Head().Child(i)] = children[i + 3];
                    }

                    output = ctx.Expr.ReplaceNodes(lambda->TailPtr(), replaces);
                    return IGraphTransformer::TStatus::Repeat;
                }

                output = ctx.Expr.NewCallable(input->Pos(), "PgResolvedCall", std::move(children));
            } else if (const auto* typePtr = std::get_if<const NPg::TTypeDesc*>(&procOrType)) {
                output = WrapWithPgCast(std::move(children[2]), (*typePtr)->TypeId, ctx.Expr);
            } else {
                Y_UNREACHABLE();
            }
            return IGraphTransformer::TStatus::Repeat;
        } catch (const yexception& e) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), e.what()));
            return IGraphTransformer::TStatus::Error;
        }
    }
}

const TTypeAnnotationNode* FromPgImpl(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext& ctx, bool& isUniversal) {
    isUniversal = false;
    if (type->GetKind() == ETypeAnnotationKind::Universal) {
        isUniversal = true;
        return nullptr;
    }

    auto res = ConvertFromPgType(type->Cast<TPgExprType>()->GetId());
    if (!res) {
        ctx.AddError(TIssue(ctx.GetPosition(pos),
            TStringBuilder() << "Unsupported type: " << *type));
        return nullptr;
    }

    auto dataType = ctx.MakeType<TDataExprType>(*res);
    return ctx.MakeType<TOptionalExprType>(dataType);
}

IGraphTransformer::TStatus FromPgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureComputable(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (IsNull(input->Head())) {
        output = input->TailPtr();
        return IGraphTransformer::TStatus::Repeat;
    }

    if (input->Head().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Pg) {
        output = input->HeadPtr();
        return IGraphTransformer::TStatus::Repeat;
    }

    bool isUniversal;
    auto resultType = FromPgImpl(input->Pos(), input->Head().GetTypeAnn(), ctx.Expr, isUniversal);
    if (isUniversal) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    if (!resultType) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(resultType);
    return IGraphTransformer::TStatus::Ok;
}

const TTypeAnnotationNode* ToPgImpl(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext& ctx, bool& isUniversal) {
    isUniversal = false;
    bool isOptional;
    const TDataExprType* dataType;
    if (!EnsureDataOrOptionalOfData(pos, type, isOptional, dataType, ctx, isUniversal)) {
        return nullptr;
    }

    if (isUniversal) {
        return nullptr;
    }

    auto pgTypeId = ConvertToPgType(dataType->GetSlot());
    return ctx.MakeType<TPgExprType>(pgTypeId);
}

IGraphTransformer::TStatus ToPgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureComputable(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (IsNull(input->Head())) {
        output = input->TailPtr();
        return IGraphTransformer::TStatus::Repeat;
    }

    if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
        output = input->HeadPtr();
        return IGraphTransformer::TStatus::Repeat;
    }

    bool isUniversal;
    auto resultType = ToPgImpl(input->Pos(), input->Head().GetTypeAnn(), ctx.Expr, isUniversal);
    if (isUniversal) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    if (!resultType) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(resultType);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgCloneWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    auto status = EnsureDependsOnTailAndRewrite(input, output, ctx.Expr, ctx.Types, 1);
    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (IsNull(input->Head())) {
        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    auto type = input->Head().GetTypeAnn();
    ui32 argType;
    bool convertToPg;
    bool isUniversal;
    if (!ExtractPgType(type, argType, convertToPg, input->Head().Pos(), ctx.Expr, isUniversal)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (isUniversal) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    if (convertToPg) {
        input->ChildRef(0) = ctx.Expr.NewCallable(input->Head().Pos(), "ToPg", { input->ChildPtr(0) });
        return IGraphTransformer::TStatus::Repeat;
    }

    auto result = ctx.Expr.MakeType<TPgExprType>(argType);
    input->SetTypeAnn(result);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    bool isResolved = input->IsCallable("PgResolvedOp");
    if (!EnsureMinArgsCount(*input, isResolved ? 3 : 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMaxArgsCount(*input, isResolved ? 4 : 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto name = input->Head().Content();
    if (isResolved) {
        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    TVector<ui32> argTypes;
    bool needRetype = false;
    for (ui32 i = isResolved ? 2 : 1; i < input->ChildrenSize(); ++i) {
        auto type = input->Child(i)->GetTypeAnn();
        ui32 argType;
        bool convertToPg;
        bool isUniversal;
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr, isUniversal)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (convertToPg) {
            input->ChildRef(i) = ctx.Expr.NewCallable(input->Child(i)->Pos(), "ToPg", { input->ChildPtr(i) });
            needRetype = true;
        }

        argTypes.push_back(argType);
    }

    if (needRetype) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (isResolved) {
        auto operId = FromString<ui32>(input->Child(1)->Content());
        const auto& oper = NPg::LookupOper(operId, argTypes);
        if (oper.Name != name) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Mismatch of resolved operator name, expected: " << name << ", but got:" << oper.Name));
            return IGraphTransformer::TStatus::Error;
        }

        auto result = ctx.Expr.MakeType<TPgExprType>(oper.ResultType);
        input->SetTypeAnn(result);
        return IGraphTransformer::TStatus::Ok;
    } else {
        try {
            const auto& oper = NPg::LookupOper(TString(name), argTypes);
            auto children = input->ChildrenList();

            switch(oper.Kind) {
                case NPg::EOperKind::LeftUnary:
                    if (IsCastRequired(argTypes[0], oper.RightType)) {
                        children[1] = WrapWithPgCast(std::move(children[1]), oper.RightType, ctx.Expr);
                    }
                    break;

                case NYql::NPg::EOperKind::Binary:
                    if (IsCastRequired(argTypes[0], oper.LeftType)) {
                        children[1] = WrapWithPgCast(std::move(children[1]), oper.LeftType, ctx.Expr);
                    }
                    if (IsCastRequired(argTypes[1], oper.RightType)) {
                        children[2] = WrapWithPgCast(std::move(children[2]), oper.RightType, ctx.Expr);
                    }
                    break;

                default:
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Right unary operator " << oper.OperId << " is not supported"));
                    return IGraphTransformer::TStatus::Error;
            }
            auto idNode = ctx.Expr.NewAtom(input->Pos(), ToString(oper.OperId));
            children.insert(children.begin() + 1, idNode);
            output = ctx.Expr.NewCallable(input->Pos(), "PgResolvedOp", std::move(children));
            return IGraphTransformer::TStatus::Repeat;
        } catch (const yexception& e) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), e.what()));
            return IGraphTransformer::TStatus::Error;
        }
    }
}

IGraphTransformer::TStatus PgArrayOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    const bool isResolved = input->Content().EndsWith("ResolvedOp");
    if (!EnsureArgsCount(*input, 3 + (isResolved ? 1 : 0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (isResolved) {
        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    auto name = input->Head().Content();

    TVector<ui32> argTypes;
    bool needRetype = false;
    for (ui32 i = 1 + (isResolved ? 1 : 0); i < input->ChildrenSize(); ++i) {
        auto type = input->Child(i)->GetTypeAnn();
        ui32 argType;
        bool convertToPg;
        bool isUniversal;
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr, isUniversal)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (convertToPg) {
            input->ChildRef(i) = ctx.Expr.NewCallable(input->Child(i)->Pos(), "ToPg", { input->ChildPtr(i) });
            needRetype = true;
        }

        argTypes.push_back(argType);
    }

    if (needRetype) {
        return IGraphTransformer::TStatus::Repeat;
    }

    ui32 elemType = NPg::UnknownOid;
    if (argTypes[1] && argTypes[1] != NPg::UnknownOid) {
        const auto& typeDesc = NPg::LookupType(argTypes[1]);
        if (!typeDesc.ArrayTypeId && typeDesc.ArrayTypeId != typeDesc.TypeId) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Expected array as right argument"));
            return IGraphTransformer::TStatus::Error;
        }

        elemType = typeDesc.ElementTypeId;
    }

    argTypes[1] = elemType;
    if (isResolved) {
        auto operId = FromString<ui32>(input->Child(1)->Content());
        const auto& oper = NPg::LookupOper(operId, argTypes);
        if (oper.Name != name) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Mismatch of resolved operator name, expected: " << name << ", but got:" << oper.Name));
            return IGraphTransformer::TStatus::Error;
        }

        const auto& opResDesc = NPg::LookupType(oper.ResultType);
        if (opResDesc.Name != "bool") {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() <<
                "Expected boolean operator result, but got: " << opResDesc.Name));
            return IGraphTransformer::TStatus::Error;
        }

        auto result = ctx.Expr.MakeType<TPgExprType>(oper.ResultType);
        input->SetTypeAnn(result);
        return IGraphTransformer::TStatus::Ok;
    } else {
        try {
            const auto& oper = NPg::LookupOper(TString(name), argTypes);

            if (oper.Kind != NYql::NPg::EOperKind::Binary) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Expected binary operator"));
                return IGraphTransformer::TStatus::Error;
            }

            auto children = input->ChildrenList();
            if (IsCastRequired(argTypes[0], oper.LeftType)) {
                children[1] = WrapWithPgCast(std::move(children[1]), oper.LeftType, ctx.Expr);
            }
            if (IsCastRequired(argTypes[1], oper.RightType)) {
                auto arrayType = NPg::LookupType(oper.RightType).ArrayTypeId;
                children[2] = WrapWithPgCast(std::move(children[2]), arrayType, ctx.Expr);
            }

            auto idNode = ctx.Expr.NewAtom(input->Pos(), ToString(oper.OperId));
            children.insert(children.begin() + 1, idNode);
            output = ctx.Expr.NewCallable(input->Pos(), input->Content() == "PgAnyOp" ? "PgAnyResolvedOp" : "PgAllResolvedOp", std::move(children));
            return IGraphTransformer::TStatus::Repeat;
        } catch (const yexception& e) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), e.what()));
            return IGraphTransformer::TStatus::Error;
        }
    }
}

IGraphTransformer::TStatus PgWindowCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto name = input->Child(0)->Content();
    if (!input->Child(1)->IsAtom() && !input->Child(1)->IsCallable("PgAnonWindow")) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            "Expected either window name or reference to an inline window"));
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureTuple(*input->Child(2), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (const auto& setting : input->Child(2)->Children()) {
        if (!EnsureTupleMinSize(*setting, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(setting->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto content = setting->Head().Content();
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Unexpected setting " << content << " in window function " << name));
        return IGraphTransformer::TStatus::Error;
    }

    if (name == "lead" || name == "lag") {
        if (input->ChildrenSize() != 4) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected one argument in function" << name));
            return IGraphTransformer::TStatus::Error;
        }

        auto arg = input->Child(3)->GetTypeAnn();
        if (arg->IsOptionalOrNull()) {
            input->SetTypeAnn(arg);
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(arg));
        }
    } else if (name == "first_value" || name == "last_value") {
        if (input->ChildrenSize() != 4) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected one argument in function" << name));
            return IGraphTransformer::TStatus::Error;
        }

        auto arg = input->Child(3)->GetTypeAnn();
        if (arg->IsOptionalOrNull()) {
            input->SetTypeAnn(arg);
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(arg));
        }
    } else if (name == "nth_value") {
        if (input->ChildrenSize() != 5) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected two arguments in function" << name));
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Child(4)->GetTypeAnn() && input->Child(4)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
            auto name = input->Child(4)->GetTypeAnn()->Cast<TPgExprType>()->GetName();
            if (name != "int4") {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(4)->Pos()), TStringBuilder() <<
                    "Expected int4 type, but got: " << name));
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(4)->Pos()), TStringBuilder() <<
                "Expected pg type, but got: " << input->Child(4)->GetTypeAnn()->GetKind()));
            return IGraphTransformer::TStatus::Error;
        }

        auto arg = input->Child(3)->GetTypeAnn();
        if (arg->IsOptionalOrNull()) {
            input->SetTypeAnn(arg);
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(arg));
        }
    } else if (name == "row_number" || name == "rank" || name == "dense_rank") {
        if (input->ChildrenSize() != 3) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected no arguments in function " << name));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("int8").TypeId));
    } else if (name == "cume_dist" || name == "percent_rank") {
        if (input->ChildrenSize() != 3) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected no arguments in function " << name));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("float8").TypeId));
    } else if (name == "ntile") {
        if (input->ChildrenSize() != 4) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected exactly one argument in function " << name));
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Child(3)->GetTypeAnn() && input->Child(3)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
            auto name = input->Child(3)->GetTypeAnn()->Cast<TPgExprType>()->GetName();
            if (name != "int4") {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(3)->Pos()), TStringBuilder() <<
                    "Expected int4 type, but got: " << name));
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(3)->Pos()), TStringBuilder() <<
                "Expected pg type, but got: " << input->Child(3)->GetTypeAnn()->GetKind()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("int4").TypeId));
    } else {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Unsupported function: " << name));
        return IGraphTransformer::TStatus::Error;
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgAggWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    bool overWindow = (input->Content() == "PgAggWindowCall");
    if (!EnsureMinArgsCount(*input, overWindow ? 3 : 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto name = input->Head().Content();
    if (overWindow) {
        if (!input->Child(1)->IsAtom() && !input->Child(1)->IsCallable("PgAnonWindow")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                "Expected either window name or reference to an inline window"));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (!EnsureTuple(*input->Child(overWindow ? 2 : 1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (const auto& setting : input->Child(overWindow ? 2 : 1)->Children()) {
        if (!EnsureTupleMinSize(*setting, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(setting->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto content = setting->Head().Content();
        if (content == "distinct") {
            if (overWindow) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "distinct over window is not supported"));
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Unexpected setting " << content << " in aggregate function " << name));
            return IGraphTransformer::TStatus::Error;
        }
    }

    TVector<ui32> argTypes;
    bool needRetype = false;
    for (ui32 i = overWindow ? 3 : 2; i < input->ChildrenSize(); ++i) {
        auto type = input->Child(i)->GetTypeAnn();
        ui32 argType;
        bool convertToPg;
        bool isUniversal;
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr, isUniversal)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (convertToPg) {
            input->ChildRef(i) = ctx.Expr.NewCallable(input->Child(i)->Pos(), "ToPg", { input->ChildPtr(i) });
            needRetype = true;
        }

        argTypes.push_back(argType);
    }

    if (needRetype) {
        return IGraphTransformer::TStatus::Repeat;
    }

    const NPg::TAggregateDesc* aggDescPtr;
    try {
        aggDescPtr = &NPg::LookupAggregation(TString(name), argTypes);
        if (aggDescPtr->Kind != NPg::EAggKind::Normal) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                "Only normal aggregation supported"));
            return IGraphTransformer::TStatus::Error;
        }
    } catch (const yexception& e) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), e.what()));
        return IGraphTransformer::TStatus::Error;
    }
    const NPg::TAggregateDesc& aggDesc = *aggDescPtr;

    ui32 argIdx = overWindow ? 3 : 2;
    for (ui32 i = 0; i < argTypes.size(); ++i, ++argIdx) {
        if (IsCastRequired(argTypes[i], aggDesc.ArgTypes[i])) {
            auto& argNode = input->ChildRef(argIdx);
            argNode = WrapWithPgCast(std::move(argNode), aggDesc.ArgTypes[i], ctx.Expr);
            needRetype = true;
        }
    }

    if (needRetype) {
        return IGraphTransformer::TStatus::Repeat;
    }

    ui32 resultType;
    if (!aggDesc.FinalFuncId) {
        resultType = aggDesc.TransTypeId;
    } else {
        resultType = NPg::LookupProc(aggDesc.FinalFuncId).ResultType;
    }

    AdjustReturnType(resultType, aggDesc.ArgTypes, 0, argTypes);
    auto result = ctx.Expr.MakeType<TPgExprType>(resultType);
    input->SetTypeAnn(result);

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgNullIfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TVector<ui32> types(2);
    bool replaced = false;

    for (ui32 i = 0; i < 2; ++i) {
        auto* item = input->Child(i);
        auto type = item->GetTypeAnn();
        ui32 argType;
        bool convertToPg;
        bool isUniversal;
        const auto pos = item->Pos();
        if (!ExtractPgType(type, argType, convertToPg, pos, ctx.Expr, isUniversal)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (convertToPg) {
            replaced = true;
            input->ChildRef(i) = ctx.Expr.NewCallable(input->Child(2)->Pos(), "ToPg", { input->ChildPtr(i) });
        }

        types[i] = argType;
    }

    if (replaced) {
        return IGraphTransformer::TStatus::Repeat;
    }

    const NPg::TTypeDesc* commonType;
    if (const auto issue = NPg::LookupCommonType(types,
        [&input, &ctx](size_t i) {
            return ctx.Expr.GetPosition(input->Child(i)->Pos());
        }, commonType))
    {
        ctx.Expr.AddError(*issue);
        return IGraphTransformer::TStatus::Error;
    }
    if (IsCastRequired(commonType->TypeId, types[0])) {
        input->ChildRef(0) = WrapWithPgCast(std::move(input->ChildRef(0)), commonType->TypeId, ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TPgExprType>(commonType->TypeId));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgQualifiedStarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgWindowWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (input->Child(1)->Content()) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), "Window reference is not supported"));
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureTuple(*input->Child(2), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (const auto& x : input->Child(2)->Children()) {
        if (!x->IsCallable({"YqlGroup", "PgGroup"})) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected YqlGroup or PgGroup"));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (!EnsureTuple(*input->Child(3), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (const auto& x : input->Child(3)->Children()) {
        if (!x->IsCallable({"YqlSort", "PgSort"})) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected YqlSort or PgSort"));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (!EnsureTuple(*input->Child(4), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool hasType = false;
    bool hasFrom = false;
    bool hasTo = false;
    bool hasFromValue = false;
    bool hasToValue = false;
    bool needFromValue = false;
    bool needToValue = false;

    for (const auto& x : input->Child(4)->Children()) {
        if (!EnsureTupleMinSize(*x, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(x->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto optionName = x->Head().Content();
        if (optionName == "exclude") {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Excludes are not supported"));
            return IGraphTransformer::TStatus::Error;
        } else if (optionName == "from_value" || optionName == "to_value") {
            hasFromValue = hasFromValue || (optionName == "from_value");
            hasToValue = hasToValue || (optionName == "to_value");
            if (!EnsureTupleSize(*x, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!x->Tail().IsCallable("Int32")) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected Int32 as frame offset"));
                return IGraphTransformer::TStatus::Error;
            }

            auto val = FromString<i32>(x->Tail().Head().Content());
            if (val < 0) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected non-negative value as frame offset"));
                return IGraphTransformer::TStatus::Error;
            }
        } else if (optionName == "type") {
            hasType = true;
            if (!EnsureTupleSize(*x, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(x->Tail(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto type = x->Tail().Content();
            if (type != "rows") {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), TStringBuilder() << "Unsupported frame type: " << type));
                return IGraphTransformer::TStatus::Error;
            }
        } else if (optionName == "from" || optionName == "to") {
            hasFrom = hasFrom || (optionName == "from");
            hasTo = hasTo || (optionName == "to");
            if (!EnsureTupleSize(*x, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(x->Tail(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto bound = x->Tail().Content();
            if (!(bound == "up" || bound == "p" || bound == "c" || bound == "f" || bound == "uf")) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), TStringBuilder() << "Unsupported frame bound: " << bound));
                return IGraphTransformer::TStatus::Error;
            }

            if (bound == "p" || bound == "f") {
                needFromValue = needFromValue || (optionName == "from");
                needToValue = needToValue || (optionName == "to");
            }

            if (optionName == "from" && bound == "uf") {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Unbounded following is unsupported as start offset"));
                return IGraphTransformer::TStatus::Error;
            }

            if (optionName == "to" && bound == "up") {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Unbounded preceding is unsupported as end offset"));
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), TStringBuilder() << "Unknown option: " << optionName));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (hasType) {
        if (!hasFrom || !hasTo) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing offset specification in the frame"));
            return IGraphTransformer::TStatus::Error;
        }
    } else {
        if (hasFrom || hasTo) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Unexpected offset specification in the frame"));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (needFromValue != hasFromValue || needToValue != hasToValue) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Wrong offset value in the frame"));
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgAnonWindowWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    ui32 n;
    if (!TryFromString(input->Child(0)->Content(), n)) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Expected number"));
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgConstWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMaxArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureTypePg(*input->Child(1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    // TODO: validate value
    bool isUniversal;
    if (!EnsureAtomOrUniversal(input->Head(), ctx.Expr, isUniversal)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (isUniversal) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    if (input->ChildrenSize() >= 3) {
        auto type = input->Child(2)->GetTypeAnn();
        ui32 typeModType;
        bool convertToPg;
        if (!ExtractPgType(type, typeModType, convertToPg, input->Child(2)->Pos(), ctx.Expr, isUniversal)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (convertToPg) {
            input->ChildRef(2) = ctx.Expr.NewCallable(input->Child(2)->Pos(), "ToPg", { input->ChildPtr(2) });
            return IGraphTransformer::TStatus::Repeat;
        }

        if (typeModType != NPg::LookupType("int4").TypeId) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected pg int4 as typemod, but got " << NPg::LookupType(typeModType).Name));
            return IGraphTransformer::TStatus::Error;
        }
    }

    auto retType = input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
    input->SetTypeAnn(retType);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgInternal0Wrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto internalId = NPg::LookupType("internal").TypeId;
    input->SetTypeAnn(ctx.Expr.MakeType<TPgExprType>(internalId));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgCastWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMaxArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto type = input->Head().GetTypeAnn();
    ui32 inputTypeId = 0;
    bool convertToPg;
    bool isUniversal;
    if (!ExtractPgType(type, inputTypeId, convertToPg, input->Pos(), ctx.Expr, isUniversal)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (isUniversal) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    if (convertToPg) {
        input->ChildRef(0) = ctx.Expr.NewCallable(input->Child(0)->Pos(), "ToPg", { input->ChildPtr(0) });
        return IGraphTransformer::TStatus::Repeat;
    }

    if (!EnsureTypePg(*input->Child(1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    auto targetTypeId = input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TPgExprType>()->GetId();

    if (inputTypeId != 0 && inputTypeId != targetTypeId) {
        bool fail = false;
        const auto& rawInputDesc = NPg::LookupType(inputTypeId);
        const NPg::TTypeDesc* inputDescPtr = &rawInputDesc;
        if (rawInputDesc.TypeId == NPg::UnknownOid) {
            inputDescPtr = &NPg::LookupType("text");
        }

        const NPg::TTypeDesc& inputDesc = *inputDescPtr;
        const auto& targetDesc = NPg::LookupType(targetTypeId);
        const bool isInputArray = (inputDesc.TypeId == inputDesc.ArrayTypeId);
        const bool isTargetArray = (targetDesc.TypeId == targetDesc.ArrayTypeId);
        if ((isInputArray && !isTargetArray && targetDesc.Category != 'S')
            || (!isInputArray && isTargetArray && inputDesc.Category != 'S')) {
            fail = true;
        } else if (inputDesc.Category != 'S' && targetDesc.Category != 'S') {
            auto elemInput = inputTypeId;
            if (isInputArray) {
                elemInput = inputDesc.ElementTypeId;
            }

            auto elemTarget = targetTypeId;
            if (isTargetArray) {
                elemTarget = targetDesc.ElementTypeId;
            }

            if (!NPg::HasCast(elemInput, elemTarget)) {
                fail = true;
            }
        }

        if (fail) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Cannot cast type " << inputDesc.Name << " into type " << targetDesc.Name));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (input->ChildrenSize() >= 3) {
        auto type = input->Child(2)->GetTypeAnn();
        ui32 typeModType;
        bool convertToPg;
        bool isUniversal;
        if (!ExtractPgType(type, typeModType, convertToPg, input->Child(2)->Pos(), ctx.Expr, isUniversal)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (convertToPg) {
            input->ChildRef(2) = ctx.Expr.NewCallable(input->Child(2)->Pos(), "ToPg", { input->ChildPtr(2) });
            return IGraphTransformer::TStatus::Repeat;
        }

        if (typeModType != NPg::LookupType("int4").TypeId) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected pg int4 as typemod, but got " << NPg::LookupType(typeModType).Name));
            return IGraphTransformer::TStatus::Error;
        }
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TPgExprType>(targetTypeId));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgAggregationTraitsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    const bool onWindow = input->Content().StartsWith("PgWindowTraits");
    const bool tupleExtractor = input->Content().EndsWith("Tuple");
    if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto func = input->Child(0)->Content();
    if (input->Child(1)->GetTypeAnn() && input->Child(1)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        input->SetTypeAnn(input->Child(1)->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    if (!EnsureType(*input->Child(1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto itemType = input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
    auto& lambda = input->ChildRef(2);
    auto convertStatus = ConvertToLambda(lambda, ctx.Expr, 1);
    if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
        return convertStatus;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, { itemType }, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto lambdaResult = lambda->GetTypeAnn();
    if (!lambdaResult) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (tupleExtractor) {
        // convert lambda with tuple type into multi lambda
        auto args = input->ChildrenList();
        if (lambda->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
            args[2] = ConvertToMultiLambda(lambda, ctx.Expr);
        }

        output = ctx.Expr.NewCallable(input->Pos(), input->Content().substr(0, input->Content().size() - 5), std::move(args));
        //ctx.Expr.Step.Repeat(TExprStep::ExpandApplyForLambdas);
        //return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
        return IGraphTransformer::TStatus::Repeat;
    }

    TVector<ui32> argTypes;
    bool needRetype = false;
    bool isUniversal;
    if (auto status = ExtractPgTypesFromMultiLambda(lambda, argTypes, needRetype, ctx.Expr, isUniversal);
        status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (isUniversal) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    const NPg::TAggregateDesc* aggDescPtr;
    try {
        aggDescPtr = &NPg::LookupAggregation(TString(func), argTypes);
        if (aggDescPtr->Kind != NPg::EAggKind::Normal) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                "Only normal aggregation supported"));
            return IGraphTransformer::TStatus::Error;
        }
    } catch (const yexception& e) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), e.what()));
        return IGraphTransformer::TStatus::Error;
    }

    const NPg::TAggregateDesc& aggDesc = *aggDescPtr;
    output = ExpandPgAggregationTraits(input->Pos(), aggDesc, onWindow, lambda, argTypes, itemType, ctx.Expr);
    return IGraphTransformer::TStatus::Repeat;
}

IGraphTransformer::TStatus PgTypeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto type = TString(input->Child(0)->Content());
    if (!NPg::HasType(type)) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(0)->Pos()),
            TStringBuilder() << "Unknown type: '" << type << "'"));
        return IGraphTransformer::TStatus::Error;
    }

    auto typeId = NPg::LookupType(type).TypeId;
    input->SetTypeAnn(ctx.Expr.MakeType<TTypeExprType>(ctx.Expr.MakeType<TPgExprType>(typeId)));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgBoolOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);

    const TStringBuf& c = input->Content();
    const bool isSingleArg = (c == "PgNot") || (c == "PgIsTrue") || (c == "PgIsFalse") || (c == "PgIsUnknown");
    if (!EnsureArgsCount(*input, isSingleArg ? 1 : 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TVector<ui32> argTypes;
    bool needRetype = false;
    for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
        auto type = input->Child(i)->GetTypeAnn();
        ui32 argType;
        bool convertToPg;
        bool isUniversal;
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr, isUniversal)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (convertToPg) {
            input->ChildRef(i) = ctx.Expr.NewCallable(input->Child(i)->Pos(), "ToPg", { input->ChildPtr(i) });
            needRetype = true;
        }

        argTypes.push_back(argType);
    }

    if (needRetype) {
        return IGraphTransformer::TStatus::Repeat;
    }

    auto boolId = NPg::LookupType("bool").TypeId;
    for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
        if (!NPg::IsCompatibleTo(argTypes[i], boolId)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i)->Pos()),
                TStringBuilder() << "Expected pg bool type, but got: " << NPg::LookupType(argTypes[i]).Name));
            return IGraphTransformer::TStatus::Error;
        }
    }

    auto result = ctx.Expr.MakeType<TPgExprType>(boolId);
    input->SetTypeAnn(result);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgArrayWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TVector<ui32> argTypes;
    bool needRetype = false;
    for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
        auto type = input->Child(i)->GetTypeAnn();
        ui32 argType;
        bool convertToPg;
        bool isUniversal;
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr, isUniversal)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (convertToPg) {
            input->ChildRef(i) = ctx.Expr.NewCallable(input->Child(i)->Pos(), "ToPg", { input->ChildPtr(i) });
            needRetype = true;
        }

        argTypes.push_back(argType);
    }

    if (needRetype) {
        return IGraphTransformer::TStatus::Repeat;
    }

    bool castsNeeded = false;
    const NPg::TTypeDesc* elemTypeDesc;
    if (const auto issue = NPg::LookupCommonType(argTypes,
        [&input, &ctx](size_t i) {
            return ctx.Expr.GetPosition(input->Child(i)->Pos());
        }, elemTypeDesc, castsNeeded))
    {
        ctx.Expr.AddError(*issue);
        return IGraphTransformer::TStatus::Error;
    }
    auto elemType = elemTypeDesc->TypeId;

    const auto& typeInfo = NPg::LookupType(elemType);
    auto result = ctx.Expr.MakeType<TPgExprType>(typeInfo.ArrayTypeId);

    input->SetTypeAnn(result);
    if (!castsNeeded) {
        return IGraphTransformer::TStatus::Ok;
    }

    TExprNode::TListType castArrayElems;
    for (ui32 i = 0; i < argTypes.size(); ++i) {
        auto child = input->ChildPtr(i);
        if (argTypes[i] == elemType) {
            castArrayElems.push_back(child);
        } else {
            castArrayElems.push_back(WrapWithPgCast(std::move(child), elemType, ctx.Expr));
        }
    }
    output = ctx.Expr.NewCallable(input->Pos(), "PgArray", std::move(castArrayElems));
    return IGraphTransformer::TStatus::Repeat;
}

IGraphTransformer::TStatus PgTypeModWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureTypePg(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto pgType = input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TPgExprType>();
    const auto& typeDesc = NPg::LookupType(pgType->GetId());
    auto funcId = typeDesc.TypeModInFuncId;
    if (typeDesc.TypeId == typeDesc.ArrayTypeId) {
        funcId = NPg::LookupType(typeDesc.ElementTypeId).TypeModInFuncId;
    }

    if (!funcId) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "No modifiers for type: " << pgType->GetName()));
        return IGraphTransformer::TStatus::Error;
    }

    TVector<TString> mods;
    for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
        if (!EnsureAtom(*input->Child(i), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        mods.push_back(TString(input->Child(i)->Content()));
    }

    if (pgType->GetName() == "interval" || pgType->GetName() == "_interval") {
        if (mods.size() < 1) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "At least one modifier is expected for pginterval"));
            return IGraphTransformer::TStatus::Error;
        }

        i32 value;
        if (!ParsePgIntervalModifier(mods[0], value)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Unsupported modifier for pginterval: " << mods[0]));
            return IGraphTransformer::TStatus::Error;
        }

        mods[0] = ToString(value);
    }

    TExprNode::TListType args;
    for (const auto& mod : mods) {
        args.push_back(ctx.Expr.Builder(input->Pos())
            .Callable("PgConst")
                .Atom(0, mod)
                .Callable(1, "PgType")
                    .Atom(0, "cstring")
                .Seal()
            .Seal()
            .Build());
    }

    auto arr = ctx.Expr.NewCallable(input->Pos(), "PgArray", std::move(args));
    output = ctx.Expr.Builder(input->Pos())
        .Callable("PgCall")
            .Atom(0, NPg::LookupProc(funcId, { 0 }).Name)
            .List(1)
            .Seal()
            .Add(2, arr)
        .Seal()
        .Build();

    return IGraphTransformer::TStatus::Repeat;
}

IGraphTransformer::TStatus PgLikeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TVector<ui32> argTypes;
    bool needRetype = false;
    for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
        auto type = input->Child(i)->GetTypeAnn();
        ui32 argType;
        bool convertToPg;
        bool isUniversal;
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr, isUniversal)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (convertToPg) {
            input->ChildRef(i) = ctx.Expr.NewCallable(input->Child(i)->Pos(), "ToPg", { input->ChildPtr(i) });
            needRetype = true;
        }

        argTypes.push_back(argType);
    }

    if (needRetype) {
        return IGraphTransformer::TStatus::Repeat;
    }

    const auto textTypeId = NPg::LookupType("text").TypeId;
    for (ui32 i = 0; i < argTypes.size(); ++i) {
        if (!argTypes[i]) {
            continue;
        }

        if (argTypes[i] != textTypeId) {
            if (NPg::IsCoercible(argTypes[i], textTypeId, NPg::ECoercionCode::Implicit)) {
                auto& argNode = input->ChildRef(i);
                argNode = WrapWithPgCast(std::move(argNode), textTypeId, ctx.Expr);
                return IGraphTransformer::TStatus::Repeat;
            }
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected pg text, but got " << NPg::LookupType(argTypes[i]).Name));
            return IGraphTransformer::TStatus::Error;
        }
    }

    auto result = ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("bool").TypeId);
    input->SetTypeAnn(result);
    return IGraphTransformer::TStatus::Ok;
}

TExprNodePtr BuildUniTypePgIn(TExprNodeList&& args, TContext& ctx) {
    auto lhs = args[0];
    std::swap(args[0], args.back());
    args.pop_back();

    return ctx.Expr.Builder(lhs->Pos())
        .Callable("SqlIn")
            .List(0)
                .Add(std::move(args))
            .Seal()
            .Add(1, lhs)
            .List(2)
                .List(0)
                    .Atom(0, "ansi")
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

IGraphTransformer::TStatus PgInWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "IN expects at least one element"));
        return IGraphTransformer::TStatus::Error;
    }

    TVector<ui32> pgTypes(input->ChildrenSize());
    {
        TExprNodeList convertedChildren;
        convertedChildren.reserve(input->ChildrenSize());
        bool convertionRequired = false;
        bool hasConvertions = false;
        for (size_t i = 0; i < input->ChildrenSize(); ++i) {
            const auto child = input->Child(i);
            bool isUniversal;
            if (!ExtractPgType(child->GetTypeAnn(), pgTypes[i], convertionRequired, child->Pos(), ctx.Expr, isUniversal)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (isUniversal) {
                input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
                return IGraphTransformer::TStatus::Ok;
            }

            if (convertionRequired) {
                hasConvertions = true;

                auto convertedChild = ctx.Expr.Builder(child->Pos())
                    .Callable("ToPg")
                        .Add(0, child)
                    .Seal()
                    .Build();
                convertedChildren.push_back(std::move(convertedChild));
            } else {
                convertedChildren.push_back(std::move(child));
            }
        }
        if (hasConvertions) {
            output = ctx.Expr.Builder(input->Pos())
                .Callable("PgIn")
                    .Add(std::move(convertedChildren))
                .Seal()
                .Build();

            return IGraphTransformer::TStatus::Repeat;
        }
    }

    auto posGetter = [&input, &ctx](size_t i) {
        return ctx.Expr.GetPosition(input->Child(i)->Pos());
    };

    struct TPgListCommonTypeConversion {
        ui32 TargetType = 0;
        TExprNodeList Items;
    };

    bool castRequired = false;
    const NPg::TTypeDesc* commonType;
    if (NPg::LookupCommonType(pgTypes, posGetter, commonType, castRequired))
    {
        const auto& lhsTypeId = input->Head().GetTypeAnn()->Cast<TPgExprType>()->GetId();

        THashMap<ui32, TPgListCommonTypeConversion> elemsByType;
        for (size_t i = 1; i < input->ChildrenSize(); ++i) {
            auto& elemsOfType = elemsByType[pgTypes[i]];
            if (elemsOfType.Items.empty()) {
                const NPg::TTypeDesc* elemCommonType;
                if (const auto issue = NPg::LookupCommonType({pgTypes[0], pgTypes[i]},
                    posGetter, elemCommonType))
                {
                    ctx.Expr.AddError(*issue);
                    return IGraphTransformer::TStatus::Error;
                }
                elemsOfType.TargetType = elemCommonType->TypeId;

                elemsOfType.Items.push_back((lhsTypeId == elemsOfType.TargetType)
                    ? input->HeadPtr()
                    : WrapWithPgCast(input->HeadPtr(), elemsOfType.TargetType, ctx.Expr));
            }
            const auto rhsItemTypeId = input->Child(i)->GetTypeAnn()->Cast<TPgExprType>()->GetId();
            elemsOfType.Items.push_back((rhsItemTypeId == elemsOfType.TargetType)
                ? input->Child(i)
                : WrapWithPgCast(input->Child(i), elemsOfType.TargetType, ctx.Expr));
        }
        TExprNodeList orClausesOfIn;
        orClausesOfIn.reserve(elemsByType.size());

        for (auto& elemsOfType: elemsByType) {
            auto& conversion = elemsOfType.second;
            orClausesOfIn.push_back(BuildUniTypePgIn(std::move(conversion.Items), ctx));
        }
        output = ctx.Expr.Builder(input->Pos())
            .Callable("Or")
                .Add(std::move(orClausesOfIn))
            .Seal()
            .Build();
    } else {
        TExprNodeList items;

        if (castRequired) {
            for (size_t i = 0; i < input->ChildrenSize(); ++i) {
                const auto itemTypeId = input->Child(i)->GetTypeAnn()->Cast<TPgExprType>()->GetId();
                items.push_back((itemTypeId == commonType->TypeId)
                    ? input->Child(i)
                    : WrapWithPgCast(input->Child(i), commonType->TypeId, ctx.Expr));
            }
        }
        output = BuildUniTypePgIn(std::move((castRequired) ? items : input->ChildrenList()), ctx);
    }
    output = ctx.Expr.Builder(input->Pos())
        .Callable("ToPg")
            .Add(0, output)
        .Seal()
        .Build();
    return IGraphTransformer::TStatus::Repeat;
}

IGraphTransformer::TStatus PgBetweenWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TVector<ui32> argTypes;
    bool needRetype = false;
    for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
        auto type = input->Child(i)->GetTypeAnn();
        ui32 argType;
        bool convertToPg;
        bool isUniversal;
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr, isUniversal)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (convertToPg) {
            input->ChildRef(i) = ctx.Expr.NewCallable(input->Child(i)->Pos(), "ToPg", { input->ChildPtr(i) });
            needRetype = true;
        }

        argTypes.push_back(argType);
    }

    if (needRetype) {
        return IGraphTransformer::TStatus::Repeat;
    }

    auto elemType = argTypes.front();
    auto elemTypeAnn = input->Child(0)->GetTypeAnn();
    for (ui32 i = 1; i < argTypes.size(); ++i) {
        if (elemType == 0) {
            elemType = argTypes[i];
            elemTypeAnn = input->Child(i)->GetTypeAnn();
        } else if (argTypes[i] != 0 && argTypes[i] != NPg::UnknownOid && argTypes[i] != elemType) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Mismatch of type of between elements: " <<
                NPg::LookupType(elemType).Name << " and " << NPg::LookupType(argTypes[i]).Name));
            return IGraphTransformer::TStatus::Error;
        }
    }

    switch (elemType) {
        case 0:
        case NPg::UnknownOid:
            output = ctx.Expr.Builder(input->Pos())
                .Callable("Nothing")
                    .Callable(0, "PgType")
                        .Atom(0, "bool")
                    .Seal()
                .Seal()
                .Build();
            break;
        default:
            if (!elemTypeAnn->IsComparable()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                      TStringBuilder() << "Cannot compare items of type: " << NPg::LookupType(elemType).Name));
                return IGraphTransformer::TStatus::Error;
            }
    }

    auto result = ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("bool").TypeId);
    input->SetTypeAnn(result);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgToRecordWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureStructType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto structType = input->Head().GetTypeAnn()->Cast<TStructExprType>();
    bool hasConversions = false;
    for (ui32 pass = 0; pass < 2; ++pass) {
        TExprNode::TListType convItems;
        for (ui32 i = 0; i < structType->GetSize(); ++i) {
            ui32 pgType;
            bool convertToPg;
            bool isUniversal;
            if (!ExtractPgType(structType->GetItems()[i]->GetItemType(), pgType, convertToPg, input->Head().Pos(), ctx.Expr, isUniversal)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (isUniversal) {
                input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
                return IGraphTransformer::TStatus::Ok;
            }

            hasConversions = hasConversions || convertToPg;
            if (pass == 1) {
                auto name = ctx.Expr.NewAtom(input->Head().Pos(), structType->GetItems()[i]->GetName());
                auto member = ctx.Expr.NewCallable(input->Head().Pos(), "Member", { input->HeadPtr(), name} );
                if (convertToPg) {
                    member = ctx.Expr.NewCallable(input->Head().Pos(), "ToPg", { member });
                }

                convItems.push_back(ctx.Expr.NewList(input->Head().Pos(), { name, member }));
            }
        }

        if (!hasConversions) {
            break;
        }

        if (pass == 1) {
            output = ctx.Expr.ChangeChild(*input, 0, ctx.Expr.NewCallable(input->Head().Pos(), "AsStruct", std::move(convItems)));
            return IGraphTransformer::TStatus::Repeat;
        }
    }

    if (!EnsureTuple(input->Tail(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (const auto& child : input->Tail().Children()) {
        if (!EnsureTupleSize(*child, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(child->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(child->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto pos = structType->FindItem(child->Tail().Content());
        if (!pos) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()),
                TStringBuilder() << "Missing member: " << child->Tail().Content()));
            return IGraphTransformer::TStatus::Error;
        }
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("record").TypeId));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgIterateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureListType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto& lambda = input->ChildRef(1);
    const auto status = ConvertToLambda(lambda, ctx.Expr, 1);
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, { input->Head().GetTypeAnn() }, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (!IsSameAnnotation(*lambda->GetTypeAnn(), *input->Head().GetTypeAnn())) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder() <<
            "Mismatch of transform lambda return type and input type: " <<
            *lambda->GetTypeAnn() << " != " << *input->Head().GetTypeAnn()));
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(input->Head().GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

} // namespace NYql::NTypeAnnImpl
