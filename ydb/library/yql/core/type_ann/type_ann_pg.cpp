#include "type_ann_pg.h"
#include "type_ann_list.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_csee.h>

#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/utils.h>

#include <util/generic/set.h>
#include <util/generic/hash.h>

namespace NYql {

namespace NTypeAnnImpl {

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

bool AdjustUnknownType(TVector<const TItemExprType*>& outputItems, TExprContext& ctx) {
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

bool ValidateInputTypes(TExprNode& node, TExprContext& ctx) {
    if (!EnsureTuple(node, ctx)) {
        return false;
    }

    for (auto& x : node.Children()) {
        if (!EnsureTupleSize(*x, 2, ctx)) {
            return false;
        }

        if (!EnsureAtom(*x->Child(0), ctx)) {
            return false;
        }

        if (!EnsureType(*x->Child(1), ctx)) {
            return false;
        }
    }

    return true;
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
            if (setItemsStack[sp] == n) {
                return op;
            }
            --sp;
            Y_ENSURE(0 <= sp);
        }
    }
    Y_UNREACHABLE();
}

IGraphTransformer::TStatus InferPgCommonType(TPositionHandle pos, const TExprNode* setItems, const TExprNode* setOps,
    TColumnOrder& resultColumnOrder, const TStructExprType*& resultStructType, TExtContext& ctx)
{
    TVector<TVector<ui32>> pgTypes;
    size_t fieldsCnt = 0;

    for (size_t i = 0; i < setItems->ChildrenSize(); ++i) {
        const auto* child = setItems->Child(i);

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
                if (!ExtractPgType(type, pgType, convertToPg, child->Pos(), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
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

IGraphTransformer::TStatus PgStarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
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
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
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
                auto idNode = ctx.Expr.NewAtom(input->Pos(), ToString((*procPtr)->ProcId));
                children.insert(children.begin() + 1, idNode);

                const auto& fargTypes = (*procPtr)->ArgTypes;
                for (size_t i = 0; i < argTypes.size(); ++i) {
                    auto targetType = (i >= fargTypes.size()) ? (*procPtr)->VariadicType : fargTypes[i];
                    if (IsCastRequired(argTypes[i], targetType)) {
                        children[i+3] = WrapWithPgCast(std::move(children[i+3]), targetType, ctx.Expr);
                    }
                }
                for (size_t i = argTypes.size(); i < fargTypes.size(); ++i) {
                    const auto& value = (*procPtr)->DefaultArgs[i + (*procPtr)->DefaultArgs.size() - fargTypes.size()];
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

const TTypeAnnotationNode* FromPgImpl(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext& ctx) {
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

    auto resultType = FromPgImpl(input->Pos(), input->Head().GetTypeAnn(), ctx.Expr);
    if (!resultType) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(resultType);
    return IGraphTransformer::TStatus::Ok;
}

const TTypeAnnotationNode* ToPgImpl(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext& ctx) {
    bool isOptional;
    const TDataExprType* dataType;
    if (!EnsureDataOrOptionalOfData(pos, type, isOptional, dataType, ctx)) {
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

    auto resultType = ToPgImpl(input->Pos(), input->Head().GetTypeAnn(), ctx.Expr);
    if (!resultType) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(resultType);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgCloneWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureDependsOnTail(*input, ctx.Expr, 1)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (IsNull(input->Head())) {
        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    auto type = input->Head().GetTypeAnn();
    ui32 argType;
    bool convertToPg;
    if (!ExtractPgType(type, argType, convertToPg, input->Head().Pos(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
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
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
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
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
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
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
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
        const auto pos = item->Pos();
        if (!ExtractPgType(type, argType, convertToPg, pos, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
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

IGraphTransformer::TStatus PgColumnRefWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMaxArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (const auto& child : input->Children()) {
        if (!EnsureAtom(*child, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgResultItemWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);

    if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (input->Head().IsList()) {
        for (const auto& x : input->Head().Children()) {
            if (!x->IsList() && !x->IsAtom()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), TStringBuilder() << "Expected atom or list, but got: " << x->Type()));
                return IGraphTransformer::TStatus::Error;
            }
        }
    } else {
        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    bool hasType = false;
    if (!input->Child(1)->IsCallable("Void")) {
        hasType = true;
        if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }
    }

    auto& lambda = input->ChildRef(2);
    const auto status = ConvertToLambda(lambda, ctx.Expr, hasType ? 1 : 0);
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!hasType) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, { input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType() }, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    input->SetTypeAnn(lambda->GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgReplaceUnknownWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto* typeAnn = input->Head().GetTypeAnn();

    if (typeAnn->GetKind() == ETypeAnnotationKind::Pg) {
        if (typeAnn->Cast<TPgExprType>()->GetId() == NPg::UnknownOid) {
            const auto* newType = ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("text").TypeId);
            output = ctx.Expr.Builder(input->Pos())
                .Callable("PgCast")
                    .Add(0, input->HeadPtr())
                    .Add(1, ExpandType(input->Pos(), *newType, ctx.Expr))
                .Seal()
                .Build();
        } else {
            output = input->HeadPtr();
        }
        return IGraphTransformer::TStatus::Repeat;
    }

    if (!EnsureListType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto listType = typeAnn->Cast<TListExprType>();
    if (!EnsureStructType(input->Head().Pos(), *listType->GetItemType(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto structType = listType->GetItemType()->Cast<TStructExprType>();
    auto structItemTypes = structType->GetItems();
    const bool noUnknowns = std::none_of(structItemTypes.cbegin(), structItemTypes.cend(), 
        [] (const TItemExprType* item) {
                const auto* itemType = item->GetItemType();
                return itemType->GetKind() == ETypeAnnotationKind::Pg && itemType->Cast<TPgExprType>()->GetId() == NPg::UnknownOid;
        });
    if (noUnknowns)
    {
        output = input->HeadPtr();
        return IGraphTransformer::TStatus::Repeat;
    }

    output = ctx.Expr.Builder(input->Pos())
        .Callable("OrderedMap")
            .Add(0, input->HeadPtr())
            .Lambda(1)
                .Param("row")
                .Callable("StaticMap")
                    .Arg(0, "row")
                    .Lambda(1)
                        .Param("cell")
                        .Callable("PgReplaceUnknown")
                            .Arg(0, "cell")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return IGraphTransformer::TStatus::Repeat;
}

IGraphTransformer::TStatus PgWhereWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool hasType = false;
    if (!input->Child(0)->IsCallable("Void")) {
        hasType = true;
        if (auto status = EnsureTypeRewrite(input->ChildRef(0), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }
    }

    auto& lambda = input->ChildRef(1);
    const auto status = ConvertToLambda(lambda, ctx.Expr, hasType ? 1 : 0);
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!hasType) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, { input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType() }, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    input->SetTypeAnn(lambda->GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgSortWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(2), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (input->Child(2)->Content() != "asc" && input->Child(2)->Content() != "desc") {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()),
            TStringBuilder() << "Unsupported sort direction: " << input->Child(2)->Content()));
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(3), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (input->Child(3)->Content() != "first" && input->Child(3)->Content() != "last") {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(3)->Pos()),
            TStringBuilder() << "Unsupported nulls ordering: " << input->Child(3)->Content()));
        return IGraphTransformer::TStatus::Error;
    }

    bool hasType = false;
    if (!input->Child(0)->IsCallable("Void")) {
        hasType = true;
        if (auto status = EnsureTypeRewrite(input->ChildRef(0), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }
    }

    auto& lambda = input->ChildRef(1);
    const auto status = ConvertToLambda(lambda, ctx.Expr, hasType ? 1 : 0);
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!hasType) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, { input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType() }, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    input->SetTypeAnn(lambda->GetTypeAnn());
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
        if (!x->IsCallable("PgGroup")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected PgGroup"));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (!EnsureTuple(*input->Child(3), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (const auto& x : input->Child(3)->Children()) {
        if (!x->IsCallable("PgSort")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected PgSort"));
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
    if (!EnsureAtom(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (input->ChildrenSize() >= 3) {
        auto type = input->Child(2)->GetTypeAnn();
        ui32 typeModType;
        bool convertToPg;
        if (!ExtractPgType(type, typeModType, convertToPg, input->Child(2)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
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
    if (!ExtractPgType(type, inputTypeId, convertToPg, input->Pos(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
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
        if (!ExtractPgType(type, typeModType, convertToPg, input->Child(2)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
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

        output = ctx.Expr.NewCallable(input->Pos(), input->Content().substr(0, input->Content().Size() - 5), std::move(args));
        //ctx.Expr.Step.Repeat(TExprStep::ExpandApplyForLambdas);
        //return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
        return IGraphTransformer::TStatus::Repeat;
    }

    TVector<ui32> argTypes;
    bool needRetype = false;
    if (auto status = ExtractPgTypesFromMultiLambda(lambda, argTypes, needRetype, ctx.Expr);
        status != IGraphTransformer::TStatus::Ok) {
        return status;
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

struct TInput {
    enum EInputPriority {
        External,
        Current,
        Projection
    };

    TString Alias;
    const TStructExprType* Type = nullptr;
    TMaybe<TColumnOrder> Order;
    EInputPriority Priority = External;
    TSet<TString> UsedExternalColumns;
};

using TInputs = TVector<TInput>;

using TProjectionOrders = TVector<TMaybe<std::pair<TColumnOrder, bool>>>;

void ScanSublinks(TExprNode::TPtr root, TNodeSet& sublinks) {
    VisitExpr(root, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("PgSubLink")) {
            sublinks.insert(node.Get());
            return false;
        }

        return true;
    });
}

bool ScanColumns(TExprNode::TPtr root, TInputs& inputs, const THashSet<TString>& possibleAliases,
    bool* hasStar, bool& hasColumnRef, THashSet<TString>& refs, THashMap<TString, THashSet<TString>>* qualifiedRefs,
    TExtContext& ctx, bool scanColumnsOnly, bool hasEmitPgStar = false, THashMap<TString, TString> usedInUsing = {}) {
    bool isError = false;
    VisitExpr(root, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("PgSubLink")) {
            return false;
        } else if (node->IsCallable("PgStar")) {
            if (!hasStar) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Star is not allowed here"));
                isError = true;
                return false;
            }

            if (*hasStar) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Duplicate star"));
                isError = true;
                return false;
            }

            if (hasColumnRef) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Star is incompatible to column reference"));
                isError = true;
                return false;
            }

            *hasStar = true;
            if (inputs.empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Star can't be used without FROM"));
                isError = true;
                return false;
            }
        } else if (node->IsCallable("PgQualifiedStar")) {
            if (!hasStar || !qualifiedRefs) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Star is not allowed here"));
                isError = true;
                return false;
            }

            if (*hasStar) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Star is incompatible to column reference"));
                isError = true;
                return false;
            }

            hasColumnRef = true;
            if (inputs.empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Column reference can't be used without FROM"));
                isError = true;
                return false;
            }

            TString alias(node->Head().Content());
            if (possibleAliases.find(alias) == possibleAliases.end()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), TStringBuilder() << "Unknown alias: " << alias));
                isError = true;
                return false;
            }

            for (ui32 priority : {TInput::Projection, TInput::Current, TInput::External}) {
                for (ui32 inputIndex = 0; inputIndex < inputs.size(); ++inputIndex) {
                    auto& x = inputs[inputIndex];
                    if (priority != x.Priority) {
                        continue;
                    }

                    if (x.Alias.empty() || alias != x.Alias) {
                        continue;
                    }

                    for (const auto& item : x.Type->GetItems()) {
                        if (!item->GetName().StartsWith("_yql_")) {
                            (*qualifiedRefs)[alias].insert(TString(item->GetName()));
                            if (x.Priority == TInput::External) {
                                x.UsedExternalColumns.insert(TString(item->GetName()));
                            }
                        }
                    }

                    break;
                }
            }
        } else if (node->IsCallable("PgColumnRef")) {
            if (hasStar && *hasStar && !hasEmitPgStar) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Star is incompatible to column reference"));
                isError = true;
                return false;
            }

            hasColumnRef = true;
            if (inputs.empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Column reference can't be used without FROM"));
                isError = true;
                return false;
            }

            if (node->ChildrenSize() == 2 && possibleAliases.find(node->Head().Content()) == possibleAliases.end()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), TStringBuilder() << "Unknown alias: " << node->Head().Content()));
                isError = true;
                return false;
            }
            auto lcase = to_lower(TString(node->Tail().Content()));
            if (auto it = usedInUsing.find(lcase); node->ChildrenSize() == 1 && it != usedInUsing.end()) {
                refs.insert(it->second);
            } else {
                TString foundAlias;
                bool matchedAlias = false;
                ui32 matchedAliasI = 0;
                TMaybe<ui32> matchedAliasIndex;
                TMaybe<ui32> matchedAliasIndexI;
                for (ui32 priority : {TInput::Projection, TInput::Current, TInput::External}) {
                    ui32 matches = 0;
                    for (ui32 inputIndex = 0; inputIndex < inputs.size(); ++inputIndex) {
                        auto& x = inputs[inputIndex];
                        if (priority != x.Priority) {
                            continue;
                        }

                        if (node->ChildrenSize() == 2) {
                            if (x.Alias.empty() || node->Head().Content() != x.Alias) {
                                continue;
                            }
                        }

                        if (!x.Alias.empty()) {
                            if (node->Tail().Content() == x.Alias) {
                                matchedAlias = true;
                                matchedAliasIndex = inputIndex;
                            } else if (AsciiEqualsIgnoreCase(node->Tail().Content(), x.Alias)) {
                                ++matchedAliasI;
                                matchedAliasIndexI = inputIndex;
                            }
                        }

                        if (x.Order && x.Order->IsDuplicated(TString(node->Tail().Content()))) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()),
                                    TStringBuilder() << "Column reference is ambiguous: " << node->Tail().Content()));
                                isError = true;
                                return false;
                        }

                        bool isVirtual;
                        auto pos = x.Type->FindItemI(node->Tail().Content(), &isVirtual);
                        if (pos) {
                            foundAlias = x.Alias;
                            ++matches;
                            if (!scanColumnsOnly && matches > 1) {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()),
                                    TStringBuilder() << "Column reference is ambiguous: " << node->Tail().Content()));
                                isError = true;
                                return false;
                            }

                            if (x.Priority == TInput::External) {
                                auto name = TString(x.Type->GetItems()[*pos]->GetCleanName(isVirtual));
                                x.UsedExternalColumns.insert(name);
                            }
                        }
                    }

                    if (matches) {
                        break;
                    }

                    if (!matches && priority == TInput::External) {
                        if (scanColumnsOnly) {
                            // projection columns aren't available yet
                            return true;
                        }

                        TInput* tableRefInput = nullptr;
                        if (matchedAlias) {
                            tableRefInput = &inputs[*matchedAliasIndex];
                        } else {
                            if (matchedAliasI > 1) {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()),
                                        TStringBuilder() << "Table reference is ambiguous: " << node->Tail().Content()));
                                isError = true;
                                return false;
                            }

                            if (matchedAliasI == 1) {
                                tableRefInput = &inputs[*matchedAliasIndexI];
                            }
                        }

                        if (!tableRefInput) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()),
                                TStringBuilder() << "No such column: " << node->Tail().Content()));
                            isError = true;
                            return false;
                        }

                        for (const auto& item : tableRefInput->Type->GetItems()) {
                            if (!item->GetName().StartsWith("_yql_")) {
                                refs.insert(TString(item->GetName()));
                                if (tableRefInput->Priority == TInput::External) {
                                    tableRefInput->UsedExternalColumns.insert(TString(item->GetName()));
                                }
                            }
                        }

                        return true;
                    }
                }

                if (foundAlias && qualifiedRefs) {
                    (*qualifiedRefs)[foundAlias].insert(TString(node->Tail().Content()));
                } else {
                    refs.insert(TString(node->Tail().Content()));
                }
            }
        }
        return true;
    });

    return !isError;
}

bool ScanColumnsForSublinks(bool& needRebuildSubLinks, bool& needRebuildTestExprs, const TNodeSet& sublinks,
    TInputs& inputs, const THashSet<TString>& possibleAliases, bool& hasColumnRef, THashSet<TString>& refs,
    THashMap<TString, THashSet<TString>>* qualifiedRefs, TExtContext& ctx, bool scanColumnsOnly) {
    needRebuildSubLinks = false;
    needRebuildTestExprs = false;
    for (const auto& s : sublinks) {
        if (s->Child(1)->IsCallable("Void")) {
            needRebuildSubLinks = true;
        }

        const auto& testRowLambda = *s->Child(3);
        if (!testRowLambda.IsCallable("Void")) {
            YQL_ENSURE(testRowLambda.IsLambda());
            if (s->Child(2)->IsCallable("Void")) {
                needRebuildTestExprs = true;

                if (!ScanColumns(testRowLambda.TailPtr(), inputs, possibleAliases, nullptr, hasColumnRef,
                    refs, qualifiedRefs, ctx, scanColumnsOnly)) {
                    return false;
                }
            }
        }
    }

    return true;
}

bool ValidateWindowRefs(const TExprNode::TPtr& root, const TExprNode* windows, TExprContext& ctx) {
    bool isError = false;
    VisitExpr(root, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("PgWindowCall")) {
            if (!windows) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                    "No window definitions"));
                isError = true;
                return false;
            }

            auto ref = node->Child(1);
            if (ref->IsAtom()) {
                auto name = ref->Content();
                if (!name) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                        "Empty window name is not allowed"));
                    isError = true;
                    return false;
                }

                bool found = false;
                for (const auto& x : windows->Children()) {
                    if (x->Head().Content() == name) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                        TStringBuilder() << "Not found window name: " << name));
                    isError = true;
                    return false;
                }
            } else {
                YQL_ENSURE(ref->IsCallable("PgAnonWindow"));
                auto index = FromString<ui32>(ref->Head().Content());
                if (index >= windows->ChildrenSize()) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                        "Wrong index of window"));
                    isError = true;
                    return false;
                }
            }
        }

        return true;
    });

    return !isError;
}

TString MakeAliasedColumn(TStringBuf alias, TStringBuf column) {
    if (!alias) {
        return TString(column);
    }

    return TStringBuilder() << "_alias_" << alias << "." << column;
}

const TItemExprType* AddAlias(const TString& alias, const TItemExprType* item, TExprContext& ctx) {
    if (!alias) {
        return item;
    }

    return ctx.MakeType<TItemExprType>(MakeAliasedColumn(alias, item->GetName()), item->GetItemType());
}

TStringBuf RemoveAlias(TStringBuf column) {
    TStringBuf tmp;
    return RemoveAlias(column, tmp);
}

TStringBuf RemoveAlias(TStringBuf column, TStringBuf& alias) {
    if (!column.StartsWith("_alias_")) {
        alias = "";
        return column;
    }

    auto columnPos = column.find('.', 7);
    YQL_ENSURE(columnPos != TString::npos);
    columnPos += 1;
    YQL_ENSURE(columnPos != column.size());
    alias = column.substr(7, columnPos - 7 - 1);
    return column.substr(columnPos);
}

const TItemExprType* RemoveAlias(const TItemExprType* item, TExprContext& ctx) {
    auto name = item->GetName();
    if (!name.StartsWith("_alias_")) {
        return item;
    }

    return ctx.MakeType<TItemExprType>(RemoveAlias(name), item->GetItemType());
}

const TItemExprType* RenameOnOrder(TExprContext& ctx, TColumnOrder& order, const TItemExprType* item) {
    if (auto newName = order.AddColumn(TString(item->GetName())); newName != item->GetName()) {
        return ctx.MakeType<TItemExprType>(newName, item->GetItemType());
    }
    return item;
}

void AddColumns(const TInputs& inputs, const bool* hasStar, const THashSet<TString>& refs,
    const THashMap<TString, THashSet<TString>>* qualifiedRefs,
    TVector<const TItemExprType*>& items, TExprContext& ctx, const THashMap<TString, TString>& usedInUsing = {})
{
    THashSet<TString> usedRefs;
    THashSet<TString> usedAliases;
    THashSet<TString> present;
    TColumnOrder order;
    for (ui32 priority : { TInput::Projection, TInput::Current, TInput::External }) {
        for (const auto& x : inputs) {
            if (priority != x.Priority) {
                continue;
            }

            if (hasStar && *hasStar) {
                if (x.Priority == TInput::External) {
                    continue;
                }

                for (ui32 i = 0; i < x.Type->GetSize(); ++i) {
                    auto item = x.Type->GetItems()[i];
                    if (!item->GetName().StartsWith("_yql_")) {
                        TString lcase = to_lower(TString(item->GetName()));
                        if (auto it = usedInUsing.find(lcase); it != usedInUsing.end()) {
                            if (!present.contains(lcase)) {
                                items.push_back(ctx.MakeType<TItemExprType>(it->second, item->GetItemType()));
                                present.emplace(lcase);
                            }
                            continue;
                        }
                        item = AddAlias(x.Alias, item, ctx);
                        items.push_back(item);
                    }
                }

                continue;
            }

            for (const auto& ref : refs) {
                if (usedRefs.contains(ref)) {
                    continue;
                }

                bool isVirtual;
                auto pos = x.Type->FindItemI(ref, &isVirtual);
                if (pos) {
                    auto item = x.Type->GetItems()[*pos];
                    TString lcase = to_lower(TString(item->GetCleanName(isVirtual)));
                    if (auto it = usedInUsing.find(lcase); it != usedInUsing.end() && !present.contains(lcase)) {
                        items.push_back(ctx.MakeType<TItemExprType>(it->second, item->GetItemType()));
                        present.emplace(lcase);
                    }
                    item = AddAlias(x.Alias, item, ctx);
                    items.push_back(item);
                    usedRefs.insert(TString(item->GetCleanName(isVirtual)));
                }
            }

            if (qualifiedRefs && qualifiedRefs->contains(x.Alias)) {
                if (usedAliases.contains(x.Alias)) {
                    continue;
                }

                for (const auto& ref : qualifiedRefs->find(x.Alias)->second) {
                    auto pos = x.Type->FindItemI(ref, nullptr);
                    if (pos) {
                        auto item = x.Type->GetItems()[*pos];
                        item = AddAlias(x.Alias, item, ctx);
                        items.push_back(item);
                    }
                }

                usedAliases.insert(x.Alias);
            }
        }
    }
    for (auto& e: items) {
        e = RenameOnOrder(ctx, order, e);
    }
}

IGraphTransformer::TStatus RebuildLambdaColumns(const TExprNode::TPtr& root, const TExprNode::TPtr& argNode,
    TExprNode::TPtr& newRoot, const TInputs& inputs, TExprNode::TPtr* expandedColumns, TExtContext& ctx,
    THashMap<TString, TString> usedInUsing={}) {
    bool hasExternalInput = false;
    for (const auto& i : inputs) {
        if (i.Priority == TInput::External) {
            hasExternalInput = true;
            break;
        }
    }

    TOptimizeExprSettings optSettings(nullptr);
    optSettings.VisitChecker = [](const TExprNode& node) {
        if (node.IsCallable("PgSubLink")) {
            return false;
        }

        return true;
    };

    return OptimizeExpr(root, newRoot, [&](const TExprNode::TPtr& node, TExprContext&) -> TExprNode::TPtr {
        if (node->IsCallable("PgStar")) {
            TVector<std::pair<TString, TString>> aliased;
            TExprNode::TListType orderAtoms;
            TColumnOrder localOrder;
            THashSet<TString> usedFromUsing;
            for (ui32 priority : { TInput::Projection, TInput::Current, TInput::External }) {
                for (const auto& x : inputs) {
                    if (priority != x.Priority) {
                        continue;
                    }

                    if (x.Priority == TInput::External) {
                        continue;
                    }

                    auto order = x.Order;
                    for (const auto& item : x.Type->GetItems()) {
                        if (!item->GetName().StartsWith("_yql_")) {
                            if (!order) {
                                auto lcase = to_lower(TString(item->GetName()));
                                if (usedFromUsing.contains(lcase)) {
                                    continue;
                                }
                                if (usedInUsing.contains(lcase)) {
                                    usedFromUsing.emplace(lcase);
                                    aliased.emplace_back(x.Alias, usedInUsing[lcase]);
                                } else {
                                    aliased.emplace_back(x.Alias, item->GetName());
                                }
                            }
                        }
                    }

                    if (order) {
                        for (const auto& [o, gen_col] : *order) {
                            if (!o.StartsWith("_yql_")) {
                                auto lcase = to_lower(o);
                                if (usedFromUsing.contains(lcase)) {
                                    continue;
                                }
                                if (usedInUsing.contains(lcase)) {
                                    usedFromUsing.emplace(lcase);
                                    aliased.emplace_back(x.Alias, usedInUsing[lcase]);
                                } else {
                                    aliased.emplace_back(x.Alias, o);
                                }
                            }
                        }
                    }
                }
            }

            decltype(aliased) aliasedRightUsingOrder;
            for (auto& [alias, name] : aliased) {
                if (usedInUsing.contains(to_lower(name))) {
                    aliasedRightUsingOrder.emplace_back(alias, name);
                }
            }
            for (auto& [alias, name] : aliased) {
                if (!usedInUsing.contains(to_lower(name))) {
                    aliasedRightUsingOrder.emplace_back(alias, name);
                }
            }
            aliased = std::move(aliasedRightUsingOrder);

            for (auto& [alias, name] : aliased) {
                if (!hasExternalInput) {
                    auto aliasedAtom = ctx.Expr.NewAtom(node->Pos(), NTypeAnnImpl::MakeAliasedColumn(alias, name));
                    auto originalAtom = ctx.Expr.NewAtom(node->Pos(), name);
                    orderAtoms.emplace_back(ctx.Expr.NewList(node->Pos(), {originalAtom, aliasedAtom}));
                } else {
                    orderAtoms.emplace_back(ctx.Expr.NewAtom(node->Pos(), NTypeAnnImpl::MakeAliasedColumn(alias, name)));
                }
            }

            if (expandedColumns) {
                *expandedColumns = ctx.Expr.NewList(node->Pos(), std::move(orderAtoms));
            }

            return argNode;
        }

        if (node->IsCallable("PgColumnRef")) {
            const TInput* matchedAliasInput = nullptr;
            const TInput* matchedAliasInputI = nullptr;
            if (node->ChildrenSize() == 1 && usedInUsing.contains(node->Tail().Content())) {
                return ctx.Expr.Builder(node->Pos())
                            .Callable("Member")
                                .Add(0, argNode)
                                .Atom(1, node->Tail().Content())
                            .Seal()
                            .Build();
            }
            for (ui32 priority : { TInput::Projection, TInput::Current, TInput::External }) {
                for (const auto& x : inputs) {
                    if (priority != x.Priority) {
                        continue;
                    }
                    if (node->ChildrenSize() == 2) {
                        if (x.Alias.empty() || node->Head().Content() != x.Alias) {
                            continue;
                        }
                    }

                    if (!x.Alias.empty()) {
                        if (node->Tail().Content() == x.Alias) {
                            matchedAliasInput = &x;
                        } else if (AsciiEqualsIgnoreCase(node->Tail().Content(), x.Alias)) {
                            matchedAliasInputI = &x;
                        }
                    }

                    auto pos = x.Type->FindItemI(node->Tail().Content(), nullptr);
                    if (pos) {
                        return ctx.Expr.Builder(node->Pos())
                            .Callable("Member")
                                .Add(0, argNode)
                                .Atom(1, MakeAliasedColumn(x.Alias, x.Type->GetItems()[*pos]->GetName()))
                            .Seal()
                            .Build();
                    }
                }
            }

            if (!matchedAliasInput && matchedAliasInputI) {
                matchedAliasInput = matchedAliasInputI;
            }

            if (matchedAliasInput) {
                return ctx.Expr.Builder(node->Pos())
                    .Callable("PgToRecord")
                        .Callable(0, "DivePrefixMembers")
                            .Add(0, argNode)
                            .List(1)
                                .Atom(0, MakeAliasedColumn(matchedAliasInput->Alias, ""))
                            .Seal()
                        .Seal()
                        .List(1)
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                                ui32 pos = 0;
                                for (ui32 i = 0; i < matchedAliasInput->Type->GetSize(); ++i) {
                                    auto columnName = matchedAliasInput->Order ?
                                        matchedAliasInput->Order.GetRef()[i].PhysicalName :
                                        matchedAliasInput->Type->GetItems()[i]->GetName();
                                    if (!columnName.StartsWith("_yql_")) {
                                        parent.List(pos++)
                                            .Atom(0, columnName)
                                            .Atom(1, columnName)
                                        .Seal();
                                    }
                                }

                                return parent;
                            })
                        .Seal()
                    .Seal()
                    .Build();
            }

            YQL_ENSURE(false, "Missing input");
        }

        if (node->IsCallable("PgQualifiedStar")) {
            TExprNode::TListType members;
            for (ui32 priority : { TInput::Projection, TInput::Current, TInput::External }) {
                for (const auto& x : inputs) {
                    if (priority != x.Priority) {
                        continue;
                    }

                    if (x.Alias.empty() || node->Head().Content() != x.Alias) {
                        continue;
                    }

                    auto order = x.Order;
                    TExprNode::TListType orderAtoms;
                    for (const auto& item : x.Type->GetItems()) {
                        if (!item->GetName().StartsWith("_yql_")) {
                            if (!order) {
                                orderAtoms.push_back(ctx.Expr.NewAtom(node->Pos(),
                                    NTypeAnnImpl::MakeAliasedColumn(hasExternalInput ? x.Alias : "", item->GetName())));
                            }

                            members.push_back(ctx.Expr.Builder(node->Pos())
                                .List()
                                    .Atom(0, NTypeAnnImpl::MakeAliasedColumn(hasExternalInput ? x.Alias : "", item->GetName()))
                                    .Callable(1, "Member")
                                        .Add(0, argNode)
                                        .Atom(1, MakeAliasedColumn(x.Alias, item->GetName()))
                                    .Seal()
                                .Seal()
                                .Build());
                        }
                    }

                    if (order) {
                        for (const auto& [o, gen_col] : *order) {
                            if (!o.StartsWith("_yql_")) {
                                orderAtoms.push_back(ctx.Expr.NewAtom(node->Pos(),
                                    NTypeAnnImpl::MakeAliasedColumn(hasExternalInput ? x.Alias : "", o)));
                            }
                        }
                    }

                    if (expandedColumns) {
                        *expandedColumns = ctx.Expr.NewList(node->Pos(), std::move(orderAtoms));
                    }

                    return ctx.Expr.NewCallable(node->Pos(), "AsStruct", std::move(members));
                }
            }

            YQL_ENSURE(false, "missing input");
        }

        return node;
    }, ctx.Expr, optSettings);
}

IGraphTransformer::TStatus RebuildSubLinks(const TExprNode::TPtr& root, TExprNode::TPtr& newRoot,
    const TNodeSet& sublinks, const TInputs& inputs, const TExprNode::TPtr& rowType, TExtContext& ctx) {
    TExprNode::TListType inputTypesItems;
    for (const auto& x : inputs) {
        inputTypesItems.push_back(ctx.Expr.Builder(root->Pos())
            .List()
                .Atom(0, x.Alias)
                .Add(1, ExpandType(root->Pos(), *x.Type, ctx.Expr))
            .Seal()
            .Build());
    }

    auto inputTypes = ctx.Expr.NewList(root->Pos(), std::move(inputTypesItems));

    return OptimizeExpr(root, newRoot, [&](const TExprNode::TPtr& node, TExprContext&) -> TExprNode::TPtr {
        if (!node->IsCallable("PgSubLink")) {
            return node;
        }

        if (!sublinks.contains(node.Get())) {
            return node;
        }

        auto children = node->ChildrenList();
        if (children[1]->IsCallable("Void")) {
            children[1] = inputTypes;
        } else {
            if (!node->Child(3)->IsCallable("Void")) {
                if (!children[2]->IsCallable("Void")) {
                    return node;
                }

                // rebuild lambda for row test
                auto argNode = ctx.Expr.NewArgument(node->Pos(), "row");
                auto valueNode = ctx.Expr.NewArgument(node->Pos(), "value");
                auto arguments = ctx.Expr.NewArguments(node->Pos(), { argNode, valueNode });
                TExprNode::TPtr newLambdaRoot;
                auto status = RebuildLambdaColumns(node->Child(3)->TailPtr(), argNode, newLambdaRoot, inputs, nullptr, ctx);
                auto oldValueNode = node->Child(3)->Head().Child(0);
                newLambdaRoot = ctx.Expr.ReplaceNode(std::move(newLambdaRoot), *oldValueNode, valueNode);
                if (status == IGraphTransformer::TStatus::Error) {
                    return nullptr;
                }

                children[2] = rowType;
                children[3] = ctx.Expr.NewLambda(node->Pos(), std::move(arguments), std::move(newLambdaRoot));
            } else {
                return node;
            }
        }

        return ctx.Expr.NewCallable(node->Pos(), node->Content(), std::move(children));
    }, ctx.Expr, TOptimizeExprSettings(nullptr));
}

void MakeOptionalColumns(const TStructExprType*& structType, TExprContext& ctx) {
    bool needRebuild = false;
    for (const auto& item : structType->GetItems()) {
        if (!item->GetItemType()->IsOptionalOrNull()) {
            needRebuild = true;
            break;
        }
    }

    if (!needRebuild) {
        return;
    }

    auto newItems = structType->GetItems();
    for (auto& item : newItems) {
        if (!item->GetItemType()->IsOptionalOrNull()) {
            item = ctx.MakeType<TItemExprType>(item->GetName(), ctx.MakeType<TOptionalExprType>(item->GetItemType()));
        }
    }

    structType = ctx.MakeType<TStructExprType>(newItems);
}

void ScanAggregations(const TExprNode::TPtr& root, bool& hasAggregations) {
    VisitExpr(root, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("PgAgg")) {
            hasAggregations = true;
            return false;
        }

        if (node->IsCallable("PgGrouping")) {
            hasAggregations = true;
            return false;
        }

        if (node->IsCallable("PgSubLink")) {
            return false;
        }

        return true;
    });

    TNodeSet sublinks;
    ScanSublinks(root, sublinks);
    for (const auto& s : sublinks) {
        const auto& testRowLambda = *s->Child(3);
        if (!testRowLambda.IsCallable("Void")) {
            YQL_ENSURE(testRowLambda.IsLambda());
            ScanAggregations(testRowLambda.TailPtr(), hasAggregations);
        }
    }
}

struct TGroupExpr {
    TExprNode::TPtr OriginalRoot;
    ui64 Hash;
    TExprNode::TPtr TypeNode;
};

ui64 CalculateExprHash(const TExprNode& root, TNodeMap<ui64>& visited) {
    auto it = visited.find(&root);
    if (it != visited.end()) {
        return it->second;
    }

    ui64 hash = 0;
    switch (root.Type()) {
    case TExprNode::EType::Callable:
        hash = CseeHash(root.Content().Size(), hash);
        hash = CseeHash(root.Content().Data(), root.Content().Size(), hash);
        [[fallthrough]];
    case TExprNode::EType::List:
        hash = CseeHash(root.ChildrenSize(), hash);
        for (ui32 i = 0; i < root.ChildrenSize(); ++i) {
            hash = CombineHashes(CalculateExprHash(*root.Child(i), visited), hash);
        }

        break;
    case TExprNode::EType::Atom:
        hash = CseeHash(root.Content().Size(), hash);
        hash = CseeHash(root.Content().Data(), root.Content().Size(), hash);
        hash = CseeHash(root.GetFlagsToCompare(), hash);
        break;
    case TExprNode::EType::World:
        break;
    case TExprNode::EType::Lambda:
        hash = CseeHash(root.ChildrenSize(), hash);
        hash = CseeHash(root.Head().ChildrenSize(), hash);
        for (ui32 argIndex = 0; argIndex < root.Head().ChildrenSize(); ++argIndex) {
            visited.emplace(root.Head().Child(argIndex), argIndex);
        }

        for (ui32 bodyIndex = 1; bodyIndex < root.ChildrenSize(); ++bodyIndex) {
            hash = CombineHashes(CalculateExprHash(*root.Child(bodyIndex), visited), hash);
        }
        break;
    default:
        YQL_ENSURE(false, "Unexpected node type");
    }

    visited.emplace(&root, hash);
    return hash;
}

bool ExprNodesEquals(const TExprNode& left, const TExprNode& right, TNodeSet& visited) {
    if (!visited.emplace(&left).second) {
        return true;
    }

    if (left.Type() != right.Type()) {
        return false;
    }

    switch (left.Type()) {
    case TExprNode::EType::Callable:
        if (left.Content() != right.Content()) {
            return false;
        }

        [[fallthrough]];
    case TExprNode::EType::List:
        if (left.ChildrenSize() != right.ChildrenSize()) {
            return false;
        }

        for (ui32 i = 0; i < left.ChildrenSize(); ++i) {
            if (!ExprNodesEquals(*left.Child(i), *right.Child(i), visited)) {
                return false;
            }
        }

        return true;
    case TExprNode::EType::Atom:
        return left.Content() == right.Content() && left.GetFlagsToCompare() == right.GetFlagsToCompare();
    case TExprNode::EType::Argument:
        return left.GetArgIndex() == right.GetArgIndex();
    case TExprNode::EType::World:
        return true;
    case TExprNode::EType::Lambda:
        if (left.ChildrenSize() != right.ChildrenSize()) {
            return false;
        }

        if (left.Head().ChildrenSize() != right.Head().ChildrenSize()) {
            return false;
        }

        for (ui32 i = 1; i < left.ChildrenSize(); ++i) {
            if (!ExprNodesEquals(*left.Child(i), *right.Child(i), visited)) {
                return false;
            }
        }

        return true;
    default:
        YQL_ENSURE(false, "Unexpected node type");
    }
}

TMaybe<bool> ScanExprForMatchedGroup(const TExprNode::TPtr& row, const TExprNode& root, const TVector<TGroupExpr>& exprs,
    TNodeOnNodeOwnedMap& replaces, TNodeMap<ui64>& hashVisited, TNodeMap<TMaybe<bool>>& nodeVisited, TExprContext& ctx, TMaybe<ui32> groupingDepth) {
    auto it = nodeVisited.find(&root);
    if (it != nodeVisited.end()) {
        return it->second;
    }

    if (root.IsCallable("PgSubLink")) {
        const auto& testRowLambda = *root.Child(3);
        if (!testRowLambda.IsCallable("Void")) {
            hashVisited[testRowLambda.Head().Child(0)] = 0; // original row
            hashVisited[testRowLambda.Head().Child(1)] = 1; // sublink value
            ScanExprForMatchedGroup(testRowLambda.Head().ChildPtr(0), testRowLambda.Tail(),
                exprs, replaces, hashVisited, nodeVisited, ctx, Nothing());
        }

        nodeVisited[&root] = false;
        return false;
    }

    if (root.IsCallable("PgAgg")) {
        nodeVisited[&root] = false;
        return false;
    }

    if (root.IsCallable("PgGrouping")) {
        groupingDepth = 0;
    }

    bool hasChanges = false;
    for (const auto& child : root.Children()) {
        auto childrenDepth = groupingDepth;
        if (childrenDepth.Defined()) {
            childrenDepth = *childrenDepth + 1;
        }

        auto ret = ScanExprForMatchedGroup(row, *child, exprs, replaces, hashVisited, nodeVisited, ctx, childrenDepth);
        if (!ret) {
            nodeVisited[&root] = Nothing();
            return Nothing();
        }

        if (!*ret) {
            hasChanges = true;
        }
    }

    if (groupingDepth.Defined() && *groupingDepth == 0) {
        for (const auto& child : root.Children()) {
            if (child->IsCallable("PgGroupRef")) {
                continue;
            }

            if (!replaces.contains(child.Get())) {
                ctx.AddError(TIssue(ctx.GetPosition(root.Pos()),
                    "arguments to GROUPING must be grouping expressions of the associated query level"));
                return Nothing();
            }
        }
    }

    if (hasChanges) {
        nodeVisited[&root] = false;
        return false;
    }

    if (!groupingDepth || *groupingDepth == 1) {
        ui64 hash = CalculateExprHash(root, hashVisited);
        for (ui32 i = 0; i < exprs.size(); ++i) {
            if (exprs[i].Hash != hash) {
                continue;
            }

            TNodeSet equalsVisited;
            if (!ExprNodesEquals(*exprs[i].OriginalRoot, root, equalsVisited)) {
                continue;
            }

            TStringBuf memberName;
            if (IsPlainMemberOverArg(root, memberName)) {
                replaces[&root] = ctx.Builder(root.Pos())
                    .Callable("PgGroupRef")
                        .Add(0, row)
                        .Add(1, exprs[i].TypeNode)
                        .Atom(2, ToString(i))
                        .Atom(3, memberName)
                    .Seal()
                    .Build();
            } else {
                replaces[&root] = ctx.Builder(root.Pos())
                    .Callable("PgGroupRef")
                        .Add(0, row)
                        .Add(1, exprs[i].TypeNode)
                        .Atom(2, ToString(i))
                    .Seal()
                    .Build();
            }

            nodeVisited[&root] = false;
            return false;
        }
    }

    nodeVisited[&root] = true;
    return true;
}

TExprNode::TPtr ReplaceGroupByExpr(const TExprNode::TPtr& root, const TExprNode& groups, TExprContext& ctx) {
    // calculate hashes
    TVector<TGroupExpr> exprs;
    TExprNode::TListType typeNodes;
    for (ui32 index = 0; index < groups.ChildrenSize(); ++index) {
        const auto& g = *groups.Child(index);
        const auto& lambda = g.Tail();
        TNodeMap<ui64> visited;
        visited[&lambda.Head().Head()] = 0;
        exprs.push_back({
            lambda.TailPtr(),
            CalculateExprHash(lambda.Tail(), visited),
            ExpandType(g.Pos(), *lambda.GetTypeAnn(), ctx)
            });
    }

    TNodeOnNodeOwnedMap replaces;
    TNodeMap<ui64> hashVisited;
    TNodeMap<TMaybe<bool>> nodeVisited;
    hashVisited[&root->Head().Head()] = 0;
    auto scanStatus = ScanExprForMatchedGroup(root->Head().HeadPtr(), root->Tail(), exprs, replaces, hashVisited, nodeVisited, ctx, Nothing());
    if (!scanStatus) {
        return nullptr;
    }

    auto ret = root;
    if (replaces.empty()) {
        return ret;
    }

    TOptimizeExprSettings settings(nullptr);
    settings.VisitTuples = true;
    auto status = RemapExpr(ret, ret, replaces, ctx, settings);
    YQL_ENSURE(status != IGraphTransformer::TStatus::Error);
    return ret;
}

ui32 RegisterGroupExpression(const TExprNode::TPtr& root, const TExprNode::TPtr& args, const TExprNode::TPtr& group,
    THashMap<ui64, TVector<ui32>>& hashes, TExprNode::TListType& groupExprsItems, TExprContext& ctx) {
    TNodeMap<ui64> visitedHashes;
    visitedHashes[&args->Head()] = 0;
    auto hash = CalculateExprHash(*root, visitedHashes);
    auto it = hashes.find(hash);
    if (it != hashes.end()) {
        for (auto i : it->second) {
            TNodeSet visitedNodes;
            if (ExprNodesEquals(*root, groupExprsItems[i]->Tail().Tail(), visitedNodes)) {
                return i;
            }
        }
    }

    auto index = groupExprsItems.size();
    hashes[hash].push_back(index);
    auto newLambda = ctx.Builder(group->Pos())
        .Lambda()
            .Param("row")
            .ApplyPartial(args, root)
                .With(0, "row")
            .Seal()
        .Seal()
        .Build();

    newLambda->Head().Head().SetArgIndex(0);
    auto newExpr = ctx.ChangeChild(*group, 1, std::move(newLambda));
    groupExprsItems.push_back(newExpr);
    return index;
}

bool BuildGroupingSets(const TExprNode& data, TExprNode::TPtr& groupSets, TExprNode::TPtr& groupExprs, TExprContext& ctx) {
    TExprNode::TListType groupSetsItems, groupExprsItems;
    THashMap<ui64, TVector<ui32>> hashes;
    for (const auto& child : data.Children()) {
        const auto& lambda = child->Tail();
        TExprNode::TPtr sets;
        if (lambda.Tail().IsCallable("PgGroupingSet")) {
            const auto& gs = lambda.Tail();
            auto kind = gs.Head().Content();
            if (kind == "cube" || kind == "rollup") {
                TExprNode::TListType indices;
                for (const auto& expr : gs.Tail().Children()) {
                    auto index = RegisterGroupExpression(expr, lambda.HeadPtr(), child, hashes, groupExprsItems, ctx);
                    indices.push_back(ctx.NewAtom(expr->Pos(), ToString(index)));
                }

                TExprNode::TListType setsItems;
                if (kind == "rollup") {
                    // generate N+1 sets
                    for (ui32 i = 0; i <= indices.size(); ++i) {
                        TExprNode::TListType oneSetItems;
                        for (ui32 j = 0; j < i; ++j) {
                            oneSetItems.push_back(indices[j]);
                        }

                        setsItems.push_back(ctx.NewList(data.Pos(), std::move(oneSetItems)));
                    }
                } else {
                    // generate 2**N sets
                    YQL_ENSURE(indices.size() <= 5, "Too many CUBE components");
                    ui32 count = (1u << indices.size());
                    for (ui32 i = 0; i < count; ++i) {
                        TExprNode::TListType oneSetItems;
                        for (ui32 j = 0; j < indices.size(); ++j) {
                            if ((1u << j) & i) {
                                oneSetItems.push_back(indices[j]);
                            }
                        }

                        setsItems.push_back(ctx.NewList(data.Pos(), std::move(oneSetItems)));
                    }
                }

                sets = ctx.NewList(data.Pos(), std::move(setsItems));
            } else {
                YQL_ENSURE(kind == "sets");
                TExprNode::TListType setsItems;
                for (ui32 setIndex = 1; setIndex < gs.ChildrenSize(); ++setIndex) {
                    const auto& g = gs.Child(setIndex);
                    TExprNode::TListType oneSetItems;
                    for (const auto& expr : g->Children()) {
                        auto index = RegisterGroupExpression(expr, lambda.HeadPtr(), child, hashes, groupExprsItems, ctx);
                        oneSetItems.push_back(ctx.NewAtom(expr->Pos(), ToString(index)));
                    }

                    setsItems.push_back(ctx.NewList(data.Pos(), std::move(oneSetItems)));
                }

                sets = ctx.NewList(data.Pos(), std::move(setsItems));
            }
        } else {
            auto index = RegisterGroupExpression(lambda.TailPtr(), lambda.HeadPtr(), child, hashes, groupExprsItems, ctx);
            sets = ctx.Builder(data.Pos())
                .List()
                    .List(0)
                        .Atom(0, ToString(index))
                    .Seal()
                .Seal()
                .Build();
        }

        groupSetsItems.push_back(sets);
    }

    groupSets = ctx.NewList(data.Pos(), std::move(groupSetsItems));
    groupExprs = ctx.NewList(data.Pos(), std::move(groupExprsItems));
    return true;
}

bool ReplaceProjectionRefs(TExprNode::TPtr& lambda, const TStringBuf& scope, const TProjectionOrders& projectionOrders,
    const TExprNode::TPtr& result, TExprContext& ctx) {
    if (result) {
        YQL_ENSURE(result->Tail().ChildrenSize() == projectionOrders.size());
    }

    TOptimizeExprSettings optSettings(nullptr);
    optSettings.VisitChecker = [](const TExprNode& node) {
        if (node.IsCallable("PgSubLink")) {
            return false;
        }

        return true;
    };

    auto status = OptimizeExpr(lambda, lambda, [&](const TExprNode::TPtr& node, TExprContext&) -> TExprNode::TPtr {
        if (node->IsCallable("PgProjectionRef")) {
            if (result) {
                YQL_ENSURE(projectionOrders.size() == result->Tail().ChildrenSize());
            }

            auto index = FromString<ui32>(node->Head().Content());
            ui32 current = 0;
            for (ui32 i = 0; i < projectionOrders.size(); ++i) {
                if (index >= current && index < current + projectionOrders[i]->first.Size()) {
                    TStringBuf column = projectionOrders[i]->first[index - current].PhysicalName;
                    TStringBuf alias;
                    column = RemoveAlias(column, alias);

                    if (result && projectionOrders[i]->second) {
                        // expression subgraph
                        const auto& lambda = result->Tail().Child(i)->Tail();
                        return lambda.TailPtr();
                    }

                    if (!result || alias.Empty()) {
                        return ctx.Builder(node->Pos())
                            .Callable("PgColumnRef")
                                .Atom(0, column)
                            .Seal()
                            .Build();
                    }

                    return ctx.Builder(node->Pos())
                        .Callable("PgColumnRef")
                            .Atom(0, alias)
                            .Atom(1, column)
                        .Seal()
                        .Build();
                }

                current += projectionOrders[i]->first.Size();
            }

            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << scope << ": position " << (1 + index) << " is not in select list"));
            return nullptr;
        }

        return node;
    }, ctx, optSettings);

    return status != IGraphTransformer::TStatus::Error;
}

bool ValidateByProjection(TExtContext& ctx, const TExprNodePtr lambda, const THashSet<TString>& refs, const TExprNodePtr projection) {
    auto projectionItems = projection->Tail().ChildrenSize();

    // if column in order is ambigous, throw error
    // column reference is not ambigous when
    // it contains only in PgResultItem with one output (not star/qualified star)
    // it has same projection lambda for all projection outputs with that name
    THashMap<TString, ui32> lowerCaseToIndex;
    TVector<TExprNodePtr> columnProjections;
    auto emplace = [&](const TString& name) {
        if (lowerCaseToIndex.emplace(to_lower(name), lowerCaseToIndex.size()).second) {
            columnProjections.emplace_back();
        }
    };
    // case when order by function of column from projection
    if (lambda->Child(0)->Children().size() == 1) {
        auto lambdaArg = lambda->Child(0)->Child(0);
        VisitExpr(lambda->Child(1), [&](const TExprNode::TPtr& node) {
            if (node->IsCallable("Member")) {
                // Projection member
                if (node->Child(0) == lambdaArg) {
                    emplace(to_lower(TString(node->Child(1)->Content())));
                }
                return false;
            }
            return true;
        });
    }
    // case when order by column (or expression with column) from input
    for (auto& name: refs) {
        emplace(name);
    }

    for (ui32 i = 0; i < projectionItems; ++i) {
        const auto lambdaPtr = projection->Tail().Child(i)->TailPtr();
        auto columnOrder = projection->Tail().Child(i)->HeadPtr();
        if (columnOrder->IsAtom()) {
            TString nameLCase = to_lower(TString(columnOrder->Content()));
            if (auto it = lowerCaseToIndex.FindPtr(nameLCase)) {
                if (!columnProjections[*it]) {
                    columnProjections[*it] = lambdaPtr;
                } else if (const TExprNode* l = &*columnProjections[*it], *r = &*lambdaPtr; !CompareExprTrees(l, r)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder() << "ORDER BY column reference '" << nameLCase << "' is ambigous"));
                    return false;
                }
            }
        } else if (EnsureTuple(*columnOrder, ctx.Expr)) {
            // if has duplicates on columns used in refs, throw error
            THashSet<TString> outputProjectionNames;
            for (auto& e: columnOrder->Children()) {
                TString nameLCase;
                if (e->IsAtom()) {
                    nameLCase = to_lower(TString(e->Content()));
                } else if (EnsureTuple(*e, ctx.Expr)) {
                    nameLCase = to_lower(TString(e->HeadPtr()->Content()));
                } else {
                    return false;
                }
                if (lowerCaseToIndex.contains(nameLCase) && !outputProjectionNames.emplace(nameLCase).second) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder() << "ORDER BY column refrence '" << nameLCase << "' is ambigous"));
                    return false;
                }
            }
        } else {
            return false;
        }
    }
    return true;
}

bool ValidateGroups(TInputs& inputs, const THashSet<TString>& possibleAliases,
    const TExprNode& data, TExtContext& ctx, TExprNode::TListType& newGroups, bool& hasNewGroups, bool scanColumnsOnly,
    bool allowAggregates, const TExprNode::TPtr& groupExprs, const TStringBuf& scope,
    const TProjectionOrders* projectionOrders, const TExprNode::TPtr& projection, const TExprNode::TPtr& result, THashMap<TString, TString> usedInUsing={}) {
    newGroups.clear();
    hasNewGroups = false;
    bool hasColumnRef = false;
    for (ui32 index = 0; index < data.ChildrenSize(); ++index) {
        const auto& group = data.Child(index);
        if (!group->IsCallable("PgGroup")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(group->Pos()), "Expected PgGroup"));
            return false;
        }

        YQL_ENSURE(group->Tail().IsLambda());
        THashSet<TString> refs;
        THashMap<TString, THashSet<TString>> qualifiedRefs;
        if (group->Child(0)->IsCallable("Void")) {
            // no effective type yet, scan lambda body
            if (!ScanColumns(group->Tail().TailPtr(), inputs, possibleAliases, nullptr, hasColumnRef,
                refs, &qualifiedRefs, ctx, scanColumnsOnly, false, usedInUsing)) {
                return false;
            }

            if (scanColumnsOnly) {
                continue;
            }

            bool hasNestedAggregations = false;
            ScanAggregations(group->Tail().TailPtr(), hasNestedAggregations);
            if (!allowAggregates && hasNestedAggregations) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(group->Pos()), "Nested aggregations aren't allowed"));
                return false;
            }

            auto newChildren = group->ChildrenList();
            if (projectionOrders) {
                auto newLambda = newChildren[1];
                if (!ReplaceProjectionRefs(newLambda, scope, *projectionOrders, result, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (newLambda != newChildren[1]) {
                    newChildren[1] = newLambda;
                    auto newGroup = ctx.Expr.ChangeChildren(*group, std::move(newChildren));
                    newGroups.push_back(newGroup);
                    hasNewGroups = true;
                    continue;
                }
            }

            TVector<const TItemExprType*> items;
            AddColumns(inputs, nullptr, refs, &qualifiedRefs, items, ctx.Expr, usedInUsing);
            auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
            if (!effectiveType->Validate(group->Pos(), ctx.Expr)) {
                return false;
            }

            if (projection && !ValidateByProjection(ctx, group->Child(1), refs, projection)) {
                return false;
            }

            auto typeNode = ExpandType(group->Pos(), *effectiveType, ctx.Expr);

            auto argNode = ctx.Expr.NewArgument(group->Pos(), "row");
            auto arguments = ctx.Expr.NewArguments(group->Pos(), { argNode });
            TExprNode::TPtr newRoot;
            auto status = RebuildLambdaColumns(group->Tail().TailPtr(), argNode, newRoot, inputs, nullptr, ctx, usedInUsing);
            if (status == IGraphTransformer::TStatus::Error) {
                return false;
            }

            auto newLambda = ctx.Expr.NewLambda(group->Pos(), std::move(arguments), std::move(newRoot));

            newChildren[0] = typeNode;
            newChildren[1] = newLambda;
            auto newGroup = ctx.Expr.NewCallable(group->Pos(), "PgGroup", std::move(newChildren));
            newGroups.push_back(newGroup);
            hasNewGroups = true;
            continue;
        }

        if (groupExprs) {
            auto ret = ReplaceGroupByExpr(group->TailPtr(), groupExprs->Tail(), ctx.Expr);
            if (!ret) {
                return false;
            }

            if (ret != group->TailPtr()) {
                newGroups.push_back(ctx.Expr.ChangeChild(*group, 1, std::move(ret)));
                hasNewGroups = true;
                continue;
            }
        }

        newGroups.push_back(data.ChildPtr(index));
    }

    return true;
}

TExprNode::TPtr SaveExtraColumns(TPositionHandle pos, const THashMap<ui32, TSet<TString>>& columns, ui32 inputsCount, TExprContext& ctx) {
    TExprNode::TListType groups;
    for (ui32 i = 0; i < inputsCount; ++i) {
        TExprNode::TListType columnsPerGroup;
        auto it = columns.find(i);
        if (it != columns.end()) {
            for (const auto& x : it->second) {
                columnsPerGroup.push_back(ctx.NewAtom(pos, x));
            }
        }

        auto columnsPerGroupList = ctx.NewList(pos, std::move(columnsPerGroup));
        groups.push_back(columnsPerGroupList);
    }

    return ctx.NewList(pos, std::move(groups));
}

TMap<TString, ui32> ExtractExternalColumns(const TExprNode& select) {
    TMap<TString, ui32> res;
    const auto& option = select.Head();
    auto setItems = GetSetting(option, "set_items");
    YQL_ENSURE(setItems);
    for (const auto& s : setItems->Tail().Children()) {
        YQL_ENSURE(s->IsCallable("PgSetItem"));
        auto extTypes = GetSetting(s->Head(), "final_ext_types");
        YQL_ENSURE(extTypes);
        ui32 inputIndex = 0;
        for (const auto& input : extTypes->Tail().Children()) {
            auto type = input->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
            for (const auto& item : type->GetItems()) {
                res.insert(std::make_pair(NTypeAnnImpl::MakeAliasedColumn(input->Head().Content(), item->GetName()), inputIndex));
            }

            ++inputIndex;
        }
    }

    return res;
}

bool IsPlainMemberOverArg(const TExprNode& expr, TStringBuf& memberName) {
    if (expr.IsCallable("Member") && expr.Head().IsArgument()) {
        memberName = expr.Tail().Content();
        return true;
    }

    return false;
}

bool ValidateSort(TInputs& inputs, TInputs& subLinkInputs, const THashSet<TString>& possibleAliases,
    const TExprNode& data, TExtContext& ctx, bool& hasNewSort, TExprNode::TListType& newSorts, bool scanColumnsOnly,
    const TExprNode::TPtr& groupExprs, const TStringBuf& scope, const TProjectionOrders* projectionOrders,
    const TExprNode::TPtr& projection, THashMap<TString, TString> usedInUsing={}) {
    newSorts.clear();
    for (ui32 index = 0; index < data.ChildrenSize(); ++index) {
        auto oneSort = data.Child(index);

        TNodeSet sublinks;
        ScanSublinks(oneSort->Child(1)->TailPtr(), sublinks);

        bool hasColumnRef = false;
        THashSet<TString> refs;
        THashMap<TString, THashSet<TString>> qualifiedRefs;
        if (!ScanColumns(oneSort->Child(1)->TailPtr(), inputs, possibleAliases, nullptr, hasColumnRef,
            refs, &qualifiedRefs, ctx, scanColumnsOnly, false, usedInUsing)) {
            return false;
        }

        bool needRebuildSubLinks;
        bool needRebuildTestExprs;
        if (!ScanColumnsForSublinks(needRebuildSubLinks, needRebuildTestExprs, sublinks, subLinkInputs, possibleAliases,
            hasColumnRef, refs, &qualifiedRefs, ctx, scanColumnsOnly)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (scanColumnsOnly) {
            continue;
        }

        auto newLambda = oneSort->ChildPtr(1);
        auto newChildren = oneSort->ChildrenList();

        if (projectionOrders) {
            if (!ReplaceProjectionRefs(newLambda, scope, *projectionOrders, nullptr, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (newLambda != oneSort->ChildPtr(1)) {
                newChildren[1] = newLambda;
                auto newSort = ctx.Expr.ChangeChildren(*oneSort, std::move(newChildren));
                newSorts.push_back(newSort);
                hasNewSort = true;
                continue;
            }
        }

        TVector<const TItemExprType*> items;
        AddColumns(needRebuildSubLinks ? subLinkInputs : inputs, nullptr, refs, &qualifiedRefs, items, ctx.Expr);

        auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
        if (!effectiveType->Validate(oneSort->Pos(), ctx.Expr)) {
            return false;
        }

        auto typeNode = ExpandType(oneSort->Pos(), *effectiveType, ctx.Expr);

        bool hasChanges = false;
        if (needRebuildSubLinks || needRebuildTestExprs) {
            auto arguments = ctx.Expr.NewArguments(oneSort->Pos(), { });

            TExprNode::TPtr newRoot;
            auto status = RebuildSubLinks(newLambda->TailPtr(), newRoot, sublinks, subLinkInputs, typeNode, ctx);
            if (status == IGraphTransformer::TStatus::Error) {
                return false;
            }

            newLambda = ctx.Expr.NewLambda(oneSort->Pos(), std::move(arguments), std::move(newRoot));
            newChildren[1] = newLambda;
            hasChanges = true;
        }

        if (!needRebuildSubLinks && newLambda->Head().ChildrenSize() == 0) {
            auto argNode = ctx.Expr.NewArgument(oneSort->Pos(), "row");
            auto arguments = ctx.Expr.NewArguments(oneSort->Pos(), { argNode });
            TExprNode::TPtr newRoot;
            auto status = RebuildLambdaColumns(newLambda->TailPtr(), argNode, newRoot, inputs, nullptr, ctx, usedInUsing);
            if (status == IGraphTransformer::TStatus::Error) {
                return false;
            }

            newLambda = ctx.Expr.NewLambda(oneSort->Pos(), std::move(arguments), std::move(newRoot));
            newChildren[0] = typeNode;
            newChildren[1] = newLambda;
            hasChanges = true;
        }

        if (hasChanges) {
            auto newSort = ctx.Expr.ChangeChildren(*oneSort, std::move(newChildren));
            newSorts.push_back(newSort);
            hasNewSort = true;
            continue;
        }

        if (groupExprs) {
            auto ret = ReplaceGroupByExpr(newLambda, groupExprs->Tail(), ctx.Expr);
            if (!ret) {
                return false;
            }

            if (ret != newLambda) {
                newSorts.push_back(ctx.Expr.ChangeChild(*oneSort, 1, std::move(ret)));
                hasNewSort = true;
                continue;
            }
        }

        bool canReplaceProjectionExpr = false;
        THashMap<ui64, TVector<ui32>> projectionHashes;
        if (projectionOrders && projection) {
            canReplaceProjectionExpr = true;
            for (const auto& x : *projectionOrders) {
                if (!x->second) { // skip stars
                    canReplaceProjectionExpr = false;
                    break;
                }
            }

            if (canReplaceProjectionExpr) {
                auto projectionItems = projection->Tail().ChildrenSize();
                if (!ValidateByProjection(ctx, oneSort->Child(1), refs, projection)) {
                    return false;
                }
                for (ui32 i = 0; i < projectionItems; ++i) {
                    TNodeMap<ui64> hashVisited;
                    const auto& lambda = projection->Tail().Child(i)->Tail();
                    if (lambda.Head().ChildrenSize() == 1) {
                        if (lambda.Tail().IsCallable() && lambda.Tail().Content() == "Member") {
                            auto lcase = lambda.Tail().Tail().Content();
                            if (usedInUsing.contains(lcase)) {
                                // skip select ..., key, ... join ... using(key) order by key. since there is no
                                // explicit `key` like expression (but contains in the output projection, because of autogeneration)
                                // it just (Member 'row 'key). But the lambda in sort by key is absolutely the same (in non-using variants
                                // it matters since key is not just key, it has an alias: _alias_a.key, and that case wasn't broken when using a
                                // qualified reference)
                                continue;
                            }
                        }
                        hashVisited[&lambda.Head().Head()] = 0;
                        ui64 hash = CalculateExprHash(lambda.Tail(), hashVisited);
                        projectionHashes[hash].push_back(i);
                    }
                }
            }
        }

        if (canReplaceProjectionExpr && newLambda->Head().ChildrenSize() == 1) {
            TNodeMap<ui64> hashVisited;
            hashVisited[&newLambda->Head().Head()] = 0;
            ui64 hash = CalculateExprHash(newLambda->Tail(), hashVisited);
            bool changedSort = false;
            for (auto projectionIndex : projectionHashes[hash]) {
                const auto& projectionLambda = projection->Tail().Child(projectionIndex)->Tail();
                TNodeSet equalsVisited;
                if (ExprNodesEquals(newLambda->Tail(), projectionLambda.Tail(), equalsVisited)) {
                    auto columnName = projectionOrders->at(projectionIndex)->first.front().PhysicalName;
                    newLambda = ctx.Expr.Builder(newLambda->Pos())
                        .Lambda()
                            .Callable("PgColumnRef")
                                .Atom(0, columnName)
                            .Seal()
                        .Seal()
                        .Build();

                    newChildren[1] = newLambda;
                    changedSort = true;
                    break;
                }
            }

            if (changedSort) {
                newChildren[0] = ctx.Expr.NewCallable(newLambda->Pos(), "Void", {});
                auto newSort = ctx.Expr.ChangeChildren(*oneSort, std::move(newChildren));
                newSorts.push_back(newSort);
                hasNewSort = true;
                continue;
            }
        }

        newSorts.push_back(data.ChildPtr(index));
    }

    return true;
}

bool GatherExtraSortColumns(const TExprNode& data, const TInputs& inputs, TExprNode::TPtr& extraInputColumns, TExprNode::TPtr& extraKeys, TExprContext& ctx) {
    ui32 inputsCount = inputs.size() - 1;
    THashMap<ui32, TSet<TString>> columns;
    TSet<TString> keys;
    extraKeys = nullptr;
    for (auto oneSort : data.Children()) {
        TNodeSet sublinks;
        ScanSublinks(oneSort->Child(1)->TailPtr(), sublinks);

        auto scanLambda = [&](const auto& lambda) {
            auto arg = &lambda.Head().Head();
            VisitExpr(lambda.TailPtr(), [&](const TExprNode::TPtr& node) {
                if (node->IsCallable("PgSubLink")) {
                    return false;
                }

                if (node->IsCallable("PgGroupRef")) {
                    if (node->ChildrenSize() == 3) {
                        keys.insert("_yql_agg_key_" + ToString(node->Tail().Content()));
                    } else {
                        keys.insert(ToString(node->Tail().Content()));
                    }
                }

                if (node->IsCallable("Member") && &node->Head() == arg) {
                    TStringBuf alias;
                    TStringBuf column = NTypeAnnImpl::RemoveAlias(node->Tail().Content(), alias);

                    TMaybe<ui32> index;
                    for (ui32 priority : {TInput::Projection, TInput::Current, TInput::External}) {
                        for (ui32 inputIndex = 0; inputIndex < inputs.size(); ++inputIndex) {
                            auto& x = inputs[inputIndex];
                            if (priority != x.Priority) {
                                continue;
                            }

                            if (!alias.empty() && (x.Alias.empty() || alias != x.Alias)) {
                                continue;
                            }

                            auto pos = x.Type->FindItemI(column, nullptr);
                            if (pos) {
                                index = inputIndex;
                                break;
                            }
                        }

                        if (index) {
                            break;
                        }
                    }

                    YQL_ENSURE(index);
                    if (inputs[*index].Priority != TInput::Projection) {
                        columns[*index].insert(TString(node->Tail().Content()));
                    }

                    return false;
                }

                return true;
            });
        };

        scanLambda(*oneSort->Child(1));
        for (const auto& s : sublinks) {
            auto c = ExtractExternalColumns(s->Tail());
            for (const auto&[name, index] : c) {
                YQL_ENSURE(index < inputsCount);
                columns[index].insert("_yql_extra_" + name);
            }

            if (!s->Child(3)->IsCallable("Void")) {
                scanLambda(*s->Child(3));
            }
        }
    }

    extraInputColumns = SaveExtraColumns(data.Pos(), columns, inputsCount, ctx);
    if (!keys.empty()) {
        extraKeys = ctx.Builder(data.Pos())
            .List()
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                    ui32 i = 0;
                    for (const auto& k : keys) {
                        parent.Atom(i++, k);
                    }

                    return parent;
                })
            .Seal()
            .Build();
    }

    for (const auto&[index, set] : columns) {
        if (!set.empty()) {
            return true;
        }
    }

    return false;
}

IGraphTransformer::TStatus PgSetItemWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto& options = input->Head();
    if (!EnsureTuple(options, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    bool scanColumnsOnly = true;
    const TStructExprType* outputRowType;
    bool hasAggregations = false;
    TProjectionOrders projectionOrders;
    bool hasProjectionOrder = false;
    for (;;) {
        outputRowType = nullptr;
        TInputs inputs;
        TInputs joinInputs;
        THashSet<TString> possibleAliases;
        bool hasResult = false;
        bool hasValues = false;
        bool hasJoinOps = false;
        bool hasExtTypes = false;
        bool hasDistinctAll = false;
        bool hasDistinctOn = false;
        bool hasFinalExtraSortColumns = false;
        bool hasEmitPgStar = false;
        bool hasUnknownsAllowed = false;
        TExprNode::TPtr groupExprs;
        TExprNode::TPtr result;
        bool isUsing = 0;
        THashMap<TString, TString> repeatedColumnsInUsing;
        THashMap<TString, const TTypeAnnotationNode*> usingColumnsAnnotation;
        // pass 0 - from/values
        // pass 1 - join
        // pass 2 - ext_types/final_ext_types, final_extra_sort_columns, projection_order or result
        // pass 3 - where, group_by, group_exprs, group_sets
        // pass 4 - having, window
        // pass 5 - result
        // pass 6 - distinct_all, distinct_on
        // pass 7 - sort
        for (ui32 pass = 0; pass < 8; ++pass) {
            if (pass > 1 && !inputs.empty() && !hasJoinOps) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing join_ops"));
                return IGraphTransformer::TStatus::Error;
            }

            for (auto& option : options.Children()) {
                if (!EnsureTupleMinSize(*option, 1, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!EnsureAtom(option->Head(), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                const auto optionName = option->Head().Content();
                if (optionName == "emit_pg_star") {
                    if (option->ChildrenSize() > 1) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                            "Non-empty emit_pg_star option is not allowed"));
                        return IGraphTransformer::TStatus::Error;
                    }
                    hasEmitPgStar = true;
                }
                else if (optionName == "fill_target_columns") {
                    if (option->ChildrenSize() > 2) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                            "Incorrect fill_target_columns option"));
                        return IGraphTransformer::TStatus::Error;
                    }
		        }
                else if (optionName == "unknowns_allowed") {
                    hasUnknownsAllowed = true;
                }
                else if (optionName == "ext_types" || optionName == "final_ext_types") {
                    if (pass != 2) {
                        continue;
                    }

                    if (hasExtTypes) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "ext_types is already set"));
                        return IGraphTransformer::TStatus::Error;
                    }

                    hasExtTypes = true;
                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto& data = option->Tail();
                    if (!ValidateInputTypes(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (const auto& x : data.Children()) {
                        auto alias = x->Head().Content();
                        auto type = x->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                        joinInputs.push_back(TInput{ TString(alias), type, Nothing(), TInput::External, {} });
                        if (!alias.empty()) {
                            possibleAliases.insert(TString(alias));
                        }
                    }
                }
                else if (optionName == "values") {
                    hasValues = true;
                    if (pass != 0) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 3, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto values = option->Child(2);

                    if (!EnsureListType(*values, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto listType = values->GetTypeAnn()->Cast<TListExprType>();
                    if (!EnsureTupleType(values->Pos(), *listType->GetItemType(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto tupleType = listType->GetItemType()->Cast<TTupleExprType>();
                    auto names = option->Child(1);
                    if (!EnsureTupleSize(*names, tupleType->GetSize(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    TVector<const TItemExprType*> outputItems;
                    TVector<TString> columns;
                    for (ui32 i = 0; i < names->ChildrenSize(); ++i) {
                        if (!EnsureAtom(*names->Child(i), ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }
                        TStringBuf columnName = names->Child(i)->Content();
                        outputItems.push_back(ctx.Expr.MakeType<TItemExprType>(columnName, tupleType->GetItems()[i]));
                    }

                    if (!hasUnknownsAllowed) {
                        hasUnknownsAllowed = (GetSetting(options, "unknowns_allowed") != nullptr);
                    }
                    if (!hasUnknownsAllowed) {
                        AdjustUnknownType(outputItems, ctx.Expr);
                    }
                    outputRowType = ctx.Expr.MakeType<TStructExprType>(outputItems);
                    if (!outputRowType->Validate(names->Pos(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }
                else if (optionName == "result") {
                    hasResult = true;
                    if (pass != 5 && pass != 2) {
                        continue;
                    }

                    result = option;
                    if (pass == 2 && GetSetting(options, "projection_order")) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (ui32 index = 0; index < data.ChildrenSize(); ++index) {
                        const auto& column = data.Child(index);
                        if (!column->IsCallable("PgResultItem")) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(column->Pos()), "Expected PgResultItem"));
                            return IGraphTransformer::TStatus::Error;
                        }
                    }

                    THashMap<TString, size_t> outputItemIndex;
                    TVector<const TItemExprType*> outputItems;
                    TExprNode::TListType newResult;
                    bool hasNewResult = false;
                    bool hasStar = false;
                    bool hasColumnRef = false;
                    projectionOrders.resize(data.ChildrenSize());
                    bool updateProjectionOrders = false;
                    for (ui32 index = 0; index < data.ChildrenSize(); ++index) {
                        const auto& column = data.Child(index);
                        YQL_ENSURE(column->Tail().IsLambda());
                        const auto& lambda = column->Tail();
                        THashSet<TString> refs;
                        THashMap<TString, THashSet<TString>> qualifiedRefs;
                        if (column->Child(1)->IsCallable("Void")) {
                            // no effective type yet, scan lambda body
                            if (scanColumnsOnly && !hasProjectionOrder) {
                                if (!projectionOrders[index].Defined()) {
                                    TColumnOrder o;
                                    bool isExpr = false;
                                    THashSet<TString> alreadyPresent;
                                    if (lambda.Tail().IsCallable("PgStar")) {
                                        for (auto& [e, n]: repeatedColumnsInUsing) {
                                            // coalesce of two inputs in first order
                                            o.AddColumn(n);
                                        }
                                        for (ui32 priority : { TInput::Projection, TInput::Current, TInput::External }) {
                                            for (const auto& x : joinInputs) {
                                                if (priority != x.Priority) {
                                                    continue;
                                                }

                                                if (x.Priority == TInput::External) {
                                                    continue;
                                                }

                                                YQL_ENSURE(x.Order);
                                                for (const auto& [col, gen_col] : *x.Order) {
                                                    if (!col.StartsWith("_yql_")) {
                                                        auto lcase = to_lower(col);
                                                        if (alreadyPresent.contains(lcase)) {
                                                            continue;
                                                        }
                                                        if (repeatedColumnsInUsing.contains(lcase)) {
                                                            alreadyPresent.emplace(lcase);
                                                        } else {
                                                            o.AddColumn(MakeAliasedColumn(x.Alias, col));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } else if (lambda.Tail().IsCallable("PgQualifiedStar")) {
                                        auto alias = lambda.Tail().Head().Content();
                                        bool found = false;
                                        for (ui32 priority : {TInput::Projection, TInput::Current, TInput::External}) {
                                            for (ui32 inputIndex = 0; inputIndex < joinInputs.size(); ++inputIndex) {
                                                auto& x = inputs[inputIndex];
                                                if (priority != x.Priority) {
                                                    continue;
                                                }

                                                if (x.Alias.empty() || alias != x.Alias) {
                                                    continue;
                                                }

                                                YQL_ENSURE(x.Order);
                                                for (const auto& [col, gen_col] : *x.Order) {
                                                    if (!col.StartsWith("_yql_")) {
                                                        o.AddColumn(MakeAliasedColumn(x.Alias, col));
                                                    }
                                                }

                                                found = true;
                                                break;
                                            }

                                            if (found) {
                                                break;
                                            }
                                        }

                                        YQL_ENSURE(found);
                                    } else {
                                        isExpr = true;
                                        o.AddColumn(TString(column->Head().Content()));
                                    }

                                    projectionOrders[index] = std::make_pair(o, isExpr);
                                    updateProjectionOrders = true;
                                }
                            }

                            if (pass != 5) {
                                continue;
                            }

                            TNodeSet sublinks;
                            ScanSublinks(lambda.TailPtr(), sublinks);

                            if (!ScanColumns(lambda.TailPtr(), joinInputs, possibleAliases, &hasStar, hasColumnRef,
                                refs, &qualifiedRefs, ctx, scanColumnsOnly, hasEmitPgStar, repeatedColumnsInUsing)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            bool needRebuildSubLinks;
                            bool needRebuildTestExprs;
                            if (!ScanColumnsForSublinks(needRebuildSubLinks, needRebuildTestExprs, sublinks, joinInputs, possibleAliases,
                                hasColumnRef, refs, &qualifiedRefs, ctx, scanColumnsOnly)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (!scanColumnsOnly) {
                                TVector<const TItemExprType*> items;
                                AddColumns(joinInputs, &hasStar, refs, &qualifiedRefs, items, ctx.Expr, repeatedColumnsInUsing);
                                auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
                                if (!effectiveType->Validate(column->Pos(), ctx.Expr)) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                auto typeNode = ExpandType(column->Pos(), *effectiveType, ctx.Expr);

                                auto newColumnChildren = column->ChildrenList();
                                auto newLambda = column->TailPtr();
                                bool hasChanges = false;
                                if (needRebuildSubLinks || needRebuildTestExprs) {
                                    auto arguments = ctx.Expr.NewArguments(column->Pos(), { });

                                    TExprNode::TPtr newRoot;
                                    auto status = RebuildSubLinks(newLambda->TailPtr(), newRoot, sublinks, joinInputs, typeNode, ctx);
                                    if (status == IGraphTransformer::TStatus::Error) {
                                        return IGraphTransformer::TStatus::Error;
                                    }

                                    newLambda = ctx.Expr.NewLambda(column->Pos(), std::move(arguments), std::move(newRoot));
                                    newColumnChildren[2] = newLambda;
                                    hasChanges = true;
                                }

                                if (!needRebuildSubLinks && newLambda->Head().ChildrenSize() == 0) {
                                    auto argNode = ctx.Expr.NewArgument(column->Pos(), "row");
                                    auto arguments = ctx.Expr.NewArguments(column->Pos(), { argNode });
                                    auto expandedColumns = column->HeadPtr();

                                    TExprNode::TPtr newRoot;
                                    auto status = RebuildLambdaColumns(newLambda->TailPtr(), argNode, newRoot, joinInputs, &expandedColumns, ctx, repeatedColumnsInUsing);
                                    if (status == IGraphTransformer::TStatus::Error) {
                                        return IGraphTransformer::TStatus::Error;
                                    }

                                    newLambda = ctx.Expr.NewLambda(column->Pos(), std::move(arguments), std::move(newRoot));
                                    newColumnChildren[0] = expandedColumns;
                                    newColumnChildren[1] = typeNode;
                                    newColumnChildren[2] = newLambda;
                                    hasChanges = true;
                                }

                                if (hasChanges) {
                                    auto newColumn = ctx.Expr.NewCallable(column->Pos(), "PgResultItem", std::move(newColumnChildren));
                                    newResult.push_back(newColumn);
                                    hasNewResult = true;
                                } else {
                                    newResult.push_back(data.ChildPtr(index));
                                }
                            }
                        }
                        else {
                            if (column->Head().IsAtom()) {
                                TStringBuf columnName = column->Head().Content();
                                auto itemExpr = ctx.Expr.MakeType<TItemExprType>(columnName, column->Tail().GetTypeAnn());
                                if (hasEmitPgStar) {
                                    if (!outputItemIndex.contains(columnName)) {
                                        outputItemIndex.emplace(columnName, outputItems.size());
                                        outputItems.emplace_back();
                                    }
                                    outputItems[outputItemIndex[columnName]] = itemExpr;
                                } else {
                                    outputItems.emplace_back(itemExpr);
                                }
                            } else {
                                // star or qualified star
                                TColumnOrder localOrder;
                                TColumnOrder aliasClashes;
                                THashMap<TString, TString> withRemovedAlias;
                                for (auto& e: column->Head().Children()) {
                                    if (e->IsAtom()) {
                                        localOrder.AddColumn(TString(e->Content()));
                                    } else {
                                        localOrder.AddColumn(TString(e->HeadPtr()->Content()));
                                        withRemovedAlias[aliasClashes.AddColumn(TString(e->TailPtr()->Content()))] = e->HeadPtr()->Content();
                                    }
                                }
                                for (const auto& item : column->Tail().GetTypeAnn()->Cast<TStructExprType>()->GetItems()) {
                                    auto name = TString(item->GetName());
                                    if (auto it = withRemovedAlias.FindPtr(name)) {
                                        name = withRemovedAlias[name];
                                    }
                                    auto rightName = localOrder.Find(name);

                                    auto itemRef = ctx.Expr.MakeType<TItemExprType>(rightName, item->GetItemType());

                                    if (hasEmitPgStar) {
                                        if (!outputItemIndex.contains(rightName)) {
                                            outputItemIndex.emplace(rightName, outputItems.size());
                                            outputItems.emplace_back();
                                        }
                                        outputItems[outputItemIndex[rightName]] = itemRef;
                                    } else {
                                        if (isUsing) {
                                            auto lcase = to_lower(TString(itemRef->GetName()));
                                            if (auto it = repeatedColumnsInUsing.find(lcase); it != repeatedColumnsInUsing.end()) {
                                                usingColumnsAnnotation[lcase] = itemRef->GetItemType();
                                                outputItems.emplace_back(itemRef);
                                                repeatedColumnsInUsing.erase(it);
                                            } else if (hasStar && usingColumnsAnnotation.contains(lcase)) {
                                                if (usingColumnsAnnotation[lcase] != itemRef->GetItemType()) {
                                                    TStringStream ss;
                                                    ss << "Expected column of same type when USING: got " << *itemRef->GetItemType() << " != " << *usingColumnsAnnotation[itemRef->GetName()];
                                                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), ss.Str()));
                                                    return IGraphTransformer::TStatus::Error;
                                                }
                                            } else {
                                                outputItems.emplace_back(itemRef);
                                            }
                                        } else {
                                            outputItems.emplace_back(itemRef);
                                        }
                                    }
                                }
                            }

                            // scan lambda for window references
                            auto windows = GetSetting(options, "window");
                            if (!ValidateWindowRefs(column->TailPtr(), windows ? &windows->Tail() : nullptr, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            newResult.push_back(data.ChildPtr(index));
                        }
                    }

                    if (updateProjectionOrders) {
                        TExprNode::TListType projectionOrderItems;
                        for (const auto& x : projectionOrders) {
                            if (x->second) {
                                YQL_ENSURE(x->first.Size() == 1);
                                projectionOrderItems.push_back(ctx.Expr.NewAtom(options.Pos(), x->first[0].LogicalName));
                            } else {
                                TExprNode::TListType columns;
                                for (const auto& [col, gen_col] : x->first) {
                                    columns.push_back(ctx.Expr.NewAtom(options.Pos(), col));
                                }

                                projectionOrderItems.push_back(ctx.Expr.NewList(options.Pos(), std::move(columns)));
                            }
                        }

                        auto newSettings = AddSetting(options, {}, "projection_order", ctx.Expr.NewList(options.Pos(), std::move(projectionOrderItems)), ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }

                    if (pass != 5) {
                        continue;
                    }

                    if (!scanColumnsOnly) {
                        if (hasNewResult) {
                            auto resultValue = ctx.Expr.NewList(options.Pos(), std::move(newResult));
                            auto newSettings = ReplaceSetting(options, {}, "result", resultValue, ctx.Expr);
                            output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                            return IGraphTransformer::TStatus::Repeat;
                        }

                        if (!hasUnknownsAllowed) {
                            hasUnknownsAllowed = (GetSetting(options, "unknowns_allowed") != nullptr);
                        }

                        TColumnOrder order;

                        for (auto& e: outputItems) {
                            e = RenameOnOrder(ctx.Expr, order, e);
                        }

                        if (!hasUnknownsAllowed) {
                            AdjustUnknownType(outputItems, ctx.Expr);
                        }
                        outputRowType = ctx.Expr.MakeType<TStructExprType>(outputItems);
                        if (!outputRowType->Validate(data.Pos(), ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        for (const auto& column : data.Children()) {
                            ScanAggregations(column->TailPtr(), hasAggregations);
                        }

                        if (groupExprs) {
                            TExprNode::TListType newResultItems;
                            bool hasChanges = false;
                            for (ui32 index = 0; index < data.ChildrenSize(); ++index) {
                                const auto& column = *data.Child(index);
                                auto ret = ReplaceGroupByExpr(column.TailPtr(), groupExprs->Tail(), ctx.Expr);
                                if (!ret) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                if (ret != column.TailPtr()) {
                                    hasChanges = true;
                                    newResultItems.push_back(ctx.Expr.ChangeChild(column, 2, std::move(ret)));
                                }
                                else {
                                    newResultItems.push_back(data.ChildPtr(index));
                                }
                            }

                            if (hasChanges) {
                                auto newResult = ctx.Expr.NewList(input->Pos(), std::move(newResultItems));
                                auto newSettings = ReplaceSetting(options, {}, "result", newResult, ctx.Expr);
                                output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                                return IGraphTransformer::TStatus::Repeat;
                            }
                        }
                    }
                }
                else if (optionName == "from") {
                    if (pass != 0) {
                        continue;
                    }

                    if (!EnsureTuple(*option, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto& data = option->Tail();
                    if (!EnsureTupleMinSize(data, 1, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (const auto& p : data.Children()) {
                        if (!EnsureTupleSize(*p, 3, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureAtom(*p->Child(1), ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureTuple(*p->Child(2), ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        for (const auto& name : p->Child(2)->Children()) {
                            if (!EnsureAtom(*name, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }
                        }

                        auto alias = TString(p->Child(1)->Content());
                        auto columnOrder = ctx.Types.LookupColumnOrder(p->Head());
                        const bool isRangeFunction = p->Head().IsCallable("PgResolvedCall");
                        const TStructExprType* inputStructType = nullptr;
                        if (isRangeFunction) {
                            if (alias.empty()) {
                                alias = TString(p->Head().Head().Content());
                            }

                            auto itemType = p->Head().GetTypeAnn();
                            if (itemType->GetKind() == ETypeAnnotationKind::List) {
                                itemType = itemType->Cast<TListExprType>()->GetItemType();
                            }

                            if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
                                inputStructType = itemType->Cast<TStructExprType>();
                                if (p->Child(2)->ChildrenSize() > 0) {
                                    if (p->Child(2)->ChildrenSize() != inputStructType->GetSize()) {
                                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                                        TStringBuilder() << "Expected exactly " << inputStructType->GetSize() << " column names for range function, but got: " << p->Child(2)->ChildrenSize()));
                                        return IGraphTransformer::TStatus::Error;
                                    }

                                    TVector<const TItemExprType*> newItems;
                                    TColumnOrder newOrder;
                                    for (ui32 i = 0; i < inputStructType->GetSize(); ++i) {
                                        auto newName = TString(p->Child(2)->Child(i)->Content());
                                        newOrder.AddColumn(newName);
                                        newItems.push_back(ctx.Expr.MakeType<TItemExprType>(newName, inputStructType->GetItems()[i]->GetItemType()));
                                    }

                                    inputStructType = ctx.Expr.MakeType<TStructExprType>(newItems);
                                    if (!inputStructType->Validate(option->Head().Pos(), ctx.Expr)) {
                                        return IGraphTransformer::TStatus::Error;
                                    }

                                    columnOrder = newOrder;
                                }
                            } else {
                                if (p->Child(2)->ChildrenSize() > 0 && p->Child(2)->ChildrenSize() != 1) {
                                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                                        TStringBuilder() << "Expected exactly one column name for range function, but got: " << p->Child(2)->ChildrenSize()));
                                    return IGraphTransformer::TStatus::Error;
                                }

                                auto memberName = (p->Child(2)->ChildrenSize() == 1) ? p->Child(2)->Head().Content() : alias;
                                TVector<const TItemExprType*> items;
                                items.push_back(ctx.Expr.MakeType<TItemExprType>(memberName, itemType));
                                inputStructType = ctx.Expr.MakeType<TStructExprType>(items);
                                columnOrder = TColumnOrder();
                                columnOrder->AddColumn(TString(memberName));
                            }
                        } else {
                            if (p->Head().IsCallable("PgSelf")) {
                                input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
                                return IGraphTransformer::TStatus::Ok;
                            }
                            if (!EnsureListType(p->Head(), ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            auto inputRowType = p->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                            if (!EnsureStructType(p->Head().Pos(), *inputRowType, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            inputStructType = inputRowType->Cast<TStructExprType>();
                        }

                        if (!alias.empty()) {
                            if (!possibleAliases.insert(alias).second) {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                                    TStringBuilder() << "Duplicated alias: " << alias));
                                return IGraphTransformer::TStatus::Error;
                            }
                        }

                        if (p->Child(2)->ChildrenSize() > 0) {
                            // explicit columns
                            ui32 realColumns = 0;
                            for (const auto& item : inputStructType->GetItems()) {
                                if (!item->GetName().StartsWith("_yql_")) {
                                    ++realColumns;
                                }
                            }

                            if (realColumns != p->Child(2)->ChildrenSize()) {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                                    TStringBuilder() << "Wrong number of columns, expected: " << realColumns
                                    << ", got: " << p->Child(2)->ChildrenSize()));
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (!columnOrder) {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                                    "No column order at source"));
                                return IGraphTransformer::TStatus::Error;
                            }

                            TVector<const TItemExprType*> newStructItems;
                            TColumnOrder newOrder;
                            for (ui32 i = 0; i < p->Child(2)->ChildrenSize(); ++i) {
                                auto pos = inputStructType->FindItemI(columnOrder->at(i).PhysicalName, nullptr);
                                YQL_ENSURE(pos);
                                auto type = inputStructType->GetItems()[*pos]->GetItemType();
                                newOrder.AddColumn(TString(p->Child(2)->Child(i)->Content()));
                                newStructItems.push_back(ctx.Expr.MakeType<TItemExprType>(p->Child(2)->Child(i)->Content(), type));
                            }

                            auto newStructType = ctx.Expr.MakeType<TStructExprType>(newStructItems);
                            if (!newStructType->Validate(p->Child(2)->Pos(), ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            inputs.push_back(TInput{ alias, newStructType, newOrder, TInput::Current, {} });
                        }
                        else {
                            inputs.push_back(TInput{ alias, inputStructType, columnOrder, TInput::Current, {} });
                        }
                    }
                }
                else if (optionName == "where" || optionName == "having") {
                    if (optionName == "where" && pass != 3) {
                        continue;
                    }

                    if (optionName == "having" && pass != 4) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    const auto& data = option->Tail();
                    if (!data.IsCallable("PgWhere")) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()), "Expected PgWhere"));
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (data.Child(0)->IsCallable("Void")) {
                        // no effective type yet, scan lambda body
                        TNodeSet sublinks;
                        ScanSublinks(data.Child(1)->TailPtr(), sublinks);

                        bool hasColumnRef = false;
                        THashSet<TString> refs;
                        THashMap<TString, THashSet<TString>> qualifiedRefs;
                        if (!ScanColumns(data.Child(1)->TailPtr(), joinInputs, possibleAliases, nullptr, hasColumnRef,
                            refs, &qualifiedRefs, ctx, scanColumnsOnly)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        bool needRebuildSubLinks;
                        bool needRebuildTestExprs;
                        if (!ScanColumnsForSublinks(needRebuildSubLinks, needRebuildTestExprs, sublinks, joinInputs, possibleAliases, hasColumnRef,
                            refs, &qualifiedRefs, ctx, scanColumnsOnly)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!scanColumnsOnly) {
                            TVector<const TItemExprType*> items;
                            AddColumns(joinInputs, nullptr, refs, &qualifiedRefs, items, ctx.Expr);
                            auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
                            if (!effectiveType->Validate(data.Pos(), ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            auto typeNode = ExpandType(data.Pos(), *effectiveType, ctx.Expr);
                            auto newLambda = data.ChildPtr(1);
                            bool hasChanges = false;
                            auto newChildren = data.ChildrenList();

                            if (needRebuildSubLinks || needRebuildTestExprs) {
                                auto arguments = ctx.Expr.NewArguments(data.Pos(), {});
                                TExprNode::TPtr newRoot;
                                auto status = RebuildSubLinks(newLambda->TailPtr(), newRoot, sublinks, joinInputs, typeNode, ctx);
                                if (status == IGraphTransformer::TStatus::Error) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                newLambda = ctx.Expr.NewLambda(data.Pos(), std::move(arguments), std::move(newRoot));
                                newChildren[1] = newLambda;
                                hasChanges = true;
                            }

                            if (!needRebuildSubLinks && newLambda->Head().ChildrenSize() == 0) {
                                auto argNode = ctx.Expr.NewArgument(data.Pos(), "row");
                                auto arguments = ctx.Expr.NewArguments(data.Pos(), { argNode });
                                TExprNode::TPtr newRoot;
                                auto status = RebuildLambdaColumns(newLambda->TailPtr(), argNode, newRoot, joinInputs, nullptr, ctx, repeatedColumnsInUsing);
                                if (status == IGraphTransformer::TStatus::Error) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                newLambda = ctx.Expr.NewLambda(data.Pos(), std::move(arguments), std::move(newRoot));

                                newChildren[0] = typeNode;
                                newChildren[1] = newLambda;
                                hasChanges = true;
                            }

                            if (hasChanges) {
                                auto newWhere = ctx.Expr.NewCallable(data.Pos(), "PgWhere", std::move(newChildren));
                                auto newSettings = ReplaceSetting(options, {}, TString(optionName), newWhere, ctx.Expr);
                                output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                                return IGraphTransformer::TStatus::Repeat;
                            }
                        }
                    }
                    else {
                        if (data.GetTypeAnn() && data.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Null) {
                            // nothing to do
                        }
                        else if (data.GetTypeAnn() && data.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
                            auto name = data.GetTypeAnn()->Cast<TPgExprType>()->GetName();
                            if (name != "bool") {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(data.Pos()), TStringBuilder() <<
                                    "Expected bool type, but got: " << name));
                                return IGraphTransformer::TStatus::Error;
                            }
                        }
                        else {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(data.Pos()), TStringBuilder() <<
                                "Expected pg type, but got: " << data.GetTypeAnn()->GetKind()));
                            return IGraphTransformer::TStatus::Error;
                        }
                    }

                    if (!scanColumnsOnly && optionName == "having" && groupExprs) {
                        auto ret = ReplaceGroupByExpr(data.TailPtr(), groupExprs->Tail(), ctx.Expr);
                        if (!ret) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (ret != data.TailPtr()) {
                            auto newSettings = ReplaceSetting(options, {}, "having", ctx.Expr.ChangeChild(data, 1, std::move(ret)), ctx.Expr);
                            output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                            return IGraphTransformer::TStatus::Repeat;
                        }
                    }
                }
                else if (optionName == "join_ops") {
                    if (pass != 1) {
                        continue;
                    }

                    hasJoinOps = true;
                    if (hasValues) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Join and values options are not compatible"));
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (inputs.empty()) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "At least one input expected"));
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    ui32 totalTupleSizes = 0;
                    for (auto child : data.Children()) {
                        if (!EnsureTuple(*child, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }
                        for (const auto& e: child->Children()) {
                            if (!EnsureTupleMinSize(*e, 1, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }
                            if (!EnsureAtom(e->Head(), ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }
                            totalTupleSizes += e->Head().Content() == "push";
                        }
                    }

                    if (totalTupleSizes != inputs.size()) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                            TStringBuilder() << "Unexpected number of joins, got: " << totalTupleSizes
                            << ", expected:" << inputs.size()));
                        return IGraphTransformer::TStatus::Error;
                    }

                    bool needRewrite = false;
                    bool needRewriteUsing = false;
                    ui32 inputIndex = 0;
                    THashSet<TString> usedInUsingBefore;
                    for (ui32 joinGroupNo = 0; joinGroupNo < data.ChildrenSize(); ++joinGroupNo) {
                        // same names allowed in group, but not allowed in different since columns must not repeat
                        THashSet<TString> usedInUsingInThatGroup;
                        for (ui32 i = 0; i < data.Child(joinGroupNo)->ChildrenSize(); ++i) {
                            auto child = data.Child(joinGroupNo)->Child(i);
                            if (!EnsureTupleMinSize(*child, 1, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (!EnsureAtom(child->Head(), ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            auto joinType = child->Head().Content();
                            if (joinType == "push") {
                                joinInputs.push_back(inputs[inputIndex++]);
                                continue;
                            }

                            if (joinType != "cross" && joinType != "inner" && joinType != "left"
                                && joinType != "right" && joinType != "full") {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                                    TStringBuilder() << "Unsupported join type: " << joinType));
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (joinType == "cross") {
                                if (!EnsureTupleSize(*child, 1, ctx.Expr)) {
                                    return IGraphTransformer::TStatus::Error;
                                }
                            }
                            else {
                                if (!EnsureTupleMinSize(*child, 2, ctx.Expr)) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                bool leftSideIsOptional = (joinType == "right" || joinType == "full");
                                bool rightSideIsOptional = (joinType == "left" || joinType == "full");
                                if (leftSideIsOptional) {
                                    for (ui32 j = 0; j < inputIndex; ++j) {
                                        MakeOptionalColumns(joinInputs[j].Type, ctx.Expr);
                                    }
                                }

                                if (rightSideIsOptional) {
                                    MakeOptionalColumns(joinInputs.back().Type, ctx.Expr);
                                }
                                if (child->Child(1)->Content() == "using") {
                                    if (!EnsureTupleMinSize(*child, 3, ctx.Expr)) {
                                        return IGraphTransformer::TStatus::Error;
                                    }
                                    isUsing = 1;
                                    auto columnNames = child->Child(2);
                                    needRewriteUsing = child->ChildrenSize() == 3;
                                    for (ui32 i = 0; i < columnNames->ChildrenSize(); ++i) {
                                        auto lcase = to_lower(TString(columnNames->Child(i)->Content()));
                                        if (usedInUsingBefore.contains(lcase)) {
                                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Duplicated column in USING clause: " << columnNames->Child(i)->Content()));
                                            return IGraphTransformer::TStatus::Error;
                                        }
                                        usedInUsingInThatGroup.emplace(lcase);
                                        repeatedColumnsInUsing.emplace(lcase, columnNames->Child(i)->Content());
                                    }
                                } else {
                                    const auto& quals = child->Tail();
                                    if (!quals.IsCallable("PgWhere")) {
                                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(quals.Pos()), "Expected PgWhere"));
                                        return IGraphTransformer::TStatus::Error;
                                    }

                                    needRewrite = needRewrite || quals.Child(0)->IsCallable("Void");
                                }
                            }
                        }
                        for (const auto& e: usedInUsingInThatGroup) {
                            usedInUsingBefore.emplace(e);
                        }
                    }

                    if (needRewrite || needRewriteUsing) {
                        TExprNode::TListType newJoinGroups;
                        inputIndex = 0;
                        for (ui32 joinGroupNo = 0; joinGroupNo < data.ChildrenSize(); ++joinGroupNo) {
                            TExprNode::TListType newGroupItems;
                            TInputs groupInputs;
                            THashSet<TString> usedInUsingBefore;
                            THashSet<TString> groupPossibleAliases;
                            TVector<TVector<ui32>> indexes;
                            TVector<THashMap<TString, TString>> usingNames;

                            for (ui32 i = 0; i < data.Child(joinGroupNo)->ChildrenSize(); ++i) {

                                auto child = data.Child(joinGroupNo)->Child(i);
                                auto joinType = child->Head().Content();
                                if (joinType == "push") {
                                    indexes.push_back(TVector{(ui32)groupInputs.size()});
                                    usingNames.emplace_back();
                                    groupInputs.push_back(inputs[inputIndex]);
                                    auto alias = inputs[inputIndex].Alias;
                                    if (!alias.empty()) {
                                        groupPossibleAliases.insert(alias);
                                    }
                                    ++inputIndex;
                                    newGroupItems.push_back(data.Child(joinGroupNo)->ChildPtr(i));
                                    continue;
                                }

                                auto rightSide = indexes.back();
                                indexes.pop_back();
                                auto leftSide = indexes.back();
                                indexes.pop_back();
                                auto newVal = leftSide;
                                for (auto& e: rightSide) {
                                    newVal.push_back(e);
                                }

                                auto rightSideUsing = usingNames.back();
                                usingNames.pop_back();
                                auto leftSideUsing = usingNames.back();
                                usingNames.pop_back();

                                indexes.emplace_back(std::move(newVal));

                                auto sideUsing = leftSideUsing;
                                for (auto& e: rightSideUsing) {
                                    sideUsing[e.first] = e.second;
                                }

                                usingNames.emplace_back(std::move(sideUsing));

                                if (joinType == "cross") {
                                    newGroupItems.push_back(data.Child(joinGroupNo)->ChildPtr(i));
                                } else if (needRewrite && child->ChildrenSize() > 1 && child->Child(1)->Content() != "using") {
                                    const auto& quals = child->Tail();
                                    bool hasColumnRef = false;
                                    THashSet<TString> refs;
                                    THashMap<TString, THashSet<TString>> qualifiedRefs;
                                    if (!ScanColumns(quals.Child(1)->TailPtr(), groupInputs, groupPossibleAliases, nullptr, hasColumnRef,
                                        refs, &qualifiedRefs, ctx, scanColumnsOnly)) {
                                        return IGraphTransformer::TStatus::Error;
                                    }

                                    if (!scanColumnsOnly) {
                                        TVector<const TItemExprType*> items;
                                        AddColumns(groupInputs, nullptr, refs, &qualifiedRefs, items, ctx.Expr);
                                        auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
                                        if (!effectiveType->Validate(quals.Pos(), ctx.Expr)) {
                                            return IGraphTransformer::TStatus::Error;
                                        }

                                        auto typeNode = ExpandType(quals.Pos(), *effectiveType, ctx.Expr);

                                        auto argNode = ctx.Expr.NewArgument(quals.Pos(), "row");
                                        auto arguments = ctx.Expr.NewArguments(quals.Pos(), { argNode });
                                        TExprNode::TPtr newRoot;
                                        auto status = RebuildLambdaColumns(quals.Child(1)->TailPtr(), argNode, newRoot, groupInputs, nullptr, ctx);
                                        if (status == IGraphTransformer::TStatus::Error) {
                                            return IGraphTransformer::TStatus::Error;
                                        }

                                        auto newLambda = ctx.Expr.NewLambda(quals.Pos(), std::move(arguments), std::move(newRoot));

                                        auto newChildren = quals.ChildrenList();
                                        newChildren[0] = typeNode;
                                        newChildren[1] = newLambda;
                                        auto newWhere = ctx.Expr.NewCallable(quals.Pos(), "PgWhere", std::move(newChildren));
                                        newGroupItems.push_back(ctx.Expr.ChangeChild(*child, 1, std::move(newWhere)));
                                    } else if (needRewriteUsing) {
                                        newGroupItems.push_back(data.Child(joinGroupNo)->ChildPtr(i));
                                    }
                                } else if (needRewriteUsing) {
                                    auto join = data.Child(joinGroupNo)->ChildPtr(i);
                                    auto inp = join->Child(2);
                                    TExprNode::TListType nodes(inp->ChildrenSize());
                                    for (ui32 colIdx = 0; colIdx < inp->ChildrenSize(); ++colIdx) {
                                        auto name = inp->Child(colIdx)->Content();
                                        TExprNode::TListType lrNames(2);
                                        auto lcase = to_lower(TString(name));

                                        if (leftSideUsing.contains(lcase)) {
                                            lrNames[0] = ctx.Expr.NewList(inp->Pos(), {ctx.Expr.NewAtom(inp->Pos(), leftSideUsing[lcase])});
                                        } else {
                                            int matchCount = 0;
                                            for (const auto& j: leftSide) {
                                                bool isVirtual;
                                                auto pos = groupInputs[j].Type->FindItemI(name, &isVirtual);
                                                if (!pos) {
                                                    continue;
                                                }
                                                lrNames[0] = ctx.Expr.NewAtom(inp->Pos(), MakeAliasedColumn(groupInputs[j].Alias, groupInputs[j].Type->GetItems()[*pos]->GetCleanName(isVirtual)));
                                                ++matchCount;
                                            }
                                            if (!matchCount) {
                                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Can't find column: " << name));
                                                return IGraphTransformer::TStatus::Error;
                                            }
                                            if (matchCount > 1) {
                                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << " common column name \"" << name << "\" appears more than once in left table "));
                                                return IGraphTransformer::TStatus::Error;
                                            }
                                        }
                                        
                                        if (rightSideUsing.contains(lcase)) {
                                            lrNames[1] = ctx.Expr.NewList(inp->Pos(), {ctx.Expr.NewAtom(inp->Pos(), rightSideUsing[lcase])});
                                        } else {
                                            int matchCount = 0;
                                            for (const auto& j: rightSide) {
                                                bool isVirtual;
                                                auto pos = groupInputs[j].Type->FindItemI(name, &isVirtual);
                                                if (!pos) {
                                                    continue;
                                                }
                                                lrNames[1] = ctx.Expr.NewAtom(inp->Pos(), MakeAliasedColumn(groupInputs[j].Alias, groupInputs[j].Type->GetItems()[*pos]->GetCleanName(isVirtual)));
                                                ++matchCount;
                                            }
                                            if (!matchCount) {
                                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Can't find column: " << name));
                                                return IGraphTransformer::TStatus::Error;
                                            }
                                            if (matchCount > 1) {
                                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << " common column name \"" << name << "\" appears more than once in right table "));
                                                return IGraphTransformer::TStatus::Error;
                                            }
                                        }

                                        usingNames.back()[lcase] = name;
                                        nodes[colIdx] = ctx.Expr.NewList(inp->Pos(), std::move(lrNames));
                                    }
                                    TExprNode::TListType newJoin(4);
                                    newJoin[0] = join->Child(0);
                                    newJoin[1] = join->Child(1);
                                    newJoin[2] = join->Child(2);
                                    newJoin[3] = ctx.Expr.NewList(inp->Pos(), std::move(nodes));
                                    newGroupItems.push_back(ctx.Expr.NewList(data.Pos(), std::move(newJoin)));
                                } else {
                                    newGroupItems.push_back(data.Child(joinGroupNo)->ChildPtr(i));
                                }

                                // after left,right,full join type of inputs in current group may be changed for next predicates
                                bool leftSideIsOptional = (joinType == "right" || joinType == "full");
                                bool rightSideIsOptional = (joinType == "left" || joinType == "full");
                                if (leftSideIsOptional) {
                                    for (ui32 j = 0; j + 1 < groupInputs.size(); ++j) {
                                        MakeOptionalColumns(groupInputs[j].Type, ctx.Expr);
                                    }
                                }

                                if (rightSideIsOptional) {
                                    MakeOptionalColumns(groupInputs.back().Type, ctx.Expr);
                                }
                            }

                            auto newGroup = ctx.Expr.NewList(option->Pos(), std::move(newGroupItems));
                            newJoinGroups.push_back(newGroup);
                        }

                        if (!scanColumnsOnly || needRewriteUsing) {
                            auto newJoinGroupsNode = ctx.Expr.NewList(option->Pos(), std::move(newJoinGroups));
                            auto newSettings = ReplaceSetting(options, {}, TString(optionName), newJoinGroupsNode, ctx.Expr);
                            output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                            return IGraphTransformer::TStatus::Repeat;
                        }
                    }
                }
                else if (optionName == "group_by") {
                    if (pass != 3) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    TExprNode::TListType newGroups;
                    bool hasNewGroups = false;
                    if (!ValidateGroups(joinInputs, possibleAliases, data, ctx, newGroups, hasNewGroups, scanColumnsOnly, false, nullptr, "GROUP BY", &projectionOrders, GetSetting(options, "result"), result, repeatedColumnsInUsing)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (!scanColumnsOnly && hasNewGroups) {
                        auto resultValue = ctx.Expr.NewList(options.Pos(), std::move(newGroups));
                        auto newSettings = ReplaceSetting(options, {}, "group_by", resultValue, ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }

                    if (!scanColumnsOnly) {
                        TExprNode::TPtr groupSets, groupExprs;
                        if (!BuildGroupingSets(data, groupSets, groupExprs, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto newSettings = RemoveSetting(options, "group_by", ctx.Expr);
                        newSettings = AddSetting(*newSettings, {}, "group_sets", groupSets, ctx.Expr);
                        newSettings = AddSetting(*newSettings, {}, "group_exprs", groupExprs, ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }
                }
                else if (optionName == "group_exprs") {
                    if (pass != 3) {
                        continue;
                    }

                    groupExprs = option;
                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (const auto& child : data.Children()) {
                        if (!child->IsCallable("PgGroup")) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), "Expected PgGroup"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (child->Tail().Tail().IsCallable("PgGroupingSet")) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), "Grouping sets aren't expanded"));
                            return IGraphTransformer::TStatus::Error;
                        }
                    }
                }
                else if (optionName == "group_sets") {
                    if (pass != 3) {
                        continue;
                    }

                    groupExprs = option;
                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (const auto& child : data.Children()) {
                        // child - one GROUP BY item - list of lists
                        if (!EnsureTuple(*child, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        for (const auto& child2 : child->Children()) {
                            if (!EnsureTuple(*child2, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            for (const auto& atom : child2->Children()) {
                                if (!EnsureAtom(*atom, ctx.Expr)) {
                                    return IGraphTransformer::TStatus::Error;
                                }
                            }
                        }
                    }
                }
                else if (optionName == "window") {
                    if (pass != 4) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto& data = option->Tail();
                    if (!EnsureTupleMinSize(data, 1, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    THashSet<TStringBuf> windowNames;
                    TExprNode::TListType newWindow;
                    bool hasChanges = false;
                    for (ui32 i = 0; i < data.ChildrenSize(); ++i) {
                        auto x = data.ChildPtr(i);
                        if (!x->IsCallable("PgWindow")) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected PgWindow"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (x->Head().Content() && !windowNames.insert(x->Head().Content()).second) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()),
                                TStringBuilder() << "Duplicated window name: " << x->Head().Content()));
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto partitions = x->Child(2);
                        auto sort = x->Child(3);

                        auto newChildren = x->ChildrenList();
                        TExprNode::TListType newGroups;
                        bool hasNewGroups = false;
                        if (!ValidateGroups(joinInputs, possibleAliases, *partitions, ctx, newGroups, hasNewGroups, scanColumnsOnly, true, groupExprs, "", nullptr, nullptr, nullptr, repeatedColumnsInUsing)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        newChildren[2] = ctx.Expr.NewList(x->Pos(), std::move(newGroups));

                        bool hasNewSort = false;
                        TExprNode::TListType newSorts;
                        if (!ValidateSort(joinInputs, joinInputs, possibleAliases, *sort, ctx, hasNewSort, newSorts, scanColumnsOnly, groupExprs, "", nullptr, nullptr, repeatedColumnsInUsing)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (hasNewSort) {
                            newChildren[3] = ctx.Expr.NewList(x->Pos(), std::move(newSorts));
                        }

                        if (hasNewGroups || hasNewSort) {
                            hasChanges = true;
                            newWindow.push_back(ctx.Expr.ChangeChildren(*x, std::move(newChildren)));
                        } else {
                            newWindow.push_back(x);
                        }
                    }

                    if (!scanColumnsOnly && hasChanges) {
                        auto windowValue = ctx.Expr.NewList(options.Pos(), std::move(newWindow));
                        auto newSettings = ReplaceSetting(options, {}, "window", windowValue, ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }
                }
                else if (optionName == "distinct_all") {
                    hasDistinctAll = true;
                    if (pass != 6) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 1, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                } else if (optionName == "distinct_on") {
                    hasDistinctOn = true;
                    if (pass != 6) {
                        continue;
                    }

                    if (scanColumnsOnly) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    bool hasNewGroups = false;
                    TExprNode::TListType newGroups;
                    TInputs projectionInputs;
                    projectionInputs.push_back(TInput{ "", outputRowType, Nothing(), TInput::Projection, {} });
                    if (!ValidateGroups(projectionInputs, {}, data, ctx, newGroups, hasNewGroups, scanColumnsOnly, false, nullptr, "DISTINCT ON", &projectionOrders, GetSetting(options, "result"), nullptr, repeatedColumnsInUsing)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (hasNewGroups) {
                        auto resultValue = ctx.Expr.NewList(options.Pos(), std::move(newGroups));
                        auto newSettings = ReplaceSetting(options, {}, "distinct_on", resultValue, ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }
                }
                else if (optionName == "sort") {
                    if (pass != 7) {
                        continue;
                    }

                    if ((hasDistinctAll || hasDistinctOn) && scanColumnsOnly) {
                        // for SELECT DISTINCT, ORDER BY expressions must appear in select list
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    TInputs projectionInputs = joinInputs;
                    // all row columns are visible too, but projection's columns have more priority
                    if (!scanColumnsOnly) {
                        projectionInputs.push_back(TInput{ "", outputRowType, Nothing(), TInput::Projection, {} });
                    }

                    bool hasNewSort = false;
                    TExprNode::TListType newSortTupleItems;
                    // no effective types yet, scan lambda bodies
                    if (!ValidateSort(projectionInputs, joinInputs, possibleAliases, data, ctx, hasNewSort, newSortTupleItems,
                        scanColumnsOnly, groupExprs, "ORDER BY", &projectionOrders, GetSetting(options, "result"), repeatedColumnsInUsing)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (hasNewSort && !scanColumnsOnly) {
                        auto newSortTuple = ctx.Expr.NewList(data.Pos(), std::move(newSortTupleItems));
                        auto newSettings = ReplaceSetting(options, {}, "sort", newSortTuple, ctx.Expr);

                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }

                    if (!scanColumnsOnly) {
                        if (!GetSetting(options, "final_extra_sort_columns") && !GetSetting(options, "final_extra_sort_keys")) {
                            TExprNode::TPtr extraColumns;
                            TExprNode::TPtr extraKeys;
                            auto hasExtraColumns = GatherExtraSortColumns(data, projectionInputs, extraColumns, extraKeys, ctx.Expr);
                            if (hasExtraColumns || extraKeys) {
                                TExprNode::TPtr newSettings;
                                if (hasExtraColumns) {
                                    newSettings = AddSetting(newSettings ? *newSettings : options, {}, "final_extra_sort_columns", extraColumns, ctx.Expr);
                                }

                                if (extraKeys) {
                                    newSettings = AddSetting(newSettings ? *newSettings : options, {}, "final_extra_sort_keys", extraKeys, ctx.Expr);
                                }

                                output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                                return IGraphTransformer::TStatus::Repeat;
                            }
                        }
                    }
                }
                else if (optionName == "final_extra_sort_columns") {
                    if (pass != 2) {
                        continue;
                    }

                    hasFinalExtraSortColumns = true;

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (!EnsureTupleSize(option->Tail(), joinInputs.size(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (const auto& x : option->Tail().Children()) {
                        if (!EnsureTuple(*x, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        for (const auto& y : x->Children()) {
                            if (!EnsureAtom(*y, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }
                        }
                    }
                } else if (optionName == "final_extra_sort_keys") {
                    if (pass != 2) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (!EnsureTuple(option->Tail(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (const auto& x : option->Tail().Children()) {
                        if (!EnsureAtom(*x, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }
                    }
                }
                else if (optionName == "projection_order") {
                    if (pass != 2) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (!EnsureTuple(option->Tail(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    projectionOrders.clear();
                    for (const auto& x : option->Tail().Children()) {
                        if (x->IsAtom()) {
                            TColumnOrder o;
                            o.AddColumn(TString(x->Content()));
                            projectionOrders.push_back(std::make_pair(o, true));
                        } else if (x->IsList()) {
                            TColumnOrder o;
                            for (const auto& y : x->Children()) {
                                if (!EnsureAtom(*y, ctx.Expr)) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                o.AddColumn(TString(y->Content()));
                            }

                            projectionOrders.push_back(std::make_pair(o, false));
                        } else {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "malformed projection_order"));
                        }
                    }
                    hasProjectionOrder = true;
                } else if (optionName == "target_columns") {
                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                } else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                        TStringBuilder() << "Unsupported option: " << optionName));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        if (!hasResult && !hasValues) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing result and values"));
            return IGraphTransformer::TStatus::Error;
        }

        if (hasResult && hasValues) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Either result or values should be specified"));
            return IGraphTransformer::TStatus::Error;
        }

        if (hasDistinctAll && hasDistinctOn) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Distinct ON isn't compatible with distinct over all columns"));
            return IGraphTransformer::TStatus::Error;
        }

        if ((hasDistinctAll || hasDistinctOn) && hasFinalExtraSortColumns) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "for SELECT DISTINCT, ORDER BY expressions must appear in select list"));
            return IGraphTransformer::TStatus::Error;
        }

        auto extTypes = GetSetting(options, "ext_types");
        if (extTypes && scanColumnsOnly) {
            const auto& data = extTypes->Tail();
            bool needRebuild = false;
            for (ui32 i = joinInputs.size() - data.ChildrenSize(), j = 0; i < joinInputs.size(); ++i, ++j) {
                const auto& x = joinInputs[i];
                YQL_ENSURE(x.Priority == TInput::External);
                for (const auto& t : data.Child(j)->Tail().GetTypeAnn()->Cast<TTypeExprType>()->
                    GetType()->Cast<TStructExprType>()->GetItems()) {
                    if (!x.UsedExternalColumns.contains(t->GetName())) {
                        needRebuild = true;
                        break;
                    }
                }

                if (needRebuild) {
                    break;
                }
            }

            TExprNode::TPtr newValue = extTypes->TailPtr();
            if (needRebuild) {
                TExprNode::TListType aliases;
                for (ui32 i = joinInputs.size() - data.ChildrenSize(), j = 0; i < joinInputs.size(); ++i, ++j) {
                    const auto& x = joinInputs[i];
                    YQL_ENSURE(x.Priority == TInput::External);

                    const auto child = data.Child(j);
                    TVector<const TItemExprType*> items;
                    const auto type = data.Child(j)->Tail().GetTypeAnn()->Cast<TTypeExprType>()->
                        GetType()->Cast<TStructExprType>();
                    for (const auto& col : x.UsedExternalColumns) {
                        bool isVirtual;
                        auto pos = type->FindItemI(col, &isVirtual);
                        YQL_ENSURE(pos);
                        items.push_back(type->GetItems()[*pos]->GetCleanItem(isVirtual, ctx.Expr));
                    }

                    auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
                    if (!effectiveType->Validate(child->Pos(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto typeNode = ExpandType(child->Pos(), *effectiveType, ctx.Expr);
                    aliases.push_back(ctx.Expr.NewList(child->Pos(), { child->HeadPtr(), typeNode }));
                }

                newValue = ctx.Expr.NewList(extTypes->Pos(), std::move(aliases));
            }

            auto newSettings = AddSetting(options, {}, "final_ext_types", newValue, ctx.Expr);
            newSettings = RemoveSetting(*newSettings, "ext_types", ctx.Expr);
            output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
            return IGraphTransformer::TStatus::Repeat;
        }

        scanColumnsOnly = !scanColumnsOnly;
        if (scanColumnsOnly) {
            break;
        }
    }

    if ((hasAggregations || GetSetting(options, "having")) && !GetSetting(options, "group_by") && !GetSetting(options, "group_sets")) {
        // add empty group by section
        auto newSettings = AddSetting(options, {}, "group_by", ctx.Expr.NewList(input->Pos(), {}), ctx.Expr);
        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
        return IGraphTransformer::TStatus::Repeat;
    }
    input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(outputRowType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgValuesListWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto& firstValue = input->Head();
    if (!EnsureTuple(firstValue, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    if (!EnsureTupleMinSize(firstValue, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const size_t tupleSize = firstValue.ChildrenSize();
    TVector<TVector<ui32>> types(tupleSize);
    for (size_t j = 0; j < tupleSize; ++j) {
        types[j].reserve(input->ChildrenSize());
    }

    bool needRetype = false;
    for (size_t i = 0; i < input->ChildrenSize(); ++i) {
        auto* value = input->Child(i);

        if (!EnsureTuple(*value, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        // Can't use EnsureTupleSize here since we need the same error message PG produces
        if (value->ChildrenSize() != tupleSize) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(value->Pos()),
                TStringBuilder() << "VALUES lists must all be the same length"));
            return IGraphTransformer::TStatus::Error;
        }

        for (size_t j = 0; j < tupleSize; ++j) {
            auto* item = value->Child(j);
            auto type = item->GetTypeAnn();
            ui32 argType;
            bool convertToPg;
            const auto pos = item->Pos();
            if (!ExtractPgType(type, argType, convertToPg, pos, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (convertToPg) {
                value->ChildRef(j) = ctx.Expr.NewCallable(pos, "ToPg", { value->ChildPtr(j) });
                needRetype = true;
            }
            if (!needRetype) {
                types[j].push_back(argType);
            }
        }
    }
    if (needRetype) {
        return IGraphTransformer::TStatus::Repeat;
    }

    TVector<ui32> commonTypes(tupleSize);
    for (size_t j = 0; j < tupleSize; ++j) {
        bool castRequired = false;
        const NPg::TTypeDesc* commonType;
        if (const auto issue = NPg::LookupCommonType(types[j],
            [j, &input, &ctx](size_t i) {
                return ctx.Expr.GetPosition(input->Child(i)->Child(j)->Pos());
            }, commonType, castRequired))
        {
            ctx.Expr.AddError(*issue);
            return IGraphTransformer::TStatus::Error;
        }
        needRetype |= castRequired;
        commonTypes[j] = commonType->TypeId;
    }
    if (!needRetype) {
        output = ctx.Expr.NewCallable(input->Pos(), "AsListStrict", std::move(input->ChildrenList()));

        return IGraphTransformer::TStatus::Repeat;
    }
    TExprNode::TListType resultValues;
    for (size_t i = 0; i < input->ChildrenSize(); ++i) {
        auto* value = input->Child(i);
        TExprNode::TListType rowValues;

        for (size_t j = 0; j < tupleSize; ++j) {
            auto* item = value->Child(j);
            if (item->GetTypeAnn()->Cast<TPgExprType>()->GetId() == commonTypes[j]) {
                rowValues.push_back(item);
            } else {
                rowValues.push_back(WrapWithPgCast(std::move(item), commonTypes[j], ctx.Expr));
            }
        }
        resultValues.push_back(ctx.Expr.NewList(value->Pos(), std::move(rowValues)));
    }
    output = ctx.Expr.NewCallable(input->Pos(), "AsListStrict", std::move(resultValues));
    return IGraphTransformer::TStatus::Repeat;
}

IGraphTransformer::TStatus PgSelectWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto& options = input->Head();
    if (!EnsureTuple(options, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TExprNode* setItems = nullptr;
    TExprNode* setOps = nullptr;
    bool hasSort = false;
    bool hasLimit = false;

    for (ui32 pass = 0; pass < 2; ++pass) {
        for (auto& option : options.Children()) {
            if (!EnsureTupleMinSize(*option, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(option->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            const auto optionName = option->Head().Content();
            if (optionName == "set_ops") {
                if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (pass == 0) {
                    if (!EnsureTupleMinSize(option->Tail(), 1, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (const auto& child : option->Tail().Children()) {
                        if (!EnsureAtom(*child, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (child->Content() != "push" && child->Content() != "union_all" &&
                            child->Content() != "union" && child->Content() != "except_all" &&
                            child->Content() != "except" && child->Content() != "intersect_all" &&
                            child->Content() != "intersect") {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()),
                                TStringBuilder() << "Unexpected operation: " << child->Content()));
                            return IGraphTransformer::TStatus::Error;
                        }
                    }

                    setOps = &option->Tail();
                }
            } else if (optionName == "set_items") {
                if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (pass == 0) {
                    if (!EnsureTupleMinSize(option->Tail(), 1, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (const auto& child : option->Tail().Children()) {
                        if (!child->IsCallable("PgSetItem")) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), "Expected PgSetItem"));
                            return IGraphTransformer::TStatus::Error;
                        }
                    }

                    setItems = &option->Tail();
                }
            } else if (optionName == "limit" || optionName == "offset") {
                hasLimit = true;
                if (pass != 0) {
                    continue;
                }

                if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto& data = option->ChildRef(1);
                if (data->GetTypeAnn() && data->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Null) {
                    // nothing to do
                } else if (data->GetTypeAnn() && data->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
                    auto name = data->GetTypeAnn()->Cast<TPgExprType>()->GetName();
                    if (name != "int4" && name != "int8") {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(data->Pos()), TStringBuilder() <<
                            "Expected int4/int8 type, but got: " << name));
                        return IGraphTransformer::TStatus::Error;
                    }
                } else {
                    const TTypeAnnotationNode* expectedType = ctx.Expr.MakeType<TOptionalExprType>(
                    ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64));
                    auto convertStatus = TryConvertTo(data, *expectedType, ctx.Expr);
                    if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(data->Pos()), "Mismatch argument types"));
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
                        auto newSettings = ReplaceSetting(options, {}, TString(optionName), option->ChildPtr(1), ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }
                }
            } else if (optionName == "sort") {
                if (pass != 1) {
                    continue;
                }

                if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto& data = option->Tail();
                if (!EnsureTuple(data, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                for (const auto& x : data.Children()) {
                    if (!x->IsCallable("PgSort")) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected PgSort"));
                    }
                }

                hasSort = true;
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                    TStringBuilder() << "Unsupported option: " << optionName));
                return IGraphTransformer::TStatus::Error;
            }
        }
    }

    if (!setItems) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing set_items"));
        return IGraphTransformer::TStatus::Error;
    }

    if (!setOps) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing set_ops"));
        return IGraphTransformer::TStatus::Error;
    }

    if (setOps->ChildrenSize() != setItems->ChildrenSize() * 2 - 1) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Mismatched count of items in set_items and set_ops"));
        return IGraphTransformer::TStatus::Error;
    }

    ui32 balance = 0;
    for (const auto& op : setOps->Children()) {
        if (op->Content() == "push") {
            balance += 1;
        } else {
            if (balance < 2) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Disbalanced set_ops"));
                return IGraphTransformer::TStatus::Error;
            }

            balance -= 1;
        }
    }

    if (balance != 1) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Disbalanced set_ops"));
        return IGraphTransformer::TStatus::Error;
    }

    const bool hasRecursive = AnyOf(setItems->Children(), [](const auto& n) {
        return n->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Unit;
    });

    if (hasRecursive) {
        bool error = false;
        if (setItems->ChildrenSize() != 2) {
            error = true;
        } else if (setItems->Child(0)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Unit) {
            error = true;
        } else if (setOps->Child(2)->Content() != "union" && setOps->Child(2)->Content() != "union_all") {
            error = true;
        } else if (hasSort || hasLimit) {
            error = true;
        }

        if (error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                "Recursive query does not have the form non-recursive-term UNION [ALL] recursive-term"));
            return IGraphTransformer::TStatus::Error;
        }

        auto nonRecursivePart = setItems->ChildPtr(0);
        auto recursivePart = setItems->ChildPtr(1);
        auto tableArg = ctx.Expr.NewArgument(input->Pos(), "table");
        auto order = *ctx.Types.LookupColumnOrder(*nonRecursivePart);
        auto withColumnOrder = KeepColumnOrder(order, tableArg, ctx.Expr);
        auto status = OptimizeExpr(recursivePart, recursivePart, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            Y_UNUSED(ctx);
            if (node->IsCallable("PgSelf")) {
                return withColumnOrder;
            }

            return node;
        }, ctx.Expr, TOptimizeExprSettings(&ctx.Types));

        YQL_ENSURE(status.Level != IGraphTransformer::TStatus::Error);

        auto lambdaBody = ctx.Expr.Builder(input->Pos())
            .Callable("PgSelect")
                .List(0)
                    .List(0)
                        .Atom(0, "set_items")
                        .List(1)
                            .Add(0, recursivePart)
                        .Seal()
                    .Seal()
                    .List(1)
                        .Atom(0, "set_ops")
                        .List(1)
                            .Atom(0, "push")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        auto lambda = ctx.Expr.NewLambda(input->Pos(), ctx.Expr.NewArguments(input->Pos(), { tableArg }), std::move(lambdaBody));

        output = ctx.Expr.Builder(input->Pos())
            .Callable(ToString("PgIterate") + (setOps->Child(2)->Content() == "union_all" ? "All" : ""))
                .Callable(0, "PgSelect")
                    .List(0)
                        .List(0)
                            .Atom(0, "set_items")
                            .List(1)
                                .Add(0, nonRecursivePart)
                            .Seal()
                        .Seal()
                        .List(1)
                            .Atom(0, "set_ops")
                            .List(1)
                                .Atom(0, "push")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Add(1, lambda)
            .Seal()
            .Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    TColumnOrder resultColumnOrder;
    const TStructExprType* resultStructType = nullptr;

    auto status = (1 == setItems->ChildrenSize() && HasSetting(*setItems->Child(0)->Child(0), "unknowns_allowed"))
        ? InferPositionalUnionType(input->Pos(), setItems->ChildrenList(), resultColumnOrder, resultStructType, ctx)
        : InferPgCommonType(input->Pos(), setItems, setOps, resultColumnOrder, resultStructType, ctx);

    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (hasSort) {
        auto option = GetSetting(options, "sort");
        YQL_ENSURE(option);
        const auto& data = option->Tail();
        TInputs projectionInputs;
        projectionInputs.push_back(TInput{ TString(), resultStructType, resultColumnOrder, TInput::Projection, {} });
        TExprNode::TListType newSortTupleItems;

        // no effective types yet, scan lambda bodies
        bool hasNewSort = false;
        TProjectionOrders projectionOrders;
        for (const auto& [col, gen_col] : resultColumnOrder) {
            TColumnOrder clmn;
            clmn.AddColumn(col);
            projectionOrders.push_back(std::make_pair(clmn, true));
        }

        if (!ValidateSort(projectionInputs, projectionInputs, {}, data, ctx, hasNewSort, newSortTupleItems, false, nullptr, "ORDER BY", &projectionOrders, nullptr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (hasNewSort) {
            auto newSortTuple = ctx.Expr.NewList(data.Pos(), std::move(newSortTupleItems));
            auto newSettings = ReplaceSetting(options, {}, "sort", newSortTuple, ctx.Expr);
            output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
            return IGraphTransformer::TStatus::Repeat;
        }
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(resultStructType));
    return ctx.Types.SetColumnOrder(*input, resultColumnOrder, ctx.Expr);
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
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
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
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
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
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
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
            if (!ExtractPgType(child->GetTypeAnn(), pgTypes[i], convertionRequired, child->Pos(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
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
        ui32 targetType = 0;
        TExprNodeList items;
    };

    bool castRequired = false;
    const NPg::TTypeDesc* commonType;
    if (NPg::LookupCommonType(pgTypes, posGetter, commonType, castRequired))
    {
        const auto& lhsTypeId = input->Head().GetTypeAnn()->Cast<TPgExprType>()->GetId();

        THashMap<ui32, TPgListCommonTypeConversion> elemsByType;
        for (size_t i = 1; i < input->ChildrenSize(); ++i) {
            auto& elemsOfType = elemsByType[pgTypes[i]];
            if (elemsOfType.items.empty()) {
                const NPg::TTypeDesc* elemCommonType;
                if (const auto issue = NPg::LookupCommonType({pgTypes[0], pgTypes[i]},
                    posGetter, elemCommonType))
                {
                    ctx.Expr.AddError(*issue);
                    return IGraphTransformer::TStatus::Error;
                }
                elemsOfType.targetType = elemCommonType->TypeId;

                elemsOfType.items.push_back((lhsTypeId == elemsOfType.targetType)
                    ? input->HeadPtr()
                    : WrapWithPgCast(input->HeadPtr(), elemsOfType.targetType, ctx.Expr));
            }
            const auto rhsItemTypeId = input->Child(i)->GetTypeAnn()->Cast<TPgExprType>()->GetId();
            elemsOfType.items.push_back((rhsItemTypeId == elemsOfType.targetType)
                ? input->Child(i)
                : WrapWithPgCast(input->Child(i), elemsOfType.targetType, ctx.Expr));
        }
        TExprNodeList orClausesOfIn;
        orClausesOfIn.reserve(elemsByType.size());

        for (auto& elemsOfType: elemsByType) {
            auto& conversion = elemsOfType.second;
            orClausesOfIn.push_back(BuildUniTypePgIn(std::move(conversion.items), ctx));
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
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
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

IGraphTransformer::TStatus PgSubLinkWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto linkType = input->Child(0)->Content();
    if (linkType != "exists" && linkType != "any" && linkType != "all" && linkType != "expr" && linkType != "array") {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Unknown link type: " << linkType));
        return IGraphTransformer::TStatus::Error;
    }

    bool hasType = false;
    if (!input->Child(1)->IsCallable("Void")) {
        if (!ValidateInputTypes(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        hasType = true;
    }

    if (!input->Child(2)->IsCallable("Void")) {
        if (!EnsureType(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!hasType) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing input types"));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (!input->Child(3)->IsCallable("Void")) {
        auto& lambda = input->ChildRef(3);
        const auto status = ConvertToLambda(lambda, ctx.Expr, (!input->Child(2)->IsCallable("Void") && hasType) ? 2 : 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }
    }

    if (!input->Child(4)->IsCallable("PgSelect")) {
        auto& lambda = input->ChildRef(4);
        const auto status = ConvertToLambda(lambda, ctx.Expr, 0);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (hasType) {
            auto select = lambda->TailPtr();
            if (!select->IsCallable("PgSelect") || select->ChildrenSize() == 0) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Expected PgSelect"));
                return IGraphTransformer::TStatus::Error;
            }

            const auto& settings = select->Head();
            auto setItemsSetting = GetSetting(settings, "set_items");
            if (!setItemsSetting) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Expected set_items"));
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureTupleSize(*setItemsSetting, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureTuple(setItemsSetting->Tail(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto setItems = setItemsSetting->Tail().ChildrenList();
            for (auto& x : setItems) {
                if (!x->IsCallable("PgSetItem") || x->ChildrenSize() == 0) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Expected PgSetItem"));
                    return IGraphTransformer::TStatus::Error;
                }

                const auto& settings = x->Head();
                auto withTypes = AddSetting(settings, x->Pos(), "ext_types", input->ChildPtr(1), ctx.Expr);
                x = ctx.Expr.ChangeChild(*x, 0, std::move(withTypes));
            }

            auto newSetItems = ctx.Expr.NewList(setItemsSetting->Pos(), std::move(setItems));
            auto newSettings = ReplaceSetting(settings, input->Pos(), "set_items", newSetItems, ctx.Expr);
            lambda = ctx.Expr.ChangeChild(*select, 0, std::move(newSettings));
            return IGraphTransformer::TStatus::Repeat;
        }
    }

    if (!hasType) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    const TTypeAnnotationNode* valueType = nullptr;
    if (linkType != "exists") {
        auto itemType = input->Child(4)->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        if (itemType->GetSize() != 1) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                "Expected one column in select"));
            return IGraphTransformer::TStatus::Error;
        }

        valueType = itemType->GetItems()[0]->GetItemType();
        if (!valueType->IsOptionalOrNull()) {
            valueType = ctx.Expr.MakeType<TOptionalExprType>(valueType);
        }
    }

    if (linkType == "all" || linkType == "any") {
        if (input->Child(3)->IsCallable("Void")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing test row expression"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Child(2)->IsCallable("Void")) {
            auto& lambda = input->ChildRef(3);
            const auto status = ConvertToLambda(lambda, ctx.Expr, hasType ? 2 : 1);
            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            auto rowType = input->Child(2)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            if (!UpdateLambdaAllArgumentsTypes(lambda, { rowType, valueType }, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!lambda->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }

            ui32 testExprType;
            bool convertToPg;
            if (!ExtractPgType(lambda->GetTypeAnn(), testExprType, convertToPg, lambda->Pos(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (testExprType && testExprType != NPg::LookupType("bool").TypeId) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Expected pg bool, but got " << NPg::LookupType(testExprType).Name));
                return IGraphTransformer::TStatus::Error;
            }
        }
    } else {
        if (!input->Child(3)->IsCallable("Void")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Test row expression is not allowed"));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (linkType == "expr") {
        input->SetTypeAnn(valueType);
    } else if (linkType == "array") {
        if (valueType->GetKind() == ETypeAnnotationKind::Pg) {
            auto typeId = valueType->Cast<TPgExprType>()->GetId();
            const auto& desc = NPg::LookupType(typeId);
            if (desc.TypeId != desc.ArrayTypeId) {
                valueType = ctx.Expr.MakeType<TPgExprType>(desc.ArrayTypeId);
            }
        }

        input->SetTypeAnn(valueType);
    } else {
        auto result = ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("bool").TypeId);
        input->SetTypeAnn(result);
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgGroupRefWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMaxArgsCount(*input, 4, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureStructType(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureType(*input->Child(1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(2), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (input->ChildrenSize() >= 4) {
        if (!EnsureAtom(*input->Child(3), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    input->SetTypeAnn(input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgGroupingWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMaxArgsCount(*input, 31, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool needRetype = false;
    for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
        auto type = input->Child(i)->GetTypeAnn();
        ui32 argType;
        bool convertToPg;
        if (!ExtractPgType(type, argType, convertToPg, input->Child(i)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (convertToPg) {
            input->ChildRef(i) = ctx.Expr.NewCallable(input->Child(i)->Pos(), "ToPg", { input->ChildPtr(i) });
            needRetype = true;
        }
    }

    if (needRetype) {
        return IGraphTransformer::TStatus::Repeat;
    }

    auto result = ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("int4").TypeId);
    input->SetTypeAnn(result);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgGroupingSetWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto kind = input->Child(0)->Content();
    if (!(kind == "cube" || kind == "rollup" || kind == "sets")) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Unexpected grouping set kind: " << kind));
        return IGraphTransformer::TStatus::Error;
    }

    if (kind != "sets") {
        if (!EnsureMaxArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
        if (!EnsureTuple(*input->Child(i), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TVoidExprType>());
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
            if (!ExtractPgType(structType->GetItems()[i]->GetItemType(), pgType, convertToPg, input->Head().Pos(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
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

} // namespace NTypeAnnImpl
}
