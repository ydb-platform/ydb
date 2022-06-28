#include "type_ann_pg.h"
#include "type_ann_list.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_pg_utils.h>

#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <util/generic/set.h>

namespace NYql {

bool ParsePgIntervalModifier(const TString& str, i32& ret);

namespace NTypeAnnImpl {

bool ValidateInputTypes(const TExprNode& node, TExprContext& ctx) {
    if (!EnsureTuple(node, ctx)) {
        return false;
    }

    for (const auto& x : node.Children()) {
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

    for (const auto& setting : input->Child(isResolved ? 2 : 1)->Children()) {
        if (!EnsureTupleMinSize(*setting, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(setting->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto content = setting->Head().Content();
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Unexpected setting " << content << " in function " << name));

        return IGraphTransformer::TStatus::Error;
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
        if (proc.Name != name) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Mismatch of resolved function name, expected: " << name << ", but got:" << proc.Name));
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

        const TTypeAnnotationNode* result = ctx.Expr.MakeType<TPgExprType>(proc.ResultType);
        if (proc.ReturnSet) {
            result = ctx.Expr.MakeType<TListExprType>(result);
        }

        input->SetTypeAnn(result);
        return IGraphTransformer::TStatus::Ok;
    } else {
        const auto& proc = NPg::LookupProc(TString(name), argTypes);
        auto children = input->ChildrenList();
        auto idNode = ctx.Expr.NewAtom(input->Pos(), ToString(proc.ProcId));
        children.insert(children.begin() + 1, idNode);
        output = ctx.Expr.NewCallable(input->Pos(), "PgResolvedCall", std::move(children));
        return IGraphTransformer::TStatus::Repeat;
    }
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

    auto name = input->Head().GetTypeAnn()->Cast<TPgExprType>()->GetName();
    const TDataExprType* dataType;
    if (name == "bool") {
        dataType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool);
    } else if (name == "int2") {
        dataType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int16);
    } else if (name == "int4") {
        dataType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int32);
    } else if (name == "int8") {
        dataType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64);
    } else if (name == "float4") {
        dataType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Float);
    } else if (name == "float8") {
        dataType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Double);
    } else if (name == "text" || name == "varchar" || name == "cstring") {
        dataType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Utf8);
    } else if (name == "bytea") {
        dataType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::String);
    } else {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Unsupported type: " << name));
        return IGraphTransformer::TStatus::Error;
    }

    auto result = ctx.Expr.MakeType<TOptionalExprType>(dataType);
    input->SetTypeAnn(result);
    return IGraphTransformer::TStatus::Ok;
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

    bool isOptional;
    const TDataExprType* dataType;
    if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TString pgType;
    switch (dataType->GetSlot()) {
    case NUdf::EDataSlot::Bool:
        pgType = "bool";
        break;
    case NUdf::EDataSlot::Int16:
        pgType = "int2";
        break;
    case NUdf::EDataSlot::Int32:
        pgType = "int4";
        break;
    case NUdf::EDataSlot::Int64:
        pgType = "int8";
        break;
    case NUdf::EDataSlot::Float:
        pgType = "float4";
        break;
    case NUdf::EDataSlot::Double:
        pgType = "float8";
        break;
    case NUdf::EDataSlot::String:
        pgType = "bytea";
        break;
    case NUdf::EDataSlot::Utf8:
        pgType = "text";
        break;
    default:
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Unsupported type: " << dataType->GetName()));
        return IGraphTransformer::TStatus::Error;
    }

    auto result = ctx.Expr.MakeType<TPgExprType>(NPg::LookupType(pgType).TypeId);
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
        const auto& oper = NPg::LookupOper(TString(name), argTypes);
        auto children = input->ChildrenList();
        auto idNode = ctx.Expr.NewAtom(input->Pos(), ToString(oper.OperId));
        children.insert(children.begin() + 1, idNode);
        output = ctx.Expr.NewCallable(input->Pos(), "PgResolvedOp", std::move(children));
        return IGraphTransformer::TStatus::Repeat;
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
                TStringBuilder() << "Expected one argument in " << name << " function"));
            return IGraphTransformer::TStatus::Error;
        }

        auto arg = input->Child(3)->GetTypeAnn();
        if (arg->IsOptionalOrNull()) {
            input->SetTypeAnn(arg);
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(arg));
        }
    } else if (name == "row_number") {
        if (input->ChildrenSize() != 3) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                "Expected no arguments in row_number function"));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("int8").TypeId));
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

    const auto& aggDesc = NPg::LookupAggregation(name, argTypes);
    if (aggDesc.Kind != NPg::EAggKind::Normal) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            "Only normal aggregation supported"));
        return IGraphTransformer::TStatus::Error;
    }

    ui32 resultType;
    if (!aggDesc.FinalFuncId) {
        resultType = aggDesc.TransTypeId;
    } else {
        resultType = NPg::LookupProc(aggDesc.FinalFuncId).ResultType;
    }

    auto result = ctx.Expr.MakeType<TPgExprType>(resultType);
    input->SetTypeAnn(result);
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
            if (!EnsureAtom(*x, ctx.Expr)) {
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
    if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
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

    input->SetTypeAnn(input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType());
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
        const auto& inputDesc = NPg::LookupType(inputTypeId);
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
            Y_ENSURE(lambda->ChildrenSize() == 2);
            auto tupleTypeSize = lambda->GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
            auto newArg = ctx.Expr.NewArgument(lambda->Pos(), "row");
            auto newBody = ctx.Expr.ReplaceNode(lambda->TailPtr(), lambda->Head().Head(), newArg);
            TExprNode::TListType bodies;
            for (ui32 i = 0; i < tupleTypeSize; ++i) {
                bodies.push_back(ctx.Expr.Builder(lambda->Pos())
                    .Callable("Nth")
                        .Add(0, newBody)
                        .Atom(1, ToString(i))
                    .Seal()
                    .Build());
            }

            args[2] = ctx.Expr.NewLambda(lambda->Pos(), ctx.Expr.NewArguments(lambda->Pos(), { newArg }), std::move(bodies));
        }

        output = ctx.Expr.NewCallable(input->Pos(), input->Content().substr(0, input->Content().Size() - 5), std::move(args));
        //ctx.Expr.Step.Repeat(TExprStep::ExpandApplyForLambdas);
        //return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
        return IGraphTransformer::TStatus::Repeat;
    }

    TVector<ui32> argTypes;
    bool needRetype = false;
    for (ui32 i = 1; i < lambda->ChildrenSize(); ++i) {
        auto type = lambda->Child(i)->GetTypeAnn();
        ui32 argType;
        bool convertToPg;
        if (!ExtractPgType(type, argType, convertToPg, lambda->Child(i)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (convertToPg) {
            needRetype = true;
        }

        argTypes.push_back(argType);
    }

    if (needRetype) {
        auto newLambda = ctx.Expr.DeepCopyLambda(*lambda);
        for (ui32 i = 1; i < lambda->ChildrenSize(); ++i) {
            auto type = lambda->Child(i)->GetTypeAnn();
            ui32 argType;
            bool convertToPg;
            if (!ExtractPgType(type, argType, convertToPg, lambda->Child(i)->Pos(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (convertToPg) {
                newLambda->ChildRef(i) = ctx.Expr.NewCallable(newLambda->Child(i)->Pos(), "ToPg", { newLambda->ChildPtr(i) });
            }
        }

        lambda = newLambda;
        return IGraphTransformer::TStatus::Repeat;
    }

    const auto& aggDesc = NPg::LookupAggregation(func, argTypes);
    if (aggDesc.Kind != NPg::EAggKind::Normal) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            "Only normal aggregation supported"));
        return IGraphTransformer::TStatus::Error;
    }

    auto idLambda = ctx.Expr.Builder(input->Pos())
        .Lambda()
            .Param("state")
            .Arg("state")
        .Seal()
        .Build();

    auto saveLambda = idLambda;
    auto loadLambda = idLambda;
    auto finishLambda = idLambda;
    if (aggDesc.FinalFuncId) {
        finishLambda = ctx.Expr.Builder(input->Pos())
            .Lambda()
            .Param("state")
            .Callable("PgResolvedCallCtx")
                .Atom(0, NPg::LookupProc(aggDesc.FinalFuncId).Name)
                .Atom(1, ToString(aggDesc.FinalFuncId))
                .List(2)
                .Seal()
                .Arg(3, "state")
            .Seal()
            .Seal()
            .Build();
    }

    auto nullValue = ctx.Expr.NewCallable(input->Pos(), "Null", {});
    auto initValue = nullValue;
    if (aggDesc.InitValue) {
        initValue = ctx.Expr.Builder(input->Pos())
            .Callable("PgCast")
                .Callable(0, "PgConst")
                    .Atom(0, aggDesc.InitValue)
                    .Callable(1, "PgType")
                        .Atom(0, "text")
                    .Seal()
                .Seal()
                .Callable(1, "PgType")
                    .Atom(0, NPg::LookupType(aggDesc.TransTypeId).Name)
                .Seal()
            .Seal()
            .Build();
    }

    const auto& transFuncDesc = NPg::LookupProc(aggDesc.TransFuncId);
    // use first non-null value as state if transFunc is strict
    bool searchNonNullForState = false;
    if (transFuncDesc.IsStrict && !aggDesc.InitValue) {
        Y_ENSURE(argTypes.size() == 1);
        searchNonNullForState = true;
    }

    TExprNode::TPtr initLambda, updateLambda;
    if (!searchNonNullForState) {
        initLambda = ctx.Expr.Builder(input->Pos())
            .Lambda()
                .Param("row")
                .Callable("PgResolvedCallCtx")
                    .Atom(0, transFuncDesc.Name)
                    .Atom(1, ToString(aggDesc.TransFuncId))
                    .List(2)
                    .Seal()
                    .Add(3, initValue)
                    .Apply(4, lambda)
                        .With(0, "row")
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        updateLambda = ctx.Expr.Builder(input->Pos())
            .Lambda()
                .Param("row")
                .Param("state")
                .Callable("Coalesce")
                    .Callable(0, "PgResolvedCallCtx")
                        .Atom(0, transFuncDesc.Name)
                        .Atom(1, ToString(aggDesc.TransFuncId))
                        .List(2)
                        .Seal()
                        .Callable(3, "Coalesce")
                            .Arg(0, "state")
                            .Add(1, initValue)
                        .Seal()
                        .Apply(4, lambda)
                            .With(0, "row")
                        .Seal()
                    .Seal()
                    .Arg(1, "state")
                .Seal()
            .Seal()
            .Build();
    } else {
        initLambda = ctx.Expr.Builder(input->Pos())
            .Lambda()
                .Param("row")
                .Apply(lambda)
                    .With(0, "row")
                .Seal()
            .Seal()
            .Build();

        if (lambdaResult->GetKind() == ETypeAnnotationKind::Null) {
            initLambda = ctx.Expr.Builder(input->Pos())
                .Lambda()
                    .Param("row")
                    .Callable("PgCast")
                        .Apply(0, initLambda)
                            .With(0, "row")
                        .Seal()
                        .Callable(1, "PgType")
                            .Atom(0, NPg::LookupType(aggDesc.TransTypeId).Name)
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        updateLambda = ctx.Expr.Builder(input->Pos())
            .Lambda()
                .Param("row")
                .Param("state")
                .Callable("If")
                    .Callable(0, "Exists")
                        .Arg(0, "state")
                    .Seal()
                    .Callable(1, "Coalesce")
                        .Callable(0, "PgResolvedCallCtx")
                            .Atom(0, transFuncDesc.Name)
                            .Atom(1, ToString(aggDesc.TransFuncId))
                            .List(2)
                            .Seal()
                            .Arg(3, "state")
                            .Apply(4, lambda)
                                .With(0, "row")
                            .Seal()
                        .Seal()
                        .Arg(1, "state")
                    .Seal()
                    .Apply(2, lambda)
                        .With(0, "row")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    auto mergeLambda = ctx.Expr.Builder(input->Pos())
        .Lambda()
            .Param("state1")
            .Param("state2")
            .Callable("Void")
            .Seal()
        .Seal()
        .Build();

    auto zero = ctx.Expr.Builder(input->Pos())
            .Callable("PgConst")
                .Atom(0, "0")
                .Callable(1, "PgType")
                    .Atom(0, "int8")
                .Seal()
            .Seal()
            .Build();

    auto defaultValue = (func != "count") ? nullValue : zero;

    if (aggDesc.SerializeFuncId) {
        const auto& serializeFuncDesc = NPg::LookupProc(aggDesc.SerializeFuncId);
        saveLambda = ctx.Expr.Builder(input->Pos())
            .Lambda()
                .Param("state")
                .Callable("PgResolvedCallCtx")
                    .Atom(0, serializeFuncDesc.Name)
                    .Atom(1, ToString(aggDesc.SerializeFuncId))
                    .List(2)
                    .Seal()
                    .Arg(3, "state")
                .Seal()
            .Seal()
            .Build();
    }

    if (aggDesc.DeserializeFuncId) {
        const auto& deserializeFuncDesc = NPg::LookupProc(aggDesc.DeserializeFuncId);
        loadLambda = ctx.Expr.Builder(input->Pos())
            .Lambda()
                .Param("state")
                .Callable("PgResolvedCallCtx")
                    .Atom(0, deserializeFuncDesc.Name)
                    .Atom(1, ToString(aggDesc.DeserializeFuncId))
                    .List(2)
                    .Seal()
                    .Arg(3, "state")
                    .Callable(4, "PgInternal0")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    if (aggDesc.CombineFuncId) {
        const auto& combineFuncDesc = NPg::LookupProc(aggDesc.CombineFuncId);
        if (combineFuncDesc.IsStrict) {
            mergeLambda = ctx.Expr.Builder(input->Pos())
                .Lambda()
                    .Param("state1")
                    .Param("state2")
                    .Callable("If")
                        .Callable(0, "Exists")
                            .Arg(0, "state1")
                        .Seal()
                        .Callable(1, "Coalesce")
                            .Callable(0, "PgResolvedCallCtx")
                                .Atom(0, combineFuncDesc.Name)
                                .Atom(1, ToString(aggDesc.CombineFuncId))
                                .List(2)
                                .Seal()
                                .Arg(3, "state1")
                                .Arg(4, "state2")
                            .Seal()
                            .Arg(1, "state1")
                        .Seal()
                        .Arg(2, "state2")
                    .Seal()
                .Seal()
                .Build();
        } else {
            mergeLambda = ctx.Expr.Builder(input->Pos())
                .Lambda()
                    .Param("state1")
                    .Param("state2")
                    .Callable("PgResolvedCallCtx")
                        .Atom(0, combineFuncDesc.Name)
                        .Atom(1, ToString(aggDesc.CombineFuncId))
                        .List(2)
                        .Seal()
                        .Arg(3, "state1")
                        .Arg(4, "state2")
                    .Seal()
                .Seal()
                .Build();
        }
    }

    auto typeNode = ExpandType(input->Pos(), *itemType, ctx.Expr);
    if (onWindow) {
        output = ctx.Expr.Builder(input->Pos())
            .Callable("WindowTraits")
                .Add(0, typeNode)
                .Add(1, initLambda)
                .Add(2, updateLambda)
                .Lambda(3)
                    .Param("value")
                    .Param("state")
                    .Callable("Void")
                    .Seal()
                .Seal()
                .Add(4, finishLambda)
                .Add(5, defaultValue)
            .Seal()
            .Build();
    } else {
        output = ctx.Expr.Builder(input->Pos())
            .Callable("AggregationTraits")
                .Add(0, typeNode)
                .Add(1, initLambda)
                .Add(2, updateLambda)
                .Add(3, saveLambda)
                .Add(4, loadLambda)
                .Add(5, mergeLambda)
                .Add(6, finishLambda)
                .Add(7, defaultValue)
            .Seal()
            .Build();
    }

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
    TString Alias;
    const TStructExprType* Type = nullptr;
    TMaybe<TColumnOrder> Order;
    bool External = false;
    TSet<TString> UsedExternalColumns;
};

using TInputs = TVector<TInput>;

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
    TExtContext& ctx) {
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

            for (ui32 pass = 0; pass < 2; ++pass) {
                for (auto& x : inputs) {
                    if (!pass == x.External) {
                        continue;
                    }

                    if (x.Alias.empty() || alias != x.Alias) {
                        continue;
                    }

                    for (const auto& item : x.Type->GetItems()) {
                        if (!item->GetName().StartsWith("_yql_")) {
                            (*qualifiedRefs)[alias].insert(TString(item->GetName()));
                            if (x.External) {
                                x.UsedExternalColumns.insert(TString(item->GetName()));
                            }
                        }
                    }

                    break;
                }
            }
        } else if (node->IsCallable("PgColumnRef")) {
            if (hasStar && *hasStar) {
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

            TString foundAlias;
            for (ui32 pass = 0; pass < 2; ++pass) {
                ui32 matches = 0;
                for (auto& x : inputs) {
                    if (!pass == x.External) {
                        continue;
                    }

                    if (node->ChildrenSize() == 2) {
                        if (x.Alias.empty() || node->Head().Content() != x.Alias) {
                            continue;
                        }
                    }

                    auto pos = x.Type->FindItem(node->Tail().Content());
                    if (pos) {
                        foundAlias = x.Alias;
                        ++matches;
                        if (matches > 1) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()),
                                TStringBuilder() << "Column reference is ambiguous: " << node->Tail().Content()));
                            isError = true;
                            return false;
                        }

                        if (x.External) {
                            x.UsedExternalColumns.insert(TString(node->Tail().Content()));
                        }
                    }
                }

                if (matches) {
                    break;
                }

                if (!matches && pass == 1) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()),
                        TStringBuilder() << "No such column: " << node->Tail().Content()));
                    isError = true;
                    return false;
                }
            }

            if (foundAlias && qualifiedRefs) {
                (*qualifiedRefs)[foundAlias].insert(TString(node->Tail().Content()));
            } else {
                refs.insert(TString(node->Tail().Content()));
            }
        }

        return true;
    });

    return !isError;
}

bool ScanColumnsForSublinks(bool& needRebuildSubLinks, const TNodeSet& sublinks,
    TInputs& inputs, const THashSet<TString>& possibleAliases, bool& hasColumnRef, THashSet<TString>& refs,
    THashMap<TString, THashSet<TString>>* qualifiedRefs, TExtContext& ctx) {
    needRebuildSubLinks = false;
    for (const auto& s : sublinks) {
        if (s->Child(1)->IsCallable("Void")) {
            needRebuildSubLinks = true;
        }

        const auto& testRowLambda = *s->Child(3);
        if (!testRowLambda.IsCallable("Void")) {
            YQL_ENSURE(testRowLambda.IsLambda());
            if (!ScanColumns(testRowLambda.TailPtr(), inputs, possibleAliases, nullptr, hasColumnRef,
                refs, qualifiedRefs, ctx)) {
                return false;
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

void AddColumns(const TInputs& inputs, const bool* hasStar, const THashSet<TString>& refs,
    const THashMap<TString, THashSet<TString>>* qualifiedRefs,
    TVector<const TItemExprType*>& items, TExprContext& ctx) {
    THashSet<TString> usedRefs;
    THashSet<TString> usedAliases;
    for (ui32 pass = 0; pass < 2; ++pass) {
        for (const auto& x : inputs) {
            if (!pass == x.External) {
                continue;
            }

            if (hasStar && *hasStar) {
                if (x.External) {
                    continue;
                }

                for (ui32 i = 0; i < x.Type->GetSize(); ++i) {
                    auto item = x.Type->GetItems()[i];
                    if (!item->GetName().StartsWith("_yql_")) {
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

                auto pos = x.Type->FindItem(ref);
                if (pos) {
                    auto item = x.Type->GetItems()[*pos];
                    item = AddAlias(x.Alias, item, ctx);
                    items.push_back(item);
                    usedRefs.insert(ref);
                }
            }

            if (qualifiedRefs && qualifiedRefs->contains(x.Alias)) {
                if (usedAliases.contains(x.Alias)) {
                    continue;
                }

                for (const auto& ref : qualifiedRefs->find(x.Alias)->second) {
                    auto pos = x.Type->FindItem(ref);
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
}

IGraphTransformer::TStatus RebuildLambdaColumns(const TExprNode::TPtr& root, const TExprNode::TPtr& argNode,
    TExprNode::TPtr& newRoot, const TInputs& inputs, TExprNode::TPtr* expandedColumns, TExtContext& ctx) {
    bool hasExternalInput = false;
    for (const auto& i : inputs) {
        if (i.External) {
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
            TExprNode::TListType orderAtoms;
            for (ui32 pass = 0; pass < 2; ++pass) {
                for (const auto& x : inputs) {
                    if (!pass == x.External) {
                        continue;
                    }

                    if (x.External) {
                        continue;
                    }

                    auto order = x.Order;
                    for (const auto& item : x.Type->GetItems()) {
                        if (!item->GetName().StartsWith("_yql_")) {
                            if (!order) {
                                orderAtoms.push_back(ctx.Expr.NewAtom(node->Pos(),
                                    NTypeAnnImpl::MakeAliasedColumn(hasExternalInput ? x.Alias : "", item->GetName())));
                            }
                        }
                    }

                    if (order) {
                        for (const auto& o : *order) {
                            if (!o.StartsWith("_yql_")) {
                                orderAtoms.push_back(ctx.Expr.NewAtom(node->Pos(),
                                    NTypeAnnImpl::MakeAliasedColumn(hasExternalInput ? x.Alias : "", o)));
                            }
                        }
                    }
                }
            }

            if (expandedColumns) {
                *expandedColumns = ctx.Expr.NewList(node->Pos(), std::move(orderAtoms));
            }

            return argNode;
        }

        if (node->IsCallable("PgColumnRef")) {
            for (ui32 pass = 0; pass < 2; ++pass) {
                for (const auto& x : inputs) {
                    if (!pass == x.External) {
                        continue;
                    }

                    if (node->ChildrenSize() == 2) {
                        if (x.Alias.empty() || node->Head().Content() != x.Alias) {
                            continue;
                        }
                    }

                    auto pos = x.Type->FindItem(node->Tail().Content());
                    if (pos) {
                        return ctx.Expr.Builder(node->Pos())
                            .Callable("Member")
                                .Add(0, argNode)
                                .Atom(1, MakeAliasedColumn(x.Alias, node->Tail().Content()))
                            .Seal()
                            .Build();
                    }
                }
            }

            YQL_ENSURE(false, "Missing input");
        }

        if (node->IsCallable("PgQualifiedStar")) {
            TExprNode::TListType members;
            for (ui32 pass = 0; pass < 2; ++pass) {
                for (const auto& x : inputs) {
                    if (!pass == x.External) {
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
                        for (const auto& o : *order) {
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
        if (!node->IsCallable("PgSubLink") || !node->Child(1)->IsCallable("Void")) {
            return node;
        }

        if (!sublinks.contains(node.Get())) {
            return node;
        }

        auto children = node->ChildrenList();
        children[1] = inputTypes;
        if (!node->Child(3)->IsCallable("Void")) {
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

bool ValidateGroups(TInputs& inputs, const THashSet<TString>& possibleAliases,
    const TExprNode& data, TExtContext& ctx, TExprNode::TListType& newGroups) {
    newGroups.clear();
    bool hasColumnRef = false;
    for (const auto& group : data.Children()) {
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
                refs, &qualifiedRefs, ctx)) {
                return false;
            }

            TVector<const TItemExprType*> items;
            AddColumns(inputs, nullptr, refs, &qualifiedRefs, items, ctx.Expr);
            auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
            if (!effectiveType->Validate(group->Pos(), ctx.Expr)) {
                return false;
            }

            auto typeNode = ExpandType(group->Pos(), *effectiveType, ctx.Expr);

            auto argNode = ctx.Expr.NewArgument(group->Pos(), "row");
            auto arguments = ctx.Expr.NewArguments(group->Pos(), { argNode });
            TExprNode::TPtr newRoot;
            auto status = RebuildLambdaColumns(group->Tail().TailPtr(), argNode, newRoot, inputs, nullptr, ctx);
            if (status == IGraphTransformer::TStatus::Error) {
                return false;
            }

            auto newLambda = ctx.Expr.NewLambda(group->Pos(), std::move(arguments), std::move(newRoot));

            auto newChildren = group->ChildrenList();
            newChildren[0] = typeNode;
            newChildren[1] = newLambda;
            auto newGroup = ctx.Expr.NewCallable(group->Pos(), "PgGroup", std::move(newChildren));
            newGroups.push_back(newGroup);
        }
    }

    return true;
}

bool ValidateSort(TInputs& inputs, const THashSet<TString>& possibleAliases,
    const TExprNode& data, TExtContext& ctx, TExprNode::TListType& newSorts) {
    newSorts.clear();
    for (auto oneSort : data.Children()) {
        bool hasColumnRef;
        THashSet<TString> refs;
        THashMap<TString, THashSet<TString>> qualifiedRefs;
        if (!ScanColumns(oneSort->Child(1)->TailPtr(), inputs, possibleAliases, nullptr, hasColumnRef,
            refs, &qualifiedRefs, ctx)) {
            return false;
        }

        TVector<const TItemExprType*> items;
        AddColumns(inputs, nullptr, refs, &qualifiedRefs, items, ctx.Expr);
        auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
        if (!effectiveType->Validate(oneSort->Pos(), ctx.Expr)) {
            return false;
        }

        auto typeNode = ExpandType(oneSort->Pos(), *effectiveType, ctx.Expr);

        auto argNode = ctx.Expr.NewArgument(oneSort->Pos(), "row");
        auto arguments = ctx.Expr.NewArguments(oneSort->Pos(), { argNode });
        TExprNode::TPtr newRoot;
        auto status = RebuildLambdaColumns(oneSort->Child(1)->TailPtr(), argNode, newRoot, inputs, nullptr, ctx);
        if (status == IGraphTransformer::TStatus::Error) {
            return false;
        }

        auto newLambda = ctx.Expr.NewLambda(oneSort->Pos(), std::move(arguments), std::move(newRoot));

        auto newChildren = oneSort->ChildrenList();
        newChildren[0] = typeNode;
        newChildren[1] = newLambda;
        auto newSort = ctx.Expr.ChangeChildren(*oneSort, std::move(newChildren));
        newSorts.push_back(newSort);
    }

    return true;
}

IGraphTransformer::TStatus PgSetItemWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto& options = input->Head();
    if (!EnsureTuple(options, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool scanColumnsOnly = true;
    for (;;) {
        const TStructExprType* outputRowType = nullptr;
        TInputs inputs;
        TInputs joinInputs;
        THashSet<TString> possibleAliases;
        bool hasResult = false;
        bool hasValues = false;
        bool hasJoinOps = false;
        bool hasExtTypes = false;

        // pass 0 - from/values
        // pass 1 - join
        // pass 2 - ext_types/final_ext_types
        // pass 3 - where, group_by
        // pass 4 - window
        // pass 5 - result
        for (ui32 pass = 0; pass < 6; ++pass) {
            if (pass > 1 && !inputs.empty() && !hasJoinOps) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing join_ops"));
                return IGraphTransformer::TStatus::Error;
            }

            for (const auto& option : options.Children()) {
                if (!EnsureTupleMinSize(*option, 1, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!EnsureAtom(option->Head(), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                const auto optionName = option->Head().Content();
                if (optionName == "ext_types" || optionName == "final_ext_types") {
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

                    const auto& data = option->Tail();
                    if (!ValidateInputTypes(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (const auto& x : data.Children()) {
                        auto alias = x->Head().Content();
                        auto type = x->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                        joinInputs.push_back(TInput{ TString(alias), type, Nothing(), true, {} });
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

                        outputItems.push_back(ctx.Expr.MakeType<TItemExprType>(names->Child(i)->Content(), tupleType->GetItems()[i]));
                    }

                    outputRowType = ctx.Expr.MakeType<TStructExprType>(outputItems);
                    if (!outputRowType->Validate(names->Pos(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }
                else if (optionName == "result") {
                    hasResult = true;
                    if (pass != 5) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    const auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    TVector<const TItemExprType*> outputItems;
                    TExprNode::TListType newResult;
                    bool hasStar = false;
                    bool hasColumnRef = false;
                    for (const auto& column : data.Children()) {
                        if (!column->IsCallable("PgResultItem")) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(column->Pos()), "Expected PgResultItem"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        YQL_ENSURE(column->Tail().IsLambda());
                        THashSet<TString> refs;
                        THashMap<TString, THashSet<TString>> qualifiedRefs;
                        if (column->Child(1)->IsCallable("Void")) {
                            // no effective type yet, scan lambda body
                            TNodeSet sublinks;
                            ScanSublinks(column->Tail().TailPtr(), sublinks);

                            if (!ScanColumns(column->Tail().TailPtr(), joinInputs, possibleAliases, &hasStar, hasColumnRef,
                                refs, &qualifiedRefs, ctx)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            bool needRebuildSubLinks;
                            if (!ScanColumnsForSublinks(needRebuildSubLinks, sublinks, joinInputs, possibleAliases,
                                hasColumnRef, refs, &qualifiedRefs, ctx)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (!scanColumnsOnly) {
                                TVector<const TItemExprType*> items;
                                AddColumns(joinInputs, &hasStar, refs, &qualifiedRefs, items, ctx.Expr);
                                auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
                                if (!effectiveType->Validate(column->Pos(), ctx.Expr)) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                auto typeNode = ExpandType(column->Pos(), *effectiveType, ctx.Expr);

                                auto newColumnChildren = column->ChildrenList();
                                if (needRebuildSubLinks) {
                                    auto arguments = ctx.Expr.NewArguments(column->Pos(), { });

                                    TExprNode::TPtr newRoot;
                                    auto status = RebuildSubLinks(column->Tail().TailPtr(), newRoot, sublinks, joinInputs, typeNode, ctx);
                                    if (status == IGraphTransformer::TStatus::Error) {
                                        return IGraphTransformer::TStatus::Error;
                                    }

                                    auto newLambda = ctx.Expr.NewLambda(column->Pos(), std::move(arguments), std::move(newRoot));
                                    newColumnChildren[2] = newLambda;
                                } else {
                                    auto argNode = ctx.Expr.NewArgument(column->Pos(), "row");
                                    auto arguments = ctx.Expr.NewArguments(column->Pos(), { argNode });
                                    auto expandedColumns = column->HeadPtr();

                                    TExprNode::TPtr newRoot;
                                    auto status = RebuildLambdaColumns(column->Tail().TailPtr(), argNode, newRoot, joinInputs, &expandedColumns, ctx);
                                    if (status == IGraphTransformer::TStatus::Error) {
                                        return IGraphTransformer::TStatus::Error;
                                    }

                                    auto newLambda = ctx.Expr.NewLambda(column->Pos(), std::move(arguments), std::move(newRoot));
                                    newColumnChildren[0] = expandedColumns;
                                    newColumnChildren[1] = typeNode;
                                    newColumnChildren[2] = newLambda;
                                }

                                auto newColumn = ctx.Expr.NewCallable(column->Pos(), "PgResultItem", std::move(newColumnChildren));
                                newResult.push_back(newColumn);
                            }
                        }
                        else {
                            if (column->Head().IsAtom()) {
                                outputItems.push_back(ctx.Expr.MakeType<TItemExprType>(column->Head().Content(), column->Tail().GetTypeAnn()));
                            } else {
                                // star or qualified star
                                for (const auto& item : column->Tail().GetTypeAnn()->Cast<TStructExprType>()->GetItems()) {
                                    outputItems.push_back(hasExtTypes ? item : RemoveAlias(item, ctx.Expr));
                                }
                            }

                            // scan lambda for window references
                            auto windows = GetSetting(options, "window");
                            if (!ValidateWindowRefs(column->TailPtr(), windows ? &windows->Tail() : nullptr, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }
                        }
                    }

                    if (!scanColumnsOnly) {
                        if (!newResult.empty()) {
                            auto resultValue = ctx.Expr.NewList(options.Pos(), std::move(newResult));
                            auto newSettings = ReplaceSetting(options, {}, "result", resultValue, ctx.Expr);
                            output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                            return IGraphTransformer::TStatus::Repeat;
                        }

                        outputRowType = ctx.Expr.MakeType<TStructExprType>(outputItems);
                        if (!outputRowType->Validate(data.Pos(), ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
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

                    const auto& data = option->Tail();
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
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                                    "Empty alias for range function is not allowed"));
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (p->Child(2)->ChildrenSize() > 0 && p->Child(2)->ChildrenSize() != 1) {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                                    TStringBuilder() << "Expected exactly one column name for range function, but got: " << p->Child(2)->ChildrenSize()));
                                return IGraphTransformer::TStatus::Error;
                            }

                            auto memberName = (p->Child(2)->ChildrenSize() == 1) ? p->Child(2)->Head().Content() : alias;
                            TVector<const TItemExprType*> items;
                            auto itemType = p->Head().GetTypeAnn();
                            if (itemType->GetKind() == ETypeAnnotationKind::List) {
                                itemType = itemType->Cast<TListExprType>()->GetItemType();
                            }

                            items.push_back(ctx.Expr.MakeType<TItemExprType>(memberName, itemType));
                            inputStructType = ctx.Expr.MakeType<TStructExprType>(items);
                            columnOrder = TColumnOrder({ TString(memberName) });
                        }
                        else {
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
                                auto pos = inputStructType->FindItem((*columnOrder)[i]);
                                YQL_ENSURE(pos);
                                auto type = inputStructType->GetItems()[*pos]->GetItemType();
                                newOrder.push_back(TString(p->Child(2)->Child(i)->Content()));
                                newStructItems.push_back(ctx.Expr.MakeType<TItemExprType>(p->Child(2)->Child(i)->Content(), type));
                            }

                            auto newStructType = ctx.Expr.MakeType<TStructExprType>(newStructItems);
                            if (!newStructType->Validate(p->Child(2)->Pos(), ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            inputs.push_back(TInput{ alias, newStructType, newOrder, false, {} });
                        }
                        else {
                            inputs.push_back(TInput{ alias, inputStructType, columnOrder, false, {} });
                        }
                    }
                }
                else if (optionName == "where" || optionName == "having") {
                    if (pass != 3) {
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

                        bool hasColumnRef;
                        THashSet<TString> refs;
                        THashMap<TString, THashSet<TString>> qualifiedRefs;
                        if (!ScanColumns(data.Child(1)->TailPtr(), joinInputs, possibleAliases, nullptr, hasColumnRef,
                            refs, &qualifiedRefs, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        bool needRebuildSubLinks;
                        if (!ScanColumnsForSublinks(needRebuildSubLinks, sublinks, joinInputs, possibleAliases, hasColumnRef,
                            refs, &qualifiedRefs, ctx)) {
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

                            if (needRebuildSubLinks) {
                                auto arguments = ctx.Expr.NewArguments(data.Pos(), {});
                                TExprNode::TPtr newRoot;
                                auto status = RebuildSubLinks(data.Child(1)->TailPtr(), newRoot, sublinks, joinInputs, typeNode, ctx);
                                if (status == IGraphTransformer::TStatus::Error) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                auto newLambda = ctx.Expr.NewLambda(data.Pos(), std::move(arguments), std::move(newRoot));

                                auto newChildren = data.ChildrenList();
                                newChildren[1] = newLambda;
                                auto newWhere = ctx.Expr.NewCallable(data.Pos(), "PgWhere", std::move(newChildren));
                                auto newSettings = ReplaceSetting(options, {}, TString(optionName), newWhere, ctx.Expr);
                                output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                                return IGraphTransformer::TStatus::Repeat;
                            }

                            auto argNode = ctx.Expr.NewArgument(data.Pos(), "row");
                            auto arguments = ctx.Expr.NewArguments(data.Pos(), { argNode });
                            TExprNode::TPtr newRoot;
                            auto status = RebuildLambdaColumns(data.Child(1)->TailPtr(), argNode, newRoot, joinInputs, nullptr, ctx);
                            if (status == IGraphTransformer::TStatus::Error) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            auto newLambda = ctx.Expr.NewLambda(data.Pos(), std::move(arguments), std::move(newRoot));

                            auto newChildren = data.ChildrenList();
                            newChildren[0] = typeNode;
                            newChildren[1] = newLambda;
                            auto newWhere = ctx.Expr.NewCallable(data.Pos(), "PgWhere", std::move(newChildren));
                            auto newSettings = ReplaceSetting(options, {}, TString(optionName), newWhere, ctx.Expr);
                            output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                            return IGraphTransformer::TStatus::Repeat;
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

                    const auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    ui32 totalTupleSizes = 0;
                    for (auto child : data.Children()) {
                        if (!EnsureTuple(*child, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        totalTupleSizes += child->ChildrenSize() + 1;
                    }

                    if (totalTupleSizes != inputs.size()) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                            TStringBuilder() << "Unexpected number of joins, got: " << totalTupleSizes
                            << ", expected:" << inputs.size()));
                        return IGraphTransformer::TStatus::Error;
                    }

                    bool needRewrite = false;
                    ui32 inputIndex = 0;
                    for (ui32 joinGroupNo = 0; joinGroupNo < data.ChildrenSize(); ++joinGroupNo) {
                        joinInputs.push_back(inputs[inputIndex]);
                        ++inputIndex;
                        for (ui32 i = 0; i < data.Child(joinGroupNo)->ChildrenSize(); ++i) {
                            auto child = data.Child(joinGroupNo)->Child(i);
                            if (!EnsureTupleMinSize(*child, 1, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (!EnsureAtom(child->Head(), ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            auto joinType = child->Head().Content();
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

                                joinInputs.push_back(inputs[inputIndex]);
                                ++inputIndex;
                            }
                            else {
                                if (!EnsureTupleSize(*child, 2, ctx.Expr)) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                bool leftSideIsOptional = (joinType == "right" || joinType == "full");
                                bool rightSideIsOptional = (joinType == "left" || joinType == "full");
                                if (leftSideIsOptional) {
                                    for (ui32 j = 0; j < inputIndex; ++j) {
                                        MakeOptionalColumns(joinInputs[j].Type, ctx.Expr);
                                    }
                                }

                                joinInputs.push_back(inputs[inputIndex]);
                                ++inputIndex;
                                if (rightSideIsOptional) {
                                    MakeOptionalColumns(joinInputs.back().Type, ctx.Expr);
                                }

                                const auto& quals = child->Tail();
                                if (!quals.IsCallable("PgWhere")) {
                                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(quals.Pos()), "Expected PgWhere"));
                                    return IGraphTransformer::TStatus::Error;
                                }

                                needRewrite = needRewrite || quals.Child(0)->IsCallable("Void");
                            }
                        }
                    }

                    if (needRewrite) {
                        TExprNode::TListType newJoinGroups;
                        inputIndex = 0;
                        for (ui32 joinGroupNo = 0; joinGroupNo < data.ChildrenSize(); ++joinGroupNo) {
                            TExprNode::TListType newGroupItems;
                            TInputs groupInputs;
                            THashSet<TString> groupPossibleAliases;
                            if (data.Child(joinGroupNo)->ChildrenSize() > 0) {
                                groupInputs.push_back(inputs[inputIndex]);
                                auto alias = inputs[inputIndex].Alias;
                                if (!alias.empty()) {
                                    groupPossibleAliases.insert(alias);
                                }
                            }

                            ++inputIndex;
                            for (ui32 i = 0; i < data.Child(joinGroupNo)->ChildrenSize(); ++i, ++inputIndex) {
                                groupInputs.push_back(inputs[inputIndex]);
                                auto alias = inputs[inputIndex].Alias;
                                if (!alias.empty()) {
                                    groupPossibleAliases.insert(alias);
                                }

                                auto child = data.Child(joinGroupNo)->Child(i);
                                auto joinType = child->Head().Content();
                                if (joinType == "cross") {
                                    newGroupItems.push_back(data.Child(joinGroupNo)->ChildPtr(i));
                                }
                                else {
                                    const auto& quals = child->Tail();
                                    bool hasColumnRef;
                                    THashSet<TString> refs;
                                    THashMap<TString, THashSet<TString>> qualifiedRefs;
                                    if (!ScanColumns(quals.Child(1)->TailPtr(), groupInputs, groupPossibleAliases, nullptr, hasColumnRef,
                                        refs, &qualifiedRefs, ctx)) {
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
                                    }
                                }

                                // after left,right,full join type of inputs in current group may be changed for next predicates
                                bool leftSideIsOptional = (joinType == "right" || joinType == "full");
                                bool rightSideIsOptional = (joinType == "left" || joinType == "full");
                                if (leftSideIsOptional) {
                                    for (ui32 j = 0; j < inputIndex; ++j) {
                                        MakeOptionalColumns(groupInputs[j].Type, ctx.Expr);
                                    }
                                }

                                if (rightSideIsOptional) {
                                    MakeOptionalColumns(groupInputs[inputIndex].Type, ctx.Expr);
                                }
                            }

                            auto newGroup = ctx.Expr.NewList(option->Pos(), std::move(newGroupItems));
                            newJoinGroups.push_back(newGroup);
                        }

                        if (!scanColumnsOnly) {
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

                    const auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    TExprNode::TListType newGroups;
                    if (!ValidateGroups(joinInputs, possibleAliases, data, ctx, newGroups)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (!scanColumnsOnly && !newGroups.empty()) {
                        auto resultValue = ctx.Expr.NewList(options.Pos(), std::move(newGroups));
                        auto newSettings = ReplaceSetting(options, {}, "group_by", resultValue, ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }
                }
                else if (optionName == "window") {
                    if (pass != 4) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    const auto& data = option->Tail();
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
                        bool needRebuildSort = false;
                        bool needRebuildPartition = false;
                        for (const auto& p : partitions->Children()) {
                            if (p->Child(0)->IsCallable("Void")) {
                                needRebuildPartition = true;
                                break;
                            }
                        }

                        for (const auto& s : sort->Children()) {
                            if (s->Child(0)->IsCallable("Void")) {
                                needRebuildSort = true;
                                break;
                            }
                        }

                        if (!needRebuildSort && !needRebuildPartition) {
                            newWindow.push_back(x);
                            continue;
                        }

                        hasChanges = true;
                        auto newChildren = x->ChildrenList();
                        if (needRebuildPartition) {
                            TExprNode::TListType newGroups;
                            if (!ValidateGroups(joinInputs, possibleAliases, *partitions, ctx, newGroups)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            newChildren[2] = ctx.Expr.NewList(x->Pos(), std::move(newGroups));
                        }

                        if (needRebuildSort) {
                            TExprNode::TListType newSorts;
                            if (!ValidateSort(joinInputs, possibleAliases, *sort, ctx, newSorts)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            newChildren[3] = ctx.Expr.NewList(x->Pos(), std::move(newSorts));
                        }

                        newWindow.push_back(ctx.Expr.ChangeChildren(*x, std::move(newChildren)));
                    }

                    if (!scanColumnsOnly && hasChanges) {
                        auto windowValue = ctx.Expr.NewList(options.Pos(), std::move(newWindow));
                        auto newSettings = ReplaceSetting(options, {}, "window", windowValue, ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }
                }
                else {
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

        auto extTypes = GetSetting(options, "ext_types");
        if (extTypes && scanColumnsOnly) {
            const auto& data = extTypes->Tail();
            bool needRebuild = false;
            for (ui32 i = joinInputs.size() - data.ChildrenSize(), j = 0; i < joinInputs.size(); ++i, ++j) {
                const auto& x = joinInputs[i];
                YQL_ENSURE(x.External);
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
                    YQL_ENSURE(x.External);

                    const auto child = data.Child(j);
                    TVector<const TItemExprType*> items;
                    const auto type = data.Child(j)->Tail().GetTypeAnn()->Cast<TTypeExprType>()->
                        GetType()->Cast<TStructExprType>();
                    for (const auto& col : x.UsedExternalColumns) {
                        auto pos = type->FindItem(col);
                        YQL_ENSURE(pos);
                        items.push_back(type->GetItems()[*pos]);
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
            input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(outputRowType));
            return IGraphTransformer::TStatus::Ok;
        }
    }
}

IGraphTransformer::TStatus PgSelectWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto& options = input->Head();
    if (!EnsureTuple(options, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TStructExprType* outputRowType = nullptr;
    TExprNode* setItems = nullptr;
    TExprNode* setOps = nullptr;
    bool hasSort = false;

    for (ui32 pass = 0; pass < 2; ++pass) {
        for (const auto& option : options.Children()) {
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

                        if (child->Content() != "push" && child->Content() != "union_all") {
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
                } else {
                    outputRowType = option->Tail().Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->
                        Cast<TStructExprType>();
                }
            } else if (optionName == "limit" || optionName == "offset") {
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

                const auto& data = option->Tail();
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

    TColumnOrder resultColumnOrder;
    const TStructExprType* resultStructType = nullptr;
    auto status = InferPositionalUnionType(input->Pos(), setItems->ChildrenList(), resultColumnOrder, resultStructType, ctx);
    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (hasSort) {
        auto option = GetSetting(options, "sort");
        YQL_ENSURE(option);
        const auto& data = option->Tail();
        TInputs projectionInputs;
        projectionInputs.push_back(TInput{ TString(), resultStructType, resultColumnOrder, false, {} });
        TExprNode::TListType newSortTupleItems;

        if (data.ChildrenSize() > 0 && data.Child(0)->Child(0)->IsCallable("Void")) {
            // no effective types yet, scan lambda bodies
            if (!ValidateSort(projectionInputs, {}, data, ctx, newSortTupleItems)) {
                return IGraphTransformer::TStatus::Error;
            }

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
    const bool isNot = input->Content() == "PgNot";
    if (!EnsureArgsCount(*input, isNot ? 1 : 2, ctx.Expr)) {
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

    auto elemType = argTypes.front();
    for (ui32 i = 1; i < argTypes.size(); ++i) {
        if (elemType == 0) {
            elemType = argTypes[i];
        } else if (argTypes[i] != 0 && argTypes[i] != elemType) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Mismatch of array type elements: " <<
                    NPg::LookupType(elemType).Name << " and " << NPg::LookupType(argTypes[i]).Name));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (!elemType) {
        elemType = NPg::LookupType("text").TypeId;
    }

    const auto& typeInfo = NPg::LookupType(elemType);
    auto result = ctx.Expr.MakeType<TPgExprType>(typeInfo.ArrayTypeId);
    input->SetTypeAnn(result);
    return IGraphTransformer::TStatus::Ok;
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
    if (!typeDesc.TypeModInFuncId) {
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

    if (pgType->GetName() == "interval") {
        if (mods.size() != 1) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Exactly one modidifer is expected for pginterval"));
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
            .Atom(0, NPg::LookupProc(typeDesc.TypeModInFuncId, { 0 }).Name)
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

    for (ui32 i = 0; i < argTypes.size(); ++i) {
        if (!argTypes[i]) {
            continue;
        }

        if (argTypes[i] != NPg::LookupType("text").TypeId) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected pg text, but got " << NPg::LookupType(argTypes[i]).Name));
            return IGraphTransformer::TStatus::Error;
        }
    }

    auto result = ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("bool").TypeId);
    input->SetTypeAnn(result);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgInWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto inputType = input->Child(0)->GetTypeAnn();
    ui32 inputTypePg;
    bool convertToPg;
    if (!ExtractPgType(inputType, inputTypePg, convertToPg, input->Child(0)->Pos(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (convertToPg) {
        input->ChildRef(0) = ctx.Expr.NewCallable(input->Child(0)->Pos(), "ToPg", { input->ChildPtr(0) });
        return IGraphTransformer::TStatus::Repeat;
    }

    auto listType = input->Child(1)->GetTypeAnn();
    if (listType && listType->GetKind() == ETypeAnnotationKind::EmptyList) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "IN expects at least one element"));
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureListType(*input->Child(1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto listItemType = listType->Cast<TListExprType>()->GetItemType();
    ui32 itemTypePg;
    if (!ExtractPgType(listItemType, itemTypePg, convertToPg, input->Child(1)->Pos(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (convertToPg) {
        output = ctx.Expr.Builder(input->Pos())
            .Callable("PgIn")
                .Add(0, input->ChildPtr(0))
                .Callable(1, "Map")
                    .Add(0, input->ChildPtr(1))
                    .Lambda(1)
                        .Param("x")
                        .Callable("ToPg")
                            .Arg(0, "x")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    if (itemTypePg && inputTypePg && itemTypePg != inputTypePg) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Mismatch of types in IN expressions: " <<
            NPg::LookupType(inputTypePg).Name << " is not equal to " << NPg::LookupType(itemTypePg).Name));
        return IGraphTransformer::TStatus::Error;
    }

    if (!listItemType->IsEquatable()) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Cannot compare items of type: " << NPg::LookupType(itemTypePg).Name));
    }

    if (!inputType->IsEquatable()) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Cannot compare items of type: " << NPg::LookupType(inputTypePg).Name));
    }

    auto result = ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("bool").TypeId);
    input->SetTypeAnn(result);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgBetweenWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
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
        } else if (argTypes[i] != 0 && argTypes[i] != elemType) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Mismatch of type of between elements: " <<
                NPg::LookupType(elemType).Name << " and " << NPg::LookupType(argTypes[i]).Name));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (elemType && !elemTypeAnn->IsComparable()) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Cannot compare items of type: " << NPg::LookupType(elemType).Name));
        return IGraphTransformer::TStatus::Error;
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
    if (linkType != "exists" && linkType != "any" && linkType != "all" && linkType != "expr") {
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
        const auto status = ConvertToLambda(lambda, ctx.Expr, hasType ? 2 : 1);
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
        if (input->Child(2)->IsCallable("Void")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing test row type"));
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Child(3)->IsCallable("Void")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing test row expression"));
            return IGraphTransformer::TStatus::Error;
        }

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
    } else {
        if (!input->Child(3)->IsCallable("Void")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Test row expression is not allowed"));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (linkType == "expr") {
        input->SetTypeAnn(valueType);
    } else {
        auto result = ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("bool").TypeId);
        input->SetTypeAnn(result);
    }

    return IGraphTransformer::TStatus::Ok;
}

} // namespace NTypeAnnImpl
}
