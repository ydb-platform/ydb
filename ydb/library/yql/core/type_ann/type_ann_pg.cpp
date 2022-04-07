#include "type_ann_pg.h"
#include "type_ann_list.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_pg_utils.h>

#include <ydb/library/yql/parser/pg_catalog/catalog.h>

namespace NYql {
namespace NTypeAnnImpl {

IGraphTransformer::TStatus PgStarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
    return IGraphTransformer::TStatus::Ok;
}

struct TPgFuncDesc {
    ui32 MinArgs;
    ui32 MaxArgs;
    EDataSlot ReturnType;
    TVector<EDataSlot> DataTypes;
};

class TPgFuncMap {
public:
    static const TPgFuncMap& Instance() {
        return *Singleton<TPgFuncMap>();
    }

    THashMap<TString, TPgFuncDesc> Funcs;

    TPgFuncMap() {
        Funcs["substring"] = { 3, 3, EDataSlot::Utf8, { EDataSlot::Utf8, EDataSlot::Int32, EDataSlot::Int32 } };
    }
};

IGraphTransformer::TStatus PgCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    bool isResolved = input->Content().StartsWith("PgResolvedCall");
    if (!EnsureMinArgsCount(*input, isResolved ? 2 : 1, ctx.Expr)) {
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

    if (ctx.Types.PgTypes || isResolved) {
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
            auto procId = FromString<ui32>(input->Child(1)->Content());
            const auto& proc = NPg::LookupProc(procId, argTypes);
            if (proc.Name != name) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Mismatch of resolved function name, expected: " << name << ", but got:" << proc.Name));
                return IGraphTransformer::TStatus::Error;
            }

            if (proc.Kind == NPg::EProcKind::Window) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Window function " << name << " cannot be called directly with window specification"));
                return IGraphTransformer::TStatus::Error;
            }

            if (proc.Kind == NPg::EProcKind::Aggregate) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Aggregate function " << name << " cannot be called directly "));
                return IGraphTransformer::TStatus::Error;
            }

            auto result = ctx.Expr.MakeType<TPgExprType>(proc.ResultType);
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
    } else {
        const TTypeAnnotationNode* result = nullptr;
        TVector<const TTypeAnnotationNode*> argTypes;
        bool isNull = false;
        bool isOptional = false;
        for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
            auto type = input->Child(i)->GetTypeAnn();
            if (type->GetKind() == ETypeAnnotationKind::Null) {
                argTypes.push_back(type);
                isNull = true;
                result = type;
                continue;
            }

            if (type->GetKind() == ETypeAnnotationKind::Optional) {
                type = RemoveOptionalType(type);
                isOptional = true;
            }

            argTypes.push_back(type);
        }

        const auto& funcs = TPgFuncMap::Instance().Funcs;
        auto it = funcs.find(name);
        if (it != funcs.end()) {
            const auto& desc = it->second;
            if (argTypes.size() > desc.MaxArgs || argTypes.size() < desc.MinArgs) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Incorrect arguments count: " << argTypes.size() << " for function: " << name));
                return IGraphTransformer::TStatus::Error;
            }

            for (ui32 i = 0; i < argTypes.size(); ++i) {
                auto expectedType = desc.DataTypes[i];
                if (argTypes[i]->GetKind() != ETypeAnnotationKind::Null) {
                    if (argTypes[i]->GetKind() != ETypeAnnotationKind::Data) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                            TStringBuilder() << "Expected type " << expectedType << " for argument " << (i + 1) << ", but got: " << argTypes[i]->GetKind() << " for function: " << name));
                        return IGraphTransformer::TStatus::Error;
                    } else {
                        auto dataType = argTypes[i]->Cast<TDataExprType>()->GetSlot();
                        if (dataType != expectedType) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                                TStringBuilder() << "Expected type " << expectedType << " for argument " << (i + 1) << ", but got: " << dataType << " for function: " << name));
                            return IGraphTransformer::TStatus::Error;
                        }
                    }
                }
            }

            result = ctx.Expr.MakeType<TDataExprType>(desc.ReturnType);
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Unsupported function: " << name));
            return IGraphTransformer::TStatus::Error;
        }

        if (!isNull && isOptional && result->GetKind() != ETypeAnnotationKind::Optional) {
            result = ctx.Expr.MakeType<TOptionalExprType>(result);
        }

        input->SetTypeAnn(result);
        return IGraphTransformer::TStatus::Ok;
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
    if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
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

    if (name == "lead" || name == "lag") {
        if (input->ChildrenSize() != 3) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected one argument in " << name << " function"));
            return IGraphTransformer::TStatus::Error;
        }

        auto arg = input->Child(2)->GetTypeAnn();
        if (arg->GetKind() == ETypeAnnotationKind::Null ||
            arg->GetKind() == ETypeAnnotationKind::Optional ||
            arg->GetKind() == ETypeAnnotationKind::Pg) {
            input->SetTypeAnn(arg);
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(arg));
        }
    } else if (name == "row_number") {
        if (input->ChildrenSize() != 2) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                "Expected no arguments in row_number function"));
            return IGraphTransformer::TStatus::Error;
        }

        if (ctx.Types.PgTypes) {
            input->SetTypeAnn(ctx.Expr.MakeType<TPgExprType>(NPg::LookupType("int8").TypeId));
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64)));
        }
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
    if (!EnsureMinArgsCount(*input, overWindow ? 2 : 1, ctx.Expr)) {
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

    if (ctx.Types.PgTypes) {
        TVector<ui32> argTypes;
        bool needRetype = false;
        for (ui32 i = overWindow ? 2 : 1; i < input->ChildrenSize(); ++i) {
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
    } else {
        const TTypeAnnotationNode* result = nullptr;
        TVector<const TTypeAnnotationNode*> argTypes;
        bool isNull = false;
        bool isOptional = false;
        for (ui32 i = overWindow ? 2 : 1; i < input->ChildrenSize(); ++i) {
            auto type = input->Child(i)->GetTypeAnn();
            if (type->GetKind() == ETypeAnnotationKind::Null) {
                argTypes.push_back(type);
                isNull = true;
                result = type;
                continue;
            }

            if (type->GetKind() == ETypeAnnotationKind::Optional) {
                type = RemoveOptionalType(type);
                isOptional = true;
            }

            argTypes.push_back(type);
        }

        if (name == "count") {
            if (argTypes.size() > 1) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Too many arguments for function: " << name));
                return IGraphTransformer::TStatus::Error;
            }

            isNull = false;
            isOptional = true;
            result = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64);
        } else if (name == "min" || name == "max") {
            if (argTypes.size() != 1) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Expected one argument for function: " << name));
                return IGraphTransformer::TStatus::Error;
            }

            if (!isNull) {
                auto argType = argTypes[0];
                if (argType->GetKind() != ETypeAnnotationKind::Data) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Expected comparable type, but got: " << argType->GetKind() << " for function: " << name));
                    return IGraphTransformer::TStatus::Error;
                }

                auto slot = argType->Cast<TDataExprType>()->GetSlot();
                if (slot == EDataSlot::Utf8 || slot == EDataSlot::Int32 || slot == EDataSlot::Double || slot == EDataSlot::Bool) {
                    result = argType;
                } else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Expected comparable type, but got: " << slot << " for function: " << name));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            isOptional = true;
        } else if (name == "sum") {
            if (argTypes.size() != 1) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Expected one argument for function: " << name));
                return IGraphTransformer::TStatus::Error;
            }

            if (!isNull) {
                auto argType = argTypes[0];
                if (argType->GetKind() != ETypeAnnotationKind::Data) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Expected additive type, but got: " << argType->GetKind() << " for function: " << name));
                    return IGraphTransformer::TStatus::Error;
                }

                auto slot = argType->Cast<TDataExprType>()->GetSlot();
                if (slot == EDataSlot::Int32) {
                    result = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64);
                } else if (slot == EDataSlot::Double) {
                    result = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Double);
                } else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Expected additive type, but got: " << slot << " for function: " << name));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            isOptional = true;
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Unsupported function: " << name));
            return IGraphTransformer::TStatus::Error;
        }

        if (!isNull && isOptional && result->GetKind() != ETypeAnnotationKind::Optional) {
            result = ctx.Expr.MakeType<TOptionalExprType>(result);
        }

        input->SetTypeAnn(result);
        return IGraphTransformer::TStatus::Ok;
    }
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
    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureTypePg(input->Tail(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    // TODO: validate value
    if (!EnsureAtom(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(input->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType());
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
    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
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

    if (!EnsureTypePg(input->Tail(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    auto targetTypeId = input->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TPgExprType>()->GetId();

    if (inputTypeId != 0 && inputTypeId != targetTypeId) {
        if (NPg::LookupType(inputTypeId).Category != 'S' &&
            NPg::LookupType(targetTypeId).Category != 'S') {
            Y_UNUSED(NPg::LookupCast(inputTypeId, targetTypeId));
        }
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TPgExprType>(targetTypeId));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PgAggregationTraitsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    const bool onWindow = input->IsCallable("PgWindowTraits");
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
        lambda = ctx.Expr.DeepCopyLambda(*lambda);
        for (ui32 i = 1; i < lambda->ChildrenSize(); ++i) {
            auto type = lambda->Child(i)->GetTypeAnn();
            ui32 argType;
            bool convertToPg;
            if (!ExtractPgType(type, argType, convertToPg, lambda->Child(i)->Pos(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (convertToPg) {
                lambda->ChildRef(i) = ctx.Expr.NewCallable(lambda->Child(i)->Pos(), "ToPg", { lambda->ChildPtr(i) });
            }
        }

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
                .Arg(2, "state")
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
                    .Add(2, initValue)
                    .Apply(3, lambda)
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
                        .Callable(2, "Coalesce")
                            .Arg(0, "state")
                            .Add(1, initValue)
                        .Seal()
                        .Apply(3, lambda)
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
                            .Arg(2, "state")
                            .Apply(3, lambda)
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
                    .Arg(2, "state")
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
                    .Arg(2, "state")
                    .Callable(3, "PgInternal0")
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
                                .Arg(2, "state1")
                                .Arg(3, "state2")
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
                        .Arg(2, "state1")
                        .Arg(3, "state2")
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

using TInputs = TVector<std::tuple<TString, const TStructExprType*, TMaybe<TColumnOrder>>>;

bool ScanColumns(TExprNode::TPtr root, const TInputs& inputs, const THashSet<TString>& possibleAliases,
    bool* hasStar, bool& hasColumnRef, THashSet<TString>& refs, THashMap<TString, THashSet<TString>>* qualifiedRefs,
    TExtContext& ctx) {
    bool isError = false;
    VisitExpr(root, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("PgStar")) {
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
        }
        else if (node->IsCallable("PgQualifiedStar")) {
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

            for (const auto& x : inputs) {
                if (std::get<0>(x).empty() || alias != std::get<0>(x)) {
                    continue;
                }

                for (const auto& item : std::get<1>(x)->GetItems()) {
                    if (!item->GetName().StartsWith("_yql_")) {
                        (*qualifiedRefs)[alias].insert(TString(item->GetName()));
                    }
                }
            }
        }
        else if (node->IsCallable("PgColumnRef")) {
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

            ui32 matches = 0;
            for (const auto& x : inputs) {
                if (node->ChildrenSize() == 2) {
                    if (std::get<0>(x).empty() || node->Head().Content() != std::get<0>(x)) {
                        continue;
                    }
                }

                auto pos = std::get<1>(x)->FindItem(node->Tail().Content());
                if (pos) {
                    ++matches;
                    if (matches > 1) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()),
                            TStringBuilder() << "Column reference is ambiguous: " << node->Tail().Content()));
                        isError = true;
                        return false;
                    }
                }
            }

            refs.insert(TString(node->Tail().Content()));
        }

        return true;
    });

    return !isError;
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

void AddColumns(const TInputs& inputs, const bool* hasStar, const THashSet<TString>& refs,
    const THashMap<TString, THashSet<TString>>* qualifiedRefs,
    TVector<const TItemExprType*>& items) {
    for (const auto& x : inputs) {
        if (hasStar && *hasStar) {
            for (ui32 i = 0; i < std::get<1>(x)->GetSize(); ++i) {
                auto item = std::get<1>(x)->GetItems()[i];
                if (!item->GetName().StartsWith("_yql_")) {
                    items.push_back(item);
                }
            }

            continue;
        }

        for (const auto& ref : refs) {
            auto pos = std::get<1>(x)->FindItem(ref);
            if (pos) {
                items.push_back(std::get<1>(x)->GetItems()[*pos]);
            }
        }

        if (qualifiedRefs && qualifiedRefs->contains(std::get<0>(x))) {
            for (const auto& ref : qualifiedRefs->find(std::get<0>(x))->second) {
                auto pos = std::get<1>(x)->FindItem(ref);
                if (pos) {
                    items.push_back(std::get<1>(x)->GetItems()[*pos]);
                }
            }
        }
    }
}

IGraphTransformer::TStatus RebuildLambdaColumns(const TExprNode::TPtr& root, const TExprNode::TPtr& argNode,
    TExprNode::TPtr& newRoot, const TInputs& inputs, TExprNode::TPtr* expandedColumns, TExtContext& ctx) {
    return OptimizeExpr(root, newRoot, [&](const TExprNode::TPtr& node, TExprContext&) -> TExprNode::TPtr {
        if (node->IsCallable("PgStar")) {
            TExprNode::TListType orderAtoms;
            for (const auto& x : inputs) {
                auto order = std::get<2>(x);
                for (const auto& item : std::get<1>(x)->GetItems()) {
                    if (!item->GetName().StartsWith("_yql_")) {
                        if (!order) {
                            orderAtoms.push_back(ctx.Expr.NewAtom(node->Pos(), item->GetName()));
                        }
                    }
                }

                if (order) {
                    for (const auto& o : *order) {
                        if (!o.StartsWith("_yql_")) {
                            orderAtoms.push_back(ctx.Expr.NewAtom(node->Pos(), o));
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
            return ctx.Expr.Builder(node->Pos())
                .Callable("Member")
                .Add(0, argNode)
                .Atom(1, node->Tail().Content())
                .Seal()
                .Build();
        }

        if (node->IsCallable("PgQualifiedStar")) {
            TExprNode::TListType members;
            for (const auto& x : inputs) {
                if (std::get<0>(x).empty() || node->Head().Content() != std::get<0>(x)) {
                    continue;
                }

                auto order = std::get<2>(x);
                TExprNode::TListType orderAtoms;
                for (const auto& item : std::get<1>(x)->GetItems()) {
                    if (!item->GetName().StartsWith("_yql_")) {
                        if (!order) {
                            orderAtoms.push_back(ctx.Expr.NewAtom(node->Pos(), item->GetName()));
                        }

                        members.push_back(ctx.Expr.Builder(node->Pos())
                            .List()
                            .Atom(0, item->GetName())
                            .Callable(1, "Member")
                            .Add(0, argNode)
                            .Atom(1, item->GetName())
                            .Seal()
                            .Seal()
                            .Build());
                    }
                }

                if (order) {
                    for (const auto& o : *order) {
                        if (!o.StartsWith("_yql_")) {
                            orderAtoms.push_back(ctx.Expr.NewAtom(node->Pos(), o));
                        }
                    }
                }

                if (expandedColumns) {
                    *expandedColumns = ctx.Expr.NewList(node->Pos(), std::move(orderAtoms));
                }

                return ctx.Expr.NewCallable(node->Pos(), "AsStruct", std::move(members));
            }

            YQL_ENSURE(false, "missing input");
        }

        return node;
    }, ctx.Expr, TOptimizeExprSettings(nullptr));
}

void MakeOptionalColumns(const TStructExprType*& structType, TExprContext& ctx) {
    bool needRebuild = false;
    for (const auto& item : structType->GetItems()) {
        if (item->GetItemType()->GetKind() != ETypeAnnotationKind::Optional
            && item->GetItemType()->GetKind() != ETypeAnnotationKind::Null) {
            needRebuild = true;
            break;
        }
    }

    if (!needRebuild) {
        return;
    }

    auto newItems = structType->GetItems();
    for (auto& item : newItems) {
        if (item->GetItemType()->GetKind() != ETypeAnnotationKind::Optional
            && item->GetItemType()->GetKind() != ETypeAnnotationKind::Null) {
            item = ctx.MakeType<TItemExprType>(item->GetName(), ctx.MakeType<TOptionalExprType>(item->GetItemType()));
        }
    }

    structType = ctx.MakeType<TStructExprType>(newItems);
}

bool ValidateGroups(const TInputs& inputs, const THashSet<TString>& possibleAliases,
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
            AddColumns(inputs, nullptr, refs, &qualifiedRefs, items);
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

bool ValidateSort(const TInputs& inputs, const THashSet<TString>& possibleAliases,
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
        AddColumns(inputs, nullptr, refs, &qualifiedRefs, items);
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

    const TStructExprType* outputRowType = nullptr;
    TInputs inputs;
    TInputs joinInputs;
    THashSet<TString> possibleAliases;
    bool hasResult = false;
    bool hasValues = false;
    bool hasJoinOps = false;

    // pass 0 - from/values
    // pass 1 - join
    // pass 2 - where, group_by,
    // pass 3 - window
    // pass 4 - result
    for (ui32 pass = 0; pass < 5; ++pass) {
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
            if (optionName == "values") {
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
            } else if (optionName == "result") {
                hasResult = true;
                if (pass != 4) {
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
                        if (!ScanColumns(column->Tail().TailPtr(), joinInputs, possibleAliases, &hasStar, hasColumnRef,
                            refs, &qualifiedRefs, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        TVector<const TItemExprType*> items;
                        AddColumns(joinInputs, &hasStar, refs, &qualifiedRefs, items);
                        auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
                        if (!effectiveType->Validate(column->Pos(), ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto expandedColumns = column->HeadPtr();
                        auto typeNode = ExpandType(column->Pos(), *effectiveType, ctx.Expr);

                        auto argNode = ctx.Expr.NewArgument(column->Pos(), "row");
                        auto arguments = ctx.Expr.NewArguments(column->Pos(), { argNode });
                        TExprNode::TPtr newRoot;
                        auto status = RebuildLambdaColumns(column->Tail().TailPtr(), argNode, newRoot, joinInputs, &expandedColumns, ctx);
                        if (status == IGraphTransformer::TStatus::Error) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto newLambda = ctx.Expr.NewLambda(column->Pos(), std::move(arguments), std::move(newRoot));

                        auto newColumnChildren = column->ChildrenList();
                        newColumnChildren[0] = expandedColumns;
                        newColumnChildren[1] = typeNode;
                        newColumnChildren[2] = newLambda;
                        auto newColumn = ctx.Expr.NewCallable(column->Pos(), "PgResultItem", std::move(newColumnChildren));
                        newResult.push_back(newColumn);
                    } else {
                        if (column->Head().IsAtom()) {
                            outputItems.push_back(ctx.Expr.MakeType<TItemExprType>(column->Head().Content(), column->Tail().GetTypeAnn()));
                        } else {
                            // star or qualified star
                            for (const auto& item : column->Tail().GetTypeAnn()->Cast<TStructExprType>()->GetItems()) {
                                outputItems.push_back(item);
                            }
                        }

                        // scan lambda for window references
                        auto windows = GetSetting(options, "window");
                        if (!ValidateWindowRefs(column->TailPtr(), windows ? &windows->Tail() : nullptr, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }
                    }
                }

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
            } else if (optionName == "from") {
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

                    auto columnOrder = ctx.Types.LookupColumnOrder(p->Head());
                    if (!EnsureListType(p->Head(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto inputRowType = p->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                    if (!EnsureStructType(p->Head().Pos(), *inputRowType, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto inputStructType = inputRowType->Cast<TStructExprType>();
                    auto alias = TString(p->Child(1)->Content());
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

                        inputs.push_back(std::make_tuple(alias, newStructType, newOrder));
                    } else {
                        inputs.push_back(std::make_tuple(alias, inputStructType, columnOrder));
                    }
                }
            } else if (optionName == "where" || optionName == "having") {
                if (pass != 2) {
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
                    bool hasColumnRef;
                    THashSet<TString> refs;
                    if (!ScanColumns(data.Child(1)->TailPtr(), joinInputs, possibleAliases, nullptr, hasColumnRef,
                        refs, nullptr, ctx)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    TVector<const TItemExprType*> items;
                    AddColumns(joinInputs, nullptr, refs, nullptr, items);
                    auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
                    if (!effectiveType->Validate(data.Pos(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto typeNode = ExpandType(data.Pos(), *effectiveType, ctx.Expr);

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
                    auto newWhere= ctx.Expr.NewCallable(data.Pos(), "PgWhere", std::move(newChildren));
                    auto newSettings = ReplaceSetting(options, {}, TString(optionName), newWhere, ctx.Expr);
                    output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                    return IGraphTransformer::TStatus::Repeat;
                } else {
                    if (data.GetTypeAnn() && data.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Null) {
                        // nothing to do
                    } else if (data.GetTypeAnn() && data.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
                        auto name = data.GetTypeAnn()->Cast<TPgExprType>()->GetName();
                        if (name != "bool") {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(data.Pos()), TStringBuilder() <<
                                "Expected bool type, but got: " << name));
                            return IGraphTransformer::TStatus::Error;
                        }
                    } else if (!EnsureSpecificDataType(data, EDataSlot::Bool, ctx.Expr, true)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }
            } else if (optionName == "join_ops") {
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
                for (auto child: data.Children()) {
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
                        } else {
                            if (!EnsureTupleSize(*child, 2, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            bool leftSideIsOptional = (joinType == "right" || joinType == "full");
                            bool rightSideIsOptional = (joinType == "left" || joinType == "full");
                            if (leftSideIsOptional) {
                                for (ui32 j = 0; j < inputIndex; ++j) {
                                    MakeOptionalColumns(std::get<1>(joinInputs[j]), ctx.Expr);
                                }
                            }

                            joinInputs.push_back(inputs[inputIndex]);
                            ++inputIndex;
                            if (rightSideIsOptional) {
                                MakeOptionalColumns(std::get<1>(joinInputs.back()), ctx.Expr);
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
                            auto alias = std::get<0>(inputs[inputIndex]);
                            if (!alias.empty()) {
                                groupPossibleAliases.insert(alias);
                            }
                        }

                        ++inputIndex;
                        for (ui32 i = 0; i < data.Child(joinGroupNo)->ChildrenSize(); ++i, ++inputIndex) {
                            groupInputs.push_back(inputs[inputIndex]);
                            auto alias = std::get<0>(inputs[inputIndex]);
                            if (!alias.empty()) {
                                groupPossibleAliases.insert(alias);
                            }

                            auto child = data.Child(joinGroupNo)->Child(i);
                            auto joinType = child->Head().Content();
                            if (joinType == "cross") {
                                newGroupItems.push_back(data.Child(joinGroupNo)->ChildPtr(i));
                            } else {
                                const auto& quals = child->Tail();
                                bool hasColumnRef;
                                THashSet<TString> refs;
                                if (!ScanColumns(quals.Child(1)->TailPtr(), groupInputs, groupPossibleAliases, nullptr, hasColumnRef,
                                    refs, nullptr, ctx)) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                TVector<const TItemExprType*> items;
                                AddColumns(groupInputs, nullptr, refs, nullptr, items);
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

                                auto predicate = ctx.Expr.Builder(quals.Pos())
                                    .Callable("Coalesce")
                                        .Add(0, newRoot)
                                        .Callable(1, "Bool")
                                            .Atom(0, "0")
                                        .Seal()
                                    .Seal()
                                    .Build();

                                auto newLambda = ctx.Expr.NewLambda(quals.Pos(), std::move(arguments), std::move(predicate));

                                auto newChildren = quals.ChildrenList();
                                newChildren[0] = typeNode;
                                newChildren[1] = newLambda;
                                auto newWhere= ctx.Expr.NewCallable(quals.Pos(), "PgWhere", std::move(newChildren));
                                newGroupItems.push_back(ctx.Expr.ChangeChild(*child, 1, std::move(newWhere)));
                            }

                            // after left,right,full join type of inputs in current group may be changed for next predicates
                            bool leftSideIsOptional = (joinType == "right" || joinType == "full");
                            bool rightSideIsOptional = (joinType == "left" || joinType == "full");
                            if (leftSideIsOptional) {
                                for (ui32 j = 0; j < inputIndex; ++j) {
                                    MakeOptionalColumns(std::get<1>(groupInputs[j]), ctx.Expr);
                                }
                            }

                            if (rightSideIsOptional) {
                                MakeOptionalColumns(std::get<1>(groupInputs[inputIndex]), ctx.Expr);
                            }
                        }

                        auto newGroup = ctx.Expr.NewList(option->Pos(), std::move(newGroupItems));
                        newJoinGroups.push_back(newGroup);
                    }

                    auto newJoinGroupsNode = ctx.Expr.NewList(option->Pos(), std::move(newJoinGroups));
                    auto newSettings = ReplaceSetting(options, {}, TString(optionName), newJoinGroupsNode, ctx.Expr);
                    output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                    return IGraphTransformer::TStatus::Repeat;
                }
            } else if (optionName == "group_by") {
                if (pass != 2) {
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

                if (!newGroups.empty()) {
                    auto resultValue = ctx.Expr.NewList(options.Pos(), std::move(newGroups));
                    auto newSettings = ReplaceSetting(options, {}, "group_by", resultValue, ctx.Expr);
                    output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                    return IGraphTransformer::TStatus::Repeat;
                }
            } else if (optionName == "window") {
                if (pass != 3) {
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

                if (hasChanges) {
                    auto windowValue = ctx.Expr.NewList(options.Pos(), std::move(newWindow));
                    auto newSettings = ReplaceSetting(options, {}, "window", windowValue, ctx.Expr);
                    output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                    return IGraphTransformer::TStatus::Repeat;
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

    input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(outputRowType));
    return IGraphTransformer::TStatus::Ok;
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
        projectionInputs.push_back(std::make_tuple(TString(), resultStructType, resultColumnOrder));
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

} // namespace NTypeAnnImpl
}
