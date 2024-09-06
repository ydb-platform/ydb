#include "sql_call_expr.h"
#include "sql_expression.h"

#include <ydb/library/yql/parser/proto_ast/gen/v1/SQLv1Lexer.h>

#include <ydb/library/yql/minikql/mkql_program_builder.h>

namespace NSQLTranslationV1 {

TNodePtr BuildSqlCall(TContext& ctx, TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args,
    TNodePtr positionalArgs, TNodePtr namedArgs, TNodePtr customUserType, const TDeferredAtom& typeConfig, TNodePtr runConfig);

using namespace NSQLv1Generated;

static bool ValidateForCounters(const TString& input) {
    for (auto c : input) {
        if (!(IsAlnum(c) || c == '_')) {
            return false;
        }
    }
    return true;
}

TNodePtr TSqlCallExpr::BuildUdf(bool forReduce) {
        auto result = Node ? Node : BuildCallable(Pos, Module, Func, Args, forReduce);
        if (to_lower(Module) == "tensorflow" && Func == "RunBatch") {
            if (Args.size() > 2) {
                Args.erase(Args.begin() + 2);
            } else {
                Ctx.Error(Pos) << "Excepted >= 3 arguments, but got: " << Args.size();
                return nullptr;
            }
        }
        return result;
}

TNodePtr TSqlCallExpr::BuildCall() {
        TVector<TNodePtr> args;
        bool warnOnYqlNameSpace = true;

        TUdfNode* udf_node = Node ? Node->GetUdfNode() : nullptr;
        if (udf_node) {
            if (!udf_node->DoInit(Ctx, nullptr)) {
                return nullptr;
            }
            TNodePtr positional_args = BuildTuple(Pos, PositionalArgs);
            TNodePtr positional = positional_args->Y("TypeOf", positional_args);
            TNodePtr named_args = BuildStructure(Pos, NamedArgs);
            TNodePtr named = named_args->Y("TypeOf", named_args);

            TNodePtr custom_user_type = new TCallNodeImpl(Pos, "TupleType", {positional, named, udf_node->GetExternalTypes()});

            return BuildSqlCall(Ctx, Pos, udf_node->GetModule(), udf_node->GetFunction(),
                                args, positional_args, named_args, custom_user_type,
                                udf_node->GetTypeConfig(), udf_node->GetRunConfig());
        }

        if (Node && !Node->FuncName()) {
            Module = "YQL";
            Func = NamedArgs.empty() ? "Apply" : "NamedApply";
            warnOnYqlNameSpace = false;
            args.push_back(Node);
        }

        if (Node && Node->FuncName()) {
            Module = Node->ModuleName() ? *Node->ModuleName() : "YQL";
            Func = *Node->FuncName();
        }
        bool mustUseNamed = !NamedArgs.empty();
        if (mustUseNamed) {
            if (Node && !Node->FuncName()) {
                mustUseNamed = false;
            }
            args.emplace_back(BuildTuple(Pos, PositionalArgs));
            args.emplace_back(BuildStructure(Pos, NamedArgs));
        } else if (IsExternalCall) {
            Func = "SqlExternalFunction";
            if (Args.size() < 2 || Args.size() > 3) {
                Ctx.Error(Pos) << "EXTERNAL FUNCTION requires from 2 to 3 arguments, but got: " << Args.size();
                return nullptr;
            }

            if (Args.size() == 3) {
                args.insert(args.end(), Args.begin(), Args.end() - 1);
                Args.erase(Args.begin(), Args.end() - 1);
            } else {
                args.insert(args.end(), Args.begin(), Args.end());
                Args.erase(Args.begin(), Args.end());
            }
            auto configNode = new TExternalFunctionConfig(Pos, CallConfig);
            auto configList = new TAstListNodeImpl(Pos, { new TAstAtomNodeImpl(Pos, "quote", 0), configNode });
            args.push_back(configList);
        } else {
            args.insert(args.end(), Args.begin(), Args.end());
        }

        auto result = BuildBuiltinFunc(Ctx, Pos, Func, args, Module, AggMode, &mustUseNamed, warnOnYqlNameSpace);
        if (mustUseNamed) {
            Error() << "Named args are used for call, but unsupported by function: " << Func;
            return nullptr;
        }

        if (WindowName) {
            result = BuildCalcOverWindow(Pos, WindowName, result);
        }

        return result;
}

bool TSqlCallExpr::Init(const TRule_value_constructor& node) {
    switch (node.Alt_case()) {
        case TRule_value_constructor::kAltValueConstructor1: {
            auto& ctor = node.GetAlt_value_constructor1();
            Func = "Variant";
            TSqlExpression expr(Ctx, Mode);
            if (!Expr(expr, Args, ctor.GetRule_expr3())) {
                return false;
            }
            if (!Expr(expr, Args, ctor.GetRule_expr5())) {
                return false;
            }
            if (!Expr(expr, Args, ctor.GetRule_expr7())) {
                return false;
            }
            break;
        }
        case TRule_value_constructor::kAltValueConstructor2: {
            auto& ctor = node.GetAlt_value_constructor2();
            Func = "Enum";
            TSqlExpression expr(Ctx, Mode);
            if (!Expr(expr, Args, ctor.GetRule_expr3())) {
                return false;
            }
            if (!Expr(expr, Args, ctor.GetRule_expr5())) {
                return false;
            }
            break;
        }
        case TRule_value_constructor::kAltValueConstructor3: {
            auto& ctor = node.GetAlt_value_constructor3();
            Func = "Callable";
            TSqlExpression expr(Ctx, Mode);
            if (!Expr(expr, Args, ctor.GetRule_expr3())) {
                return false;
            }
            if (!Expr(expr, Args, ctor.GetRule_expr5())) {
                return false;
            }
            break;
        }
        case TRule_value_constructor::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
    PositionalArgs = Args;
    return true;
}

bool TSqlCallExpr::ExtractCallParam(const TRule_external_call_param& node) {
    TString paramName = Id(node.GetRule_an_id1(), *this);
    paramName = to_lower(paramName);

    if (CallConfig.contains(paramName)) {
        Ctx.Error() << "WITH " << to_upper(paramName).Quote()
            << " clause should be specified only once";
        return false;
    }

    const bool optimizeForParam = paramName == "optimize_for";
    const auto columnRefState = optimizeForParam ? EColumnRefState::AsStringLiteral : EColumnRefState::Deny;

    TColumnRefScope scope(Ctx, columnRefState);
    if (optimizeForParam) {
        scope.SetNoColumnErrContext("in external call params");
    }

    TSqlExpression expression(Ctx, Mode);
    auto value = expression.Build(node.GetRule_expr3());
    if (value && optimizeForParam) {
        TDeferredAtom atom;
        MakeTableFromExpression(Ctx.Pos(), Ctx, value, atom);
        value = new TCallNodeImpl(Ctx.Pos(), "String", { atom.Build() });
    }

    if (!value) {
        return false;
    }

    CallConfig[paramName] = value;
    return true;
}

bool TSqlCallExpr::ConfigureExternalCall(const TRule_external_call_settings& node) {
    bool success = ExtractCallParam(node.GetRule_external_call_param1());
    for (auto& block: node.GetBlock2()) {
        success = ExtractCallParam(block.GetRule_external_call_param2()) && success;
    }

    return success;
}

bool TSqlCallExpr::Init(const TRule_using_call_expr& node) {
    // using_call_expr: ((an_id_or_type NAMESPACE an_id_or_type) | an_id_expr | bind_parameter | (EXTERNAL FUNCTION)) invoke_expr;
    const auto& block = node.GetBlock1();
    switch (block.Alt_case()) {
        case TRule_using_call_expr::TBlock1::kAlt1: {
            auto& subblock = block.GetAlt1();
            Module = Id(subblock.GetRule_an_id_or_type1(), *this);
            Func = Id(subblock.GetRule_an_id_or_type3(), *this);
            break;
        }
        case TRule_using_call_expr::TBlock1::kAlt2: {
            Func = Id(block.GetAlt2().GetRule_an_id_expr1(), *this);
            break;
        }
        case TRule_using_call_expr::TBlock1::kAlt3: {
            TString bindName;
            if (!NamedNodeImpl(block.GetAlt3().GetRule_bind_parameter1(), bindName, *this)) {
                return false;
            }
            Node = GetNamedNode(bindName);
            if (!Node) {
                return false;
            }
            break;
        }
        case TRule_using_call_expr::TBlock1::kAlt4: {
            IsExternalCall = true;
            break;
        }
        case TRule_using_call_expr::TBlock1::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
    YQL_ENSURE(!DistinctAllowed);
    UsingCallExpr = true;
    TColumnRefScope scope(Ctx, EColumnRefState::Allow);
    return Init(node.GetRule_invoke_expr2());
}

void TSqlCallExpr::InitName(const TString& name) {
    Module = "";
    Func = name;
}

void TSqlCallExpr::InitExpr(const TNodePtr& expr) {
    Node = expr;
}

bool TSqlCallExpr::FillArg(const TString& module, const TString& func, size_t& idx, const TRule_named_expr& node) {
    const bool isNamed = node.HasBlock2();

    TMaybe<EColumnRefState> status;
    // TODO: support named args
    if (!isNamed) {
        status = GetFunctionArgColumnStatus(Ctx, module, func, idx);
    }

    TNodePtr expr;
    if (status) {
        TColumnRefScope scope(Ctx, *status, /* isTopLevel = */ false);
        expr = NamedExpr(node);
    } else {
        expr = NamedExpr(node);
    }

    if (!expr) {
        return false;
    }

    Args.emplace_back(std::move(expr));
    if (!isNamed) {
        ++idx;
    }
    return true;
}

bool TSqlCallExpr::FillArgs(const TRule_named_expr_list& node) {
    TString module = Module;
    TString func = Func;
    if (Node && Node->FuncName()) {
        module = Node->ModuleName() ? *Node->ModuleName() : "YQL";
        func = *Node->FuncName();
    }

    size_t idx = 0;
    if (!FillArg(module, func, idx, node.GetRule_named_expr1())) {
        return false;
    }

    for (auto& b: node.GetBlock2()) {
        if (!FillArg(module, func, idx, b.GetRule_named_expr2())) {
            return false;
        }
    }

    return true;
}

bool TSqlCallExpr::Init(const TRule_invoke_expr& node) {
    // invoke_expr: LPAREN (opt_set_quantifier named_expr_list COMMA? | ASTERISK)? RPAREN invoke_expr_tail;
    // invoke_expr_tail:
    //     (null_treatment | filter_clause)? (OVER window_name_or_specification)?
    // ;
    Pos = Ctx.Pos();
    if (node.HasBlock2()) {
        switch (node.GetBlock2().Alt_case()) {
        case TRule_invoke_expr::TBlock2::kAlt1: {
            const auto& alt = node.GetBlock2().GetAlt1();
            TPosition distinctPos;
            if (IsDistinctOptSet(alt.GetRule_opt_set_quantifier1(), distinctPos)) {
                if (!DistinctAllowed) {
                    if (UsingCallExpr) {
                        Ctx.Error(distinctPos) << "DISTINCT can not be used in PROCESS/REDUCE";
                    } else {
                        Ctx.Error(distinctPos) << "DISTINCT can only be used in aggregation functions";
                    }
                    return false;
                }
                YQL_ENSURE(AggMode == EAggregateMode::Normal);
                AggMode = EAggregateMode::Distinct;
                Ctx.IncrementMonCounter("sql_features", "DistinctInCallExpr");
            }
            if (!FillArgs(alt.GetRule_named_expr_list2())) {
                return false;
            }
            for (const auto& arg : Args) {
                if (arg->GetLabel()) {
                    NamedArgs.push_back(arg);
                }
                else {
                    PositionalArgs.push_back(arg);
                    if (!NamedArgs.empty()) {
                        Ctx.Error(arg->GetPos()) << "Unnamed arguments can not follow after named one";
                        return false;
                    }
                }
            }
            break;
        }
        case TRule_invoke_expr::TBlock2::kAlt2:
            if (IsExternalCall) {
                Ctx.Error() << "You should set EXTERNAL FUNCTION type. Example: EXTERNAL FUNCTION('YANDEX-CLOUD', ...)";
            } else {
                Args.push_back(new TAsteriskNode(Pos));
            }
            break;
        case TRule_invoke_expr::TBlock2::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
        }
    }

    const auto& tail = node.GetRule_invoke_expr_tail4();

    if (tail.HasBlock1()) {
        if (IsExternalCall) {
            Ctx.Error() << "Additional clause after EXTERNAL FUNCTION(...) is not supported";
            return false;
        }

        switch (tail.GetBlock1().Alt_case()) {
        case TRule_invoke_expr_tail::TBlock1::kAlt1: {
            if (!tail.HasBlock2()) {
                Ctx.Error() << "RESPECT/IGNORE NULLS can only be used with window functions";
                return false;
            }
            const auto& alt = tail.GetBlock1().GetAlt1();
            if (alt.GetRule_null_treatment1().Alt_case() == TRule_null_treatment::kAltNullTreatment2) {
                SetIgnoreNulls();
            }
            break;
        }
        case TRule_invoke_expr_tail::TBlock1::kAlt2: {
            Ctx.Error() << "FILTER clause is not supported yet";
            return false;
        }
        case TRule_invoke_expr_tail::TBlock1::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
        }
    }

    if (tail.HasBlock2()) {
        if (AggMode == EAggregateMode::Distinct) {
            Ctx.Error() << "DISTINCT is not yet supported in window functions";
            return false;
        }
        SetOverWindow();
        auto winRule = tail.GetBlock2().GetRule_window_name_or_specification2();
        switch (winRule.Alt_case()) {
        case TRule_window_name_or_specification::kAltWindowNameOrSpecification1: {
            WindowName = Id(winRule.GetAlt_window_name_or_specification1().GetRule_window_name1().GetRule_an_id_window1(), *this);
            break;
        }
        case TRule_window_name_or_specification::kAltWindowNameOrSpecification2: {
            if (!Ctx.WinSpecsScopes) {
                auto pos = Ctx.TokenPosition(tail.GetBlock2().GetToken1());
                Ctx.Error(pos) << "Window and aggregation functions are not allowed in this context";
                return false;
            }

            TWindowSpecificationPtr spec = WindowSpecification(
                winRule.GetAlt_window_name_or_specification2().GetRule_window_specification1().GetRule_window_specification_details2());
            if (!spec) {
                return false;
            }

            WindowName = Ctx.MakeName("_yql_anonymous_window");
            TWinSpecs& specs = Ctx.WinSpecsScopes.back();
            YQL_ENSURE(!specs.contains(WindowName));
            specs[WindowName] = spec;
            break;
        }
        case TRule_window_name_or_specification::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
        }
        Ctx.IncrementMonCounter("sql_features", "WindowFunctionOver");
    }

    return true;
}

void TSqlCallExpr::IncCounters() {
    if (Node) {
        Ctx.IncrementMonCounter("sql_features", "NamedNodeUseApply");
    } else if (!Module.empty()) {
        if (ValidateForCounters(Module)) {
            Ctx.IncrementMonCounter("udf_modules", Module);
            Ctx.IncrementMonCounter("sql_features", "CallUdf");
            if (ValidateForCounters(Func)) {
                auto scriptType = NKikimr::NMiniKQL::ScriptTypeFromStr(Module);
                if (scriptType == NKikimr::NMiniKQL::EScriptType::Unknown) {
                   Ctx.IncrementMonCounter("udf_functions", Module + "." + Func);
                }
            }
        }
    } else if (ValidateForCounters(Func)) {
        Ctx.IncrementMonCounter("sql_builtins", Func);
        Ctx.IncrementMonCounter("sql_features", "CallBuiltin");
    }
}

} // namespace NSQLTranslationV1
