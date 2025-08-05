#include "sql_call_expr.h"
#include "sql_expression.h"

#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NSQLTranslationV1 {

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
        auto result = Node_ ? Node_ : BuildCallable(Pos_, Module_, Func_, Args_, forReduce);
        if (to_lower(Module_) == "tensorflow" && Func_ == "RunBatch") {
            if (Args_.size() > 2) {
                Args_.erase(Args_.begin() + 2);
            } else {
                Ctx_.Error(Pos_) << "Excepted >= 3 arguments, but got: " << Args_.size();
                return nullptr;
            }
        }
        return result;
}

TNodePtr TSqlCallExpr::BuildCall() {
        TVector<TNodePtr> args;
        bool warnOnYqlNameSpace = true;

        TUdfNode* udf_node = Node_ ? Node_->GetUdfNode() : nullptr;
        if (udf_node) {
            if (!udf_node->DoInit(Ctx_, nullptr)) {
                return nullptr;
            }
            TNodePtr positional_args = BuildTuple(Pos_, PositionalArgs_);
            TNodePtr positional = positional_args->Y("TypeOf", positional_args);
            TNodePtr named_args = BuildStructure(Pos_, NamedArgs_);
            TNodePtr named = named_args->Y("TypeOf", named_args);

            TNodePtr custom_user_type = new TCallNodeImpl(Pos_, "TupleType", {positional, named, udf_node->GetExternalTypes()});
            TNodePtr options = udf_node->BuildOptions();

            if (udf_node->IsScript()) {
                auto udf = BuildScriptUdf(Pos_, udf_node->GetModule(), udf_node->GetFunction(), udf_node->GetScriptArgs(), options);
                TVector<TNodePtr> applyArgs;
                applyArgs.push_back(new TAstAtomNodeImpl(Pos_, !NamedArgs_.empty() ? "NamedApply" : "Apply", TNodeFlags::Default));
                applyArgs.push_back(udf);
                if (!NamedArgs_.empty()) {
                    applyArgs.push_back(BuildTuple(Pos_, PositionalArgs_));
                    applyArgs.push_back(BuildStructure(Pos_, NamedArgs_));
                } else {
                    applyArgs.insert(applyArgs.end(), PositionalArgs_.begin(), PositionalArgs_.end());
                }

                return new TAstListNodeImpl(Pos_, applyArgs);
            }

            return BuildSqlCall(Ctx_, Pos_, udf_node->GetModule(), udf_node->GetFunction(),
                                args, positional_args, named_args, custom_user_type,
                                udf_node->GetTypeConfig(), udf_node->GetRunConfig(), options,
                                udf_node->GetDepends());
        }

        if (Node_ && (!Node_->FuncName() || Node_->IsScript())) {
            Module_ = "YQL";
            Func_ = NamedArgs_.empty() ? "Apply" : "NamedApply";
            warnOnYqlNameSpace = false;
            args.push_back(Node_);
        }

        if (Node_ && Node_->FuncName() && !Node_->IsScript()) {
            Module_ = Node_->ModuleName() ? *Node_->ModuleName() : "YQL";
            Func_ = *Node_->FuncName();
        }
        bool mustUseNamed = !NamedArgs_.empty();
        if (mustUseNamed) {
            if (Node_ && (!Node_->FuncName() || Node_->IsScript())) {
                mustUseNamed = false;
            }
            args.emplace_back(BuildTuple(Pos_, PositionalArgs_));
            args.emplace_back(BuildStructure(Pos_, NamedArgs_));
        } else if (IsExternalCall_) {
            Func_ = "SqlExternalFunction";
            if (Args_.size() < 2 || Args_.size() > 3) {
                Ctx_.Error(Pos_) << "EXTERNAL FUNCTION requires from 2 to 3 arguments, but got: " << Args_.size();
                return nullptr;
            }

            if (Args_.size() == 3) {
                args.insert(args.end(), Args_.begin(), Args_.end() - 1);
                Args_.erase(Args_.begin(), Args_.end() - 1);
            } else {
                args.insert(args.end(), Args_.begin(), Args_.end());
                Args_.erase(Args_.begin(), Args_.end());
            }
            auto configNode = new TExternalFunctionConfig(Pos_, CallConfig_);
            auto configList = new TAstListNodeImpl(Pos_, { new TAstAtomNodeImpl(Pos_, "quote", 0), configNode });
            args.push_back(configList);
        } else {
            args.insert(args.end(), Args_.begin(), Args_.end());
        }

        auto result = BuildBuiltinFunc(Ctx_, Pos_, Func_, args, Module_, AggMode_, &mustUseNamed, warnOnYqlNameSpace);
        if (mustUseNamed) {
            Error() << "Named args are used for call, but unsupported by function: " << Func_;
            return nullptr;
        }

        if (WindowName_) {
            result = BuildCalcOverWindow(Pos_, WindowName_, result);
        }

        return result;
}

bool TSqlCallExpr::Init(const TRule_value_constructor& node) {
    switch (node.Alt_case()) {
        case TRule_value_constructor::kAltValueConstructor1: {
            auto& ctor = node.GetAlt_value_constructor1();
            Func_ = "Variant";
            TSqlExpression expr(Ctx_, Mode_);
            if (!Expr(expr, Args_, ctor.GetRule_expr3())) {
                return false;
            }
            if (!Expr(expr, Args_, ctor.GetRule_expr5())) {
                return false;
            }
            if (!Expr(expr, Args_, ctor.GetRule_expr7())) {
                return false;
            }
            break;
        }
        case TRule_value_constructor::kAltValueConstructor2: {
            auto& ctor = node.GetAlt_value_constructor2();
            Func_ = "Enum";
            TSqlExpression expr(Ctx_, Mode_);
            if (!Expr(expr, Args_, ctor.GetRule_expr3())) {
                return false;
            }
            if (!Expr(expr, Args_, ctor.GetRule_expr5())) {
                return false;
            }
            break;
        }
        case TRule_value_constructor::kAltValueConstructor3: {
            auto& ctor = node.GetAlt_value_constructor3();
            Func_ = "Callable";
            TSqlExpression expr(Ctx_, Mode_);
            if (!Expr(expr, Args_, ctor.GetRule_expr3())) {
                return false;
            }
            if (!Expr(expr, Args_, ctor.GetRule_expr5())) {
                return false;
            }
            break;
        }
        case TRule_value_constructor::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
    PositionalArgs_ = Args_;
    return true;
}

bool TSqlCallExpr::ExtractCallParam(const TRule_external_call_param& node) {
    TString paramName = Id(node.GetRule_an_id1(), *this);
    paramName = to_lower(paramName);

    if (CallConfig_.contains(paramName)) {
        Ctx_.Error() << "WITH " << to_upper(paramName).Quote()
            << " clause should be specified only once";
        return false;
    }

    const bool optimizeForParam = paramName == "optimize_for";
    const auto columnRefState = optimizeForParam ? EColumnRefState::AsStringLiteral : EColumnRefState::Deny;

    TColumnRefScope scope(Ctx_, columnRefState);
    if (optimizeForParam) {
        scope.SetNoColumnErrContext("in external call params");
    }

    TSqlExpression expression(Ctx_, Mode_);
    auto value = expression.Build(node.GetRule_expr3());
    if (value && optimizeForParam) {
        TDeferredAtom atom;
        MakeTableFromExpression(Ctx_.Pos(), Ctx_, value, atom);
        value = new TCallNodeImpl(Ctx_.Pos(), "String", { atom.Build() });
    }

    if (!value) {
        return false;
    }

    CallConfig_[paramName] = value;
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
            Module_ = Id(subblock.GetRule_an_id_or_type1(), *this);
            Func_ = Id(subblock.GetRule_an_id_or_type3(), *this);
            break;
        }
        case TRule_using_call_expr::TBlock1::kAlt2: {
            Func_ = Id(block.GetAlt2().GetRule_an_id_expr1(), *this);
            break;
        }
        case TRule_using_call_expr::TBlock1::kAlt3: {
            TString bindName;
            if (!NamedNodeImpl(block.GetAlt3().GetRule_bind_parameter1(), bindName, *this)) {
                return false;
            }
            Node_ = GetNamedNode(bindName);
            if (!Node_) {
                return false;
            }
            break;
        }
        case TRule_using_call_expr::TBlock1::kAlt4: {
            IsExternalCall_ = true;
            break;
        }
        case TRule_using_call_expr::TBlock1::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
    YQL_ENSURE(!DistinctAllowed_);
    UsingCallExpr_ = true;
    TColumnRefScope scope(Ctx_, EColumnRefState::Allow);
    return Init(node.GetRule_invoke_expr2());
}

void TSqlCallExpr::InitName(const TString& name) {
    Module_ = "";
    Func_ = name;
}

void TSqlCallExpr::InitExpr(const TNodePtr& expr) {
    Node_ = expr;
}

bool TSqlCallExpr::FillArg(const TString& module, const TString& func, size_t& idx, const TRule_named_expr& node) {
    const bool isNamed = node.HasBlock2();

    TMaybe<EColumnRefState> status;
    // TODO: support named args
    if (!isNamed) {
        status = GetFunctionArgColumnStatus(Ctx_, module, func, idx);
    }

    TNodePtr expr;
    if (status) {
        TColumnRefScope scope(Ctx_, *status, /* isTopLevel = */ false);
        expr = NamedExpr(node);
    } else {
        expr = NamedExpr(node);
    }

    if (!expr) {
        return false;
    }

    Args_.emplace_back(std::move(expr));
    if (!isNamed) {
        ++idx;
    }
    return true;
}

bool TSqlCallExpr::FillArgs(const TRule_named_expr_list& node) {
    TString module = Module_;
    TString func = Func_;
    if (Node_ && Node_->FuncName() && !Node_->IsScript()) {
        module = Node_->ModuleName() ? *Node_->ModuleName() : "YQL";
        func = *Node_->FuncName();
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
    Pos_ = Ctx_.Pos();
    if (node.HasBlock2()) {
        switch (node.GetBlock2().Alt_case()) {
        case TRule_invoke_expr::TBlock2::kAlt1: {
            const auto& alt = node.GetBlock2().GetAlt1();
            TPosition distinctPos;
            if (IsDistinctOptSet(alt.GetRule_opt_set_quantifier1(), distinctPos)) {
                if (!DistinctAllowed_) {
                    if (UsingCallExpr_) {
                        Ctx_.Error(distinctPos) << "DISTINCT can not be used in PROCESS/REDUCE";
                    } else {
                        Ctx_.Error(distinctPos) << "DISTINCT can only be used in aggregation functions";
                    }
                    return false;
                }
                YQL_ENSURE(AggMode_ == EAggregateMode::Normal);
                AggMode_ = EAggregateMode::Distinct;
                Ctx_.IncrementMonCounter("sql_features", "DistinctInCallExpr");
            }
            if (!FillArgs(alt.GetRule_named_expr_list2())) {
                return false;
            }
            for (const auto& arg : Args_) {
                if (arg->GetLabel()) {
                    NamedArgs_.push_back(arg);
                }
                else {
                    PositionalArgs_.push_back(arg);
                    if (!NamedArgs_.empty()) {
                        Ctx_.Error(arg->GetPos()) << "Unnamed arguments can not follow after named one";
                        return false;
                    }
                }
            }
            break;
        }
        case TRule_invoke_expr::TBlock2::kAlt2:
            if (IsExternalCall_) {
                Ctx_.Error() << "You should set EXTERNAL FUNCTION type. Example: EXTERNAL FUNCTION('YANDEX-CLOUD', ...)";
            } else {
                Args_.push_back(new TAsteriskNode(Pos_));
            }
            break;
        case TRule_invoke_expr::TBlock2::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
        }
    }

    const auto& tail = node.GetRule_invoke_expr_tail4();

    if (tail.HasBlock1()) {
        if (IsExternalCall_) {
            Ctx_.Error() << "Additional clause after EXTERNAL FUNCTION(...) is not supported";
            return false;
        }

        switch (tail.GetBlock1().Alt_case()) {
        case TRule_invoke_expr_tail::TBlock1::kAlt1: {
            if (!tail.HasBlock2()) {
                Ctx_.Error() << "RESPECT/IGNORE NULLS can only be used with window functions";
                return false;
            }
            const auto& alt = tail.GetBlock1().GetAlt1();
            if (alt.GetRule_null_treatment1().Alt_case() == TRule_null_treatment::kAltNullTreatment2) {
                SetIgnoreNulls();
            }
            break;
        }
        case TRule_invoke_expr_tail::TBlock1::kAlt2: {
            Ctx_.Error() << "FILTER clause is not supported yet";
            return false;
        }
        case TRule_invoke_expr_tail::TBlock1::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
        }
    }

    if (tail.HasBlock2()) {
        if (Ctx_.DistinctOverWindow) {
            AggMode_ == EAggregateMode::Distinct ? SetOverWindowDistinct() : SetOverWindow();
        } else {
            if (AggMode_ == EAggregateMode::Distinct) {
                Ctx_.Error() << "DISTINCT is not yet supported in window functions";
                return false;
            }
            SetOverWindow();
        }
        auto winRule = tail.GetBlock2().GetRule_window_name_or_specification2();
        switch (winRule.Alt_case()) {
        case TRule_window_name_or_specification::kAltWindowNameOrSpecification1: {
            WindowName_ = Id(winRule.GetAlt_window_name_or_specification1().GetRule_window_name1().GetRule_an_id_window1(), *this);
            break;
        }
        case TRule_window_name_or_specification::kAltWindowNameOrSpecification2: {
            if (!Ctx_.WinSpecsScopes) {
                auto pos = Ctx_.TokenPosition(tail.GetBlock2().GetToken1());
                Ctx_.Error(pos) << "Window and aggregation functions are not allowed in this context";
                return false;
            }

            TWindowSpecificationPtr spec = WindowSpecification(
                winRule.GetAlt_window_name_or_specification2().GetRule_window_specification1().GetRule_window_specification_details2());
            if (!spec) {
                return false;
            }

            WindowName_ = Ctx_.MakeName("_yql_anonymous_window");
            TWinSpecs& specs = Ctx_.WinSpecsScopes.back();
            YQL_ENSURE(!specs.contains(WindowName_));
            specs[WindowName_] = spec;
            break;
        }
        case TRule_window_name_or_specification::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
        }
        Ctx_.IncrementMonCounter("sql_features", "WindowFunctionOver");
    }

    return true;
}

void TSqlCallExpr::IncCounters() {
    if (Node_) {
        Ctx_.IncrementMonCounter("sql_features", "NamedNodeUseApply");
    } else if (!Module_.empty()) {
        if (ValidateForCounters(Module_)) {
            Ctx_.IncrementMonCounter("udf_modules", Module_);
            Ctx_.IncrementMonCounter("sql_features", "CallUdf");
            if (ValidateForCounters(Func_)) {
                auto scriptType = NKikimr::NMiniKQL::ScriptTypeFromStr(Module_);
                if (scriptType == NKikimr::NMiniKQL::EScriptType::Unknown) {
                   Ctx_.IncrementMonCounter("udf_functions", Module_ + "." + Func_);
                }
            }
        }
    } else if (ValidateForCounters(Func_)) {
        Ctx_.IncrementMonCounter("sql_builtins", Func_);
        Ctx_.IncrementMonCounter("sql_features", "CallBuiltin");
    }
}

} // namespace NSQLTranslationV1
