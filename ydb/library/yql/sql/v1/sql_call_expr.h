#pragma once

#include "sql_translation.h"

namespace NSQLTranslationV1 {

TNodePtr BuildSqlCall(TContext& ctx, TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args,
    TNodePtr positionalArgs, TNodePtr namedArgs, TNodePtr customUserType, const TDeferredAtom& typeConfig, TNodePtr runConfig);

using namespace NSQLv1Generated;

class TSqlCallExpr: public TSqlTranslation {
public:
    TSqlCallExpr(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TSqlCallExpr(const TSqlCallExpr& call, const TVector<TNodePtr>& args)
        : TSqlTranslation(call.Ctx, call.Mode)
        , Pos(call.Pos)
        , Func(call.Func)
        , Module(call.Module)
        , Node(call.Node)
        , Args(args)
        , AggMode(call.AggMode)
        , DistinctAllowed(call.DistinctAllowed)
        , UsingCallExpr(call.UsingCallExpr)
        , IsExternalCall(call.IsExternalCall)
        , CallConfig(call.CallConfig)
    {
    }

    void AllowDistinct() {
        DistinctAllowed = true;
    }

    void InitName(const TString& name);
    void InitExpr(const TNodePtr& expr);

    bool Init(const TRule_using_call_expr& node);
    bool Init(const TRule_value_constructor& node);
    bool Init(const TRule_invoke_expr& node);
    bool ConfigureExternalCall(const TRule_external_call_settings& node);
    void IncCounters();

    TNodePtr BuildUdf(bool forReduce);

    TNodePtr BuildCall();

    TPosition GetPos() const {
        return Pos;
    }

    const TVector<TNodePtr>& GetArgs() const {
        return Args;
    }

    void SetOverWindow() {
        YQL_ENSURE(AggMode == EAggregateMode::Normal);
        AggMode = EAggregateMode::OverWindow;
    }

    void SetIgnoreNulls() {
        Func += "_IgnoreNulls";
    }

    bool IsExternal() {
        return IsExternalCall;
    }

private:
    bool ExtractCallParam(const TRule_external_call_param& node);
    bool FillArg(const TString& module, const TString& func, size_t& idx, const TRule_named_expr& node);
    bool FillArgs(const TRule_named_expr_list& node);

private:
    TPosition Pos;
    TString Func;
    TString Module;
    TNodePtr Node;
    TVector<TNodePtr> Args;
    TVector<TNodePtr> PositionalArgs;
    TVector<TNodePtr> NamedArgs;
    EAggregateMode AggMode = EAggregateMode::Normal;
    TString WindowName;
    bool DistinctAllowed = false;
    bool UsingCallExpr = false;
    bool IsExternalCall = false;
    TFunctionConfig CallConfig;
};

} // namespace NSQLTranslationV1
