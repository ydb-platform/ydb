#pragma once

#include "sql_translation.h"

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

class TSqlCallExpr: public TSqlTranslation {
public:
    TSqlCallExpr(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TSqlCallExpr(const TSqlCallExpr& call, const TVector<TNodePtr>& args)
        : TSqlTranslation(call.Ctx_, call.Mode_)
        , Pos_(call.Pos_)
        , Func_(call.Func_)
        , Module_(call.Module_)
        , Node_(call.Node_)
        , Args_(args)
        , AggMode_(call.AggMode_)
        , DistinctAllowed_(call.DistinctAllowed_)
        , UsingCallExpr_(call.UsingCallExpr_)
        , IsExternalCall_(call.IsExternalCall_)
        , CallConfig_(call.CallConfig_)
    {
    }

    void AllowDistinct() {
        DistinctAllowed_ = true;
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
        return Pos_;
    }

    const TVector<TNodePtr>& GetArgs() const {
        return Args_;
    }

    void SetOverWindow() {
        YQL_ENSURE(AggMode_ == EAggregateMode::Normal);
        AggMode_ = EAggregateMode::OverWindow;
    }

    void SetOverWindowDistinct() {
        YQL_ENSURE(AggMode_ == EAggregateMode::Distinct);
        AggMode_ = EAggregateMode::OverWindowDistinct;
    }

    void SetIgnoreNulls() {
        Func_ += "_IgnoreNulls";
    }

    bool IsExternal() {
        return IsExternalCall_;
    }

private:
    bool ExtractCallParam(const TRule_external_call_param& node);
    bool FillArg(const TString& module, const TString& func, size_t& idx, const TRule_named_expr& node);
    bool FillArgs(const TRule_named_expr_list& node);

private:
    TPosition Pos_;
    TString Func_;
    TString Module_;
    TNodePtr Node_;
    TVector<TNodePtr> Args_;
    TVector<TNodePtr> PositionalArgs_;
    TVector<TNodePtr> NamedArgs_;
    EAggregateMode AggMode_ = EAggregateMode::Normal;
    TString WindowName_;
    bool DistinctAllowed_ = false;
    bool UsingCallExpr_ = false;
    bool IsExternalCall_ = false;
    TFunctionConfig CallConfig_;
};

} // namespace NSQLTranslationV1
