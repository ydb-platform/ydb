#pragma once

#include "sql_translation.h"

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

class TSqlCallExpr: public TSqlTranslation {
public:
    explicit TSqlCallExpr(const TSqlTranslation& that)
        : TSqlTranslation(that)
    {
    }

    TSqlCallExpr(const TSqlCallExpr&) = default;

    TSqlCallExpr(const TSqlCallExpr& call, const TVector<TNodePtr>& args)
        : TSqlCallExpr(call)
    {
        Args_ = args;
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

    TNodeResult BuildCall();

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
    TSQLStatus FillArg(const TString& module, const TString& func, size_t& idx, const TRule_named_expr& node);
    TSQLStatus FillArgs(const TRule_named_expr_list& node);

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
