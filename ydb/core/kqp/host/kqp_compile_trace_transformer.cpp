#include "kqp_compile_trace_transformer.h"

#include <ydb/core/kqp/tracing/kqp_compile_trace_expr.h>

namespace NKikimr::NKqp {

using namespace NYql;

namespace {

// Size of the expression a span works on, recorded at Diagnostic verbosity
// since counting visits the whole graph.
void AddExprSize(NWilson::TSpan& span, const TCompileTrace& trace, const TExprNode::TPtr& input) {
    if (!span || !input || !trace.IsEnabled(TCompileTraceLevels::Step)) {
        return;
    }
    span.Attribute("ydb.expr.nodes", CountExprNodes(*input));
}

class TCompileTraceTransformer : public TGraphTransformerBase {
public:
    TCompileTraceTransformer(TAutoPtr<IGraphTransformer>&& transformer, TIntrusivePtr<TKikimrQueryContext> queryCtx,
            ui8 verbosity, TString name, const char* component)
        : Transformer(std::move(transformer))
        , QueryCtx(std::move(queryCtx))
        , Verbosity(verbosity)
        , Name(std::move(name))
        , Component(component)
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        auto* trace = QueryCtx->CompileTrace.Get();
        if (!Span && trace && trace->IsEnabled(Verbosity)) {
            const ui32 iteration = trace->NextIteration(Name);
            Span = trace->StartSpan(Verbosity, Name, Component);
            Span.Attribute("ydb.compile.iteration", static_cast<i64>(iteration));
            AddExprSize(Span, *trace, input);
            CpuStart = trace->GetCpuTime();
        }
        return Traced(trace, [&] {
            return Transformer->Transform(input, output, ctx);
        });
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        return Transformer->GetAsyncFuture(input);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        return Traced(QueryCtx->CompileTrace.Get(), [&] {
            return Transformer->ApplyAsyncChanges(input, output, ctx);
        });
    }

    void Rewind() final {
        if (Span) {
            Span.EndError("Transformer rewound");
        }
        Transformer->Rewind();
    }

    TStatistics GetStatistics() const final {
        return Transformer->GetStatistics();
    }

private:
    template <class TFunc>
    TStatus Traced(TCompileTrace* trace, TFunc&& func) {
        if (trace) {
            trace->Push(Span);
        }
        TStatus status = TStatus::Error;
        try {
            status = func();
        } catch (...) {
            if (trace) {
                trace->Pop(Span);
                trace->EndSpan(Span, CpuStart, false);
            }
            throw;
        }
        if (trace) {
            trace->Pop(Span);
        }
        if (!trace) {
            return status;
        }
        if (status.HasRestart) {
            Span.Attribute("ydb.compile.restarted", true);
        }
        if (status == TStatus::Ok || status.HasRestart) {
            trace->EndSpan(Span, CpuStart);
        } else if (status == TStatus::Error) {
            trace->EndSpan(Span, CpuStart, false);
        }
        return status;
    }

private:
    TAutoPtr<IGraphTransformer> Transformer;
    const TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    const ui8 Verbosity;
    const TString Name;
    const char* const Component;
    NWilson::TSpan Span;
    ui64 CpuStart = 0;
};

class TCompileTraceMarker : public TSyncTransformerBase {
public:
    TCompileTraceMarker(TIntrusivePtr<TKikimrQueryContext> queryCtx, bool begin, ui8 verbosity, TString name,
            const char* component, const void* group)
        : QueryCtx(std::move(queryCtx))
        , Begin(begin)
        , Verbosity(verbosity)
        , Name(std::move(name))
        , Component(component)
        , Group(group)
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) final {
        output = input;
        if (auto* trace = QueryCtx->CompileTrace.Get()) {
            if (Begin) {
                if (auto* span = trace->BeginRegion(Verbosity, Name, Component, Group)) {
                    AddExprSize(*span, *trace, input);
                }
            } else {
                trace->EndRegion(Name);
            }
        }
        return TStatus::Ok;
    }

    void Rewind() final {
    }

private:
    const TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    const bool Begin;
    const ui8 Verbosity;
    const TString Name;
    const char* const Component;
    const void* const Group;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateCompileTraceTransformer(TAutoPtr<IGraphTransformer>&& transformer,
        TIntrusivePtr<TKikimrQueryContext> queryCtx, ui8 verbosity, TString name, const char* component) {
    return new TCompileTraceTransformer(std::move(transformer), std::move(queryCtx), verbosity, std::move(name), component);
}

TAutoPtr<IGraphTransformer> CreateCompileTraceBegin(TIntrusivePtr<TKikimrQueryContext> queryCtx,
        ui8 verbosity, TString name, const char* component, const void* group) {
    return new TCompileTraceMarker(std::move(queryCtx), true, verbosity, std::move(name), component, group);
}

TAutoPtr<IGraphTransformer> CreateCompileTraceEnd(TIntrusivePtr<TKikimrQueryContext> queryCtx, TString name) {
    return new TCompileTraceMarker(std::move(queryCtx), false, 0, std::move(name), nullptr, nullptr);
}

} // namespace NKikimr::NKqp
