#include "kqp_compile_trace_transformer.h"

namespace NKikimr::NKqp {

using namespace NYql;

namespace {

class TCompileTraceTransformer : public TGraphTransformerBase {
public:
    TCompileTraceTransformer(TAutoPtr<IGraphTransformer>&& transformer, TIntrusivePtr<TKikimrQueryContext> queryCtx,
            ui8 verbosity, TString name)
        : Transformer(std::move(transformer))
        , QueryCtx(std::move(queryCtx))
        , Verbosity(verbosity)
        , Name(std::move(name))
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        auto* trace = QueryCtx->CompileTrace.Get();
        if (!Span && trace) {
            Span = trace->StartSpan(Verbosity, Name);
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
        if (!trace) {
            return func();
        }
        trace->Push(Span);
        TStatus status = TStatus::Error;
        try {
            status = func();
        } catch (...) {
            trace->Pop(Span);
            TCompileTrace::EndSpan(Span, false);
            throw;
        }
        trace->Pop(Span);
        if (status == TStatus::Ok || status.HasRestart) {
            TCompileTrace::EndSpan(Span);
        } else if (status == TStatus::Error) {
            TCompileTrace::EndSpan(Span, false);
        }
        return status;
    }

private:
    TAutoPtr<IGraphTransformer> Transformer;
    const TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    const ui8 Verbosity;
    const TString Name;
    NWilson::TSpan Span;
};

class TCompileTraceMarker : public TSyncTransformerBase {
public:
    TCompileTraceMarker(TIntrusivePtr<TKikimrQueryContext> queryCtx, bool begin, ui8 verbosity, TString name,
            const void* group)
        : QueryCtx(std::move(queryCtx))
        , Begin(begin)
        , Verbosity(verbosity)
        , Name(std::move(name))
        , Group(group)
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) final {
        output = input;
        if (auto* trace = QueryCtx->CompileTrace.Get()) {
            if (Begin) {
                trace->BeginRegion(Verbosity, Name, Group);
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
    const void* const Group;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateCompileTraceTransformer(TAutoPtr<IGraphTransformer>&& transformer,
        TIntrusivePtr<TKikimrQueryContext> queryCtx, ui8 verbosity, TString name) {
    return new TCompileTraceTransformer(std::move(transformer), std::move(queryCtx), verbosity, std::move(name));
}

TAutoPtr<IGraphTransformer> CreateCompileTraceBegin(TIntrusivePtr<TKikimrQueryContext> queryCtx,
        ui8 verbosity, TString name, const void* group) {
    return new TCompileTraceMarker(std::move(queryCtx), true, verbosity, std::move(name), group);
}

TAutoPtr<IGraphTransformer> CreateCompileTraceEnd(TIntrusivePtr<TKikimrQueryContext> queryCtx, TString name) {
    return new TCompileTraceMarker(std::move(queryCtx), false, 0, std::move(name), nullptr);
}

} // namespace NKikimr::NKqp
