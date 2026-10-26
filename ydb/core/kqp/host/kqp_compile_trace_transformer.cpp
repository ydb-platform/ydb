#include "kqp_compile_trace_transformer.h"

#include <ydb/core/kqp/tracing/kqp_compile_trace.h>

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
        if (auto* trace = QueryCtx->CompileTrace.Get()) {
            // Does nothing if the region is still open, e.g. after Repeat.
            trace->BeginRegion(Verbosity, Name, this);
        }
        return Traced([&] {
            return Transformer->Transform(input, output, ctx);
        });
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        return Transformer->GetAsyncFuture(input);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        return Traced([&] {
            return Transformer->ApplyAsyncChanges(input, output, ctx);
        });
    }

    void Rewind() final {
        EndRegion(false);
        Transformer->Rewind();
    }

    TStatistics GetStatistics() const final {
        return Transformer->GetStatistics();
    }

private:
    template <class TFunc>
    TStatus Traced(TFunc&& func) {
        TStatus status = TStatus::Error;
        try {
            status = func();
        } catch (...) {
            EndRegion(false);
            throw;
        }
        // An error may carry the restart flag too.
        if (status == TStatus::Error) {
            EndRegion(false);
        } else if (status == TStatus::Ok || status.HasRestart) {
            EndRegion(true);
        }
        return status;
    }

    void EndRegion(bool success) {
        if (auto* trace = QueryCtx->CompileTrace.Get()) {
            trace->EndRegion(Name, this, success);
        }
    }

private:
    TAutoPtr<IGraphTransformer> Transformer;
    const TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    const ui8 Verbosity;
    const TString Name;
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
                trace->EndRegion(Name, Group);
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

TAutoPtr<IGraphTransformer> CreateCompileTraceEnd(TIntrusivePtr<TKikimrQueryContext> queryCtx, TString name,
        const void* group) {
    return new TCompileTraceMarker(std::move(queryCtx), false, 0, std::move(name), group);
}

} // namespace NKikimr::NKqp
