#pragma once

#include <ydb/core/kqp/tracing/kqp_compile_trace.h>

#include <yql/essentials/core/yql_graph_transformer.h>

#include <functional>

namespace NKikimr::NKqp {

// Pipelines can outlive one compilation. Resolve the current trace on each turn;
// the provider owns the context that supplies it, without coupling tracing to it.
using TCompileTraceProvider = std::function<TCompileTrace*()>;

// Wraps the transformer into a compilation region that lasts until it finishes
// or restarts the pipeline, including its asynchronous waits.
TAutoPtr<NYql::IGraphTransformer> CreateCompileTraceTransformer(TAutoPtr<NYql::IGraphTransformer>&& transformer,
    TCompileTraceProvider getTrace, ui8 verbosity, TString name);

// Pipeline stages that open and close a compilation region around the stages
// between them, for groups of transformers added by TTransformationPipeline.
// The regions of one pipeline share the group (see TCompileTrace::BeginRegion).
TAutoPtr<NYql::IGraphTransformer> CreateCompileTraceBegin(TCompileTraceProvider getTrace,
    ui8 verbosity, TString name, const void* group, TCompileTraceAttributes attributes = {});
TAutoPtr<NYql::IGraphTransformer> CreateCompileTraceEnd(TCompileTraceProvider getTrace,
    TString name, const void* group);

} // namespace NKikimr::NKqp
