#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_provider.h>

#include <yql/essentials/core/yql_graph_transformer.h>

namespace NKikimr::NKqp {

// Wraps the transformer into a compilation region that lasts until it finishes
// or restarts the pipeline, including its asynchronous waits.
TAutoPtr<NYql::IGraphTransformer> CreateCompileTraceTransformer(TAutoPtr<NYql::IGraphTransformer>&& transformer,
    TIntrusivePtr<NYql::TKikimrQueryContext> queryCtx, ui8 verbosity, TString name);

// Pipeline stages that open and close a compilation region around the stages
// between them, for groups of transformers added by TTransformationPipeline.
// The regions of one pipeline share the group (see TCompileTrace::BeginRegion).
TAutoPtr<NYql::IGraphTransformer> CreateCompileTraceBegin(TIntrusivePtr<NYql::TKikimrQueryContext> queryCtx,
    ui8 verbosity, TString name, const void* group);
TAutoPtr<NYql::IGraphTransformer> CreateCompileTraceEnd(TIntrusivePtr<NYql::TKikimrQueryContext> queryCtx,
    TString name, const void* group);

} // namespace NKikimr::NKqp
