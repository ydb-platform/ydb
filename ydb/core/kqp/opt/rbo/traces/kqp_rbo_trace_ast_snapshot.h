#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>

#include <util/generic/string.h>
#include <util/generic/ptr.h>

#include <memory>

namespace NKikimr::NKqp {

namespace NOpt {
struct TKqpBuildQueryContext;
} // namespace NOpt

class TRboTraceAstSnapshotLog;

std::shared_ptr<TRboTraceAstSnapshotLog> CreateKqpRboTraceAstSnapshotLog(TString stage);

TAutoPtr<NYql::IGraphTransformer> CreateKqpRboTraceAstSnapshotTransformer(
    std::shared_ptr<TRboTraceAstSnapshotLog> traceLog,
    TString title);

TAutoPtr<NYql::IGraphTransformer> CreateKqpRboTraceBuildQuerySnapshotTransformer(
    std::shared_ptr<TRboTraceAstSnapshotLog> traceLog,
    TString title,
    TIntrusivePtr<NOpt::TKqpBuildQueryContext> buildCtx);

} // namespace NKikimr::NKqp
