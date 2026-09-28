#pragma once

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

#include <optional>

namespace NFq {

class ICheckpointProviderIntegration : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ICheckpointProviderIntegration>;

    struct TCleanupGraphSinkArguments {
        TVector<ui64> TaskIds;
        ui64 OutputIndex = 0;
        THashMap<TString, TString> SecureParams;
        THashMap<TString, TString> RequestContext;
    };

    struct TCleanupGraphSink {
        NYql::NDqProto::TTaskOutputSink Sink;
        TCleanupGraphSinkArguments Args;
    };

    // Matches TTaskOutputSink::Type.
    virtual TStringBuf GetSinkName() const = 0;

    // Called for sinks matching GetSinkName() before deleting their last checkpoint.
    // Implementations must be idempotent: cleanup can be retried if checkpoint deletion fails.
    // Report failures through the returned future instead of throwing synchronously.
    // Each entry describes one stage output and its tasks, with shared parameters.
    // GC only cleans generations strictly below generationUpperBound; no bound means all generations.
    virtual NThreading::TFuture<NYql::TIssues> CleanupGraphSinks(TVector<TCleanupGraphSink>&& sinks, std::optional<ui64> generationUpperBound) = 0;
};

// Keys are provider names, e.g. PqProviderName.
using TCheckpointProviderIntegrations = THashMap<TString, ICheckpointProviderIntegration::TPtr>;

} // namespace NFq
