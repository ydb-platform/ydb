#pragma once

#include "traces/kqp_rbo_trace_log.h"
#include "verification/transformation_event.h"

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_cbo.h>

#include <library/cpp/json/writer/json_value.h>

#include <optional>
#include <utility>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

struct TExprContext;
struct TTypeAnnotationContext;
class IGraphTransformer;

} // namespace NYql

namespace NMiniKQL {

class IFunctionRegistry;

} // namespace NMiniKQL

namespace NKikimr::NKqp {

struct TRBOTransformationDebugState {
    void Reset(std::optional<ui64> stopAfter) {
        StopAfter = stopAfter;
        Stopped = false;
        Events.clear();
    }

    bool OnRuleApplication(const TString& stage, const TString& name) {
        return OnEvent(ERBOTransformationEventKindV1::RuleApplication, stage, name);
    }

    bool OnAtomicStageCommit(const TString& stage, const TString& name) {
        return OnEvent(ERBOTransformationEventKindV1::AtomicStageCommit, stage, name);
    }

private:
    bool OnEvent(
        ERBOTransformationEventKindV1 kind,
        const TString& stage,
        const TString& name)
    {
        if (!StopAfter) {
            return false;
        }

        Events.push_back(TRBOTransformationEventV1{
            Events.size() + 1,
            kind,
            stage,
            name,
        });
        if (Events.size() != *StopAfter) {
            return false;
        }

        Stopped = true;
        return true;
    }

public:
    std::optional<ui64> StopAfter;
    bool Stopped = false;
    TVector<TRBOTransformationEventV1> Events;
};

class TRBOContext {
public:
    TRBOContext(NOpt::TKqpOptimizeContext& kqpCtx, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typeCtx,
        NYql::IGraphTransformer& typeAnnTransformer, const NMiniKQL::IFunctionRegistry& funcRegistry);

    bool NeedToLog() const {
        return TraceLog.NeedToLog();
    }

    template <typename TEnricher>
    void EnrichRuleLog(TEnricher&& enricher) {
        if (auto* tile = TraceLog.CurrentRuleTile()) {
            std::forward<TEnricher>(enricher)(*tile);
        }
    }

    template <typename TEnricher>
    void EnrichRuleLogAfterTree(TEnricher&& enricher) {
        if (TraceLog.CurrentRuleTile()) {
            TraceLog.AddPostBuildEnricher(std::forward<TEnricher>(enricher));
        }
    }

    NOpt::TKqpOptimizeContext& KqpCtx;
    NYql::TExprContext& ExprCtx;
    NYql::TTypeAnnotationContext& TypeCtx;
    NYql::IGraphTransformer& TypeAnnTransformer;
    const NMiniKQL::IFunctionRegistry& FuncRegistry;
    NOpt::TKqpProviderContext CBOCtx;
    std::optional<NJson::TJsonValue> ExecutionJson;
    std::optional<NJson::TJsonValue> ExplainJson;

    // Query-local, opt-in diagnostic state. Failed rule attempts never emit an
    // event; global mutating stages emit one event after their atomic commit.
    TRBOTransformationDebugState TransformationDebug;

    TRBOTraceLog TraceLog;
};

} // namespace NKikimr::NKqp
