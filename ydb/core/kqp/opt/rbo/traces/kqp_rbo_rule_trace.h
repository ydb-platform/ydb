#pragma once

#include "../kqp_rbo.h"

#include <optional>
#include <util/generic/strbuf.h>

namespace NKikimr::NKqp {

class TRuleTraceAttempt {
public:
    TRuleTraceAttempt(TRBOContext& ctx, TStringBuf ruleName);

    void CloseRule();
    void SubmitIfHasInfo(TOpRoot& root, TStringBuf stageName);
    void SubmitApplied(TOpRoot& root, TStringBuf stageName);

private:
    void Submit(TOpRoot& root, TStringBuf stageName);

    TRBOContext& Ctx;
    std::optional<optimizer_trace::Trace::Tile> Tile;
    bool Closed = false;
};

void SubmitInitialPlanTrace(TOpRoot& root, TRBOContext& ctx);

} // namespace NKikimr::NKqp
