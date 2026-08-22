#include "kqp_rbo_trace_log.h"

#include <stdexcept>
#include <utility>

namespace NKikimr::NKqp {

void TRBOTraceLog::Configure(optimizer_trace::Trace& trace, TSubmitCallback submit) {
    Trace = &trace;
    CurrentStage = nullptr;
    CurrentTile = nullptr;
    SubmitTraceTile = std::move(submit);
    QueryText_.reset();
}

bool TRBOTraceLog::NeedToLog() const {
    return Trace != nullptr;
}

void TRBOTraceLog::SetQueryText(std::optional<std::string> queryText) {
    QueryText_ = std::move(queryText);
}

const std::optional<std::string>& TRBOTraceLog::QueryText() const {
    return QueryText_;
}

optimizer_trace::Trace& TRBOTraceLog::trace() {
    if (!Trace) {
        throw std::logic_error("RBO trace log is not configured");
    }
    return *Trace;
}

optimizer_trace::Trace::Stage& TRBOTraceLog::stage(const std::string& name) {
    CurrentStage = &trace().stage(name);
    return *CurrentStage;
}

optimizer_trace::Trace::Stage& TRBOTraceLog::currentStage() {
    if (!CurrentStage) {
        throw std::logic_error("RBO trace log has no current stage");
    }
    return *CurrentStage;
}

optimizer_trace::Trace::Tile* TRBOTraceLog::CurrentRuleTile() {
    return CurrentTile;
}

void TRBOTraceLog::SetCurrentRuleTile(optimizer_trace::Trace::Tile* tile) {
    if (tile) {
        ClearPostBuildEnrichers();
    }
    CurrentTile = tile;
}

void TRBOTraceLog::AddPostBuildEnricher(TPostBuildEnricher enricher) {
    PostBuildEnrichers.push_back(std::move(enricher));
}

void TRBOTraceLog::ApplyPostBuildEnrichers(optimizer_trace::Trace::Tile& tile, const TTraceBuildState& state) {
    for (const auto& enricher : PostBuildEnrichers) {
        enricher(tile, state);
    }
}

void TRBOTraceLog::ClearPostBuildEnrichers() {
    PostBuildEnrichers.clear();
}

optimizer_trace::Trace::Tile& TRBOTraceLog::AddTile(optimizer_trace::Trace::Tile tile) {
    return currentStage().addTile(std::move(tile));
}

void TRBOTraceLog::Submit(optimizer_trace::Trace::Tile& tile) {
    if (SubmitTraceTile) {
        SubmitTraceTile(tile);
    }
}

} // namespace NKikimr::NKqp
