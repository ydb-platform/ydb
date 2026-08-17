#pragma once

#include "../html_log/cpp/optimizer_trace.h"

#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace NKikimr::NKqp {

struct TTraceBuildState;

class TRBOTraceLog {
public:
    using TSubmitCallback = std::function<void(optimizer_trace::Trace::Tile&)>;
    using TPostBuildEnricher = std::function<void(optimizer_trace::Trace::Tile&, const TTraceBuildState&)>;

    void Configure(optimizer_trace::Trace& trace, TSubmitCallback submit);
    bool NeedToLog() const;
    void SetQueryText(std::optional<std::string> queryText);
    const std::optional<std::string>& QueryText() const;

    optimizer_trace::Trace& trace();
    optimizer_trace::Trace::Stage& stage(const std::string& name);
    optimizer_trace::Trace::Stage& currentStage();
    optimizer_trace::Trace::Tile* CurrentRuleTile();
    void SetCurrentRuleTile(optimizer_trace::Trace::Tile* tile);
    void AddPostBuildEnricher(TPostBuildEnricher enricher);
    void ApplyPostBuildEnrichers(optimizer_trace::Trace::Tile& tile, const TTraceBuildState& state);
    void ClearPostBuildEnrichers();

    optimizer_trace::Trace::Tile& AddTile(optimizer_trace::Trace::Tile tile);
    void Submit(optimizer_trace::Trace::Tile& tile);

private:
    optimizer_trace::Trace* Trace = nullptr;
    optimizer_trace::Trace::Stage* CurrentStage = nullptr;
    optimizer_trace::Trace::Tile* CurrentTile = nullptr;
    TSubmitCallback SubmitTraceTile;
    std::vector<TPostBuildEnricher> PostBuildEnrichers;
    std::optional<std::string> QueryText_;
};

} // namespace NKikimr::NKqp
