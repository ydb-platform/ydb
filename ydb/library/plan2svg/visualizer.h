#pragma once

// The implementation behind TPlanVisualizer in plan2svg.h. Internal to the
// library and to its tests; consumers get the facade instead.

#include "config.h"
#include "metrics.h"
#include "model.h"
#include "plan.h"

#include <library/cpp/json/json_reader.h>

#include <util/generic/string.h>

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace NPlan2Svg {

class TVisualizer {

public:

    void LoadPlans(const TString& plans, bool simplified = false);
    void LoadPlans(const NJson::TJsonValue& root);
    void LoadPlansSafe(const TString& plans, bool simplified = false);
    void LoadPlansSafe(const NJson::TJsonValue& root);
    void LoadSafe(const std::function<void()>& load);
    void LoadPlan(const TString& planNodeType, const NJson::TJsonValue& root);
    void PostProcessPlans();
    TString PrintSvg();
    TString PrintSvgSafe();
    ui32 NextGroupId() { return ++GroupId; }

    std::vector<std::shared_ptr<TPlan>> Plans;
    // Set by LoadPlansSafe when loading threw, cleared when it is entered again.
    // The failed call itself is rolled back: a plan that stopped halfway through
    // loading draws a picture that lies about the query, so the visualizer is
    // left as it was before the call.
    TString LoadError;
    ui64 MaxTime = 1;
    ui64 BaseTime = 0;
    ui64 UpdateTime = 0;
    TPlanViewConfig Config;
    std::map<std::string, std::shared_ptr<TStage>> CteStages;
    std::map<std::string, TPlan*> CteSubPlans;
    ui32 GroupId = 0;
};

} // namespace NPlan2Svg
