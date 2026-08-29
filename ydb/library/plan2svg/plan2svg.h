#pragma once

// Renders a query plan JSON as an SVG timeline. Consumers only need this header
// and the three entry points on TPlanVisualizer below: LoadPlans, PrintSvg and
// PrintSvgSafe.
//
// The implementation is split as:
//   metrics.*  aggregations and time series      model.h    plan data structures
//   format.*   value to string helpers           config.*   layout constants, palette
//   svg.*      SVG element primitives            parse.*    JSON field extraction
//   plan.h     TPlan                             assets/    static icon and script blobs
//   loader.cpp JSON to TPlan   layout.cpp  sizing and hot path   render.cpp  SVG emission

#include "config.h"
#include "metrics.h"
#include "model.h"
#include "plan.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/yson/json2yson.h>

#include <util/generic/string.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace NPlan2Svg {

class TPlanVisualizer {

public:

    void LoadPlans(const TString& plans, bool simplified = false);
    void LoadPlans(const NJson::TJsonValue& root);
    void LoadPlan(const TString& planNodeType, const NJson::TJsonValue& root);
    void PostProcessPlans();
    TString PrintSvg();
    TString PrintSvgSafe();
    ui32 NextGroupId() { return ++GroupId; }

    std::vector<std::shared_ptr<TPlan>> Plans;
    ui64 MaxTime = 1;
    ui64 BaseTime = 0;
    ui64 UpdateTime = 0;
    TPlanViewConfig Config;
    std::map<std::string, std::shared_ptr<TStage>> CteStages;
    std::map<std::string, TPlan*> CteSubPlans;
    ui32 GroupId = 0;
};

} // namespace NPlan2Svg
