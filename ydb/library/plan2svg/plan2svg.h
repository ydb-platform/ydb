#pragma once

// Renders a query plan JSON as an SVG timeline. This is the whole public
// surface: load one or more plans, then ask for the SVG.
//
// The implementation is split as:
//   visualizer.h the plan set behind this facade   plan.h     TPlan
//   metrics.*  aggregations and time series        model.h    plan data structures
//   format.*   value to string helpers             config.*   layout constants, palette
//   svg.*      SVG element primitives              parse.*    JSON field extraction
//   loader.cpp JSON to TPlan   layout.cpp  sizing and hot path   render.cpp  SVG emission
//   assets/    static icon and script blobs

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/string.h>

#include <memory>

namespace NPlan2Svg {

class TVisualizer;

class TPlanVisualizer {

public:
    TPlanVisualizer();
    ~TPlanVisualizer();

    // Accepts the plan JSON either as text or already parsed. Both are lenient:
    // input that is not a plan simply loads nothing.
    void LoadPlans(const TString& plans, bool simplified = false);
    void LoadPlans(const NJson::TJsonValue& root);

    // PrintSvg throws on a plan it cannot render; PrintSvgSafe returns an SVG
    // carrying the error message instead.
    TString PrintSvg();
    TString PrintSvgSafe();

private:
    std::unique_ptr<TVisualizer> Impl;
};

} // namespace NPlan2Svg
