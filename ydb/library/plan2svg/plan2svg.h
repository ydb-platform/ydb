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

    // Accepts the plan JSON either as text or already parsed. Both are lenient
    // about input that is not a plan - it simply loads nothing - but a plan that
    // is malformed or that the loader does not understand throws.
    void LoadPlans(const TString& plans, bool simplified = false);
    void LoadPlans(const NJson::TJsonValue& root);

    // The non-throwing counterparts, for callers that cannot let an exception
    // out: an actor handler, or any path where a bad plan should degrade to a
    // picture of the error. The failure is reported by the following
    // PrintSvgSafe, so a caller pairing these two needs no try at all.
    void LoadPlansSafe(const TString& plans, bool simplified = false);
    void LoadPlansSafe(const NJson::TJsonValue& root);

    // PrintSvg throws on a plan it cannot render, and on a load that failed
    // earlier; PrintSvgSafe returns an SVG carrying the error message instead.
    TString PrintSvg();
    TString PrintSvgSafe();

    // The message of the failure recorded by the latest LoadPlansSafe, empty
    // when it succeeded. For callers that report the failure through their own
    // channel - an issue, a log line - instead of, or besides, the picture.
    const TString& GetLoadError() const;

private:
    std::unique_ptr<TVisualizer> Impl;
};

} // namespace NPlan2Svg
