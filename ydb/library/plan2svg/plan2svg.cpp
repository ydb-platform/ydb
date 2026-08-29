#include "plan2svg.h"

#include "visualizer.h"

namespace NPlan2Svg {

TPlanVisualizer::TPlanVisualizer()
    : Impl(std::make_unique<TVisualizer>()) {
}

// Out of line so that plan2svg.h can hold a unique_ptr to an incomplete type.
TPlanVisualizer::~TPlanVisualizer() = default;

void TPlanVisualizer::LoadPlans(const TString& plans, bool simplified) {
    Impl->LoadPlans(plans, simplified);
}

void TPlanVisualizer::LoadPlans(const NJson::TJsonValue& root) {
    Impl->LoadPlans(root);
}

TString TPlanVisualizer::PrintSvg() {
    return Impl->PrintSvg();
}

TString TPlanVisualizer::PrintSvgSafe() {
    return Impl->PrintSvgSafe();
}

} // namespace NPlan2Svg
