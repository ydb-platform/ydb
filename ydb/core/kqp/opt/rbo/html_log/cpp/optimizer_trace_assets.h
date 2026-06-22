#pragma once

#include <array>
#include <string_view>

namespace optimizer_trace_assets {

std::string_view css();
std::string_view js();
std::array<std::string_view, 1> jsParts();
std::string_view brotliDecoderJs();
std::string_view brotliDecoderWasm();
std::string_view brotliDecoderLicense();

}  // namespace optimizer_trace_assets
