#include "optimizer_trace_assets.h"

#include <library/cpp/resource/resource.h>

#include <util/generic/string.h>

namespace optimizer_trace_assets {
namespace {

constexpr char kCssResource[] = "/ydb/core/kqp/opt/rbo/optimizer_trace.css";
constexpr char kJsResource[] = "/ydb/core/kqp/opt/rbo/optimizer_trace.js";
constexpr char kBrotliDecoderJsResource[] = "/ydb/core/kqp/opt/rbo/brotli_decoder.js";
constexpr char kBrotliDecoderWasmResource[] = "/ydb/core/kqp/opt/rbo/brotli_dec_wasm_bg.wasm";
constexpr char kBrotliDecoderLicenseResource[] = "/ydb/core/kqp/opt/rbo/brotli_decoder_LICENSE-MIT";

std::string_view AsView(const TString& data) {
    return {data.data(), data.size()};
}

}  // namespace

std::string_view css() {
    static const TString data = NResource::Find(kCssResource);
    return AsView(data);
}

std::string_view js() {
    static const TString data = NResource::Find(kJsResource);
    return AsView(data);
}

std::array<std::string_view, 1> jsParts() {
    return {{js()}};
}

std::string_view brotliDecoderJs() {
    static const TString data = NResource::Find(kBrotliDecoderJsResource);
    return AsView(data);
}

std::string_view brotliDecoderWasm() {
    static const TString data = NResource::Find(kBrotliDecoderWasmResource);
    return AsView(data);
}

std::string_view brotliDecoderLicense() {
    static const TString data = NResource::Find(kBrotliDecoderLicenseResource);
    return AsView(data);
}

}  // namespace optimizer_trace_assets
