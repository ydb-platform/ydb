#include "common.h"

#include <util/generic/strbuf.h>

#include <grpcpp/server_context.h>

namespace NTestUtils {

namespace {

TString CaptureMetadata(grpc::ServerContext* ctx, TStringBuf name) {
    const auto& metadata = ctx->client_metadata();
    const auto range = metadata.equal_range(grpc::string_ref(name.data(), name.size()));
    TString value;
    for (auto it = range.first; it != range.second; ++it) {
        value = TString(it->second.cbegin(), it->second.cend());
    }
    return value;
}

} // namespace

TString CaptureXUserIP(grpc::ServerContext* ctx) {
    return CaptureMetadata(ctx, "x-user-ip");
}

TString CaptureUserAgent(grpc::ServerContext* ctx) {
    return CaptureMetadata(ctx, "user-agent");
}

}  // namespace NTestUtils
