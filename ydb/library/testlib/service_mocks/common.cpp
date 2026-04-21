#include "common.h"

#include <grpcpp/server_context.h>

namespace NTestUtils {

TString CaptureXUserIP(grpc::ServerContext* ctx) {
    const auto& meta = ctx->client_metadata();
    const auto xUserIPHeaderRange = meta.equal_range("x-user-ip");
    TString xUserIP;
    for (auto it = xUserIPHeaderRange.first; it != xUserIPHeaderRange.second; ++it) {
        xUserIP = TString(it->second.cbegin(), it->second.cend());
    }
    return xUserIP;
}

}  // namespace NTestUtils