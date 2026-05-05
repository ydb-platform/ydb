#pragma once

#include <util/generic/string.h>

namespace grpc {
class ServerContext;
}  // namespace grpc

namespace NTestUtils {

TString CaptureXUserIP(grpc::ServerContext* ctx);

}  // namespace NTestUtils
