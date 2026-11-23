#pragma once

#include <ydb/public/api/protos/ydb_discovery.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKvVolumeTool {

struct TEndpointCandidate {
    TString HostPort;
    TString SslTargetNameOverride;
};

TVector<TEndpointCandidate> SelectEndpoints(const Ydb::Discovery::EndpointInfo& info, bool useTls);

} // namespace NKvVolumeTool
