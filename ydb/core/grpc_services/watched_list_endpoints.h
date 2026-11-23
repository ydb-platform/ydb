#pragma once

#include "rpc_calls.h"

namespace NKikimr::NGRpcService {

// The EDS stream uses the regular ListEndpoints request path for each snapshot.
// The lookup reports the exact board revision (including serverless resolution)
// so the subsequent wait cannot miss a concurrent change.
class TWatchedListEndpointsRequest : public TEvListEndpointsRequest {
public:
    using TEvListEndpointsRequest::TEvListEndpointsRequest;
    virtual void OnSnapshot(const TActorId& cache, const TString& path, ui64 revision) = 0;
};

}
