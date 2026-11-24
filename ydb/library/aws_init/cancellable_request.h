#pragma once

#include <aws/core/http/HttpRequest.h>

namespace NKikimr {

// Opt a request into cancellation before transmission and while its connection is idle.
struct TCancellableAwsRequest {
    Aws::Http::ContinueRequestHandler CanContinue;

    bool operator()(const Aws::Http::HttpRequest* request) const {
        return CanContinue(request);
    }
};

} // namespace NKikimr
