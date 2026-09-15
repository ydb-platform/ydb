#pragma once

#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr::NSqsTopic {
    template <class TRequest>
    static const TRequest& GetRequest(NGRpcService::IRequestOpCtx* ctx) {
        Y_ASSERT(ctx != nullptr);
        const auto* request = ctx->GetRequest();
        Y_ASSERT(request != nullptr);
        return dynamic_cast<const TRequest&>(*request);
    }
} // namespace NKikimr::NSqsTopic
