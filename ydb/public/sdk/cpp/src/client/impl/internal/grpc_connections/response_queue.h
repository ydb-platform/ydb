#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include "params.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>


namespace NYdb::inline Dev {

class IResponseQueue {
public:
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual void Post(IObjectInQueue* action) = 0;

    virtual ~IResponseQueue() = default;
};

std::unique_ptr<IResponseQueue> CreateResponseQueue(std::shared_ptr<IConnectionsParams> params);

} // namespace NYdb
