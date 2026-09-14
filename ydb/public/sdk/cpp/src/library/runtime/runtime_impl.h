#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <cstddef>

namespace NYdb::inline Dev::NRuntime {

// Only the immortal runtime owns these pools; ordinary client pools are unchanged.
IExecutor::TPtr CreateExecutor(std::size_t threadCount, std::size_t maxQueueSize = 0);

// Network threads are configured on first use, which may precede driver creation.
NYdbGrpc::TGRpcClientLow& GetNetwork(std::size_t threadCount = NYdbGrpc::DEFAULT_NUM_THREADS);

} // namespace NYdb::NRuntime
