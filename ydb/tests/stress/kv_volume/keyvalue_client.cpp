#include "keyvalue_client.h"

#include "grpc_async_executor.h"
#include "keyvalue_client_v1.h"
#include "keyvalue_client_v2.h"
#include "types.h"

#include <util/string/builder.h>

#include <algorithm>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace NKvVolumeStress {

namespace {

std::mutex GrpcExecutorMutex;
std::weak_ptr<TGrpcAsyncExecutor> SharedGrpcExecutor;

ui32 ResolveGrpcCqThreads(ui32 configuredThreads) {
    if (configuredThreads > 0) {
        return configuredThreads;
    }

    const ui32 hardwareThreads = std::max<ui32>(1, std::thread::hardware_concurrency());
    return hardwareThreads;
}

std::shared_ptr<TGrpcAsyncExecutor> GetSharedGrpcExecutor(ui32 configuredThreads) {
    const ui32 grpcCqThreads = ResolveGrpcCqThreads(configuredThreads);

    std::lock_guard lock(GrpcExecutorMutex);
    if (auto executor = SharedGrpcExecutor.lock()) {
        if (executor->ThreadCount() != grpcCqThreads) {
            throw std::runtime_error(TStringBuilder()
                << "gRPC async executor already initialized with "
                << executor->ThreadCount()
                << " CQ thread(s), requested "
                << grpcCqThreads
                << " thread(s)");
        }
        return executor;
    }

    auto executor = std::make_shared<TGrpcAsyncExecutor>(grpcCqThreads);
    SharedGrpcExecutor = executor;
    return executor;
}

} // namespace

std::unique_ptr<IKeyValueClient> MakeKeyValueClient(const TString& hostPort, const TOptions& options) {
    auto executor = GetSharedGrpcExecutor(options.GrpcCqThreads);

    if (options.Version == "v1") {
        return std::make_unique<TKeyValueClientV1>(hostPort, std::move(executor));
    }

    if (options.Version == "v2") {
        return std::make_unique<TKeyValueClientV2>(hostPort, std::move(executor));
    }

    throw std::runtime_error(TStringBuilder() << "unsupported --version: " << options.Version);
}

} // namespace NKvVolumeStress
