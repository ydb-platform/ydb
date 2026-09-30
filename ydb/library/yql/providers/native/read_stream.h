#pragma once

#include <ydb/library/yql/providers/native/operation_context.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <library/cpp/threading/future/future.h>
#include <arrow/record_batch.h>

#include <functional>
#include <memory>

namespace NYql::NNative {

// One result per pull. An empty successful result is a progress message, not EOF.
struct TReadResult {
    std::shared_ptr<arrow::RecordBatch> Batch;
    TString Error;
    bool Retryable = false;
    bool Finished = false;
    ui64 Bytes = 0;
};

struct TReadContext : TOperationContext {
    ui64 MaxBatchBytes = 0;
};

class IReadStream {
public:
    virtual ~IReadStream() = default;
    virtual NThreading::TFuture<TReadResult> Next() = 0;
    // Thread safe, idempotent; completes an outstanding Next locally, including
    // stream creation. Transport cancellation depends on the stream implementation.
    virtual void Cancel() = 0;
};

using TReadStreamFactory = std::function<std::shared_ptr<IReadStream>(const TReadContext&)>;

struct TReadActorSettings {
    TDuration Timeout;
    ui64 MaxBatchBytes = 0;
    ui32 MaxRetries = 0;
    TVector<TString> Columns;
};

std::pair<NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateNativeReadActor(
    TReadStreamFactory factory,
    TReadActorSettings settings,
    NDq::IDqAsyncIoFactory::TSourceArguments&& args);

} // namespace NYql::NNative
