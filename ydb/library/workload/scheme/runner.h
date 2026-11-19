#pragma once

#include "config.h"

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>

#include <array>
#include <functional>

namespace NYdbWorkload::NScheme {

enum class EObjectState { Missing, WithoutBatch, WithBatch };

inline constexpr ui32 DEFAULT_WORKLOAD_THREADS = 10;
inline constexpr ui32 MAX_WORKLOAD_THREADS = 256;

// Errors may be thrown directly or stored in the future. Uncertain DDL is not retried.
class IClient {
public:
    virtual ~IClient() = default;
    // Ensure the directory exists and refresh the inventory used by Inspect.
    virtual void PrepareDirectory(const TString& path) = 0;
    virtual NThreading::TFuture<void> Create(const TObject& object) = 0;
    virtual NThreading::TFuture<EObjectState> Inspect(const TObject& object) = 0;
    virtual NThreading::TFuture<void> Alter(const TObject& object, bool add) = 0;
};

enum class EOperation { CreateTable, CreateTopic, AddColumns, DropColumns, AddConsumers, DropConsumers, Count };
constexpr size_t OperationCount = static_cast<size_t>(EOperation::Count);
const char* OperationName(EOperation operation);

class TRunner {
public:
    using TCancelled = std::function<bool()>;
    TRunner(const TConfig& config, IClient& client, TCancelled cancelled = [] { return false; },
        ui32 threads = DEFAULT_WORKLOAD_THREADS);

    void Init();
    void Run(TDuration duration);
    std::array<ui64, OperationCount> Statistics() const;

private:
    void Count(EOperation operation);

private:
    const TConfig& Config_;
    IClient& Client_;
    TCancelled Cancelled_;
    const ui32 Threads_;
    std::array<ui64, OperationCount> Counters_{};
};

} // namespace NYdbWorkload::NScheme
