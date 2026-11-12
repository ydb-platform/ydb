#pragma once

#include "config.h"

#include <util/datetime/base.h>

#include <array>
#include <atomic>
#include <functional>

namespace NYdbWorkload::NScheme {

enum class EObjectState { Missing, WithoutBatch, WithBatch };

// Methods throw on failure. In particular, an uncertain DDL outcome is not retried.
class IClient {
public:
    virtual ~IClient() = default;
    // Ensure the directory exists and refresh the inventory used by Inspect.
    virtual void PrepareDirectory(const TString& path) = 0;
    virtual void Create(const TObject& object) = 0;
    virtual EObjectState Inspect(const TObject& object) = 0;
    virtual void Alter(const TObject& object, bool add) = 0;
};

enum class EOperation { CreateTable, CreateTopic, AddColumns, DropColumns, AddConsumers, DropConsumers, Count };
constexpr size_t OperationCount = static_cast<size_t>(EOperation::Count);
const char* OperationName(EOperation operation);

class TRunner {
public:
    using TCancelled = std::function<bool()>;
    TRunner(const TConfig& config, IClient& client, TCancelled cancelled = [] { return false; });

    void Init();
    void Run(TDuration duration);
    std::array<ui64, OperationCount> Statistics() const;

private:
    void Execute(ui64 concurrency, ui64 objects, const std::function<void(ui32, ui32, const TCancelled&)>& worker);
    void Count(EOperation operation);

    const TConfig& Config;
    IClient& Client;
    TCancelled Cancelled;
    std::array<std::atomic<ui64>, OperationCount> Counters{};
};

} // namespace NYdbWorkload::NScheme
