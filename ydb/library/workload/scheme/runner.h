#pragma once

#include "config.h"

#include <util/datetime/base.h>

#include <array>
#include <atomic>
#include <functional>

namespace NYdbWorkload::NScheme {

// Methods throw on failure. In particular, an uncertain DDL outcome is not retried.
class IClient {
public:
    virtual ~IClient() = default;
    virtual void MakeDirectory(const TString& path) = 0;
    virtual void Create(const TObject& object) = 0;
    virtual bool Inspect(const TObject& object) = 0; // Whether the complete ALTER batch is present.
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
    void Execute(ui32 concurrency, const std::function<void(ui32, ui32, const TCancelled&)>& worker);
    void Count(EOperation operation);

    const TConfig& Config;
    IClient& Client;
    TCancelled Cancelled;
    std::array<std::atomic<ui64>, OperationCount> Counters{};
};

} // namespace NYdbWorkload::NScheme
