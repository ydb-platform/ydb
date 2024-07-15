#pragma once


#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NMemory {

struct TProcessMemoryInfo {
    ui64 AllocatedMemory;
    std::optional<ui64> AnonRss;
    std::optional<ui64> CGroupLimit;
};

struct IProcessMemoryInfoProvider : public TThrRefBase {
    virtual TProcessMemoryInfo Get() const = 0;
};

struct TProcessMemoryInfoProvider : public IProcessMemoryInfoProvider {
    TProcessMemoryInfo Get() const override;
};

enum class EMemoryConsumerKind {
    SharedCache,
    MemTable,
};

class TMemoryConsumer : public IMemoryConsumer {
public:
    TMemoryConsumer(EMemoryConsumerKind kind)
        : Kind(kind) 
    {
    }

    EMemoryConsumerKind GetKind() const {
        return Kind;
    }

    ui64 GetLimit() const override {
        return Limit;
    }

    void SetLimit(ui64 value) { // TODO: use
        Limit = value;
    }

    ui64 GetConsumption() const {
        return Consumption;
    }

    void SetConsumption(ui64 value) override {
        Consumption = value;
    }

private:
    const EMemoryConsumerKind Kind;
    std::atomic<ui64> Consumption = 0;
    std::atomic<ui64> Limit = 0;
};

class TMemoryConsumersCollection : public TThrRefBase {
public:
    TIntrusivePtr<TMemoryConsumer> Register(EMemoryConsumerKind consumer) {
        TGuard<TMutex> guard(Mutex);

        auto inserted = Consumers.emplace(consumer, MakeIntrusive<TMemoryConsumer>(consumer));

        Y_ABORT_UNLESS(inserted.second, "Consumers should be unique");

        return inserted.first->second;
    }

    TVector<TIntrusivePtr<TMemoryConsumer>> GetConsumers() const {
        TGuard<TMutex> guard(Mutex);
        
        TVector<TIntrusivePtr<TMemoryConsumer>> result(::Reserve(Consumers.size()));

        // Note: don't care about destroyed consumers as that is possible only during process stopping

        for (const auto& consumer : Consumers) {
            result.emplace_back(consumer.second);
        }

        return result;
    }

private:
    TMutex Mutex;
    TMap<EMemoryConsumerKind, TIntrusivePtr<TMemoryConsumer>> Consumers;
};

NActors::IActor* CreateMemoryController(
    TDuration interval, 
    TIntrusivePtr<TMemoryConsumersCollection> consumersCollection,
    TIntrusiveConstPtr<IProcessMemoryInfoProvider> processMemoryInfoProvider,
    const NKikimrConfig::TMemoryControllerConfig& config, 
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

}