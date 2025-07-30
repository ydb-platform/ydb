#include <ydb/core/base/memory_controller_iface.h>

namespace NKikimr::NMemory {

using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

class TPortionsMemoryConsumersCollection;

class TPortionsMemoryConsumer: public IMemoryConsumer {
public:
    TPortionsMemoryConsumer(TActorId owner, std::weak_ptr<TPortionsMemoryConsumersCollection> collection);

    ui64 GetConsumption() const;

    void SetConsumption(ui64 consumption);

private:
    const std::weak_ptr<TPortionsMemoryConsumersCollection> Collection;
    std::atomic<ui64> Consumption = 0;

public:
    const TActorId Owner;
};

class TPortionsMemoryConsumersCollection: public std::enable_shared_from_this<TPortionsMemoryConsumersCollection> {
    friend TPortionsMemoryConsumer;

public:
    TPortionsMemoryConsumersCollection(TIntrusivePtr<NMemory::IMemoryConsumer> memoryConsumer);

    TIntrusivePtr<TPortionsMemoryConsumer> Register(TActorId owner);

    void Unregister(TActorId owner);

    TVector<TIntrusivePtr<TPortionsMemoryConsumer>> GetRegisteredConsumers() const;

    ui64 GetTotalConsumption() const;

private:
    void ChangeTotalConsumption(ui64 delta);

private:
    TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;
    TMap<TActorId, TIntrusivePtr<TPortionsMemoryConsumer>> Consumers;
    std::atomic<ui64> TotalConsumption = 0;
};

} // namespace NKikimr::NMemory