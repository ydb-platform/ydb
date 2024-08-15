#include <ydb/core/base/memory_controller_iface.h>

namespace NKikimr::NMemory {

using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

class TMemTableMemoryConsumersCollection;

class TMemTableMemoryConsumer : public IMemoryConsumer {
public:
   TMemTableMemoryConsumer(TActorId owner, ui32 table, std::weak_ptr<TMemTableMemoryConsumersCollection> collection);

    ui64 GetConsumption() const;

    void SetConsumption(ui64 consumption);

private:
    const std::weak_ptr<TMemTableMemoryConsumersCollection> Collection;
    std::atomic<ui64> Consumption = 0;

public:
    const TActorId Owner;
    const ui32 Table;
};

class TMemTableMemoryConsumersCollection : public std::enable_shared_from_this<TMemTableMemoryConsumersCollection> {
    friend TMemTableMemoryConsumer;

public:
    TMemTableMemoryConsumersCollection(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TIntrusivePtr<NMemory::IMemoryConsumer> memoryConsumer);

    TIntrusivePtr<TMemTableMemoryConsumer> Register(TActorId owner, ui32 table);

    void Unregister(TActorId owner, ui32 table);

    ui32 CompactionComplete(TIntrusivePtr<TMemTableMemoryConsumer> consumer);

    /**
     * @return consumers and their consumptions that should be compacted
     */
    TVector<std::pair<TIntrusivePtr<TMemTableMemoryConsumer>, ui64>> SelectForCompaction(ui64 limitBytes);

    ui64 GetTotalConsumption() const;

    ui64 GetTotalCompacting() const;

private:
    void ChangeTotalConsumption(ui64 delta);

    void ChangeTotalCompacting(ui64 delta);

private:
    const TCounterPtr MemTableTotalBytesCounter, MemTableCompactingBytesCounter, MemTableCompactedBytesCounter;
    TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;
    TMap<std::pair<TActorId, ui32>, TIntrusivePtr<TMemTableMemoryConsumer>> Consumers;
    THashSet<TIntrusivePtr<TMemTableMemoryConsumer>> NonCompacting;
    THashMap<TIntrusivePtr<TMemTableMemoryConsumer>, ui64> Compacting;
    std::atomic<ui64> TotalConsumption = 0;

    // Approximate value, updates only on compaction start/stop
    // Counts only self triggered compactions
    ui64 TotalCompacting = 0;
};

}