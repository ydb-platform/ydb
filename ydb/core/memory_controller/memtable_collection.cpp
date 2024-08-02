#include "memtable_collection.h"

namespace NKikimr::NMemory {

TMemTableMemoryConsumer::TMemTableMemoryConsumer(TActorId owner, ui32 table, std::weak_ptr<TMemTableMemoryConsumersCollection> collection)
    : Collection(std::move(collection))
    , Owner(owner)
    , Table(table)
{}

ui64 TMemTableMemoryConsumer::GetConsumption() const {
    return Consumption;
}

void TMemTableMemoryConsumer::SetConsumption(ui64 consumption) {
    ui64 before = Consumption.exchange(consumption);
    if (auto t = Collection.lock()) {
        t->ChangeTotalConsumption(consumption - before);
    }
}


TMemTableMemoryConsumersCollection::TMemTableMemoryConsumersCollection(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TIntrusivePtr<NMemory::IMemoryConsumer> memoryConsumer)
    : MemTableTotalBytesCounter(counters->GetCounter("Consumer/MemTable/Total"))
    , MemTableCompactingBytesCounter(counters->GetCounter("Consumer/MemTable/Compacting"))
    , MemTableCompactedBytesCounter(counters->GetCounter("Consumer/MemTable/Compacted", true))
    , MemoryConsumer(std::move(memoryConsumer))
{}

TIntrusivePtr<TMemTableMemoryConsumer> TMemTableMemoryConsumersCollection::Register(TActorId owner, ui32 table) {
    auto &ptr = Consumers[{owner, table}];
    if (!ptr) {
        ptr = MakeIntrusive<TMemTableMemoryConsumer>(owner, table, weak_from_this());
        NonCompacting.insert(ptr);
    }
    return ptr;
}

void TMemTableMemoryConsumersCollection::Unregister(TActorId owner, ui32 table) {
    auto it = Consumers.find({owner, table});
    if (it != Consumers.end()) {
        auto& consumer = it->second;
        CompactionComplete(consumer);
        consumer->SetConsumption(0);
        Y_DEBUG_ABORT_UNLESS(NonCompacting.contains(consumer));
        NonCompacting.erase(consumer);
        Consumers.erase(it);
    }
}

ui32 TMemTableMemoryConsumersCollection::CompactionComplete(TIntrusivePtr<TMemTableMemoryConsumer> consumer) {
    auto it = Compacting.find(consumer);
    if (it != Compacting.end()) {
        ui32 table = it->first->Table;
        MemTableCompactedBytesCounter->Add(it->second);
        ChangeTotalCompacting(-it->second);
        NonCompacting.insert(it->first);
        Compacting.erase(it);
        return table;
    }
    return Max<ui32>();
}

TVector<std::pair<TIntrusivePtr<TMemTableMemoryConsumer>, ui64>> TMemTableMemoryConsumersCollection::SelectForCompaction(ui64 limitBytes) {
    TVector<std::pair<TIntrusivePtr<TMemTableMemoryConsumer>, ui64>> result;

    ui64 toCompact = TotalConsumption > limitBytes ? TotalConsumption -  limitBytes : 0;
    if (toCompact <= TotalCompacting) {
        // nothing to compact more
        return result;
    }

    for (const auto &consumer : NonCompacting) {
        ui64 consumption = consumer->GetConsumption();
        if (consumption) {
            result.emplace_back(consumer, consumption);
        }
    }

    Sort(result, [](const auto &x, const auto &y) { return x.second > y.second; });

    size_t take = 0;
    for (auto it = result.begin(); it != result.end() && toCompact > TotalCompacting; it++) {
        auto consumer = it->first;

        Compacting[consumer] = it->second;
        Y_ABORT_UNLESS(NonCompacting.erase(consumer));

        ChangeTotalCompacting(it->second);

        take++;
    }

    result.resize(take);
    return result;
}

ui64 TMemTableMemoryConsumersCollection::GetTotalConsumption() const {
    return TotalConsumption;
}

ui64 TMemTableMemoryConsumersCollection::GetTotalCompacting() const {
    return TotalCompacting;
}

void TMemTableMemoryConsumersCollection::ChangeTotalConsumption(ui64 delta) {
    ui64 consumption = TotalConsumption.fetch_add(delta) + delta;
    MemoryConsumer->SetConsumption(consumption);
    MemTableTotalBytesCounter->Set(consumption);
}

void TMemTableMemoryConsumersCollection::ChangeTotalCompacting(ui64 delta) {
    ui64 compacting = (TotalCompacting += delta);
    MemTableCompactingBytesCounter->Set(compacting);
}

}