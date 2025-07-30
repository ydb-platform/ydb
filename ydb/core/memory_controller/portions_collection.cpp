#include "portions_collection.h"

namespace NKikimr::NMemory {

TPortionsMemoryConsumer::TPortionsMemoryConsumer(TActorId owner, std::weak_ptr<TPortionsMemoryConsumersCollection> collection)
    : Collection(std::move(collection))
    , Owner(owner)
{}

ui64 TPortionsMemoryConsumer::GetConsumption() const {
    return Consumption;
}

void TPortionsMemoryConsumer::SetConsumption(ui64 consumption) {
    ui64 before = Consumption.exchange(consumption);
    if (auto t = Collection.lock()) {
        t->ChangeTotalConsumption(consumption - before);
    }
}


TPortionsMemoryConsumersCollection::TPortionsMemoryConsumersCollection(TIntrusivePtr<NMemory::IMemoryConsumer> memoryConsumer)
    : MemoryConsumer(std::move(memoryConsumer))
{}

TIntrusivePtr<TPortionsMemoryConsumer> TPortionsMemoryConsumersCollection::Register(TActorId owner) {
    auto &ptr = Consumers[owner];
    if (!ptr) {
        ptr = MakeIntrusive<TPortionsMemoryConsumer>(owner, weak_from_this());
    }
    return ptr;
}

void TPortionsMemoryConsumersCollection::Unregister(TActorId owner) {
    auto it = Consumers.find(owner);
    if (it != Consumers.end()) {
        auto& consumer = it->second;
        consumer->SetConsumption(0);
        Consumers.erase(it);
    }
}

TVector<TIntrusivePtr<TPortionsMemoryConsumer>> TPortionsMemoryConsumersCollection::GetRegisteredConsumers() const {
    TVector<TIntrusivePtr<TPortionsMemoryConsumer>> result;
    result.reserve(Consumers.size());
    for (const auto& consumer : Consumers) {
        result.push_back(consumer.second);
    }

    return result;
}

ui64 TPortionsMemoryConsumersCollection::GetTotalConsumption() const {
    return TotalConsumption;
}

void TPortionsMemoryConsumersCollection::ChangeTotalConsumption(ui64 delta) {
    ui64 consumption = TotalConsumption.fetch_add(delta) + delta;
    MemoryConsumer->SetConsumption(consumption);
}

}