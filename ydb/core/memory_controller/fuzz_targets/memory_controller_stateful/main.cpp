#include <ydb/core/memory_controller/memtable_collection.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace {

using namespace NKikimr;
using namespace NKikimr::NMemory;

constexpr size_t MaxConsumers = 24;
constexpr size_t MaxOperations = 512;

class TFuzzMemoryConsumer final : public IMemoryConsumer {
public:
    void SetConsumption(ui64 value) override {
        Consumption = value;
    }

    ui64 GetConsumption() const {
        return Consumption;
    }

private:
    std::atomic<ui64> Consumption = 0;
};

struct TConsumerSlot {
    TActorId Owner;
    ui32 Table = 0;
    TIntrusivePtr<TMemTableMemoryConsumer> Consumer;
    ui64 Consumption = 0;
    bool Compacting = false;
};

TActorId MakeOwner(FuzzedDataProvider& fdp) {
    return TActorId(1, 1, fdp.ConsumeIntegralInRange<ui64>(1, 64), 1);
}

TConsumerSlot& PickConsumer(TVector<TConsumerSlot>& consumers, FuzzedDataProvider& fdp) {
    return consumers[fdp.ConsumeIntegralInRange<size_t>(0, consumers.size() - 1)];
}

ui64 SumConsumption(const TVector<TConsumerSlot>& consumers) {
    ui64 result = 0;
    for (const auto& slot : consumers) {
        result += slot.Consumption;
    }
    return result;
}

ui64 SumCompacting(const TVector<TConsumerSlot>& consumers) {
    ui64 result = 0;
    for (const auto& slot : consumers) {
        if (slot.Compacting) {
            result += slot.Consumption;
        }
    }
    return result;
}

void CheckState(
        const std::shared_ptr<TMemTableMemoryConsumersCollection>& collection,
        const TIntrusivePtr<TFuzzMemoryConsumer>& collectionConsumer,
        const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
        const TVector<TConsumerSlot>& consumers) {
    const ui64 total = SumConsumption(consumers);
    const ui64 compacting = SumCompacting(consumers);
    Y_ABORT_UNLESS(collection->GetTotalConsumption() == total);
    Y_ABORT_UNLESS(collection->GetTotalCompacting() == compacting);
    Y_ABORT_UNLESS(collectionConsumer->GetConsumption() == total);
    Y_ABORT_UNLESS(counters->GetCounter("Consumer/MemTable/Total")->Val() == static_cast<i64>(total));
    Y_ABORT_UNLESS(counters->GetCounter("Consumer/MemTable/Compacting")->Val() == static_cast<i64>(compacting));
}

void RunMemoryCollectionFuzz(FuzzedDataProvider& fdp) {
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto collectionConsumer = MakeIntrusive<TFuzzMemoryConsumer>();
    auto collection = std::make_shared<TMemTableMemoryConsumersCollection>(counters, collectionConsumer);
    TVector<TConsumerSlot> consumers;

    for (size_t step = 0; step < MaxOperations && fdp.remaining_bytes(); ++step) {
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 6)) {
            case 0: {
                if (consumers.size() >= MaxConsumers) {
                    break;
                }
                const TActorId owner = MakeOwner(fdp);
                const ui32 table = fdp.ConsumeIntegralInRange<ui32>(1, 32);
                auto it = std::find_if(consumers.begin(), consumers.end(), [&](const TConsumerSlot& slot) {
                    return slot.Owner == owner && slot.Table == table;
                });
                auto consumer = collection->Register(owner, table);
                if (it == consumers.end()) {
                    consumers.push_back({owner, table, consumer, 0, false});
                } else {
                    Y_ABORT_UNLESS(it->Consumer == consumer);
                }
                break;
            }

            case 1: {
                if (consumers.empty()) {
                    break;
                }
                const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, consumers.size() - 1);
                TConsumerSlot slot = consumers[index];
                collection->Unregister(slot.Owner, slot.Table);
                consumers.erase(consumers.begin() + index);
                break;
            }

            case 2: {
                if (consumers.empty()) {
                    break;
                }
                auto& slot = PickConsumer(consumers, fdp);
                if (!slot.Compacting) {
                    slot.Consumption = fdp.ConsumeIntegralInRange<ui64>(0, 1 << 20);
                    slot.Consumer->SetConsumption(slot.Consumption);
                    Y_ABORT_UNLESS(slot.Consumer->GetConsumption() == slot.Consumption);
                }
                break;
            }

            case 3: {
                const ui64 limit = fdp.ConsumeIntegralInRange<ui64>(0, 2 << 20);
                auto selected = collection->SelectForCompaction(limit);
                ui64 last = Max<ui64>();
                for (const auto& [consumer, consumption] : selected) {
                    Y_ABORT_UNLESS(consumption <= last);
                    last = consumption;
                    auto it = std::find_if(consumers.begin(), consumers.end(), [&](const TConsumerSlot& slot) {
                        return slot.Consumer == consumer;
                    });
                    Y_ABORT_UNLESS(it != consumers.end());
                    Y_ABORT_UNLESS(!it->Compacting);
                    Y_ABORT_UNLESS(it->Consumption == consumption);
                    Y_ABORT_UNLESS(consumption > 0);
                    it->Compacting = true;
                }
                break;
            }

            case 4: {
                TVector<size_t> compacting;
                for (size_t i = 0; i < consumers.size(); ++i) {
                    if (consumers[i].Compacting) {
                        compacting.push_back(i);
                    }
                }
                if (!compacting.empty()) {
                    auto& slot = consumers[compacting[fdp.ConsumeIntegralInRange<size_t>(0, compacting.size() - 1)]];
                    const ui32 table = collection->CompactionComplete(slot.Consumer);
                    Y_ABORT_UNLESS(table == slot.Table);
                    slot.Compacting = false;
                }
                break;
            }

            case 5: {
                if (!consumers.empty()) {
                    auto& slot = PickConsumer(consumers, fdp);
                    const ui32 table = collection->CompactionComplete(slot.Consumer);
                    if (slot.Compacting) {
                        Y_ABORT_UNLESS(table == slot.Table);
                        slot.Compacting = false;
                    } else {
                        Y_ABORT_UNLESS(table == Max<ui32>());
                    }
                }
                break;
            }

            case 6:
                collection->SelectForCompaction(SumConsumption(consumers));
                break;
        }

        CheckState(collection, collectionConsumer, counters, consumers);
    }

    while (!consumers.empty()) {
        auto slot = consumers.back();
        consumers.pop_back();
        collection->Unregister(slot.Owner, slot.Table);
    }
    CheckState(collection, collectionConsumer, counters, consumers);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    RunMemoryCollectionFuzz(fdp);
    return 0;
}
