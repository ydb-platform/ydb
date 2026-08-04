#include <ydb/core/util/concurrent_rw_hash.h>

#include <util/system/event.h>
#include <util/system/thread.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

constexpr ui32 KeyCount = 32;
constexpr ui32 BucketCount = 8;
constexpr size_t MaxThreads = 4;
constexpr size_t MaxSteps = 48;

using TMap = TConcurrentRWHashMap<ui32, ui32, BucketCount>;

class TInput {
public:
    TInput(const ui8* data, size_t size)
        : Data_(data)
        , Size_(size)
    {
    }

    bool Empty() const {
        return Pos_ >= Size_;
    }

    ui8 Byte() {
        if (Empty()) {
            return 0;
        }
        return Data_[Pos_++];
    }

    ui32 Int() {
        ui32 value = 0;
        for (int i = 0; i < 4; ++i) {
            value = (value << 8) | Byte();
        }
        return value;
    }

private:
    const ui8* Data_;
    size_t Size_;
    size_t Pos_ = 0;
};

struct TOp {
    ui8 Kind = 0;
    ui32 Key = 1;
    ui32 Value = 0;
};

struct TScript {
    std::vector<TOp> Ops;
};

class TStartLatch {
public:
    explicit TStartLatch(size_t count)
        : Left_(count)
    {
    }

    void Wait() {
        if (Left_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            Event_.Signal();
        }
        Event_.Wait();
    }

private:
    std::atomic<size_t> Left_;
    TSystemEvent Event_;
};

ui32 Key(TInput& input) {
    return 1 + input.Byte() % KeyCount;
}

ui32 EncodedValue(size_t thread, size_t step, ui32 raw) {
    return static_cast<ui32>((thread + 1) << 24) ^ static_cast<ui32>(step << 12) ^ raw;
}

std::vector<TScript> DecodeScripts(TInput& input, size_t threads, size_t maxSteps) {
    std::vector<TScript> scripts(threads);
    for (size_t thread = 0; thread < threads; ++thread) {
        const size_t steps = 1 + (input.Byte() % maxSteps);
        scripts[thread].Ops.reserve(steps);
        for (size_t step = 0; step < steps; ++step) {
            scripts[thread].Ops.push_back(TOp{
                .Kind = input.Byte(),
                .Key = Key(input),
                .Value = EncodedValue(thread, step, input.Int()),
            });
        }
    }
    return scripts;
}

template <class TCallable>
void RunThreads(size_t threads, TCallable&& callable) {
    TStartLatch latch(threads);
    std::vector<std::unique_ptr<TThread>> workers;
    workers.reserve(threads);

    for (size_t thread = 0; thread < threads; ++thread) {
        workers.push_back(std::make_unique<TThread>([&, thread] {
            latch.Wait();
            callable(thread);
        }));
    }

    for (auto& worker : workers) {
        worker->Start();
    }
    for (auto& worker : workers) {
        worker->Join();
    }
}

std::unordered_map<ui32, ui32> Snapshot(const TMap& map) {
    std::unordered_map<ui32, ui32> snapshot;
    for (const auto& bucket : map.Buckets) {
        TReadGuard guard(bucket.GetLock());
        for (const auto& [key, value] : bucket.GetMap()) {
            Y_ABORT_UNLESS(key >= 1 && key <= KeyCount);
            Y_ABORT_UNLESS(snapshot.emplace(key, value).second);
        }
    }
    return snapshot;
}

void CheckGetHas(const TMap& map, const std::unordered_map<ui32, ui32>& snapshot) {
    for (ui32 key = 1; key <= KeyCount; ++key) {
        const auto it = snapshot.find(key);
        const bool expected = it != snapshot.end();
        Y_ABORT_UNLESS(map.Has(key) == expected);

        ui32 value = 0xA5A5A5A5u;
        Y_ABORT_UNLESS(map.Get(key, value) == expected);
        if (expected) {
            Y_ABORT_UNLESS(value == it->second);
            Y_ABORT_UNLESS(map.Get(key) == it->second);
        } else {
            Y_ABORT_UNLESS(value == 0xA5A5A5A5u);
        }
    }
}

void RunInsertIfAbsentRace(const std::vector<TScript>& scripts, size_t threads) {
    TMap map;
    std::array<std::atomic<ui32>, KeyCount + 1> initCounts;
    std::array<ui32, KeyCount + 1> attempts = {};

    for (auto& count : initCounts) {
        count.store(0, std::memory_order_relaxed);
    }
    for (const auto& script : scripts) {
        for (const auto& op : script.Ops) {
            ++attempts[op.Key];
        }
    }

    RunThreads(threads, [&] (size_t thread) {
        for (const auto& op : scripts[thread].Ops) {
            const ui32 value = map.InsertIfAbsentWithInit(op.Key, [&] {
                initCounts[op.Key].fetch_add(1, std::memory_order_relaxed);
                return op.Value;
            });
            ui32 readValue = 0;
            Y_ABORT_UNLESS(map.Get(op.Key, readValue));
            Y_ABORT_UNLESS(value == readValue);
        }
    });

    const auto snapshot = Snapshot(map);
    CheckGetHas(map, snapshot);
    for (ui32 key = 1; key <= KeyCount; ++key) {
        if (attempts[key]) {
            Y_ABORT_UNLESS(snapshot.contains(key));
            Y_ABORT_UNLESS(initCounts[key].load(std::memory_order_relaxed) == 1);
        } else {
            Y_ABORT_UNLESS(!snapshot.contains(key));
            Y_ABORT_UNLESS(initCounts[key].load(std::memory_order_relaxed) == 0);
        }
    }
}

void RunMixedRace(const std::vector<TScript>& scripts, size_t threads) {
    TMap map;
    std::array<std::unordered_set<ui32>, KeyCount + 1> possibleValues;

    for (ui32 key = 1; key <= KeyCount; ++key) {
        const ui32 initial = 0xD0000000u | key;
        map.Insert(key, initial);
        possibleValues[key].insert(initial);
    }
    for (const auto& script : scripts) {
        for (const auto& op : script.Ops) {
            possibleValues[op.Key].insert(op.Value);
        }
    }

    RunThreads(threads, [&] (size_t thread) {
        for (const auto& op : scripts[thread].Ops) {
            switch (op.Kind % 7) {
                case 0:
                    map.Insert(op.Key, op.Value);
                    break;
                case 1: {
                    ui32 previous = 0;
                    if (map.Swap(op.Key, op.Value, previous)) {
                        Y_ABORT_UNLESS(possibleValues[op.Key].contains(previous));
                    }
                    break;
                }
                case 2:
                    Y_ABORT_UNLESS(possibleValues[op.Key].contains(map.InsertIfAbsent(op.Key, op.Value)));
                    break;
                case 3:
                    map.InsertIfAbsentWithInit(op.Key, [&] {
                        return op.Value;
                    });
                    break;
                case 4:
                    map.Erase(op.Key);
                    break;
                case 5: {
                    ui32 value = 0;
                    if (map.Get(op.Key, value)) {
                        Y_ABORT_UNLESS(possibleValues[op.Key].contains(value));
                    }
                    break;
                }
                case 6:
                    map.Has(op.Key);
                    break;
            }
        }
    });

    const auto snapshot = Snapshot(map);
    CheckGetHas(map, snapshot);
    for (const auto& [key, value] : snapshot) {
        Y_ABORT_UNLESS(possibleValues[key].contains(value));
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TInput input(data, size);
    if (input.Empty()) {
        return 0;
    }

    const size_t threads = 2 + (input.Byte() % (MaxThreads - 1));
    const size_t maxSteps = 1 + (input.Byte() % MaxSteps);
    const auto scripts = DecodeScripts(input, threads, maxSteps);

    RunInsertIfAbsentRace(scripts, threads);
    RunMixedRace(scripts, threads);

    return 0;
}
