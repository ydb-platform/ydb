#include <library/cpp/testing/unittest/registar.h>
#include "blobstorage_groupinfo_partlayout.h"
#include <thread>
#include <mutex>
#include <condition_variable>

using namespace NKikimr;

ui32 CountEffectiveReplicas(const TSubgroupPartLayout& layout, ui32 numRows, ui32 numCols) {
    // number of effective replicas is calculated as the maximum number of rows that can be mapped to unique columns in
    // layout with bit at intersecting cell set to zero; here we state that numCols >= numRows
    UNIT_ASSERT(numCols >= numRows);
    std::vector<ui32> index(numCols);
    for (size_t i = 0; i < index.size(); ++i) {
        index[i] = i;
    }
    ui32 res = 0;
    do {
        ui32 value = 0;
        for (ui32 row = 0; row < numRows; ++row) {
            // extract the cell at row, col and count it if it is nonzero
            value += layout.GetDisksWithPart(row) >> index[row] & 1;
        }
        if (value > res) {
            res = value;
        }
    } while (std::next_permutation(index.begin(), index.end()));
    return res;
}

class TCheckQueue {
    std::mutex Mutex;
    std::list<std::thread> Threads;
    std::deque<TSubgroupPartLayout> Queue;
    std::condition_variable Cvar, ReverseCvar;
    volatile bool Done = false;
    std::atomic_size_t CasesChecked = 0;
    std::atomic_uint64_t Timing = 0;

public:
    TCheckQueue(TBlobStorageGroupType gtype) {
        for (int n = std::thread::hardware_concurrency(); n; --n) {
            Threads.emplace_back([=] {
                for (;;) {
                    TSubgroupPartLayout layout;
                    {
                        std::unique_lock<std::mutex> lock(Mutex);
                        Cvar.wait(lock, [&] { return Done || !Queue.empty(); });
                        if (Queue.empty() && Done) {
                            return;
                        }
                        Y_ABORT_UNLESS(!Queue.empty());
                        layout = std::move(Queue.front());
                        Queue.pop_front();
                        if (Queue.size() < 1024) {
                            ReverseCvar.notify_one();
                        }
                    }

                    // count effective replicas
                    THPTimer timer;
                    ui32 count = layout.CountEffectiveReplicas(gtype);
                    Timing += 1e9 * timer.Passed();

                    // compare it with generic value
                    ui32 generic = CountEffectiveReplicas(layout, gtype.TotalPartCount(), gtype.BlobSubgroupSize());

                    // verify the value
                    Y_ABORT_UNLESS(count == generic, "count# %" PRIu32 " generic# %" PRIu32 " layout# %s erasure# %s",
                        count, generic, layout.ToString(gtype).data(),
                        TBlobStorageGroupType::ErasureSpeciesName(gtype.GetErasure()).data());

                    ++CasesChecked;
                }
            });
        }
    }

    ~TCheckQueue() {
        {
            std::unique_lock<std::mutex> lock(Mutex);
            Done = true;
        }
        Cvar.notify_all();
        for (auto& thread : Threads) {
            thread.join();
        }
        Cerr << "Checked " << (size_t)CasesChecked << " cases, took " << (ui64)Timing / 1000 << " us" << Endl;
    }

    void operator ()(const TSubgroupPartLayout& layout) {
        std::unique_lock<std::mutex> lock(Mutex);
        ReverseCvar.wait(lock, [&] { return Queue.size() < 1024; });
        Queue.push_back(layout);
        Cvar.notify_one();
    }
};

void TestErasureSet(ui32 firstIdx, ui32 step) {
    for (ui32 erasure = firstIdx; erasure < TBlobStorageGroupType::ErasureSpeciesCount; erasure += step) {
        if (erasure == TBlobStorageGroupType::ErasureMirror3dc || erasure == TBlobStorageGroupType::ErasureMirror3of4) {
            continue;
        }
        TBlobStorageGroupType gtype(static_cast<TBlobStorageGroupType::EErasureSpecies>(erasure));
        if (gtype.BlobSubgroupSize() > 8) {
            continue;
        }
        Cerr << "testing erasure " << TBlobStorageGroupType::ErasureSpeciesName(erasure) << Endl;
        const ui32 totalPartCount = gtype.TotalPartCount();
        const ui32 blobSubgroupSize = gtype.BlobSubgroupSize();
        TCheckQueue checker(gtype);
        for (ui32 main = 0; main < 1 << totalPartCount; ++main) {
            Cerr << "  main# " << main << Endl;
            std::vector<ui32> handoffs(gtype.Handoff(), 0);
            for (;;) {
                // generate subgroup layout
                TSubgroupPartLayout layout;
                for (ui32 nodeId = 0; nodeId < blobSubgroupSize; ++nodeId) {
                    if (nodeId < totalPartCount) {
                        if (main & 1 << nodeId) {
                            layout.AddItem(nodeId, nodeId, gtype);
                        }
                    } else {
                        for (ui32 i = 0; i < totalPartCount; ++i) {
                            if (handoffs[nodeId - totalPartCount] & 1 << i) {
                                layout.AddItem(nodeId, i, gtype);
                            }
                        }
                    }
                }

                checker(layout);

                // increment handoffs
                ui32 carry = 1;
                for (size_t i = 0; carry && i < handoffs.size(); ++i) {
                    carry = !(++handoffs[i] & (1 << totalPartCount) - 1);
                }
                if (carry) {
                    break;
                }
            }
        }
    }
}

Y_UNIT_TEST_SUITE(TSubgroupPartLayoutTest) {
    Y_UNIT_TEST(CountEffectiveReplicas1of4) {
        TestErasureSet(0, 4);
    }
    Y_UNIT_TEST(CountEffectiveReplicas2of4) {
        TestErasureSet(1, 4);
    }
    Y_UNIT_TEST(CountEffectiveReplicas3of4) {
        TestErasureSet(2, 4);
    }
    Y_UNIT_TEST(CountEffectiveReplicas4of4) {
        TestErasureSet(3, 4);
    }
}
