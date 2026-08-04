#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugeheap.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/system/yassert.h>

namespace {

using namespace NKikimr;
using namespace NKikimr::NHuge;

constexpr ui32 MaxOps = 256;
constexpr ui32 ChunkSize = 134274560u;
constexpr ui32 AppendBlockSize = 56896u;
constexpr ui32 MinHugeBlobInBytes = 56u << 10u;
constexpr ui32 MilestoneBlobInBytes = 512u << 10u;
constexpr ui32 MaxBlobInBytes = 10u << 20u;
constexpr ui32 Overhead = 8;
constexpr ui32 StepsBetweenPowersOf2 = 6;

THolder<THeap> MakeHeap(bool useBucketsV2, ui32 freeChunksReservation, bool softLocking) {
    return MakeHolder<THeap>(
        "huge-heap-fuzz",
        ChunkSize,
        AppendBlockSize,
        MinHugeBlobInBytes,
        MilestoneBlobInBytes,
        MaxBlobInBytes,
        Overhead,
        StepsBetweenPowersOf2,
        useBucketsV2,
        freeChunksReservation,
        softLocking);
}

ui32 PickBlobSize(FuzzedDataProvider& provider) {
    static constexpr ui32 Sizes[] = {
        MinHugeBlobInBytes,
        64u << 10u,
        512u << 10u,
        1u << 20u,
        6u << 20u,
        MaxBlobInBytes,
    };
    return Sizes[provider.ConsumeIntegralInRange<size_t>(0, std::size(Sizes) - 1)];
}

bool ContainsSlot(const TVector<THugeSlot>& slots, const THugeSlot& slot) {
    return Find(slots.begin(), slots.end(), slot) != slots.end();
}

void CheckHeap(THeap& heap) {
    const THeapStat stat = heap.GetStat();
    Y_ABORT_UNLESS(stat.CurrentlyUsedChunks < 4096);
    Y_ABORT_UNLESS(stat.CanBeFreedChunks < 4096);
    TSet<TChunkIdx> owned;
    heap.GetOwnedChunks(owned);
    Y_ABORT_UNLESS(owned.size() < 4096);
    TStringStream html;
    heap.RenderHtml(html);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    const bool useBucketsV2 = provider.ConsumeBool();
    const bool softLocking = provider.ConsumeBool();
    const ui32 freeChunksReservation = provider.ConsumeIntegralInRange<ui32>(0, 2);

    THolder<THeap> heap = MakeHeap(useBucketsV2, freeChunksReservation, softLocking);
    TVector<THugeSlot> live;
    THashSet<ui32> chunks;
    ui32 nextChunk = 1;
    bool finishedRecovery = false;

    const ui32 ops = provider.ConsumeIntegralInRange<ui32>(0, MaxOps);
    for (ui32 opIndex = 0; opIndex < ops && provider.remaining_bytes(); ++opIndex) {
        switch (provider.ConsumeIntegralInRange<ui8>(0, 9)) {
            case 0: {
                const ui32 chunk = nextChunk++;
                chunks.insert(chunk);
                if (finishedRecovery) {
                    heap->AddChunk(chunk);
                } else {
                    heap->RecoveryModeAddChunk(chunk);
                }
                break;
            }

            case 1: {
                if (!finishedRecovery) {
                    heap->FinishRecovery();
                    finishedRecovery = true;
                }
                break;
            }

            case 2: {
                if (!finishedRecovery) {
                    break;
                }
                const ui32 size = PickBlobSize(provider);
                THugeSlot slot;
                ui32 slotSize = 0;
                if (heap->Allocate(size, &slot, &slotSize)) {
                    Y_ABORT_UNLESS(slot.GetSize() >= size);
                    Y_ABORT_UNLESS(!ContainsSlot(live, slot));
                    live.push_back(slot);
                }
                break;
            }

            case 3: {
                if (!finishedRecovery || live.empty()) {
                    break;
                }
                const size_t index = provider.ConsumeIntegralInRange<size_t>(0, live.size() - 1);
                const THugeSlot slot = live[index];
                heap->Free(slot.GetDiskPart());
                live[index] = live.back();
                live.pop_back();
                break;
            }

            case 4: {
                if (!finishedRecovery) {
                    break;
                }
                const ui32 chunk = heap->RemoveChunk();
                if (chunk) {
                    chunks.erase(chunk);
                }
                break;
            }

            case 5: {
                if (!finishedRecovery || live.empty()) {
                    break;
                }
                const THugeSlot slot = live[provider.ConsumeIntegralInRange<size_t>(0, live.size() - 1)];
                (void)heap->LockChunkForAllocation(slot.GetChunkId(), slot.GetSize());
                break;
            }

            case 6: {
                if (!finishedRecovery) {
                    break;
                }
                TString serialized = heap->Serialize();
                Y_ABORT_UNLESS(THeap::CheckEntryPoint(serialized));
                THolder<THeap> restored = MakeHeap(useBucketsV2, freeChunksReservation, softLocking);
                restored->ParseFromString(serialized);
                restored->FinishRecovery();
                Y_ABORT_UNLESS(restored->ToString() == heap->ToString());
                heap = std::move(restored);
                break;
            }

            case 7: {
                if (!finishedRecovery) {
                    break;
                }
                NKikimrVDiskData::THugeKeeperHeap proto;
                heap->SaveToProto(proto);
                THolder<THeap> restored = MakeHolder<THeap>("huge-heap-fuzz", proto, softLocking);
                restored->FinishRecovery();
                Y_ABORT_UNLESS(restored->ToString() == heap->ToString());
                heap = std::move(restored);
                break;
            }

            case 8: {
                if (finishedRecovery || chunks.empty()) {
                    break;
                }
                const ui32 size = PickBlobSize(provider);
                const ui32 slotSize = heap->SlotSizeOfThisSize(size);
                if (!slotSize) {
                    break;
                }
                auto it = chunks.begin();
                std::advance(it, provider.ConsumeIntegralInRange<size_t>(0, chunks.size() - 1));
                const ui32 chunk = *it;
                const ui32 slotsInChunk = Max<ui32>(1, ChunkSize / slotSize);
                const ui32 slotId = provider.ConsumeIntegralInRange<ui32>(0, Min<ui32>(slotsInChunk - 1, 1024));
                THugeSlot slot(chunk, slotId * slotSize, slotSize);
                if (!ContainsSlot(live, slot)) {
                    heap->RecoveryModeAllocate(slot.GetDiskPart());
                    live.push_back(slot);
                }
                break;
            }

            case 9: {
                if (!finishedRecovery) {
                    std::vector<ui32> shred;
                    for (ui32 chunk : chunks) {
                        if (provider.ConsumeBool()) {
                            shred.push_back(chunk);
                        }
                    }
                    heap->ShredNotify(shred);
                } else {
                    std::vector<ui32> shred;
                    if (!live.empty()) {
                        shred.push_back(live[provider.ConsumeIntegralInRange<size_t>(0, live.size() - 1)].GetChunkId());
                    }
                    heap->ShredNotify(shred);
                }
                break;
            }
        }

        if (finishedRecovery) {
            CheckHeap(*heap);
        }
    }

    return 0;
}
