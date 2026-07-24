#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_chunk_tracker.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace {

using namespace NKikimr;
using namespace NKikimr::NPDisk;
using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

constexpr ui32 MaxOps = 256;

TOwner MakeOwner(ui8 index) {
    return TOwner(OwnerBeginUser + 1 + index % 32);
}

TColor::E ColorByIndex(ui8 index) {
    static constexpr TColor::E Colors[] = {
        TColor::GREEN,
        TColor::CYAN,
        TColor::LIGHT_YELLOW,
        TColor::YELLOW,
        TColor::LIGHT_ORANGE,
        TColor::PRE_ORANGE,
        TColor::ORANGE,
        TColor::RED,
        TColor::BLACK,
    };
    return Colors[index % std::size(Colors)];
}

bool HasUsage(const THashMap<TOwner, i64>& used) {
    for (const auto& [_, value] : used) {
        if (value) {
            return true;
        }
    }
    return false;
}

void CheckInvariants(TChunkTracker& tracker, const THashSet<TOwner>& active, const THashMap<TOwner, i64>& used) {
    i64 totalUsed = 0;
    for (TOwner owner : active) {
        const i64 ownerUsed = tracker.GetOwnerUsed(owner);
        const i64 ownerHard = tracker.GetOwnerHardLimit(owner);
        const i64 ownerFree = tracker.GetOwnerFree(owner, true);
        Y_ABORT_UNLESS(ownerUsed == used.Value(owner, 0));
        Y_ABORT_UNLESS(ownerUsed >= 0);
        Y_ABORT_UNLESS(ownerHard >= 0);
        Y_ABORT_UNLESS(ownerFree >= 0);
        Y_ABORT_UNLESS(ownerUsed + ownerFree == ownerHard);
        totalUsed += ownerUsed;

        double previousOccupancy = 0;
        TColor::E previous = tracker.EstimateSpaceColor(owner, 0, &previousOccupancy);
        const i64 sharedFree = tracker.GetOwnerFree(owner, false);
        for (i64 allocation = 1; allocation <= Min<i64>(sharedFree, 32); ++allocation) {
            double occupancy = 0;
            const TColor::E color = tracker.EstimateSpaceColor(owner, allocation, &occupancy);
            Y_ABORT_UNLESS(color >= previous);
            Y_ABORT_UNLESS(occupancy + 1e-9 >= previousOccupancy);
            previous = color;
            previousOccupancy = occupancy;
        }
    }

    if (!active.empty()) {
        const TOwner owner = *active.begin();
        Y_ABORT_UNLESS(tracker.GetOwnerFree(owner, false) >= 0);
        Y_ABORT_UNLESS(tracker.GetTotalUsed() == totalUsed);
        Y_ABORT_UNLESS(tracker.GetTotalUsed() + tracker.GetOwnerFree(owner, false) == tracker.GetTotalHardLimit());
    } else {
        Y_ABORT_UNLESS(tracker.GetTotalUsed() == 0);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    TChunkTracker tracker;
    TKeeperParams params;
    params.TotalChunks = provider.ConsumeIntegralInRange<i64>(300, 2048);
    params.ExpectedOwnerCount = provider.ConsumeIntegralInRange<i64>(0, 16);
    params.SysLogSize = provider.ConsumeIntegralInRange<i64>(0, 16);
    params.CommonLogSize = 0;
    params.MaxCommonLogChunks = provider.ConsumeIntegralInRange<i64>(0, 64);
    params.CommonStaticLogChunks = 0;
    params.HasStaticGroups = false;
    params.SpaceColorBorder = ColorByIndex(provider.ConsumeIntegral<ui8>());
    params.SeparateCommonLog = true;
    params.ChunkBaseLimit = provider.ConsumeIntegralInRange<ui32>(13, 130);

    TString error;
    if (!tracker.Reset(params, TColorLimits::MakeChunkLimits(params.ChunkBaseLimit), error)) {
        return 0;
    }

    THashSet<TOwner> active;
    THashMap<TOwner, i64> used;

    const ui32 ops = provider.ConsumeIntegralInRange<ui32>(0, MaxOps);
    for (ui32 i = 0; i < ops && provider.remaining_bytes(); ++i) {
        switch (provider.ConsumeIntegralInRange<ui8>(0, 5)) {
            case 0: {
                if (HasUsage(used)) {
                    break;
                }
                const TOwner owner = MakeOwner(provider.ConsumeIntegral<ui8>());
                if (!active.contains(owner)) {
                    active.insert(owner);
                    used[owner] = 0;
                    tracker.AddOwner(owner, TVDiskID(), provider.ConsumeIntegralInRange<ui32>(0, 8));
                }
                break;
            }

            case 1: {
                if (HasUsage(used) || active.empty()) {
                    break;
                }
                const ui32 index = provider.ConsumeIntegralInRange<ui32>(0, active.size() - 1);
                auto it = active.begin();
                std::advance(it, index);
                const TOwner owner = *it;
                tracker.RemoveOwner(owner);
                active.erase(it);
                used.erase(owner);
                break;
            }

            case 2: {
                if (HasUsage(used) || active.empty()) {
                    break;
                }
                const ui32 index = provider.ConsumeIntegralInRange<ui32>(0, active.size() - 1);
                auto it = active.begin();
                std::advance(it, index);
                tracker.SetOwnerWeight(*it, provider.ConsumeIntegralInRange<ui32>(0, 8));
                break;
            }

            case 3: {
                if (active.empty()) {
                    break;
                }
                const ui32 index = provider.ConsumeIntegralInRange<ui32>(0, active.size() - 1);
                auto it = active.begin();
                std::advance(it, index);
                const TOwner owner = *it;
                const i64 sharedFree = tracker.GetOwnerFree(owner, false);
                if (sharedFree <= 1) {
                    break;
                }
                const i64 count = provider.ConsumeIntegralInRange<i64>(1, Min<i64>(sharedFree, 16));
                double occupancy = 0;
                if (tracker.EstimateSpaceColor(owner, count, &occupancy) != TColor::BLACK) {
                    TString reason;
                    const bool ok = tracker.TryAllocate(owner, count, reason);
                    Y_ABORT_UNLESS(ok);
                    used[owner] += count;
                }
                break;
            }

            case 4: {
                TVector<TOwner> candidates;
                for (const auto& [owner, value] : used) {
                    if (value > 0) {
                        candidates.push_back(owner);
                    }
                }
                if (candidates.empty()) {
                    break;
                }
                const TOwner owner = candidates[provider.ConsumeIntegralInRange<size_t>(0, candidates.size() - 1)];
                const i64 count = provider.ConsumeIntegralInRange<i64>(1, used[owner]);
                tracker.Release(owner, count);
                used[owner] -= count;
                break;
            }

            case 5: {
                if (!HasUsage(used)) {
                    tracker.SetExpectedOwnerCount(provider.ConsumeIntegralInRange<size_t>(0, 32));
                    tracker.SetColorBorder(ColorByIndex(provider.ConsumeIntegral<ui8>()));
                }
                break;
            }
        }

        CheckInvariants(tracker, active, used);
    }

    return 0;
}
