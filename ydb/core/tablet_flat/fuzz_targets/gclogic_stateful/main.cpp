#include <ydb/core/tablet_flat/flat_executor_gclogic.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <array>
#include <limits>

namespace {

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

constexpr ui64 TabletId = 72075186224047637ull;
constexpr ui32 Generation = 7;
constexpr ui32 NumChannels = 4;
constexpr ui32 MaxOps = 256;

TIntrusiveConstPtr<TTabletStorageInfo> MakeTabletInfo() {
    TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(TabletId, TTabletTypes::Dummy);
    info->Channels.resize(NumChannels);
    for (ui32 channel = 0; channel < NumChannels; ++channel) {
        info->Channels[channel].History.emplace_back(1, 100 + channel, TInstant());
    }
    return info;
}

TAutoPtr<NPageCollection::TSteppedCookieAllocator> MakeCookies() {
    TVector<NPageCollection::TSlot> slots;
    for (ui32 channel = 0; channel < NumChannels; ++channel) {
        slots.emplace_back(channel, 100 + channel);
    }
    return new NPageCollection::TSteppedCookieAllocator(
        TabletId,
        ui64(Generation) << 32,
        {0, 1u << 20},
        slots);
}

TLogoBlobID MakeBlob(ui32 channel, ui32 step, ui32 cookie) {
    return TLogoBlobID(TabletId, Generation, step, channel, 1 + (cookie & 0xffff), cookie);
}

TVector<TLogoBlobID> Sorted(TVector<TLogoBlobID> values) {
    Sort(values);
    return values;
}

void AppendUnique(TVector<TLogoBlobID>& dst, const TLogoBlobID& id) {
    if (Find(dst.begin(), dst.end(), id) == dst.end()) {
        dst.push_back(id);
    }
}

void CheckNoIntersection(const TVector<TLogoBlobID>& keep, const TVector<TLogoBlobID>& doNotKeep) {
    auto sortedKeep = Sorted(keep);
    auto sortedDoNotKeep = Sorted(doNotKeep);
    auto itKeep = sortedKeep.begin();
    auto itDoNotKeep = sortedDoNotKeep.begin();
    while (itKeep != sortedKeep.end() && itDoNotKeep != sortedDoNotKeep.end()) {
        Y_ABORT_UNLESS(*itKeep != *itDoNotKeep);
        if (*itKeep < *itDoNotKeep) {
            ++itKeep;
        } else {
            ++itDoNotKeep;
        }
    }
}

class TOracle {
public:
    void AddCreated(const TLogoBlobID& id) {
        AppendUnique(Created, id);
    }

    void AddDeleted(const TLogoBlobID& id) {
        AppendUnique(Deleted, id);
    }

    void Apply(const TGCBlobDelta& delta) {
        for (const auto& id : delta.Created) {
            AddCreated(id);
        }
        for (const auto& id : delta.Deleted) {
            AddDeleted(id);
        }
    }

    void CheckSnapshot(const TGCLogEntry& snapshot) const {
        Y_ABORT_UNLESS(Sorted(snapshot.Delta.Created) == Sorted(Created));
        Y_ABORT_UNLESS(Sorted(snapshot.Delta.Deleted) == Sorted(Deleted));
    }

private:
    TVector<TLogoBlobID> Created;
    TVector<TLogoBlobID> Deleted;
};

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    TExecutorGCLogic logic(MakeTabletInfo(), MakeCookies());
    TOracle oracle;

    std::array<ui32, NumChannels> nextStep;
    nextStep.fill(1);
    ui32 commitStep = 1;
    ui32 nextCookie = 1;
    TSet<ui32> held;
    ui32 lastSnapshotStep = 0;
    auto nextSnapshotStep = [&] {
        ui32 step = Max(++lastSnapshotStep, commitStep++);
        for (ui32 channel = 0; channel < NumChannels; ++channel) {
            step = Max(step, nextStep[channel]);
        }
        return step;
    };

    const ui32 ops = provider.ConsumeIntegralInRange<ui32>(0, MaxOps);
    for (ui32 opIndex = 0; opIndex < ops && provider.remaining_bytes(); ++opIndex) {
        switch (provider.ConsumeIntegralInRange<ui8>(0, 7)) {
            case 0:
            case 1: {
                TGCBlobDelta delta;
                const ui32 count = provider.ConsumeIntegralInRange<ui32>(1, 8);
                for (ui32 i = 0; i < count; ++i) {
                    const ui32 channel = provider.ConsumeIntegralInRange<ui32>(0, NumChannels - 1);
                    const TLogoBlobID id = MakeBlob(channel, nextStep[channel]++, nextCookie++);
                    delta.Created.push_back(id);
                    if (provider.ConsumeBool()) {
                        delta.Deleted.push_back(id);
                    }
                }
                TGCLogEntry entry({Generation, commitStep++}, delta);
                logic.ApplyLogEntry(entry);
                oracle.Apply(delta);
                break;
            }

            case 2: {
                TLogCommit commit(false, commitStep++, ECommit::Redo, {});
                const ui32 count = provider.ConsumeIntegralInRange<ui32>(0, 8);
                for (ui32 i = 0; i < count; ++i) {
                    const ui32 channel = provider.ConsumeIntegralInRange<ui32>(0, NumChannels - 1);
                    const TLogoBlobID id = MakeBlob(channel, nextStep[channel]++, nextCookie++);
                    commit.GcDelta.Created.push_back(id);
                    if (provider.ConsumeBool()) {
                        commit.GcDelta.Deleted.push_back(id);
                    }
                }
                oracle.Apply(commit.GcDelta);
                logic.WriteToLog(commit);
                break;
            }

            case 3: {
                TGCLogEntry snapshot = logic.SnapshotLog(nextSnapshotStep());
                oracle.CheckSnapshot(snapshot);
                break;
            }

            case 4: {
                const ui32 step = provider.ConsumeIntegralInRange<ui32>(1, 1024);
                if (!held.contains(step)) {
                    held.insert(step);
                    logic.HoldBarrier(step);
                }
                break;
            }

            case 5: {
                if (!held.empty()) {
                    const ui32 index = provider.ConsumeIntegralInRange<ui32>(0, held.size() - 1);
                    auto it = held.begin();
                    std::advance(it, index);
                    const ui32 step = *it;
                    held.erase(it);
                    logic.ReleaseBarrier(step);
                }
                break;
            }

            case 6: {
                TGCBlobDelta delta;
                if (provider.ConsumeBool()) {
                    const ui32 channel = provider.ConsumeIntegralInRange<ui32>(0, NumChannels - 1);
                    const TLogoBlobID id = MakeBlob(channel, nextStep[channel]++, nextCookie++);
                    delta.Created.push_back(id);
                    oracle.AddCreated(id);
                }
                TGCLogEntry snapshot({Generation, commitStep++}, delta);
                TVector<std::pair<ui32, ui64>> barriers;
                logic.ApplyLogSnapshot(snapshot, barriers);
                break;
            }

            case 7: {
                TVector<TLogoBlobID> keep;
                TVector<TLogoBlobID> doNotKeep;
                const ui32 count = provider.ConsumeIntegralInRange<ui32>(0, 16);
                for (ui32 i = 0; i < count; ++i) {
                    const ui32 channel = provider.ConsumeIntegralInRange<ui32>(0, NumChannels - 1);
                    const ui32 step = provider.ConsumeIntegralInRange<ui32>(1, 64);
                    const ui32 cookie = provider.ConsumeIntegralInRange<ui32>(1, 256);
                    TLogoBlobID id = MakeBlob(channel, step, cookie);
                    AppendUnique(provider.ConsumeBool() ? keep : doNotKeep, id);
                    if (provider.ConsumeBool()) {
                        AppendUnique(doNotKeep, id);
                    }
                }
                Sort(keep);
                Sort(doNotKeep);
                DeduplicateGCKeepVectors(&keep, &doNotKeep, Generation, provider.ConsumeIntegralInRange<ui32>(0, 64));
                CheckNoIntersection(keep, doNotKeep);
                break;
            }
        }

        const ui32 active = logic.GetActiveGcBarrier();
        Y_ABORT_UNLESS(active == (held.empty() ? Max<ui32>() : *held.begin()));
        oracle.CheckSnapshot(logic.SnapshotLog(nextSnapshotStep()));
    }

    return 0;
}
