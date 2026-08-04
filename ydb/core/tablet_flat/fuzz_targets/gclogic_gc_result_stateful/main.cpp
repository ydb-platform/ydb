#include <ydb/core/tablet_flat/flat_executor_gclogic.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <array>

namespace {

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;
using namespace NActors;

constexpr ui64 TabletId = 72075186224047637ull;
constexpr ui32 Generation = 11;
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

TEvBlobStorage::TEvCollectGarbageResult::TPtr MakeResult(ui32 channel, NKikimrProto::EReplyStatus status) {
    using TResult = TEvBlobStorage::TEvCollectGarbageResult;
    TAutoPtr<IEventHandle> base = new IEventHandle(
        TActorId(),
        TActorId(),
        new TResult(status, TabletId, Generation, 1, channel));
    return IEventHandle::Downcast<TResult>(std::move(base));
}

class TFuzzGCLogic : public TExecutorGCLogic {
public:
    using TExecutorGCLogic::TExecutorGCLogic;

    void AddCommitted(ui32 channel, ui32 timeStep, ui32 blobStep, ui32 cookie, bool deleted) {
        const TLogoBlobID id = MakeBlob(channel, blobStep, cookie);
        if (auto* info = ChannelInfo.FindPtr(channel)) {
            TGCBlobDelta& delta = info->CommittedDelta[TGCTime(Generation, timeStep)];
            delta.Created.push_back(id);
            if (deleted) {
                delta.Deleted.push_back(id);
            }
        } else {
            TGCLogEntry entry(TGCTime(Generation, timeStep));
            entry.Delta.Created.push_back(id);
            if (deleted) {
                entry.Delta.Deleted.push_back(id);
            }
            ApplyLogEntry(entry);
        }
    }

    void SeedSent(ui32 channel, ui32 collectStep, ui32 waitFor, ui32 knownStep) {
        auto* info = ChannelInfo.FindPtr(channel);
        Y_ABORT_UNLESS(info);
        info->CollectSent = TGCTime(Generation, collectStep);
        info->KnownGcBarrier = TGCTime(Generation, Max(collectStep, knownStep));
        info->MinUncollectedTime = TGCTime(Generation, collectStep + 1);
        info->GcWaitFor = Max<ui32>(1, waitFor);
        info->PendingRetry = false;
    }

    bool HasOutstanding(ui32 channel) const {
        if (auto* info = ChannelInfo.FindPtr(channel)) {
            return info->GcWaitFor > 0;
        }
        return false;
    }

    void CheckChannel(ui32 channel) const {
        auto* info = ChannelInfo.FindPtr(channel);
        if (!info) {
            return;
        }

        Y_ABORT_UNLESS(info->GcWaitFor < 1024);
        Y_ABORT_UNLESS(info->FailCount < 1024);
        if (!info->CollectSent) {
            Y_ABORT_UNLESS(info->GcWaitFor == 0 || info->FailCount > 0);
        }
        if (info->GcWaitFor == 0 && info->FailCount == 0) {
            Y_ABORT_UNLESS(!info->PendingRetry);
        }
        for (const auto& [time, delta] : info->CommittedDelta) {
            Y_ABORT_UNLESS(time.Generation == Generation);
            for (const auto& id : delta.Created) {
                Y_ABORT_UNLESS(id.TabletID() == TabletId);
                Y_ABORT_UNLESS(id.Channel() == channel);
            }
            for (const auto& id : delta.Deleted) {
                Y_ABORT_UNLESS(id.TabletID() == TabletId);
                Y_ABORT_UNLESS(id.Channel() == channel);
            }
        }
    }
};

class TFuzzActorRuntime final : public TTestActorRuntimeBase {
public:
    TFuzzActorRuntime()
        : TTestActorRuntimeBase(1, false)
    {
        Initialize();
    }
};

void Run(const ui8* data, size_t size, const TActorContext& ctx) {
    FuzzedDataProvider provider(data, size);
    TFuzzGCLogic logic(MakeTabletInfo(), MakeCookies());

    std::array<ui32, NumChannels> nextStep;
    nextStep.fill(1);
    ui32 nextCookie = 1;
    ui32 snapshotStep = 1;

    const ui32 ops = provider.ConsumeIntegralInRange<ui32>(0, MaxOps);
    for (ui32 opIndex = 0; opIndex < ops && provider.remaining_bytes(); ++opIndex) {
        const ui32 channel = provider.ConsumeIntegralInRange<ui32>(0, NumChannels - 1);
        switch (provider.ConsumeIntegralInRange<ui8>(0, 4)) {
            case 0: {
                const ui32 count = provider.ConsumeIntegralInRange<ui32>(1, 8);
                for (ui32 i = 0; i < count; ++i) {
                    const ui32 timeStep = nextStep[channel]++;
                    logic.AddCommitted(channel, timeStep, timeStep, nextCookie++, provider.ConsumeBool());
                }
                break;
            }

            case 1: {
                if (!logic.HasOutstanding(channel)) {
                    logic.AddCommitted(channel, nextStep[channel], nextStep[channel], nextCookie++, provider.ConsumeBool());
                    ++nextStep[channel];
                }
                const ui32 collectStep = provider.ConsumeIntegralInRange<ui32>(1, Max<ui32>(1, nextStep[channel] - 1));
                const ui32 waitFor = provider.ConsumeIntegralInRange<ui32>(1, 8);
                const ui32 knownStep = provider.ConsumeIntegralInRange<ui32>(0, collectStep);
                logic.SeedSent(channel, collectStep, waitFor, knownStep);
                break;
            }

            case 2:
            case 3: {
                if (!logic.HasOutstanding(channel)) {
                    break;
                }
                const bool ok = provider.ConsumeBool();
                auto ev = MakeResult(channel, ok ? NKikimrProto::OK : NKikimrProto::ERROR);
                const TDuration retry = logic.OnCollectGarbageResult(ev, ctx, ctx.SelfID);
                Y_ABORT_UNLESS(retry >= TDuration::Zero());
                break;
            }

            case 4: {
                TGCLogEntry snapshot = logic.SnapshotLog(snapshotStep++);
                for (const auto& id : snapshot.Delta.Created) {
                    Y_ABORT_UNLESS(id.TabletID() == TabletId);
                    Y_ABORT_UNLESS(id.Channel() < NumChannels);
                }
                for (const auto& id : snapshot.Delta.Deleted) {
                    Y_ABORT_UNLESS(id.TabletID() == TabletId);
                    Y_ABORT_UNLESS(id.Channel() < NumChannels);
                }
                break;
            }
        }

        for (ui32 c = 0; c < NumChannels; ++c) {
            logic.CheckChannel(c);
        }
        (void)logic.IntrospectStateSize();
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    static TFuzzActorRuntime runtime;
    runtime.RunCall([&] {
        Run(data, size, TActivationContext::AsActorContext());
        return true;
    });
    return 0;
}
