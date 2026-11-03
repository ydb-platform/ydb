#include "flat_executor_gclogic.h"
#include "flat_sausage_grind.h"
#include <ydb/core/base/tablet.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

namespace {

constexpr ui32 HistoryCutterUtBlobSize = 42;

TLogoBlobID HistoryCutterUtBlob(ui64 tabletId, ui32 generation, ui32 channel) {
    return TLogoBlobID(tabletId, generation, 0, channel, HistoryCutterUtBlobSize, 0);
}

// The allocator is only exercised on the WriteToLog/SendCollectGarbage paths;
// ApplyLogEntry touches only ChannelInfo and HistoryCutter.
TAutoPtr<NPageCollection::TSteppedCookieAllocator> MakeGCCookies(
        const TTabletStorageInfo& info, ui32 generation = 1) {
    // Every channel needs a slot or the allocator asserts; the group is a placeholder.
    TVector<NPageCollection::TSlot> slots;
    for (ui32 ch = 0; ch < (ui32)info.Channels.size(); ++ch) {
        const ui32 group = info.Channels[ch].History.empty()
            ? 1u : info.Channels[ch].History.front().GroupID;
        slots.emplace_back(static_cast<ui8>(ch), group);
    }
    return new NPageCollection::TSteppedCookieAllocator(
        info.TabletID,
        ui64(generation) << 32,
        NPageCollection::TCookieRange{0, 999},
        TArrayRef<const NPageCollection::TSlot>(slots)
    );
}

// Drives one SendCollectGarbage pass from inside the actor system, since the call
// needs a real TActorContext to dispatch TEvCollectGarbage.
class TCollectGarbageDriver : public NActors::TActorBootstrapped<TCollectGarbageDriver> {
public:
    TCollectGarbageDriver(TExecutorGCLogic* logic, NActors::TActorId done)
        : Logic(logic), Done(done) {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        Logic->SendCollectGarbage(ctx);
        ctx.Send(Done, new NActors::TEvents::TEvWakeup());
        Die(ctx);
    }

private:
    TExecutorGCLogic* const Logic;
    const NActors::TActorId Done;
};

class TGCActionDriver : public NActors::TActorBootstrapped<TGCActionDriver> {
public:
    TGCActionDriver(std::function<void(const TActorContext&)> action, TActorId done)
        : Action(std::move(action)), Done(done) {}

    void Bootstrap(const TActorContext& ctx) {
        Action(ctx);
        ctx.Send(Done, new TEvents::TEvWakeup());
        Die(ctx);
    }

private:
    const std::function<void(const TActorContext&)> Action;
    const TActorId Done;
};

struct THistoryCutEnv {
    static constexpr ui64 TabletId = 61;
    static constexpr ui32 Channel = 2;
    static constexpr ui32 Generation = 20;

    struct TCollect {
        TActorId Recipient;
        ui32 Channel;
        ui32 Counter;
        bool Hard;
        ui32 BarrierGeneration;
        ui32 BarrierStep;
    };

    TTestBasicRuntime Runtime{1};
    TIntrusivePtr<TTabletStorageInfo> Info;
    THolder<TExecutorGCLogic> Logic;
    TVector<TCollect> Collects;
    TVector<std::pair<ui32, ui32>> Cuts;
    TActorId Edge;
    ui32 Step = 0;

    explicit THistoryCutEnv(TTabletTypes::EType tabletType = TTabletTypes::Dummy, bool delegateChannels = true)
        : Info(new TTabletStorageInfo(TabletId, tabletType))
    {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        app->FeatureFlags.SetEnableCutHistory(true);
        Runtime.Initialize(app->Unwrap());
        Edge = Runtime.AllocateEdgeActor();
        for (ui32 ch = 0; ch <= Channel; ++ch) {
            Info->Channels.emplace_back();
            Info->Channels.back().Channel = ch;
            Info->Channels.back().History.emplace_back(0, 101);
        }
        // Channel 0 belongs to the tablet's log GC, not the executor's data GC.
        Info->Channels[0].History.emplace_back(10, 102);
        Info->Channels[Channel].History.emplace_back(10, 102);
        Runtime.RegisterService(MakeBlobStorageProxyID(101), Edge);
        Runtime.RegisterService(MakeBlobStorageProxyID(102), Edge);
        TFeatureFlags flags;
        flags.SetEnableCutHistory(true);
        Logic = MakeHolder<TExecutorGCLogic>(Info, MakeGCCookies(*Info, Generation), flags);
        if (delegateChannels) {
            for (const auto& channel : Info->Channels) {
                Logic->InitializeChannel(channel.Channel);
            }
        }
        Logic->FollowersSyncComplete(true);
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage) {
                const auto* gc = ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                Collects.push_back({ev->Recipient, gc->Channel, gc->PerGenerationCounter,
                    gc->Hard, gc->CollectGeneration, gc->CollectStep});
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (ev->GetTypeRewrite() == TEvTablet::EvCutTabletHistory) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Recipient, Edge);
                const auto& record = ev->Get<TEvTablet::TEvCutTabletHistory>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetTabletID(), TabletId);
                UNIT_ASSERT_VALUES_EQUAL(record.GetChannel(), Channel);
                Cuts.emplace_back(record.GetFromGeneration(), record.GetGroupID());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
    }

    void Execute(std::function<void(const TActorContext&)> action) {
        Runtime.Register(new TGCActionDriver(std::move(action), Edge));
        Runtime.GrabEdgeEvent<TEvents::TEvWakeup>(Edge);
    }

    void Snapshot() {
        NKikimrExecutorFlat::TLogSnapshot snap;
        Logic->SnapToLog(snap, ++Step);
        Execute([&](const TActorContext& ctx) {
            Logic->OnCommitLog(Step, Step, ctx);
            Logic->Confirm(ctx, Edge);
        });
    }

    TDuration Reply(size_t index, NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
        const auto request = Collects.at(index);
        TDuration retry;
        Execute([&](const TActorContext& ctx) {
            TAutoPtr<IEventHandle> handle(new IEventHandle(ctx.SelfID, ctx.SelfID,
                new TEvBlobStorage::TEvCollectGarbageResult(status, TabletId, Generation,
                    request.Counter, request.Channel)));
            auto result = IEventHandle::Downcast<TEvBlobStorage::TEvCollectGarbageResult>(std::move(handle));
            retry = Logic->OnCollectGarbageResult(result, ctx);
        });
        return retry;
    }

    void RestoreBarrier() {
        TGCLogEntry snapshot(TGCTime(Generation, 0));
        Logic->ApplyLogSnapshot(snapshot, {{Channel, ui64(Generation) << 32}});
    }

    void CheckHardBarrier(size_t index) {
        UNIT_ASSERT_C(index < Collects.size(), "missing hard barrier for unused channel");
        const auto& gc = Collects[index];
        UNIT_ASSERT(gc.Hard);
        UNIT_ASSERT_VALUES_EQUAL(gc.Channel, Channel);
        UNIT_ASSERT_VALUES_EQUAL(gc.Recipient, MakeBlobStorageProxyID(101));
        UNIT_ASSERT_VALUES_EQUAL(gc.BarrierGeneration, 9);
        UNIT_ASSERT_VALUES_EQUAL(gc.BarrierStep, Max<ui32>());
    }

    void CheckCut() {
        UNIT_ASSERT_VALUES_EQUAL(Cuts.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Cuts[0].first, 0);
        UNIT_ASSERT_VALUES_EQUAL(Cuts[0].second, 101);
    }
};

// Drives one OnCollectGarbageResult call with a synthetic result event.
class TCollectGarbageResultDriver : public NActors::TActorBootstrapped<TCollectGarbageResultDriver> {
public:
    TCollectGarbageResultDriver(TExecutorGCLogic* logic, NKikimrProto::EReplyStatus status,
                                ui64 tabletId, ui32 channel, NActors::TActorId done,
                                TDuration* retryDelay)
        : Logic(logic), Status(status), TabletId(tabletId), Channel(channel), Done(done), RetryDelay(retryDelay) {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        TAutoPtr<IEventHandle> ieh(new IEventHandle(ctx.SelfID, ctx.SelfID,
            new TEvBlobStorage::TEvCollectGarbageResult(Status, TabletId, 1, 1, Channel)));
        auto ptr = IEventHandle::Downcast<TEvBlobStorage::TEvCollectGarbageResult>(std::move(ieh));
        *RetryDelay = Logic->OnCollectGarbageResult(ptr, ctx);
        ctx.Send(Done, new NActors::TEvents::TEvWakeup());
        Die(ctx);
    }

private:
    TExecutorGCLogic* const Logic;
    NKikimrProto::EReplyStatus Status;
    ui64 TabletId;
    ui32 Channel;
    const NActors::TActorId Done;
    TDuration* RetryDelay;
};

// Drives one RetryGcRequests call for a given channel.
class TRetryGcRequestDriver : public NActors::TActorBootstrapped<TRetryGcRequestDriver> {
public:
    TRetryGcRequestDriver(TExecutorGCLogic* logic, ui32 channel, NActors::TActorId done)
        : Logic(logic), Channel(channel), Done(done) {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        Logic->RetryGcRequests(Channel, ctx);
        ctx.Send(Done, new NActors::TEvents::TEvWakeup());
        Die(ctx);
    }

private:
    TExecutorGCLogic* const Logic;
    ui32 Channel;
    const NActors::TActorId Done;
};

} // namespace

Y_UNIT_TEST_SUITE(TFlatTableExecutorGC) {
    bool TestDeduplication(TVector<TLogoBlobID> keep, TVector<TLogoBlobID> dontkeep, ui32 gen, ui32 step, TVector<TLogoBlobID> expectKeep, TVector<TLogoBlobID> expectnot) {
        DeduplicateGCKeepVectors(&keep, &dontkeep, gen, step);
        return (keep == expectKeep) && (dontkeep == expectnot);
    }

    Y_UNIT_TEST(TestGCVectorDeduplicaton) {
        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            0, 0,
            {
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            }
        ));


        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            1, 0,
            {
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 2, 1, 0, 1),
            }
        ));

        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            1, 3,
            {
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
            }
        ));

        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 2, 1, 0, 0),
            },
            0, 0,
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 2, 1, 0, 0),
            }
        ));
    }
}


Y_UNIT_TEST_SUITE(THistoryCutter) {
    Y_UNIT_TEST(UndelegatedChannelsRequireExecutorGcEvidence) {
        THistoryCutEnv env(TTabletTypes::KeyValue, false);
        env.Snapshot();
        env.Snapshot();
        UNIT_ASSERT_C(env.Collects.empty(), "executor must not collect unowned channels");
        UNIT_ASSERT_C(env.Cuts.empty(), "executor must not cut unowned channel history");

        TGCBlobDelta delta;
        delta.Created.push_back(HistoryCutterUtBlob(THistoryCutEnv::TabletId, 15, 1));
        TGCLogEntry entry(TGCTime(15, 0), delta);
        env.Logic->ApplyLogEntry(entry);
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Collects[0].Channel, 1);
        UNIT_ASSERT(!env.Collects[0].Hard);
        env.Reply(0);
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 1);
        UNIT_ASSERT(env.Cuts.empty());
    }

    Y_UNIT_TEST(UnusedChannelHistoryIsCut) {
        THistoryCutEnv env;
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL_C(env.Collects.size(), 2,
            "unused data channel must initialize GC on both historical groups");
        for (const auto& gc : env.Collects) {
            UNIT_ASSERT_VALUES_EQUAL(gc.Channel, THistoryCutEnv::Channel);
            UNIT_ASSERT(!gc.Hard);
            UNIT_ASSERT_VALUES_EQUAL(gc.BarrierGeneration, THistoryCutEnv::Generation);
            UNIT_ASSERT_VALUES_EQUAL(gc.BarrierStep, 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Collects[0].Recipient, MakeBlobStorageProxyID(101));
        UNIT_ASSERT_VALUES_EQUAL(env.Collects[1].Recipient, MakeBlobStorageProxyID(102));
        env.Reply(0);
        // A snapshot while one group is still outstanding must not nominate a cut.
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 2);
        UNIT_ASSERT(env.Cuts.empty());
        env.Reply(1);
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 3);
        env.CheckHardBarrier(2);
        UNIT_ASSERT(env.Cuts.empty());
        env.Reply(2);
        env.CheckCut();
    }

    Y_UNIT_TEST(LiveBlobRemainsPinnedAfterSoftGc) {
        THistoryCutEnv env;
        const auto blob = HistoryCutterUtBlob(THistoryCutEnv::TabletId, 5, THistoryCutEnv::Channel);
        TGCBlobDelta delta;
        delta.Created.push_back(blob);
        TGCLogEntry entry(TGCTime(5, 0), delta);
        env.Logic->ApplyLogEntry(entry);
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 2);
        env.Reply(0);
        env.Reply(1);
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 2);
        UNIT_ASSERT(env.Cuts.empty());
        UNIT_ASSERT(env.Logic->HistoryCutter.GetHistoryToCut(THistoryCutEnv::Channel).empty());
    }

    Y_UNIT_TEST(UnusedChannelRetriesFailedSoftGcBeforeCut) {
        THistoryCutEnv env;
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 2);
        UNIT_ASSERT(!env.Reply(0, NKikimrProto::ERROR));
        UNIT_ASSERT(env.Reply(1));
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 2);
        UNIT_ASSERT(env.Cuts.empty());
        env.Execute([&](const TActorContext& ctx) {
            env.Logic->RetryGcRequests(THistoryCutEnv::Channel, ctx);
        });
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 4);
        UNIT_ASSERT(!env.Collects[2].Hard);
        UNIT_ASSERT(!env.Collects[3].Hard);
        env.Reply(2);
        env.Reply(3);
        env.Snapshot();
        env.CheckHardBarrier(4);
        env.Reply(4);
        env.CheckCut();
    }

    Y_UNIT_TEST(ExhaustedRetriesWaitForSoftGcBeforeCut) {
        THistoryCutEnv env;
        env.RestoreBarrier();
        env.Step = 1;
        env.Snapshot();
        env.CheckHardBarrier(0);
        auto retry = env.Reply(0, NKikimrProto::ERROR);
        UNIT_ASSERT(retry);

        // New executor data makes the retries send a soft GC batch after the
        // failed hard barrier. Keep failing until automatic retries stop.
        TGCBlobDelta delta;
        delta.Created.emplace_back(THistoryCutEnv::TabletId,
            THistoryCutEnv::Generation, 1, THistoryCutEnv::Channel,
            HistoryCutterUtBlobSize, 0);
        TGCLogEntry entry(TGCTime(THistoryCutEnv::Generation, 1), delta);
        env.Logic->ApplyLogEntry(entry);
        for (ui32 attempts = 0; retry; ++attempts) {
            UNIT_ASSERT_C(attempts < 100, "GC retries must eventually stop");
            const auto index = env.Collects.size();
            env.Execute([&](const TActorContext& ctx) {
                env.Logic->RetryGcRequests(THistoryCutEnv::Channel, ctx);
            });
            UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), index + 1);
            UNIT_ASSERT(!env.Collects[index].Hard);
            retry = env.Reply(index, NKikimrProto::ERROR);
        }

        const auto index = env.Collects.size();
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL_C(env.Collects.size(), index + 1,
            "a new soft GC batch must complete before sending a hard barrier");
        UNIT_ASSERT(!env.Collects[index].Hard);
        UNIT_ASSERT(env.Cuts.empty());
        env.Reply(index);
        UNIT_ASSERT(env.Cuts.empty());
        // No further snapshot is required to resume the confirmed cut.
        env.CheckHardBarrier(index + 1);
        env.Reply(index + 1);
        env.CheckCut();
    }

    Y_UNIT_TEST(DeferredHistoryCutResumesAfterFeatureFlagIsEnabled) {
        THistoryCutEnv env;
        env.RestoreBarrier();
        env.Step = 1;
        auto addBlob = [&] {
            TGCBlobDelta delta;
            delta.Created.emplace_back(THistoryCutEnv::TabletId,
                THistoryCutEnv::Generation, env.Step, THistoryCutEnv::Channel,
                HistoryCutterUtBlobSize, 0);
            TGCLogEntry entry(TGCTime(THistoryCutEnv::Generation, env.Step), delta);
            env.Logic->ApplyLogEntry(entry);
        };
        auto maintenance = [&] {
            env.Execute([&](const TActorContext& ctx) {
                env.Logic->RetryPendingHistoryCuts(ctx);
            });
        };

        addBlob();
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 1);
        UNIT_ASSERT(!env.Collects[0].Hard);
        env.Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(false);
        env.Reply(0);
        maintenance();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 1);
        UNIT_ASSERT(env.Cuts.empty());

        // Pausing history cuts must not put ordinary soft GC into backoff.
        addBlob();
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 2);
        UNIT_ASSERT(!env.Collects[1].Hard);
        env.Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(true);
        maintenance();
        UNIT_ASSERT_VALUES_EQUAL_C(env.Collects.size(), 2,
            "maintenance must wait for the outstanding soft GC batch");
        env.Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(false);
        env.Reply(1);
        maintenance();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 2);
        UNIT_ASSERT(env.Cuts.empty());

        env.Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(true);
        maintenance();
        env.CheckHardBarrier(2);
        maintenance();
        UNIT_ASSERT_VALUES_EQUAL_C(env.Collects.size(), 3,
            "maintenance must not duplicate hard barriers already in flight");
        env.Reply(2);
        env.CheckCut();
        maintenance();
        env.CheckCut();
    }

    Y_UNIT_TEST(SoftGcCompletionDoesNotConfirmSnapshotNomination) {
        THistoryCutEnv env;
        env.RestoreBarrier();
        env.Step = 1;
        TGCBlobDelta delta;
        delta.Created.emplace_back(THistoryCutEnv::TabletId,
            THistoryCutEnv::Generation, 1, THistoryCutEnv::Channel,
            HistoryCutterUtBlobSize, 0);
        TGCLogEntry entry(TGCTime(THistoryCutEnv::Generation, 1), delta);
        env.Logic->ApplyLogEntry(entry);

        NKikimrExecutorFlat::TLogSnapshot snap;
        env.Logic->SnapToLog(snap, ++env.Step);
        env.Execute([&](const TActorContext& ctx) {
            env.Logic->OnCommitLog(env.Step, env.Step, ctx);
        });
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 1);
        UNIT_ASSERT(!env.Collects[0].Hard);
        env.Reply(0);
        UNIT_ASSERT_VALUES_EQUAL_C(env.Collects.size(), 1,
            "GC completion must not authorize an unconfirmed history cut");
        UNIT_ASSERT(env.Cuts.empty());

        env.Execute([&](const TActorContext& ctx) {
            env.Logic->Confirm(ctx, env.Edge);
        });
        env.CheckHardBarrier(1);
        env.Reply(1);
        env.CheckCut();
    }

    Y_UNIT_TEST(FailedHardBarrierBatchDoesNotCutOnLastSuccess) {
        THistoryCutEnv env;
        env.Info->Channels[THistoryCutEnv::Channel].History.emplace(
            env.Info->Channels[THistoryCutEnv::Channel].History.begin() + 1, 5, 103);
        env.Runtime.RegisterService(MakeBlobStorageProxyID(103), env.Edge);
        env.RestoreBarrier();
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 2);
        UNIT_ASSERT(env.Collects[0].Hard);
        UNIT_ASSERT(env.Collects[1].Hard);
        UNIT_ASSERT(!env.Reply(0, NKikimrProto::ERROR));
        UNIT_ASSERT(env.Reply(1));
        UNIT_ASSERT(env.Cuts.empty());
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 2);
        // Retry alone must resume the confirmed cut on an idle tablet.
        env.Execute([&](const TActorContext& ctx) {
            env.Logic->RetryGcRequests(THistoryCutEnv::Channel, ctx);
        });
        UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 4);
        env.Reply(2);
        UNIT_ASSERT(env.Cuts.empty());
        env.Reply(3);
        UNIT_ASSERT_VALUES_EQUAL(env.Cuts.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.Cuts[0].first, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Cuts[0].second, 101);
        UNIT_ASSERT_VALUES_EQUAL(env.Cuts[1].first, 5);
        UNIT_ASSERT_VALUES_EQUAL(env.Cuts[1].second, 103);
    }

    Y_UNIT_TEST(RepeatedGroupHardBarrierIsDeduplicatedOnRetry) {
        THistoryCutEnv env;
        env.Info->Channels[THistoryCutEnv::Channel].History.emplace(
            env.Info->Channels[THistoryCutEnv::Channel].History.begin() + 1, 5, 101);
        env.RestoreBarrier();
        // Delegation during activation must preserve the recovered barrier.
        env.Logic->InitializeChannel(THistoryCutEnv::Channel);
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL_C(env.Collects.size(), 1,
            "repeated group must receive one hard barrier at the highest cut generation");
        env.CheckHardBarrier(0);
        UNIT_ASSERT(env.Reply(0, NKikimrProto::ERROR));
        env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL_C(env.Collects.size(), 1,
            "snapshot must not send hard barriers during GC backoff");
        UNIT_ASSERT(env.Cuts.empty());

        env.Execute([&](const TActorContext& ctx) {
            env.Logic->RetryGcRequests(THistoryCutEnv::Channel, ctx);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(env.Collects.size(), 2,
            "retry must also send only one hard barrier to the repeated group");
        // The confirmed cut must resume even if the tablet stays idle.
        env.CheckHardBarrier(1);
        UNIT_ASSERT(env.Cuts.empty());
        env.Reply(1);
        UNIT_ASSERT_VALUES_EQUAL(env.Cuts.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.Cuts[0].first, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Cuts[1].first, 5);
        UNIT_ASSERT_VALUES_EQUAL(env.Cuts[0].second, 101);
        UNIT_ASSERT_VALUES_EQUAL(env.Cuts[1].second, 101);
    }

    Y_UNIT_TEST(HistoryIsCutWithoutHardBarrier) {
        for (bool softGcPending : {false, true}) {
            THistoryCutEnv env;
            env.Info->Channels[THistoryCutEnv::Channel].History.emplace(
                env.Info->Channels[THistoryCutEnv::Channel].History.begin() + 1, 5, 101);
            // A live blob pins [0, 5) on group 101, so the empty [5, 10)
            // interval on that same group can be cut without a hard barrier.
            TGCBlobDelta delta;
            delta.Created.push_back(HistoryCutterUtBlob(THistoryCutEnv::TabletId, 2, THistoryCutEnv::Channel));
            TGCLogEntry entry(TGCTime(2, 0), delta);
            env.Logic->ApplyLogEntry(entry);
            env.Snapshot();
            UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 2);
            env.Reply(0);
            env.Reply(1);
            UNIT_ASSERT(env.Cuts.empty());

            if (softGcPending) {
                TGCBlobDelta nextDelta;
                nextDelta.Created.emplace_back(THistoryCutEnv::TabletId,
                    THistoryCutEnv::Generation, 1, THistoryCutEnv::Channel,
                    HistoryCutterUtBlobSize, 0);
                TGCLogEntry nextEntry(TGCTime(THistoryCutEnv::Generation, 1), nextDelta);
                env.Logic->ApplyLogEntry(nextEntry);
            }
            env.Snapshot();
            UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), softGcPending ? 3 : 2);
            if (softGcPending) {
                UNIT_ASSERT(!env.Collects[2].Hard);
                UNIT_ASSERT(env.Cuts.empty());
                UNIT_ASSERT(env.Reply(2, NKikimrProto::ERROR));
                // A failed soft batch must retain confirmation across its retry.
                env.Execute([&](const TActorContext& ctx) {
                    env.Logic->RetryGcRequests(THistoryCutEnv::Channel, ctx);
                });
                UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 4);
                UNIT_ASSERT(!env.Collects[3].Hard);
                UNIT_ASSERT(env.Cuts.empty());
                env.Reply(3);
                UNIT_ASSERT_VALUES_EQUAL(env.Collects.size(), 4);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(env.Cuts.size(), 1,
                "a confirmed cut without hard barriers must not wait for another GC reply");
            UNIT_ASSERT_VALUES_EQUAL(env.Cuts[0].first, 5);
            UNIT_ASSERT_VALUES_EQUAL(env.Cuts[0].second, 101);
            env.Snapshot();
            UNIT_ASSERT_VALUES_EQUAL_C(env.Cuts.size(), 1, "completed cuts must not be sent again");
        }
    }

    Y_UNIT_TEST(TestHistoryCutter) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(1, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        ui32 group = 0;
        for (ui32 gen : {1, 2, 5, 6, 7, 9, 10}) {
            info->Channels[0].History.emplace_back(gen, ++group);
        }
        THistoryCutter cutter(info);
        for (ui32 gen : {3, 4, 8, 9}) {
            cutter.SeenBlob(TLogoBlobID(1, gen, 1, 0, 42, 0));
        }
        std::vector<const TTabletChannelInfo::THistoryEntry*> toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[1]->FromGeneration, 5);
        UNIT_ASSERT_VALUES_EQUAL(toCut[2]->FromGeneration, 6);
    }



    Y_UNIT_TEST(NoCutsWhenHistoryHasLessThanTwoEntries) {
        {
            TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(7, TTabletTypes::Dummy);
            info->Channels.emplace_back();
            THistoryCutter cutter(info);
            UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
        }
        {
            TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(7, TTabletTypes::Dummy);
            info->Channels.emplace_back();
            info->Channels[0].History.emplace_back(1, 100);
            THistoryCutter cutter(info);
            cutter.SeenBlob(HistoryCutterUtBlob(7, 999, 0));
            UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
        }
    }

    Y_UNIT_TEST(BecomeUncertainDisablesCutsForThatChannel) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(2, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 10);
        info->Channels[0].History.emplace_back(100, 20);
        THistoryCutter cutter(info);
        cutter.BecomeUncertain(0);
        UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
    }

    Y_UNIT_TEST(BecomeUncertainDoesNotAffectOtherChannels) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(3, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(10, 2);
        info->Channels.emplace_back();
        info->Channels[1].History.emplace_back(1, 3);
        info->Channels[1].History.emplace_back(10, 4);
        THistoryCutter cutter(info);
        cutter.BecomeUncertain(0);
        // Channel 1: no blobs seen in [1, 10) => first history entry is cuttable.
        auto toCut = cutter.GetHistoryToCut(1);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
    }

    Y_UNIT_TEST(ForeignTabletBlobIsIgnored) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(4, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(10, 2);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(99999, 5, 0)); // wrong tablet
        // No valid seen generations => entire first segment looks empty.
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
    }

    Y_UNIT_TEST(SeenGenerationInsideRangeBlocksCut) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(5, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(10, 1);
        info->Channels[0].History.emplace_back(100, 2);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(5, 50, 0)); // 50 in [10, 100)
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT(toCut.empty());
    }

    Y_UNIT_TEST(SeenGenerationAtNextBoundaryAllowsCutOfPreviousSegment) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(6, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(10, 1);
        info->Channels[0].History.emplace_back(100, 2);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(6, 100, 0)); // first seen at next boundary, none in [10, 100)
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 10);
    }

    Y_UNIT_TEST(LastHistoryEntryIsNeverCut) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(8, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(5, 2);
        info->Channels[0].History.emplace_back(9, 3);
        THistoryCutter cutter(info);
        // No blobs seen — both leading segments cuttable; latest entry (9) must not appear.
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[1]->FromGeneration, 5);
    }

    Y_UNIT_TEST(ChannelIsolation) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(9, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(10, 2);
        info->Channels.emplace_back();
        info->Channels[1].History.emplace_back(1, 3);
        info->Channels[1].History.emplace_back(10, 4);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(9, 5, 0)); // only channel 0
        UNIT_ASSERT(cutter.GetHistoryToCut(1).size() == 1);
        UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
    }

    Y_UNIT_TEST(DuplicateSeenBlobIsIdempotent) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(10, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(10, 2);
        THistoryCutter cutter(info);
        const auto b = HistoryCutterUtBlob(10, 5, 0);
        cutter.SeenBlob(b);
        cutter.SeenBlob(b);
        cutter.SeenBlob(b);
        UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
    }

    // One barrier per cut entry when every entry sits on its own group. The barrier
    // generation is the *next* entry's FromGeneration minus one: everything the cut
    // entry covered is collectable, everything the next entry covers is not.
    Y_UNIT_TEST(HardBarrierPerCutEntry) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(40, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1u, 1u);
        info->Channels[0].History.emplace_back(10u, 2u);
        info->Channels[0].History.emplace_back(20u, 3u);
        THistoryCutter cutter(info);
        // Nothing seen: [1, 10) and [10, 20) are cuttable, the latest entry never is.
        auto barriers = cutter.GetHardBarriers(0);
        UNIT_ASSERT_VALUES_EQUAL(barriers.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(barriers.at(1), 9);
        UNIT_ASSERT_VALUES_EQUAL(barriers.at(2), 19);
        UNIT_ASSERT(!barriers.contains(3));
    }

    // A group reused by several cut entries gets a single barrier at the highest
    // generation. Separate barriers per entry would put several on one group, and a
    // retry could deliver the lower one last, which reads as a barrier decrease.
    Y_UNIT_TEST(RepeatedGroupCollapsesIntoHighestBarrier) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(41, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1u, 1u);
        info->Channels[0].History.emplace_back(5u, 2u);
        info->Channels[0].History.emplace_back(9u, 1u); // group 1 again
        info->Channels[0].History.emplace_back(20u, 3u);
        THistoryCutter cutter(info);
        // Nothing seen: the three leading entries are cuttable.
        auto barriers = cutter.GetHardBarriers(0);
        UNIT_ASSERT_VALUES_EQUAL(barriers.size(), 2);
        // Group 1 backs both [1, 5) and [9, 20); one barrier at 19 collects both.
        UNIT_ASSERT_VALUES_EQUAL(barriers.at(1), 19);
        UNIT_ASSERT_VALUES_EQUAL(barriers.at(2), 8);
    }

    // The rule the seenGroups bookkeeping enforces: a group that also backs a history
    // entry we are keeping must not get a hard barrier, or the barrier would collect
    // the blobs that entry still owns.
    Y_UNIT_TEST(NoHardBarrierWhenRetainedEntryUsesSameGroup) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(42, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1u, 1u);
        info->Channels[0].History.emplace_back(5u, 1u); // same group as the retained entry
        info->Channels[0].History.emplace_back(9u, 2u);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(42, 2, 0)); // pins [1, 5)
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 5);
        // [5, 9) is cuttable, but group 1 still backs the retained [1, 5).
        UNIT_ASSERT(cutter.GetHardBarriers(0).empty());
    }

    // A retained entry only blocks its own group. Cut entries on other groups keep
    // their barriers, and the barrier of a group reused after the retained entry is
    // still raised to the highest cut generation.
    Y_UNIT_TEST(RetainedEntryBlocksOnlyItsOwnGroup) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(43, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1u, 1u);
        info->Channels[0].History.emplace_back(5u, 2u); // retained
        info->Channels[0].History.emplace_back(9u, 1u); // group 1 again
        info->Channels[0].History.emplace_back(20u, 3u);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(43, 6, 0)); // pins [5, 9)
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[1]->FromGeneration, 9);
        auto barriers = cutter.GetHardBarriers(0);
        // Group 2 is pinned by [5, 9); group 1 is cut on both sides of it.
        UNIT_ASSERT_VALUES_EQUAL(barriers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(barriers.at(1), 19);
    }

    // Ablation for the seenGroups walk: the skipped entry [5, 9) must be folded into
    // seenGroups before its group is considered again, so group 2 stays barrier-free
    // even though a later cut entry sits on it.
    Y_UNIT_TEST(GroupOfRetainedEntryStaysBlockedLater) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(44, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1u, 1u);
        info->Channels[0].History.emplace_back(5u, 2u); // retained
        info->Channels[0].History.emplace_back(9u, 2u); // same group, cuttable
        info->Channels[0].History.emplace_back(20u, 3u);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(44, 6, 0)); // pins [5, 9)
        auto barriers = cutter.GetHardBarriers(0);
        UNIT_ASSERT_VALUES_EQUAL(barriers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(barriers.at(1), 4);
        UNIT_ASSERT_C(!barriers.contains(2),
            "group 2 still backs the retained [5, 9) entry and must not get a hard barrier");
    }

    Y_UNIT_TEST(NoHardBarriersWhenNothingIsCuttable) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(45, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1u, 1u);
        info->Channels[0].History.emplace_back(10u, 2u);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(45, 5, 0)); // pins the only cuttable entry
        UNIT_ASSERT(cutter.GetHardBarriers(0).empty());
    }

    Y_UNIT_TEST(NoHardBarriersWhenHistoryHasLessThanTwoEntries) {
        {
            TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(46, TTabletTypes::Dummy);
            info->Channels.emplace_back();
            THistoryCutter cutter(info);
            UNIT_ASSERT(cutter.GetHardBarriers(0).empty());
        }
        {
            TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(46, TTabletTypes::Dummy);
            info->Channels.emplace_back();
            info->Channels[0].History.emplace_back(1u, 1u);
            THistoryCutter cutter(info);
            UNIT_ASSERT(cutter.GetHardBarriers(0).empty());
        }
    }

    Y_UNIT_TEST(NoHardBarriersWhenChannelIsUncertain) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(47, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1u, 1u);
        info->Channels[0].History.emplace_back(10u, 2u);
        THistoryCutter cutter(info);
        // Without BecomeUncertain this channel would yield a barrier on group 1.
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetHardBarriers(0).size(), 1);
        cutter.BecomeUncertain(0);
        UNIT_ASSERT(cutter.GetHardBarriers(0).empty());
    }

    Y_UNIT_TEST(NoHardBarriersForUnknownChannel) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(48, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1u, 1u);
        info->Channels[0].History.emplace_back(10u, 2u);
        THistoryCutter cutter(info);
        // Channel index past the end must be answered, not asserted on.
        UNIT_ASSERT(cutter.GetHardBarriers(1).empty());
        UNIT_ASSERT(cutter.GetHardBarriers(Max<ui32>()).empty());
    }

    Y_UNIT_TEST(HardBarriersAreComputedPerChannel) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(49, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1u, 1u);
        info->Channels[0].History.emplace_back(10u, 2u);
        info->Channels.emplace_back();
        info->Channels[1].History.emplace_back(1u, 3u);
        info->Channels[1].History.emplace_back(10u, 4u);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(49, 5, 0)); // channel 0 only
        UNIT_ASSERT(cutter.GetHardBarriers(0).empty());
        auto barriers = cutter.GetHardBarriers(1);
        UNIT_ASSERT_VALUES_EQUAL(barriers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(barriers.at(3), 9);
    }

    // A pending DoNotKeep mark must pin the history entry that resolves its
    // blob's generation to a group, or the entry is cut before GC delivers the
    // flag and the group becomes irresolvable. Ablation: drop the SeenBlob call
    // in ApplyDelta's delta.Deleted loop and GetHistoryToCut returns [10, 100).
    Y_UNIT_TEST(DeletedBlobInApplyDeltaPinsHistoryEntry) {
        const ui64 tabletId = 30;

        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(tabletId, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        // Two history entries: [10, 100) -> group 1, [100, inf) -> group 2.
        info->Channels[0].History.emplace_back(10u, 1u);
        info->Channels[0].History.emplace_back(100u, 2u);

        TFeatureFlags flags;
        flags.SetEnableCutHistory(true);

        TExecutorGCLogic gcLogic(info, MakeGCCookies(*info), flags);

        // Put a DoNotKeep blob at generation 50 (inside [10, 100)) into Deleted.
        TGCBlobDelta delta;
        delta.Deleted.push_back(HistoryCutterUtBlob(tabletId, 50, 0));

        // ApplyLogEntry is the public entry point; it calls ApplyDelta internally.
        TGCLogEntry entry(TGCTime(1, 1), delta);
        gcLogic.ApplyLogEntry(entry);

        // The history entry covering [10, 100) must be blocked: generation 50
        // was seen there, so the entry must not appear in the cut list.
        auto toCut = gcLogic.HistoryCutter.GetHistoryToCut(0);
        UNIT_ASSERT_C(toCut.empty(),
            "history entry [10, 100) must not be cuttable while gen-50 blob has a pending DoNotKeep mark");
    }

    Y_UNIT_TEST(CreatedBlobInApplyDeltaPinsHistoryEntry) {
        const ui64 tabletId = 30;

        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(tabletId, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        // Two history entries: [10, 100) -> group 1, [100, inf) -> group 2.
        info->Channels[0].History.emplace_back(10u, 1u);
        info->Channels[0].History.emplace_back(100u, 2u);

        TFeatureFlags flags;
        flags.SetEnableCutHistory(true);

        TExecutorGCLogic gcLogic(info, MakeGCCookies(*info), flags);

        // Put a Keep blob at generation 50 (inside [10, 100)) into Created.
        TGCBlobDelta delta;
        delta.Created.push_back(HistoryCutterUtBlob(tabletId, 50, 0));

        // ApplyLogEntry is the public entry point; it calls ApplyDelta internally.
        TGCLogEntry entry(TGCTime(1, 1), delta);
        gcLogic.ApplyLogEntry(entry);

        // The history entry covering [10, 100) must be blocked: generation 50
        // was seen there, so the entry must not appear in the cut list.
        auto toCut = gcLogic.HistoryCutter.GetHistoryToCut(0);
        UNIT_ASSERT(toCut.empty());
    }

    // The precondition the sentinel guard relies on: generations below the first
    // surviving history entry resolve to Max<ui32>().
    Y_UNIT_TEST(GroupForGenerationReturnsSentinelBelowFirstEntry) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(31, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(10u, 77u); // first entry starts at gen 10

        // A generation strictly below the first entry must resolve to Max<ui32>().
        ui32 group = info->Channels[0].GroupForGeneration(5);
        UNIT_ASSERT_VALUES_EQUAL_C(group, Max<ui32>(),
            "GroupForGeneration must return Max<ui32>() for generations below the first history entry");
    }

    // SendCollectGarbage used to dereference &affectedGroups[GroupForGeneration(gen)]
    // unchecked; below the first surviving entry that is Max<ui32>(), and the collect
    // sent there ended in a BS error -> TEvPoison -> boot loop. Setup mirrors the state
    // after a cut: one history entry from generation 10, GC marks left for 5 and 6.
    // Ablation: drop the `if (vec)` guards and the sentinel proxy receives a collect.
    Y_UNIT_TEST(SendCollectGarbageSkipsSentinelGroup) {
        const ui64 tabletId = 32;
        const ui32 channel = 2;
        const ui32 survivingGroup = 77;
        const ui32 survivingFromGen = 10;

        TTestBasicRuntime runtime(1);
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        runtime.Initialize(app->Unwrap());

        // Stand in for the BS proxies. Without these the collect requests resolve to no
        // mailbox and are dropped before anything can observe them.
        const auto survivingEdge = runtime.AllocateEdgeActor();
        const auto sentinelEdge = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageProxyID(survivingGroup), survivingEdge);
        runtime.RegisterService(MakeBlobStorageProxyID(Max<ui32>()), sentinelEdge);

        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(tabletId, TTabletTypes::Dummy);
        info->Channels.resize(channel + 1);
        for (ui32 ch = 0; ch <= channel; ++ch) {
            info->Channels[ch].Channel = ch;
            // Single entry: everything below survivingFromGen resolves to the sentinel.
            info->Channels[ch].History.emplace_back(survivingFromGen, survivingGroup);
        }

        TFeatureFlags flags;
        flags.SetEnableCutHistory(true);

        // Tablet generation must exceed the blob generations below so they are collectable.
        TExecutorGCLogic gcLogic(info, MakeGCCookies(*info, 20), flags);
        gcLogic.FollowersSyncComplete(true);

        TGCBlobDelta delta;
        // Generations 5 and 6 sit below the surviving entry -> sentinel group.
        delta.Created.push_back(HistoryCutterUtBlob(tabletId, 5, channel));
        delta.Deleted.push_back(HistoryCutterUtBlob(tabletId, 6, channel));
        // Generation 12 resolves normally and must still be collected.
        delta.Created.push_back(HistoryCutterUtBlob(tabletId, 12, channel));
        TGCLogEntry entry(TGCTime(1, 1), delta);
        gcLogic.ApplyLogEntry(entry);

        // Count collect requests per proxy. GrabEdgeEvent cannot express "nothing
        // arrived" -- it throws once the queue drains -- so observe and count instead.
        THashMap<TActorId, ui32> collectsByProxy;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage) {
                ++collectsByProxy[ev->Recipient];
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        const auto done = runtime.AllocateEdgeActor();
        runtime.Register(new TCollectGarbageDriver(&gcLogic, done));
        runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(done);

        // The collect is dispatched before the driver's wakeup, so by now it has been observed.
        UNIT_ASSERT_C(collectsByProxy[MakeBlobStorageProxyID(survivingGroup)] > 0,
            "the surviving history entry must still receive its collect request");
        UNIT_ASSERT_VALUES_EQUAL_C(collectsByProxy.Value(MakeBlobStorageProxyID(Max<ui32>()), 0u), 0u,
            "a collect request must never be addressed to the Max<ui32>() sentinel group");
        UNIT_ASSERT_VALUES_EQUAL_C(collectsByProxy.size(), 1u,
            "collects must go to the surviving group only");
        // The guard's second observable: monitoring reports both below-sentinel marks
        // (the gen-5 keep and the gen-6 delete) as dropped.
        UNIT_ASSERT_VALUES_EQUAL(gcLogic.TakeSentinelDroppedMarks(), 2u);
    }

    // Regression: vacuum progress must not cancel another channel's pending backoff retry.
    Y_UNIT_TEST(BackoffPreservedOnSuccessOfOtherChannel) {
        const ui64 tabletId = 51;
        const ui32 group0 = 301;
        const ui32 group1 = 401;

        TTestBasicRuntime runtime(1);
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        runtime.Initialize(app->Unwrap());

        const auto edge0 = runtime.AllocateEdgeActor();
        const auto edge1 = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageProxyID(group0), edge0);
        runtime.RegisterService(MakeBlobStorageProxyID(group1), edge1);

        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(tabletId, TTabletTypes::Dummy);
        info->Channels.resize(2);
        info->Channels[0].Channel = 0;
        info->Channels[0].History.emplace_back(1u, group0);
        info->Channels[1].Channel = 1;
        info->Channels[1].History.emplace_back(1u, group1);

        TFeatureFlags flags;
        flags.SetEnableCutHistory(true);

        TExecutorGCLogic gcLogic(info, MakeGCCookies(*info, 5), flags);
        gcLogic.FollowersSyncComplete(true);

        {
            TGCBlobDelta delta;
            delta.Created.push_back(TLogoBlobID(tabletId, 1, 1, 0, 42, 0));
            delta.Created.push_back(TLogoBlobID(tabletId, 1, 1, 1, 42, 0));
            TGCLogEntry entry(TGCTime(1, 1), delta);
            gcLogic.ApplyLogEntry(entry);
        }

        THashMap<ui32, ui32> collectsByChannel;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage) {
                ++collectsByChannel[ev->Get<TEvBlobStorage::TEvCollectGarbage>()->Channel];
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // Initial send: both channels dispatch, and the later checks compare against this baseline rather than a fixed count.
        {
            const auto done = runtime.AllocateEdgeActor();
            runtime.Register(new TCollectGarbageDriver(&gcLogic, done));
            runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(done);
        }
        const ui32 base0 = collectsByChannel.Value(0u, 0u);
        const ui32 base1 = collectsByChannel.Value(1u, 0u);
        UNIT_ASSERT_C(base0 > 0, "channel 0 must dispatch its initial collect request");
        UNIT_ASSERT_C(base1 > 0, "channel 1 must dispatch its initial collect request");

        // Channel 0 succeeds; no retry must be scheduled.
        {
            TDuration retryDelay;
            const auto done = runtime.AllocateEdgeActor();
            runtime.Register(new TCollectGarbageResultDriver(&gcLogic, NKikimrProto::OK, tabletId, 0, done, &retryDelay));
            runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(done);
            UNIT_ASSERT_C(!retryDelay, "ch0 OK must not schedule a retry");
        }

        // Channel 1 fails; a backoff retry must be scheduled.
        TDuration ch1RetryDelay;
        {
            const auto done = runtime.AllocateEdgeActor();
            runtime.Register(new TCollectGarbageResultDriver(&gcLogic, NKikimrProto::ERROR, tabletId, 1, done, &ch1RetryDelay));
            runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(done);
            UNIT_ASSERT_C(ch1RetryDelay, "ch1 error must schedule a backoff retry");
        }

        // Simulate vacuum progress: a global SendCollectGarbage triggered by ch0 success.
        {
            const auto done = runtime.AllocateEdgeActor();
            runtime.Register(new TCollectGarbageDriver(&gcLogic, done));
            runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(done);
        }

        UNIT_ASSERT_VALUES_EQUAL_C(collectsByChannel.Value(1u, 0u), base1,
            "ch1 backoff must survive a global SendCollectGarbage triggered by ch0 success");

        // Fire the retry via the retry driver; ch1 must now send its deferred request.
        {
            const auto done = runtime.AllocateEdgeActor();
            runtime.Register(new TRetryGcRequestDriver(&gcLogic, 1, done));
            runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(done);
        }
        UNIT_ASSERT_C(collectsByChannel.Value(1u, 0u) > base1,
            "ch1 must send its deferred request when the retry driver fires");
    }
}

}
}
