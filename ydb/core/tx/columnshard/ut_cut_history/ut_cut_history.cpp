#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/blob_manager.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/gc.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/history_cutter.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NKikimr {

namespace {

TIntrusivePtr<TTabletStorageInfo> MakeTabletInfo(ui64 tabletId, ui32 nChannels, const TVector<std::pair<ui32, ui32>>& historySlots)
{
    auto info = MakeIntrusive<TTabletStorageInfo>();
    info->TabletID = tabletId;
    info->TabletType = TTabletTypes::ColumnShard;
    info->Channels.resize(nChannels);
    for (ui32 ch = 0; ch < nChannels; ++ch) {
        info->Channels[ch].Channel = ch;
        info->Channels[ch].Type = TBlobStorageGroupType(TBlobStorageGroupType::ErasureNone);
        for (const auto& [fromGen, group] : historySlots) {
            TTabletChannelInfo::THistoryEntry e;
            e.FromGeneration = fromGen;
            e.GroupID = group;
            info->Channels[ch].History.push_back(e);
        }
    }
    return info;
}

static constexpr ui32 BlobSize = 1_KB;

TLogoBlobID MakeBlob(ui64 tabletId, ui32 channel, ui32 gen, ui32 step = 1, ui32 cookie = 1) {
    return TLogoBlobID(tabletId, gen, step, channel, BlobSize, cookie);
}

NOlap::TUnifiedBlobId MakeUnifiedBlob(const TLogoBlobID& logo) {
    return NOlap::TUnifiedBlobId(logo.TabletID(), logo);
}

class TCutHistoryController: public NYDBTest::ICSController {
public:
    bool IsCSCutHistoryEnabled() const override {
        return true;
    }

    void OnHistoryEntryNominated(const ui32 channel, const ui32 fromGeneration) override {
        TGuard<TMutex> g(Mutex);
        Nominated.emplace_back(channel, fromGeneration);
    }

    void OnHistoryEntryCut(const ui32 channel, const ui32 fromGeneration) override {
        TGuard<TMutex> g(Mutex);
        Cut.emplace_back(channel, fromGeneration);
    }

    TVector<std::pair<ui32, ui32>> GetNominated() const {
        TGuard<TMutex> g(Mutex);
        return Nominated;
    }

    TVector<std::pair<ui32, ui32>> GetCut() const {
        TGuard<TMutex> g(Mutex);
        return Cut;
    }

    void Reset() {
        TGuard<TMutex> g(Mutex);
        Nominated.clear();
        Cut.clear();
    }

private:
    mutable TMutex Mutex;
    TVector<std::pair<ui32, ui32>> Nominated;
    TVector<std::pair<ui32, ui32>> Cut;
};

}   // anonymous namespace

using TEntryKey = NOlap::NBlobOperations::NBlobStorage::TEntryKey;
using THistoryCutterWrapper = NOlap::NBlobOperations::NBlobStorage::THistoryCutterWrapper;
using ECutState = NOlap::NBlobOperations::NBlobStorage::ECutState;

// Only a live sensor instance is needed: values are asserted through the cutter's own state.
static const NColumnShard::THistoryCutterCounters& TestSignals() {
    static const NColumnShard::TBlobsManagerCounters counters("UT_BlobsManager");
    return counters.HistoryCutterCounters;
}

class TTestableHistoryCutter: public THistoryCutterWrapper {
public:
    using THistoryCutterWrapper::DecrementCounter;
    using THistoryCutterWrapper::GetCounterForTest;
    using THistoryCutterWrapper::GetCutStateForTest;
    using THistoryCutterWrapper::GetDisprovalAttemptsForTest;
    using THistoryCutterWrapper::IsChannelPoisonedForTest;
    using THistoryCutterWrapper::IsDrained;
    using THistoryCutterWrapper::StartSweepForTest;
    using THistoryCutterWrapper::THistoryCutterWrapper;
};

struct TCutterEnv {
    TIntrusivePtr<TTabletStorageInfo> Info;
    std::shared_ptr<NOlap::TBlobManager> Bm;
    std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> Shared;
};

// The cutter holds weak_ptrs to both managers, so keep this object in scope ahead of the cutter.
TCutterEnv MakeCutterEnv(
    const ui64 tabletId, const ui32 gen, const ui32 nChannels = 3, const TVector<std::pair<ui32, ui32>>& history = { { 0, 100 }, { 5, 200 } }) {
    auto info = MakeTabletInfo(tabletId, nChannels, history);
    return { info, std::make_shared<NOlap::TBlobManager>(info, gen, NOlap::TTabletId(tabletId)),
        std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(tabletId)) };
}

// Runs callbacks under a real actor context so Send/Register paths execute for real.
struct TEvRunInActor: public NActors::TEventLocal<TEvRunInActor, EventSpaceBegin(NActors::TEvents::ES_PRIVATE)> {
    std::function<void(const NActors::TActorContext&)> Fn;

    explicit TEvRunInActor(std::function<void(const NActors::TActorContext&)> fn)
        : Fn(std::move(fn))
    {
    }
};

class TRunnerActor: public NActors::TActor<TRunnerActor> {
public:
    TRunnerActor()
        : NActors::TActor<TRunnerActor>(&TRunnerActor::StateWork)
    {
    }

    STFUNC(StateWork) {
        if (auto* run = ev->CastAsLocal<TEvRunInActor>()) {
            run->Fn(ActorContext());
        }
    }
};

TEvBlobStorage::TEvRangeResult::TResponse MakeResponse(const TLogoBlobID& id, const bool keep = false, const bool doNotKeep = false) {
    return TEvBlobStorage::TEvRangeResult::TResponse(id, /*buffer=*/TString(), keep, doNotKeep);
}

// One candidate entry probed against one group, with an edge actor standing in for the BS proxy.
struct TRangeProbeEnv {
    static constexpr ui64 TabletId = 5150;
    static constexpr ui32 DataChannel = 2;
    static constexpr ui32 OldFromGen = 0;
    static constexpr ui32 OldGroup = 100;
    static constexpr ui32 NextFromGen = 5;

    TTestBasicRuntime Runtime;
    TAppPrepare App;
    NYDBTest::TControllers::TGuard<TCutHistoryController> Guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
    TCutterEnv Env;
    std::optional<TTestableHistoryCutter> CutterHolder;
    TActorId EdgeTablet;
    TActorId EdgeBs;
    TActorId Runner;
    TEntryKey Key{ DataChannel, OldFromGen };
    TEvBlobStorage::TEvRange::TPtr RequestHandle;

    TRangeProbeEnv()
        : Env(MakeCutterEnv(TabletId, NextFromGen, /*nChannels=*/3, { { OldFromGen, OldGroup }, { NextFromGen, 200 } }))
    {
        Runtime.Initialize(App.Unwrap());
        // The runtime drops scheduled events by default, and the probe deadline is one.
        Runtime.SetScheduledEventFilter([](auto&, auto&, auto, auto&) {
            return false;
        });
        EdgeTablet = Runtime.AllocateEdgeActor();
        EdgeBs = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(MakeBlobStorageProxyID(OldGroup), EdgeBs);
        Runner = Runtime.Register(new TRunnerActor());
        CutterHolder.emplace(Env.Info, NextFromGen, Env.Bm, Env.Shared, EdgeTablet, TestSignals());
    }

    TTestableHistoryCutter& Cutter() {
        return *CutterHolder;
    }

    void RunInActor(std::function<void(const NActors::TActorContext&)> fn) {
        Runtime.Send(new IEventHandle(Runner, EdgeTablet, new TEvRunInActor(std::move(fn))));
        Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    }

    void Start() {
        RunInActor([&](const NActors::TActorContext& ctx) {
            TVector<NOlap::NBlobOperations::NBlobStorage::TRangeProbe> probes{ { DataChannel, OldFromGen, NextFromGen, OldGroup } };
            ctx.Register(
                NOlap::NBlobOperations::NBlobStorage::CreateCutHistoryRangeProbeActor(EdgeTablet, TabletId, std::move(probes), /*round=*/0));
        });
    }

    const TEvBlobStorage::TEvRange* GrabRequest() {
        if (!RequestHandle) {
            RequestHandle = Runtime.GrabEdgeEvent<TEvBlobStorage::TEvRange>(EdgeBs, TDuration::Seconds(5));
        }
        return RequestHandle ? RequestHandle->Get() : nullptr;
    }

    void Run(const TVector<TEvBlobStorage::TEvRangeResult::TResponse>& responses, const NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
        Start();
        const auto* request = GrabRequest();
        UNIT_ASSERT(request);
        auto result = std::make_unique<TEvBlobStorage::TEvRangeResult>(status, request->From, request->To, OldGroup);
        result->Responses.assign(responses.begin(), responses.end());
        Runtime.Send(new IEventHandle(RequestHandle->Sender, EdgeBs, result.release(), 0, RequestHandle->Cookie));
    }

    THashSet<TEntryKey> GrabVerdict(ui64& failures) {
        auto done = Runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvCutHistoryRangeProbeDone>(EdgeTablet, TDuration::Seconds(10));
        UNIT_ASSERT_C(done, "the probe must always answer, so a missing verdict is a stuck sweep");
        failures = done->Get()->Failures;
        THashSet<TEntryKey> disproved;
        for (const auto& [ch, fromGen] : done->Get()->Disproved) {
            disproved.insert(TEntryKey{ ch, fromGen });
        }
        return disproved;
    }

    THashSet<TEntryKey> AssertDisproved(const ui64 expectedFailures) {
        ui64 failures = 0;
        auto disproved = GrabVerdict(failures);
        UNIT_ASSERT_VALUES_EQUAL(disproved.size(), 1);
        UNIT_ASSERT(disproved.contains(Key));
        UNIT_ASSERT_VALUES_EQUAL(failures, expectedFailures);
        return disproved;
    }

    void AssertNotDisproved() {
        ui64 failures = 0;
        const auto disproved = GrabVerdict(failures);
        UNIT_ASSERT_C(disproved.empty(), "the range held nothing of ours in the window");
        UNIT_ASSERT_VALUES_EQUAL(failures, 0);
    }
};

// Stands in for Local: observers see an edge actor's events zero or several times, so a real actor counts them.
class TCutRequestCounter: public NActors::TActor<TCutRequestCounter> {
public:
    explicit TCutRequestCounter(ui32& count)
        : NActors::TActor<TCutRequestCounter>(&TCutRequestCounter::StateWork)
        , Count(count)
    {
    }

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == TEvTablet::TEvCutTabletHistory::EventType) {
            ++Count;
        }
    }

private:
    ui32& Count;
};

// One clean candidate swept into a real barrier actor; the BS proxy is an edge actor.
struct TBarrierHarness {
    static constexpr ui64 TabletId = 4042;
    static constexpr ui32 DataChannel = 2;
    static constexpr ui32 OldFromGen = 0;
    static constexpr ui32 OldGroup = 100;
    static constexpr ui32 CurrentGen = 5;
    // Longer than the barrier actor's reply watchdog plus its largest retry backoff.
    static constexpr TDuration PastWatchdog = TDuration::Seconds(125);

    TAppPrepare App;
    TTestBasicRuntime Runtime;
    NYDBTest::TControllers::TGuard<TCutHistoryController> Controller;
    ui32 CutRequests = 0;
    const TActorId EdgeTablet;
    const TActorId Launcher;
    const TActorId EdgeBs;
    const TActorId Runner;
    TCutterEnv Env;
    TTestableHistoryCutter Cutter;
    const TEntryKey Key{ DataChannel, OldFromGen };

    TBarrierHarness()
        : Controller(InitRuntime())
        , EdgeTablet(Runtime.AllocateEdgeActor())
        , Launcher(Runtime.Register(new TCutRequestCounter(CutRequests)))
        , EdgeBs(Runtime.AllocateEdgeActor())
        , Runner(Runtime.Register(new TRunnerActor()))
        , Env(MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, { { OldFromGen, OldGroup }, { CurrentGen, 200 } }))
        , Cutter(Env.Info, CurrentGen, Env.Bm, Env.Shared, EdgeTablet, TestSignals())
    {
        Runtime.RegisterService(MakeBlobStorageProxyID(OldGroup), EdgeBs);
        Cutter.SetLauncherActorId(Launcher);
    }

    void SweepCleanToBarrier() {
        bool nominated = false;
        RunInActor([&](const NActors::TActorContext& ctx) {
            nominated = Cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT(Runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(EdgeTablet));
        Cutter.SetPortionSnapshot({});
        RunInActor([&](const NActors::TActorContext& ctx) {
            Cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT(Cutter.GetCutStateForTest(Key) == ECutState::SentBarrier);
    }

    TEvBlobStorage::TEvCollectGarbage::TPtr GrabBarrier() {
        auto request = Runtime.GrabEdgeEvent<TEvBlobStorage::TEvCollectGarbage>(EdgeBs);
        UNIT_ASSERT(request);
        return request;
    }

    void Reply(const TEvBlobStorage::TEvCollectGarbage::TPtr& request, const NKikimrProto::EReplyStatus status) {
        Runtime.Send(new IEventHandle(request->Sender, EdgeBs, new TEvBlobStorage::TEvCollectGarbageResult(status, TabletId, CurrentGen,
                                                                   request->Get()->PerGenerationCounter, DataChannel), 0, request->Cookie));
    }

private:
    NYDBTest::TControllers::TGuard<TCutHistoryController> InitRuntime() {
        Runtime.Initialize(App.Unwrap());
        // Measure-only rounds stop before the barrier.
        Runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        // The runtime drops scheduled events by default, and the barrier watchdog is one.
        Runtime.SetScheduledEventFilter([](auto&, auto&, auto, auto&) {
            return false;
        });
        return NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
    }

    void RunInActor(std::function<void(const NActors::TActorContext&)> fn) {
        Runtime.Send(new IEventHandle(Runner, EdgeTablet, new TEvRunInActor(std::move(fn))));
        Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    }
};

Y_UNIT_TEST_SUITE(TCutHistoryCutterCounters) {
    // Channels 0-2, history {fromGen=0, group=100} and active {fromGen=5, group=200}; only ch >= 2 is tracked.

    Y_UNIT_TEST(DecrementToZeroOnPortionRemoved) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 222;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, 5);
        TTestableHistoryCutter cutter(info, 5, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ 2, 0 };

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[42].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 3)));
        cutter.OnBootComplete(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 1);

        cutter.OnPortionRemoved(42);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 0);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(2));
    }

    Y_UNIT_TEST(ForeignBlobIgnored) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 333;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, 5);
        TTestableHistoryCutter cutter(info, 5, bm, shared, TActorId(), TestSignals());

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[55].push_back(MakeUnifiedBlob(MakeBlob(/*foreign*/ 999, 2, 3)));
        cutter.OnBootComplete(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, 0 }), 0);

        cutter.OnPortionRemoved(55);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(2));
    }

    Y_UNIT_TEST(ActiveEntryBlobIgnored) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 444;
        static constexpr ui32 CurrentGen = 5;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, 3, { { 0, 100 }, { CurrentGen, 200 } });
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, TActorId(), TestSignals());

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[77].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, CurrentGen)));
        cutter.OnBootComplete(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, 0 }), 0);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, CurrentGen }), 0);
    }

    Y_UNIT_TEST(BootCompleteWithEmptyMap) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 555;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, 3, 3, { { 0, 100 }, { 3, 200 } });
        TTestableHistoryCutter cutter(info, 3, bm, shared, TActorId(), TestSignals());

        cutter.OnBootComplete({});
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, 0 }), 0);
        UNIT_ASSERT(!cutter.IsSweepInFlight());
        UNIT_ASSERT(cutter.GetSweepCandidates()->empty());
    }

    Y_UNIT_TEST(DoubleBlobPerPortionDeduplicates) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 666;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, 5);
        TTestableHistoryCutter cutter(info, 5, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ 2, 0 };

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[88].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 1, 1, 1)));
        portionBlobs[88].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 2, 1, 2)));
        cutter.OnBootComplete(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 1);

        cutter.OnPortionRemoved(88);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 0);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(2));
    }

    Y_UNIT_TEST(SeenGroupsCheck) {
        // History [{0,100}, {5,100}, {10,200}]: {5,100} is blocked by the earlier {0,100}, the others pass.
        using TEntry = TTabletChannelInfo::THistoryEntry;
        std::vector<TEntry> hist;
        auto addEntry = [&](ui32 fromGen, ui32 group) {
            TEntry e;
            e.FromGeneration = fromGen;
            e.GroupID = group;
            hist.push_back(e);
        };
        addEntry(0, 100);
        addEntry(5, 100);
        addEntry(10, 200);

        UNIT_ASSERT(THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/0));
        UNIT_ASSERT(!THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/5));
        UNIT_ASSERT(THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/10));
        UNIT_ASSERT(!THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/99));

        // {5,100} is blocked by {0,100} unless that one was cut: cut entries are transparent.
        UNIT_ASSERT(THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/5, /*cutFromGenerations=*/{ 0 }));
        UNIT_ASSERT(!THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/5, /*cutFromGenerations=*/{ 10 }));
    }

    // Disproved candidates return to None, and survivors failing the final re-check get no barrier.
    Y_UNIT_TEST(SweepDisprovalPath) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        // 3 channels; history: {fromGen=0, group=100}, {fromGen=5, group=200}, {fromGen=10, group=300 (active)}.
        auto info = MakeTabletInfo(/*tabletId=*/777, /*nChannels=*/3, { { 0, 100 }, { 5, 200 }, { 10, 300 } });
        // An expired manager weak_ptr keeps IsDrained() false, so the re-check never reaches barrier-send.
        TTestableHistoryCutter cutter(info, /*currentGen=*/20, std::weak_ptr<NOlap::TBlobManager>(),
            std::weak_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager>(), TActorId(), TestSignals());

        const TEntryKey keyA{ /*channel=*/2, /*fromGeneration=*/0 };
        const TEntryKey keyB{ /*channel=*/2, /*fromGeneration=*/5 };
        cutter.StartSweepForTest({ keyA, keyB });
        UNIT_ASSERT(cutter.IsSweepInFlight());
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetSweepCandidates()->size(), 2);

        const auto ctx = NActors::TActivationContext::AsActorContext();

        cutter.OnBatchComplete({ keyA }, /*exhausted=*/true, ctx);

        UNIT_ASSERT(!cutter.IsSweepInFlight());
        UNIT_ASSERT(cutter.GetCutStateForTest(keyA) == ECutState::None);
        // Survivor keyB failed the final re-check (IsDrained false) → also None, no barrier.
        UNIT_ASSERT(cutter.GetCutStateForTest(keyB) == ECutState::None);
    }

    // Shared-out blobs must pin the entry: they are in no GC queue, but a barrier collects them anyway.
    Y_UNIT_TEST(SharedBlobsPinDrainGate) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 888;
        static constexpr ui64 BorrowerTabletId = 999;
        // History: {fromGen=0, group=100}, {fromGen=5, group=200 (active)}.
        auto [info, bm, shared] = MakeCutterEnv(TabletId, /*gen=*/5);

        TTestableHistoryCutter cutter(info, /*currentGen=*/5, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ /*channel=*/2, /*fromGeneration=*/0 };

        UNIT_ASSERT(cutter.IsDrained(key));

        // Share out one of OUR blobs living in the old range (channel 2, gen 1 < 5).
        const NOlap::TUnifiedBlobId sharedOut(/*dsGroup=*/100, TLogoBlobID(TabletId, /*gen=*/1, /*step=*/1, /*channel=*/2, 100, 1));
        UNIT_ASSERT(shared->UpsertSharedBlobOnLoad(sharedOut, NOlap::TTabletId(BorrowerTabletId)));
        UNIT_ASSERT_C(!cutter.IsDrained(key), "shared-out blob in the old range must pin the entry");
    }

    // An entry whose range still holds queued blobs must not be nominated (HasNoBlobsInRange arm).
    Y_UNIT_TEST(QueuedBlobsPinDrainGate) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 888;
        static constexpr ui32 ChannelCount = 3;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 ActiveFromGen = 5;
        static constexpr ui32 ActiveGroup = 200;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OtherChannel = 1;
        static constexpr ui32 GenInOldRange = 1;
        const TVector<std::pair<ui32, ui32>> history{ { OldFromGen, OldGroup }, { ActiveFromGen, ActiveGroup } };
        const TEntryKey key{ DataChannel, OldFromGen };

        // Both owners must outlive the cutter: an expired weak_ptr looks exactly like "not drained".
        auto makeCutter = [&](std::shared_ptr<NOlap::TBlobManager>& bmOut,
                              std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager>& sharedOut) {
            auto info = MakeTabletInfo(TabletId, ChannelCount, history);
            bmOut = std::make_shared<NOlap::TBlobManager>(info, ActiveFromGen, NOlap::TTabletId(TabletId));
            sharedOut = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
                NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
            return TTestableHistoryCutter(info, ActiveFromGen, bmOut, sharedOut, TActorId(), TestSignals());
        };

        std::shared_ptr<NOlap::TBlobManager> bm;
        std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> shared;
        auto cutter = makeCutter(bm, shared);
        UNIT_ASSERT_C(cutter.IsDrained(key), "empty queues: the old entry starts drained");

        // A blob still awaiting collection, inside the entry's range.
        bm->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeUnifiedBlob(MakeBlob(TabletId, DataChannel, GenInOldRange)));
        UNIT_ASSERT_C(!cutter.IsDrained(key), "a blob still in the delete queue must pin the entry");

        // Same channel, but the active entry's generation — outside this range.
        std::shared_ptr<NOlap::TBlobManager> bmOutside;
        std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> sharedOutside;
        auto cutterOutside = makeCutter(bmOutside, sharedOutside);
        bmOutside->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeUnifiedBlob(MakeBlob(TabletId, DataChannel, ActiveFromGen)));
        UNIT_ASSERT_C(cutterOutside.IsDrained(key), "a blob outside the range must not pin the entry");

        // Another channel entirely.
        std::shared_ptr<NOlap::TBlobManager> bmOtherChannel;
        std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> sharedOtherChannel;
        auto cutterOtherChannel = makeCutter(bmOtherChannel, sharedOtherChannel);
        bmOtherChannel->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeUnifiedBlob(MakeBlob(TabletId, OtherChannel, GenInOldRange)));
        UNIT_ASSERT_C(cutterOtherChannel.IsDrained(key), "a blob on another channel must not pin the entry");
    }

    // Underflow poisons the channel; nomination then skips it though every other gate is open.
    Y_UNIT_TEST(UnderflowPoisonsChannelAndBlocksNomination) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 1010;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, /*gen=*/5, /*nChannels=*/3, { { OldFromGen, 100 }, { 5, 200 } });
        TTestableHistoryCutter cutter(info, /*currentGen=*/5, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ DataChannel, OldFromGen };

        // Positive control: without the poison every nomination gate is open.
        UNIT_ASSERT(cutter.IsDrained(key));
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 0);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(DataChannel));

        // Decrement with no matching counter — the underflow branch.
        cutter.DecrementCounter(key);
        UNIT_ASSERT(cutter.IsChannelPoisonedForTest(DataChannel));

        // The poisoned channel yields no candidates, so TryNominate returns before any actor-system send.
        const auto ctx = NActors::TActivationContext::AsActorContext();
        UNIT_ASSERT(!cutter.TryNominate(ctx));
        UNIT_ASSERT(guard->GetNominated().empty());
        UNIT_ASSERT(!cutter.IsSweepInFlight());
    }

    // OnBootComplete must clear every piece of ephemeral state; each field is asserted.
    Y_UNIT_TEST(BootCompleteResetsEphemeralState) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 2020;
        static constexpr ui32 CurrentGen = 10;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/4, { { 0, 100 }, { 5, 200 }, { CurrentGen, 300 } });
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, TActorId(), TestSignals());
        const auto ctx = NActors::TActivationContext::AsActorContext();

        const TEntryKey keyOld{ /*channel=*/2, /*fromGeneration=*/0 };
        const TEntryKey keyMid{ /*channel=*/2, /*fromGeneration=*/5 };
        static constexpr ui32 PoisonChannel = 3;

        // Dirty everything reachable: counters, backoff, poison, and an in-flight sweep with a cursor.
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> oldPortions;
        oldPortions[1].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 1)));
        cutter.OnBootComplete(oldPortions);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(keyOld), 1);

        cutter.StartSweepForTest({ keyOld });
        cutter.OnBatchComplete({ keyOld }, /*exhausted=*/true, ctx);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetDisprovalAttemptsForTest(keyOld), 1);

        cutter.DecrementCounter(TEntryKey{ PoisonChannel, 0 });
        UNIT_ASSERT(cutter.IsChannelPoisonedForTest(PoisonChannel));

        cutter.StartSweepForTest({ keyMid });
        cutter.SetPortionSnapshot({ { NOlap::TInternalPathId::FromRawValue(1), 7 } });
        UNIT_ASSERT(cutter.IsSweepInFlight());
        UNIT_ASSERT(cutter.HasPortionSnapshot());
        UNIT_ASSERT(cutter.GetCutStateForTest(keyMid) == ECutState::Verifying);

        // Reboot with a different portion map: nothing from above may survive.
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> newPortions;
        newPortions[2].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 7, /*step=*/1, /*cookie=*/2)));
        cutter.OnBootComplete(newPortions);

        UNIT_ASSERT(!cutter.IsSweepInFlight());
        UNIT_ASSERT(cutter.GetSweepCandidates()->empty());
        UNIT_ASSERT(cutter.GetActiveSweepCandidates()->empty());
        UNIT_ASSERT(!cutter.HasPortionSnapshot());
        UNIT_ASSERT(cutter.GetCutStateForTest(keyMid) == ECutState::None);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetDisprovalAttemptsForTest(keyOld), 0);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(PoisonChannel));
        // Counters reflect only the new map: gen=7 falls in entry {2,5}, not {2,0}.
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(keyOld), 0);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(keyMid), 1);
    }

    // Backoff formula: 5m doubling per attempt, shift clamped at 12, capped at 6h.
    Y_UNIT_TEST(DisprovedCooldownFormula) {
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(0), TDuration::Minutes(5));
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(1), TDuration::Minutes(10));
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(2), TDuration::Minutes(20));
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(6), TDuration::Minutes(320));
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(7), THistoryCutterWrapper::DisprovedRetryMaxCooldown);
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(12), THistoryCutterWrapper::DisprovedRetryMaxCooldown);
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(Max<ui32>()), THistoryCutterWrapper::DisprovedRetryMaxCooldown);
    }

    // Happy path against real actors: nominate, sweep, hard barrier at nextFromGen-1, cut.
    Y_UNIT_TEST(SweepHappyPathSendsHardBarrier) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 3030;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 ActiveFromGen = 5;
        static constexpr ui32 ActiveGroup = 200;
        static constexpr ui32 CurrentGen = ActiveFromGen;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeLauncher = runtime.AllocateEdgeActor();
        const auto edgeBs = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageProxyID(OldGroup), edgeBs);
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] =
            MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, { { OldFromGen, OldGroup }, { ActiveFromGen, ActiveGroup } });
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        const TEntryKey key{ DataChannel, OldFromGen };

        // Empty queues: the old entry is drained and must be nominated.
        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
        UNIT_ASSERT_VALUES_EQUAL(guard->GetNominated().size(), 1);
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::Verifying);

        // Clean exhausted sweep (empty snapshot, nothing disproved) → barrier send.
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::SentBarrier);

        auto collect = runtime.GrabEdgeEvent<TEvBlobStorage::TEvCollectGarbage>(edgeBs);
        UNIT_ASSERT(collect);
        UNIT_ASSERT(collect->Get()->Hard);
        UNIT_ASSERT(collect->Get()->Collect);
        UNIT_ASSERT_VALUES_EQUAL(collect->Get()->TabletId, TabletId);
        UNIT_ASSERT_VALUES_EQUAL(collect->Get()->Channel, DataChannel);
        UNIT_ASSERT_VALUES_EQUAL(collect->Get()->CollectGeneration, ActiveFromGen - 1);

        runtime.Send(new IEventHandle(collect->Sender, edgeBs, new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, TabletId,
                                                                   CurrentGen, collect->Get()->PerGenerationCounter, DataChannel)));

        auto cutReq = runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher);
        UNIT_ASSERT(cutReq);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetTabletID(), TabletId);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetChannel(), DataChannel);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetFromGeneration(), OldFromGen);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetGroupID(), OldGroup);

        auto done = runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvCutHistoryBarrierDone>(edgeTablet);
        UNIT_ASSERT(done);
        UNIT_ASSERT(done->Get()->Ok);

        cutter.OnBarrierResult(key, done->Get()->Ok, runtime.GetCurrentTime());
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::Cut);
        UNIT_ASSERT_VALUES_EQUAL(guard->GetCut().size(), 1);

        // A cut entry is never nominated again: Hive gets one request per entry, not a nominate/cut loop.
        const auto nominationsAfterCut = guard->GetNominated().size();
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT_C(!nominated, "a cut entry must not be renominated");
        UNIT_ASSERT_VALUES_EQUAL(guard->GetNominated().size(), nominationsAfterCut);
        UNIT_ASSERT_VALUES_EQUAL(guard->GetCut().size(), 1);
    }

    // One Attempts increment per disproving sweep; cooldown gates renomination in mock time.
    Y_UNIT_TEST(DisprovalBackoffGatesRenomination) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 4040;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 CurrentGen = 5;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeBs = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageProxyID(OldGroup), edgeBs);
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, { { OldFromGen, OldGroup }, { CurrentGen, 200 } });
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        const TEntryKey key{ DataChannel, OldFromGen };

        auto tryNominate = [&](bool expected) {
            bool result = !expected;
            runInActor([&](const NActors::TActorContext& ctx) {
                result = cutter.TryNominate(ctx);
            });
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
        };
        auto disproveSweep = [&]() {
            UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
            cutter.SetPortionSnapshot({});
            runInActor([&](const NActors::TActorContext& ctx) {
                cutter.OnBatchComplete({ key }, /*exhausted=*/true, ctx);
            });
        };

        tryNominate(true);
        disproveSweep();
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetDisprovalAttemptsForTest(key), 1);

        // Attempts=1 after one disproval, so cooldown(1)=10m: 2m and 6m stay blocked, 11m passes.
        runtime.AdvanceCurrentTime(TDuration::Minutes(2));
        tryNominate(false);
        runtime.AdvanceCurrentTime(TDuration::Minutes(4));
        tryNominate(false);
        runtime.AdvanceCurrentTime(TDuration::Minutes(5));
        tryNominate(true);
        disproveSweep();
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetDisprovalAttemptsForTest(key), 2);

        // attempts=2 → 20m window: 12m blocked, 21m clears.
        runtime.AdvanceCurrentTime(TDuration::Minutes(12));
        tryNominate(false);
        runtime.AdvanceCurrentTime(TDuration::Minutes(9));
        tryNominate(true);

        // Nothing disproves the entry now: the sweep reaches the barrier and the backoff record is erased.
        UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvBlobStorage::TEvCollectGarbage>(edgeBs));
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::SentBarrier);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetDisprovalAttemptsForTest(key), 0);
    }

    // A failed barrier enters the ~10m disproval cooldown instead of retrying every cadence, and repeated
    // failures plateau there because the pre-barrier erase resets Attempts before each OnBarrierResult.
    Y_UNIT_TEST(BarrierFailureEntersDisprovalCooldown) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 4041;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 CurrentGen = 5;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeBs = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageProxyID(OldGroup), edgeBs);
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, { { OldFromGen, OldGroup }, { CurrentGen, 200 } });
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        const TEntryKey key{ DataChannel, OldFromGen };

        auto tryNominate = [&](bool expected) {
            bool result = !expected;
            runInActor([&](const NActors::TActorContext& ctx) {
                result = cutter.TryNominate(ctx);
            });
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
        };
        // Drives a clean sweep to the barrier; the only assertion here is TEvCollectGarbage reaching the proxy.
        auto sweepCleanToBarrier = [&]() {
            UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
            cutter.SetPortionSnapshot({});
            runInActor([&](const NActors::TActorContext& ctx) {
                cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
            });
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvBlobStorage::TEvCollectGarbage>(edgeBs));
        };

        // First sweep → barrier → failure → cooldown window (~10m).
        tryNominate(true);
        sweepCleanToBarrier();
        cutter.OnBarrierResult(key, /*ok=*/false, runtime.GetCurrentTime());

        // cooldown(1) is about 10m from the failure: 2m and 7m stay inside the window, 11m clears it.
        runtime.AdvanceCurrentTime(TDuration::Minutes(2));
        tryNominate(false);
        runtime.AdvanceCurrentTime(TDuration::Minutes(5));
        tryNominate(false);
        runtime.AdvanceCurrentTime(TDuration::Minutes(4));
        tryNominate(true);

        // A second failure must keep the window at ~10m rather than escalating to ~20m.
        sweepCleanToBarrier();
        cutter.OnBarrierResult(key, /*ok=*/false, runtime.GetCurrentTime());

        // Second window: 2m blocked, 11m total from the failure clears it — same ~10m as the first.
        runtime.AdvanceCurrentTime(TDuration::Minutes(2));
        tryNominate(false);
        runtime.AdvanceCurrentTime(TDuration::Minutes(9));
        tryNominate(true);
    }

    // The first barrier gets no reply, so the watchdog resends it; replies to both then arrive, and only one cut may follow.
    Y_UNIT_TEST(LostBarrierReplyIsResentAndCutsOnce) {
        TBarrierHarness h;
        h.SweepCleanToBarrier();
        auto first = h.GrabBarrier();
        h.Runtime.SimulateSleep(TBarrierHarness::PastWatchdog);
        auto second = h.GrabBarrier();
        UNIT_ASSERT(second->Get()->Hard);
        UNIT_ASSERT_VALUES_EQUAL(second->Get()->Channel, TBarrierHarness::DataChannel);
        UNIT_ASSERT_VALUES_EQUAL(second->Get()->CollectGeneration, first->Get()->CollectGeneration);
        UNIT_ASSERT(h.Cutter.GetCutStateForTest(h.Key) == ECutState::SentBarrier);

        h.Reply(first, NKikimrProto::OK);
        auto done = h.Runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvCutHistoryBarrierDone>(h.EdgeTablet);
        UNIT_ASSERT(done->Get()->Ok);
        h.Reply(second, NKikimrProto::OK);
        h.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(h.CutRequests, 1);

        h.Cutter.OnBarrierResult(h.Key, done->Get()->Ok, h.Runtime.GetCurrentTime());
        UNIT_ASSERT(h.Cutter.GetCutStateForTest(h.Key) == ECutState::Cut);
    }

    // No reply ever comes: after the retry limit the entry leaves SentBarrier for the failed-barrier cooldown.
    Y_UNIT_TEST(UnansweredBarrierGivesUpAfterRetries) {
        TBarrierHarness h;
        h.SweepCleanToBarrier();
        for (int attempt = 0; attempt < 3; ++attempt) {
            h.GrabBarrier();
            h.Runtime.SimulateSleep(TBarrierHarness::PastWatchdog);
        }
        auto done = h.Runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvCutHistoryBarrierDone>(h.EdgeTablet);
        UNIT_ASSERT(!done->Get()->Ok);
        UNIT_ASSERT_VALUES_EQUAL(h.CutRequests, 0);

        h.Cutter.OnBarrierResult(h.Key, done->Get()->Ok, h.Runtime.GetCurrentTime());
        UNIT_ASSERT(h.Cutter.GetCutStateForTest(h.Key) == ECutState::None);
        UNIT_ASSERT_VALUES_EQUAL(h.Cutter.GetDisprovalAttemptsForTest(h.Key), 1);
    }

    Y_UNIT_TEST(InFlightGCTaskPinsDrainGate) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 889;
        static constexpr ui32 CurrentGen = 5;
        const TVector<std::pair<ui32, ui32>> history{ { 0, 100 }, { CurrentGen, 200 } };
        const TEntryKey key{ 2, 0 };

        auto info = MakeTabletInfo(TabletId, 3, history);
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, TActorId(), TestSignals());
        UNIT_ASSERT(cutter.IsDrained(key));

        auto storageCounters = std::make_shared<NOlap::NBlobOperations::TStorageCounters>(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
        auto gcCounters =
            std::make_shared<NOlap::NBlobOperations::TRemoveGCCounters>(NOlap::NBlobOperations::TConsumerCounters("GC", *storageCounters));
        auto task = bm->BuildGCTask(NOlap::NBlobOperations::TGlobal::DefaultStorageId, bm, shared, gcCounters);
        UNIT_ASSERT_C(task, "the first GC round carries the barrier even with empty queues");
        UNIT_ASSERT_C(!cutter.IsDrained(key), "a GC task in flight must pin every entry");

        const TGenStep barrier{ CurrentGen, 0 };
        bm->OnGCStartOnComplete(barrier);
        bm->OnGCFinishedOnComplete(barrier);
        UNIT_ASSERT_C(cutter.IsDrained(key), "the pin is released once the task commits");
    }

    Y_UNIT_TEST(OrphanedDeleteMarkUnderCutEntryIsErasedNotCollected) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 890;
        static constexpr ui32 CurrentGen = 7;
        static constexpr ui32 DataChannel = 2;
        const TVector<std::pair<ui32, ui32>> history{ { 5, 200 } };

        auto info = MakeTabletInfo(TabletId, 3, history);
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));

        const TLogoBlobID orphan(TabletId, /*gen=*/0, /*step=*/0, DataChannel, BlobSize, /*cookie=*/1);
        UNIT_ASSERT_VALUES_EQUAL(info->GroupFor(orphan), Max<ui32>());
        bm->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), NOlap::TUnifiedBlobId(Max<ui32>(), orphan));

        auto storageCounters = std::make_shared<NOlap::NBlobOperations::TStorageCounters>(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
        auto gcCounters =
            std::make_shared<NOlap::NBlobOperations::TRemoveGCCounters>(NOlap::NBlobOperations::TConsumerCounters("GC", *storageCounters));
        auto task = bm->BuildGCTask(NOlap::NBlobOperations::TGlobal::DefaultStorageId, bm, shared, gcCounters);
        UNIT_ASSERT(task);
        const TString sentinelPrefix = "g=" + ToString(Max<ui32>()) + ";";
        for (const auto& [address, lists] : task->GetListsByGroupId()) {
            UNIT_ASSERT_C(
                !address.DebugString().StartsWith(sentinelPrefix), "no GC request may target the sentinel group: " + address.DebugString());
        }
        bm->OnGCStartOnComplete(TGenStep{ CurrentGen, 0 });
        bm->OnGCFinishedOnComplete(TGenStep{ CurrentGen, 0 });
        UNIT_ASSERT_C(bm->HasNoBlobsInRange(DataChannel, 0, 5), "the orphaned mark left the delete queue with the task");
    }

    // A live portion in the range blocks nomination; the portion erase from MoveData opens the gate.
    Y_UNIT_TEST(LivePortionBlocksNomination) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 3131;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 CurrentGen = 5;
        static constexpr ui64 PortionId = 77;

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, { { OldFromGen, 100 }, { CurrentGen, 200 } });
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ DataChannel, OldFromGen };
        const auto ctx = NActors::TActivationContext::AsActorContext();

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[PortionId].push_back(MakeUnifiedBlob(MakeBlob(TabletId, DataChannel, OldFromGen + 3)));
        cutter.OnBootComplete(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 1);
        UNIT_ASSERT(cutter.IsDrained(key));

        // Drained but not empty: the counter alone must hold nomination back.
        UNIT_ASSERT_C(!cutter.TryNominate(ctx), "a live portion in the range must block nomination");
        UNIT_ASSERT(guard->GetNominated().empty());

        cutter.OnPortionRemoved(PortionId);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 0);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(DataChannel));
    }

    // The range probe reports our own live blob in the window, and the bounds address exactly that channel.
    Y_UNIT_TEST(RangeProbeOurBlobDisproves) {
        TRangeProbeEnv env;
        env.Run({ MakeResponse(MakeBlob(TRangeProbeEnv::TabletId, TRangeProbeEnv::DataChannel, 3)) });

        const auto request = env.GrabRequest();
        // A blob id sorts channel before generation, so a channel-bounded request is an exact window, not a superset.
        UNIT_ASSERT_VALUES_EQUAL(request->From.Generation(), TRangeProbeEnv::OldFromGen);
        UNIT_ASSERT_VALUES_EQUAL(request->To.Generation(), TRangeProbeEnv::NextFromGen - 1);
        UNIT_ASSERT_VALUES_EQUAL(request->From.Channel(), TRangeProbeEnv::DataChannel);
        UNIT_ASSERT_VALUES_EQUAL(request->To.Channel(), TRangeProbeEnv::DataChannel);
        UNIT_ASSERT(request->IsIndexOnly);
        UNIT_ASSERT(!request->MustRestoreFirst);

        env.AssertDisproved(/*failures=*/0);
    }

    // Edge generations: the window includes fromGen and excludes nextFromGen.
    Y_UNIT_TEST(RangeProbeWindowEdges) {
        {
            TRangeProbeEnv env;
            env.Run({ MakeResponse(MakeBlob(TRangeProbeEnv::TabletId, TRangeProbeEnv::DataChannel, TRangeProbeEnv::OldFromGen)) });
            env.AssertDisproved(/*failures=*/0);
        }
        {
            TRangeProbeEnv env;
            env.Run({ MakeResponse(MakeBlob(TRangeProbeEnv::TabletId, TRangeProbeEnv::DataChannel, TRangeProbeEnv::NextFromGen - 1)) });
            env.AssertDisproved(/*failures=*/0);
        }
        {
            TRangeProbeEnv env;
            env.Run({ MakeResponse(MakeBlob(TRangeProbeEnv::TabletId, TRangeProbeEnv::DataChannel, TRangeProbeEnv::NextFromGen)) });
            env.AssertNotDisproved();
        }
    }

    // Blobs of another tablet, or of another channel sharing the group, say nothing about this entry.
    Y_UNIT_TEST(RangeProbeIgnoresForeignAndOtherChannel) {
        TRangeProbeEnv env;
        env.Run({ MakeResponse(MakeBlob(/*foreign*/ 999999, TRangeProbeEnv::DataChannel, 3)),
            MakeResponse(MakeBlob(TRangeProbeEnv::TabletId, /*otherChannel*/ 1, 3)) });
        env.AssertNotDisproved();
    }

    // A blob already released by GC is not evidence that the range is still occupied.
    // A DoNotKeep blob is this tablet's own garbage declaration coming back, so it cannot pin the window.
    Y_UNIT_TEST(RangeProbeIgnoresCollectedGarbage) {
        TRangeProbeEnv env;
        env.Run({ MakeResponse(MakeBlob(TRangeProbeEnv::TabletId, TRangeProbeEnv::DataChannel, 3), /*keep=*/false, /*doNotKeep=*/true) });
        env.AssertNotDisproved();
    }

    // An error answer is ambiguous, and ambiguity must never authorise an irreversible hard barrier.
    Y_UNIT_TEST(RangeProbeErrorFailsClosed) {
        TRangeProbeEnv env;
        env.Run({}, NKikimrProto::ERROR);
        env.AssertDisproved(/*failures=*/1);
    }

    // Silence is ambiguous too: the probe deadline disproves whatever never answered.
    Y_UNIT_TEST(RangeProbeTimeoutFailsClosed) {
        TRangeProbeEnv env;
        env.Start();
        UNIT_ASSERT(env.GrabRequest());
        // Simulated sleep, not AdvanceCurrentTime: the probe deadline arrives as a scheduled event.
        env.Runtime.SimulateSleep(TDuration::Minutes(2));
        env.AssertDisproved(/*failures=*/1);
    }

    // A probe queued behind slow ones gets a full deadline of its own instead of their leftover time.
    Y_UNIT_TEST(RangeProbeQueuedBehindSlowProbesGetsItsOwnDeadline) {
        TRangeProbeEnv env;
        static constexpr ui32 Window = 10;
        const ui32 probesCount = NOlap::NBlobOperations::NBlobStorage::THistoryCutterWrapper::MaxRangeProbesInFlight + 1;
        env.RunInActor([&](const NActors::TActorContext& ctx) {
            TVector<NOlap::NBlobOperations::NBlobStorage::TRangeProbe> probes;
            for (ui32 i = 0; i < probesCount; ++i) {
                probes.push_back({ TRangeProbeEnv::DataChannel, i * Window, (i + 1) * Window, TRangeProbeEnv::OldGroup });
            }
            ctx.Register(NOlap::NBlobOperations::NBlobStorage::CreateCutHistoryRangeProbeActor(
                env.EdgeTablet, TRangeProbeEnv::TabletId, std::move(probes), /*round=*/0));
        });
        auto reply = [&](const TEvBlobStorage::TEvRange::TPtr& request, const TVector<TEvBlobStorage::TEvRangeResult::TResponse>& responses) {
            auto result = std::make_unique<TEvBlobStorage::TEvRangeResult>(
                NKikimrProto::OK, request->Get()->From, request->Get()->To, TRangeProbeEnv::OldGroup);
            result->Responses.assign(responses.begin(), responses.end());
            env.Runtime.Send(new IEventHandle(request->Sender, env.EdgeBs, result.release(), 0, request->Cookie));
        };

        // The probes in flight answer after 40 s, each finding a live blob; only then does the queued probe start.
        TVector<TEvBlobStorage::TEvRange::TPtr> slow;
        for (ui32 i = 0; i + 1 < probesCount; ++i) {
            slow.push_back(env.Runtime.GrabEdgeEvent<TEvBlobStorage::TEvRange>(env.EdgeBs, TDuration::Seconds(5)));
            UNIT_ASSERT(slow.back());
        }
        env.Runtime.SimulateSleep(TDuration::Seconds(40));
        for (const auto& request : slow) {
            reply(request,
                { MakeResponse(MakeBlob(TRangeProbeEnv::TabletId, TRangeProbeEnv::DataChannel, request->Get()->From.Generation() + 1)) });
        }
        auto queued = env.Runtime.GrabEdgeEvent<TEvBlobStorage::TEvRange>(env.EdgeBs, TDuration::Seconds(5));
        UNIT_ASSERT(queued);

        // It also takes 40 s: 80 s after the batch started, but well inside its own deadline.
        env.Runtime.SimulateSleep(TDuration::Seconds(40));
        reply(queued, {});
        ui64 failures = 0;
        const auto disproved = env.GrabVerdict(failures);
        UNIT_ASSERT_VALUES_EQUAL_C(failures, 0, "the queued probe timed out on time the slow probes had used up");
        UNIT_ASSERT_VALUES_EQUAL(disproved.size(), probesCount - 1);
        UNIT_ASSERT_C(
            !disproved.contains(TEntryKey{ TRangeProbeEnv::DataChannel, (probesCount - 1) * Window }), "the empty queued range was disproved");
    }

    // The verdict feeds the ordinary sweep completion, so a disproved entry gets no barrier and stays uncut.
    Y_UNIT_TEST(RangeProbeVerdictLeavesEntryUncut) {
        TRangeProbeEnv env;
        env.Run({ MakeResponse(MakeBlob(TRangeProbeEnv::TabletId, TRangeProbeEnv::DataChannel, 3)) });
        const auto disproved = env.AssertDisproved(/*failures=*/0);

        env.Cutter().StartSweepForTest({ env.Key });
        env.RunInActor([&](const NActors::TActorContext& ctx) {
            env.Cutter().OnBatchComplete(disproved, /*exhausted=*/true, ctx);
        });

        UNIT_ASSERT(env.Cutter().GetCutStateForTest(env.Key) == ECutState::None);
        UNIT_ASSERT_C(!env.Runtime.GrabEdgeEvent<TEvBlobStorage::TEvCollectGarbage>(env.EdgeBs, TDuration::Seconds(1)),
            "a disproved entry must not reach the barrier");
    }

    // A Compare round cuts on the portion verdict, so a knob flip mid-round must not resolve it early.
    Y_UNIT_TEST(ProofSourceLatchedForTheRound) {
        TRangeProbeEnv env;
        auto& csConfig = env.Runtime.GetAppData().ColumnShardConfig;
        csConfig.SetCutHistoryProofSource(NKikimrConfig::TColumnShardConfig::CUT_HISTORY_PROOF_COMPARE);
        env.Cutter().StartSweepForTest({ env.Key });
        UNIT_ASSERT(env.Cutter().IsSweepInFlight());

        csConfig.SetCutHistoryProofSource(NKikimrConfig::TColumnShardConfig::CUT_HISTORY_PROOF_BS_RANGE);

        env.RunInActor([&](const NActors::TActorContext& ctx) {
            env.Cutter().OnRangeProbeComplete(env.Cutter().GetSweepRound(), {}, /*failures=*/0, ctx);
        });

        UNIT_ASSERT_C(env.Cutter().IsSweepInFlight(), "a Compare round must still wait for the portion verdict after the knob flips");
        UNIT_ASSERT_C(
            env.Cutter().GetCutStateForTest(env.Key) == ECutState::Verifying, "the range verdict alone may not resolve a Compare round");
    }

    // A delete owed to the range would be stranded by the cut, so the boot proof defers that entry.
    Y_UNIT_TEST(BootProbeDefersEntryWithPendingDeletes) {
        TRangeProbeEnv env;
        auto& csConfig = env.Runtime.GetAppData().ColumnShardConfig;
        csConfig.SetCutHistoryProofSource(NKikimrConfig::TColumnShardConfig::CUT_HISTORY_PROOF_BS_RANGE);

        bool nominated = true;
        env.RunInActor([&](const NActors::TActorContext& ctx) {
            env.Env.Bm->DeleteBlobOnComplete(NOlap::TTabletId(TRangeProbeEnv::TabletId),
                MakeUnifiedBlob(MakeBlob(TRangeProbeEnv::TabletId, TRangeProbeEnv::DataChannel, TRangeProbeEnv::OldFromGen + 1)));
            nominated = env.Cutter().TryNominateAtBoot(ctx);
        });
        UNIT_ASSERT_C(!nominated, "an entry with a pending delete in range must not be nominated at boot");
        UNIT_ASSERT_C(env.Cutter().GetCutStateForTest(env.Key) == ECutState::None, "the entry stays untouched for the next boot");
        UNIT_ASSERT_C(!env.Cutter().IsSweepInFlight(), "nothing may be in flight when every candidate is deferred");
    }

    // With the range clean the same boot pass nominates the entry without any cadence or portion scan.
    Y_UNIT_TEST(BootProbeNominatesCleanEntry) {
        TRangeProbeEnv env;
        auto& csConfig = env.Runtime.GetAppData().ColumnShardConfig;
        csConfig.SetCutHistoryProofSource(NKikimrConfig::TColumnShardConfig::CUT_HISTORY_PROOF_BS_RANGE);

        bool nominated = false;
        env.RunInActor([&](const NActors::TActorContext& ctx) {
            nominated = env.Cutter().TryNominateAtBoot(ctx);
        });
        UNIT_ASSERT_C(nominated, "a clean range must be nominated straight from boot");
        UNIT_ASSERT_C(env.Cutter().GetCutStateForTest(env.Key) == ECutState::Verifying, "the entry enters the round");
        UNIT_ASSERT(env.Cutter().IsSweepInFlight());
    }

    // A GC-blocked round is not a disproval: IsDrained refuses without starting a backoff, so a later pass may retry.
    Y_UNIT_TEST(BootProbeRetriesAfterGcBlockedRound) {
        TRangeProbeEnv env;
        auto& csConfig = env.Runtime.GetAppData().ColumnShardConfig;
        csConfig.SetCutHistoryProofSource(NKikimrConfig::TColumnShardConfig::CUT_HISTORY_PROOF_BS_RANGE);

        bool first = false;
        env.RunInActor([&](const NActors::TActorContext& ctx) {
            first = env.Cutter().TryNominateAtBoot(ctx);
        });
        UNIT_ASSERT_C(first, "the clean range must be nominated at boot");

        // A delete owed to the range lands mid-round, so the pre-barrier IsDrained refuses the survivor.
        env.RunInActor([&](const NActors::TActorContext& ctx) {
            env.Env.Bm->DeleteBlobOnComplete(NOlap::TTabletId(TRangeProbeEnv::TabletId),
                MakeUnifiedBlob(MakeBlob(TRangeProbeEnv::TabletId, TRangeProbeEnv::DataChannel, TRangeProbeEnv::OldFromGen + 1)));
            env.Cutter().OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT_C(!env.Cutter().IsSweepInFlight(), "the blocked round must finish");
        UNIT_ASSERT_C(env.Cutter().GetCutStateForTest(env.Key) == ECutState::None, "the entry returns to None");
        UNIT_ASSERT_VALUES_EQUAL_C(env.Cutter().GetDisprovalAttemptsForTest(env.Key), 0, "a GC-blocked round must not start the backoff");
    }

    // A range the probe disproved is not re-probed on every background pass; it waits out its backoff first.
    Y_UNIT_TEST(BootProbeRespectsDisprovalCooldown) {
        TRangeProbeEnv env;
        auto& csConfig = env.Runtime.GetAppData().ColumnShardConfig;
        csConfig.SetCutHistoryProofSource(NKikimrConfig::TColumnShardConfig::CUT_HISTORY_PROOF_BS_RANGE);
        auto nominate = [&]() {
            bool nominated = false;
            env.RunInActor([&](const NActors::TActorContext& ctx) {
                nominated = env.Cutter().TryNominateAtBoot(ctx);
            });
            return nominated;
        };

        UNIT_ASSERT_C(nominate(), "the clean range must be nominated at boot");
        env.RunInActor([&](const NActors::TActorContext& ctx) {
            env.Cutter().OnBatchComplete({ env.Key }, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT_VALUES_EQUAL(env.Cutter().GetDisprovalAttemptsForTest(env.Key), 1);

        UNIT_ASSERT_C(!nominate(), "a disproved range must not be re-probed before its cooldown ends");
        env.Runtime.AdvanceCurrentTime(TDuration::Minutes(11));
        UNIT_ASSERT_C(nominate(), "after the cooldown the range is probed again");
    }

    // Repeated passes must not re-nominate an entry whose round is still running.
    Y_UNIT_TEST(BootProbeSkipsEntryAlreadyInFlight) {
        TRangeProbeEnv env;
        auto& csConfig = env.Runtime.GetAppData().ColumnShardConfig;
        csConfig.SetCutHistoryProofSource(NKikimrConfig::TColumnShardConfig::CUT_HISTORY_PROOF_BS_RANGE);

        bool nominated = false;
        env.RunInActor([&](const NActors::TActorContext& ctx) {
            nominated = env.Cutter().TryNominateAtBoot(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT(env.Cutter().IsSweepInFlight());

        bool second = true;
        env.RunInActor([&](const NActors::TActorContext& ctx) {
            second = env.Cutter().TryNominateAtBoot(ctx);
        });
        UNIT_ASSERT_C(!second, "a pass while a round is in flight must nominate nothing");
    }

    // Hive erases a cut entry wherever it sits, and the lookup then hands its generations to the previous entry.
    Y_UNIT_TEST(MiddleEntryCutRemapsItsRangeToThePreviousGroup) {
        auto info = MakeTabletInfo(/*tabletId=*/4040, /*nChannels=*/4, { { 1, 100 }, { 5, 200 }, { 9, 300 } });
        auto& history = info->Channels[3].History;
        history.erase(history.begin() + 1);

        for (const ui32 gen : { 1u, 2u, 4u, 5u, 8u }) {
            UNIT_ASSERT_VALUES_EQUAL_C(info->GroupFor(3, gen), 100u, "gen " << gen);
        }
        for (const ui32 gen : { 9u, 10u }) {
            UNIT_ASSERT_VALUES_EQUAL_C(info->GroupFor(3, gen), 300u, "gen " << gen);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(info->GroupFor(2, 5), 200u, "a cut on one channel leaves the others alone");
    }

    // Cutting (C3, 5, G) out of [1->G0, 5->G, 9->G2] puts the hard barrier into G alone, so G0 blobs are never collected.
    Y_UNIT_TEST(MiddleEntryBarrierTargetsOnlyItsOwnGroup) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 4040;
        static constexpr ui32 Channel = 3;
        static constexpr ui32 GroupG0 = 100;
        static constexpr ui32 GroupG = 200;
        static constexpr ui32 GroupG2 = 300;
        static constexpr ui32 CurrentGen = 10;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeLauncher = runtime.AllocateEdgeActor();
        const auto edgeG0 = runtime.AllocateEdgeActor();
        const auto edgeG = runtime.AllocateEdgeActor();
        const auto edgeG2 = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageProxyID(GroupG0), edgeG0);
        runtime.RegisterService(MakeBlobStorageProxyID(GroupG), edgeG);
        runtime.RegisterService(MakeBlobStorageProxyID(GroupG2), edgeG2);
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/4, { { 1, GroupG0 }, { 5, GroupG }, { 9, GroupG2 } });
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        const TEntryKey middle{ Channel, 5 };

        // An edge-bound event can pass an observer more than once, so count distinct barriers by their per-generation counter.
        THashMap<ui32, THashSet<ui32>> collectsPerGroup;
        auto observer = runtime.AddObserver<TEvBlobStorage::TEvCollectGarbage>([&](TEvBlobStorage::TEvCollectGarbage::TPtr& ev) {
            for (const auto& [group, edge] : { std::pair{ GroupG0, edgeG0 }, std::pair{ GroupG, edgeG }, std::pair{ GroupG2, edgeG2 } }) {
                if (ev->Recipient == MakeBlobStorageProxyID(group) || ev->GetRecipientRewrite() == edge) {
                    collectsPerGroup[group].insert(ev->Get()->PerGenerationCounter);
                }
            }
        });

        cutter.StartSweepForTest({ middle });
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT(cutter.GetCutStateForTest(middle) == ECutState::SentBarrier);

        auto collect = runtime.GrabEdgeEvent<TEvBlobStorage::TEvCollectGarbage>(edgeG);
        UNIT_ASSERT(collect);
        UNIT_ASSERT(collect->Get()->Hard);
        UNIT_ASSERT_VALUES_EQUAL(collect->Get()->Channel, Channel);
        UNIT_ASSERT_VALUES_EQUAL(collect->Get()->CollectGeneration, 8u);

        runtime.Send(new IEventHandle(collect->Sender, edgeG,
            new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, TabletId, CurrentGen, collect->Get()->PerGenerationCounter, Channel)));
        auto cutReq = runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher);
        UNIT_ASSERT(cutReq);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetChannel(), Channel);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetFromGeneration(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetGroupID(), GroupG);

        UNIT_ASSERT_VALUES_EQUAL(collectsPerGroup[GroupG].size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL_C(collectsPerGroup[GroupG0].size(), 0u, "G0 still holds the earlier generations and must get no barrier");
        UNIT_ASSERT_VALUES_EQUAL(collectsPerGroup[GroupG2].size(), 0u);
    }

    // After the middle cut the G0 entry's window grows to [1, 9); its live portion keeps it uncut even if the range read is empty.
    Y_UNIT_TEST(EarlierEntryStaysPinnedAfterMiddleCut) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        auto& csConfig = runtime.GetAppData().ColumnShardConfig;
        csConfig.SetCutHistoryMeasureOnly(false);
        csConfig.SetCutHistoryProofSource(NKikimrConfig::TColumnShardConfig::CUT_HISTORY_PROOF_BS_RANGE);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 4141;
        static constexpr ui32 Channel = 3;
        static constexpr ui32 GroupG0 = 100;
        static constexpr ui32 GroupG2 = 300;
        static constexpr ui32 CurrentGen = 10;
        static constexpr ui64 PortionP6 = 6;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeG0 = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageProxyID(GroupG0), edgeG0);
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        // Hive already erased {5, G}, so the tablet boots with C3 = [1->G0, 9->G2]; C2 has only its active entry.
        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/4, { { 1, GroupG0 }, { 9, GroupG2 } });
        info->Channels[2].History.erase(info->Channels[2].History.begin());
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        const TEntryKey earlier{ Channel, 1 };

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[PortionP6].push_back(MakeUnifiedBlob(MakeBlob(TabletId, Channel, 1)));
        portionBlobs[PortionP6].push_back(MakeUnifiedBlob(MakeBlob(TabletId, Channel, 2)));
        cutter.OnBootComplete(portionBlobs);
        // The counter counts live portions, not blobs: P6 with both g1 and g2 is one.
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(earlier), 1);

        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominateAtBoot(ctx);
        });
        UNIT_ASSERT_C(nominated, "boot nomination does not look at portions; the proof and the counter gate decide");

        const auto probes = cutter.BuildRangeProbes();
        const auto probe = std::find_if(probes.begin(), probes.end(), [](const auto& p) {
            return p.Channel == Channel;
        });
        UNIT_ASSERT(probe != probes.end());
        UNIT_ASSERT_VALUES_EQUAL(probe->FromGeneration, 1u);
        UNIT_ASSERT_VALUES_EQUAL(probe->NextFromGeneration, 9u);
        UNIT_ASSERT_VALUES_EQUAL(probe->Group, GroupG0);

        ui32 collectsToG0 = 0;
        auto observer = runtime.AddObserver<TEvBlobStorage::TEvCollectGarbage>([&](TEvBlobStorage::TEvCollectGarbage::TPtr& ev) {
            if (ev->Recipient == MakeBlobStorageProxyID(GroupG0) || ev->GetRecipientRewrite() == edgeG0) {
                ++collectsToG0;
            }
        });

        // Worst case: the range read wrongly reports nothing, and the live-portion counter alone must refuse the barrier.
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnRangeProbeComplete(cutter.GetSweepRound(), {}, /*failures=*/0, ctx);
        });
        UNIT_ASSERT(cutter.GetCutStateForTest(earlier) == ECutState::None);
        UNIT_ASSERT_VALUES_EQUAL_C(collectsToG0, 0u, "a G0 entry pinned by a live portion must never get a hard barrier");
        UNIT_ASSERT(guard->GetCut().empty());
    }

}   // TCutHistoryCutterCounters

}   // namespace NKikimr
