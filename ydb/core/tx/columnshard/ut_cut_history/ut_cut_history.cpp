#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/sessions.h>
#include <ydb/core/tx/columnshard/data_sharing/modification/tasks/modification.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NColumnShard {
namespace {
using namespace NTxUT;
using EBackground = NYDBTest::ICSController::EBackground;
constexpr ui32 OldGroup = 2181038080;
constexpr ui32 NewGroup = 2181038081;
constexpr ui64 TabletId = TTestTxConfig::TxTablet0;
constexpr ui64 TableId = 1;

class TFixture {
public:
    NYDBTest::TControllers::TGuard<NOlap::TWaitCompactionController> Controller =
        NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
    TTestBasicRuntime Runtime;
    TIntrusivePtr<NFake::TProxyDS> OldProxy = new NFake::TProxyDS(TGroupId::FromValue(OldGroup));
    TIntrusivePtr<NFake::TProxyDS> NewProxy = new NFake::TProxyDS(TGroupId::FromValue(NewGroup));
    TActorId Sender;
    TActorId TabletActor;
    std::vector<std::pair<ui32, ui32>> History{ { 0, OldGroup } };
    TPlanStep ReadStep;
    TestTableDescription Table;

    TFixture() {
        TTester::Setup(
            Runtime, { new NFake::TProxyDS(TGroupId::FromValue(0)), OldProxy, NewProxy, new NFake::TProxyDS(TGroupId::FromValue(Max<ui32>())) });
        Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(true);
        Runtime.SetScheduledLimit(10000);
        auto previous = Runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&, TDuration, TInstant&) {
            return true;
        });
        Runtime.SetScheduledEventFilter(
            [previous](TTestActorRuntimeBase& r, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline) {
                if (event->HasEvent() && dynamic_cast<TEvPrivate::TEvContinueCutHistory*>(event->GetBase())) {
                    return false;
                }
                return previous(r, event, delay, deadline);
            });
        Controller->DisableBackground(EBackground::TTL);
        Sender = Runtime.AllocateEdgeActor();
        Boot();
    }

    void Boot() {
        auto info = MakeIntrusive<TTabletStorageInfo>();
        info->TabletID = TabletId;
        info->TabletType = TTabletTypes::ColumnShard;
        info->Channels.resize(3);
        for (ui32 channel = 0; channel < 3; ++channel) {
            auto& ch = info->Channels[channel];
            ch.Channel = channel;
            ch.Type = TBlobStorageGroupType(BootGroupErasure);
            ch.History.emplace_back(0, OldGroup);
            if (channel == 2) {
                for (size_t i = 1; i < History.size(); ++i) {
                    ch.History.emplace_back(History[i].first, History[i].second);
                }
            }
        }
        auto setup = MakeIntrusive<TTabletSetupInfo>(&CreateColumnShard, TMailboxType::Simple, ui32(0), TMailboxType::Simple, ui32(0));
        TabletActor = Runtime.Register(CreateTablet(Sender, info.Get(), setup.Get(), 0));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot);
        Runtime.DispatchEvents(options);
        Runtime.SimulateSleep(TDuration::Seconds(1));
    }

    void Restart(std::optional<ui32> group = {}) {
        if (group) {
            History.emplace_back(Controller->GetTheOnlyShard()->Generation() + 1, *group);
        }
        Runtime.Send(new IEventHandle(TabletActor, Sender, new TKikimrEvents::TEvPoisonPill));
        Boot();
    }

    void Drive(ui32 count = 10) {
        for (ui32 i = 0; i < count; ++i) {
            Wakeup(Runtime, Sender, TabletId);
            Runtime.SimulateSleep(TDuration::Seconds(1));
        }
    }

    void Schema(const bool tieredIndex = false) {
        NKikimrTxColumnShard::TSchemaTxBody tx;
        auto* init = tx.MutableInitShard();
        init->SetOwnerPath("/Root/olap");
        init->SetOwnerPathId(TableId);
        auto* table = init->AddTables();
        TSchemeShardLocalPathId::FromRawValue(TableId).ToProto(*table);
        TTestSchema::TTableSpecials specials;
        if (tieredIndex) {
            TTestSchema::TStorageTier warm("warm");
            warm.EvictAfter = TDuration::Zero();
            specials.Tiers.push_back(warm);
        }
        auto* schema = table->MutableSchema();
        TTestSchema::InitSchema(Table.Schema, Table.Pk, specials, schema);
        auto* metadata = schema->MutableOptions()->MutableMetadataManagerConstructor();
        metadata->SetClassName("local_db");
        metadata->MutableLocalDB()->SetFetchOnStart(false);
        metadata->MutableLocalDB()->SetMemoryCacheSize(128 << 20);
        if (tieredIndex) {
            *schema->AddIndexes() = NOlap::NIndexes::TIndexMetaContainer(
                std::make_shared<NOlap::NIndexes::NMax::TIndexMeta>(3000, "ts_max_bs", NOlap::IStoragesManager::DefaultStorageId, false, 1))
                                        .SerializeToProto();
            TTestSchema::InitTiersAndTtl(specials, table->MutableTtlSettings());
        }
        ReadStep = SetupSchema(Runtime, Sender, tx.SerializeAsString(), 1000);
    }

    void Write(ui64 txId, ui64 from, ui64 to) {
        std::vector<ui64> ids;
        UNIT_ASSERT(WriteData(Runtime, Sender, TabletId, txId, TableId, MakeTestBlob({ from, to }, Table.Schema), Table.Schema, &ids));
        ReadStep = ProposeCommit(Runtime, Sender, TabletId, txId, ids);
        PlanCommit(Runtime, Sender, TabletId, ReadStep, TSet<ui64>{ txId });
    }

    ui64 ReadRows() {
        return ReadAllAsBatch(Runtime, TableId, NOlap::TSnapshot(ReadStep.Val(), 1), Table.Schema)->num_rows();
    }

    std::vector<TLogoBlobID> LiveOldBlobs() const {
        std::vector<TLogoBlobID> result;
        for (const auto& [id, blob] : OldProxy->AllMyBlobs()) {
            if (id.TabletID() == TabletId && id.Channel() >= 2 && !blob.DoNotKeep) {
                result.push_back(id);
            }
        }
        return result;
    }

    auto Counters() {
        return GetServiceCounters(Runtime.GetDynamicCounters(0), "tablets")->GetSubgroup("subsystem", "columnshard")->GetSubgroup("module_id",
            "CS");
    }

    ui64 Samples(const TString& name) {
        const auto histogram = Counters()->FindHistogram("Histogram/CutHistory/" + name + "/DurationMs");
        UNIT_ASSERT(histogram);
        const auto snapshot = histogram->Snapshot();
        ui64 result = 0;
        for (ui32 i = 0; i < snapshot->Count(); ++i) {
            result += snapshot->Value(i);
        }
        return result;
    }
};
}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardCutHistory) {
    Y_UNIT_TEST(EmptyClosedInterval) {
        TFixture f;
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 0u);
        const auto aborted = f.Counters()->GetCounter("Deriviative/CutHistory/ScansAborted/Count", true);
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 0u);
        ui32 cuts = 0;
        ui32 batches = 0;
        bool holdGC = true;
        std::vector<TAutoPtr<IEventHandle>> held;
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (const auto* gc = dynamic_cast<TEvBlobStorage::TEvCollectGarbageResult*>(ev->GetBase());
                holdGC && gc && gc->TabletId == TabletId && gc->Channel == 2) {
                held.emplace_back(ev.Release());
            } else if (dynamic_cast<TEvPrivate::TEvContinueCutHistory*>(ev->GetBase())) {
                ++batches;
            } else if (const auto* cut = dynamic_cast<TEvTablet::TEvCutTabletHistory*>(ev->GetBase()); cut && cut->Record.GetChannel() == 2) {
                UNIT_ASSERT_VALUES_EQUAL(cut->Record.GetFromGeneration(), 0u);
                UNIT_ASSERT_VALUES_EQUAL(cut->Record.GetGroupID(), OldGroup);
                ++cuts;
                ev.Reset();
            }
        });
        f.Restart(NewGroup);
        f.Drive();
        UNIT_ASSERT(!held.empty());
        UNIT_ASSERT_VALUES_EQUAL(cuts, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batches, 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 1u);
        holdGC = false;
        for (auto& ev : held) {
            f.Runtime.Send(ev.Release());
        }
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL_C(cuts, 1u, "empty closed data interval must issue CutHistory");
        UNIT_ASSERT_VALUES_EQUAL(batches, 1u);
        f.Controller->DisableBackground(EBackground::GC);
        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL_C(cuts, 2u, "persisted covering barrier must suffice without fresh GC");
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 2u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("ScanToSend"), 2u);
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(f.Counters()->GetCounter("Deriviative/CutHistory/RequestsSent/Count", true)->Val(), 2u);
        f.Runtime.SendToPipe(
            TabletId, f.Sender, new NMon::TEvRemoteHttpInfo("/app?TabletID=" + ToString(TabletId)), 0, GetPipeConfigWithRetries());
        const auto page = f.Runtime.GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(f.Sender);
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "Persisted CutHistory send attempts");
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "toGeneration=");
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "recipient=");
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "TabletID: " + ToString(TabletId));
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "Channel: 2");
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "FromGeneration: 0");
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "GroupID: " + ToString(OldGroup));
        const auto journalStart = page->Get()->Html.find("<h3>Persisted CutHistory");
        const auto journal = page->Get()->Html.substr(journalStart, page->Get()->Html.find("</pre>", journalStart) - journalStart);
        f.Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(false);
        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(cuts, 2u);
        f.Runtime.SendToPipe(
            TabletId, f.Sender, new NMon::TEvRemoteHttpInfo("/app?TabletID=" + ToString(TabletId)), 0, GetPipeConfigWithRetries());
        const auto rebootedPage = f.Runtime.GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(f.Sender);
        UNIT_ASSERT_STRING_CONTAINS(rebootedPage->Get()->Html, "GroupID: " + ToString(OldGroup));
        UNIT_ASSERT_STRING_CONTAINS(rebootedPage->Get()->Html, journal);
    }

    Y_UNIT_TEST(CleanedPortionWaitsForQueuedBlob) {
        TFixture f;
        f.Controller->DisableBackground(EBackground::Compaction);
        f.Controller->SetOverrideMaxReadStaleness(TDuration::Zero());
        f.Schema();
        f.Write(1, 0, 1000);
        f.Drive();
        const auto oldBlobs = f.LiveOldBlobs();
        UNIT_ASSERT(!oldBlobs.empty());
        TAutoPtr<IEventHandle> continuation;
        bool holdScan = true;
        ui32 cuts = 0;
        ui32 metadataRequests = 0;
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (holdScan && dynamic_cast<TEvPrivate::TEvContinueCutHistory*>(ev->GetBase())) {
                UNIT_ASSERT(!continuation);
                continuation = ev.Release();
            } else if (dynamic_cast<TEvPrivate::TEvAskTabletDataAccessors*>(ev->GetBase())) {
                ++metadataRequests;
            } else if (const auto* cut = dynamic_cast<TEvTablet::TEvCutTabletHistory*>(ev->GetBase()); cut && cut->Record.GetChannel() == 2) {
                UNIT_ASSERT_VALUES_EQUAL(cut->Record.GetFromGeneration(), 0u);
                UNIT_ASSERT_VALUES_EQUAL(cut->Record.GetGroupID(), OldGroup);
                ++cuts;
                ev.Reset();
            }
        });
        f.Restart(NewGroup);
        UNIT_ASSERT(continuation);
        const auto* shard = f.Controller->GetTheOnlyShard();
        const ui32 generation = shard->Generation();
        const ui32 to = f.History.back().first;
        const auto storage =
            std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(shard->GetStoragesManager()->GetDefaultOperator());
        UNIT_ASSERT(storage);
        const auto manager = std::dynamic_pointer_cast<NOlap::TBlobManager>(storage->GetBlobsTracker());
        UNIT_ASSERT(manager);
        f.Drive();
        UNIT_ASSERT(manager->HasCollectedThrough(to));
        UNIT_ASSERT(!storage->HasGCInFlight());
        UNIT_ASSERT(!manager->HasBlobsInRange(2, 0, to));
        f.Controller->DisableBackground(EBackground::GC);
        const auto& index = shard->GetIndexAs<NOlap::TColumnEngineForLogs>();
        UNIT_ASSERT(!index.GetTables().empty());
        bool hadPortion = false;
        for (const auto& [_, granule] : index.GetTables()) {
            hadPortion |= !granule->GetPortions().empty();
        }
        UNIT_ASSERT(hadPortion);
        const auto dropStep = SetupSchema(f.Runtime, f.Sender, TTestSchema::DropTableTxBody(TableId, 2), 1001);
        for (ui32 i = 0; i < 60 && !manager->HasBlobsInRange(2, 0, to); ++i) {
            PlanCommit(f.Runtime, f.Sender, TPlanStep{ dropStep.Val() + i + 1 }, TSet<ui64>{});
            f.Drive(1);
        }
        for (const auto& [_, granule] : index.GetTables()) {
            UNIT_ASSERT(granule->GetPortions().empty());
            UNIT_ASSERT(granule->GetInsertedPortions().empty());
        }
        bool oldBlobQueued = false;
        for (const auto& id : oldBlobs) {
            oldBlobQueued |= storage->HasToDelete(NOlap::TUnifiedBlobId(OldGroup, id), (NOlap::TTabletId)TabletId);
        }
        UNIT_ASSERT(oldBlobQueued);
        UNIT_ASSERT(manager->HasBlobsInRange(2, 0, to));
        UNIT_ASSERT(manager->HasCollectedThrough(to));
        UNIT_ASSERT(!storage->HasGCInFlight());
        UNIT_ASSERT(!storage->GetStopped());
        UNIT_ASSERT(shard->GetSharingSessionsManager()->CanCutHistory());
        UNIT_ASSERT(!storage->GetSharedBlobs()->HasBlobsInRange(2, 0, to));
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 0u);
        metadataRequests = 0;
        holdScan = false;
        f.Runtime.Send(continuation.Release(), 0, true);
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 1u);
        UNIT_ASSERT_VALUES_EQUAL(metadataRequests, 0u);
        UNIT_ASSERT_VALUES_EQUAL_C(cuts, 0u, "queued old blob must be the sole remaining cut blocker");
        f.Controller->EnableBackground(EBackground::GC);
        for (ui32 i = 0; i < 60 && cuts == 0; ++i) {
            ForwardToTablet(f.Runtime, TabletId, f.Sender, new TEvPrivate::TEvPeriodicWakeup());
            f.Runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT(!storage->HasGCInFlight());
        UNIT_ASSERT(!manager->HasBlobsInRange(2, 0, to));
        UNIT_ASSERT(storage->CanCutHistory(2, 0, to));
        UNIT_ASSERT_VALUES_EQUAL(f.Controller->GetTheOnlyShard()->Generation(), generation);
        UNIT_ASSERT_VALUES_EQUAL_C(cuts, 1u, "periodic wakeup must retry after the delete queue drains in the same boot");
    }

    Y_UNIT_TEST(ColdCacheBatchingAndLateSharing) {
        TFixture f;
        f.Schema();
        f.Controller->DisableBackground(EBackground::Compaction);
        f.Controller->DisableBackground(EBackground::Cleanup);
        f.Restart(NewGroup);
        for (ui32 i = 0; i < 40; ++i) {
            f.Write(i + 1, i * 100, (i + 1) * 100);
        }
        TAutoPtr<IEventHandle> continuation;
        IEventHandle* replaying = nullptr;
        ui32 batches = 0;
        ui32 misses = 0;
        ui32 cuts = 0;
        bool linksApplied = false;
        bool failMetadata = false;
        ui32 metadataResults = 0;
        const auto aborted = f.Counters()->GetCounter("Deriviative/CutHistory/ScansAborted/Count", true);
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (dynamic_cast<NOlap::NDataSharing::NEvents::TEvApplyLinksModificationFinished*>(ev->GetBase())) {
                linksApplied = true;
                ev.Reset();
            } else if (dynamic_cast<TEvPrivate::TEvContinueCutHistory*>(ev->GetBase())) {
                if (ev.Get() == replaying) {
                    replaying = nullptr;
                    return;
                }
                ++batches;
                UNIT_ASSERT(!continuation);
                continuation = ev.Release();
            } else if (auto* info = dynamic_cast<TEvPrivate::TEvMetadataAccessorsInfo*>(ev->GetBase()); failMetadata && info) {
                auto result = info->ExtractResult();
                auto data = result.ExtractValue();
                UNIT_ASSERT(!data.GetPortions().empty());
                data.AddError(data.GetPortions().begin()->second->GetPortionInfo().GetPathId(), "injected metadata failure");
                auto* failed = new TEvPrivate::TEvMetadataAccessorsInfo(info->GetProcessor(), info->GetGeneration(),
                    NOlap::NResourceBroker::NSubscribe::TResourceContainer(std::move(data), result.ExtractResourcesGuard()));
                ev.Reset(new IEventHandle(ev->Recipient, ev->Sender, failed, ev->Flags, ev->Cookie));
                failMetadata = false;
                ++metadataResults;
            } else if (const auto* ask = dynamic_cast<TEvPrivate::TEvAskTabletDataAccessors*>(ev->GetBase())) {
                for (const auto& [_, portions] : ask->GetPortions()) {
                    misses += portions.GetPortionsCount();
                }
            } else if (const auto* cut = dynamic_cast<TEvTablet::TEvCutTabletHistory*>(ev->GetBase()); cut && cut->Record.GetChannel() == 2) {
                ++cuts;
                ev.Reset();
            }
        });
        const auto resume = [&] {
            replaying = continuation.Get();
            f.Runtime.Send(continuation.Release(), 0, true);
            f.Runtime.SimulateSleep(TDuration::Seconds(1));
        };
        f.Restart();
        UNIT_ASSERT(continuation);
        UNIT_ASSERT_VALUES_EQUAL(misses, 0u);
        resume();
        UNIT_ASSERT(continuation);
        UNIT_ASSERT_C(misses > 0 && misses <= 32, "first cold metadata batch must load at most 32 portions");
        UNIT_ASSERT_VALUES_EQUAL(f.ReadRows(), 3900u);
        misses = 0;
        f.Write(50, 5000, 5001);
        UNIT_ASSERT_VALUES_EQUAL(f.ReadRows(), 4000u);
        while (continuation) {
            resume();
        }
        f.Drive();
        UNIT_ASSERT_C(batches >= 3, "scan must yield between bounded batches");
        UNIT_ASSERT_C(misses <= 1, "scan must reuse metadata warmed by the foreground read");
        UNIT_ASSERT_VALUES_EQUAL(cuts, 1u);
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 0u);

        f.Restart();
        UNIT_ASSERT(continuation);
        failMetadata = true;
        misses = 0;
        const ui64 scans = f.Samples("Scan");
        resume();
        for (ui32 i = 0; i < 60 && metadataResults == 0; ++i) {
            f.Drive(1);
        }
        UNIT_ASSERT_VALUES_EQUAL(metadataResults, 1u);
        UNIT_ASSERT(misses > 0);
        UNIT_ASSERT(!continuation);
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 1u);
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), scans);
        UNIT_ASSERT_VALUES_EQUAL(cuts, 1u);

        f.Restart();
        UNIT_ASSERT(continuation);
        NOlap::NDataSharing::TTaskForTablet task((NOlap::TTabletId)TabletId);
        f.Runtime.SendToPipe(TabletId, f.Sender, new NOlap::NDataSharing::NEvents::TEvApplyLinksModification((NOlap::TTabletId)TabletId,
                                                     "late-sharing", 0, task), 0, GetPipeConfigWithRetries());
        f.Drive();
        UNIT_ASSERT(linksApplied);
        resume();
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 2u);
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 2u);
        UNIT_ASSERT(!continuation);
        UNIT_ASSERT_VALUES_EQUAL_C(cuts, 1u, "an actual sharing admission must invalidate the unsent boot proof");
    }

    Y_UNIT_TEST(UncommittedPinsOnlyItsInterval) {
        TFixture f;
        f.Schema();
        f.Controller->DisableBackground(EBackground::Compaction);
        f.Controller->DisableBackground(EBackground::Cleanup);
        std::vector<ui64> ids;
        UNIT_ASSERT(WriteData(f.Runtime, f.Sender, TabletId, 1, TableId, MakeTestBlob({ 0, 1000 }, f.Table.Schema), f.Table.Schema, &ids,
            NEvWrite::EModificationType::Upsert, 42));
        UNIT_ASSERT(!f.LiveOldBlobs().empty());
        std::set<ui32> cutFrom;
        auto observer = f.Runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            if (ev->Get()->Record.GetChannel() == 2) {
                UNIT_ASSERT_C(ev->Get()->Record.GetFromGeneration() != 0, "uncommitted old data must pin its interval");
                cutFrom.emplace(ev->Get()->Record.GetFromGeneration());
                ev.Reset();
            }
        });
        f.Restart(NewGroup);
        f.Drive();
        f.Restart(OldGroup);
        f.Drive();
        const ui32 emptyReusedFrom = f.History.back().first;
        f.Restart(NewGroup);
        f.Drive();
        UNIT_ASSERT_C(cutFrom.contains(emptyReusedFrom), "an earlier live use of this group must not pin a later empty interval");
        UNIT_ASSERT(!f.LiveOldBlobs().empty());
    }

    Y_UNIT_TEST(TieredDefaultIndexPinsHistory) {
        TFixture f;
        f.Controller->SetSkipSpecialCheckForEvict(true);
        f.Controller->SetOverrideMaxReadStaleness(TDuration::Zero());
        f.Schema(true);
        f.Write(1, 0, 1000);
        f.Controller->WaitCompactions(TDuration::Seconds(10));
        TTestSchema::TTableSpecials specials;
        TTestSchema::TStorageTier warm("warm");
        warm.EvictAfter = TDuration::Zero();
        specials.Tiers.push_back(warm);
        f.Controller->OverrideTierConfigs(f.Runtime, f.Sender, TTestSchema::BuildSnapshot(specials));
        f.Controller->EnableBackground(EBackground::TTL);
        ForwardToTablet(f.Runtime, TabletId, f.Sender, new TEvPrivate::TEvPeriodicWakeup());
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
        while ((!f.Controller->GetTTLStartedCounter().Val() ||
                   f.Controller->GetTTLFinishedCounter().Val() < f.Controller->GetTTLStartedCounter().Val()) &&
               TInstant::Now() < deadline) {
            f.Runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT(f.Controller->GetTTLFinishedCounter().Val());
        UNIT_ASSERT_VALUES_EQUAL(f.Controller->GetTTLFinishedCounter().Val(), f.Controller->GetTTLStartedCounter().Val());
        f.Drive();
        const auto& index = f.Controller->GetTheOnlyShard()->GetIndexAs<NOlap::TColumnEngineForLogs>();
        ui32 livePortions = 0;
        for (const auto& [_, granule] : index.GetTables()) {
            for (const auto& [_, portion] : granule->GetPortions()) {
                if (portion->HasRemoveSnapshot()) {
                    continue;
                }
                ++livePortions;
                const auto& schema = portion->GetSchema(index.GetVersionedIndex())->GetIndexInfo();
                UNIT_ASSERT_VALUES_EQUAL(portion->GetColumnStorageId(1, schema), CanonizePath(warm.Name));
                UNIT_ASSERT_VALUES_EQUAL(portion->GetIndexStorageId(3000, schema), NOlap::IStoragesManager::DefaultStorageId);
            }
        }
        UNIT_ASSERT(livePortions);
        const auto oldBlobs = f.LiveOldBlobs();
        UNIT_ASSERT_C(!oldBlobs.empty(), "DEFAULT index must remain after column payload eviction");
        auto observer = f.Runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            UNIT_ASSERT_C(ev->Get()->Record.GetChannel() != 2, "tiered portion's DEFAULT index must pin history");
            ev.Reset();
        });
        const ui64 scans = f.Samples("Scan");
        f.Restart(NewGroup);
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), scans + 1);
        UNIT_ASSERT_VALUES_EQUAL(f.LiveOldBlobs().size(), oldBlobs.size());
    }
}
}   // namespace NKikimr::NColumnShard
