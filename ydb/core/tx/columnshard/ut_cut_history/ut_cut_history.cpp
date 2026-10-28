#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/tablet/tablet_impl.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

#include <ydb/library/testlib/helpers.h>

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
    std::vector<TDuration> ContinuationDelays;

    TFixture() {
        TTester::Setup(
            Runtime, { new NFake::TProxyDS(TGroupId::FromValue(0)), OldProxy, NewProxy, new NFake::TProxyDS(TGroupId::FromValue(Max<ui32>())) });
        Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(true);
        Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(true);
        Runtime.SetScheduledLimit(10000);
        auto previous = Runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&, TDuration, TInstant&) {
            return true;
        });
        Runtime.SetScheduledEventFilter(
            [this, previous](TTestActorRuntimeBase& r, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline) {
                if (event->HasEvent() && dynamic_cast<TEvPrivate::TEvContinueFindEmptyHistoryIntervals*>(event->GetBase())) {
                    ContinuationDelays.push_back(delay);
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
        info->Channels.resize(FirstDataChannel + 1);
        for (ui32 channel = 0; channel < info->Channels.size(); ++channel) {
            auto& ch = info->Channels[channel];
            ch.Channel = channel;
            ch.Type = TBlobStorageGroupType(BootGroupErasure);
            ch.History.emplace_back(0, OldGroup);
            if (channel == FirstDataChannel) {
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

    void Schema(const bool tieredIndex = false, const ui32 tableCount = 1, const bool inheritPortionStorage = false) {
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
            *schema->AddIndexes() =
                NOlap::NIndexes::TIndexMetaContainer(std::make_shared<NOlap::NIndexes::NMax::TIndexMeta>(
                                                         3000, "ts_max_bs", NOlap::IStoragesManager::DefaultStorageId, inheritPortionStorage, 1))
                    .SerializeToProto();
            TTestSchema::InitTiersAndTtl(specials, table->MutableTtlSettings());
        }
        for (ui32 i = 1; i < tableCount; ++i) {
            auto* nextTable = init->AddTables();
            *nextTable = *table;
            TSchemeShardLocalPathId::FromRawValue(TableId + i).ToProto(*nextTable);
        }
        ReadStep = SetupSchema(Runtime, Sender, tx.SerializeAsString(), 1000);
    }

    TTestSchema::TStorageTier EvictToWarm() {
        TTestSchema::TTableSpecials specials;
        TTestSchema::TStorageTier warm("warm");
        warm.EvictAfter = TDuration::Zero();
        specials.Tiers.push_back(warm);
        Controller->OverrideTierConfigs(Runtime, Sender, TTestSchema::BuildSnapshot(specials));
        Controller->EnableBackground(EBackground::TTL);
        ForwardToTablet(Runtime, TabletId, Sender, new TEvPrivate::TEvPeriodicWakeup());
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
        while ((!Controller->GetTTLStartedCounter().Val() ||
                   Controller->GetTTLFinishedCounter().Val() < Controller->GetTTLStartedCounter().Val()) &&
               TInstant::Now() < deadline) {
            Runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT(Controller->GetTTLFinishedCounter().Val());
        UNIT_ASSERT_VALUES_EQUAL(Controller->GetTTLFinishedCounter().Val(), Controller->GetTTLStartedCounter().Val());
        Drive();
        return warm;
    }

    void Write(ui64 txId, ui64 from, ui64 to, ui64 tableId = TableId) {
        std::vector<ui64> ids;
        UNIT_ASSERT(WriteData(Runtime, Sender, TabletId, txId, tableId, MakeTestBlob({ from, to }, Table.Schema), Table.Schema, &ids));
        ReadStep = ProposeCommit(Runtime, Sender, TabletId, txId, ids);
        PlanCommit(Runtime, Sender, TabletId, ReadStep, TSet<ui64>{ txId });
    }

    ui64 ReadRows(ui64 tableId = TableId) {
        return ReadAllAsBatch(Runtime, tableId, NOlap::TSnapshot(ReadStep.Val(), 1), Table.Schema)->num_rows();
    }

    std::vector<TLogoBlobID> LiveOldBlobs() const {
        std::vector<TLogoBlobID> result;
        for (const auto& [id, blob] : OldProxy->AllMyBlobs()) {
            if (id.TabletID() == TabletId && id.Channel() >= FirstDataChannel && !blob.DoNotKeep) {
                result.push_back(id);
            }
        }
        return result;
    }

    TString Journal() {
        Runtime.SendToPipe(
            TabletId, Sender, new NMon::TEvRemoteHttpInfo("/app?page=cuthistory&TabletID=" + ToString(TabletId)), 0, GetPipeConfigWithRetries());
        const auto page = Runtime.GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(Sender);
        const auto start = page->Get()->Html.find("<pre>") + 5;
        return page->Get()->Html.substr(start, page->Get()->Html.find("</pre>", start) - start);
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
        ui32 batches = 0;
        f.Controller->DisableBackground(EBackground::GC);
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (dynamic_cast<TEvPrivate::TEvContinueFindEmptyHistoryIntervals*>(ev->GetBase())) {
                ++batches;
            }
        });
        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(false);
        f.Restart(NewGroup);
        f.Restart();
        f.Drive();
        UNIT_ASSERT(f.Runtime.GetAppData().FeatureFlags.GetEnableCutHistory());
        UNIT_ASSERT_VALUES_EQUAL(batches, 0u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 0u);
        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(true);
        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(batches, 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 1u);
        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 2u);
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(f.Counters()->GetCounter("Deriviative/CutHistory/CuttableIntervals/Count", true)->Val(), 2u);
        f.Runtime.SendToPipe(
            TabletId, f.Sender, new NMon::TEvRemoteHttpInfo("/app?TabletID=" + ToString(TabletId)), 0, GetPipeConfigWithRetries());
        const auto mainPage = f.Runtime.GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(f.Sender);
        UNIT_ASSERT_STRING_CONTAINS(mainPage->Get()->Html, "page=cuthistory&amp;TabletID=");
        UNIT_ASSERT(!mainPage->Get()->Html.Contains("Persisted CutHistory request intents"));
        f.Runtime.SendToPipe(TabletId, f.Sender, new NMon::TEvRemoteHttpInfo("/app?page=cuthistory&TabletID=" + ToString(TabletId)), 0,
            GetPipeConfigWithRetries());
        const auto page = f.Runtime.GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(f.Sender);
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "Persisted CutHistory request intents");
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "ToGeneration: " + ToString(f.History.back().first));
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "Recipient {");
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "TimestampUs: ");
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "SendingGeneration: " + ToString(f.Controller->GetTheOnlyShard()->Generation()));
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "TabletID: " + ToString(TabletId));
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "Channel: " + ToString(FirstDataChannel));
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "FromGeneration: 0");
        UNIT_ASSERT_STRING_CONTAINS(page->Get()->Html, "GroupID: " + ToString(OldGroup));
        const auto journalStart = page->Get()->Html.find("<pre>") + 5;
        const auto journal = page->Get()->Html.substr(journalStart, page->Get()->Html.find("</pre>", journalStart) - journalStart);
        f.Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(false);
        f.Restart();
        f.Drive();
        f.Runtime.SendToPipe(TabletId, f.Sender, new NMon::TEvRemoteHttpInfo("/app?page=cuthistory&TabletID=" + ToString(TabletId)), 0,
            GetPipeConfigWithRetries());
        const auto rebootedPage = f.Runtime.GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(f.Sender);
        UNIT_ASSERT_STRING_CONTAINS(rebootedPage->Get()->Html, "GroupID: " + ToString(OldGroup));
        UNIT_ASSERT_STRING_CONTAINS(rebootedPage->Get()->Html, journal);
    }

    Y_UNIT_TEST(CurrentAndFutureIntervalsAreSkipped) {
        TFixture f;
        f.Controller->DisableBackground(EBackground::GC);
        const ui32 nextGeneration = f.Controller->GetTheOnlyShard()->Generation() + 1;
        f.History.emplace_back(nextGeneration, NewGroup);
        f.History.emplace_back(nextGeneration + 1, OldGroup);
        f.History.emplace_back(nextGeneration + 2, NewGroup);
        f.Restart();
        UNIT_ASSERT_VALUES_EQUAL(f.Controller->GetTheOnlyShard()->Generation(), nextGeneration);
        f.Drive();
        UNIT_ASSERT(f.Journal().empty());
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 0u);

        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(f.Counters()->GetCounter("Deriviative/CutHistory/CuttableIntervals/Count", true)->Val(), 1u);
        UNIT_ASSERT_STRING_CONTAINS(f.Journal(), "FromGeneration: 0");
    }

    Y_UNIT_TEST(ColdCacheBatchingAndMetadataFailure) {
        constexpr ui64 portionCount = 7;
        TFixture f;
        auto* config = f.Runtime.GetAppData().ColumnShardConfig.MutableCutHistory();
        config->SetScanBatchSize(2);
        config->SetContinuationDelayMs(3);
        config->SetContinuationJitterMs(2);
        f.Schema(false, 2);
        f.Controller->DisableBackground(EBackground::Compaction);
        f.Controller->DisableBackground(EBackground::Cleanup);
        f.Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(false);
        f.Restart(NewGroup);
        for (ui64 i = 0; i < portionCount; ++i) {
            f.Write(i + 1, i, i + 1, TableId + i % 2);
        }
        f.Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(true);
        UNIT_ASSERT(f.LiveOldBlobs().empty());
        const auto found = f.Counters()->GetCounter("Deriviative/CutHistory/CuttableIntervals/Count", true);
        const auto aborted = f.Counters()->GetCounter("Deriviative/CutHistory/ScansAborted/Count", true);
        bool failMetadata = false;
        ui32 scanBatches = 0;
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (auto* info = dynamic_cast<TEvPrivate::TEvMetadataAccessorsInfo*>(ev->GetBase())) {
                auto result = info->ExtractResult();
                auto data = result.ExtractValue();
                UNIT_ASSERT(!data.GetPortions().empty());
                UNIT_ASSERT(data.GetPortions().size() <= Max<ui32>(1, config->GetScanBatchSize()));
                ++scanBatches;
                if (failMetadata) {
                    data.AddError(data.GetPortions().begin()->second->GetPortionInfo().GetPathId(), "injected metadata failure");
                }
                auto* failed = new TEvPrivate::TEvMetadataAccessorsInfo(info->GetProcessor(), info->GetGeneration(),
                    NOlap::NResourceBroker::NSubscribe::TResourceContainer(std::move(data), result.ExtractResourcesGuard()));
                ev.Reset(new IEventHandle(ev->Recipient, ev->Sender, failed, ev->Flags, ev->Cookie));
                failMetadata = false;
            }
        });
        f.Controller->DisableBackground(EBackground::GC);
        f.ContinuationDelays.clear();
        f.Restart();
        for (ui32 i = 0; i < 60 && found->Val() == 0; ++i) {
            f.Drive(1);
        }
        UNIT_ASSERT(scanBatches >= 4);
        UNIT_ASSERT(!f.ContinuationDelays.empty());
        for (const auto delay : f.ContinuationDelays) {
            UNIT_ASSERT(delay >= TDuration::MilliSeconds(3) && delay <= TDuration::MilliSeconds(5));
        }
        UNIT_ASSERT_VALUES_EQUAL(found->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 0u);

        failMetadata = true;
        config->SetScanBatchSize(0);
        config->SetScanMemoryLimitBytes(0);
        config->SetContinuationDelayMs(0);
        config->SetContinuationJitterMs(0);
        f.ContinuationDelays.clear();
        f.Restart();
        for (ui32 i = 0; i < 60 && aborted->Val() == 0; ++i) {
            f.Drive(1);
        }
        f.Drive();
        UNIT_ASSERT(!failMetadata);
        UNIT_ASSERT(!f.ContinuationDelays.empty());
        for (const auto delay : f.ContinuationDelays) {
            UNIT_ASSERT_VALUES_EQUAL(delay, TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(found->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 1u);
    }

    Y_UNIT_TEST(UncommittedPinsOnlyItsInterval) {
        TFixture f;
        f.Schema();
        f.Controller->DisableBackground(EBackground::Compaction);
        f.Controller->DisableBackground(EBackground::Cleanup);
        f.Runtime.GetAppData().ColumnShardConfig.MutableCutHistory()->SetScanBatchSize(1);
        std::vector<ui64> ids;
        for (ui64 txId = 1; txId <= 2; ++txId) {
            UNIT_ASSERT(WriteData(f.Runtime, f.Sender, TabletId, txId, TableId, MakeTestBlob({ (txId - 1) * 1000, txId * 1000 }, f.Table.Schema),
                f.Table.Schema, &ids, NEvWrite::EModificationType::Upsert, 42));
        }
        UNIT_ASSERT(!f.LiveOldBlobs().empty());
        f.Restart(NewGroup);
        f.Drive();
        f.ContinuationDelays.clear();
        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(f.ContinuationDelays.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 1u);
        f.Restart(OldGroup);
        f.Drive();
        const ui32 emptyReusedFrom = f.History.back().first;
        f.Restart(NewGroup);
        f.Restart();
        f.Drive();
        const auto journal = f.Journal();
        UNIT_ASSERT_C(!journal.Contains("FromGeneration: 0\n"), "uncommitted old data must pin its interval");
        UNIT_ASSERT_STRING_CONTAINS(journal, "FromGeneration: " + ToString(emptyReusedFrom) + "\n");
        UNIT_ASSERT(!f.LiveOldBlobs().empty());
    }

    Y_UNIT_TEST_TWIN(TieredIndexStorageControlsHistory, inheritPortionStorage) {
        TFixture f;
        f.Controller->SetSkipSpecialCheckForEvict(true);
        f.Controller->SetOverrideMaxReadStaleness(TDuration::Zero());
        f.Schema(true, 1, inheritPortionStorage);
        f.Write(1, 0, 1000);
        f.Write(2, 0, 1000);
        for (ui32 i = 0; i < 60 && !f.Controller->GetCompactionFinishedCounter().Val(); ++i) {
            f.Drive(1);
        }
        UNIT_ASSERT(f.Controller->GetCompactionFinishedCounter().Val());
        const auto& index = f.Controller->GetTheOnlyShard()->GetIndexAs<NOlap::TColumnEngineForLogs>();
        for (const auto& [_, granule] : index.GetTables()) {
            for (const auto& [_, portion] : granule->GetPortions()) {
                if (!portion->HasRemoveSnapshot()) {
                    UNIT_ASSERT(portion->GetPortionType() == NOlap::EPortionType::Compacted);
                    UNIT_ASSERT(portion->GetIndexBlobBytes());
                }
            }
        }
        const auto warm = f.EvictToWarm();
        ui32 livePortions = 0;
        ui64 cleanupStep = f.ReadStep.Val();
        for (const auto& [_, granule] : index.GetTables()) {
            for (const auto& [_, portion] : granule->GetPortions()) {
                if (portion->HasRemoveSnapshot()) {
                    cleanupStep = Max(cleanupStep, portion->GetRemoveSnapshotVerified().GetPlanStep());
                    continue;
                }
                ++livePortions;
                UNIT_ASSERT(portion->GetIndexBlobBytes());
                const auto& schema = portion->GetSchema(index.GetVersionedIndex())->GetIndexInfo();
                UNIT_ASSERT_VALUES_EQUAL(portion->GetColumnStorageId(1, schema), CanonizePath(warm.Name));
                UNIT_ASSERT_VALUES_EQUAL(portion->GetIndexStorageId(3000, schema),
                    inheritPortionStorage ? CanonizePath(warm.Name) : NOlap::IStoragesManager::DefaultStorageId);
            }
        }
        UNIT_ASSERT(livePortions);
        f.ReadStep = TPlanStep{ cleanupStep + 1 };
        PlanCommit(f.Runtime, f.Sender, TabletId, f.ReadStep, TSet<ui64>{});
        f.Drive();
        ui32 obsoletePortions = 0;
        for (ui32 i = 0; i < 60; ++i) {
            obsoletePortions = 0;
            for (const auto& [_, granule] : index.GetTables()) {
                for (const auto& [_, portion] : granule->GetPortions()) {
                    obsoletePortions += portion->HasRemoveSnapshot();
                }
            }
            if (!obsoletePortions && (!inheritPortionStorage || f.LiveOldBlobs().empty())) {
                break;
            }
            f.Drive(1);
        }
        UNIT_ASSERT_VALUES_EQUAL(obsoletePortions, 0u);
        const auto oldBlobs = f.LiveOldBlobs();
        UNIT_ASSERT_VALUES_EQUAL_C(oldBlobs.empty(), inheritPortionStorage, "only a DEFAULT index keeps old local blobs after eviction");
        const auto found = f.Counters()->GetCounter("Deriviative/CutHistory/CuttableIntervals/Count", true);
        const ui64 foundBefore = found->Val();
        const ui64 scans = f.Samples("Scan");
        f.Restart(NewGroup);
        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), scans + 1);
        UNIT_ASSERT_VALUES_EQUAL(found->Val() - foundBefore, inheritPortionStorage ? 1u : 0u);
        UNIT_ASSERT_VALUES_EQUAL(f.LiveOldBlobs().size(), oldBlobs.size());
        UNIT_ASSERT_VALUES_EQUAL(f.ReadRows(), 1000u);
    }
}
}   // namespace NKikimr::NColumnShard
