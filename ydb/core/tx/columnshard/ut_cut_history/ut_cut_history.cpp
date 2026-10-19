#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/tablet/tablet_impl.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/sessions.h>
#include <ydb/core/tx/columnshard/data_sharing/modification/tasks/modification.h>
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

using TCachePolicy = NOlap::NGeneralCache::TPortionsMetadataCachePolicy;
using TCacheEvents = NGeneralCache::NPublic::TEvents<TCachePolicy>;
using TCacheCallback = NGeneralCache::NPublic::ICallback<TCachePolicy>;

class TFailMetadataCallback: public TCacheCallback {
    std::shared_ptr<TCacheCallback> Callback;

    bool DoIsAborted() const override {
        return Callback->IsAborted();
    }

    void DoOnResultReady(THashMap<TCachePolicy::TAddress, TCachePolicy::TObject>&& objects, THashSet<TCachePolicy::TAddress>&& removed,
        NGeneralCache::NPublic::TErrorAddresses<TCachePolicy>&&) override {
        UNIT_ASSERT(!objects.empty());
        THashMap<TCachePolicy::TAddress, TString> errors;
        errors.emplace(objects.begin()->first, "injected metadata failure");
        Callback->OnResultReady(
            std::move(objects), std::move(removed), NGeneralCache::NPublic::TErrorAddresses<TCachePolicy>(std::move(errors)));
    }

public:
    explicit TFailMetadataCallback(std::shared_ptr<TCacheCallback> callback)
        : Callback(std::move(callback))
    {
    }
};

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
    std::vector<std::pair<ui32, ui32>> SecondChannelHistory;
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
                if (event->HasEvent() && dynamic_cast<TEvFindEmptyHistoryIntervals::TEvContinue*>(event->GetBase())) {
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
        info->Channels.resize(FirstDataChannel + 1 + !SecondChannelHistory.empty());
        for (ui32 channel = 0; channel < info->Channels.size(); ++channel) {
            auto& ch = info->Channels[channel];
            ch.Channel = channel;
            ch.Type = TBlobStorageGroupType(BootGroupErasure);
            if (channel == FirstDataChannel + 1) {
                for (const auto& [from, group] : SecondChannelHistory) {
                    ch.History.emplace_back(from, group);
                }
                continue;
            }
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

    void PrepareThreeClosedIntervals() {
        Schema();
        Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(false);
        Restart(NewGroup);
        Restart(OldGroup);
        Restart(NewGroup);
        Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(true);
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
        ui32 cuts = 0;
        ui32 batches = 0;
        ui32 gcCompletions = 0;
        f.Schema();
        f.Drive();
        f.Controller->DisableBackground(EBackground::GC);
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (dynamic_cast<TEvFindEmptyHistoryIntervals::TEvContinue*>(ev->GetBase())) {
                ++batches;
            } else if (dynamic_cast<TEvPrivate::TEvGarbageCollectionFinished*>(ev->GetBase())) {
                ++gcCompletions;
            } else if (const auto* cut = dynamic_cast<TEvTablet::TEvCutTabletHistory*>(ev->GetBase());
                       cut && cut->Record.GetChannel() == FirstDataChannel) {
                UNIT_ASSERT_VALUES_EQUAL(cut->Record.GetFromGeneration(), 0u);
                UNIT_ASSERT_VALUES_EQUAL(cut->Record.GetGroupID(), OldGroup);
                ++cuts;
                ev.Reset();
            }
        });
        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(false);
        f.Restart(NewGroup);
        f.Restart();
        f.Drive();
        UNIT_ASSERT(f.Runtime.GetAppData().FeatureFlags.GetEnableCutHistory());
        UNIT_ASSERT_VALUES_EQUAL(cuts, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batches, 0u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 0u);
        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(true);
        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL_C(cuts, 0u, "empty metadata cannot rule out orphaned blobs before GC");
        UNIT_ASSERT_VALUES_EQUAL(batches, 0u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 0u);
        f.Controller->EnableBackground(EBackground::GC);
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(cuts, 1u);
        UNIT_ASSERT_VALUES_EQUAL(batches, 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 1u);
        const auto gcCompletionsBeforeWrite = gcCompletions;
        f.Write(1, 0, 1);
        f.Drive();
        UNIT_ASSERT(gcCompletions > gcCompletionsBeforeWrite);
        UNIT_ASSERT_VALUES_EQUAL(cuts, 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 1u);
        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL_C(cuts, 2u, "empty interval must remain eligible after first GC on reboot");
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 2u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("ScanToSend"), 2u);
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(f.Counters()->GetCounter("Deriviative/CutHistory/RequestsSent/Count", true)->Val(), 2u);
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
        const auto journal = f.Journal();
        f.Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(false);
        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(cuts, 2u);
        UNIT_ASSERT_VALUES_EQUAL(f.Journal(), journal);
    }

    Y_UNIT_TEST(RequestLimitKeepsEverySentBatchCommitted) {
        TFixture f;
        f.Schema();
        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(false);
        constexpr ui32 intervalCount = CutHistoryRequestLimit + 1;
        for (ui32 i = 0; i < intervalCount; ++i) {
            f.Restart(i % 2 ? OldGroup : NewGroup);
        }
        TAutoPtr<IEventHandle> continuation;
        TAutoPtr<IEventHandle> commit;
        bool holdContinuation = true;
        bool holdCommit = false;
        ui32 lastHeldCommitStep = 0;
        std::set<ui32> cutFrom;
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (holdContinuation && dynamic_cast<TEvFindEmptyHistoryIntervals::TEvContinue*>(ev->GetBase())) {
                continuation = ev.Release();
            } else if (const auto* log = dynamic_cast<TEvTabletBase::TEvWriteLogResult*>(ev->GetBase());
                       holdCommit && log && log->EntryId.TabletID() == TabletId && log->EntryId.Cookie() == 0 &&
                       log->EntryId.Step() > lastHeldCommitStep) {
                lastHeldCommitStep = log->EntryId.Step();
                commit = ev.Release();
                holdCommit = false;
            } else if (const auto* cut = dynamic_cast<TEvTablet::TEvCutTabletHistory*>(ev->GetBase())) {
                UNIT_ASSERT_VALUES_EQUAL(cut->Record.GetChannel(), FirstDataChannel);
                UNIT_ASSERT(cutFrom.emplace(cut->Record.GetFromGeneration()).second);
                ev.Reset();
            }
        });
        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(true);
        f.Restart();
        f.Drive(1);
        UNIT_ASSERT(continuation);
        f.Controller->DisableBackground(EBackground::GC);
        holdContinuation = false;
        holdCommit = true;
        f.Runtime.Send(continuation.Release(), 0, true);
        f.Drive(1);
        UNIT_ASSERT(commit);
        UNIT_ASSERT_VALUES_EQUAL_C(cutFrom.size(), 0u, "the first batch must wait for its journal commit");

        holdCommit = true;
        f.Runtime.Send(commit.Release(), 0, true);
        f.Drive(1);
        UNIT_ASSERT_VALUES_EQUAL_C(cutFrom.size(), CutHistoryRequestLimit, "the next batch must wait for its own journal commit");
        UNIT_ASSERT_C(commit, "remaining intervals must continue without another scan");
        f.Runtime.Send(commit.Release(), 0, true);
        f.Drive(1);
        UNIT_ASSERT_VALUES_EQUAL(cutFrom.size(), intervalCount);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("ScanToSend"), intervalCount);
        const auto journal = f.Journal();
        UNIT_ASSERT(!journal.Contains("FromGeneration: 0\n"));
        for (ui32 i = 1; i < intervalCount; ++i) {
            UNIT_ASSERT(cutFrom.contains(f.History[i].first));
            UNIT_ASSERT_STRING_CONTAINS(journal, "FromGeneration: " + ToString(f.History[i].first) + "\n");
        }
    }

    Y_UNIT_TEST_TWIN(OrphanCollectedWithSwappedChannelGroups, restartDuringGC) {
        TFixture f;
        f.Schema();
        f.Drive();
        f.Controller->DisableBackground(EBackground::GC);
        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(false);
        f.SecondChannelHistory = { { 0, NewGroup } };
        while (f.Controller->GetTheOnlyShard()->Generation() < 10) {
            f.Restart();
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Controller->GetTheOnlyShard()->Generation(), 10u);
        TLogoBlobID blob;
        {
            const auto storage = f.Controller->GetTheOnlyShard()->GetStoragesManager()->GetDefaultOperator();
            const auto manager = std::dynamic_pointer_cast<NOlap::TBlobManager>(storage->GetBlobsTracker());
            UNIT_ASSERT(manager);
            const TString data = "blob";
            for (ui32 attempt = 0; attempt < 2; ++attempt) {
                auto batch = manager->StartBlobBatch();
                const auto id = batch.AllocateNextBlobId(data);
                if (id.GetLogoBlobId().Channel() != FirstDataChannel) {
                    continue;
                }
                blob = id.GetLogoBlobId();
                UNIT_ASSERT_VALUES_EQUAL(id.GetDsGroup(), OldGroup);
                f.Runtime.Send(new IEventHandle(MakeBlobStorageProxyID(OldGroup), f.Sender,
                    new TEvBlobStorage::TEvPut(blob, data, TInstant::Max(), NKikimrBlobStorage::UserData)));
                const auto result = f.Runtime.GrabEdgeEvent<TEvBlobStorage::TEvPutResult>(f.Sender);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
                batch.OnBlobWriteResult(blob, result->Get()->Status);
                break;   // Discard the batch without saving Keep or a portion reference.
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(blob.Generation(), 10u);
        const auto getStatus = [&] {
            f.Runtime.Send(new IEventHandle(MakeBlobStorageProxyID(OldGroup), f.Sender,
                new TEvBlobStorage::TEvGet(blob, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead)));
            const auto result = f.Runtime.GrabEdgeEvent<TEvBlobStorage::TEvGetResult>(f.Sender);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
            return result->Get()->Responses[0].Status;
        };
        std::map<std::pair<ui32, ui32>, TGenStep> barriers;
        std::map<ui32, ui32> cuts;
        TAutoPtr<IEventHandle> orphanChannelGC;
        TAutoPtr<IEventHandle> completion;
        ui32 preparedGeneration = 0;
        bool holdCompletion = true;
        bool holdOrphanChannel = true;
        bool oppositeChannelCollected = false;
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (const auto* gc = dynamic_cast<TEvBlobStorage::TEvCollectGarbage*>(ev->GetBase());
                gc && gc->TabletId == TabletId && gc->Channel >= FirstDataChannel && gc->Collect) {
                UNIT_ASSERT(!gc->Hard);
                UNIT_ASSERT(!gc->Keep || gc->Keep->empty());
                UNIT_ASSERT(!gc->DoNotKeep || gc->DoNotKeep->empty());
                barriers.emplace(std::make_pair(gc->Channel, ev->Cookie), TGenStep(gc->CollectGeneration, gc->CollectStep));
                if (holdOrphanChannel && gc->Channel == FirstDataChannel && ev->Cookie == OldGroup) {
                    UNIT_ASSERT(!orphanChannelGC);
                    if (!preparedGeneration) {
                        preparedGeneration = gc->CollectGeneration;
                    }
                    orphanChannelGC = ev.Release();
                }
            } else if (const auto* result = dynamic_cast<TEvBlobStorage::TEvCollectGarbageResult*>(ev->GetBase());
                       result && result->Channel == FirstDataChannel + 1 && ev->Cookie == OldGroup) {
                UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);
                oppositeChannelCollected = true;
            } else if (holdCompletion && dynamic_cast<TEvPrivate::TEvGarbageCollectionFinished*>(ev->GetBase())) {
                UNIT_ASSERT(!completion);
                completion = ev.Release();
            } else if (const auto* cut = dynamic_cast<TEvTablet::TEvCutTabletHistory*>(ev->GetBase())) {
                UNIT_ASSERT_VALUES_EQUAL(cut->Record.GetFromGeneration(), 0u);
                UNIT_ASSERT(cuts.emplace(cut->Record.GetChannel(), cut->Record.GetGroupID()).second);
                ev.Reset();
            }
        });
        f.SecondChannelHistory.emplace_back(11, OldGroup);
        f.Restart(NewGroup);
        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(true);
        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 0u);
        UNIT_ASSERT(cuts.empty());
        UNIT_ASSERT_VALUES_EQUAL(getStatus(), NKikimrProto::OK);
        f.Controller->EnableBackground(EBackground::GC);
        f.Drive();
        UNIT_ASSERT(orphanChannelGC);
        UNIT_ASSERT(oppositeChannelCollected);
        UNIT_ASSERT_VALUES_EQUAL_C(getStatus(), NKikimrProto::OK, "collecting the same group on another channel must not collect blob");
        if (restartDuringGC) {
            // Reboot with durable preparation, but without completion of the orphan's channel.
            orphanChannelGC.Reset();
            barriers.clear();
            oppositeChannelCollected = false;
            f.Restart();
            f.Drive();
            UNIT_ASSERT(preparedGeneration < f.Controller->GetTheOnlyShard()->Generation());
            UNIT_ASSERT(f.History.back().first <= preparedGeneration);
            UNIT_ASSERT(orphanChannelGC);
            UNIT_ASSERT(oppositeChannelCollected);
            UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 0u);
        }
        UNIT_ASSERT_VALUES_EQUAL(barriers.size(), 4u);
        for (ui32 channel = FirstDataChannel; channel <= FirstDataChannel + 1; ++channel) {
            for (ui32 group : { OldGroup, NewGroup }) {
                UNIT_ASSERT(barriers.at({ channel, group }) == TGenStep(f.Controller->GetTheOnlyShard()->Generation(), 0));
                UNIT_ASSERT(barriers.at({ channel, group }) >= TGenStep(preparedGeneration, 0));
            }
        }
        UNIT_ASSERT(cuts.empty());
        UNIT_ASSERT_VALUES_EQUAL_C(getStatus(), NKikimrProto::OK, "collecting the same group on another channel must not collect blob");
        holdOrphanChannel = false;
        f.Runtime.Send(orphanChannelGC.Release(), 0, true);
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(getStatus(), NKikimrProto::NODATA);
        UNIT_ASSERT(completion);
        UNIT_ASSERT_C(cuts.empty(), "all BS acknowledgements must be followed by durable shard GC completion");
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 0u);
        holdCompletion = false;
        f.Runtime.Send(completion.Release(), 0, true);
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(cuts.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(cuts.at(FirstDataChannel), OldGroup);
        UNIT_ASSERT_VALUES_EQUAL(cuts.at(FirstDataChannel + 1), NewGroup);
        UNIT_ASSERT_VALUES_EQUAL(f.Counters()->GetCounter("Deriviative/CutHistory/RequestsSent/Count", true)->Val(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("ScanToSend"), cuts.size());
    }

    Y_UNIT_TEST_TWIN(SharedBlobsExcludeOnlyOwnInterval, borrowed) {
        TFixture f;
        f.PrepareThreeClosedIntervals();
        const auto selfTablet = static_cast<NOlap::TTabletId>(TabletId);
        const auto otherTablet = static_cast<NOlap::TTabletId>(TabletId + 1);
        NOlap::NDataSharing::TStorageTabletTask storageTask(NOlap::IStoragesManager::DefaultStorageId, selfTablet);
        for (const auto& id : { TLogoBlobID(TabletId, f.History[1].first, 1, FirstDataChannel, 10, 0),
                 TLogoBlobID(TabletId + 1, 1, 1, FirstDataChannel, 10, 0), TLogoBlobID(TabletId, 1, 1, FirstDataChannel + 1, 10, 0) }) {
            const NOlap::TUnifiedBlobId blob(NewGroup, id);
            if (borrowed) {
                storageTask.AddInitOwner(blob, otherTablet);
            } else {
                storageTask.AddLink(blob, otherTablet);
            }
        }
        NOlap::NDataSharing::TTaskForTablet task(selfTablet);
        task.AddStorage(std::move(storageTask));
        f.Runtime.SendToPipe(TabletId, f.Sender, new NOlap::NDataSharing::NEvents::TEvApplyLinksModification(selfTablet,
                                                     "persisted-cut-history-links", 0, task), 0, GetPipeConfigWithRetries());
        f.Drive();
        UNIT_ASSERT(!f.Controller->GetTheOnlyShard()->GetSharingSessionsManager()->CanCutHistory());
        THashSet<ui32> cuts;
        auto observer = f.Runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetChannel(), FirstDataChannel);
            cuts.insert(ev->Get()->Record.GetFromGeneration());
            ev.Reset();
        });
        f.Restart();
        UNIT_ASSERT(f.Controller->GetTheOnlyShard()->GetSharingSessionsManager()->CanCutHistory());
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(cuts.size(), 2u);
        UNIT_ASSERT(cuts.contains(0));
        UNIT_ASSERT(cuts.contains(f.History[2].first));
    }

    Y_UNIT_TEST(CurrentAndFutureIntervalsAreSkipped) {
        TFixture f;
        f.Schema();
        const ui32 nextGeneration = f.Controller->GetTheOnlyShard()->Generation() + 1;
        f.History.emplace_back(nextGeneration, NewGroup);
        f.History.emplace_back(nextGeneration + 1, OldGroup);
        f.History.emplace_back(nextGeneration + 2, NewGroup);
        std::vector<ui32> cutFrom;
        auto observer = f.Runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            if (ev->Get()->Record.GetChannel() == FirstDataChannel) {
                cutFrom.push_back(ev->Get()->Record.GetFromGeneration());
                ev.Reset();
            }
        });
        f.Restart();
        UNIT_ASSERT_VALUES_EQUAL(f.Controller->GetTheOnlyShard()->Generation(), nextGeneration);
        f.Drive();
        UNIT_ASSERT_C(cutFrom.empty(), "intervals ending at or after the current generation must not be cut");
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 0u);

        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(cutFrom.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(cutFrom.front(), 0u);
    }

    Y_UNIT_TEST(TableDroppedAfterSnapshotAbortsScan) {
        TFixture f;
        f.Controller->DisableBackground(EBackground::Compaction);
        f.Controller->SetOverrideMaxReadStaleness(TDuration::Zero());
        f.Schema(false, 2);
        f.Write(1, 0, 1000);
        f.Drive();
        f.Restart(NewGroup);
        TAutoPtr<IEventHandle> continuation;
        bool holdScan = true;
        ui32 cuts = 0;
        ui32 metadataRequests = 0;
        const auto aborted = f.Counters()->GetCounter("Deriviative/CutHistory/ScansAborted/Count", true);
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (holdScan && dynamic_cast<TEvFindEmptyHistoryIntervals::TEvContinue*>(ev->GetBase())) {
                UNIT_ASSERT(!continuation);
                continuation = ev.Release();
            } else if (const auto* ask = dynamic_cast<TCacheEvents::TEvAskData*>(ev->GetBase());
                       ask && ask->GetConsumer() == NOlap::NBlobOperations::EConsumer::SCAN) {
                ++metadataRequests;
            } else if (dynamic_cast<TEvTablet::TEvCutTabletHistory*>(ev->GetBase())) {
                ++cuts;
                ev.Reset();
            }
        });
        f.Restart();
        f.Drive();
        UNIT_ASSERT(continuation);
        const auto& index = f.Controller->GetTheOnlyShard()->GetIndexAs<NOlap::TColumnEngineForLogs>();
        std::optional<TInternalPathId> droppedPath;
        for (const auto& [pathId, granule] : index.GetTables()) {
            if (!granule->GetPortions().empty() || !granule->GetInsertedPortions().empty()) {
                UNIT_ASSERT(!droppedPath);
                droppedPath = pathId;
            }
        }
        UNIT_ASSERT(droppedPath);
        const auto dropStep = SetupSchema(f.Runtime, f.Sender, TTestSchema::DropTableTxBody(TableId, 2), 1001);
        for (ui32 i = 0; i < 60 && index.IsGranuleExists(*droppedPath); ++i) {
            PlanCommit(f.Runtime, f.Sender, TPlanStep{ dropStep.Val() + i + 1 }, TSet<ui64>{});
            f.Drive(1);
        }
        UNIT_ASSERT_C(!index.IsGranuleExists(*droppedPath), "drop must remove the captured path before its metadata request");
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 0u);
        metadataRequests = 0;
        holdScan = false;
        f.Runtime.Send(continuation.Release(), 0, true);
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(metadataRequests, 1u);
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 0u);
        UNIT_ASSERT_VALUES_EQUAL(cuts, 0u);
    }

    Y_UNIT_TEST(PersistedDeletionSurvivesRestart) {
        TFixture f;
        f.Controller->DisableBackground(EBackground::Compaction);
        f.Controller->SetOverrideMaxReadStaleness(TDuration::Zero());
        f.Schema(false, 2);
        f.Write(1, 0, 1000);
        f.Drive();
        const auto oldBlobs = f.LiveOldBlobs();
        UNIT_ASSERT(!oldBlobs.empty());
        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(false);
        f.Restart(NewGroup);
        f.Drive();
        for (const auto& id : oldBlobs) {
            UNIT_ASSERT(f.OldProxy->AllMyBlobs().at(id).Keep);
        }
        f.Controller->DisableBackground(EBackground::GC);
        const auto dropStep = SetupSchema(f.Runtime, f.Sender, TTestSchema::DropTableTxBody(TableId, 2), 1001);
        auto storage = f.Controller->GetTheOnlyShard()->GetStoragesManager()->GetDefaultOperator();
        const auto hasPendingDeletion = [&] {
            for (const auto& id : oldBlobs) {
                if (storage->HasToDelete(NOlap::TUnifiedBlobId(OldGroup, id), static_cast<NOlap::TTabletId>(TabletId))) {
                    return true;
                }
            }
            return false;
        };
        for (ui32 i = 0; i < 60 && !hasPendingDeletion(); ++i) {
            PlanCommit(f.Runtime, f.Sender, TPlanStep{ dropStep.Val() + i + 1 }, TSet<ui64>{});
            f.Drive(1);
        }
        UNIT_ASSERT(hasPendingDeletion());
        const auto& index = f.Controller->GetTheOnlyShard()->GetIndexAs<NOlap::TColumnEngineForLogs>();
        for (const auto& [_, granule] : index.GetTables()) {
            UNIT_ASSERT(granule->GetPortions().empty());
            UNIT_ASSERT(granule->GetInsertedPortions().empty());
        }
        ui32 cuts = 0;
        ui32 preparedGeneration = 0;
        bool holdPreparation = true;
        bool recoveredRequest = false;
        TAutoPtr<IEventHandle> preparedGC;
        std::shared_ptr<NOlap::IBlobsGCAction> recoveredGC;
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (const auto* gc = dynamic_cast<TEvBlobStorage::TEvCollectGarbage*>(ev->GetBase());
                gc && gc->TabletId == TabletId && gc->Channel == FirstDataChannel && ev->Cookie == OldGroup && gc->Collect) {
                if (holdPreparation) {
                    UNIT_ASSERT(!preparedGC);
                    UNIT_ASSERT(gc->DoNotKeep && !gc->DoNotKeep->empty());
                    preparedGeneration = gc->CollectGeneration;
                    preparedGC = ev.Release();
                } else {
                    UNIT_ASSERT(!gc->DoNotKeep || gc->DoNotKeep->empty());
                    UNIT_ASSERT(gc->CollectGeneration >= preparedGeneration);
                    recoveredRequest = true;
                    // Let recovery finish, but keep the later deletion-draining GC disabled.
                    f.Controller->DisableBackground(EBackground::GC);
                }
            } else if (const auto* finished = dynamic_cast<TEvPrivate::TEvGarbageCollectionFinished*>(ev->GetBase());
                       finished && !holdPreparation) {
                UNIT_ASSERT(!recoveredGC);
                recoveredGC = finished->Action;
            } else if (const auto* cut = dynamic_cast<TEvTablet::TEvCutTabletHistory*>(ev->GetBase())) {
                UNIT_ASSERT_VALUES_EQUAL(cut->Record.GetChannel(), FirstDataChannel);
                UNIT_ASSERT_VALUES_EQUAL(cut->Record.GetGroupID(), OldGroup);
                ++cuts;
                ev.Reset();
            }
        });
        // Prepare an advancing GC, then reboot before its DontKeep is delivered.
        f.Restart();
        f.Controller->EnableBackground(EBackground::GC);
        f.Drive();
        UNIT_ASSERT(preparedGC);
        UNIT_ASSERT(f.History.back().first <= preparedGeneration);
        preparedGC.Reset();
        holdPreparation = false;
        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(true);
        f.Restart();
        storage = f.Controller->GetTheOnlyShard()->GetStoragesManager()->GetDefaultOperator();
        f.Drive();
        UNIT_ASSERT(recoveredRequest);
        UNIT_ASSERT(recoveredGC && !recoveredGC->IsInProgress());
        UNIT_ASSERT(hasPendingDeletion());
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Counters()->GetCounter("Deriviative/CutHistory/ScansAborted/Count", true)->Val(), 0u);
        UNIT_ASSERT_VALUES_EQUAL_C(cuts, 0u, "persisted deletions must exclude the interval after the recovered barrier is committed");
    }

    Y_UNIT_TEST(ColdCacheBatchingAndCancellation) {
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
        const auto sent = f.Counters()->GetCounter("Deriviative/CutHistory/RequestsSent/Count", true);
        const auto aborted = f.Counters()->GetCounter("Deriviative/CutHistory/ScansAborted/Count", true);
        bool cutSent = false;
        bool failMetadata = false;
        bool disableAfterBatch = false;
        ui32 scanBatches = 0;
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (const auto* cut = dynamic_cast<TEvTablet::TEvCutTabletHistory*>(ev->GetBase());
                cut && cut->Record.GetChannel() == FirstDataChannel) {
                UNIT_ASSERT_VALUES_EQUAL(cut->Record.GetFromGeneration(), 0u);
                UNIT_ASSERT_VALUES_EQUAL(cut->Record.GetGroupID(), OldGroup);
                cutSent = true;
                ev.Reset();
            } else if (auto* ask = dynamic_cast<TCacheEvents::TEvAskData*>(ev->GetBase());
                       ask && ask->GetConsumer() == NOlap::NBlobOperations::EConsumer::SCAN) {
                auto addresses = ask->ExtractAddresses();
                auto callback = ask->ExtractCallback();
                UNIT_ASSERT(!addresses.empty());
                UNIT_ASSERT(addresses.size() <= Max<ui32>(1, config->GetScanBatchSize()));
                ++scanBatches;
                if (disableAfterBatch) {
                    UNIT_ASSERT_VALUES_EQUAL(addresses.size(), 1u);
                    f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(false);
                    disableAfterBatch = false;
                }
                if (failMetadata) {
                    callback = std::make_shared<TFailMetadataCallback>(std::move(callback));
                    failMetadata = false;
                }
                auto* replacement = new TCacheEvents::TEvAskData(ask->GetConsumer(), std::move(addresses), std::move(callback));
                auto handle = MakeHolder<IEventHandle>(ev->Recipient, ev->Sender, replacement, ev->Flags, ev->Cookie);
                // The cache service has already been resolved to an actor before this observer runs.
                handle->Rewrite(ev->GetTypeRewrite(), ev->GetRecipientRewrite());
                ev.Reset(handle.Release());
            }
        });
        f.ContinuationDelays.clear();
        f.Restart();
        for (ui32 i = 0; i < 60 && !cutSent; ++i) {
            f.Drive(1);
        }
        UNIT_ASSERT(cutSent);
        UNIT_ASSERT(scanBatches >= 4);
        UNIT_ASSERT(!f.ContinuationDelays.empty());
        for (const auto delay : f.ContinuationDelays) {
            UNIT_ASSERT(delay >= TDuration::MilliSeconds(3) && delay <= TDuration::MilliSeconds(5));
        }
        UNIT_ASSERT_VALUES_EQUAL(sent->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 0u);

        cutSent = false;
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
        UNIT_ASSERT(!cutSent);
        UNIT_ASSERT_VALUES_EQUAL(sent->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 1u);

        scanBatches = 0;
        config->SetScanBatchSize(portionCount);
        config->SetScanMemoryLimitBytes(1);
        disableAfterBatch = true;
        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(scanBatches, 1u);
        UNIT_ASSERT(!cutSent);
        UNIT_ASSERT_VALUES_EQUAL(sent->Val(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(aborted->Val(), 2u);
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
        std::set<ui32> cutFrom;
        auto observer = f.Runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            if (ev->Get()->Record.GetChannel() == FirstDataChannel) {
                UNIT_ASSERT_C(ev->Get()->Record.GetFromGeneration() != 0, "uncommitted old data must pin its interval");
                cutFrom.emplace(ev->Get()->Record.GetFromGeneration());
                ev.Reset();
            }
        });
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
        UNIT_ASSERT_C(cutFrom.contains(emptyReusedFrom), "an earlier live use of this group must not pin a later empty interval");
        UNIT_ASSERT(!f.LiveOldBlobs().empty());
    }

    Y_UNIT_TEST_TWIN(SharingOrFeatureDisableDuringJournalCommitPreventsCut, disableFeature) {
        TFixture f;
        f.Schema();
        TAutoPtr<IEventHandle> continuation;
        TAutoPtr<IEventHandle> commit;
        bool holdContinuation = true;
        bool holdCommit = false;
        ui32 cuts = 0;
        auto observer = f.Runtime.AddObserver<IEventHandle>([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (holdContinuation && dynamic_cast<TEvFindEmptyHistoryIntervals::TEvContinue*>(ev->GetBase())) {
                continuation = ev.Release();
            } else if (const auto* log = dynamic_cast<TEvTabletBase::TEvWriteLogResult*>(ev->GetBase());
                       holdCommit && log && log->EntryId.TabletID() == TabletId && log->EntryId.Cookie() == 0) {
                commit = ev.Release();
                holdCommit = false;
            } else if (dynamic_cast<TEvTablet::TEvCutTabletHistory*>(ev->GetBase())) {
                ++cuts;
                ev.Reset();
            }
        });
        f.Restart(NewGroup);
        f.Restart();
        f.Drive();
        UNIT_ASSERT(continuation);
        f.Controller->DisableBackground(EBackground::GC);
        holdContinuation = false;
        holdCommit = true;
        f.Runtime.Send(continuation.Release(), 0, true);
        f.Drive();
        UNIT_ASSERT(commit);
        UNIT_ASSERT_VALUES_EQUAL_C(cuts, 0u, "journal commit must precede outgoing cuts");
        if (disableFeature) {
            f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(false);
        } else {
            NOlap::NDataSharing::TTaskForTablet task(static_cast<NOlap::TTabletId>(TabletId));
            f.Runtime.SendToPipe(
                TabletId, f.Sender, new NOlap::NDataSharing::NEvents::TEvApplyLinksModification(static_cast<NOlap::TTabletId>(TabletId),
                                        "commit-window-sharing", 0, task), 0, GetPipeConfigWithRetries());
            f.Drive();
            UNIT_ASSERT(!f.Controller->GetTheOnlyShard()->GetSharingSessionsManager()->CanCutHistory());
        }
        f.Runtime.Send(commit.Release(), 0, true);
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(cuts, 0u);
        UNIT_ASSERT_STRING_CONTAINS(f.Journal(), "GroupID: " + ToString(OldGroup));
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("ScanToSend"), 0u);
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
        ui32 cuts = 0;
        auto observer = f.Runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            if (ev->Get()->Record.GetChannel() == FirstDataChannel) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetGroupID(), OldGroup);
                ++cuts;
            }
            ev.Reset();
        });
        const ui64 scans = f.Samples("Scan");
        f.Restart(NewGroup);
        f.Restart();
        f.Drive();
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), scans + 1);
        UNIT_ASSERT_VALUES_EQUAL(cuts, inheritPortionStorage ? 1u : 0u);
        UNIT_ASSERT_VALUES_EQUAL(f.LiveOldBlobs().size(), oldBlobs.size());
        UNIT_ASSERT_VALUES_EQUAL(f.ReadRows(), 1000u);
    }
}
}   // namespace NKikimr::NColumnShard
