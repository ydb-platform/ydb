#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/tablet/tablet_impl.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tx_helpers.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/data_sharing/modification/events/change_owning.h>
#include <ydb/core/tx/columnshard/data_sharing/modification/tasks/modification.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

#include <ydb/library/testlib/helpers.h>
#include <ydb/public/lib/value/value.h>

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
        objects.erase(objects.begin());
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
    ui64 ScansBeforeStart = 0;

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

    TFixture() {
        TTester::Setup(
            Runtime, { new NFake::TProxyDS(TGroupId::FromValue(0)), OldProxy, NewProxy, new NFake::TProxyDS(TGroupId::FromValue(Max<ui32>())) });
        Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(true);
        Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(true);
        Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistorySending(false);
        Runtime.SetScheduledLimit(10000);
        auto previous = Runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&, TDuration, TInstant&) {
            return true;
        });
        Runtime.SetScheduledEventFilter(
            [previous](TTestActorRuntimeBase& r, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline) {
                if (event->HasEvent() && dynamic_cast<TEvFindEmptyHistoryIntervals::TEvContinue*>(event->GetBase())) {
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

    void WakeupAndDispatch(ui32 count = 10) {
        for (ui32 i = 0; i < count; ++i) {
            Wakeup(Runtime, Sender, TabletId);
            Runtime.SimulateSleep(TDuration::Seconds(1));
        }
    }

    void PrepareClosedIntervals(ui32 count = 3) {
        Schema();
        Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(false);
        for (ui32 i = 0; i < count; ++i) {
            Restart(i % 2 ? OldGroup : NewGroup);
        }
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
        WakeupAndDispatch();
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

    void PrepareTableForDrop() {
        Controller->DisableBackground(EBackground::Compaction);
        Controller->SetOverrideMaxReadStaleness(TDuration::Zero());
        Schema(false, 2);
        Write(1, 0, 1000);
        WakeupAndDispatch();
    }

    void DropTableAndWaitForCleanup() {
        const auto& index = Controller->GetTheOnlyShard()->GetIndexAs<NOlap::TColumnEngineForLogs>();
        std::optional<TInternalPathId> droppedPath;
        for (const auto& [pathId, granule] : index.GetTables()) {
            if (!granule->GetPortions().empty() || !granule->GetInsertedPortions().empty()) {
                droppedPath = pathId;
                break;
            }
        }
        UNIT_ASSERT(droppedPath);
        const auto dropStep = SetupSchema(Runtime, Sender, TTestSchema::DropTableTxBody(TableId, 2), 1001);
        for (ui32 i = 0; i < 60 && index.IsGranuleExists(*droppedPath); ++i) {
            PlanCommit(Runtime, Sender, TPlanStep{ dropStep.Val() + i + 1 }, TSet<ui64>{});
            WakeupAndDispatch(1);
        }
        UNIT_ASSERT_C(!index.IsGranuleExists(*droppedPath), "drop must remove the populated path before continuing");
    }

    std::vector<TLogoBlobID> PreparePersistedDeletion() {
        PrepareTableForDrop();
        const auto blobs = GetLiveOldBlobs();
        UNIT_ASSERT(!blobs.empty());
        Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(false);
        Restart(NewGroup);
        WakeupAndDispatch();
        Controller->DisableBackground(EBackground::GC);
        DropTableAndWaitForCleanup();
        const auto storage = Controller->GetTheOnlyShard()->GetStoragesManager()->GetDefaultOperator();
        bool hasPendingDeletion = false;
        for (const auto& id : blobs) {
            if (storage->HasToDelete(NOlap::TUnifiedBlobId(OldGroup, id), static_cast<NOlap::TTabletId>(TabletId))) {
                hasPendingDeletion = true;
                break;
            }
        }
        UNIT_ASSERT(hasPendingDeletion);
        return blobs;
    }

    std::vector<TLogoBlobID> GetLiveOldBlobs() const {
        std::vector<TLogoBlobID> result;
        for (const auto& [id, blob] : OldProxy->AllMyBlobs()) {
            if (id.TabletID() == TabletId && id.Channel() >= FirstDataChannel && !blob.DoNotKeep) {
                result.push_back(id);
            }
        }
        return result;
    }

    TLogoBlobID WriteOrphan(ui32 channel) {
        const auto storage = Controller->GetTheOnlyShard()->GetStoragesManager()->GetDefaultOperator();
        const auto manager = std::dynamic_pointer_cast<NOlap::TBlobManager>(storage->GetBlobsTracker());
        UNIT_ASSERT(manager);
        const TString data = "blob";
        const ui32 channelCount = 1 + !SecondChannelHistory.empty();
        for (ui32 attempt = 0; attempt < channelCount; ++attempt) {
            auto batch = manager->StartBlobBatch();
            const auto id = batch.AllocateNextBlobId(data);
            if (id.GetLogoBlobId().Channel() != channel) {
                continue;
            }
            Runtime.Send(new IEventHandle(MakeBlobStorageProxyID(id.GetDsGroup()), Sender,
                new TEvBlobStorage::TEvPut(id.GetLogoBlobId(), data, TInstant::Max(), NKikimrBlobStorage::UserData)));
            const auto result = Runtime.GrabEdgeEvent<TEvBlobStorage::TEvPutResult>(Sender);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
            batch.OnBlobWriteResult(id.GetLogoBlobId(), result->Get()->Status);
            return id.GetLogoBlobId();   // Discard the batch without saving Keep or a portion reference.
        }
        UNIT_FAIL("Could not allocate an orphan on channel " << channel);
        return {};
    }

    TLogoBlobID PrepareOrphanWithSwappedChannelGroups(bool enableCutHistory = true) {
        Schema();
        WakeupAndDispatch();
        Controller->DisableBackground(EBackground::GC);
        Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(false);
        SecondChannelHistory = { { 0, NewGroup } };
        Restart();
        const auto blob = WriteOrphan(FirstDataChannel);
        SecondChannelHistory.emplace_back(blob.Generation() + 1, OldGroup);
        Restart(NewGroup);
        Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(enableCutHistory);
        Restart();
        WakeupAndDispatch();
        return blob;
    }

    NKikimrProto::EReplyStatus ReadBlobStatus(const TLogoBlobID& blob, ui32 group = OldGroup) {
        Runtime.Send(new IEventHandle(
            MakeBlobStorageProxyID(group), Sender, new TEvBlobStorage::TEvGet(blob, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead)));
        const auto result = Runtime.GrabEdgeEvent<TEvBlobStorage::TEvGetResult>(Sender);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        return result->Get()->Responses[0].Status;
    }

    std::map<ui64, NKikimrTxColumnShard::TCutHistoryRequests> Journal() {
        NKikimrMiniKQL::TResult result;
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(Runtime, TabletId, R"(
            (
                (let range '('('Sequence (Null) (Void))))
                (return (AsList (SetResult 'Result
                    (SelectRange 'CutHistoryRequests range '('Sequence 'RequestsProto) '()))))
            )
        )", result), NKikimrProto::OK);
        const auto rows = NClient::TValue::Create(result)[0]["List"];
        std::map<ui64, NKikimrTxColumnShard::TCutHistoryRequests> journal;
        for (size_t i = 0; i < rows.Size(); ++i) {
            UNIT_ASSERT(journal[ui64(rows[i]["Sequence"])].ParseFromString(TString(rows[i]["RequestsProto"])));
        }
        return journal;
    }

    template <class TCondition>
    void WaitFor(TCondition&& condition) {
        for (ui32 i = 0; i < 60 && !condition(); ++i) {
            WakeupAndDispatch(1);
        }
        UNIT_ASSERT_C(condition(), "condition was not reached after dispatching background activity");
    }

    void Start(std::optional<ui32> group = {}) {
        ScansBeforeStart = Samples("Scan");
        Restart(group);
    }

    void WaitForCutHistory() {
        WaitFor([&] {
            return Samples("Scan") > ScansBeforeStart;
        });
        WakeupAndDispatch(1);
    }

    auto StartWithBlockedJournalCommit() {
        TBlockEvents<TEvFindEmptyHistoryIntervals::TEvScanFinished> scan(Runtime);
        Start();
        WaitFor([&] {
            return !scan.empty();
        });
        Controller->DisableBackground(EBackground::GC);
        auto commits =
            std::make_unique<TBlockEvents<TEvTabletBase::TEvWriteLogResult>>(Runtime, [holdNextCommit = true](const auto& ev) mutable {
                if (!holdNextCommit || ev->Get()->EntryId.TabletID() != TabletId || ev->Get()->EntryId.Cookie() != 0) {
                    return false;
                }
                holdNextCommit = false;
                return true;
            });
        scan.Stop().Unblock();
        WaitFor([&] {
            return !commits->empty();
        });
        return commits;
    }

    using TRequests = std::map<std::pair<ui32, ui32>, ui32>;

    TRequests LatestRequests() {
        const auto journal = Journal();
        TRequests result;
        if (!journal.empty()) {
            for (const auto& request : journal.rbegin()->second.GetRequests()) {
                UNIT_ASSERT(result.emplace(std::make_pair(request.GetChannel(), request.GetFromGeneration()), request.GetGroupID()).second);
            }
        }
        return result;
    }

    TRequests ExpectedRequests(ui32 count) const {
        TRequests result;
        for (ui32 i = 0; i < count; ++i) {
            result.emplace(std::make_pair(FirstDataChannel, History[i].first), History[i].second);
        }
        return result;
    }

    void PrepareSharedBlobs() {
        PrepareClosedIntervals(3);
        const auto selfTablet = static_cast<NOlap::TTabletId>(TabletId);
        const auto otherTablet = static_cast<NOlap::TTabletId>(TabletId + 1);
        NOlap::NDataSharing::TStorageTabletTask storageTask(NOlap::IStoragesManager::DefaultStorageId, selfTablet);
        const auto makeBlob = [&](ui32 historyIndex) {
            const auto [generation, group] = History[historyIndex];
            return NOlap::TUnifiedBlobId(group, TLogoBlobID(TabletId, generation, 1, FirstDataChannel, 10, 0));
        };
        storageTask.AddLink(makeBlob(1), otherTablet);
        storageTask.AddInitOwner(makeBlob(2), otherTablet);
        NOlap::NDataSharing::TTaskForTablet task(selfTablet);
        task.AddStorage(std::move(storageTask));
        Runtime.SendToPipe(TabletId, Sender, new NOlap::NDataSharing::NEvents::TEvApplyLinksModification(selfTablet,
                                                 "persisted-cut-history-links", 0, task), 0, GetPipeConfigWithRetries());
        WakeupAndDispatch();
    }

    void PrepareUncommittedPortions() {
        Schema();
        Controller->DisableBackground(EBackground::Compaction);
        Controller->DisableBackground(EBackground::Cleanup);
        std::vector<ui64> ids;
        UNIT_ASSERT(WriteData(Runtime, Sender, TabletId, 1, TableId, MakeTestBlob({ 0, 1000 }, Table.Schema), Table.Schema, &ids,
            NEvWrite::EModificationType::Upsert, 42));
        UNIT_ASSERT(!GetLiveOldBlobs().empty());
        Restart(NewGroup);
        WakeupAndDispatch();
    }

    void PrepareColdCachePortions(ui32 portionCount = 3) {
        auto* config = Runtime.GetAppData().ColumnShardConfig.MutableCutHistory();
        config->SetScanBatchSize(2);
        Schema(false, 2);
        Controller->DisableBackground(EBackground::Compaction);
        Controller->DisableBackground(EBackground::Cleanup);
        Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(false);
        Restart(NewGroup);
        for (ui64 i = 0; i < portionCount; ++i) {
            Write(i + 1, i, i + 1, TableId + i % 2);
        }
        Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(true);
        UNIT_ASSERT(GetLiveOldBlobs().empty());
    }

    auto ObserveScanMetadataRequests(ui32& scanBatches, bool& failMetadata) {
        auto* config = Runtime.GetAppData().ColumnShardConfig.MutableCutHistory();
        return Runtime.AddObserver<IEventHandle>([config, &scanBatches, &failMetadata](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            auto* ask = dynamic_cast<TCacheEvents::TEvAskData*>(ev->GetBase());
            if (!ask || ask->GetConsumer() != NOlap::NBlobOperations::EConsumer::SCAN) {
                return;
            }
            auto addresses = ask->ExtractAddresses();
            auto callback = ask->ExtractCallback();
            UNIT_ASSERT(!addresses.empty());
            UNIT_ASSERT(addresses.size() <= Max<ui32>(1, config->GetScanBatchSize()));
            ++scanBatches;
            if (failMetadata) {
                UNIT_ASSERT_VALUES_EQUAL_C(addresses.size(), 1u, "an oversized portion must be fetched alone");
                callback = std::make_shared<TFailMetadataCallback>(std::move(callback));
                failMetadata = false;
            }
            auto* replacement = new TCacheEvents::TEvAskData(ask->GetConsumer(), std::move(addresses), std::move(callback));
            auto handle = MakeHolder<IEventHandle>(ev->Recipient, ev->Sender, replacement, ev->Flags, ev->Cookie);
            handle->Rewrite(ev->GetTypeRewrite(), ev->GetRecipientRewrite());
            ev.Reset(handle.Release());
        });
    }

    void PrepareTieredIndex(bool inheritPortionStorage) {
        Controller->SetSkipSpecialCheckForEvict(true);
        Controller->SetOverrideMaxReadStaleness(TDuration::Zero());
        Schema(true, 1, inheritPortionStorage);
        Write(1, 0, 1000);
        Write(2, 0, 1000);
        for (ui32 i = 0; i < 60 && !Controller->GetCompactionFinishedCounter().Val(); ++i) {
            WakeupAndDispatch(1);
        }
        UNIT_ASSERT(Controller->GetCompactionFinishedCounter().Val());
        const auto& index = Controller->GetTheOnlyShard()->GetIndexAs<NOlap::TColumnEngineForLogs>();
        for (const auto& [_, granule] : index.GetTables()) {
            for (const auto& [_, portion] : granule->GetPortions()) {
                if (!portion->HasRemoveSnapshot()) {
                    UNIT_ASSERT(portion->GetPortionType() == NOlap::EPortionType::Compacted);
                    UNIT_ASSERT(portion->GetIndexBlobBytes());
                }
            }
        }
        const auto warm = EvictToWarm();
        ui32 livePortions = 0;
        ui64 cleanupStep = ReadStep.Val();
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
        ReadStep = TPlanStep{ cleanupStep + 1 };
        PlanCommit(Runtime, Sender, TabletId, ReadStep, TSet<ui64>{});
        WakeupAndDispatch();
        ui32 obsoletePortions = 0;
        for (ui32 i = 0; i < 60; ++i) {
            obsoletePortions = 0;
            for (const auto& [_, granule] : index.GetTables()) {
                for (const auto& [_, portion] : granule->GetPortions()) {
                    obsoletePortions += portion->HasRemoveSnapshot();
                }
            }
            if (!obsoletePortions && (!inheritPortionStorage || GetLiveOldBlobs().empty())) {
                break;
            }
            WakeupAndDispatch(1);
        }
        UNIT_ASSERT_VALUES_EQUAL(obsoletePortions, 0u);
        const auto oldBlobs = GetLiveOldBlobs();
        UNIT_ASSERT_VALUES_EQUAL_C(oldBlobs.empty(), inheritPortionStorage, "only a DEFAULT index keeps old local blobs after eviction");
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
    Y_UNIT_TEST_TWIN(JournalCommitControlsSending, disableBeforeComplete) {
        TFixture f;
        constexpr ui32 intervalCount = CutHistoryJournalLimit + 1;
        f.PrepareClosedIntervals(intervalCount);
        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistorySending(true);
        bool firstGCCompleted = false;
        auto gcObserver = f.Runtime.AddObserver<TEvPrivate::TEvGarbageCollectionFinished>([&](auto&) {
            firstGCCompleted = true;
        });
        TFixture::TRequests sent;
        auto observer = f.Runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](auto& ev) {
            UNIT_ASSERT_C(firstGCCompleted, "CutHistory must follow first GC completion");
            const auto& request = ev->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(request.GetTabletID(), TabletId);
            UNIT_ASSERT(sent.emplace(std::make_pair(request.GetChannel(), request.GetFromGeneration()), request.GetGroupID()).second);
            ev.Reset();
        });
        auto commits = f.StartWithBlockedJournalCommit();
        UNIT_ASSERT_C(sent.empty(), "requests must wait for their journal commit");

        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistorySending(!disableBeforeComplete);
        commits->Stop().Unblock();
        f.WaitForCutHistory();
        UNIT_ASSERT(f.LatestRequests() == f.ExpectedRequests(intervalCount));
        UNIT_ASSERT(sent == (disableBeforeComplete ? TFixture::TRequests{} : f.LatestRequests()));
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("ScanToSend"), sent.size());
        UNIT_ASSERT_VALUES_EQUAL(f.Counters()->GetCounter("Deriviative/CutHistory/RequestsSent/Count", true)->Val(), sent.size());
        const auto journal = f.Journal();
        UNIT_ASSERT_VALUES_EQUAL(journal.size(), 1u);
        ui32 i = 0;
        for (const auto& request : journal.begin()->second.GetRequests()) {
            ++i;
            UNIT_ASSERT_VALUES_EQUAL(request.GetToGeneration(), f.History[i].first);
        }
    }

    Y_UNIT_TEST(JournalKeepsRecentRequestSets) {
        TFixture f;
        constexpr ui32 intervalCount = 2;
        f.PrepareClosedIntervals(intervalCount);
        std::vector<ui32> generations;
        for (ui32 i = 0; i <= CutHistoryJournalLimit; ++i) {
            f.Start();
            f.WaitForCutHistory();
            generations.push_back(f.Controller->GetTheOnlyShard()->Generation());
        }
        const auto journal = f.Journal();
        UNIT_ASSERT_VALUES_EQUAL(journal.size(), CutHistoryJournalLimit);
        auto generation = generations.begin() + 1;
        for (const auto& [_, requests] : journal) {
            UNIT_ASSERT_VALUES_EQUAL(requests.RequestsSize(), intervalCount);
            for (const auto& request : requests.GetRequests()) {
                UNIT_ASSERT_VALUES_EQUAL(request.GetSendingGeneration(), *generation);
            }
            ++generation;
        }
    }

    Y_UNIT_TEST(FirstGCCollectsOrphanWithSwappedChannelGroups) {
        TFixture f;
        const auto blob = f.PrepareOrphanWithSwappedChannelGroups(false);
        TBlockEvents<TEvBlobStorage::TEvCollectGarbage> orphanChannelGC(f.Runtime, [](const auto& ev) {
            const auto& gc = *ev->Get();
            if (gc.TabletId != TabletId || gc.Channel != FirstDataChannel || ev->Cookie != OldGroup || !gc.Collect) {
                return false;
            }
            UNIT_ASSERT(!gc.Hard);
            return true;
        });
        bool otherChannelCollected = false;
        auto observer = f.Runtime.AddObserver<TEvBlobStorage::TEvCollectGarbageResult>([&](auto& ev) {
            if (ev->Get()->Channel == FirstDataChannel + 1 && ev->Cookie == OldGroup) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
                otherChannelCollected = true;
            }
        });
        f.Controller->EnableBackground(EBackground::GC);
        f.WaitFor([&] {
            return !orphanChannelGC.empty() && otherChannelCollected;
        });
        UNIT_ASSERT_VALUES_EQUAL(f.ReadBlobStatus(blob), NKikimrProto::OK);

        orphanChannelGC.clear();
        otherChannelCollected = false;
        f.Restart();
        f.WaitFor([&] {
            return !orphanChannelGC.empty() && otherChannelCollected;
        });
        UNIT_ASSERT_VALUES_EQUAL_C(
            f.ReadBlobStatus(blob), NKikimrProto::OK, "GC on the other channel must not collect the orphan, including after recovery");
        orphanChannelGC.Stop().Unblock();
        f.WaitFor([&] {
            return f.ReadBlobStatus(blob) == NKikimrProto::NODATA;
        });
    }

    Y_UNIT_TEST(OrphanDoesNotKeepHistoryAfterFirstGC) {
        TFixture f;
        const auto blob = f.PrepareOrphanWithSwappedChannelGroups();
        UNIT_ASSERT_VALUES_EQUAL(f.ReadBlobStatus(blob), NKikimrProto::OK);
        UNIT_ASSERT(f.Journal().empty());
        f.Controller->EnableBackground(EBackground::GC);
        f.WaitForCutHistory();
        const TFixture::TRequests expected{ { { FirstDataChannel, 0 }, OldGroup }, { { FirstDataChannel + 1, 0 }, NewGroup } };
        UNIT_ASSERT(f.LatestRequests() == expected);
        UNIT_ASSERT_VALUES_EQUAL(f.ReadBlobStatus(blob), NKikimrProto::NODATA);
    }

    Y_UNIT_TEST(SharedBlobsExcludeOwnIntervals) {
        TFixture f;
        f.PrepareSharedBlobs();
        f.Start();
        f.WaitForCutHistory();
        UNIT_ASSERT(f.LatestRequests() == f.ExpectedRequests(1));
    }

    Y_UNIT_TEST(CurrentGenerationIntervalIsSkipped) {
        TFixture f;
        f.Schema();
        ui32 gcCompletions = 0;
        auto observer = f.Runtime.AddObserver<TEvPrivate::TEvGarbageCollectionFinished>([&](auto&) {
            ++gcCompletions;
        });
        f.Start(NewGroup);
        f.WaitFor([&] {
            return gcCompletions != 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(f.History.back().first, f.Controller->GetTheOnlyShard()->Generation());
        UNIT_ASSERT_C(f.Journal().empty(), "an interval ending at the current generation must not be journaled");

        f.Start();
        f.WaitForCutHistory();
        UNIT_ASSERT(f.LatestRequests() == f.ExpectedRequests(1));
        const auto completionsBeforeWrite = gcCompletions;
        f.Write(1, 0, 1);
        f.WaitFor([&] {
            return gcCompletions > completionsBeforeWrite;
        });
        UNIT_ASSERT_VALUES_EQUAL_C(f.Journal().size(), 1u, "ordinary GC must not repeat CutHistory in the same boot");
        UNIT_ASSERT_VALUES_EQUAL(f.Samples("Scan"), 1u);
    }

    Y_UNIT_TEST(PersistedDeletionSurvivesRestart) {
        TFixture f;
        const auto blobs = f.PreparePersistedDeletion();
        {
            TBlockEvents<TEvBlobStorage::TEvCollectGarbage> pendingGC(f.Runtime, [](const auto& ev) {
                const auto& gc = *ev->Get();
                if (gc.TabletId != TabletId || gc.Channel != FirstDataChannel || ev->Cookie != OldGroup || !gc.Collect) {
                    return false;
                }
                UNIT_ASSERT(gc.DoNotKeep && !gc.DoNotKeep->empty());
                return true;
            });
            f.Restart();
            f.Controller->EnableBackground(EBackground::GC);
            f.WaitFor([&] {
                return !pendingGC.empty();
            });
        }
        auto observer = f.Runtime.AddObserver<TEvBlobStorage::TEvCollectGarbage>([&](auto& ev) {
            const auto& gc = *ev->Get();
            if (gc.TabletId == TabletId && gc.Channel == FirstDataChannel && ev->Cookie == OldGroup && gc.Collect) {
                f.Controller->DisableBackground(EBackground::GC);
            }
        });
        f.Runtime.GetAppData().FeatureFlags.SetEnableColumnshardCutHistory(true);
        f.Start();
        f.WaitForCutHistory();
        UNIT_ASSERT_C(f.Journal().empty(), "persisted deletions must exclude history after barrier recovery");
        for (const auto& blob : blobs) {
            UNIT_ASSERT_VALUES_EQUAL(f.ReadBlobStatus(blob), NKikimrProto::OK);
        }
    }

    Y_UNIT_TEST(ColdCacheBatchingAndCancellation) {
        TFixture f;
        f.PrepareColdCachePortions();
        auto* config = f.Runtime.GetAppData().ColumnShardConfig.MutableCutHistory();
        bool failMetadata = false;
        ui32 scanBatches = 0;
        auto observer = f.ObserveScanMetadataRequests(scanBatches, failMetadata);
        f.Start();
        f.WaitForCutHistory();
        UNIT_ASSERT(scanBatches >= 2);
        UNIT_ASSERT(f.LatestRequests() == f.ExpectedRequests(1));

        failMetadata = true;
        config->SetScanMemoryLimitBytes(1);
        const auto aborted = f.Counters()->GetCounter("Deriviative/CutHistory/ScansAborted/Count", true);
        f.Start();
        f.WaitFor([&] {
            return aborted->Val() != 0;
        });
        UNIT_ASSERT(!failMetadata);
        UNIT_ASSERT_VALUES_EQUAL_C(f.Journal().size(), 1u, "an aborted scan must not append partial requests");
    }

    Y_UNIT_TEST(UncommittedPortionsKeepHistory) {
        TFixture f;
        f.PrepareUncommittedPortions();
        f.Start();
        f.WaitForCutHistory();
        UNIT_ASSERT(f.Journal().empty());
        UNIT_ASSERT(!f.GetLiveOldBlobs().empty());
    }

    Y_UNIT_TEST_TWIN(TieredIndexStorageControlsHistory, inheritPortionStorage) {
        TFixture f;
        f.PrepareTieredIndex(inheritPortionStorage);
        const auto oldBlobs = f.GetLiveOldBlobs();
        f.Restart(NewGroup);
        f.Start();
        f.WaitForCutHistory();
        UNIT_ASSERT(f.LatestRequests() == (inheritPortionStorage ? f.ExpectedRequests(1) : TFixture::TRequests{}));
        UNIT_ASSERT_VALUES_EQUAL(f.GetLiveOldBlobs().size(), oldBlobs.size());
        UNIT_ASSERT_VALUES_EQUAL(f.ReadRows(), 1000u);
    }
}
}   // namespace NKikimr::NColumnShard
