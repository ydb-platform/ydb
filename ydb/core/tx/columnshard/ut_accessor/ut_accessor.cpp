#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/tablet/tablet_impl.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>

#include <functional>

namespace NKikimr::NColumnShard {

struct TAccessorTestAccess {
    static auto* Executor(TColumnShard& shard) {
        return shard.Executor();
    }

    static void Execute(TColumnShard& shard, NTabletFlatExecutor::ITransaction* tx) {
        shard.Execute(tx);
    }
};

namespace {
using namespace NTxUT;
using EBackground = NYDBTest::ICSController::EBackground;
using TPortions = std::vector<NOlap::TPortionInfo::TConstPtr>;
constexpr ui64 TabletId = TTestTxConfig::TxTablet0;
constexpr ui32 GroupId = 2181038080;
constexpr ui32 IndexCount = 8;

class TAccessorsCallback: public NOlap::NDataAccessorControl::IAccessorCallback {
public:
    bool Done = false;
    std::vector<std::shared_ptr<NOlap::TPortionDataAccessor>> Accessors;

    void OnAccessorsFetched(std::vector<std::shared_ptr<NOlap::TPortionDataAccessor>>&& accessors) override {
        Accessors = std::move(accessors);
        Done = true;
    }
};

class TTestTx: public NTabletFlatExecutor::ITransaction {
    const std::function<bool(NTabletFlatExecutor::TTransactionContext&)> OnExecute;
    const std::function<void()> OnComplete;
    bool& Done;

public:
    TTestTx(std::function<bool(NTabletFlatExecutor::TTransactionContext&)> onExecute, std::function<void()> onComplete, bool& done)
        : OnExecute(std::move(onExecute))
        , OnComplete(std::move(onComplete))
        , Done(done)
    {
    }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override {
        return OnExecute(txc);
    }

    void Complete(const TActorContext&) override {
        if (OnComplete) {
            OnComplete();
        }
        Done = true;
    }
};

class TFixture {
    TActorId TabletActor;
    TestTableDescription Table;

public:
    NYDBTest::TControllers::TGuard<NOlap::TWaitCompactionController> Controller =
        NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
    TTestBasicRuntime Runtime;
    TActorId Sender;

    explicit TFixture(const ui32 portionCount) {
        TTester::Setup(Runtime, { new NFake::TProxyDS(TGroupId::FromValue(0)), new NFake::TProxyDS(TGroupId::FromValue(GroupId)),
                                    new NFake::TProxyDS(TGroupId::FromValue(Max<ui32>())) });
        Runtime.GetAppData().FeatureFlags.SetEnableCutHistory(false);
        Runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&, TDuration, TInstant&) {
            return false;
        });
        for (const auto background : { EBackground::TTL, EBackground::Compaction, EBackground::Cleanup, EBackground::GC }) {
            Controller->DisableBackground(background);
        }
        Sender = Runtime.AllocateEdgeActor();
        Boot();

        NKikimrTxColumnShard::TSchemaTxBody tx;
        auto* init = tx.MutableInitShard();
        init->SetOwnerPath("/Root/olap");
        init->SetOwnerPathId(1);
        for (ui32 i = 0; i < portionCount; ++i) {
            auto* table = init->AddTables();
            TSchemeShardLocalPathId::FromRawValue(i + 1).ToProto(*table);
            auto* schema = table->MutableSchema();
            TTestSchema::InitSchema(Table.Schema, Table.Pk, {}, schema);
            auto* metadata = schema->MutableOptions()->MutableMetadataManagerConstructor();
            metadata->SetClassName("local_db");
            metadata->MutableLocalDB()->SetFetchOnStart(false);
            for (ui32 index = 0; index < IndexCount; ++index) {
                NOlap::NIndexes::TRequestSettings settings;
                settings.FalsePositiveProbability = 0.01;
                *schema->AddIndexes() = NOlap::NIndexes::TIndexMetaContainer(
                    std::make_shared<NOlap::NIndexes::TBloomIndexMeta>(4000 + index, "bloom_" + ToString(index),
                        NOlap::IStoragesManager::LocalMetadataStorageId, false, 1, settings,
                        NOlap::NIndexes::TReadDataExtractorContainer(std::make_shared<NOlap::NIndexes::TDefaultDataExtractor>()),
                        NOlap::NIndexes::IBitsStorageConstructor::GetDefault()))
                                            .SerializeToProto();
            }
        }
        Y_UNUSED(SetupSchema(Runtime, Sender, tx.SerializeAsString(), 1000));
        for (ui32 i = 0; i < portionCount; ++i) {
            Write(2 * i + 1, i + 1);
            Write(2 * i + 2, i + 1);
        }
        Controller->EnableBackground(EBackground::Compaction);
        for (ui32 i = 0; i < 60 && Controller->GetCompactionFinishedCounter().Val() < portionCount; ++i) {
            Wakeup(Runtime, Sender, TabletId);
            Runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT(Controller->GetCompactionFinishedCounter().Val() >= portionCount);
        Controller->DisableBackground(EBackground::Compaction);
        UNIT_ASSERT_VALUES_EQUAL(Portions().size(), portionCount);
    }

    TColumnShard& Shard() {
        return *const_cast<TColumnShard*>(Controller->GetTheOnlyShard());
    }

    void Boot() {
        auto info = MakeIntrusive<TTabletStorageInfo>();
        info->TabletID = TabletId;
        info->TabletType = TTabletTypes::ColumnShard;
        info->Channels.resize(FirstDataChannel + 1);
        for (ui32 channel = 0; channel < info->Channels.size(); ++channel) {
            auto& entry = info->Channels[channel];
            entry.Channel = channel;
            entry.Type = TBlobStorageGroupType(BootGroupErasure);
            entry.History.emplace_back(0, GroupId);
        }
        auto setup = MakeIntrusive<TTabletSetupInfo>(&CreateColumnShard, TMailboxType::Simple, ui32(0), TMailboxType::Simple, ui32(0));
        TabletActor = Runtime.Register(CreateTablet(Sender, info.Get(), setup.Get(), 0));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot);
        Runtime.DispatchEvents(options);
        Runtime.SimulateSleep(TDuration::Seconds(1));
    }

    void Write(const ui64 txId, const ui64 tableId) {
        std::vector<ui64> ids;
        UNIT_ASSERT(WriteData(Runtime, Sender, TabletId, txId, tableId, MakeTestBlob({ 0, 5000 }, Table.Schema), Table.Schema, &ids));
        const auto step = ProposeCommit(Runtime, Sender, TabletId, txId, ids);
        PlanCommit(Runtime, Sender, TabletId, step, TSet<ui64>{ txId });
    }

    TPortions Portions() {
        TPortions result;
        for (const auto& [_, granule] : Shard().GetIndexAs<NOlap::TColumnEngineForLogs>().GetTables()) {
            for (const auto& [id, portion] : granule->GetPortions()) {
                if (!portion->HasRemoveSnapshot()) {
                    result.emplace_back(portion);
                }
            }
        }
        SortBy(result, [](const auto& portion) {
            return portion->GetPortionId();
        });
        return result;
    }

    void RunTx(std::function<bool(NTabletFlatExecutor::TTransactionContext&)> execute, std::function<void()> complete = {}) {
        bool done = false;
        Runtime.RunCall([&] {
            TAccessorTestAccess::Execute(Shard(), new TTestTx(std::move(execute), std::move(complete), done));
            return true;
        });
        Runtime.WaitFor("test transaction", [&] {
            return done;
        });
    }

    void PersistIndexesAndRestart() {
        const ui64 edge = Runtime.RunCall([&] {
            return TAccessorTestAccess::Executor(Shard())->CompactTable(Schema::IndexIndexes::TableId);
        });
        UNIT_ASSERT(edge);
        Runtime.WaitFor("index compaction", [&] {
            return TAccessorTestAccess::Executor(Shard())->GetFinishedCompactionInfo(Schema::IndexIndexes::TableId).Edge >= edge;
        });
        Runtime.Send(new IEventHandle(NSharedCache::MakeSharedPageCacheId(0), Sender, new NMemory::TEvConsumerLimit(0)));
        Runtime.Send(new IEventHandle(TabletActor, Sender, new TKikimrEvents::TEvPoisonPill));
        Boot();
    }

    void WarmRecords(const TPortions& portions) {
        RunTx([&](auto& txc) {
            NIceDb::TNiceDb db(txc.DB);
            bool ready = true;
            for (const auto& portion : portions) {
                auto rows = db.Table<Schema::IndexColumnsV2>().Key(portion->GetPathId().GetRawValue(), portion->GetPortionId()).Select();
                if (!rows.IsReady()) {
                    ready = false;
                } else {
                    UNIT_ASSERT(!rows.EndOfSet());
                }
            }
            return ready;
        });
    }

    std::shared_ptr<TAccessorsCallback> Ask(const TPortions& portions) {
        auto result = std::make_shared<TAccessorsCallback>();
        THashMap<TInternalPathId, NOlap::NDataAccessorControl::TPortionsByConsumer> request;
        for (const auto& portion : portions) {
            request[portion->GetPathId()]
                .UpsertConsumer(NOlap::NGeneralCache::TPortionsMetadataCachePolicy::DefaultConsumer())
                .AddPortion(portion->GetPortionId());
        }
        ForwardToTablet(Runtime, TabletId, Sender, new TEvPrivate::TEvAskTabletDataAccessors(std::move(request), result));
        return result;
    }

    void EraseMetadata(const std::shared_ptr<NOlap::TPortionDataAccessor>& accessor, const bool eraseMemory) {
        const auto granule = Shard().GetIndexAs<NOlap::TColumnEngineForLogs>().GetGranuleOptional(accessor->GetPortionInfo().GetPathId());
        const auto portion = granule->GetPortionOptional(accessor->GetPortionInfo().GetPortionId(), false);
        Runtime.RunCall([&] {
            granule->ModifyPortionOnComplete(portion, [](const std::shared_ptr<NOlap::TPortionInfo>& p) {
                p->SetRemoveSnapshot(NOlap::TSnapshot(1000000, 1));
            });
            return true;
        });
        // Match cleanup's database deletion in Execute and optional in-memory erasure in Complete.
        RunTx(
            [&](auto& txc) {
                NOlap::TDbWrapper db(txc.DB, nullptr);
                accessor->RemoveFromDatabase(db);
                return true;
            },
            [&] {
                if (eraseMemory) {
                    UNIT_ASSERT(granule->ErasePortion(portion->GetPortionId()));
                }
            });
    }
};
}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardAccessorRetry) {
    Y_UNIT_TEST(CompleteIndexListAfterPageFault) {
        TFixture f(1);
        f.RunTx([](auto& txc) {
            txc.DB.Alter().SetFamilyBlobs(Schema::IndexIndexes::TableId, 0, Max<ui32>(), 1_KB);
            return true;
        });
        f.PersistIndexesAndRestart();
        const auto result = f.Ask(f.Portions());
        f.Runtime.WaitFor("accessors", [&] {
            return result->Done;
        });
        UNIT_ASSERT_VALUES_EQUAL(result->Accessors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Accessors.front()->GetIndexesVerified().size(), IndexCount);
    }

    Y_UNIT_TEST_TWIN(RemovedPortionDuringRetry, wasReady) {
        TFixture f(wasReady ? 2 : 1);
        const auto original = f.Ask({ f.Portions().front() });
        f.Runtime.WaitFor("original accessor", [&] {
            return original->Done;
        });
        UNIT_ASSERT_VALUES_EQUAL(original->Accessors.size(), 1);
        f.PersistIndexesAndRestart();
        const auto portions = f.Portions();
        f.WarmRecords(portions);
        if (wasReady) {
            const auto warm = f.Ask({ portions.front() });
            f.Runtime.WaitFor("ready accessor", [&] {
                return warm->Done;
            });
        }
        std::vector<IEventHandle::TPtr> blocked;
        bool hold = true;
        auto observer = f.Runtime.AddObserver<NSharedCache::TEvResult>([&](auto& event) {
            if (hold && event->Cookie == static_cast<ui64>(NSharedCache::ERequestTypeCookie::Transaction)) {
                blocked.emplace_back(event.Release());
            }
        });
        const auto result = f.Ask(portions);
        f.Runtime.WaitFor("index page fault", [&] {
            return !blocked.empty() || result->Done;
        });
        UNIT_ASSERT(!result->Done);
        hold = false;
        f.EraseMetadata(original->Accessors.front(), wasReady);
        for (auto& event : blocked) {
            f.Runtime.Send(event.Release(), 0, true);
        }
        f.Runtime.WaitFor("accessor retry", [&] {
            return result->Done;
        });
        if (wasReady) {
            UNIT_ASSERT_VALUES_EQUAL(result->Accessors.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result->Accessors.front()->GetPortionInfo().GetPortionId(), portions.back()->GetPortionId());
            UNIT_ASSERT_VALUES_EQUAL(result->Accessors.front()->GetIndexesVerified().size(), IndexCount);
        } else {
            UNIT_ASSERT(result->Accessors.empty());
        }
    }
}
}   // namespace NKikimr::NColumnShard
