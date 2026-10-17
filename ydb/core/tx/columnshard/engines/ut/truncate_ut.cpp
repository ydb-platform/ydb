#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup_portions.h>
#include <ydb/core/tx/columnshard/engines/changes/ttl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/tables_manager.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>
#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>

#include <library/cpp/testing/unittest/registar.h>

#include <exception>
#include <functional>

namespace NKikimr::NOlap {
namespace {

// These tests exercise metadata selection and do not read or write blobs.
class TMetadataOnlyAccessorsManager: public NDataAccessorControl::IDataAccessorsManager {
    void DoAskData(const std::shared_ptr<TDataAccessorsRequest>&) override {
        UNIT_FAIL("Unexpected accessor fetch in metadata-only test");
    }

    void DoAddPortion(const std::shared_ptr<TPortionDataAccessor>&) override {
        UNIT_FAIL("Unexpected accessor insertion in metadata-only test");
    }

    void DoRemovePortion(const TPortionInfo::TConstPtr&) override {
    }

public:
    TMetadataOnlyAccessorsManager()
        : IDataAccessorsManager(TActorId())
    {
    }
};

class TTestBodyActor: public NActors::TActorBootstrapped<TTestBodyActor> {
    const std::function<void()> Body;
    const TActorId Recipient;
    std::exception_ptr& Error;

public:
    TTestBodyActor(std::function<void()> body, TActorId recipient, std::exception_ptr& error)
        : Body(std::move(body))
        , Recipient(recipient)
        , Error(error)
    {
    }

    void Bootstrap() {
        try {
            Body();
        } catch (...) {
            Error = std::current_exception();
        }
        Send(Recipient, new NActors::TEvents::TEvWakeup);
        PassAway();
    }
};

void RunInActorContext(std::function<void()> body) {
    TTestBasicRuntime runtime;
    NTxUT::TTester::Setup(runtime);
    const auto sender = runtime.AllocateEdgeActor();
    std::exception_ptr error;
    runtime.Register(new TTestBodyActor(std::move(body), sender, error));
    UNIT_ASSERT(runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(sender));
    if (error) {
        std::rethrow_exception(error);
    }
}

class TTruncateFixture {
public:
    const TInternalPathId PathId = TInternalPathId::FromRawValue(1);
    const std::shared_ptr<IStoragesManager> Storages = TTestStoragesManager::GetInstance();
    std::shared_ptr<TTruncateSnapshots> History = std::make_shared<TTruncateSnapshots>();
    TColumnEngineForLogs Engine{ 0, std::make_shared<TSchemaObjectsCache>(), std::make_shared<TMetadataOnlyAccessorsManager>(), Storages,
        TSnapshot(1, 1), NTest::MakePortionTestIndexInfo(), std::make_shared<NColumnShard::TPortionIndexStats>() };

    TTruncateFixture() {
        Engine.RegisterTable(PathId);
        Engine.MutableGranuleVerified(PathId).BindTruncateSnapshots(History);
    }

    std::shared_ptr<TPortionInfo> MakePortion(ui64 id, const TSnapshot& min, const TSnapshot& max) const {
        auto portion = NTest::MakeTestCompactedPortion(PathId, id, id * 10, id * 10 + 9, 10, max, std::nullopt);
        auto proto =
            portion->GetMeta().SerializeToProto({ TUnifiedBlobId(1, TLogoBlobID(1, 1, 1, 1, 100, 0)) }, NPortion::EProduced::SPLIT_COMPACTED);
        min.SerializeToProto(*proto.MutableRecordSnapshotMin());
        max.SerializeToProto(*proto.MutableRecordSnapshotMax());
        auto constructor = portion->BuildConstructor(false);
        TPortionMetaConstructor meta;
        TFakeGroupSelector groupSelector;
        UNIT_ASSERT(meta.LoadMetadata(proto, NTest::MakePortionTestIndexInfo(), groupSelector));
        constructor->MutableMeta() = std::move(meta);
        return constructor->Build();
    }

    std::shared_ptr<TTTLColumnEngineChanges> MakeChanges(const std::shared_ptr<TPortionInfo>& portion, bool move) const {
        auto changes = std::make_shared<TTTLColumnEngineChanges>(NActualizer::TRWAddress({}, {}), TSaverContext(Storages));
        if (move) {
            changes->AddMovePortions({ portion });
            changes->SetTargetCompactionLevel(1);
        } else {
            changes->AddPortionToRemove(portion);
        }
        return changes;
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TTruncateEngine) {
    Y_UNIT_TEST_DUO(CleanupDoesNotReselectErasedPortion, SamePlanStep) {
        RunInActorContext([] {
            TTruncateFixture f;
            auto portion = f.MakePortion(1, TSnapshot(10, 1), TSnapshot(10, 1));
            portion->SetRemoveSnapshot(TSnapshot(20, 5));
            f.Engine.AppendPortion(portion);
            auto laterPortion = f.MakePortion(2, TSnapshot(10, 1), TSnapshot(10, 1));
            laterPortion->SetRemoveSnapshot(TSnapshot(40, 1));
            f.Engine.AppendPortion(laterPortion);
            auto locks = std::make_shared<NDataLocks::TManager>();
            const TLegacySnapshotHolders holders(TSnapshot(30, 1), {});
            auto first = f.Engine.StartCleanupPortions(holders, {}, locks);
            UNIT_ASSERT(first);
            UNIT_ASSERT_VALUES_EQUAL(first->GetPortionsToDrop().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(first->GetPortionsToDrop().front()->GetPortionId(), portion->GetPortionId());
            auto guard = locks->RegisterLock(std::make_shared<NDataLocks::TListPortionsLock>(
                "running_cleanup", first->GetPortionsToDrop(), NDataLocks::ELockCategory::Cleanup));

            // A delayed truncate requeues both the in-flight portion and one still awaiting cleanup.
            const TSnapshot truncate(SamePlanStep ? 20 : 15, 3);
            f.History->emplace(truncate, ETruncateState::MarkedPortions);
            f.Engine.ApplyTruncateSnapshots(f.PathId);
            UNIT_ASSERT_VALUES_EQUAL(portion->GetRemoveSnapshotVerified(), truncate);
            UNIT_ASSERT_VALUES_EQUAL(laterPortion->GetRemoveSnapshotVerified(), truncate);

            first->SetFetchedDataAccessors(
                TDataAccessorsResult(), TDataAccessorsInitializationContext(f.Engine.GetVersionedIndexReadonlyCopy()));
            first->SetStage(NChanges::EStage::Written);
            TWriteIndexCompleteContext complete(
                NActors::TActivationContext::AsActorContext(), 0, 0, TDuration::Zero(), f.Engine, TSnapshot(30, 1));
            first->WriteIndexOnComplete(nullptr, complete);
            guard->Release(*locks);
            UNIT_ASSERT(!f.Engine.GetGranuleVerified(f.PathId).GetPortionOptional(portion->GetPortionId()));
            auto next = f.Engine.StartCleanupPortions(holders, {}, locks);
            UNIT_ASSERT(next);
            UNIT_ASSERT_VALUES_EQUAL_C(next->GetPortionsToDrop().size(), 1, "cleanup reselected an already erased portion");
            UNIT_ASSERT_VALUES_EQUAL(next->GetPortionsToDrop().front()->GetPortionId(), laterPortion->GetPortionId());
            UNIT_ASSERT(next->HasTruncatesToRemove());
        });
    }

    Y_UNIT_TEST(MixedInputCancellation) {
        RunInActorContext([] {
            TTruncateFixture f;
            const TSnapshot truncate(15, 1);
            for (const bool move : { false, true }) {
                auto mixed = f.MakeChanges(f.MakePortion(1, TSnapshot(10, 1), TSnapshot(20, 1)), move);
                auto newer = f.MakeChanges(f.MakePortion(2, truncate, TSnapshot(20, 1)), move);
                UNIT_ASSERT(!mixed->IsCancelled(f.Engine));
                for (const auto state : { ETruncateState::Created, ETruncateState::MarkedPortions, ETruncateState::RemovedFromDB }) {
                    (*f.History)[truncate] = state;
                    UNIT_ASSERT(mixed->IsCancelled(f.Engine));
                    UNIT_ASSERT(!newer->IsCancelled(f.Engine));
                }
                f.History->clear();
            }
        });
    }

    Y_UNIT_TEST(MixedInputLockKeepsMarker) {
        RunInActorContext([] {
            for (const auto category : { NDataLocks::ELockCategory::Compaction, NDataLocks::ELockCategory::Actualization }) {
                TTruncateFixture f;
                auto portion = f.MakePortion(1, TSnapshot(10, 1), TSnapshot(20, 1));
                f.Engine.AppendPortion(portion);
                auto changes = f.MakeChanges(portion, category == NDataLocks::ELockCategory::Compaction);
                auto locks = std::make_shared<NDataLocks::TManager>();
                auto guard = locks->RegisterLock(
                    std::make_shared<NDataLocks::TListPortionsLock>("background", std::vector<TPortionInfo::TConstPtr>{ portion }, category));
                f.History->emplace(TSnapshot(15, 1), ETruncateState::MarkedPortions);
                f.Engine.ApplyTruncateSnapshots(f.PathId);
                UNIT_ASSERT(!portion->HasRemoveSnapshot());
                const TLegacySnapshotHolders holders(TSnapshot(30, 1), {});
                // The portion has no cleanup entry, but its producer still needs T=15.
                UNIT_ASSERT(!f.Engine.StartCleanupPortions(holders, {}, locks));
                UNIT_ASSERT(!f.Engine.StartCleanupPortions(holders, {}, locks));
                UNIT_ASSERT(changes->IsCancelled(f.Engine));
                guard->Release(*locks);
                auto cleanup = f.Engine.StartCleanupPortions(holders, {}, locks);
                UNIT_ASSERT(cleanup && cleanup->HasTruncatesToRemove());
                UNIT_ASSERT(cleanup->GetPortionsToDrop().empty());
            }
        });
    }

    Y_UNIT_TEST(PartialCleanupKeepsMarker) {
        RunInActorContext([] {
            TTruncateFixture f;
            constexpr ui64 portionsCount = 1001;
            for (ui64 id = 1; id <= portionsCount; ++id) {
                f.Engine.AppendPortion(f.MakePortion(id, TSnapshot(10, 1), TSnapshot(10, 1)));
            }
            f.History->emplace(TSnapshot(15, 1), ETruncateState::MarkedPortions);
            f.Engine.ApplyTruncateSnapshots(f.PathId);
            auto locks = std::make_shared<NDataLocks::TManager>();
            const TLegacySnapshotHolders holders(TSnapshot(30, 1), {});
            auto first = f.Engine.StartCleanupPortions(holders, {}, locks);
            UNIT_ASSERT(first && !first->GetPortionsToDrop().empty());
            UNIT_ASSERT(first->GetPortionsToDrop().size() < portionsCount);
            UNIT_ASSERT(!first->HasTruncatesToRemove());
            // Selection has transferred the first batch out of the GC indexes.
            auto last = f.Engine.StartCleanupPortions(holders, {}, locks);
            UNIT_ASSERT(last && last->HasTruncatesToRemove());
            UNIT_ASSERT_VALUES_EQUAL(first->GetPortionsToDrop().size() + last->GetPortionsToDrop().size(), portionsCount);
        });
    }

    Y_UNIT_TEST(MarkerWaitsForMarkingAndSnapshotFloor) {
        RunInActorContext([] {
            TTruncateFixture f;
            const TSnapshot truncate(15, 100);
            f.History->emplace(truncate, ETruncateState::Created);
            f.Engine.ApplyTruncateSnapshots(f.PathId);
            auto locks = std::make_shared<NDataLocks::TManager>();
            UNIT_ASSERT(!f.Engine.StartCleanupPortions(TLegacySnapshotHolders(TSnapshot(30, 1), {}), {}, locks));
            f.History->at(truncate) = ETruncateState::MarkedPortions;
            UNIT_ASSERT(!f.Engine.StartCleanupPortions(TLegacySnapshotHolders(TSnapshot(15, 99), {}), {}, locks));
            auto cleanup = f.Engine.StartCleanupPortions(TLegacySnapshotHolders(truncate, {}), {}, locks);
            UNIT_ASSERT(cleanup && cleanup->HasTruncatesToRemove());
        });
    }

    Y_UNIT_TEST(MarkerCleanupRetainsLaterTxIds) {
        RunInActorContext([] {
            TTruncateFixture f;
            f.History->emplace(TSnapshot(14, 1), ETruncateState::RemovedFromDB);
            f.History->emplace(TSnapshot(15, 99), ETruncateState::MarkedPortions);
            f.History->emplace(TSnapshot(15, 100), ETruncateState::Created);
            f.History->emplace(TSnapshot(15, 101), ETruncateState::MarkedPortions);
            f.History->emplace(TSnapshot(16, 1), ETruncateState::MarkedPortions);
            f.Engine.ApplyTruncateSnapshots(f.PathId);
            auto locks = std::make_shared<NDataLocks::TManager>();
            const auto select = [&](const TSnapshot& floor) {
                return f.Engine.StartCleanupPortions(TLegacySnapshotHolders(floor, {}), {}, locks);
            };
            auto first = select(TSnapshot(15, 99));
            UNIT_ASSERT(first && first->HasTruncatesToRemove());
            f.History->at(TSnapshot(15, 99)) = ETruncateState::RemovedFromDB;
            // The lower TxId is retired, the middle one is not yet marked, and the others are too new.
            UNIT_ASSERT(!select(TSnapshot(15, 100)));
            f.History->at(TSnapshot(15, 100)) = ETruncateState::MarkedPortions;
            auto middle = select(TSnapshot(15, 100));
            UNIT_ASSERT(middle && middle->HasTruncatesToRemove());
            f.History->at(TSnapshot(15, 100)) = ETruncateState::RemovedFromDB;
            UNIT_ASSERT(!select(TSnapshot(15, 100)));
            auto lastInStep = select(TSnapshot(15, 101));
            UNIT_ASSERT(lastInStep && lastInStep->HasTruncatesToRemove());
            f.History->at(TSnapshot(15, 101)) = ETruncateState::RemovedFromDB;
            UNIT_ASSERT(!select(TSnapshot(15, 101)));
            auto nextStep = select(TSnapshot(16, 1));
            UNIT_ASSERT(nextStep && nextStep->HasTruncatesToRemove());
        });
    }

    Y_UNIT_TEST(HistoryPersistsRemovalBeforeComplete) {
        RunInActorContext([] {
            using namespace NColumnShard;
            TTruncateFixture f;
            NTable::NTest::TDbExec storage;
            const auto source = TSchemeShardLocalPathId::FromRawValue(1);
            const auto copy = TSchemeShardLocalPathId::FromRawValue(2);
            TTableInfo table({ TUnifiedPathId::BuildValid(f.PathId, source) });
            f.Engine.MutableGranuleVerified(f.PathId).BindTruncateSnapshots(table.GetTruncateSnapshotsPtr());
            const TSnapshot oldTruncate(15, 1);
            const TSnapshot newTruncate(25, 1);
            storage.Begin();
            {
                NIceDb::TNiceDb db(*storage.operator->());
                db.Materialize<Schema>();
                table.AddTruncate(oldTruncate, db);
                table.MarkTruncatePortions();
            }
            storage.Commit();
            storage.Begin();
            {
                NIceDb::TNiceDb db(*storage.operator->());
                table.RemoveTruncatesOnExecute({ oldTruncate }, db);
            }
            storage.Commit();
            UNIT_ASSERT(table.GetTruncateSnapshots().contains(oldTruncate));
            UNIT_ASSERT(f.MakeChanges(f.MakePortion(1, TSnapshot(10, 1), TSnapshot(20, 1)), false)->IsCancelled(f.Engine));

            auto readDurableHistory = [&](TSchemeShardLocalPathId alias) {
                storage.Begin();
                NIceDb::TNiceDb db(*storage.operator->());
                auto row = db.Table<Schema::TableInfoV1>().Key(f.PathId.GetRawValue(), alias.GetRawValue()).Select();
                UNIT_ASSERT(row.IsReady() && !row.EndOfSet());
                auto restored = TTableInfo::InitFromDBV1(row);
                storage.Commit();
                return TTruncateSnapshots(restored.GetTruncateSnapshots());
            };
            UNIT_ASSERT(readDurableHistory(source).empty());

            // Another transaction serializes the history before cleanup.Complete.
            storage.Begin();
            {
                NIceDb::TNiceDb db(*storage.operator->());
                table.AddTruncate(newTruncate, db);
                table.MarkTruncatePortions();
                table.CopySchemeShardLocalPathId(db, source, copy, TSnapshot(26, 1));
            }
            storage.Commit();
            UNIT_ASSERT(table.GetTruncateSnapshots().at(oldTruncate) == ETruncateState::RemovedFromDB);
            for (const auto alias : { source, copy }) {
                const auto history = readDurableHistory(alias);
                UNIT_ASSERT_VALUES_EQUAL(history.size(), 1);
                UNIT_ASSERT(history.contains(newTruncate));
            }
            // Replaying durable data before Complete must not resurrect the old marker.
            storage.Replay(NTable::NTest::EPlay::Boot);
            UNIT_ASSERT(!readDurableHistory(source).contains(oldTruncate));
            UNIT_ASSERT(!readDurableHistory(copy).contains(oldTruncate));
            table.RemoveTruncatesOnComplete({ oldTruncate });
            UNIT_ASSERT_VALUES_EQUAL(table.GetTruncateSnapshots().size(), 1);
            UNIT_ASSERT(table.GetTruncateSnapshots().contains(newTruncate));
        });
    }
}

}   // namespace NKikimr::NOlap
