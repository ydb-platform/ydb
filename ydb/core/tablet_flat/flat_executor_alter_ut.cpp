#include "flat_executor_ut_common.h"
#include "logic_alter_main.h"

namespace NKikimr::NTabletFlatExecutor {

Y_UNIT_TEST_SUITE(TFlatTableExecutor_Alter) {
    Y_UNIT_TEST(ObsoleteSchemaBodiesDeletedOnce) {
        const TVector<NPageCollection::TSlot> slots{{1, 1}};
        TLogicAlter logic(new NPageCollection::TSteppedCookieAllocator(
            123, ui64(3) << 32, NPageCollection::TCookieRange{0, 999}, slots));
        const TLogoBlobID obsolete(123, 2, 1, 1, 42, 0);
        const TLogoBlobID current(123, 3, 1, 1, 42, 0);
        logic.RestoreLog({1, obsolete});
        logic.Clear();
        logic.RestoreLog({1, current});

        const TLogoBlobID unrelated(123, 2, 2, 2, 42, 0);
        TLogCommit commit(true, 2, ECommit::Snap, {});
        commit.GcDelta.Deleted.push_back(unrelated);
        NKikimrExecutorFlat::TLogSnapshot snapshot;
        logic.SnapToLog(snapshot, commit);
        // Deletions must enter the current commit's GC delta, not only boot's snapshot.
        UNIT_ASSERT_VALUES_EQUAL(commit.GcDelta.Deleted.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(commit.GcDelta.Deleted[0], unrelated);
        UNIT_ASSERT_VALUES_EQUAL(commit.GcDelta.Deleted[1], obsolete);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GcSnapLeftSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.SchemeInfoBodiesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(snapshot.GetSchemeInfoBodies(0)), current);

        TLogCommit nextCommit(true, 3, ECommit::Snap, {});
        NKikimrExecutorFlat::TLogSnapshot nextSnapshot;
        logic.SnapToLog(nextSnapshot, nextCommit);
        UNIT_ASSERT(nextCommit.GcDelta.Deleted.empty());
        UNIT_ASSERT_VALUES_EQUAL(nextSnapshot.GcSnapLeftSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(nextSnapshot.SchemeInfoBodiesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(nextSnapshot.GetSchemeInfoBodies(0)), current);
    }

    struct TTxSchema : ITransaction {
        explicit TTxSchema(bool create) : Create(create) {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            if (Create) {
                txc.DB.Alter()
                    .AddTable("test", 101)
                    .AddColumn(101, "key", 1, NScheme::TUint64::TypeId, false, false)
                    .AddColumnToKey(101, 1);
            } else {
                const auto* table = txc.DB.GetScheme().GetTableInfo(101);
                UNIT_ASSERT(table);
                UNIT_ASSERT_VALUES_EQUAL(table->Name, "test");
                UNIT_ASSERT_VALUES_EQUAL(table->KeyColumns.size(), 1);
            }
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

        const bool Create;
    };

    void CheckSchemaFromRedo(bool reassign) {
        struct TReassignedStarter : NFake::TStarter {
            NFake::TStorageInfo* MakeTabletInfo(ui64 tablet, ui32 channels) noexcept override {
                auto* info = TStarter::MakeTabletInfo(tablet, channels);
                info->Channels[1].History.emplace_back(3, 2);
                return info;
            }
        } starter;

        TMyEnvBase env;
        auto fire = [&](NFake::TStarter* customStarter) {
            env.FireTablet(env.Edge, env.Tablet, [&env](const TActorId& tablet, TTabletStorageInfo* info) {
                return new NFake::TDummy(tablet, info, env.Edge, 0);
            }, 0, customStarter);
            env.WaitFor<NFake::TEvReady>();
        };
        fire(nullptr);

        ui32 snapshots = 0;
        TVector<TLogoBlobID> schemaBodies;
        auto observer = env->AddObserver<TEvTablet::TEvCommit>([&](auto& ev) {
            const auto* commit = ev->Get();
            snapshots += commit->IsSnapshot;
            for (const auto& blob : commit->GcDiscovered) {
                if (blob.Channel() == 1) {
                    schemaBodies.push_back(blob);
                }
            }
        });
        env.SendSync(new NFake::TEvExecute{new TTxSchema(true)});
        UNIT_ASSERT_VALUES_EQUAL(snapshots, 0); // The schema exists only in redo.
        UNIT_ASSERT_VALUES_EQUAL(schemaBodies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(schemaBodies.front().Generation(), 2);
        env.SendSync(new TEvents::TEvPoison, false, true);

        schemaBodies.clear();
        fire(reassign ? &starter : nullptr);
        env.SendSync(new NFake::TEvExecute{new TTxSchema(false)});
        UNIT_ASSERT_VALUES_EQUAL(schemaBodies.size(), reassign ? 1 : 0);
        if (reassign) {
            UNIT_ASSERT_VALUES_EQUAL(schemaBodies.front().Generation(), 3);
        }
        env.SendSync(new TEvents::TEvPoison, false, true);

        // The rewritten schema must also survive replay on a subsequent boot.
        fire(reassign ? &starter : nullptr);
        env.SendSync(new NFake::TEvExecute{new TTxSchema(false)});
        env.SendSync(new TEvents::TEvPoison, false, true);
    }

    Y_UNIT_TEST(RewriteReassignedSchemaFromRedo) {
        CheckSchemaFromRedo(true);
    }

    Y_UNIT_TEST(KeepSchemaFromRedoOnUnchangedChannel) {
        CheckSchemaFromRedo(false);
    }
}

} // namespace NKikimr::NTabletFlatExecutor
