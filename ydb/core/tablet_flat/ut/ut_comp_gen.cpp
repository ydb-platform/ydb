#include "flat_comp_ut_common.h"

#include <ydb/core/tablet_flat/flat_comp_gen.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {
namespace NCompGen {

using namespace NTest;

Y_UNIT_TEST_SUITE(TGenCompaction) {

    constexpr ui32 Table = 1;

    struct Schema : NIceDb::Schema {
        struct Data : Table<1> {
            struct Key : Column<1, NScheme::NTypeIds::Uint64> { };
            struct Value : Column<2, NScheme::NTypeIds::Uint32> { };

            using TKey = TableKey<Key>;
            using TColumns = TableColumns<Key, Value>;
        };

        using TTables = SchemaTables<Data>;
    };

    Y_UNIT_TEST(OverloadFactorDuringForceCompaction) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            TCompactionPolicy policy;

            // almost randome values except forceCountToCompact = 1 and forceSizeToCompact = 100 GB
            TCompactionPolicy::TGenerationPolicy genPolicy(1, 1, 1, 10ULL*1024*1024*1024, "whoknows", true);

            for (size_t i = 0; i < 5; ++i) {
                policy.Generations.push_back(genPolicy);
            }

            backend.DB.Alter().SetCompactionPolicy(Table, policy);

            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });

        const ui64 rowsPerTx = 16 * 1024;
        for (ui64 index = 0; index < 3; ++index) {
            const ui64 base = index;
            auto db = backend.Begin();
            for (ui64 seq = 0; seq < rowsPerTx; ++seq) {
                db.Table<Schema::Data>().Key(base + seq * 3).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
            backend.SimpleMemCompaction(&strategy);
        }

        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(strategy.GetOverloadFactor(), 1);
        UNIT_ASSERT(strategy.AllowForcedCompaction());

        // run forced mem compaction
        backend.SimpleMemCompaction(&strategy, true);

        // forced compaction is in progress (waiting resource broker to start gen compaction)
        UNIT_ASSERT(!strategy.AllowForcedCompaction());
        UNIT_ASSERT_VALUES_EQUAL(strategy.GetOverloadFactor(), 1);

        // finish forced compaction
        while (broker.HasPending()) {
            UNIT_ASSERT(broker.RunPending());
            if (backend.StartedCompactions.empty())
                continue;

            auto result = backend.RunCompaction();
            auto changes = strategy.CompactionFinished(
                    result.CompactionId, std::move(result.Params), std::move(result.Result));
            backend.ApplyChanges(Table, std::move(changes));
        }

        UNIT_ASSERT(strategy.AllowForcedCompaction());
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(strategy.GetOverloadFactor(), 0);
    }

    Y_UNIT_TEST(ForcedCompactionNoGenerations) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            TCompactionPolicy policy;
            backend.DB.Alter().SetCompactionPolicy(Table, policy);

            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });

        // Insert some rows
        {
            auto db = backend.Begin();
            for (ui64 key = 0; key < 64; ++key) {
                db.Table<Schema::Data>().Key(key).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
        }

        // Start a forced mem compaction with id 123
        {
            auto memCompactionId = strategy.BeginMemCompaction(0, { 0, TEpoch::Max() }, 123);
            UNIT_ASSERT(memCompactionId != 0);
            auto outcome = backend.RunCompaction(memCompactionId);

            UNIT_ASSERT(outcome.Params);
            UNIT_ASSERT(!outcome.Params->Parts);
            UNIT_ASSERT(outcome.Params->IsFinal);

            auto changes = strategy.CompactionFinished(
                    memCompactionId, std::move(outcome.Params), std::move(outcome.Result));

            // We expect forced compaction to place results on level 255
            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartsLevel, 255u);

            // We expect forced compaction to be finished and a new one immediately allowed
            UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionId(), 123u);
            UNIT_ASSERT(strategy.AllowForcedCompaction());
        }

        // Insert some more rows
        {
            auto db = backend.Begin();
            for (ui64 key = 64; key < 128; ++key) {
                db.Table<Schema::Data>().Key(key).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
        }

        // Start a forced mem compaction with id 234
        {
            auto memCompactionId = strategy.BeginMemCompaction(0, { 0, TEpoch::Max() }, 234);
            UNIT_ASSERT(memCompactionId != 0);
            auto outcome = backend.RunCompaction(memCompactionId);

            UNIT_ASSERT(outcome.Params);
            UNIT_ASSERT(outcome.Params->Parts);
            UNIT_ASSERT(outcome.Params->IsFinal);

            auto changes = strategy.CompactionFinished(
                    memCompactionId, std::move(outcome.Params), std::move(outcome.Result));

            // We expect forced compaction to place results on level 255
            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartsLevel, 255u);

            // We expect forced compaction to be finished and a new one immediately allowed
            UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionId(), 234);
            UNIT_ASSERT(strategy.AllowForcedCompaction());
        }

        // Don't expect any tasks or change requests
        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT(!backend.StartedCompactions);
        UNIT_ASSERT(!backend.CheckChangesFlag());
    }

    Y_UNIT_TEST(EraseAllMemCompactionKeepsEraseMarkers) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        {
            auto db = backend.Begin();
            db.Materialize<Schema>();
            backend.DB.Alter().SetCompactionPolicy(Table, TCompactionPolicy());
            db.Table<Schema::Data>().Key(0).Update<Schema::Data::Value>(42);
            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });
        backend.SimpleMemCompaction(&strategy);

        {
            backend.Begin();
            backend.DB.Truncate(Table, TRowVersion(5, 1));
            backend.Commit();
        }
        {
            auto db = backend.Begin();
            db.Table<Schema::Data>().Key(1).Update<Schema::Data::Value>(43);
            backend.Commit();
        }
        backend.SimpleMemCompaction(&strategy);

        {
            auto db = backend.Begin();
            db.Table<Schema::Data>().Key(1).Delete();
            backend.Commit();
        }

        auto compactionId = strategy.BeginMemCompaction(0, { 0, TEpoch::Max() }, 0);
        UNIT_ASSERT(!backend.StartedCompactions.at(compactionId)->IsFinal);
        auto outcome = backend.RunCompaction(compactionId);
        auto changes = strategy.CompactionFinished(
                compactionId, std::move(outcome.Params), std::move(outcome.Result));
        backend.ApplyChanges(Table, std::move(changes));

        {
            auto db = backend.Begin();
            auto row = db.Table<Schema::Data>().Key(1).Select<Schema::Data::Value>();
            UNIT_ASSERT(row.IsReady());
            UNIT_ASSERT(!row.IsValid());
            backend.Commit();
        }
    }

    Y_UNIT_TEST(EraseAllCompactionWithoutGenerations) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        {
            auto db = backend.Begin();
            db.Materialize<Schema>();
            backend.DB.Alter().SetCompactionPolicy(Table, TCompactionPolicy());
            db.Table<Schema::Data>().Key(0).Update<Schema::Data::Value>(42);
            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });
        backend.SimpleMemCompaction(&strategy);
        {
            backend.Begin();
            backend.DB.Truncate(Table, TRowVersion(5, 1));
            backend.Commit();
        }

        for (ui32 value = 0; value < 3; ++value) {
            {
                auto db = backend.Begin();
                db.Table<Schema::Data>().Key(1).UpdateV<Schema::Data::Value>(TRowVersion(6 + value, 1), value);
                backend.Commit();
            }
            backend.SimpleMemCompaction(&strategy);
            // The old snapshot group and the current group each need one part.
            UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 2u);
        }

        auto compactionId = strategy.BeginMemCompaction(0, { 0, TEpoch::Max() }, 123);
        UNIT_ASSERT_VALUES_EQUAL(backend.StartedCompactions.at(compactionId)->Parts.size(), 1u);
        auto outcome = backend.RunCompaction(compactionId);
        auto changes = strategy.CompactionFinished(
                compactionId, std::move(outcome.Params), std::move(outcome.Result));
        backend.ApplyChanges(Table, std::move(changes));
        UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionId(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 2u);

        {
            auto db = backend.Begin();
            auto current = db.Table<Schema::Data>().Key(1).Select<Schema::Data::Value>();
            UNIT_ASSERT(current.IsReady());
            UNIT_ASSERT(current.IsValid());
            UNIT_ASSERT_VALUES_EQUAL(current.GetValue<Schema::Data::Value>(), 2u);
            auto hidden = db.Table<Schema::Data>().Key(0).Select<Schema::Data::Value>();
            UNIT_ASSERT(hidden.IsReady());
            UNIT_ASSERT(!hidden.IsValid());

            const ui64 oldKey = 0;
            const NIceDb::TTypeValue rawKey(oldKey);
            const ui32 tags[] = { 2 };
            TRowState historical;
            UNIT_ASSERT(backend.DB.Select(Table, { &rawKey, 1 }, tags, historical, 0, TRowVersion(4, 1)) == EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(historical.Get(0).AsValue<ui32>(), 42u);
            backend.Commit();
        }
    }

    void TestEraseAllBorrowedCompaction(bool removePart, bool forceAfter = false) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        {
            auto db = backend.Begin();
            db.Materialize<Schema>();
            backend.DB.Alter().SetCompactionPolicy(Table, TCompactionPolicy());
            db.Table<Schema::Data>().Key(0).Update<Schema::Data::Value>(42);
            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });
        backend.SimpleMemCompaction(&strategy);
        {
            backend.Begin();
            backend.DB.Truncate(Table, TRowVersion(5, 1));
            backend.Commit();
        }

        {
            auto db = backend.Begin();
            db.Table<Schema::Data>().Key(1).UpdateV<Schema::Data::Value>(TRowVersion(6, 1), 43);
            backend.Commit();
        }
        backend.SimpleMemCompaction(&strategy);
        {
            backend.Begin();
            backend.DB.Truncate(Table, TRowVersion(10, 1));
            backend.Commit();
        }

        strategy.Stop();
        ++backend.TabletId;
        strategy.Start({ });
        {
            auto db = backend.Begin();
            db.Table<Schema::Data>().Key(2).UpdateV<Schema::Data::Value>(TRowVersion(11, 1), 44);
            backend.Commit();
        }
        backend.SimpleMemCompaction(&strategy);

        // The owned visible group is now ahead of two borrowed hidden groups.
        UNIT_ASSERT(strategy.ScheduleBorrowedCompaction());
        if (removePart) {
            // Part removal cancels pending work and rebuilds the strategy.
            for (const auto& part : backend.TableParts(Table)) {
                if (part->Label.TabletID() == backend.TabletId) {
                    const auto label = part->Label;
                    auto subset = backend.DB.PartSwitchSubset(Table, TEpoch::Zero(), { label }, { });
                    backend.DB.Replace(Table, *subset, { }, { });
                    strategy.PartsRemoved({ label });
                    break;
                }
            }
        }
        ui32 compactions = 0;
        while (broker.RunPending()) {
            UNIT_ASSERT_C(++compactions <= 2, "Borrowed compaction made no progress");
            auto outcome = backend.RunCompaction();
            UNIT_ASSERT_VALUES_EQUAL(outcome.Params->Parts.size(), 1u);
            UNIT_ASSERT(outcome.Params->Parts.front()->Label.TabletID() != backend.TabletId);
            broker.FinishTask(outcome.Params->TaskId, EResourceStatus::Finished);
            auto changes = strategy.CompactionFinished(
                outcome.CompactionId, std::move(outcome.Params), std::move(outcome.Result));
            backend.ApplyChanges(Table, std::move(changes));
        }

        UNIT_ASSERT_VALUES_EQUAL(compactions, 2u);
        UNIT_ASSERT(!strategy.ScheduleBorrowedCompaction());
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), removePart ? 2u : 3u);
        for (const auto& part : backend.TableParts(Table)) {
            UNIT_ASSERT_VALUES_EQUAL(part->Label.TabletID(), backend.TabletId);
        }
        if (forceAfter) {
            // Borrowed compaction left the oldest hidden group at the front.
            // Repeated full passes must reach every original visibility group.
            const auto parts = backend.TableParts(Table);
            for (size_t pass = 0; pass < parts.size(); ++pass) {
                const auto forcedId = backend.SimpleMemCompaction(&strategy, true);
                UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionId(), forcedId);
            }
            for (const auto& part : parts) {
                UNIT_ASSERT(!backend.DB.GetPartView(Table, part->Label));
            }
            for (ui32 pass = 0; pass < 3; ++pass) {
                {
                    auto db = backend.Begin();
                    db.Table<Schema::Data>().Key(2).UpdateV<Schema::Data::Value>(TRowVersion(20 + pass, 1), 45 + pass);
                    backend.Commit();
                }
                backend.SimpleMemCompaction(&strategy);
                UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), parts.size());
                backend.SimpleMemCompaction(&strategy, true);
                UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), parts.size());
            }
        }
    }

    Y_UNIT_TEST(EraseAllFullCompactionAfterBorrowed) {
        TestEraseAllBorrowedCompaction(false, true);
    }

    Y_UNIT_TEST(EraseAllBorrowedCompactionMakesProgress) {
        TestEraseAllBorrowedCompaction(false);
    }

    Y_UNIT_TEST(EraseAllBorrowedCompactionAfterPartsRemoved) {
        TestEraseAllBorrowedCompaction(true);
    }

    Y_UNIT_TEST(EraseAllColdCompactionMakesProgress) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        {
            auto db = backend.Begin();
            db.Materialize<Schema>();
            backend.DB.Alter().SetCompactionPolicy(Table, TCompactionPolicy());
            db.Table<Schema::Data>().Key(1).Update<Schema::Data::Value>(43);
            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });
        backend.SimpleMemCompaction(&strategy);

        TIntrusiveConstPtr<TColdPart> cold = new TColdPart(
            TLogoBlobID(backend.TabletId - 1, 1, 1, 1, 1, 0), TEpoch::Zero(), TRowVersion(5, 1));
        backend.DB.Merge(Table, cold);
        strategy.PartMerged(cold, 255);

        UNIT_ASSERT(strategy.ScheduleBorrowedCompaction());
        UNIT_ASSERT(broker.RunPending());
        UNIT_ASSERT_VALUES_EQUAL(backend.StartedCompactions.size(), 1u);
        const auto& params = *backend.StartedCompactions.begin()->second;
        UNIT_ASSERT(params.Parts.empty());
        UNIT_ASSERT_VALUES_EQUAL(params.ColdParts.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(params.ColdParts.front()->Label, cold->Label);
        UNIT_ASSERT(!params.IsFinal);
        strategy.Stop();

        // A full compaction must also reach cold data behind another warm group.
        strategy.Start({ });
        const auto compactionId = strategy.BeginMemCompaction(0, { 0, TEpoch::Max() }, 123);
        const auto& forced = *backend.StartedCompactions.at(compactionId);
        UNIT_ASSERT(forced.Parts.empty());
        UNIT_ASSERT_VALUES_EQUAL(forced.ColdParts.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(forced.ColdParts.front()->Label, cold->Label);
        strategy.Stop();
    }

    void TestEraseAllForcedCompaction(bool concurrentWrites, ui32 hiddenGroups = 1) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        {
            auto db = backend.Begin();
            db.Materialize<Schema>();
            TCompactionPolicy policy;
            policy.Generations.emplace_back(10 * 1024 * 1024, 2, 10, 100 * 1024 * 1024, "compact_gen1", true);
            backend.DB.Alter().SetCompactionPolicy(Table, policy);
            db.Table<Schema::Data>().Key(0).Update<Schema::Data::Value>(42);
            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });
        backend.SimpleMemCompaction(&strategy);

        for (ui32 group = 1; group <= hiddenGroups; ++group) {
            backend.Begin();
            backend.DB.Truncate(Table, TRowVersion(5 * group, 1));
            backend.Commit();

            auto db = backend.Begin();
            db.Table<Schema::Data>().Key(group).Update<Schema::Data::Value>(43);
            backend.Commit();
            backend.SimpleMemCompaction(&strategy);
        }

        constexpr ui64 forcedId = 123;
        const auto memCompactionId = strategy.BeginMemCompaction(0, { 0, TEpoch::Max() }, forcedId);
        auto memOutcome = backend.RunCompaction(memCompactionId);
        UNIT_ASSERT(memOutcome.Result->Parts.empty());
        UNIT_ASSERT(memOutcome.Result->Epoch == TEpoch::Max());
        auto memChanges = strategy.CompactionFinished(
            memCompactionId, std::move(memOutcome.Params), std::move(memOutcome.Result));
        backend.ApplyChanges(Table, std::move(memChanges));
        UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionId(), 0u);

        for (ui32 group = 0; group <= hiddenGroups; ++group) {
            UNIT_ASSERT(broker.RunPending());
            UNIT_ASSERT_VALUES_EQUAL(backend.StartedCompactions.size(), 1u);
            const auto compactionId = backend.StartedCompactions.begin()->first;
            if (concurrentWrites) {
                // These writes must not keep extending the original forced pass.
                {
                    auto db = backend.Begin();
                    db.Table<Schema::Data>().Key(100 + group).Update<Schema::Data::Value>(44);
                    backend.Commit();
                }
                backend.SimpleMemCompaction(&strategy);
            }
            auto outcome = backend.RunCompaction(compactionId);
            UNIT_ASSERT_VALUES_EQUAL(outcome.Params->Parts.size(),
                concurrentWrites && group == hiddenGroups ? hiddenGroups + 1 : 1u);
            broker.FinishTask(outcome.Params->TaskId, EResourceStatus::Finished);
            auto changes = strategy.CompactionFinished(
                    outcome.CompactionId, std::move(outcome.Params), std::move(outcome.Result));
            backend.ApplyChanges(Table, std::move(changes));
            UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionId(), group < hiddenGroups ? 0u : forcedId);
        }
        UNIT_ASSERT(strategy.AllowForcedCompaction());
        if (concurrentWrites) {
            // The last write can still be merged with the visible final part.
            // Afterwards the remaining hidden groups must not cause repeated
            // background rewrites of the single visible part.
            UNIT_ASSERT(broker.RunPending());
            auto outcome = backend.RunCompaction();
            UNIT_ASSERT_VALUES_EQUAL(outcome.Params->Parts.size(), 2u);
            broker.FinishTask(outcome.Params->TaskId, EResourceStatus::Finished);
            auto changes = strategy.CompactionFinished(
                outcome.CompactionId, std::move(outcome.Params), std::move(outcome.Result));
            backend.ApplyChanges(Table, std::move(changes));
            UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionId(), forcedId);
            UNIT_ASSERT(strategy.AllowForcedCompaction());
        }
        UNIT_ASSERT(!broker.HasPending());
    }

    Y_UNIT_TEST(EraseAllForcedCompactionFinishesAllGroups) {
        TestEraseAllForcedCompaction(false);
    }

    Y_UNIT_TEST(EraseAllForcedCompactionDoesNotChaseNewWrites) {
        TestEraseAllForcedCompaction(true);
    }

    Y_UNIT_TEST(EraseAllBackgroundCompactionDoesNotRewriteSingleGroup) {
        TestEraseAllForcedCompaction(true, 2);
    }

    Y_UNIT_TEST(ForcedCompactionWithGenerations) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            TCompactionPolicy policy;
            policy.Generations.emplace_back(10 * 1024 * 1024, 2, 10, 100 * 1024 * 1024, "compact_gen1", true);
            policy.Generations.emplace_back(100 * 1024 * 1024, 2, 10, 200 * 1024 * 1024, "compact_gen2", true);
            policy.Generations.emplace_back(200 * 1024 * 1024, 2, 10, 200 * 1024 * 1024, "compact_gen3", true);
            backend.DB.Alter().SetCompactionPolicy(Table, policy);

            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });

        // Don't expect any tasks or change requests
        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT(!backend.StartedCompactions);
        UNIT_ASSERT(!backend.CheckChangesFlag());

        // Insert some rows
        {
            auto db = backend.Begin();
            for (ui64 key = 0; key < 64; ++key) {
                db.Table<Schema::Data>().Key(key).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
        }

        // Start a forced mem compaction with id 123
        {
            auto memCompactionId = strategy.BeginMemCompaction(0, { 0, TEpoch::Max() }, 123);
            UNIT_ASSERT(memCompactionId != 0);
            auto outcome = backend.RunCompaction(memCompactionId);

            UNIT_ASSERT(outcome.Params);
            UNIT_ASSERT(!outcome.Params->Parts);
            UNIT_ASSERT(outcome.Params->IsFinal);

            auto changes = strategy.CompactionFinished(
                    memCompactionId, std::move(outcome.Params), std::move(outcome.Result));

            // We expect forced compaction to place results on level 1
            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartsLevel, 1u);

            // We expect forced compaction to be finished and a new one immediately allowed
            UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionId(), 123u);
            UNIT_ASSERT(strategy.AllowForcedCompaction());
        }

        // Don't expect any tasks or change requests
        UNIT_ASSERT(!broker.HasPending());
        UNIT_ASSERT(!backend.StartedCompactions);
        UNIT_ASSERT(!backend.CheckChangesFlag());

        // Insert some more rows
        {
            auto db = backend.Begin();
            for (ui64 key = 64; key < 128; ++key) {
                db.Table<Schema::Data>().Key(key).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
        }

        // Start one more force compaction with id 234
        {
            auto memCompactionId = strategy.BeginMemCompaction(0, { 0, TEpoch::Max() }, 234);
            UNIT_ASSERT(memCompactionId != 0);
            auto outcome = backend.RunCompaction(memCompactionId);

            UNIT_ASSERT(outcome.Params);
            UNIT_ASSERT(!outcome.Params->Parts);
            UNIT_ASSERT(!outcome.Params->IsFinal);

            auto changes = strategy.CompactionFinished(
                    memCompactionId, std::move(outcome.Params), std::move(outcome.Result));

            // We expect forced compaction to again place results (there are none) on level 1
            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartsLevel, 1u);

            // We expect compaction 234 not to be finished yet
            UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionId(), 123u);
            UNIT_ASSERT(!strategy.AllowForcedCompaction());
        }

        // There should be a compaction task pending right now
        UNIT_ASSERT(broker.RunPending());
        UNIT_ASSERT(!broker.HasPending());

        // There should be compaction started right now
        UNIT_ASSERT_VALUES_EQUAL(backend.StartedCompactions.size(), 1u);

        // Perform this compaction
        {
            auto outcome = backend.RunCompaction();
            UNIT_ASSERT(outcome.Params->Parts);
            UNIT_ASSERT(outcome.Params->IsFinal);
            UNIT_ASSERT_VALUES_EQUAL(outcome.Result->Parts.size(), 1u);

            auto* genParams = CheckedCast<TGenCompactionParams*>(outcome.Params.Get());
            UNIT_ASSERT_VALUES_EQUAL(genParams->Generation, 1u);

            auto changes = strategy.CompactionFinished(
                    outcome.CompactionId, std::move(outcome.Params), std::move(outcome.Result));

            // We expect the result to be uplifted to level 1
            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartsLevel, 1u);

            // We expect compaction 234 to be finished
            UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionId(), 234u);
            UNIT_ASSERT(strategy.AllowForcedCompaction());
        }
    }

    Y_UNIT_TEST(ForcedCompactionWithFinalParts) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            TCompactionPolicy policy;
            backend.DB.Alter().SetCompactionPolicy(Table, policy);

            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });

        // Insert some rows
        {
            auto db = backend.Begin();
            for (ui64 key = 0; key < 64; ++key) {
                db.Table<Schema::Data>().Key(key).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
        }

        backend.SimpleMemCompaction(&strategy);

        // Alter schema to policy with generations
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            TCompactionPolicy policy;
            policy.Generations.emplace_back(10 * 1024 * 1024, 2, 10, 100 * 1024 * 1024, "compact_gen1", true);
            policy.Generations.emplace_back(100 * 1024 * 1024, 2, 10, 200 * 1024 * 1024, "compact_gen2", true);
            policy.Generations.emplace_back(200 * 1024 * 1024, 2, 10, 200 * 1024 * 1024, "compact_gen3", true);
            backend.DB.Alter().SetCompactionPolicy(Table, policy);

            backend.Commit();
            strategy.ReflectSchema();
        }

        // Insert some more rows
        {
            auto db = backend.Begin();
            for (ui64 key = 64; key < 128; ++key) {
                db.Table<Schema::Data>().Key(key).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
        }

        // Start force compaction with id 123
        {
            auto memCompactionId = strategy.BeginMemCompaction(0, { 0, TEpoch::Max() }, 123);
            UNIT_ASSERT(memCompactionId != 0);
            auto outcome = backend.RunCompaction(memCompactionId);

            UNIT_ASSERT(outcome.Params);
            UNIT_ASSERT(!outcome.Params->Parts);
            UNIT_ASSERT(!outcome.Params->IsFinal);

            auto changes = strategy.CompactionFinished(
                    memCompactionId, std::move(outcome.Params), std::move(outcome.Result));

            // We expect forced compaction to place results before final parts on level 3
            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartsLevel, 3u);

            // We expect compaction 123 not to be finished yet
            UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionId(), 0u);
            UNIT_ASSERT(!strategy.AllowForcedCompaction());
        }

        // There should be a compaction task pending right now
        UNIT_ASSERT(broker.RunPending());
        UNIT_ASSERT(!broker.HasPending());

        // There should be compaction started right now
        UNIT_ASSERT_VALUES_EQUAL(backend.StartedCompactions.size(), 1u);

        // Perform this compaction
        {
            auto outcome = backend.RunCompaction();
            UNIT_ASSERT(outcome.Params->IsFinal);
            UNIT_ASSERT_VALUES_EQUAL(outcome.Params->Parts.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(outcome.Result->Parts.size(), 1u);

            auto* genParams = CheckedCast<TGenCompactionParams*>(outcome.Params.Get());
            UNIT_ASSERT_VALUES_EQUAL(genParams->Generation, 3u);

            auto changes = strategy.CompactionFinished(
                    outcome.CompactionId, std::move(outcome.Params), std::move(outcome.Result));

            // We expect the result to be uplifted to level 1
            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartsLevel, 1u);

            // We expect compaction 234 to be finished
            UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionId(), 123u);
            UNIT_ASSERT(strategy.AllowForcedCompaction());
        }
    }

    Y_UNIT_TEST(ForcedCompactionByDeletedRows) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            TCompactionPolicy policy;
            policy.DroppedRowsPercentToCompact = 50;
            policy.Generations.emplace_back(10 * 1024 * 1024, 2, 10, 100 * 1024 * 1024, "compact_gen1", true);
            policy.Generations.emplace_back(100 * 1024 * 1024, 2, 10, 200 * 1024 * 1024, "compact_gen2", true);
            policy.Generations.emplace_back(200 * 1024 * 1024, 2, 10, 200 * 1024 * 1024, "compact_gen3", true);
            for (auto& gen : policy.Generations) {
                gen.ExtraCompactionPercent = 0;
                gen.ExtraCompactionMinSize = 0;
                gen.ExtraCompactionExpPercent = 0;
                gen.ExtraCompactionExpMaxSize = 0;
            }
            backend.DB.Alter().SetCompactionPolicy(Table, policy);

            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });

        // Insert some rows
        {
            auto db = backend.Begin();
            for (ui64 key = 0; key < 16; ++key) {
                db.Table<Schema::Data>().Key(key).Update<Schema::Data::Value>(42);
            }
            backend.Commit();
        }

        backend.SimpleMemCompaction(&strategy);

        // Erase more than 50% of rows
        {
            auto db = backend.Begin();
            for (ui64 key = 0; key < 10; ++key) {
                db.Table<Schema::Data>().Key(key).Delete();
            }
            backend.Commit();
        }

        backend.SimpleMemCompaction(&strategy);
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 2u);

        // We expect a forced compaction to be pending right now
        UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionTs(), TInstant());
        UNIT_ASSERT(!strategy.AllowForcedCompaction());
        UNIT_ASSERT(broker.HasPending());

        time.Move(TInstant::Seconds(60));

        // finish forced compactions
        while (broker.HasPending()) {
            UNIT_ASSERT(broker.RunPending());
            if (backend.StartedCompactions.empty())
                continue;

            auto result = backend.RunCompaction();
            auto changes = strategy.CompactionFinished(
                    result.CompactionId, std::move(result.Params), std::move(result.Result));
            backend.ApplyChanges(Table, std::move(changes));
        }

        UNIT_ASSERT(strategy.AllowForcedCompaction());
        UNIT_ASSERT_VALUES_EQUAL(strategy.GetLastFinishedForcedCompactionTs(), TInstant::Seconds(60));
    }

    Y_UNIT_TEST(ForcedCompactionByUnreachableMvccData) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;
        ui64 performedCompactions = 0;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            TCompactionPolicy policy;
            policy.DroppedRowsPercentToCompact = 50;
            policy.Generations.emplace_back(10 * 1024 * 1024, 2, 10, 100 * 1024 * 1024, "compact_gen1", true);
            policy.Generations.emplace_back(100 * 1024 * 1024, 2, 10, 200 * 1024 * 1024, "compact_gen2", true);
            policy.Generations.emplace_back(200 * 1024 * 1024, 2, 10, 200 * 1024 * 1024, "compact_gen3", true);
            for (auto& gen : policy.Generations) {
                gen.ExtraCompactionPercent = 0;
                gen.ExtraCompactionMinSize = 0;
                gen.ExtraCompactionExpPercent = 0;
                gen.ExtraCompactionExpMaxSize = 0;
            }
            backend.DB.Alter().SetCompactionPolicy(Table, policy);

            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });

        // Insert some rows at v1
        {
            auto db = backend.Begin();
            for (ui64 key = 0; key < 16; ++key) {
                db.Table<Schema::Data>().Key(key).UpdateV<Schema::Data::Value>(TRowVersion(1, 1), 42);
            }
            backend.Commit();
        }

        backend.SimpleMemCompaction(&strategy);

        // Delete all rows at v2
        {
            auto db = backend.Begin();
            for (ui64 key = 0; key < 16; ++key) {
                db.Table<Schema::Data>().Key(key).DeleteV(TRowVersion(2, 2));
            }
            backend.Commit();
        }

        backend.SimpleMemCompaction(&strategy);
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 2u);

        // We expect a forced compaction to be pending right now
        UNIT_ASSERT(!strategy.AllowForcedCompaction());
        UNIT_ASSERT(broker.HasPending());

        // Finish forced compactions
        while (broker.HasPending()) {
            UNIT_ASSERT(broker.RunPending());
            if (backend.StartedCompactions.empty())
                continue;

            UNIT_ASSERT_C(performedCompactions++ < 100, "too many compactions");
            auto result = backend.RunCompaction();
            auto changes = strategy.CompactionFinished(
                    result.CompactionId, std::move(result.Params), std::move(result.Result));
            backend.ApplyChanges(Table, std::move(changes));
        }

        // Everything should be compacted to a single part (erased data still visible)
        UNIT_ASSERT(strategy.AllowForcedCompaction());
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 1u);

        // Delete all versions from minimum up to almost v2
        {
            backend.Begin();
            backend.DB.RemoveRowVersions(Schema::Data::TableId, TRowVersion::Min(), TRowVersion(2, 1));
            backend.Commit();
        }

        // Notify strategy about removed row versions change
        strategy.ReflectRemovedRowVersions();

        // Nothing should be pending at this time, because all data is still visible
        UNIT_ASSERT(strategy.AllowForcedCompaction());
        UNIT_ASSERT(!broker.HasPending());

        // Delete all versions from almost v2 up to v2
        {
            backend.Begin();
            backend.DB.RemoveRowVersions(Schema::Data::TableId, TRowVersion(2, 1), TRowVersion(2, 2));
            backend.Commit();
        }

        // Notify strategy about removed row versions change
        strategy.ReflectRemovedRowVersions();

        // We expect a forced compaction to be pending right now
        UNIT_ASSERT(!strategy.AllowForcedCompaction());
        UNIT_ASSERT(broker.HasPending());

        // Finish forced compactions
        while (broker.HasPending()) {
            UNIT_ASSERT(broker.RunPending());
            if (backend.StartedCompactions.empty())
                continue;

            UNIT_ASSERT_C(performedCompactions++ < 100, "too many compactions");
            auto result = backend.RunCompaction();
            auto changes = strategy.CompactionFinished(
                    result.CompactionId, std::move(result.Params), std::move(result.Result));
            backend.ApplyChanges(Table, std::move(changes));
        }

        // Table should become completely empty
        UNIT_ASSERT(strategy.AllowForcedCompaction());
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 0u);
    }

    Y_UNIT_TEST(ForcedCompactionByUnreachableMvccDataRestart) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;
        ui64 performedCompactions = 0;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            TCompactionPolicy policy;
            policy.DroppedRowsPercentToCompact = 50;
            policy.Generations.emplace_back(10 * 1024 * 1024, 2, 10, 100 * 1024 * 1024, "compact_gen1", true);
            policy.Generations.emplace_back(100 * 1024 * 1024, 2, 10, 200 * 1024 * 1024, "compact_gen2", true);
            policy.Generations.emplace_back(200 * 1024 * 1024, 2, 10, 200 * 1024 * 1024, "compact_gen3", true);
            for (auto& gen : policy.Generations) {
                gen.ExtraCompactionPercent = 0;
                gen.ExtraCompactionMinSize = 0;
                gen.ExtraCompactionExpPercent = 0;
                gen.ExtraCompactionExpMaxSize = 0;
            }
            backend.DB.Alter().SetCompactionPolicy(Table, policy);

            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });

        // Insert some rows at v1
        {
            auto db = backend.Begin();
            for (ui64 key = 0; key < 16; ++key) {
                db.Table<Schema::Data>().Key(key).UpdateV<Schema::Data::Value>(TRowVersion(1, 1), 42);
            }
            backend.Commit();
        }

        // Delete all rows at v2
        {
            auto db = backend.Begin();
            for (ui64 key = 0; key < 16; ++key) {
                db.Table<Schema::Data>().Key(key).DeleteV(TRowVersion(2, 2));
            }
            backend.Commit();
        }

        backend.SimpleMemCompaction(&strategy);
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 1u);

        // We expect nothing to be pending right now
        UNIT_ASSERT(strategy.AllowForcedCompaction());
        UNIT_ASSERT(!broker.HasPending());

        strategy.Stop();

        // Delete all versions from minimum up to v2
        {
            backend.Begin();
            backend.DB.RemoveRowVersions(Schema::Data::TableId, TRowVersion::Min(), TRowVersion(2, 2));
            backend.Commit();
        }

        // Start a new strategy
        TGenCompactionStrategy strategy2(Table, &backend, &broker, &time, &logger, "suffix");
        strategy2.Start({ });

        // We expect a forced compaction to be pending right now
        UNIT_ASSERT(!strategy2.AllowForcedCompaction());
        UNIT_ASSERT(broker.HasPending());

        // Finish forced compactions
        while (broker.HasPending()) {
            UNIT_ASSERT(broker.RunPending());
            if (backend.StartedCompactions.empty())
                continue;

            UNIT_ASSERT_C(performedCompactions++ < 100, "too many compactions");
            auto result = backend.RunCompaction();
            auto changes = strategy2.CompactionFinished(
                    result.CompactionId, std::move(result.Params), std::move(result.Result));
            backend.ApplyChanges(Table, std::move(changes));
        }

        // Table should become completely empty
        UNIT_ASSERT(strategy2.AllowForcedCompaction());
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 0u);
    }

    Y_UNIT_TEST(ForcedCompactionByUnreachableMvccDataBorrowed) {
        TSimpleBackend backend;
        TSimpleBroker broker;
        TSimpleLogger logger;
        TSimpleTime time;
        ui64 performedCompactions = 0;

        // Initialize the schema
        {
            auto db = backend.Begin();
            db.Materialize<Schema>();

            TCompactionPolicy policy;
            policy.DroppedRowsPercentToCompact = 50;
            policy.Generations.emplace_back(10 * 1024 * 1024, 2, 10, 100 * 1024 * 1024, "compact_gen1", true);
            policy.Generations.emplace_back(100 * 1024 * 1024, 2, 10, 200 * 1024 * 1024, "compact_gen2", true);
            policy.Generations.emplace_back(200 * 1024 * 1024, 2, 10, 200 * 1024 * 1024, "compact_gen3", true);
            for (auto& gen : policy.Generations) {
                gen.ExtraCompactionPercent = 0;
                gen.ExtraCompactionMinSize = 0;
                gen.ExtraCompactionExpPercent = 0;
                gen.ExtraCompactionExpMaxSize = 0;
            }
            backend.DB.Alter().SetCompactionPolicy(Table, policy);

            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, &logger, "suffix");
        strategy.Start({ });

        // Insert some rows at v1
        {
            auto db = backend.Begin();
            for (ui64 key = 0; key < 16; ++key) {
                db.Table<Schema::Data>().Key(key).UpdateV<Schema::Data::Value>(TRowVersion(1, 1), 42);
            }
            backend.Commit();
        }

        // Delete all rows at v2
        {
            auto db = backend.Begin();
            for (ui64 key = 0; key < 16; ++key) {
                db.Table<Schema::Data>().Key(key).DeleteV(TRowVersion(2, 2));
            }
            backend.Commit();
        }

        backend.SimpleMemCompaction(&strategy);
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 1u);

        // We expect nothing to be pending right now
        UNIT_ASSERT(strategy.AllowForcedCompaction());
        UNIT_ASSERT(!broker.HasPending());

        strategy.Stop();

        // Delete all versions from minimum up to v2
        {
            backend.Begin();
            backend.DB.RemoveRowVersions(Schema::Data::TableId, TRowVersion::Min(), TRowVersion(2, 2));
            backend.Commit();
        }

        // Change tablet id so all data would be treated as borrowed
        backend.TabletId++;

        // Start a new strategy
        TGenCompactionStrategy strategy2(Table, &backend, &broker, &time, &logger, "suffix");
        strategy2.Start({ });

        // We expect nothing to be pending right now
        UNIT_ASSERT(strategy2.AllowForcedCompaction());
        UNIT_ASSERT(!broker.HasPending());

        // Insert a single row at v3
        {
            auto db = backend.Begin();
            for (ui64 key = 16; key < 17; ++key) {
                db.Table<Schema::Data>().Key(key).UpdateV<Schema::Data::Value>(TRowVersion(3, 3), 42);
            }
            backend.Commit();
        }

        backend.SimpleMemCompaction(&strategy2);
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 2u);

        // We expect a forced compaction to be pending right now
        UNIT_ASSERT(!strategy2.AllowForcedCompaction());
        UNIT_ASSERT(broker.HasPending());

        // Finish forced compactions
        while (broker.HasPending()) {
            UNIT_ASSERT(broker.RunPending());
            if (backend.StartedCompactions.empty())
                continue;

            UNIT_ASSERT_C(performedCompactions++ < 100, "too many compactions");
            auto result = backend.RunCompaction();
            auto changes = strategy2.CompactionFinished(
                    result.CompactionId, std::move(result.Params), std::move(result.Result));
            backend.ApplyChanges(Table, std::move(changes));
        }

        // Table should be left with a single sst
        UNIT_ASSERT(strategy2.AllowForcedCompaction());
        UNIT_ASSERT_VALUES_EQUAL(backend.TableParts(Table).size(), 1u);
    }

};

} // NCompGen
} // NTable
} // NKikimr
