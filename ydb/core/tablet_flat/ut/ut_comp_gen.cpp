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
        UNIT_ASSERT(!backend.PendingReads);
        UNIT_ASSERT(!backend.StartedCompactions);
        UNIT_ASSERT(!backend.CheckChangesFlag());
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
        UNIT_ASSERT(!backend.PendingReads);
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
        UNIT_ASSERT(!backend.PendingReads);
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
