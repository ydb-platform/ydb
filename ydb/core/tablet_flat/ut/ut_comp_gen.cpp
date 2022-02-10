#include "flat_comp_ut_common.h"

#include <ydb/core/tablet_flat/flat_comp_gen.h> 

#include <library/cpp/testing/unittest/registar.h>

constexpr ui32 Table = 1;

namespace NKikimr {
namespace NTable {
namespace NCompGen {

using namespace NTest;

Y_UNIT_TEST_SUITE(TGenCompaction) {

    struct Schema : NIceDb::Schema {
        struct Data : Table<1> {
            struct Key : Column<1, NScheme::NTypeIds::Uint64> { };
            struct Value : Column<2, NScheme::NTypeIds::Uint32> { };

            using TKey = TableKey<Key>;
            using TColumns = TableColumns<Key, Value>;
        };

        using TTables = SchemaTables<Data>;
    };

    Y_UNIT_TEST(ShouldIncreaseOverloadWhenForceCompaction) {
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
            TCompactionPolicy::TGenerationPolicy genPolicy(1, 1, 1, 10*1024*1024*1024, "whoknows", true);

            for (size_t i = 0; i < 5; ++i) {
                policy.Generations.push_back(genPolicy);
            }

            backend.DB.Alter().SetCompactionPolicy(Table, policy);

            backend.Commit();
        }

        TGenCompactionStrategy strategy(Table, &backend, &broker, &time, "suffix");
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
        UNIT_ASSERT_VALUES_EQUAL(strategy.GetOverloadFactor(), 0);

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
};

} // NCompGen
} // NTable
} // NKikimr
