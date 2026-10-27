#include <ydb/core/tablet_flat/flat_mem_warm.h>
#include <ydb/core/tablet_flat/flat_table.h>
#include <ydb/core/tablet_flat/flat_util_misc.h>
#include <ydb/core/tablet_flat/util_pool.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NKikimr::NTable {
namespace {

    constexpr size_t MiB = 1024 * 1024;

    TIntrusiveConstPtr<TRowScheme> MakeStringScheme() {
        TVector<TColumn> columns;
        columns.emplace_back("key", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "");
        columns.emplace_back("value", 1, NScheme::TTypeInfo(NScheme::NTypeIds::String), "");
        columns.front().KeyOrder = 0;
        return TRowScheme::Make(columns, NUtil::TAsIs());
    }

    void WriteString(TMemTable& table, ui64 key, const TString& value) {
        const TRawTypeValue keys[] = { { &key, sizeof(key), NScheme::NTypeIds::Uint64 } };
        const TUpdateOp ops[] = { { 1, ECellOp::Set, { value.data(), value.size(), NScheme::NTypeIds::String } } };
        table.Update(ERowOp::Upsert, keys, ops, {}, TRowVersion::Min(), nullptr);
    }

    void WriteString(TTable& table, ui64 key, const TString& value) {
        const TRawTypeValue keys[] = { { &key, sizeof(key), NScheme::NTypeIds::Uint64 } };
        const TUpdateOp ops[] = { { 1, ECellOp::Set, { value.data(), value.size(), NScheme::NTypeIds::String } } };
        table.Update(ERowOp::Upsert, keys, ops, {}, TRowVersion::Min());
    }

    void PrintMemTableMemory(const char* scenario, size_t stringSize, size_t rows, const TMemTable& table) {
        const size_t used = table.GetUsedMem();
        const size_t wasted = table.GetWastedMem();
        Cerr << "memtable," << scenario << ",string_bytes=" << stringSize << ",rows=" << rows
             << ",payload_bytes=" << stringSize * rows << ",reported_used_bytes=" << used
             << ",pool_wasted_bytes=" << wasted << ",retained_bytes=" << used + wasted
             << ",wasted_percent=" << 100.0 * wasted / (used + wasted) << Endl;
    }

} // namespace

Y_UNIT_TEST_SUITE(TMemTableAllocation) {
    Y_UNIT_TEST(SmallStringsReuseAnOlderChunkTail) {
        NUtil::TMemoryPool pool(4032);
        const TString first(3 * MiB, 'a');
        const TString second(2 * MiB, 'b');
        const TString small(MiB / 2, 'c');
        auto* firstData = static_cast<char*>(pool.Append(first.data(), first.size()));
        pool.Append(second.data(), second.size());

        const size_t unavailable = pool.Wasted() - pool.Available();
        const size_t capacity = pool.Total();

        UNIT_ASSERT(pool.Append(small.data(), small.size()) == firstData + first.size());
        const TString next(15 * MiB / 4, 'd');
        auto* nextData = static_cast<char*>(pool.Append(next.data(), next.size()));

        UNIT_ASSERT_VALUES_EQUAL(pool.Total(), capacity);
        UNIT_ASSERT_VALUES_EQUAL(pool.Wasted() - pool.Available(), unavailable);
        UNIT_ASSERT_VALUES_EQUAL(pool.Used(), first.size() + second.size() + small.size() + next.size());
        UNIT_ASSERT_VALUES_EQUAL(pool.Used() + pool.Wasted(), pool.Total());
        UNIT_ASSERT(memcmp(firstData, first.data(), first.size()) == 0);
        UNIT_ASSERT(memcmp(firstData + first.size(), small.data(), small.size()) == 0);
        UNIT_ASSERT(memcmp(nextData, next.data(), next.size()) == 0);
        Cerr << "pool,unused_tail,used_bytes=" << pool.Used() << ",wasted_bytes=" << pool.Wasted()
             << ",available_bytes=" << pool.Available() << ",unavailable_bytes=" << unavailable << Endl;
    }

    Y_UNIT_TEST(LargeStringChunkGrowth) {
        NUtil::TMemoryPool pool(4032);
        const TString value(2 * MiB, 'x');
        size_t bytesIn8MiBBin = 0;
        size_t bytesIn64MiBBin = 0;

        for (size_t row = 0; row < 96; ++row) {
            const size_t before = pool.Total();
            pool.Append(value.data(), value.size());
            UNIT_ASSERT_VALUES_EQUAL(pool.Used(), (row + 1) * value.size());
            UNIT_ASSERT_VALUES_EQUAL(pool.Used() + pool.Wasted(), pool.Total());
            if (pool.Total() != before) {
                const size_t chunk = pool.Total() - before;
                const size_t bin = FastClp2(chunk);
                if (bin == 8 * MiB) {
                    bytesIn8MiBBin += chunk;
                }
                if (bin == 64 * MiB) {
                    bytesIn64MiBBin += chunk;
                }
                Cerr << "pool,growth,row=" << row + 1 << ",chunk_bytes=" << chunk << ",histogram_bin=" << bin
                     << ",used_bytes=" << pool.Used() << ",wasted_bytes=" << pool.Wasted()
                     << ",total_bytes=" << pool.Total() << Endl;
            }
        }

        UNIT_ASSERT_C(bytesIn8MiBBin > 0, bytesIn8MiBBin);
        UNIT_ASSERT_C(bytesIn64MiBBin > 0, bytesIn64MiBBin);
    }

    Y_UNIT_TEST(BulkStringMemoryUsage) {
        // Direct updates exercise the Clone/AddChunk path, without redo, requests, or external blobs
        for (size_t stringSize : { 64 * 1024ul, MiB / 4, MiB, 2 * MiB, 3 * MiB, 8 * MiB }) {
            TMemTable table(MakeStringScheme(), TEpoch::Zero(), 0);
            const TString value(stringSize, 'x');
            const size_t rows = (64 * MiB + stringSize - 1) / stringSize;
            for (size_t row = 0; row < rows; ++row) {
                WriteString(table, row, value);
            }
            UNIT_ASSERT_VALUES_EQUAL(table.GetRowCount(), rows);
            UNIT_ASSERT_C(table.GetUsedMem() >= rows * stringSize, table.GetUsedMem());
            PrintMemTableMemory("bulk", stringSize, rows, table);
        }

        TMemTable table(MakeStringScheme(), TEpoch::Zero(), 0);
        const TString value(2 * MiB, 'x');
        for (size_t row = 0; row < 96; ++row) {
            WriteString(table, row, value);
        }
        UNIT_ASSERT_VALUES_EQUAL(table.GetRowCount(), 96);
        UNIT_ASSERT_C(table.GetUsedMem() >= 192 * MiB, table.GetUsedMem());
        PrintMemTableMemory("large_bulk", value.size(), 96, table);
    }

    Y_UNIT_TEST(RollbackRetainsStringCapacity) {
        TMemTable table(MakeStringScheme(), TEpoch::Zero(), 0);
        const size_t initialUsed = table.GetUsedMem();
        const TString value(2 * MiB, 'x');
        table.PrepareRollback();
        for (size_t row = 0; row < 32; ++row) {
            WriteString(table, row, value);
        }
        PrintMemTableMemory("before_rollback", value.size(), 32, table);

        table.RollbackChanges();

        UNIT_ASSERT_VALUES_EQUAL(table.GetRowCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(table.GetUsedMem(), initialUsed);
        UNIT_ASSERT_C(table.GetWastedMem() >= 64 * MiB, table.GetWastedMem());
        PrintMemTableMemory("after_rollback", value.size(), 0, table);
    }

    Y_UNIT_TEST(TableAllocatedCapacitySurvivesRollbackAndSnapshot) {
        TScheme::TTableInfo schema("test", 1);
        schema.Columns.emplace(0, TColumn("key", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), ""));
        schema.Columns.emplace(1, TColumn("value", 1, NScheme::TTypeInfo(NScheme::NTypeIds::String), ""));
        schema.Columns.at(0).KeyOrder = 0;
        schema.KeyColumns.push_back(0);
        TTable table(TEpoch::Zero(), {});
        table.SetScheme(schema);
        WriteString(table, 0, "seed");
        const ui64 initialUsed = table.GetMemSize();

        table.PrepareRollback();
        const TString value(2 * MiB, 'x');
        for (ui64 row = 1; row <= 32; ++row) {
            WriteString(table, row, value);
        }
        table.RollbackChanges();

        UNIT_ASSERT_VALUES_EQUAL(table.GetMemSize(), initialUsed);
        UNIT_ASSERT_VALUES_EQUAL(table.GetMemRowCount(), 1);
        UNIT_ASSERT_C(table.GetMemAllocatedSize() > 64 * MiB, table.GetMemAllocatedSize());
        const ui64 allocated = table.GetMemAllocatedSize();
        table.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(table.GetMemAllocatedSize(), allocated);
        WriteString(table, 1, "another seed");
        UNIT_ASSERT(table.GetMemAllocatedSize() > allocated);
        UNIT_ASSERT_VALUES_EQUAL(table.GetMemAllocatedSize(), table.GetMemSize() + table.GetMemWaste());

        const ui64 withMutable = table.GetMemAllocatedSize();
        table.PrepareRollback();
        table.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(table.GetMemAllocatedSize(), withMutable);
        table.RollbackChanges();
        UNIT_ASSERT_VALUES_EQUAL(table.GetMemAllocatedSize(), withMutable);
    }
}

} // namespace NKikimr::NTable
