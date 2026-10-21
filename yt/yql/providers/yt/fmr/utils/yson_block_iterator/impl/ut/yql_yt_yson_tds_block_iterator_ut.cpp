#include <library/cpp/testing/unittest/registar.h>

#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_tds_block_iterator.h>
#include <yt/yql/providers/yt/fmr/test_tools/yson/yql_yt_yson_helpers.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>

namespace NYql::NFmr {

namespace {

// Two rows sharing the same reduce key (_yql_key_hash=1, k=5) but differing only in the
// tiebreaker column _yql_sort - e.g. a join's build-side row (sort=0) and one of its probe-side
// rows (sort=1). Mirrors the real production row shape from yt/yql/providers/yt/fmr/job/impl/
// yql_yt_reduce_reader.cpp's dumps.
const TVector<TString> KeyColumns = {"_yql_key_hash", "k", "_yql_sort"};

std::vector<TIndexedBlock> ReadAllBlocks(TTableDataServiceBlockIterator::TPtr iter) {
    std::vector<TIndexedBlock> blocks;
    TIndexedBlock block;
    while (iter->NextBlock(block)) {
        blocks.push_back(block);
    }
    return blocks;
}

ui64 CountRows(const std::vector<TIndexedBlock>& blocks) {
    ui64 total = 0;
    for (const auto& b : blocks) {
        total += b.Rows.size();
    }
    return total;
}

} // namespace

Y_UNIT_TEST_SUITE(TableDataServiceBlockIteratorBoundaryTests) {
    Y_UNIT_TEST(TieBreakerColumnMustNotSplitReduceGroupAcrossBoundary) {
        auto tds = MakeLocalTableDataService();
        const TString tableId = "t";
        const TString partId = "p0";
        const TString group = GetTableDataServiceGroup(tableId, partId);
        const TString chunkId = GetTableDataServiceChunkId(0, TString());

        const TString chunkText =
            "{\"_yql_key_hash\"=1u;\"k\"=5u;\"_yql_sort\"=0u};\n"
            "{\"_yql_key_hash\"=1u;\"k\"=5u;\"_yql_sort\"=1u};\n";
        tds->Put(group, chunkId, GetBinaryYson(chunkText)).GetValueSync();

        // The task boundary itself is built from the sort=0 row's real key - exactly what
        // happens in production when that row is the last physical row before a task cut.
        const TString boundaryRow = GetBinaryYson(
            "{\"_yql_key_hash\"=1u;\"k\"=5u;\"_yql_sort\"=0u}",
            NYson::EYsonType::Node
        );

        std::vector<TTableRange> tableRanges = {TTableRange{.PartId = partId, .MinChunk = 0, .MaxChunk = 1}};
        std::vector<ESortOrder> sortOrders(KeyColumns.size(), ESortOrder::Ascending);

        // Without a cap (numBoundaryKeyColumns defaults to comparing every key column, including
        // the tiebreaker): reproduces the bug - the sort=1 row looks like it's past the boundary
        // and gets dropped, even though it shares the exact same reduce key.
        {
            auto iter = MakeIntrusive<TTableDataServiceBlockIterator>(
                tableId, tableRanges, tds, KeyColumns, sortOrders,
                /*neededColumns*/ TVector<TString>{}, /*serializedColumnGroupsSpec*/ TString(),
                /*isFirstRowKeysInclusive*/ Nothing(), /*isLastRowKeysInclusive*/ true,
                /*firstRowKeys*/ Nothing(), /*lastRowKeys*/ boundaryRow
            );
            UNIT_ASSERT_VALUES_EQUAL(CountRows(ReadAllBlocks(iter)), 1);
        }

        // With the fix - numBoundaryKeyColumns=2 caps comparison to [_yql_key_hash, k], excluding
        // _yql_sort: both rows of the reduce group survive the same boundary.
        {
            auto iter = MakeIntrusive<TTableDataServiceBlockIterator>(
                tableId, tableRanges, tds, KeyColumns, sortOrders,
                /*neededColumns*/ TVector<TString>{}, /*serializedColumnGroupsSpec*/ TString(),
                /*isFirstRowKeysInclusive*/ Nothing(), /*isLastRowKeysInclusive*/ true,
                /*firstRowKeys*/ Nothing(), /*lastRowKeys*/ boundaryRow,
                /*readAheadChunks*/ ui64{4}, /*numBoundaryKeyColumns*/ size_t{2}
            );
            UNIT_ASSERT_VALUES_EQUAL(CountRows(ReadAllBlocks(iter)), 2);
        }
    }
}

} // namespace NYql::NFmr
