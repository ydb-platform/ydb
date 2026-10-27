#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/chunks/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/granules/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/optimizer/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/portions/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/schemas/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/sys_view/chunks/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/sys_view/granules/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/sys_view/optimizer/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/sys_view/portions/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/sys_view/schemas/schema.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NReader {
namespace {

const auto PathId =
    NColumnShard::TUnifiedPathId::BuildValid(TInternalPathId::FromRawValue(1), NColumnShard::TSchemeShardLocalPathId::FromRawValue(11));
constexpr ui64 TabletId = 22;
constexpr ui64 PortionId = 33;
constexpr ui32 EntityId = 0xfedcba98;
constexpr ui64 ChunkId = 0x123456789abcdef0;

void CheckUint64Keys(const NArrow::TSimpleRow& row, const std::vector<std::pair<TString, ui64>>& expected) {
    UNIT_ASSERT_VALUES_EQUAL(row.GetColumnsCount(), expected.size());
    for (ui32 i = 0; i < expected.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(row.GetSchema()->field(i)->name(), expected[i].first);
        UNIT_ASSERT_VALUES_EQUAL(row.GetLogicalTypes()[i].GetTypeId(), NScheme::NTypeIds::Uint64);
        UNIT_ASSERT_VALUES_EQUAL(row.GetValueVerified<ui64>(i), expected[i].second);
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(SysViewKeys) {
    Y_UNIT_TEST(ChunksPreserveMixedWidthKeyOrder) {
        for (const auto& row : { NSimple::NSysView::NChunks::TSchemaAdapter::GetPKSimpleRow(PathId, TabletId, PortionId, EntityId, ChunkId),
                 NTrivial::NSysView::NChunks::TSchemaAdapter::GetPKTrivialRow(PathId, TabletId, PortionId, EntityId, ChunkId) }) {
            UNIT_ASSERT_VALUES_EQUAL(row.GetColumnsCount(), 5);
            UNIT_ASSERT_VALUES_EQUAL(row.GetValueVerified<ui64>("PathId"), 11);
            UNIT_ASSERT_VALUES_EQUAL(row.GetValueVerified<ui64>("TabletId"), TabletId);
            UNIT_ASSERT_VALUES_EQUAL(row.GetValueVerified<ui64>("PortionId"), PortionId);
            UNIT_ASSERT_VALUES_EQUAL(row.GetValueVerified<ui32>("InternalEntityId"), EntityId);
            UNIT_ASSERT_VALUES_EQUAL(row.GetValueVerified<ui64>("ChunkIdx"), ChunkId);
            UNIT_ASSERT_VALUES_EQUAL(row.GetLogicalTypes()[3].GetTypeId(), NScheme::NTypeIds::Uint32);
            UNIT_ASSERT_VALUES_EQUAL(row.GetLogicalTypes()[4].GetTypeId(), NScheme::NTypeIds::Uint64);
        }
    }

    Y_UNIT_TEST(GranulesUseDeclaredKey) {
        for (const auto& row : { NSimple::NSysView::NGranules::TSchemaAdapter::GetPKSimpleRow(PathId.SchemeShardLocalPathId, TabletId),
                 NTrivial::NSysView::NGranules::TSchemaAdapter::GetPKTrivialRow(PathId.SchemeShardLocalPathId, TabletId) }) {
            CheckUint64Keys(row, { { "PathId", 11 }, { "TabletId", TabletId } });
        }
    }

    Y_UNIT_TEST(PortionsUseDeclaredKey) {
        for (const auto& row : { NSimple::NSysView::NPortions::TSchemaAdapter::GetPKSimpleRow(PathId, TabletId, PortionId),
                 NTrivial::NSysView::NPortions::TSchemaAdapter::GetPKTrivialRow(PathId, TabletId, PortionId) }) {
            CheckUint64Keys(row, { { "PathId", 11 }, { "TabletId", TabletId }, { "PortionId", PortionId } });
        }
    }

    Y_UNIT_TEST(OptimizerUsesDeclaredKey) {
        for (const auto& row : { NSimple::NSysView::NOptimizer::TSchemaAdapter::GetPKSimpleRow(PathId.SchemeShardLocalPathId, TabletId, 44),
                 NTrivial::NSysView::NOptimizer::TSchemaAdapter::GetPKTrivialRow(PathId.SchemeShardLocalPathId, TabletId, 44) }) {
            CheckUint64Keys(row, { { "PathId", 11 }, { "TabletId", TabletId }, { "TaskId", 44 } });
        }
    }

    Y_UNIT_TEST(SchemasUseDeclaredKey) {
        for (const auto& row : { NSimple::NSysView::NSchemas::TSchemaAdapter::GetPKSimpleRow(TabletId, 55, 66),
                 NTrivial::NSysView::NSchemas::TSchemaAdapter::GetPKTrivialRow(TabletId, 55, 66) }) {
            CheckUint64Keys(row, { { "TabletId", TabletId }, { "PresetId", 55 }, { "SchemaVersion", 66 } });
        }
    }
}

}   // namespace NKikimr::NOlap::NReader
