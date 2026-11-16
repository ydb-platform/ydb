#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/schema_diff.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NTest {

namespace {

NKikimrSchemeOp::TColumnTableSchema MakeSchemaV1() {
    NKikimrSchemeOp::TColumnTableSchema proto;
    const std::vector<NArrow::NTest::TTestColumn> columns = {
        NArrow::NTest::TTestColumn("timestamp", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)).SetNullable(false),
        NArrow::NTest::TTestColumn("value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)),
        NArrow::NTest::TTestColumn("to_drop", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)),
    };
    *proto.MutableColumns()->Add() = columns[0].CreateColumn(1);
    *proto.MutableColumns()->Add() = columns[1].CreateColumn(2);
    *proto.MutableColumns()->Add() = columns[2].CreateColumn(3);
    proto.AddKeyColumnNames("timestamp");
    proto.SetVersion(1);
    return proto;
}

NKikimrSchemeOp::TColumnTableSchema MakeSchemaV2AfterDropAndAdd() {
    NKikimrSchemeOp::TColumnTableSchema proto;
    const std::vector<NArrow::NTest::TTestColumn> columns = {
        NArrow::NTest::TTestColumn("timestamp", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)).SetNullable(false),
        NArrow::NTest::TTestColumn("value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)),
        NArrow::NTest::TTestColumn("added", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)),
    };
    *proto.MutableColumns()->Add() = columns[0].CreateColumn(1);
    *proto.MutableColumns()->Add() = columns[1].CreateColumn(2);
    *proto.MutableColumns()->Add() = columns[2].CreateColumn(4);
    proto.AddKeyColumnNames("timestamp");
    proto.SetVersion(2);
    return proto;
}

}   // namespace

Y_UNIT_TEST_SUITE(TIndexInfoSchemaDiff) {
    Y_UNIT_TEST(GetColumnsConsistentAfterDropAndAddColumns) {
        auto storages = TTestStoragesManager::GetInstance();
        auto cache = std::make_shared<TSchemaObjectsCache>();

        const auto schemaV1 = MakeSchemaV1();
        auto indexInfoV1 = TIndexInfo::BuildFromProto(1, schemaV1, storages, cache);
        UNIT_ASSERT(indexInfoV1);

        const auto schemaV2 = MakeSchemaV2AfterDropAndAdd();
        const auto diffProto = TSchemaDiffView::MakeSchemasDiff(schemaV1, schemaV2);
        UNIT_ASSERT(diffProto.DropColumnsSize() > 0);
        UNIT_ASSERT(diffProto.UpsertColumnsSize() > 0);

        auto indexInfoV2 = TIndexInfo::BuildFromProto(diffProto, *indexInfoV1, storages, cache);
        UNIT_ASSERT(indexInfoV2);

        const auto& columnsMap = indexInfoV2->GetColumns();
        UNIT_ASSERT(!columnsMap.contains(3));
        UNIT_ASSERT(columnsMap.contains(4));
        UNIT_ASSERT_VALUES_EQUAL(columnsMap.at(4).first, "added");

        for (const ui32 columnId : indexInfoV2->GetColumnIds(false)) {
            auto it = columnsMap.find(columnId);
            UNIT_ASSERT_C(it != columnsMap.end(), TStringBuilder() << "missing column_id=" << columnId);
            UNIT_ASSERT(!it->second.first.empty());
        }
    }

    Y_UNIT_TEST(ChangeTieringActualizationOption) {
        const auto storages = TTestStoragesManager::GetInstance();
        const auto cache = std::make_shared<TSchemaObjectsCache>();

        const auto schemaV1 = MakeSchemaV1();
        const auto indexInfoV1 = TIndexInfo::BuildFromProto(1, schemaV1, storages, cache);
        UNIT_ASSERT(indexInfoV1);
        UNIT_ASSERT(!indexInfoV1->GetTieringNeedActualization());

        auto schemaV2 = schemaV1;
        schemaV2.SetVersion(2);
        schemaV2.MutableOptions()->SetTieringNeedActualization(true);
        const auto fullV2 = TIndexInfo::BuildFromProto(1, schemaV2, storages, cache);
        UNIT_ASSERT(fullV2);
        UNIT_ASSERT(fullV2->GetTieringNeedActualization());

        const auto diffV2 = TSchemaDiffView::MakeSchemasDiff(schemaV1, schemaV2);
        const auto indexInfoV2 = TIndexInfo::BuildFromProto(diffV2, *indexInfoV1, storages, cache);
        UNIT_ASSERT(indexInfoV2);
        UNIT_ASSERT(indexInfoV2->GetTieringNeedActualization());

        auto schemaV3 = schemaV2;
        schemaV3.SetVersion(3);
        schemaV3.MutableOptions()->SetTieringNeedActualization(false);
        const auto fullV3 = TIndexInfo::BuildFromProto(1, schemaV3, storages, cache);
        UNIT_ASSERT(fullV3);
        UNIT_ASSERT(!fullV3->GetTieringNeedActualization());

        const auto diffV3 = TSchemaDiffView::MakeSchemasDiff(schemaV2, schemaV3);
        const auto indexInfoV3 = TIndexInfo::BuildFromProto(diffV3, *indexInfoV2, storages, cache);
        UNIT_ASSERT(indexInfoV3);
        UNIT_ASSERT(!indexInfoV3->GetTieringNeedActualization());

        std::vector<NKikimrTxColumnShard::TSchemaPresetVersionInfo> rows;
        *rows.emplace_back().MutableDiff() = diffV2;
        *rows.emplace_back().MutableDiff() = diffV3;
        const auto merged = TSchemaDiffView::Merge(rows);
        UNIT_ASSERT(merged.HasDiff());

        const auto fromMerged = TIndexInfo::BuildFromProto(merged.GetDiff(), *indexInfoV1, storages, cache);
        UNIT_ASSERT(fromMerged);
        UNIT_ASSERT(!fromMerged->GetTieringNeedActualization());
    }
}

}   // namespace NKikimr::NOlap::NTest
