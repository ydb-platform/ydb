#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/schema_diff.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/snapshot_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
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
    Y_UNIT_TEST(SetNotNullPrimaryKeySurvivesSchemaReload) {
        auto storages = TTestStoragesManager::GetInstance();
        auto cache = std::make_shared<TSchemaObjectsCache>();
        auto nullableProto = MakeSchemaV1();
        nullableProto.MutableColumns(0)->SetNotNull(false);
        auto nullable = TIndexInfo::BuildFromProto(1, nullableProto, storages, cache);
        UNIT_ASSERT(nullable);

        auto validatingProto = nullableProto;
        validatingProto.SetVersion(2);
        validatingProto.MutableColumns(0)->SetSetNotNullInProgress(true);
        auto validating = TIndexInfo::BuildFromProto(
            TSchemaDiffView::MakeSchemasDiff(nullableProto, validatingProto), *nullable, storages, cache);
        UNIT_ASSERT(validating);
        UNIT_ASSERT(validating->GetPrimaryKey()->field(0)->nullable());

        auto appliedProto = validatingProto;
        appliedProto.SetVersion(3);
        appliedProto.MutableColumns(0)->ClearSetNotNullInProgress();
        appliedProto.MutableColumns(0)->SetNotNull(true);
        auto applied = TIndexInfo::BuildFromProto(
            TSchemaDiffView::MakeSchemasDiff(validatingProto, appliedProto), *validating, storages, cache);
        auto reloaded = TIndexInfo::BuildFromProto(1, appliedProto, storages, std::make_shared<TSchemaObjectsCache>());
        UNIT_ASSERT(applied);
        UNIT_ASSERT(reloaded);
        UNIT_ASSERT(!applied->GetPrimaryKey()->field(0)->nullable());
        UNIT_ASSERT(applied->GetPrimaryKey()->Equals(reloaded->GetPrimaryKey()));
        UNIT_ASSERT(nullable->CheckCompatible(*applied).IsSuccess());
        UNIT_ASSERT(nullable->CheckCompatible(*reloaded).IsSuccess());
        UNIT_ASSERT(applied->CheckCompatible(*nullable).IsFail());
        UNIT_ASSERT(applied->CheckWriteCompatibility(*nullable).IsFail());

        TVersionedIndex versions;
        versions.AddIndex(TSnapshot(1, 1), cache->UpsertIndexInfo(std::move(*nullable)));
        versions.AddIndex(TSnapshot(2, 1), cache->UpsertIndexInfo(std::move(*validating)));
        versions.AddIndex(TSnapshot(3, 1), cache->UpsertIndexInfo(std::move(*applied)));
        UNIT_ASSERT(!versions.GetPrimaryKey()->field(0)->nullable());

        auto reloadCache = std::make_shared<TSchemaObjectsCache>();
        TVersionedIndex restored;
        for (const auto& proto : {appliedProto, nullableProto, validatingProto}) {
            auto index = TIndexInfo::BuildFromProto(1, proto, storages, reloadCache);
            UNIT_ASSERT(index);
            restored.AddIndex(TSnapshot(proto.GetVersion(), 1), reloadCache->UpsertIndexInfo(std::move(*index)));
            UNIT_ASSERT(!restored.GetPrimaryKey()->field(0)->nullable());
        }
    }

    Y_UNIT_TEST(SetNotNullWriteBarrierSurvivesSchemaDiff) {
        auto storages = TTestStoragesManager::GetInstance();
        auto cache = std::make_shared<TSchemaObjectsCache>();
        const auto nullableProto = MakeSchemaV1();
        auto nullable = TIndexInfo::BuildFromProto(1, nullableProto, storages, cache);
        UNIT_ASSERT(nullable);

        auto validatingProto = nullableProto;
        validatingProto.SetVersion(2);
        validatingProto.MutableColumns(1)->SetSetNotNullInProgress(true);
        auto validating = TIndexInfo::BuildFromProto(
            TSchemaDiffView::MakeSchemasDiff(nullableProto, validatingProto), *nullable, storages, cache);
        UNIT_ASSERT(validating);
        UNIT_ASSERT(validating->IsNullableVerified(2));
        UNIT_ASSERT(validating->GetColumnFeaturesVerified(2).GetIsSetNotNullInProgress());
        UNIT_ASSERT_VALUES_EQUAL(validating->GetSetNotNullColumnIds(), std::vector<ui32>{2});
        UNIT_ASSERT(validating->CheckWriteCompatibility(*nullable).IsFail());
        UNIT_ASSERT(validating->CheckWriteCompatibility(*validating).IsSuccess());

        auto appliedProto = validatingProto;
        appliedProto.SetVersion(3);
        appliedProto.MutableColumns(1)->ClearSetNotNullInProgress();
        appliedProto.MutableColumns(1)->SetNotNull(true);
        auto applied = TIndexInfo::BuildFromProto(
            TSchemaDiffView::MakeSchemasDiff(validatingProto, appliedProto), *validating, storages, cache);
        UNIT_ASSERT(applied);
        UNIT_ASSERT(!applied->IsNullableVerified(2));
        UNIT_ASSERT(!applied->GetColumnFeaturesVerified(2).GetIsSetNotNullInProgress());
        UNIT_ASSERT(applied->GetSetNotNullColumnIds().empty());
        UNIT_ASSERT(applied->CheckWriteCompatibility(*nullable).IsFail());
        UNIT_ASSERT(applied->CheckWriteCompatibility(*validating).IsSuccess());

        // Revalidate old NOT NULL schemas after a DROP/SET cycle.
        auto validatingAgainProto = validatingProto;
        validatingAgainProto.SetVersion(5);
        auto validatingAgain = TIndexInfo::BuildFromProto(1, validatingAgainProto, storages, cache);
        UNIT_ASSERT(validatingAgain);
        UNIT_ASSERT(validatingAgain->CheckWriteCompatibility(*applied).IsFail());

        auto revertedProto = nullableProto;
        revertedProto.SetVersion(3);
        auto reverted = TIndexInfo::BuildFromProto(
            TSchemaDiffView::MakeSchemasDiff(validatingProto, revertedProto), *validating, storages, cache);
        UNIT_ASSERT(reverted);
        UNIT_ASSERT(reverted->CheckWriteCompatibility(*nullable).IsSuccess());
        UNIT_ASSERT(!reverted->GetColumnFeaturesVerified(2).GetIsSetNotNullInProgress());
        UNIT_ASSERT(reverted->GetSetNotNullColumnIds().empty());

        auto reloaded = TIndexInfo::BuildFromProto(1, validatingProto, storages, std::make_shared<TSchemaObjectsCache>());
        UNIT_ASSERT(reloaded);
        UNIT_ASSERT(reloaded->IsNullableVerified(2));
        UNIT_ASSERT(reloaded->GetColumnFeaturesVerified(2).GetIsSetNotNullInProgress());
        UNIT_ASSERT(reloaded->CheckWriteCompatibility(*nullable).IsFail());
    }

    Y_UNIT_TEST(SetNotNullChecksIncomingValuesAndPresenceFlags) {
        auto proto = MakeSchemaV1();
        proto.MutableColumns(1)->SetSetNotNullInProgress(true);
        auto cache = std::make_shared<TSchemaObjectsCache>();
        auto index = TIndexInfo::BuildFromProto(1, proto, TTestStoragesManager::GetInstance(), cache);
        UNIT_ASSERT(index);
        TSnapshotSchema schema(cache->UpsertIndexInfo(std::move(*index)), TSnapshot(1, 1));

        arrow::TimestampBuilder keys(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
        arrow::StringBuilder values;
        UNIT_ASSERT(keys.Append(1).ok());
        UNIT_ASSERT(values.Append("present").ok());
        auto batch = arrow::RecordBatch::Make(
            arrow::schema({arrow::field("timestamp", keys.type()), arrow::field("value", arrow::utf8())}), 1,
            {keys.Finish().ValueOrDie(), values.Finish().ValueOrDie()});
        UNIT_ASSERT(schema.PrepareForModification(batch, NEvWrite::EModificationType::Upsert).IsSuccess());

        auto missing = batch->RemoveColumn(1).ValueOrDie();
        UNIT_ASSERT(schema.PrepareForModification(missing, NEvWrite::EModificationType::Upsert).IsFail());
        UNIT_ASSERT(schema.PrepareForModification(missing, NEvWrite::EModificationType::Delete).IsSuccess());
        UNIT_ASSERT(values.AppendNull().ok());
        auto nullBatch = batch->SetColumn(1, batch->schema()->field(1), values.Finish().ValueOrDie()).ValueOrDie();
        UNIT_ASSERT(schema.PrepareForModification(nullBatch, NEvWrite::EModificationType::Upsert).IsFail());
        UNIT_ASSERT(schema.PrepareForModification(nullBatch, NEvWrite::EModificationType::Update).IsFail());

        arrow::BooleanBuilder exists;
        UNIT_ASSERT(exists.Append(false).ok());
        auto conditional = batch->AddColumn(2, arrow::field("$$EXISTS::value", arrow::boolean()),
            exists.Finish().ValueOrDie()).ValueOrDie();
        UNIT_ASSERT(schema.PrepareForModification(conditional, NEvWrite::EModificationType::Upsert).IsFail());

        UNIT_ASSERT(exists.Append(true).ok());
        auto unconditional = conditional->SetColumn(2, conditional->schema()->field(2), exists.Finish().ValueOrDie()).ValueOrDie();
        UNIT_ASSERT(schema.PrepareForModification(unconditional, NEvWrite::EModificationType::Upsert).IsSuccess());
        UNIT_ASSERT(exists.AppendNull().ok());
        auto unknown = conditional->SetColumn(2, conditional->schema()->field(2), exists.Finish().ValueOrDie()).ValueOrDie();
        UNIT_ASSERT(schema.PrepareForModification(unknown, NEvWrite::EModificationType::Upsert).IsFail());
        auto malformed = conditional->SetColumn(2, arrow::field("$$EXISTS::value", arrow::utf8()), batch->column(1)).ValueOrDie();
        UNIT_ASSERT(schema.PrepareForModification(malformed, NEvWrite::EModificationType::Upsert).IsFail());

        UNIT_ASSERT(values.AppendNull().ok());
        UNIT_ASSERT(values.Append("present").ok());
        auto slice = values.Finish().ValueOrDie()->Slice(1, 1);
        UNIT_ASSERT(slice->null_bitmap_data());
        UNIT_ASSERT_VALUES_EQUAL(slice->null_count(), 0);
        auto sliced = batch->SetColumn(1, batch->schema()->field(1), slice).ValueOrDie();
        UNIT_ASSERT(schema.PrepareForModification(sliced, NEvWrite::EModificationType::Upsert).IsSuccess());

        proto.SetVersion(2);
        proto.MutableColumns(1)->ClearSetNotNullInProgress();
        proto.MutableColumns(1)->SetNotNull(true);
        auto applied = TIndexInfo::BuildFromProto(1, proto, TTestStoragesManager::GetInstance(), cache);
        UNIT_ASSERT(applied);
        TSnapshotSchema appliedSchema(cache->UpsertIndexInfo(std::move(*applied)), TSnapshot(2, 1));
        UNIT_ASSERT(appliedSchema.PrepareForModification(sliced, NEvWrite::EModificationType::Upsert).IsSuccess());
        UNIT_ASSERT(appliedSchema.PrepareForModification(nullBatch, NEvWrite::EModificationType::Upsert).IsFail());
    }

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
}

}   // namespace NKikimr::NOlap::NTest
