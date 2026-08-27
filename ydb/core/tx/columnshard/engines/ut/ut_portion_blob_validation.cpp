#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/portions/common.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/snapshot_scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom/const.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NTest {

namespace {

constexpr ui32 PkColumnId = 1;
constexpr ui32 ValueColumnId = 2;
constexpr ui32 BloomIndexId = 1002;

ISnapshotSchema::TPtr MakeSchemaWithBlobBloom() {
    auto storages = TTestStoragesManager::GetInstance();
    auto cache = std::make_shared<TSchemaObjectsCache>();

    NKikimrSchemeOp::TColumnTableSchema proto;
    const std::vector<NArrow::NTest::TTestColumn> columns = {
        NArrow::NTest::TTestColumn("pk", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)),
        NArrow::NTest::TTestColumn("value", NScheme::TTypeInfo(NScheme::NTypeIds::Int32)),
    };
    *proto.MutableColumns()->Add() = columns[0].CreateColumn(PkColumnId);
    *proto.MutableColumns()->Add() = columns[1].CreateColumn(ValueColumnId);
    proto.AddKeyColumnNames("pk");
    proto.SetVersion(1);
    proto.MutableOptions()->MutableCompactionPlannerConstructor()->SetClassName("l-buckets");
    *proto.MutableOptions()->MutableCompactionPlannerConstructor()->MutableLBuckets() =
        NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLOptimizer();

    NIndexes::TRequestSettings bloomRequest;
    bloomRequest.FalsePositiveProbability = NIndexes::NDefaults::FalsePositiveProbability;
    // Store bloom in default blob storage together with columns (not inplace metadata).
    *proto.AddIndexes() = NIndexes::TIndexMetaContainer(
        std::make_shared<NIndexes::TBloomIndexMeta>(BloomIndexId, "bloom_value", IStoragesManager::DefaultStorageId, false, ValueColumnId,
            bloomRequest, NIndexes::TReadDataExtractorContainer(std::make_shared<NIndexes::TDefaultDataExtractor>()),
            NIndexes::IBitsStorageConstructor::GetDefault()))
                              .SerializeToProto();

    auto indexInfoOpt = TIndexInfo::BuildFromProto(1, proto, storages, cache);
    UNIT_ASSERT(indexInfoOpt);
    return std::make_shared<TSnapshotSchema>(cache->UpsertIndexInfo(std::move(*indexInfoOpt)), TSnapshot(1, 1));
}

std::shared_ptr<arrow::RecordBatch> MakeBatch() {
    arrow::UInt64Builder pkBuilder;
    arrow::Int32Builder valueBuilder;
    UNIT_ASSERT(pkBuilder.AppendValues({ 1, 2, 3, 4, 5 }).ok());
    UNIT_ASSERT(valueBuilder.AppendValues({ 10, 20, 30, 40, 50 }).ok());
    auto schema = arrow::schema({ arrow::field("pk", arrow::uint64()), arrow::field("value", arrow::int32()) });
    return arrow::RecordBatch::Make(schema, 5, { pkBuilder.Finish().ValueOrDie(), valueBuilder.Finish().ValueOrDie() });
}

std::shared_ptr<NChunks::TChunkPreparation> BuildColumnChunkFromArray(
    const ISnapshotSchema& schema, const ui32 columnId, const std::shared_ptr<arrow::Array>& column) {
    auto loader = schema.GetIndexInfo().GetColumnLoaderVerified(columnId);
    const auto& columnFeatures = schema.GetIndexInfo().GetColumnFeaturesVerified(columnId);
    const auto& accessorConstructor = loader->GetAccessorConstructor();
    auto accessor = std::make_shared<NArrow::NAccessor::TTrivialArray>(column);
    const auto loadContext = loader->BuildAccessorContext(accessor->GetRecordsCount());
    auto arrToWrite = accessorConstructor->Construct(accessor, loadContext);
    UNIT_ASSERT(arrToWrite.IsSuccess());
    return std::make_shared<NChunks::TChunkPreparation>(
        accessorConstructor->SerializeToString(*arrToWrite, loadContext), *arrToWrite, TChunkAddress(columnId, 0), columnFeatures);
}

}   // namespace

Y_UNIT_TEST_SUITE(TPortionBlobValidation) {
    Y_UNIT_TEST(ChunkAddressHashNoCollisionForKnownPair) {
        // Regression: `<<` binds weaker than `+`, so the old hash was `entity << (16 + chunk)`
        // and collided for addresses like (1,1) and (2,0).
        const ui64 hash11 = THash<TChunkAddress>()(TChunkAddress(1, 1));
        const ui64 hash20 = THash<TChunkAddress>()(TChunkAddress(2, 0));
        UNIT_ASSERT_VALUES_UNEQUAL(hash11, hash20);
        UNIT_ASSERT_VALUES_EQUAL(hash11, (((ui64)1) << 16) + 1);
        UNIT_ASSERT_VALUES_EQUAL(hash20, (((ui64)2) << 16) + 0);
    }

    // Smoke test for the mixed column+index blob path that failed in #47860:
    // bloom index stored in default blob storage (not inplace), then portion Build/Finalize
    // with FullValidation of all column and index blob ranges.
    Y_UNIT_TEST(BuildPortionWithBlobStoredBloomIndex) {
        const auto schema = MakeSchemaWithBlobBloom();
        const auto batch = MakeBatch();
        auto counters = std::make_shared<NColumnShard::TIndexationCounters>("test")->SplitterCounters;
        auto storages = TTestStoragesManager::GetInstance();

        THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> entityChunks;
        for (const auto& field : batch->schema()->fields()) {
            const auto columnId = schema->GetColumnIdOptional(field->name());
            UNIT_ASSERT(columnId);
            const int fieldIndex = batch->schema()->GetFieldIndex(field->name());
            UNIT_ASSERT(fieldIndex >= 0);
            const auto columnChunk = BuildColumnChunkFromArray(*schema, *columnId, batch->column(fieldIndex));
            UNIT_ASSERT(entityChunks.emplace(*columnId, std::vector<std::shared_ptr<IPortionDataChunk>>{ columnChunk }).second);
        }

        TIndexInfo::TSecondaryData secondaryData;
        secondaryData.MutableExternalData() = entityChunks;
        schema->GetIndexInfo()
            .AppendIndex(entityChunks, BloomIndexId, storages, batch->num_rows(), IStoragesManager::DefaultStorageId, secondaryData)
            .Validate();
        UNIT_ASSERT(secondaryData.GetExternalData().contains(BloomIndexId));
        UNIT_ASSERT(secondaryData.GetSecondaryInplaceData().empty());

        auto schemaDetails = std::make_shared<TDefaultSchemaDetails>(schema, std::make_shared<NArrow::NSplitter::TSerializationStats>());
        TGeneralSerializedSlice slice(secondaryData.GetExternalData(), schemaDetails, counters);
        const NSplitter::TEntityGroups groups(
            NYDBTest::TControllers::GetColumnShardController()->GetBlobSplitSettings(), NBlobOperations::TGlobal::DefaultStorageId);
        std::vector<TSplittedBlob> blobs;
        UNIT_ASSERT(slice.GroupBlobs(blobs, groups));
        UNIT_ASSERT(!blobs.empty());

        auto constructor = TWritePortionInfoWithBlobsConstructor::BuildByBlobs(std::move(blobs), secondaryData.GetSecondaryInplaceData(),
            TInternalPathId::FromRawValue(1), schema->GetVersion(), schema->GetSnapshot(), storages, EPortionType::Written,
            schema->GetIndexInfo());

        NArrow::TFirstLastSpecialKeys primaryKeys(slice.GetFirstLastPKBatch(schema->GetIndexInfo().GetReplaceKey()));
        auto& portionCtor = constructor.GetPortionConstructor().MutablePortionConstructor();
        static_cast<TWrittenPortionInfoConstructor&>(portionCtor).SetInsertWriteId(TInsertWriteId(1));
        portionCtor.SetPortionId(1);
        portionCtor.AddMetadata(*schema, 0, primaryKeys, std::nullopt);
        portionCtor.MutableMeta().SetTierName(IStoragesManager::DefaultStorageId);
        portionCtor.MutableMeta().SetCompactionLevel(0);

        auto portion = TWritePortionInfoWithBlobsResult(std::move(constructor));
        portion.RegisterFakeBlobIds();
        portion.FinalizePortionConstructor(TSnapshot(1, 1));

        const auto& accessor = portion.GetPortionResult();
        UNIT_ASSERT(accessor.GetBlobIds().size() >= 1);
        UNIT_ASSERT(accessor.GetRecordsVerified().size() >= 1);

        bool hasBlobIndex = false;
        for (auto&& index : accessor.GetIndexesVerified()) {
            if (index.GetIndexId() == BloomIndexId && index.HasBlobRange()) {
                hasBlobIndex = true;
                TBlobRange::Validate(accessor.GetBlobIds(), index.GetBlobRangeVerified()).Validate();
            }
        }
        UNIT_ASSERT(hasBlobIndex);

        for (auto&& record : accessor.GetRecordsVerified()) {
            TBlobRange::Validate(accessor.GetBlobIds(), record.GetBlobRange()).Validate();
        }
    }
}

}   // namespace NKikimr::NOlap::NTest
