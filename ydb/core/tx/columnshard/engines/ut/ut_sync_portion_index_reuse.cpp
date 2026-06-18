#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/portions/read_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/snapshot_scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom/const.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <library/cpp/testing/unittest/registar.h>

#include <array>

namespace NKikimr::NOlap::NTest {

namespace {

constexpr ui32 PkColumnId = 1;
constexpr ui32 ValueColumnId = 2;
constexpr ui32 MaxIndexId = 1001;
constexpr ui32 BloomIndexId = 1002;
constexpr const char* MaxIndexMarker = "REUSE_MAX_INDEX_MARKER_v1";
constexpr const char* BloomIndexMarker = "REUSE_BLOOM_INDEX_MARKER_v1";
constexpr std::array<i32, 3> TestValueColumnData = { 10, 20, 30 };
constexpr i32 ExpectedMaxColumnValue = TestValueColumnData.back();

struct TTestSchemaOptions {
    bool WithMaxIndex = false;
    bool WithBloomIndex = false;
};

void FillDefaultSchemaProto(NKikimrSchemeOp::TColumnTableSchema& proto, const ui64 presetId) {
    const std::vector<NArrow::NTest::TTestColumn> columns = {
        NArrow::NTest::TTestColumn("pk", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)),
        NArrow::NTest::TTestColumn("value", NScheme::TTypeInfo(NScheme::NTypeIds::Int32)),
    };
    *proto.MutableColumns()->Add() = columns[0].CreateColumn(PkColumnId);
    *proto.MutableColumns()->Add() = columns[1].CreateColumn(ValueColumnId);
    proto.AddKeyColumnNames("pk");
    proto.SetVersion(presetId);
    proto.MutableOptions()->MutableCompactionPlannerConstructor()->SetClassName("l-buckets");
    *proto.MutableOptions()->MutableCompactionPlannerConstructor()->MutableLBuckets() =
        NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLOptimizer();
}

ISnapshotSchema::TPtr MakeTestSchema(const ui64 presetId, const TTestSchemaOptions& options) {
    auto storages = TTestStoragesManager::GetInstance();
    auto cache = std::make_shared<TSchemaObjectsCache>();

    NKikimrSchemeOp::TColumnTableSchema proto;
    FillDefaultSchemaProto(proto, presetId);

    if (options.WithMaxIndex) {
        *proto.AddIndexes() = NIndexes::TIndexMetaContainer(std::make_shared<NIndexes::NMax::TIndexMeta>(MaxIndexId, "max_value",
                                                                IStoragesManager::LocalMetadataStorageId, false, ValueColumnId))
                                  .SerializeToProto();
    }
    if (options.WithBloomIndex) {
        NIndexes::TRequestSettings bloomRequest;
        bloomRequest.FalsePositiveProbability = NIndexes::NDefaults::FalsePositiveProbability;
        *proto.AddIndexes() = NIndexes::TIndexMetaContainer(
            std::make_shared<NIndexes::TBloomIndexMeta>(BloomIndexId, "bloom_value", IStoragesManager::LocalMetadataStorageId, false,
                ValueColumnId, bloomRequest, NIndexes::TReadDataExtractorContainer(std::make_shared<NIndexes::TDefaultDataExtractor>()),
                NIndexes::IBitsStorageConstructor::GetDefault()))
                                  .SerializeToProto();
    }

    auto indexInfoOpt = TIndexInfo::BuildFromProto(presetId, proto, storages, cache);
    UNIT_ASSERT(indexInfoOpt);
    return std::make_shared<TSnapshotSchema>(cache->UpsertIndexInfo(std::move(*indexInfoOpt)), TSnapshot(1, 1));
}

std::shared_ptr<arrow::RecordBatch> MakeTestBatch() {
    arrow::UInt64Builder pkBuilder;
    arrow::Int32Builder valueBuilder;
    UNIT_ASSERT(pkBuilder.AppendValues({ 1, 2, 3 }).ok());
    UNIT_ASSERT(valueBuilder.AppendValues({ TestValueColumnData[0], TestValueColumnData[1], TestValueColumnData[2] }).ok());
    auto schema = arrow::schema({ arrow::field("pk", arrow::uint64()), arrow::field("value", arrow::int32()) });
    return arrow::RecordBatch::Make(schema, 3, { pkBuilder.Finish().ValueOrDie(), valueBuilder.Finish().ValueOrDie() });
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

THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> BuildColumnChunks(
    const ISnapshotSchema::TPtr& schema, const std::shared_ptr<arrow::RecordBatch>& batch) {
    THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> chunks;
    for (const auto& field : batch->schema()->fields()) {
        const auto columnId = schema->GetColumnIdOptional(field->name());
        if (!columnId || schema->IsSpecialColumnId(*columnId)) {
            continue;
        }
        const int fieldIndex = batch->schema()->GetFieldIndex(field->name());
        UNIT_ASSERT(fieldIndex >= 0);
        const auto columnChunk = BuildColumnChunkFromArray(*schema, *columnId, batch->column(fieldIndex));
        UNIT_ASSERT(chunks.emplace(*columnId, std::vector<std::shared_ptr<IPortionDataChunk>>{ columnChunk }).second);
    }
    return chunks;
}

TWritePortionInfoWithBlobsResult BuildTestPortion(const ISnapshotSchema::TPtr& schema, const std::shared_ptr<arrow::RecordBatch>& batch,
    const THashMap<ui32, TString>& indexMarkers, const std::shared_ptr<NColumnShard::TSplitterCounters>& splitterCounters,
    const TString& tier = IStoragesManager::DefaultStorageId) {
    auto storages = TTestStoragesManager::GetInstance();
    const auto entityChunks = BuildColumnChunks(schema, batch);

    std::shared_ptr<TDefaultSchemaDetails> schemaDetails(
        new TDefaultSchemaDetails(schema, std::make_shared<NArrow::NSplitter::TSerializationStats>()));
    TGeneralSerializedSlice slice(entityChunks, schemaDetails, splitterCounters);

    const NSplitter::TEntityGroups groups(
        NYDBTest::TControllers::GetColumnShardController()->GetBlobSplitSettings(), NBlobOperations::TGlobal::DefaultStorageId);

    THashMap<ui32, std::shared_ptr<IPortionDataChunk>> inplaceChunks;
    for (auto&& [indexId, marker] : indexMarkers) {
        inplaceChunks[indexId] =
            std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(indexId, 0), batch->num_rows(), marker.size(), marker);
    }

    std::vector<TSplittedBlob> blobs;
    UNIT_ASSERT(slice.GroupBlobs(blobs, groups));

    auto constructor = TWritePortionInfoWithBlobsConstructor::BuildByBlobs(std::move(blobs), inplaceChunks, TInternalPathId::FromRawValue(1),
        schema->GetVersion(), schema->GetSnapshot(), storages, EPortionType::Written);

    NArrow::TFirstLastSpecialKeys primaryKeys(slice.GetFirstLastPKBatch(schema->GetIndexInfo().GetReplaceKey()));
    auto& portionCtor = constructor.GetPortionConstructor().MutablePortionConstructor();
    static_cast<TWrittenPortionInfoConstructor&>(portionCtor).SetInsertWriteId(TInsertWriteId(1));
    portionCtor.SetPortionId(1);
    portionCtor.AddMetadata(*schema, 0, primaryKeys, std::nullopt);
    portionCtor.MutableMeta().SetTierName(tier);
    portionCtor.MutableMeta().SetCompactionLevel(0);

    return TWritePortionInfoWithBlobsResult(std::move(constructor));
}

void FillReadBlobs(TWritePortionInfoWithBlobsResult& portion, NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
    for (auto& blob : portion.MutableBlobs()) {
        const TString& data = blob.GetResultBlob();
        for (auto&& chunkAddress : blob.GetChunks()) {
            const auto* recordInfo = portion.GetPortionResult().GetRecordPointer(chunkAddress);
            AFL_VERIFY(recordInfo);
            const auto range = portion.GetPortionResult().RestoreBlobRange(recordInfo->GetBlobRange());
            blobs.Add(IStoragesManager::DefaultStorageId, range, data.substr(range.Offset, range.Size));
        }
    }
}

TString GetInplaceIndexData(const TWritePortionInfoWithBlobsResult& result, const ui32 indexId) {
    for (auto&& index : result.GetPortionResult().GetIndexesVerified()) {
        if (index.GetIndexId() != indexId) {
            continue;
        }
        if (const auto* data = index.GetBlobDataOptional()) {
            return *data;
        }
    }
    UNIT_ASSERT_C(false, "inplace index not found: " + ::ToString(indexId));
    return "";
}

i32 GetMaxIndexValue(const ISnapshotSchema::TPtr& schema, const TString& indexData) {
    const auto maxMeta =
        std::static_pointer_cast<NIndexes::NMax::TIndexMeta>(schema->GetIndexInfo().GetIndexVerified(MaxIndexId).GetObjectPtr());
    const auto dataType = schema->GetFieldByColumnIdOptional(ValueColumnId)->type();
    const auto scalar = maxMeta->GetMaxScalarVerified({ indexData }, dataType);
    return std::static_pointer_cast<arrow::Int32Scalar>(scalar)->value;
}

std::shared_ptr<NColumnShard::TSplitterCounters> MakeSplitterCounters() {
    return std::make_shared<NColumnShard::TIndexationCounters>("test")->SplitterCounters;
}

void FinalizeWritePortion(TWritePortionInfoWithBlobsResult& portion) {
    portion.RegisterFakeBlobIds();
    portion.FinalizePortionConstructor(TSnapshot(1, 1));
}

std::optional<TWritePortionInfoWithBlobsResult> RunSyncPortion(TWritePortionInfoWithBlobsResult& writePortion,
    const ISnapshotSchema::TPtr& schemaFrom, const ISnapshotSchema::TPtr& schemaTo, const TString& targetTier,
    const std::shared_ptr<NColumnShard::TSplitterCounters>& splitterCounters) {
    NBlobOperations::NRead::TCompositeReadBlobs blobs;
    FillReadBlobs(writePortion, blobs);

    auto readPortion = TReadPortionInfoWithBlobs::RestorePortion(writePortion.GetPortionResult(), blobs, schemaFrom->GetIndexInfo());
    return TReadPortionInfoWithBlobs::SyncPortion(
        std::move(readPortion), schemaFrom, schemaTo, targetTier, TTestStoragesManager::GetInstance(), splitterCounters);
}

bool IsMemoryTierAvailable() {
    return TTestStoragesManager::GetInstance()->GetOperatorOptional(IStoragesManager::MemoryStorageId) != nullptr;
}

}   // namespace

Y_UNIT_TEST_SUITE(TReuseIndexChunksTests) {
    Y_UNIT_TEST(PlacesInplaceIndexChunk) {
        const auto schema = MakeTestSchema(1, { .WithMaxIndex = true });
        const auto batch = MakeTestBatch();
        const TString marker(MaxIndexMarker);

        TIndexInfo::TSecondaryData secondaryData;
        std::vector<std::shared_ptr<IPortionDataChunk>> chunks = {
            std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(MaxIndexId, 0), batch->num_rows(), marker.size(), marker),
        };

        const auto status = schema->GetIndexInfo().ReuseIndexChunks(std::move(chunks), MaxIndexId, TTestStoragesManager::GetInstance(),
            batch->num_rows(), IStoragesManager::DefaultStorageId, secondaryData);
        UNIT_ASSERT(status.Ok());

        const auto& inplace = secondaryData.GetSecondaryInplaceData();
        UNIT_ASSERT_EQUAL(inplace.size(), 1u);
        UNIT_ASSERT(inplace.contains(MaxIndexId));
        UNIT_ASSERT_VALUES_EQUAL(inplace.at(MaxIndexId)->GetData(), marker);
    }
}

Y_UNIT_TEST_SUITE(TSyncPortionIndexReuseTests) {
    Y_UNIT_TEST(TierChangeReusesMaxIndex) {
        if (!IsMemoryTierAvailable()) {
            return;
        }

        const auto schema = MakeTestSchema(1, { .WithMaxIndex = true });
        const auto batch = MakeTestBatch();
        const auto splitterCounters = MakeSplitterCounters();

        auto writePortion = BuildTestPortion(schema, batch, { { MaxIndexId, MaxIndexMarker } }, splitterCounters);
        FinalizeWritePortion(writePortion);

        auto syncResult = RunSyncPortion(writePortion, schema, schema, IStoragesManager::MemoryStorageId, splitterCounters);

        UNIT_ASSERT(syncResult);
        FinalizeWritePortion(*syncResult);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->GetPortionResult().GetPortionInfo().GetTierNameDef(IStoragesManager::DefaultStorageId),
            IStoragesManager::MemoryStorageId);
        UNIT_ASSERT_VALUES_EQUAL(GetInplaceIndexData(*syncResult, MaxIndexId), MaxIndexMarker);
    }

    Y_UNIT_TEST(DifferentSchemaReusesCompatibleBloomIndex) {
        const auto schemaFrom = MakeTestSchema(1, { .WithBloomIndex = true });
        const auto schemaTo = MakeTestSchema(2, { .WithBloomIndex = true });
        const auto batch = MakeTestBatch();
        const auto splitterCounters = MakeSplitterCounters();

        auto writePortion = BuildTestPortion(schemaFrom, batch, { { BloomIndexId, BloomIndexMarker } }, splitterCounters);
        FinalizeWritePortion(writePortion);

        auto syncResult = RunSyncPortion(writePortion, schemaFrom, schemaTo, IStoragesManager::DefaultStorageId, splitterCounters);

        UNIT_ASSERT(syncResult);
        FinalizeWritePortion(*syncResult);
        UNIT_ASSERT_VALUES_EQUAL(GetInplaceIndexData(*syncResult, BloomIndexId), BloomIndexMarker);
    }

    Y_UNIT_TEST(DifferentSchemaRebuildsIncompatibleMaxIndex) {
        const auto schemaFrom = MakeTestSchema(1, { .WithMaxIndex = true });
        const auto schemaTo = MakeTestSchema(2, { .WithMaxIndex = true });
        const auto batch = MakeTestBatch();
        const auto splitterCounters = MakeSplitterCounters();

        auto writePortion = BuildTestPortion(schemaFrom, batch, { { MaxIndexId, MaxIndexMarker } }, splitterCounters);
        FinalizeWritePortion(writePortion);

        auto syncResult = RunSyncPortion(writePortion, schemaFrom, schemaTo, IStoragesManager::DefaultStorageId, splitterCounters);

        UNIT_ASSERT(syncResult);
        FinalizeWritePortion(*syncResult);
        const TString indexData = GetInplaceIndexData(*syncResult, MaxIndexId);
        UNIT_ASSERT_VALUES_UNEQUAL(indexData, MaxIndexMarker);
        UNIT_ASSERT_VALUES_EQUAL(GetMaxIndexValue(schemaTo, indexData), ExpectedMaxColumnValue);
    }

    Y_UNIT_TEST(MissingIndexChunksRebuildsIndex) {
        const auto schemaFrom = MakeTestSchema(1, { .WithMaxIndex = true });
        const auto schemaTo = MakeTestSchema(2, { .WithMaxIndex = true });
        const auto batch = MakeTestBatch();
        const auto splitterCounters = MakeSplitterCounters();

        auto writePortion = BuildTestPortion(schemaFrom, batch, {}, splitterCounters);
        FinalizeWritePortion(writePortion);

        auto syncResult = RunSyncPortion(writePortion, schemaFrom, schemaTo, IStoragesManager::DefaultStorageId, splitterCounters);

        UNIT_ASSERT(syncResult);
        FinalizeWritePortion(*syncResult);
        const TString indexData = GetInplaceIndexData(*syncResult, MaxIndexId);
        UNIT_ASSERT_VALUES_UNEQUAL(indexData, MaxIndexMarker);
        UNIT_ASSERT_VALUES_EQUAL(GetMaxIndexValue(schemaTo, indexData), ExpectedMaxColumnValue);
    }

    // ReuseIndexChunks rejects index blobs above MaxBlobSize; SyncPortion must rebuild via AppendIndex.
    Y_UNIT_TEST(OversizedIndexFallsBackToRebuild) {
        const auto schemaFrom = MakeTestSchema(1, { .WithMaxIndex = true });
        const auto schemaTo = MakeTestSchema(2, { .WithMaxIndex = true });
        const auto batch = MakeTestBatch();
        const auto splitterCounters = MakeSplitterCounters();
        const auto storages = TTestStoragesManager::GetInstance();
        const auto maxBlobSize =
            storages->GetOperatorVerified(IStoragesManager::LocalMetadataStorageId)->GetBlobSplitSettings().GetMaxBlobSize();
        const TString hugeMarker(maxBlobSize + 1024, 'X');

        auto writePortion = BuildTestPortion(schemaFrom, batch, { { MaxIndexId, hugeMarker } }, splitterCounters);
        FinalizeWritePortion(writePortion);

        auto syncResult = RunSyncPortion(writePortion, schemaFrom, schemaTo, IStoragesManager::DefaultStorageId, splitterCounters);

        UNIT_ASSERT(syncResult);
        FinalizeWritePortion(*syncResult);
        const TString indexData = GetInplaceIndexData(*syncResult, MaxIndexId);
        UNIT_ASSERT_VALUES_UNEQUAL(indexData, hugeMarker);
        UNIT_ASSERT_VALUES_EQUAL(GetMaxIndexValue(schemaTo, indexData), ExpectedMaxColumnValue);
    }

    Y_UNIT_TEST(MixedIndexesPartialReuse) {
        const TTestSchemaOptions options{ .WithMaxIndex = true, .WithBloomIndex = true };
        const auto schemaFrom = MakeTestSchema(1, options);
        const auto schemaTo = MakeTestSchema(2, options);
        const auto batch = MakeTestBatch();
        const auto splitterCounters = MakeSplitterCounters();

        auto writePortion =
            BuildTestPortion(schemaFrom, batch, { { MaxIndexId, MaxIndexMarker }, { BloomIndexId, BloomIndexMarker } }, splitterCounters);
        FinalizeWritePortion(writePortion);

        auto syncResult = RunSyncPortion(writePortion, schemaFrom, schemaTo, IStoragesManager::DefaultStorageId, splitterCounters);

        UNIT_ASSERT(syncResult);
        FinalizeWritePortion(*syncResult);
        const TString maxIndexData = GetInplaceIndexData(*syncResult, MaxIndexId);
        UNIT_ASSERT_VALUES_UNEQUAL(maxIndexData, MaxIndexMarker);
        UNIT_ASSERT_VALUES_EQUAL(GetMaxIndexValue(schemaTo, maxIndexData), ExpectedMaxColumnValue);
        UNIT_ASSERT_VALUES_EQUAL(GetInplaceIndexData(*syncResult, BloomIndexId), BloomIndexMarker);
    }
}

}   // namespace NKikimr::NOlap::NTest
