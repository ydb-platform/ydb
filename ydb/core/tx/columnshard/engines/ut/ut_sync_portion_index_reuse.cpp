#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/portions/read_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/snapshot_scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
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
constexpr ui32 MaxIndexId = 1001;
constexpr const char* IndexMarker = "REUSE_INDEX_MARKER_v1";

ISnapshotSchema::TPtr MakeSchemaWithMaxIndex(const ui64 presetId = 1) {
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
    proto.SetVersion(presetId);
    proto.MutableOptions()->MutableCompactionPlannerConstructor()->SetClassName("l-buckets");
    *proto.MutableOptions()->MutableCompactionPlannerConstructor()->MutableLBuckets() =
        NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLOptimizer();
    *proto.AddIndexes() = NIndexes::TIndexMetaContainer(
        std::make_shared<NIndexes::NMax::TIndexMeta>(MaxIndexId, "max_value", IStoragesManager::LocalMetadataStorageId, false, ValueColumnId))
                              .SerializeToProto();

    auto indexInfoOpt = TIndexInfo::BuildFromProto(presetId, proto, storages, cache);
    UNIT_ASSERT(indexInfoOpt);
    return std::make_shared<TSnapshotSchema>(cache->UpsertIndexInfo(std::move(*indexInfoOpt)), TSnapshot(1, 1));
}

std::shared_ptr<arrow::RecordBatch> MakeTestBatch() {
    arrow::UInt64Builder pkBuilder;
    arrow::Int32Builder valueBuilder;
    UNIT_ASSERT(pkBuilder.AppendValues({ 1, 2, 3 }).ok());
    UNIT_ASSERT(valueBuilder.AppendValues({ 10, 20, 30 }).ok());
    auto schema = arrow::schema({ arrow::field("pk", arrow::uint64()), arrow::field("value", arrow::int32()) });
    return arrow::RecordBatch::Make(schema, 3, { pkBuilder.Finish().ValueOrDie(), valueBuilder.Finish().ValueOrDie() });
}

THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> BuildColumnChunks(
    const ISnapshotSchema::TPtr& schema, const std::shared_ptr<arrow::RecordBatch>& batch) {
    THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> chunks;
    const auto& indexFields = schema->GetIndexInfo().ArrowSchema();

    auto itIncoming = batch->schema()->fields().begin();
    const auto itIncomingEnd = batch->schema()->fields().end();
    auto itIndex = indexFields.begin();
    const auto itIndexEnd = indexFields.end();

    while (itIncoming != itIncomingEnd && itIndex != itIndexEnd) {
        if ((*itIncoming)->name() == (*itIndex)->name()) {
            const ui32 incomingIndex = itIncoming - batch->schema()->fields().begin();
            const ui32 columnIndex = itIndex - indexFields.begin();
            const ui32 columnId = schema->GetIndexInfo().GetColumnIdByIndexVerified(columnIndex);
            auto loader = schema->GetIndexInfo().GetColumnLoaderVerified(columnId);
            const auto& columnFeatures = schema->GetIndexInfo().GetColumnFeaturesVerified(columnId);
            const auto& accessorConstructor = loader->GetAccessorConstructor();
            const auto incomingColumn = batch->column(incomingIndex);
            auto accessor = std::make_shared<NArrow::NAccessor::TTrivialArray>(incomingColumn);
            const auto loadContext = loader->BuildAccessorContext(accessor->GetRecordsCount());
            auto arrToWrite = accessorConstructor->Construct(accessor, loadContext);
            UNIT_ASSERT(arrToWrite.IsSuccess());

            std::vector<std::shared_ptr<IPortionDataChunk>> columnChunks = { std::make_shared<NChunks::TChunkPreparation>(
                accessorConstructor->SerializeToString(*arrToWrite, loadContext), *arrToWrite, TChunkAddress(columnId, 0), columnFeatures) };
            UNIT_ASSERT(chunks.emplace(columnId, std::move(columnChunks)).second);
            ++itIncoming;
        }
        ++itIndex;
    }
    UNIT_ASSERT(itIncoming == itIncomingEnd);
    return chunks;
}

TWritePortionInfoWithBlobsResult BuildTestPortion(const ISnapshotSchema::TPtr& schema, const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::optional<TString>& indexMarker, const std::shared_ptr<NColumnShard::TSplitterCounters>& splitterCounters,
    const TString& tier = IStoragesManager::DefaultStorageId) {
    auto storages = TTestStoragesManager::GetInstance();
    const auto entityChunks = BuildColumnChunks(schema, batch);

    std::shared_ptr<TDefaultSchemaDetails> schemaDetails(
        new TDefaultSchemaDetails(schema, std::make_shared<NArrow::NSplitter::TSerializationStats>()));
    TGeneralSerializedSlice slice(entityChunks, schemaDetails, splitterCounters);

    const NSplitter::TEntityGroups groups(
        NYDBTest::TControllers::GetColumnShardController()->GetBlobSplitSettings(), NBlobOperations::TGlobal::DefaultStorageId);

    THashMap<ui32, std::shared_ptr<IPortionDataChunk>> inplaceChunks;
    if (indexMarker) {
        inplaceChunks[MaxIndexId] =
            std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(MaxIndexId, 0), batch->num_rows(), indexMarker->size(), *indexMarker);
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
    UNIT_ASSERT_C(false, "inplace index not found");
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

}   // namespace

Y_UNIT_TEST_SUITE(TReuseIndexChunksTests) {
    Y_UNIT_TEST(PlacesInplaceIndexChunk) {
        const auto schema = MakeSchemaWithMaxIndex();
        const auto batch = MakeTestBatch();
        const TString marker(IndexMarker);

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

    Y_UNIT_TEST(RejectsOversizedBlob) {
        const auto schema = MakeSchemaWithMaxIndex();
        const auto storages = TTestStoragesManager::GetInstance();
        const auto maxBlobSize =
            storages->GetOperatorVerified(IStoragesManager::LocalMetadataStorageId)->GetBlobSplitSettings().GetMaxBlobSize();
        const TString hugeMarker(maxBlobSize + 1024, 'X');

        TIndexInfo::TSecondaryData secondaryData;
        std::vector<std::shared_ptr<IPortionDataChunk>> chunks = {
            std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(MaxIndexId, 0), 3, hugeMarker.size(), hugeMarker),
        };

        const auto status = schema->GetIndexInfo().ReuseIndexChunks(
            std::move(chunks), MaxIndexId, storages, 3, IStoragesManager::DefaultStorageId, secondaryData);
        UNIT_ASSERT(status.IsFail());
        UNIT_ASSERT(secondaryData.GetSecondaryInplaceData().empty());
    }
}

Y_UNIT_TEST_SUITE(TSyncPortionIndexReuseTests) {
    Y_UNIT_TEST(TierChangeReusesMaxIndex) {
#ifndef KIKIMR_DISABLE_S3_OPS
        const auto schema = MakeSchemaWithMaxIndex();
        const auto batch = MakeTestBatch();
        const auto splitterCounters = MakeSplitterCounters();

        auto writePortion = BuildTestPortion(schema, batch, TString(IndexMarker), splitterCounters);
        FinalizeWritePortion(writePortion);

        NBlobOperations::NRead::TCompositeReadBlobs blobs;
        FillReadBlobs(writePortion, blobs);

        auto readPortion = TReadPortionInfoWithBlobs::RestorePortion(writePortion.GetPortionResult(), blobs, schema->GetIndexInfo());
        auto syncResult = TReadPortionInfoWithBlobs::SyncPortion(
            std::move(readPortion), schema, schema, IStoragesManager::MemoryStorageId, TTestStoragesManager::GetInstance(), splitterCounters);

        UNIT_ASSERT(syncResult);
        FinalizeWritePortion(*syncResult);
        UNIT_ASSERT_VALUES_EQUAL(syncResult->GetPortionResult().GetPortionInfo().GetTierNameDef(IStoragesManager::DefaultStorageId),
            IStoragesManager::MemoryStorageId);
        UNIT_ASSERT_VALUES_EQUAL(GetInplaceIndexData(*syncResult, MaxIndexId), IndexMarker);
#else
        UNIT_ASSERT(true);
#endif
    }

    Y_UNIT_TEST(MissingIndexChunksRebuildsIndex) {
        const auto schemaFrom = MakeSchemaWithMaxIndex(1);
        const auto schemaTo = MakeSchemaWithMaxIndex(2);
        const auto batch = MakeTestBatch();
        const auto splitterCounters = MakeSplitterCounters();

        auto writePortion = BuildTestPortion(schemaFrom, batch, std::nullopt, splitterCounters);
        FinalizeWritePortion(writePortion);

        NBlobOperations::NRead::TCompositeReadBlobs blobs;
        FillReadBlobs(writePortion, blobs);

        auto readPortion = TReadPortionInfoWithBlobs::RestorePortion(writePortion.GetPortionResult(), blobs, schemaFrom->GetIndexInfo());
        auto syncResult = TReadPortionInfoWithBlobs::SyncPortion(std::move(readPortion), schemaFrom, schemaTo,
            IStoragesManager::DefaultStorageId, TTestStoragesManager::GetInstance(), splitterCounters);

        UNIT_ASSERT(syncResult);
        FinalizeWritePortion(*syncResult);
        const TString indexData = GetInplaceIndexData(*syncResult, MaxIndexId);
        UNIT_ASSERT_VALUES_UNEQUAL(indexData, IndexMarker);
        UNIT_ASSERT_VALUES_EQUAL(GetMaxIndexValue(schemaTo, indexData), 30);
    }

    Y_UNIT_TEST(OversizedIndexFallsBackToRebuild) {
        const auto schemaFrom = MakeSchemaWithMaxIndex(1);
        const auto schemaTo = MakeSchemaWithMaxIndex(2);
        const auto batch = MakeTestBatch();
        const auto splitterCounters = MakeSplitterCounters();
        const auto storages = TTestStoragesManager::GetInstance();
        const auto maxBlobSize =
            storages->GetOperatorVerified(IStoragesManager::LocalMetadataStorageId)->GetBlobSplitSettings().GetMaxBlobSize();
        const TString hugeMarker(maxBlobSize + 1024, 'X');

        auto writePortion = BuildTestPortion(schemaFrom, batch, hugeMarker, splitterCounters);
        FinalizeWritePortion(writePortion);

        NBlobOperations::NRead::TCompositeReadBlobs blobs;
        FillReadBlobs(writePortion, blobs);

        auto readPortion = TReadPortionInfoWithBlobs::RestorePortion(writePortion.GetPortionResult(), blobs, schemaFrom->GetIndexInfo());
        auto syncResult = TReadPortionInfoWithBlobs::SyncPortion(
            std::move(readPortion), schemaFrom, schemaTo, IStoragesManager::DefaultStorageId, storages, splitterCounters);

        UNIT_ASSERT(syncResult);
        FinalizeWritePortion(*syncResult);
        const TString indexData = GetInplaceIndexData(*syncResult, MaxIndexId);
        UNIT_ASSERT_VALUES_UNEQUAL(indexData, hugeMarker);
        UNIT_ASSERT_VALUES_EQUAL(GetMaxIndexValue(schemaTo, indexData), 30);
    }
}

}   // namespace NKikimr::NOlap::NTest
