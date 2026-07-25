#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/portions/read_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/snapshot_scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom_ngramm/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>

namespace NKikimr::NOlap::NTest {

namespace {

constexpr ui32 PkColumnId = 1;
constexpr ui32 ValueColumnId = 2;
constexpr ui32 NGrammIndexId = 1001;

// The index blob is filter_size_bytes long no matter how few records the portion has, so a filter above the
// storage limit is reproducible with a three-row portion.
constexpr ui32 FilterSizeBytes = NLocalIndex::NBloom::TConstants::MaxFilterSizeBytes;
constexpr i64 MaxBlobSize = FilterSizeBytes / 2;

ISnapshotSchema::TPtr MakeSchemaWithNGrammIndex(const ui64 version, const ui32 filterSizeBytes = FilterSizeBytes,
    const ui32 recordsCountBase = 1024, const TString& indexStorageId = IStoragesManager::DefaultStorageId) {
    NKikimrSchemeOp::TColumnTableSchema proto;
    const std::vector<NArrow::NTest::TTestColumn> columns = {
        NArrow::NTest::TTestColumn("pk", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)),
        NArrow::NTest::TTestColumn("value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)),
    };
    *proto.MutableColumns()->Add() = columns[0].CreateColumn(PkColumnId);
    *proto.MutableColumns()->Add() = columns[1].CreateColumn(ValueColumnId);
    proto.AddKeyColumnNames("pk");
    proto.SetVersion(version);
    proto.MutableOptions()->MutableCompactionPlannerConstructor()->SetClassName("l-buckets");
    *proto.MutableOptions()->MutableCompactionPlannerConstructor()->MutableLBuckets() =
        NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLOptimizer();

    NLocalIndex::NBloom::TRequestSettings request;
    request.NGrammSize = 3;
    request.DeprecatedHashesCount = 2;
    request.DeprecatedFilterSizeBytes = filterSizeBytes;
    request.DeprecatedRecordsCount = recordsCountBase;
    *proto.AddIndexes() = NIndexes::TIndexMetaContainer(
        std::make_shared<NIndexes::NBloomNGramm::TIndexMeta>(NGrammIndexId, "ngramm_value", indexStorageId, false, ValueColumnId,
            NIndexes::TReadDataExtractorContainer(std::make_shared<NIndexes::TDefaultDataExtractor>()),
            NIndexes::IBitsStorageConstructor::GetDefault(), request))
                              .SerializeToProto();

    auto cache = std::make_shared<TSchemaObjectsCache>();
    auto indexInfo = TIndexInfo::BuildFromProto(version, proto, TTestStoragesManager::GetInstance(), cache);
    UNIT_ASSERT(indexInfo);
    return std::make_shared<TSnapshotSchema>(cache->UpsertIndexInfo(std::move(*indexInfo)), TSnapshot(1, 1));
}

std::shared_ptr<arrow::RecordBatch> MakeTestBatch(const ui32 firstPk = 1, const ui32 rowsCount = 3) {
    arrow::UInt64Builder pkBuilder;
    arrow::StringBuilder valueBuilder;
    for (ui32 i = 0; i < rowsCount; ++i) {
        UNIT_ASSERT(pkBuilder.Append(firstPk + i).ok());
        const TString value = "value_" + ::ToString(firstPk + i);
        UNIT_ASSERT(valueBuilder.Append(value.data(), value.size()).ok());
    }
    auto schema = arrow::schema({ arrow::field("pk", arrow::uint64()), arrow::field("value", arrow::utf8()) });
    return arrow::RecordBatch::Make(schema, rowsCount, { pkBuilder.Finish().ValueOrDie(), valueBuilder.Finish().ValueOrDie() });
}

THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> BuildColumnChunks(
    const ISnapshotSchema::TPtr& schema, const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
    THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> chunks;
    for (const auto& field : batches.front()->schema()->fields()) {
        const ui32 columnId = schema->GetColumnIdVerified(field->name());
        auto loader = schema->GetIndexInfo().GetColumnLoaderVerified(columnId);
        const auto& accessorConstructor = loader->GetAccessorConstructor();
        for (ui32 chunkIdx = 0; chunkIdx < batches.size(); ++chunkIdx) {
            auto accessor = std::make_shared<NArrow::NAccessor::TTrivialArray>(batches[chunkIdx]->GetColumnByName(field->name()));
            const auto loadContext = loader->BuildAccessorContext(accessor->GetRecordsCount());
            auto arrToWrite = accessorConstructor->Construct(accessor, loadContext);
            UNIT_ASSERT(arrToWrite.IsSuccess());
            chunks[columnId].emplace_back(
                std::make_shared<NChunks::TChunkPreparation>(accessorConstructor->SerializeToString(*arrToWrite, loadContext), *arrToWrite,
                    TChunkAddress(columnId, chunkIdx), schema->GetIndexInfo().GetColumnFeaturesVerified(columnId)));
        }
    }
    return chunks;
}

TWritePortionInfoWithBlobsResult BuildPortionWithoutIndexes(const ISnapshotSchema::TPtr& schema,
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<NColumnShard::TSplitterCounters>& splitterCounters) {
    std::shared_ptr<TDefaultSchemaDetails> schemaDetails(
        new TDefaultSchemaDetails(schema, std::make_shared<NArrow::NSplitter::TSerializationStats>()));
    TGeneralSerializedSlice slice(BuildColumnChunks(schema, { batch }), schemaDetails, splitterCounters);

    const NSplitter::TEntityGroups groups(NSplitter::TSplitSettings(), NBlobOperations::TGlobal::DefaultStorageId);
    std::vector<TSplittedBlob> blobs;
    UNIT_ASSERT(slice.GroupBlobs(blobs, groups));

    auto constructor = TWritePortionInfoWithBlobsConstructor::BuildByBlobs(std::move(blobs), {}, TInternalPathId::FromRawValue(1),
        schema->GetVersion(), schema->GetSnapshot(), TTestStoragesManager::GetInstance(), EPortionType::Written, schema->GetIndexInfo());

    NArrow::TFirstLastSpecialKeys primaryKeys(slice.GetFirstLastPKBatch(schema->GetIndexInfo().GetReplaceKey()));
    auto& portionCtor = constructor.GetPortionConstructor().MutablePortionConstructor();
    static_cast<TWrittenPortionInfoConstructor&>(portionCtor).SetInsertWriteId(TInsertWriteId(1));
    portionCtor.SetPortionId(1);
    portionCtor.AddMetadata(*schema, 0, primaryKeys, std::nullopt);
    portionCtor.MutableMeta().SetTierName(IStoragesManager::DefaultStorageId);
    portionCtor.MutableMeta().SetCompactionLevel(0);

    TWritePortionInfoWithBlobsResult result(std::move(constructor));
    result.RegisterFakeBlobIds();
    result.FinalizePortionConstructor(TSnapshot(1, 1));
    return result;
}

NBlobOperations::NRead::TCompositeReadBlobs ReadBlobs(TWritePortionInfoWithBlobsResult& portion) {
    NBlobOperations::NRead::TCompositeReadBlobs blobs;
    for (auto& blob : portion.MutableBlobs()) {
        const TString& data = blob.GetResultBlob();
        for (auto&& chunkAddress : blob.GetChunks()) {
            const auto* recordInfo = portion.GetPortionResult().GetRecordPointer(chunkAddress);
            AFL_VERIFY(recordInfo);
            const auto range = portion.GetPortionResult().RestoreBlobRange(recordInfo->GetBlobRange());
            blobs.Add(IStoragesManager::DefaultStorageId, range, data.substr(range.Offset, range.Size));
        }
    }
    return blobs;
}

bool HasIndex(const TWritePortionInfoWithBlobsResult& portion, const ui32 indexId) {
    for (auto&& index : portion.GetPortionResult().GetIndexesVerified()) {
        if (index.GetIndexId() == indexId) {
            return true;
        }
    }
    return false;
}

}   // namespace

Y_UNIT_TEST_SUITE(TIndexBlobSizeLimitTests) {
    // Issue #26733: actualization rebuilds the index for the target tier, and a filter above the storage
    // MaxBlobSize used to abort the tablet with "blob size for secondary data ... bigger than limit". A
    // filter that cannot even be split (its base size is above the limit) is folded down to the limit.
    Y_UNIT_TEST(OversizedIndexOnActualizationIsClampedToLimit) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverrideBlobSplitSettings(NSplitter::TSplitSettings().SetMaxBlobSize(MaxBlobSize).SetMinBlobSize(MaxBlobSize / 4));

        const auto schemaFrom = MakeSchemaWithNGrammIndex(1);
        const auto schemaTo = MakeSchemaWithNGrammIndex(2);
        const auto splitterCounters = std::make_shared<NColumnShard::TIndexationCounters>("test")->SplitterCounters;

        auto writePortion = BuildPortionWithoutIndexes(schemaFrom, MakeTestBatch(), splitterCounters);
        auto blobs = ReadBlobs(writePortion);
        auto readPortion = TReadPortionInfoWithBlobs::RestorePortion(writePortion.GetPortionResult(), blobs, schemaFrom->GetIndexInfo());

        auto syncResult = TReadPortionInfoWithBlobs::SyncPortion(std::move(readPortion), schemaFrom, schemaTo,
            IStoragesManager::DefaultStorageId, TTestStoragesManager::GetInstance(), splitterCounters);

        UNIT_ASSERT(syncResult);
        syncResult->RegisterFakeBlobIds();
        syncResult->FinalizePortionConstructor(TSnapshot(1, 1));
        UNIT_ASSERT(HasIndex(*syncResult, NGrammIndexId));
        for (const auto& index : syncResult->GetPortionResult().GetIndexesVerified()) {
            if (index.GetIndexId() == NGrammIndexId) {
                UNIT_ASSERT_LE(index.GetDataSize(), MaxBlobSize);
            }
        }
    }

    // The same index fits when the storage limit is the production one, so the portion keeps its index.
    Y_UNIT_TEST(IndexUnderLimitIsBuilt) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverrideBlobSplitSettings(NSplitter::TSplitSettings());

        const auto schemaFrom = MakeSchemaWithNGrammIndex(1);
        const auto schemaTo = MakeSchemaWithNGrammIndex(2);
        const auto splitterCounters = std::make_shared<NColumnShard::TIndexationCounters>("test")->SplitterCounters;

        auto writePortion = BuildPortionWithoutIndexes(schemaFrom, MakeTestBatch(), splitterCounters);
        auto blobs = ReadBlobs(writePortion);
        auto readPortion = TReadPortionInfoWithBlobs::RestorePortion(writePortion.GetPortionResult(), blobs, schemaFrom->GetIndexInfo());

        auto syncResult = TReadPortionInfoWithBlobs::SyncPortion(std::move(readPortion), schemaFrom, schemaTo,
            IStoragesManager::DefaultStorageId, TTestStoragesManager::GetInstance(), splitterCounters);

        UNIT_ASSERT(syncResult);
        syncResult->RegisterFakeBlobIds();
        syncResult->FinalizePortionConstructor(TSnapshot(1, 1));
        UNIT_ASSERT(HasIndex(*syncResult, NGrammIndexId));
    }

    // An index above MaxBlobSize of a blob storage is split into several chunks covering record subranges,
    // each fitting the limit, instead of being dropped.
    Y_UNIT_TEST(OversizedIndexOnDefaultStorageIsSplit) {
        constexpr i64 blobLimit = 10_KB;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverrideBlobSplitSettings(NSplitter::TSplitSettings().SetMaxBlobSize(blobLimit).SetMinBlobSize(blobLimit / 4));

        // 512 records over 4 column chunks: the whole-portion filter is 32 KB (8 KB doubled twice), while
        // a 128-record subrange keeps the base 8 KB, which fits the 10 KB limit.
        const auto schema = MakeSchemaWithNGrammIndex(1, 8_KB, 128);
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        for (ui32 i = 0; i < 4; ++i) {
            batches.emplace_back(MakeTestBatch(1 + i * 128, 128));
        }
        const auto chunks = BuildColumnChunks(schema, batches);

        TIndexInfo::TSecondaryData secondaryData;
        secondaryData.MutableExternalData() = chunks;
        const auto conclusion = schema->GetIndexInfo().AppendIndex(
            chunks, NGrammIndexId, TTestStoragesManager::GetInstance(), 512, IStoragesManager::DefaultStorageId, secondaryData);
        UNIT_ASSERT_C(conclusion.Ok(), conclusion.GetErrorMessage());

        const auto it = secondaryData.GetExternalData().find(NGrammIndexId);
        UNIT_ASSERT(it != secondaryData.GetExternalData().end());
        UNIT_ASSERT_VALUES_EQUAL(it->second.size(), 4);
        ui32 recordsSum = 0;
        for (const auto& chunk : it->second) {
            UNIT_ASSERT_LE(chunk->GetPackedSize(), blobLimit);
            recordsSum += chunk->GetRecordsCountVerified();
        }
        UNIT_ASSERT_VALUES_EQUAL(recordsSum, 512);
    }

    // An oversized inplace (_LOCAL) index cannot be split: it is dropped completely, leaving no chunks and
    // no inplace metadata, so nothing about it is persisted with the portion (IndexColumnsV2 stays clean).
    Y_UNIT_TEST(OversizedLocalIndexIsSkippedWithoutMeta) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverrideBlobSplitSettings(NSplitter::TSplitSettings().SetMaxBlobSize(MaxBlobSize).SetMinBlobSize(MaxBlobSize / 4));

        const auto schemaFrom = MakeSchemaWithNGrammIndex(1, FilterSizeBytes, 1024, IStoragesManager::LocalMetadataStorageId);
        const auto schemaTo = MakeSchemaWithNGrammIndex(2, FilterSizeBytes, 1024, IStoragesManager::LocalMetadataStorageId);
        const auto splitterCounters = std::make_shared<NColumnShard::TIndexationCounters>("test")->SplitterCounters;

        {
            TIndexInfo::TSecondaryData secondaryData;
            const auto chunks = BuildColumnChunks(schemaFrom, { MakeTestBatch() });
            secondaryData.MutableExternalData() = chunks;
            const auto conclusion = schemaFrom->GetIndexInfo().AppendIndex(
                chunks, NGrammIndexId, TTestStoragesManager::GetInstance(), 3, IStoragesManager::DefaultStorageId, secondaryData);
            UNIT_ASSERT_C(conclusion.Ok(), conclusion.GetErrorMessage());
            UNIT_ASSERT(!secondaryData.GetExternalData().contains(NGrammIndexId));
            UNIT_ASSERT(secondaryData.GetSecondaryInplaceData().empty());
        }

        auto writePortion = BuildPortionWithoutIndexes(schemaFrom, MakeTestBatch(), splitterCounters);
        auto blobs = ReadBlobs(writePortion);
        auto readPortion = TReadPortionInfoWithBlobs::RestorePortion(writePortion.GetPortionResult(), blobs, schemaFrom->GetIndexInfo());

        auto syncResult = TReadPortionInfoWithBlobs::SyncPortion(std::move(readPortion), schemaFrom, schemaTo,
            IStoragesManager::DefaultStorageId, TTestStoragesManager::GetInstance(), splitterCounters);

        UNIT_ASSERT(syncResult);
        syncResult->RegisterFakeBlobIds();
        syncResult->FinalizePortionConstructor(TSnapshot(1, 1));
        UNIT_ASSERT(!HasIndex(*syncResult, NGrammIndexId));
    }
}

}   // namespace NKikimr::NOlap::NTest
