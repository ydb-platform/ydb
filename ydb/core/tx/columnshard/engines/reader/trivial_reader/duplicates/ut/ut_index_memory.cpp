#include <ydb/core/formats/arrow/program/abstract.h>
#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/formats/arrow/program/execution.h>
#include <ydb/core/tx/columnshard/column_fetching/manager.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/metadata_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/common/description.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/script.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetch_steps.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/scheme/common/cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/meta.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom/const.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>

using namespace NKikimr;
using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NReader;

namespace {

constexpr ui32 PkColumnId = 0;
constexpr ui32 BloomIndexId = 1002;
constexpr ui32 UnrelatedIndexId = 1003;
constexpr ui64 BloomPayloadBytes = 4096;
constexpr ui64 UnrelatedPayloadBytes = 128;

class TMockDataAccessorsManager: public NDataAccessorControl::IDataAccessorsManager {
private:
    void DoAskData(const std::shared_ptr<TDataAccessorsRequest>&) override {
    }

    void DoAddPortion(const std::shared_ptr<TPortionDataAccessor>&) override {
    }

    void DoRemovePortion(const TPortionInfo::TConstPtr&) override {
    }

public:
    using IDataAccessorsManager::IDataAccessorsManager;
};

struct TSchemaBundle {
    std::shared_ptr<const TVersionedIndex> VersionedIndex;
    ISnapshotSchema::TPtr Schema;
};

TSchemaBundle MakeSchemaWithBloom() {
    NKikimrSchemeOp::TColumnTableSchema proto;
    const NArrow::NTest::TTestColumn pk("pk", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
    *proto.MutableColumns()->Add() = pk.CreateColumn(PkColumnId);
    proto.AddKeyColumnNames("pk");
    proto.SetVersion(1);

    NIndexes::TRequestSettings bloomRequest;
    bloomRequest.FalsePositiveProbability = NIndexes::NDefaults::FalsePositiveProbability;
    *proto.AddIndexes() = NIndexes::TIndexMetaContainer(
        std::make_shared<NIndexes::TBloomIndexMeta>(BloomIndexId, "bloom_pk", IStoragesManager::LocalMetadataStorageId, false, PkColumnId,
            bloomRequest, NIndexes::TReadDataExtractorContainer(std::make_shared<NIndexes::TDefaultDataExtractor>()),
            NIndexes::IBitsStorageConstructor::GetDefault()))
                              .SerializeToProto();

    auto cache = std::make_shared<TSchemaObjectsCache>();
    auto indexInfo = TIndexInfo::BuildFromProto(1, proto, TTestStoragesManager::GetInstance(), cache);
    UNIT_ASSERT(indexInfo);
    auto indexInfoCache = std::make_shared<TObjectCache<TSchemaVersionId, TIndexInfo>>();
    auto entry = indexInfoCache->Upsert(TSchemaVersionId(1, 1), std::move(*indexInfo));
    auto versionedIndex = std::make_shared<TVersionedIndex>();
    versionedIndex->AddIndex(TSnapshot(1, 1), std::move(entry));
    return TSchemaBundle{ versionedIndex, versionedIndex->GetLastSchema() };
}

ui64 ReservedMemory(NTrivial::TPortionDataSource& source, const THashMap<ui32, NArrow::NSSA::IDataSource::TFetchIndexContext>& indexes) {
    NArrow::NSSA::TProcessorContext context(source, std::make_unique<NArrow::NAccessor::TAccessorsCollection>(), std::nullopt, false);
    auto conclusion = source.StartReserveMemory(context, {}, indexes, {}, std::make_shared<NArrow::NSSA::TFilterCalculationPolicy>());
    UNIT_ASSERT(conclusion.IsSuccess());
    auto execution = conclusion.DetachResult();
    UNIT_ASSERT(execution.IsPending());
    auto job = std::dynamic_pointer_cast<NCommon::TAllocateMemoryStep::TFetchingStepAllocation::TStartJob>(execution.ExtractPendingJob());
    UNIT_ASSERT(job);
    return job->GetMemory();
}

NArrow::NSSA::IDataSource::TFetchIndexContext IndexFetch(const NArrow::NSSA::TIndexCheckOperation::EOperation operation) {
    NArrow::NSSA::IDataSource::TFetchIndexContext::TOperationsBySubColumn operations;
    operations.Add("", NArrow::NSSA::TIndexCheckOperation(operation, true));
    return NArrow::NSSA::IDataSource::TFetchIndexContext(PkColumnId, operations);
}

class TReserveProbe: public NActors::TActorBootstrapped<TReserveProbe> {
private:
    std::shared_ptr<TReadContext> ReadContext;
    std::shared_ptr<TPortionInfo> Portion;
    std::shared_ptr<TPortionDataAccessor> Accessor;
    ISnapshotSchema::TPtr Schema;
    ui64* EqualsMemory = nullptr;
    ui64* GreaterMemory = nullptr;
    ui64* EmptyMemory = nullptr;
    bool* Finished = nullptr;

public:
    TReserveProbe(std::shared_ptr<TReadContext> readContext, std::shared_ptr<TPortionInfo> portion,
        std::shared_ptr<TPortionDataAccessor> accessor, ISnapshotSchema::TPtr schema, ui64* equalsMemory, ui64* greaterMemory, ui64* emptyMemory,
        bool* finished)
        : ReadContext(std::move(readContext))
        , Portion(std::move(portion))
        , Accessor(std::move(accessor))
        , Schema(std::move(schema))
        , EqualsMemory(equalsMemory)
        , GreaterMemory(greaterMemory)
        , EmptyMemory(emptyMemory)
        , Finished(finished)
    {
    }

    void Bootstrap() {
        auto special = std::make_shared<NTrivial::TSpecialReadContext>(ReadContext);
        auto source = std::make_shared<NTrivial::TPortionDataSource>(0, Portion, special, false);
        source->InitStageData(std::make_unique<NCommon::TFetchedData>(false, source->GetRecordsCountOptional()));
        auto accessor = Accessor;
        source->SetPortionAccessor(std::move(accessor));
        auto builder = NCommon::TFetchingScriptBuilder::MakeForTests(Schema);
        builder.AddStep(std::make_shared<NCommon::TBuildStageResultStep>());
        source->MutableExecutionContext().SetCursorStep(NCommon::TFetchingScriptCursor(std::move(builder).Build(), 0));

        *EqualsMemory = ReservedMemory(*source, { { PkColumnId, IndexFetch(NArrow::NSSA::TIndexCheckOperation::EOperation::Equals) } });
        *GreaterMemory = ReservedMemory(*source, { { PkColumnId, IndexFetch(NArrow::NSSA::TIndexCheckOperation::EOperation::Greater) } });
        *EmptyMemory = ReservedMemory(*source, {});
        *Finished = true;
        PassAway();
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TIndexReadMemoryTracking) {
    Y_UNIT_TEST(ReserveMemoryAddsStoredIndexChunk) {
        const auto bundle = MakeSchemaWithBloom();
        const auto& schema = bundle.Schema;
        const auto& versionedIndex = bundle.VersionedIndex;

        TReadDescription readDesc(0, TSnapshot(1, 1), ERequestSorting::NONE, false, EReaderClass::Trivial,
            std::make_shared<TUserTableAccessor>("test", NColumnShard::TUnifiedPathId::BuildValid(TInternalPathId::FromRawValue(1),
                                                             NColumnShard::TSchemeShardLocalPathId::FromRawValue(1))), std::nullopt);
        readDesc.SetScanCursor(nullptr);
        auto readMetadata = std::make_shared<NTrivial::TReadMetadata>(versionedIndex, readDesc);
        readMetadata->SetPKRangesFilter(readDesc.PKRangesFilter);

        NActors::TTestActorRuntimeBase runtime(1, false);
        runtime.Initialize();
        const auto edge = runtime.AllocateEdgeActor();
        auto readContext = std::make_shared<TReadContext>(TTestStoragesManager::GetInstance(), std::make_shared<TMockDataAccessorsManager>(edge),
            std::make_shared<NColumnFetching::TColumnDataManager>(edge),
            NColumnShard::TConcreteScanCounters(NColumnShard::TScanCounters(), nullptr), readMetadata, edge, edge, TComputeShardingPolicy(), 0,
            NConveyorComposite::TCPULimitsConfig(), nullptr);

        auto portion = NTest::MakeTestCompactedPortion(TInternalPathId::FromRawValue(1), 1, 1, 2, 10, TSnapshot(1, 1), std::nullopt);
        const TString bloomPayload(BloomPayloadBytes, 'b');
        const TString unrelatedPayload(UnrelatedPayloadBytes, 'u');
        std::vector<TIndexChunk> indexChunks;
        indexChunks.emplace_back(BloomIndexId, 0, 10, bloomPayload.size(), bloomPayload);
        indexChunks.emplace_back(UnrelatedIndexId, 0, 10, unrelatedPayload.size(), unrelatedPayload);
        auto accessor =
            std::make_shared<TPortionDataAccessor>(portion, std::vector<TUnifiedBlobId>{}, std::vector<TColumnRecord>{}, indexChunks, false);

        ui64 equalsMemory = 0;
        ui64 greaterMemory = 0;
        ui64 emptyMemory = 0;
        bool finished = false;
        runtime.Register(new TReserveProbe(readContext, portion, accessor, schema, &equalsMemory, &greaterMemory, &emptyMemory, &finished));
        NActors::TDispatchOptions options;
        options.CustomFinalCondition = [&finished]() {
            return finished;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(5));

        UNIT_ASSERT(finished);
        UNIT_ASSERT_VALUES_EQUAL(equalsMemory, BloomPayloadBytes);
        UNIT_ASSERT_VALUES_EQUAL(greaterMemory, 0);
        UNIT_ASSERT_VALUES_EQUAL(emptyMemory, 0);
    }
}
