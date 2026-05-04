#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/duplicates/manager.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/duplicates/events.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/duplicates/filters.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/common/description.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/columns_set.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_portion.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/snapshot_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/scheme/common/cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/schema_version.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/column_fetching/manager.h>
#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/general_cache/usage/events.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NReader;
using namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering;

namespace {

void InitializeRuntimeWithLogging(NActors::TTestActorRuntimeBase& runtime) {
    runtime.Initialize();
    // Register TX_COLUMNSHARD_SCAN component in test runtime
    runtime.GetLogSettings(0)->Append(
        NKikimrServices::TX_COLUMNSHARD_SCAN,
        NKikimrServices::TX_COLUMNSHARD_SCAN + 1,
        [](NActors::NLog::EComponent) -> const TString& {
            static TString name = "TX_COLUMNSHARD_SCAN";
            return name;
        }
    );
    runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_TRACE);
}

std::shared_ptr<arrow::Schema> GetTestPKSchema() {
    static auto schema = arrow::schema({ arrow::field("pk", arrow::uint64()) });
    return schema;
}

std::shared_ptr<arrow::RecordBatch> MakePKBatch(const std::vector<ui64>& keys) {
    arrow::UInt64Builder builder;
    AFL_VERIFY(builder.AppendValues(keys).ok());
    auto array = builder.Finish().ValueOrDie();
    return arrow::RecordBatch::Make(GetTestPKSchema(), keys.size(), { array });
}

TIndexInfo MakeTestIndexInfo() {
    THashMap<ui32, NTable::TColumn> columns = {
        { 0, NTable::TColumn("pk", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "") }
    };
    std::vector<ui32> pkIds = { 0 };
    for (ui64 i = 0; i < pkIds.size(); ++i) {
        TValidator::CheckNotNull(columns.FindPtr(pkIds[i]))->KeyOrder = i;
    }
    return TIndexInfo::BuildDefault(1, TTestStoragesManager::GetInstance(), columns, pkIds);
}

std::shared_ptr<TPortionInfo> MakeTestPortion(
    ui64 portionId, ui64 startKey, ui64 endKey, ui32 recordsCount)
{
    auto batch = MakePKBatch({ startKey, endKey });
    TString serialized = NArrow::SerializeBatchNoCompression(batch);
    TIndexInfo indexInfo = MakeTestIndexInfo();

    NKikimrTxColumnShard::TIndexPortionMeta metaProto;
    metaProto.SetIsCompacted(true);
    metaProto.SetPrimaryKeyBorders(serialized);
    metaProto.MutableRecordSnapshotMin()->SetPlanStep(1);
    metaProto.MutableRecordSnapshotMin()->SetTxId(1);
    metaProto.MutableRecordSnapshotMax()->SetPlanStep(1);
    metaProto.MutableRecordSnapshotMax()->SetTxId(1);
    metaProto.SetDeletionsCount(0);
    metaProto.SetCompactionLevel(0);
    metaProto.SetRecordsCount(recordsCount);
    metaProto.SetColumnRawBytes(100);
    metaProto.SetColumnBlobBytes(100);
    metaProto.SetIndexRawBytes(0);
    metaProto.SetIndexBlobBytes(0);
    metaProto.SetNumSlices(1);
    metaProto.MutableCompactedPortion()->MutableAppearanceSnapshot()->SetPlanStep(1);
    metaProto.MutableCompactedPortion()->MutableAppearanceSnapshot()->SetTxId(1);

    TPortionMetaConstructor metaConstructor;
    TFakeGroupSelector groupSelector;
    AFL_VERIFY(metaConstructor.LoadMetadata(metaProto, indexInfo, groupSelector));

    TCompactedPortionInfoConstructor constructor(
        TInternalPathId::FromRawValue(1), portionId);
    constructor.SetSchemaVersion(1);
    constructor.SetAppearanceSnapshot(TSnapshot(1, 1));
    constructor.MutableMeta() = metaConstructor;
    return constructor.Build();
}

class TTestFilterSubscriber : public IFilterSubscriber {
public:
    bool FilterReady = false;
    bool Failed = false;
    TString FailureReason;
    NArrow::TColumnFilter ReceivedFilter =
        NArrow::TColumnFilter::BuildAllowFilter();

    void OnFilterReady(NArrow::TColumnFilter&& filter) override {
        FilterReady = true;
        ReceivedFilter = std::move(filter);
    }
    void OnFailure(const TString& reason) override {
        Failed = true;
        FailureReason = reason;
    }
};

NActors::IEventHandle* MakeFilterRequestHandle(
    const NActors::TActorId& recipient,
    const NActors::TActorId& sender,
    ui64 portionId, ui64 recordsCount,
    const std::shared_ptr<IFilterSubscriber>& subscriber)
{
    auto dummyBatch = MakePKBatch({ 0 });
    NArrow::TSimpleRow dummyRow(dummyBatch, 0);
    auto abortionFlag = std::make_shared<TAtomicCounter>(0);
    auto* ev = new TEvRequestFilter(
        dummyRow, dummyRow, portionId, recordsCount,
        TSnapshot(1, 1), subscriber,
        std::shared_ptr<const TAtomicCounter>(abortionFlag));
    return new NActors::IEventHandle(recipient, sender, ev);
}

using TGlobalColumnAddr =
    NOlap::NGeneralCache::TGlobalColumnAddress;
using TColumnDataCachePolicy =
    NOlap::NGeneralCache::TColumnDataCachePolicy;
using TColumnDataMap =
    THashMap<TGlobalColumnAddr,
             std::shared_ptr<NArrow::NAccessor::IChunkedArray>>;

class TMockDataAccessorsManager
    : public NDataAccessorControl::IDataAccessorsManager {
private:
    void DoAskData(
        const std::shared_ptr<TDataAccessorsRequest>& request) override
    {
        auto subscriber = request->ExtractSubscriber();
        TDataAccessorsResult result;
        subscriber->OnResult(request->GetRequestId(),
                             std::move(result));
    }
    void DoAddPortion(
        const std::shared_ptr<TPortionDataAccessor>&) override {}
    void DoRemovePortion(const TPortionInfo::TConstPtr&) override {}

public:
    TMockDataAccessorsManager(const NActors::TActorId& tabletActorId)
        : IDataAccessorsManager(tabletActorId) {}
};

class TMockColumnDataCacheService
    : public NActors::TActor<TMockColumnDataCacheService> {
    using TBase = NActors::TActor<TMockColumnDataCacheService>;
    TColumnDataMap Data;

public:
    TMockColumnDataCacheService(TColumnDataMap&& data)
        : TBase(&TMockColumnDataCacheService::StateWork)
        , Data(std::move(data)) {}

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::NGeneralCache::NPublic::
                      TEvents<TColumnDataCachePolicy>::TEvAskData,
                  Handle);
            default:
                break;
        }
    }

    void Handle(
        NKikimr::NGeneralCache::NPublic::
            TEvents<TColumnDataCachePolicy>::TEvAskData::TPtr& ev,
        const NActors::TActorContext&)
    {
        auto callback = ev->Get()->ExtractCallback();
        auto addresses = ev->Get()->ExtractAddresses();

        TColumnDataMap result;
        THashSet<TGlobalColumnAddr> removed;
        for (const auto& addr : addresses) {
            auto it = Data.find(addr);
            if (it != Data.end()) {
                result.emplace(addr, it->second);
            }
        }
        THashMap<TGlobalColumnAddr, TString> errors;
        NKikimr::NGeneralCache::NPublic::
            TErrorAddresses<TColumnDataCachePolicy>
                errorAddresses(std::move(errors));
        callback->OnResultReady(std::move(result),
                                std::move(removed),
                                std::move(errorAddresses));
    }
};

std::shared_ptr<NArrow::NAccessor::IChunkedArray> MakeUInt64Column(
    const std::vector<ui64>& values)
{
    arrow::UInt64Builder builder;
    AFL_VERIFY(builder.AppendValues(values).ok());
    return std::make_shared<NArrow::NAccessor::TTrivialArray>(
        builder.Finish().ValueOrDie());
}

void RegisterColumnData(
    TColumnDataMap& store,
    const NActors::TActorId& tabletActorId, ui64 portionId,
    const std::vector<ui64>& pkValues,
    const std::vector<ui64>& planStepValues,
    const std::vector<ui64>& txIdValues,
    const std::vector<ui64>& writeIdValues)
{
    TPortionAddress addr(TInternalPathId::FromRawValue(1), portionId);
    store.emplace(TGlobalColumnAddr(tabletActorId, addr, 0),
                  MakeUInt64Column(pkValues));
    store.emplace(
        TGlobalColumnAddr(tabletActorId, addr,
            NPortion::TSpecialColumns::SPEC_COL_PLAN_STEP_INDEX),
        MakeUInt64Column(planStepValues));
    store.emplace(
        TGlobalColumnAddr(tabletActorId, addr,
            NPortion::TSpecialColumns::SPEC_COL_TX_ID_INDEX),
        MakeUInt64Column(txIdValues));
    store.emplace(
        TGlobalColumnAddr(tabletActorId, addr,
            NPortion::TSpecialColumns::SPEC_COL_WRITE_ID_INDEX),
        MakeUInt64Column(writeIdValues));
}

std::shared_ptr<TReadContext> MakeTestReadContext(
    const TSnapshot& requestSnapshot,
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>&
        dataAccessorsManager,
    const std::shared_ptr<NColumnFetching::TColumnDataManager>&
        columnDataManager,
    const NActors::TActorId& scanActorId,
    const ERequestSorting sorting = ERequestSorting::NONE)
{
    auto indexInfoCache =
        std::make_shared<TObjectCache<TSchemaVersionId, TIndexInfo>>();
    auto entryGuard =
        indexInfoCache->Upsert(TSchemaVersionId(1, 1),
                               MakeTestIndexInfo());
    auto versionedIndex = std::make_shared<TVersionedIndex>();
    versionedIndex->AddIndex(TSnapshot(1, 1), std::move(entryGuard));

    TReadDescription readDesc(
        0, requestSnapshot, sorting);
    readDesc.SetScanCursor(nullptr);

    auto readMetadata = std::make_shared<NTrivial::TReadMetadata>(
        versionedIndex, readDesc);
    readMetadata->SetPKRangesFilter(readDesc.PKRangesFilter);

    NColumnShard::TConcreteScanCounters scanCounters(
        NColumnShard::TScanCounters(), nullptr);

    return std::make_shared<TReadContext>(
        TTestStoragesManager::GetInstance(),
        dataAccessorsManager, columnDataManager,
        scanCounters, readMetadata,
        scanActorId, scanActorId, scanActorId,
        TComputeShardingPolicy(), 0,
        NConveyorComposite::TCPULimitsConfig());
}

struct TManagerSetupResult {
    NActors::TActorId ManagerId;
    std::shared_ptr<NTrivial::TSpecialReadContext> Context;
};

class TManagerSetupActor
    : public NActors::TActorBootstrapped<TManagerSetupActor> {
    std::shared_ptr<TReadContext> ReadCtx;
    std::deque<std::shared_ptr<TPortionInfo>> Portions;
    TManagerSetupResult* Result;

public:
    TManagerSetupActor(
        std::shared_ptr<TReadContext> readCtx,
        std::deque<std::shared_ptr<TPortionInfo>> portions,
        TManagerSetupResult* result)
        : ReadCtx(std::move(readCtx))
        , Portions(std::move(portions))
        , Result(result) {}

    void Bootstrap() {
        Result->Context =
            std::make_shared<NTrivial::TSpecialReadContext>(ReadCtx);
        auto* manager =
            new TDuplicateManager(*Result->Context, Portions);
        Result->ManagerId = RegisterWithSameMailbox(manager);
        PassAway();
    }
};

NActors::TActorId SetupDuplicateManager(
    NActors::TTestActorRuntimeBase& runtime,
    const TSnapshot& requestSnapshot,
    const std::deque<std::shared_ptr<TPortionInfo>>& portions,
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>&
        dataAccessorsManager,
    const std::shared_ptr<NColumnFetching::TColumnDataManager>&
        columnDataManager,
    const NActors::TActorId& scanActorId,
    TManagerSetupResult& result,
    const ERequestSorting sorting = ERequestSorting::NONE)
{
    auto readContext = MakeTestReadContext(
        requestSnapshot, dataAccessorsManager,
        columnDataManager, scanActorId, sorting);
    runtime.Register(new TManagerSetupActor(
        readContext, portions, &result));

    NActors::TDispatchOptions options;
    options.CustomFinalCondition =
        [&result]() { return !!result.ManagerId; };
    runtime.DispatchEvents(options, TDuration::Seconds(1));
    AFL_VERIFY(result.ManagerId);
    return result.ManagerId;
}

}  // namespace

Y_UNIT_TEST_SUITE(TDuplicateManagerActorTests) {

Y_UNIT_TEST(SingleExclusivePortionGetsAllowAllFilter) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(edgeActor);
    auto cdm =
        std::make_shared<NColumnFetching::TColumnDataManager>(edgeActor);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 10, 20, 100));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(1, 1), portions, dam, cdm, edgeActor, setup);

    auto subscriber = std::make_shared<TTestFilterSubscriber>();
    runtime.Send(
        MakeFilterRequestHandle(actorId, edgeActor, 1, 100, subscriber));

    NActors::TDispatchOptions options;
    options.CustomFinalCondition =
        [&subscriber]() { return subscriber->FilterReady; };
    runtime.DispatchEvents(options, TDuration::Seconds(1));

    UNIT_ASSERT_C(subscriber->FilterReady,
                  "subscriber should have received the filter");
    UNIT_ASSERT(!subscriber->Failed);
    UNIT_ASSERT(subscriber->ReceivedFilter.IsTotalAllowFilter());
    UNIT_ASSERT_VALUES_EQUAL(
        subscriber->ReceivedFilter.GetRecordsCountVerified(), 100);
}

Y_UNIT_TEST(MultipleExclusivePortionsEachGetsAllowAllFilter) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(edgeActor);
    auto cdm =
        std::make_shared<NColumnFetching::TColumnDataManager>(edgeActor);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 10, 20, 50));
    portions.push_back(MakeTestPortion(2, 30, 40, 75));
    portions.push_back(MakeTestPortion(3, 50, 60, 200));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(1, 1), portions, dam, cdm, edgeActor, setup);

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 50, sub1));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 75, sub2));
    runtime.Send(
        MakeFilterRequestHandle(actorId, edgeActor, 3, 200, sub3));

    NActors::TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return sub1->FilterReady && sub2->FilterReady &&
               sub3->FilterReady;
    };
    runtime.DispatchEvents(options, TDuration::Seconds(1));

    UNIT_ASSERT(sub1->FilterReady);
    UNIT_ASSERT(sub1->ReceivedFilter.IsTotalAllowFilter());
    UNIT_ASSERT_VALUES_EQUAL(
        sub1->ReceivedFilter.GetRecordsCountVerified(), 50);
    UNIT_ASSERT(sub2->FilterReady);
    UNIT_ASSERT(sub2->ReceivedFilter.IsTotalAllowFilter());
    UNIT_ASSERT_VALUES_EQUAL(
        sub2->ReceivedFilter.GetRecordsCountVerified(), 75);
    UNIT_ASSERT(sub3->FilterReady);
    UNIT_ASSERT(sub3->ReceivedFilter.IsTotalAllowFilter());
    UNIT_ASSERT_VALUES_EQUAL(
        sub3->ReceivedFilter.GetRecordsCountVerified(), 200);
}

Y_UNIT_TEST(ExclusivePortionAmongOverlapping) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(edgeActor);
    auto cdm =
        std::make_shared<NColumnFetching::TColumnDataManager>(edgeActor);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 10, 25, 50));
    portions.push_back(MakeTestPortion(2, 20, 30, 60));
    portions.push_back(MakeTestPortion(3, 40, 50, 80));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(1, 1), portions, dam, cdm, edgeActor, setup);

    auto subscriber = std::make_shared<TTestFilterSubscriber>();
    runtime.Send(
        MakeFilterRequestHandle(actorId, edgeActor, 3, 80, subscriber));

    NActors::TDispatchOptions options;
    options.CustomFinalCondition =
        [&subscriber]() { return subscriber->FilterReady; };
    runtime.DispatchEvents(options, TDuration::Seconds(1));

    UNIT_ASSERT(subscriber->FilterReady);
    UNIT_ASSERT(!subscriber->Failed);
    UNIT_ASSERT(subscriber->ReceivedFilter.IsTotalAllowFilter());
    UNIT_ASSERT_VALUES_EQUAL(
        subscriber->ReceivedFilter.GetRecordsCountVerified(), 80);
}

Y_UNIT_TEST(ExclusivePortionUsesPortionRecordsCount) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(edgeActor);
    auto cdm =
        std::make_shared<NColumnFetching::TColumnDataManager>(edgeActor);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 100, 200, 42));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(1, 1), portions, dam, cdm, edgeActor, setup);

    auto subscriber = std::make_shared<TTestFilterSubscriber>();
    runtime.Send(
        MakeFilterRequestHandle(actorId, edgeActor, 1, 999, subscriber));

    NActors::TDispatchOptions options;
    options.CustomFinalCondition =
        [&subscriber]() { return subscriber->FilterReady; };
    runtime.DispatchEvents(options, TDuration::Seconds(1));

    UNIT_ASSERT(subscriber->FilterReady);
    UNIT_ASSERT_VALUES_EQUAL(
        subscriber->ReceivedFilter.GetRecordsCountVerified(), 42);
}

Y_UNIT_TEST(PoisonStopsActor) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(edgeActor);
    auto cdm =
        std::make_shared<NColumnFetching::TColumnDataManager>(edgeActor);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 10, 20, 100));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(1, 1), portions, dam, cdm, edgeActor, setup);

    auto subscriberBefore = std::make_shared<TTestFilterSubscriber>();
    runtime.Send(MakeFilterRequestHandle(
        actorId, edgeActor, 1, 100, subscriberBefore));

    NActors::TDispatchOptions options;
    options.CustomFinalCondition =
        [&subscriberBefore]() { return subscriberBefore->FilterReady; };
    runtime.DispatchEvents(options, TDuration::Seconds(1));
    UNIT_ASSERT(subscriberBefore->FilterReady);

    auto subscriberAfter = std::make_shared<TTestFilterSubscriber>();
    runtime.Send(new NActors::IEventHandle(
        actorId, edgeActor, new NActors::TEvents::TEvPoison()), 0);
    runtime.Send(MakeFilterRequestHandle(
        actorId, edgeActor, 1, 100, subscriberAfter));

    NActors::TDispatchOptions poisonOptions;
    poisonOptions.CustomFinalCondition = [&]() { return true; };
    runtime.DispatchEvents(poisonOptions);

    UNIT_ASSERT(!subscriberAfter->FilterReady);
}

Y_UNIT_TEST(OverlappingPortionsMergeDeduplication) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam =
        std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm =
        std::make_shared<NColumnFetching::TColumnDataManager>(
            tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 3, 3));
    portions.push_back(MakeTestPortion(2, 2, 4, 3));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId,
        1, {1, 2, 3}, {10, 10, 10}, {1, 1, 1}, {0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId,
        2, {2, 3, 4}, {20, 20, 20}, {1, 1, 1}, {0, 0, 0});

    NActors::TActorId columnCacheServiceId =
        NColumnFetching::TGeneralCache::MakeServiceId(
            runtime.GetNodeId(0));
    runtime.RegisterService(columnCacheServiceId,
        runtime.Register(
            new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm,
        tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(
        MakeFilterRequestHandle(actorId, edgeActor, 1, 3, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition =
        [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    runtime.Send(
        MakeFilterRequestHandle(actorId, edgeActor, 2, 3, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition =
        [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady,
        "P1 filter should be ready; failure=" << sub1->FailureReason);
    UNIT_ASSERT_C(!sub1->Failed,
        "P1 should not fail: " << sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady,
        "P2 filter should be ready; failure=" << sub2->FailureReason);
    UNIT_ASSERT_C(!sub2->Failed,
        "P2 should not fail: " << sub2->FailureReason);

    auto p1Filter = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(p1Filter.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(p1Filter[0], true);
    UNIT_ASSERT_VALUES_EQUAL(p1Filter[1], false);
    UNIT_ASSERT_VALUES_EQUAL(p1Filter[2], false);

    auto p2Filter = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(p2Filter.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(p2Filter[0], true);
    UNIT_ASSERT_VALUES_EQUAL(p2Filter[1], true);
    UNIT_ASSERT_VALUES_EQUAL(p2Filter[2], true);
}

Y_UNIT_TEST(BoundaryOverlapSingleKey) {
    // P1=[1,5] pk={1,3,5} version=10  P2=[5,9] pk={5,7,9} version=20
    // Only key 5 overlaps. P2 wins on key 5.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 5, 3));
    portions.push_back(MakeTestPortion(2, 5, 9, 3));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1, 3, 5}, {10, 10, 10}, {1, 1, 1}, {0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {5, 7, 9}, {20, 20, 20}, {1, 1, 1}, {0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 3, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition = [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 3, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);

    // P1: key 1 kept, key 3 kept, key 5 deduped by newer P2
    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);

    // P2: all keys kept (key 5 is newer, keys 7,9 unique)
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], true);
}

Y_UNIT_TEST(ReverseRequestOrder) {
    // Same overlap as OverlappingPortionsMergeDeduplication, but request P2 first.
    // P1: pk={1,2,3} version=10 (older)
    // P2: pk={2,3,4} version=20 (newer)
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 3, 3));
    portions.push_back(MakeTestPortion(2, 2, 4, 3));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1, 2, 3}, {10, 10, 10}, {1, 1, 1}, {0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {2, 3, 4}, {20, 20, 20}, {1, 1, 1}, {0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub1 = std::make_shared<TTestFilterSubscriber>();

    // Request P2 FIRST, then P1
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 3, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 3, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition = [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);

    // P1: key 1 unique, keys 2,3 deduped by newer P2
    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);

    // P2: all keys kept
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], true);
}

Y_UNIT_TEST(FullContainment) {
    // P1=[1,10] pk={1,3,5,7,10} version=10 (outer, older)
    // P2=[3,7]  pk={3,5,7}       version=20 (inner, newer)
    // Keys 3,5,7 overlap. P2 wins.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 10, 5));
    portions.push_back(MakeTestPortion(2, 3, 7, 3));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1, 3, 5, 7, 10}, {10, 10, 10, 10, 10}, {1, 1, 1, 1, 1}, {0, 0, 0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {3, 5, 7}, {20, 20, 20}, {1, 1, 1}, {0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 5, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition = [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 3, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);

    // P1: keys 1,10 unique (kept); keys 3,5,7 deduped by newer P2
    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 5u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[3], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[4], true);

    // P2: all keys kept (newer)
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], true);
}

Y_UNIT_TEST(IdenticalKeyRanges) {
    // P1 and P2 have the exact same key range and same keys.
    // P1: pk={1,2,3} version=10 (older) → all deduped
    // P2: pk={1,2,3} version=20 (newer) → all kept
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 3, 3));
    portions.push_back(MakeTestPortion(2, 1, 3, 3));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1, 2, 3}, {10, 10, 10}, {1, 1, 1}, {0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {1, 2, 3}, {20, 20, 20}, {1, 1, 1}, {0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 3, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition = [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 3, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);

    // P1: all deduped by newer P2
    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);

    // P2: all kept (newer)
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], true);
}

Y_UNIT_TEST(ThreePortionsChainOverlap) {
    // P1=[1,4] pk={1,2,3,4} version=10
    // P2=[3,6] pk={3,4,5,6} version=20
    // P3=[5,8] pk={5,6,7,8} version=30
    // Chain: P1-P2 overlap on 3,4; P2-P3 overlap on 5,6.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 4, 4));
    portions.push_back(MakeTestPortion(2, 3, 6, 4));
    portions.push_back(MakeTestPortion(3, 5, 8, 4));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1, 2, 3, 4}, {10, 10, 10, 10}, {1, 1, 1, 1}, {0, 0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {3, 4, 5, 6}, {20, 20, 20, 20}, {1, 1, 1, 1}, {0, 0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {5, 6, 7, 8}, {30, 30, 30, 30}, {1, 1, 1, 1}, {0, 0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 4, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition = [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 4, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 4, sub3));
    NActors::TDispatchOptions opt3;
    opt3.CustomFinalCondition = [&]() { return sub3->FilterReady || sub3->Failed; };
    runtime.DispatchEvents(opt3, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);

    // P1: keys 1,2 unique; keys 3,4 deduped by P2
    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[3], false);

    // P2: keys 3,4 kept (newer than P1); keys 5,6 deduped by P3
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f2[3], false);

    // P3: all keys kept (newest or unique)
    auto f3 = sub3->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f3.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f3[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[2], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[3], true);
}

Y_UNIT_TEST(ThreePortionsRequestMiddleFirst) {
    // Same layout as ThreePortionsChainOverlap, but request P2 first.
    // P1=[1,4], P2=[3,6], P3=[5,8]
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 4, 4));
    portions.push_back(MakeTestPortion(2, 3, 6, 4));
    portions.push_back(MakeTestPortion(3, 5, 8, 4));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1, 2, 3, 4}, {10, 10, 10, 10}, {1, 1, 1, 1}, {0, 0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {3, 4, 5, 6}, {20, 20, 20, 20}, {1, 1, 1, 1}, {0, 0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {5, 6, 7, 8}, {30, 30, 30, 30}, {1, 1, 1, 1}, {0, 0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    // Request P2 first (middle portion)
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 4, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 4, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition = [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 4, sub3));
    NActors::TDispatchOptions opt3;
    opt3.CustomFinalCondition = [&]() { return sub3->FilterReady || sub3->Failed; };
    runtime.DispatchEvents(opt3, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);

    // Results must be the same regardless of request order
    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[3], false);

    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f2[3], false);

    auto f3 = sub3->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f3.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f3[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[2], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[3], true);
}

Y_UNIT_TEST(OlderPortionWinsOnTxIdTiebreak) {
    // Same planStep, but different txId. Higher txId wins.
    // P1: pk={1,2,3} planStep=10 txId=5
    // P2: pk={2,3,4} planStep=10 txId=9
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 3, 3));
    portions.push_back(MakeTestPortion(2, 2, 4, 3));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1, 2, 3}, {10, 10, 10}, {5, 5, 5}, {0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {2, 3, 4}, {10, 10, 10}, {9, 9, 9}, {0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 3, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition = [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 3, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);

    // P1: key 1 unique; keys 2,3 deduped by P2 (higher txId)
    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);

    // P2: all kept (higher txId wins on overlap, key 4 unique)
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], true);
}

Y_UNIT_TEST(SingleKeyPortionSharedBoundary) {
    // Borders after construction:
    //   key 5: Start={P1,P2}, Finish={P1}
    //   key 10: Start={},     Finish={P2}
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 5, 5, 1));   // single-key portion
    portions.push_back(MakeTestPortion(2, 5, 10, 3));   // starts at same key

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {5}, {10, }, {1, }, {0, });
    RegisterColumnData(columnStore, tabletActorId, 2,
        {5, 7, 10}, {20, 20, 20}, {1, 1, 1}, {0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 1, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition = [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 3, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);

    // P1: single key 5 deduped by newer P2 (version 20 > 10)
    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], false);

    // P2: key 5 kept (newer), keys 7,10 unique
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], true);
}

Y_UNIT_TEST(MixedExclusiveAndOverlapping) {
    // P1=[1,3] exclusive, P2=[5,8] overlaps with P3=[7,10]
    // P1 should get allow-all. P2/P3 go through merge.
    // P2: pk={5,6,7,8} version=10
    // P3: pk={7,8,9,10} version=20
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 3, 2));
    portions.push_back(MakeTestPortion(2, 5, 8, 4));
    portions.push_back(MakeTestPortion(3, 7, 10, 4));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 2,
        {5, 6, 7, 8}, {10, 10, 10, 10}, {1, 1, 1, 1}, {0, 0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {7, 8, 9, 10}, {20, 20, 20, 20}, {1, 1, 1, 1}, {0, 0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto subExcl = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    // P1 is exclusive — should get allow-all immediately
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 2, subExcl));
    NActors::TDispatchOptions optE;
    optE.CustomFinalCondition = [&]() { return subExcl->FilterReady; };
    runtime.DispatchEvents(optE, TDuration::Seconds(1));

    UNIT_ASSERT(subExcl->FilterReady);
    UNIT_ASSERT(subExcl->ReceivedFilter.IsTotalAllowFilter());
    UNIT_ASSERT_VALUES_EQUAL(subExcl->ReceivedFilter.GetRecordsCountVerified(), 2);

    // Now request overlapping portions
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 4, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 4, sub3));
    NActors::TDispatchOptions opt3;
    opt3.CustomFinalCondition = [&]() { return sub3->FilterReady || sub3->Failed; };
    runtime.DispatchEvents(opt3, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);

    // P2: keys 5,6 unique; keys 7,8 deduped by P3
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f2[3], false);

    // P3: all kept
    auto f3 = sub3->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f3.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f3[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[2], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[3], true);
}

Y_UNIT_TEST(ManyUpdatesDeleteAllFilterCoversAllRows) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 10, 10));
    portions.push_back(MakeTestPortion(2, 1, 10, 10));
    portions.push_back(MakeTestPortion(3, 1, 10, 10));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1,2,3,4,5,6,7,8,9,10},
        {10,10,10,10,10,10,10,10,10,10},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {1,2,3,4,5,6,7,8,9,10},
        {20,20,20,20,20,20,20,20,20,20},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {1,2,3,4,5,6,7,8,9,10},
        {30,30,30,30,30,30,30,30,30,30},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 10, sub1));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 10, sub2));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 10, sub3));

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        return (sub1->FilterReady || sub1->Failed) &&
               (sub2->FilterReady || sub2->Failed) &&
               (sub3->FilterReady || sub3->Failed);
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);

    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f1.size(), 10u,
        "P1 filter must cover all 10 records but got " << f1.size());
    for (ui32 i = 0; i < f1.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f1[i], false,
            "P1 row " << i << " must be deduped (newer P3 exists)");
    }

    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f2.size(), 10u,
        "P2 filter must cover all 10 records but got " << f2.size());
    for (ui32 i = 0; i < f2.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f2[i], false,
            "P2 row " << i << " must be deduped (newer P3 exists)");
    }

    auto f3 = sub3->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f3.size(), 10u,
        "P3 filter must cover all 10 records but got " << f3.size());
    for (ui32 i = 0; i < f3.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f3[i], true,
            "P3 row " << i << " must be kept (newest version)");
    }
}

Y_UNIT_TEST(DeletePortionRequestedFirst) {
    // Request the newest (delete) portion FIRST, then the older ones.
    // In production the scan pipeline may request newer portions before older.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 10, 10));
    portions.push_back(MakeTestPortion(2, 1, 10, 10));
    portions.push_back(MakeTestPortion(3, 1, 10, 10));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1,2,3,4,5,6,7,8,9,10},
        {10,10,10,10,10,10,10,10,10,10},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {1,2,3,4,5,6,7,8,9,10},
        {20,20,20,20,20,20,20,20,20,20},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {1,2,3,4,5,6,7,8,9,10},
        {30,30,30,30,30,30,30,30,30,30},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 10, sub3));
    NActors::TDispatchOptions opt3;
    opt3.CustomFinalCondition = [&]() { return sub3->FilterReady || sub3->Failed; };
    runtime.DispatchEvents(opt3, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 10, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition = [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 10, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);

    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f1.size(), 10u,
        "P1 filter must cover all 10 records but got " << f1.size());
    for (ui32 i = 0; i < f1.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f1[i], false,
            "P1 row " << i << " must be deduped");
    }

    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f2.size(), 10u,
        "P2 filter must cover all 10 records but got " << f2.size());
    for (ui32 i = 0; i < f2.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f2[i], false,
            "P2 row " << i << " must be deduped");
    }

    auto f3 = sub3->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f3.size(), 10u,
        "P3 filter must cover all 10 records but got " << f3.size());
    for (ui32 i = 0; i < f3.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f3[i], true,
            "P3 row " << i << " must be kept (newest)");
    }
}

Y_UNIT_TEST(ManyNarrowPortionsBatchSplit) {
    // 12 single-key portions at keys 1..12, plus a wide portion [1,12].
    // This creates 12+ intervals, and the wide portion plus 12 narrow ones
    // = 13 unique portions > BATCH_PORTIONS_COUNT_SOFT_LIMIT (10),
    // forcing ScheduleNext to split into multiple batches.
    // All requests sent simultaneously.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    const ui64 N = 12;
    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(100, 1, N, N));

    std::vector<ui64> widePk, widePlanStep, wideTxId, wideWriteId;
    for (ui64 k = 1; k <= N; ++k) {
        widePk.push_back(k);
        widePlanStep.push_back(10);
        wideTxId.push_back(1);
        wideWriteId.push_back(0);
    }

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 100,
        widePk, widePlanStep, wideTxId, wideWriteId);

    for (ui64 k = 1; k <= N; ++k) {
        portions.push_back(MakeTestPortion(k, k, k, 1));
        RegisterColumnData(columnStore, tabletActorId, k,
            {k}, {50}, {1}, {0});
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto subWide = std::make_shared<TTestFilterSubscriber>();
    std::vector<std::shared_ptr<TTestFilterSubscriber>> narrowSubs(N);

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 100, N, subWide));
    for (ui64 k = 0; k < N; ++k) {
        narrowSubs[k] = std::make_shared<TTestFilterSubscriber>();
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, k + 1, 1, narrowSubs[k]));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        if (!subWide->FilterReady && !subWide->Failed) return false;
        for (auto& s : narrowSubs) {
            if (!s->FilterReady && !s->Failed) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(15));

    UNIT_ASSERT_C(subWide->FilterReady && !subWide->Failed,
        subWide->FailureReason);
    auto fWide = subWide->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(fWide.size(), N,
        "Wide filter must cover " << N << " records but got " << fWide.size());
    for (ui32 i = 0; i < fWide.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(fWide[i], false,
            "Wide row " << i << " must be deduped by narrow portion v=50");
    }

    for (ui64 k = 0; k < N; ++k) {
        UNIT_ASSERT_C(narrowSubs[k]->FilterReady && !narrowSubs[k]->Failed,
            "Narrow P" << (k+1) << " failed: " << narrowSubs[k]->FailureReason);
        auto f = narrowSubs[k]->ReceivedFilter.BuildTrivialFilter();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), 1u,
            "Narrow P" << (k+1) << " filter must be 1 entry but got " << f.size());
        UNIT_ASSERT_VALUES_EQUAL_C(f[0], true,
            "Narrow P" << (k+1) << " must be kept (v=50 > v=10)");
    }
}

Y_UNIT_TEST(ManyUpdatesAllRequestsSimultaneous) {
    // Simulates test_many_updates: wide original, many narrow updates,
    // wide delete. ALL filter requests sent at once before any dispatch.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    const ui64 N = 10;
    const ui64 numUpdates = 8;

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, N, N));

    TColumnDataMap columnStore;
    {
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 1; k <= N; ++k) {
            pk.push_back(k); ps.push_back(10); tx.push_back(1); wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, 1, pk, ps, tx, wr);
    }

    for (ui64 u = 0; u < numUpdates; ++u) {
        ui64 key = (u % N) + 1;
        ui64 portionId = 100 + u;
        portions.push_back(MakeTestPortion(portionId, key, key, 1));
        RegisterColumnData(columnStore, tabletActorId, portionId,
            {key}, {20 + u}, {1}, {0});
    }

    ui64 deletePortion = 200;
    portions.push_back(MakeTestPortion(deletePortion, 1, N, N));
    {
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 1; k <= N; ++k) {
            pk.push_back(k); ps.push_back(50); tx.push_back(1); wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, deletePortion, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs;
    for (const auto& p : portions) {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs.push_back(s);
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, p->GetPortionId(),
            p->GetRecordsCount(), s));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s->FilterReady && !s->Failed) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(15));

    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        UNIT_ASSERT_C(subs[idx]->FilterReady && !subs[idx]->Failed,
            "Portion " << portions[idx]->GetPortionId()
            << " failed: " << subs[idx]->FailureReason);

        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        ui64 expected = portions[idx]->GetRecordsCount();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), expected,
            "Portion " << portions[idx]->GetPortionId()
            << " filter must cover " << expected
            << " records but got " << f.size());

        if (portions[idx]->GetPortionId() == deletePortion) {
            for (ui32 i = 0; i < f.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(f[i], true,
                    "Delete portion row " << i << " must be kept (newest)");
            }
        } else {
            for (ui32 i = 0; i < f.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(f[i], false,
                    "Portion " << portions[idx]->GetPortionId()
                    << " row " << i << " must be deduped by delete");
            }
        }
    }
}

Y_UNIT_TEST(ManyUpdatesWithNarrowPortionDeleteAll) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 10, 10));
    portions.push_back(MakeTestPortion(2, 6, 6, 1));
    portions.push_back(MakeTestPortion(3, 1, 10, 10));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1,2,3,4,5,6,7,8,9,10},
        {10,10,10,10,10,10,10,10,10,10},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {6}, {20}, {1}, {0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {1,2,3,4,5,6,7,8,9,10},
        {30,30,30,30,30,30,30,30,30,30},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 10, sub1));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 1, sub2));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 10, sub3));

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        return (sub1->FilterReady || sub1->Failed) &&
               (sub2->FilterReady || sub2->Failed) &&
               (sub3->FilterReady || sub3->Failed);
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);

    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f1.size(), 10u,
        "P1 filter must cover all 10 records but got " << f1.size());
    for (ui32 i = 0; i < f1.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f1[i], false,
            "P1 row " << i << " (pk=" << (i+1) << ") must be deduped");
    }

    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f2.size(), 1u,
        "P2 filter must cover 1 record but got " << f2.size());
    UNIT_ASSERT_VALUES_EQUAL(f2[0], false);

    auto f3 = sub3->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f3.size(), 10u,
        "P3 filter must cover all 10 records but got " << f3.size());
    for (ui32 i = 0; i < f3.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f3[i], true,
            "P3 row " << i << " must be kept (newest)");
    }
}

Y_UNIT_TEST(ManyUpdatesMultipleNarrowPortions) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 10, 10));
    portions.push_back(MakeTestPortion(2, 3, 3, 1));
    portions.push_back(MakeTestPortion(3, 6, 6, 1));
    portions.push_back(MakeTestPortion(4, 9, 9, 1));
    portions.push_back(MakeTestPortion(5, 1, 10, 10));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1,2,3,4,5,6,7,8,9,10},
        {10,10,10,10,10,10,10,10,10,10},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {3}, {20}, {1}, {0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {6}, {25}, {1}, {0});
    RegisterColumnData(columnStore, tabletActorId, 4,
        {9}, {28}, {1}, {0});
    RegisterColumnData(columnStore, tabletActorId, 5,
        {1,2,3,4,5,6,7,8,9,10},
        {30,30,30,30,30,30,30,30,30,30},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();
    auto sub4 = std::make_shared<TTestFilterSubscriber>();
    auto sub5 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 10, sub1));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 1, sub2));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 1, sub3));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 4, 1, sub4));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 5, 10, sub5));

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        return (sub1->FilterReady || sub1->Failed) &&
               (sub2->FilterReady || sub2->Failed) &&
               (sub3->FilterReady || sub3->Failed) &&
               (sub4->FilterReady || sub4->Failed) &&
               (sub5->FilterReady || sub5->Failed);
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);
    UNIT_ASSERT_C(sub4->FilterReady && !sub4->Failed, sub4->FailureReason);
    UNIT_ASSERT_C(sub5->FilterReady && !sub5->Failed, sub5->FailureReason);

    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f1.size(), 10u,
        "P1 filter must cover all 10 records but got " << f1.size());
    for (ui32 i = 0; i < f1.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f1[i], false,
            "P1 row " << i << " (pk=" << (i+1) << ") must be deduped");
    }

    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f2.size(), 1u,
        "P2 filter must cover 1 record but got " << f2.size());
    UNIT_ASSERT_VALUES_EQUAL(f2[0], false);

    auto f3 = sub3->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f3.size(), 1u,
        "P3 filter must cover 1 record but got " << f3.size());
    UNIT_ASSERT_VALUES_EQUAL(f3[0], false);

    auto f4 = sub4->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f4.size(), 1u,
        "P4 filter must cover 1 record but got " << f4.size());
    UNIT_ASSERT_VALUES_EQUAL(f4[0], false);

    auto f5 = sub5->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f5.size(), 10u,
        "P5 filter must cover all 10 records but got " << f5.size());
    for (ui32 i = 0; i < f5.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f5[i], true,
            "P5 row " << i << " must be kept (newest version)");
    }
}

Y_UNIT_TEST(MultipleWideOverlappingPortions) {
    // 5 wide portions [1,10], each with all 10 keys, increasing versions.
    // All requests sent simultaneously. Tests many-to-many deduplication.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    const ui32 portionCount = 5;
    const ui64 N = 10;
    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;

    for (ui32 p = 1; p <= portionCount; ++p) {
        portions.push_back(MakeTestPortion(p, 1, N, N));
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 1; k <= N; ++k) {
            pk.push_back(k);
            ps.push_back(p * 10);
            tx.push_back(1);
            wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, p, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs(portionCount);
    for (ui32 p = 0; p < portionCount; ++p) {
        subs[p] = std::make_shared<TTestFilterSubscriber>();
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, p + 1, N, subs[p]));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s->FilterReady && !s->Failed) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));

    for (ui32 p = 0; p < portionCount; ++p) {
        UNIT_ASSERT_C(subs[p]->FilterReady && !subs[p]->Failed,
            "P" << (p+1) << " failed: " << subs[p]->FailureReason);
        auto f = subs[p]->ReceivedFilter.BuildTrivialFilter();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), N,
            "P" << (p+1) << " filter must cover " << N
            << " records but got " << f.size());

        bool isNewest = (p + 1 == portionCount);
        for (ui32 i = 0; i < f.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(f[i], isNewest,
                "P" << (p+1) << " row " << i
                << (isNewest ? " must be kept" : " must be deduped"));
        }
    }
}

Y_UNIT_TEST(NarrowUpdatesDeleteAllReverseRequestOrder) {
    // Same as ManyUpdatesMultipleNarrowPortions but requests in REVERSE order:
    // newest (delete) first, then narrow updates, then original write.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 10, 10));
    portions.push_back(MakeTestPortion(2, 3, 3, 1));
    portions.push_back(MakeTestPortion(3, 6, 6, 1));
    portions.push_back(MakeTestPortion(4, 9, 9, 1));
    portions.push_back(MakeTestPortion(5, 1, 10, 10));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1,2,3,4,5,6,7,8,9,10},
        {10,10,10,10,10,10,10,10,10,10},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {3}, {20}, {1}, {0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {6}, {25}, {1}, {0});
    RegisterColumnData(columnStore, tabletActorId, 4,
        {9}, {28}, {1}, {0});
    RegisterColumnData(columnStore, tabletActorId, 5,
        {1,2,3,4,5,6,7,8,9,10},
        {30,30,30,30,30,30,30,30,30,30},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();
    auto sub4 = std::make_shared<TTestFilterSubscriber>();
    auto sub5 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 5, 10, sub5));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 4, 1, sub4));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 1, sub3));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 1, sub2));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 10, sub1));

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        return (sub1->FilterReady || sub1->Failed) &&
               (sub2->FilterReady || sub2->Failed) &&
               (sub3->FilterReady || sub3->Failed) &&
               (sub4->FilterReady || sub4->Failed) &&
               (sub5->FilterReady || sub5->Failed);
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);
    UNIT_ASSERT_C(sub4->FilterReady && !sub4->Failed, sub4->FailureReason);
    UNIT_ASSERT_C(sub5->FilterReady && !sub5->Failed, sub5->FailureReason);

    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f1.size(), 10u,
        "P1 filter must cover 10 records but got " << f1.size());
    for (ui32 i = 0; i < f1.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f1[i], false,
            "P1 row " << i << " must be deduped");
    }

    UNIT_ASSERT_VALUES_EQUAL(sub2->ReceivedFilter.BuildTrivialFilter().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(sub3->ReceivedFilter.BuildTrivialFilter().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(sub4->ReceivedFilter.BuildTrivialFilter().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(sub2->ReceivedFilter.BuildTrivialFilter()[0], false);
    UNIT_ASSERT_VALUES_EQUAL(sub3->ReceivedFilter.BuildTrivialFilter()[0], false);
    UNIT_ASSERT_VALUES_EQUAL(sub4->ReceivedFilter.BuildTrivialFilter()[0], false);

    auto f5 = sub5->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f5.size(), 10u,
        "P5 filter must cover 10 records but got " << f5.size());
    for (ui32 i = 0; i < f5.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f5[i], true,
            "P5 row " << i << " must be kept");
    }
}

Y_UNIT_TEST(WidePortionContainedInAnother) {
    // P1=[1,20] v=10 (20 records), P2=[5,15] v=20 (11 records),
    // P3=[1,20] v=30 (20 records, delete).
    // P2 is fully contained within P1 and P3.
    // All requests sent at once.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 20, 20));
    portions.push_back(MakeTestPortion(2, 5, 15, 11));
    portions.push_back(MakeTestPortion(3, 1, 20, 20));

    TColumnDataMap columnStore;
    {
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 1; k <= 20; ++k) {
            pk.push_back(k); ps.push_back(10); tx.push_back(1); wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, 1, pk, ps, tx, wr);
    }
    {
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 5; k <= 15; ++k) {
            pk.push_back(k); ps.push_back(20); tx.push_back(1); wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, 2, pk, ps, tx, wr);
    }
    {
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 1; k <= 20; ++k) {
            pk.push_back(k); ps.push_back(30); tx.push_back(1); wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, 3, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 20, sub1));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 11, sub2));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 20, sub3));

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        return (sub1->FilterReady || sub1->Failed) &&
               (sub2->FilterReady || sub2->Failed) &&
               (sub3->FilterReady || sub3->Failed);
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);

    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f1.size(), 20u,
        "P1 filter must cover 20 records but got " << f1.size());
    for (ui32 i = 0; i < f1.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f1[i], false,
            "P1 row " << i << " must be deduped by P3");
    }

    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f2.size(), 11u,
        "P2 filter must cover 11 records but got " << f2.size());
    for (ui32 i = 0; i < f2.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f2[i], false,
            "P2 row " << i << " must be deduped by P3");
    }

    auto f3 = sub3->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f3.size(), 20u,
        "P3 filter must cover 20 records but got " << f3.size());
    for (ui32 i = 0; i < f3.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f3[i], true,
            "P3 row " << i << " must be kept (newest)");
    }
}

Y_UNIT_TEST(ManyUpdatesEveryKeyHasNarrowUpdate) {
    // Every key 1..10 has a narrow single-key update portion.
    // P_orig=[1,10] v=10, P_upd_1=[1,1] v=20, ..., P_upd_10=[10,10] v=20,
    // P_del=[1,10] v=30.
    // 12 total portions. All requests sent at once.
    // This stresses interval creation with maximum number of borders.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    const ui64 N = 10;
    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;

    portions.push_back(MakeTestPortion(1, 1, N, N));
    {
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 1; k <= N; ++k) {
            pk.push_back(k); ps.push_back(10); tx.push_back(1); wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, 1, pk, ps, tx, wr);
    }

    for (ui64 k = 1; k <= N; ++k) {
        ui64 pid = 10 + k;
        portions.push_back(MakeTestPortion(pid, k, k, 1));
        RegisterColumnData(columnStore, tabletActorId, pid,
            {k}, {20}, {1}, {0});
    }

    ui64 delPid = 100;
    portions.push_back(MakeTestPortion(delPid, 1, N, N));
    {
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 1; k <= N; ++k) {
            pk.push_back(k); ps.push_back(30); tx.push_back(1); wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, delPid, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs;
    for (const auto& p : portions) {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs.push_back(s);
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, p->GetPortionId(),
            p->GetRecordsCount(), s));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s->FilterReady && !s->Failed) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(15));

    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        UNIT_ASSERT_C(subs[idx]->FilterReady && !subs[idx]->Failed,
            "Portion " << portions[idx]->GetPortionId()
            << " failed: " << subs[idx]->FailureReason);

        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        ui64 expected = portions[idx]->GetRecordsCount();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), expected,
            "Portion " << portions[idx]->GetPortionId()
            << " filter must cover " << expected
            << " records but got " << f.size());

        if (portions[idx]->GetPortionId() == delPid) {
            for (ui32 i = 0; i < f.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(f[i], true,
                    "Delete portion row " << i << " must be kept");
            }
        } else {
            for (ui32 i = 0; i < f.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(f[i], false,
                    "Portion " << portions[idx]->GetPortionId()
                    << " row " << i << " must be deduped");
            }
        }
    }
}

Y_UNIT_TEST(ManyUpdatesRealisticScenario) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    const ui64 numKeys = 20;
    const ui64 updatesPerKey = 10;
    const ui64 numBulk = 5;

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;
    ui64 nextPortionId = 1;
    ui64 nextVersion = 10;

    for (ui64 k = 0; k < numKeys; ++k) {
        ui64 pid = nextPortionId++;
        portions.push_back(MakeTestPortion(pid, k, k, 1));
        RegisterColumnData(columnStore, tabletActorId, pid,
            {k}, {nextVersion}, {1}, {0});
    }
    nextVersion += 10;

    for (ui64 u = 0; u < updatesPerKey; ++u) {
        for (ui64 k = 0; k < numKeys; ++k) {
            ui64 pid = nextPortionId++;
            portions.push_back(MakeTestPortion(pid, k, k, 1));
            RegisterColumnData(columnStore, tabletActorId, pid,
                {k}, {nextVersion + u}, {1}, {0});
        }
    }
    nextVersion += updatesPerKey + 10;

    for (ui64 b = 0; b < numBulk; ++b) {
        ui64 pid = nextPortionId++;
        portions.push_back(MakeTestPortion(pid, 0, numKeys - 1, numKeys));
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 0; k < numKeys; ++k) {
            pk.push_back(k);
            ps.push_back(nextVersion + b);
            tx.push_back(1);
            wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, pid, pk, ps, tx, wr);
    }

    ui64 newestBulkPid = nextPortionId - 1;

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(1000, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs;
    for (const auto& p : portions) {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs.push_back(s);
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, p->GetPortionId(),
            p->GetRecordsCount(), s));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s->FilterReady && !s->Failed) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(30));

    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        UNIT_ASSERT_C(subs[idx]->FilterReady && !subs[idx]->Failed,
            "Portion " << portions[idx]->GetPortionId()
            << " failed: " << subs[idx]->FailureReason);

        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        ui64 expected = portions[idx]->GetRecordsCount();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), expected,
            "Portion " << portions[idx]->GetPortionId()
            << " filter must cover " << expected
            << " records but got " << f.size());
    }

    THashMap<ui64, ui32> trueCountPerKey;
    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        const auto& p = portions[idx];
        ui64 startKey = p->IndexKeyStart().GetValue<ui64>(0).value();
        for (ui32 i = 0; i < f.size(); ++i) {
            if (f[i]) {
                trueCountPerKey[startKey + i]++;
            }
        }
    }

    ui64 totalTrue = 0;
    for (const auto& [key, count] : trueCountPerKey) {
        UNIT_ASSERT_VALUES_EQUAL_C(count, 1u,
            "Key " << key << " has " << count
            << " surviving versions (expected exactly 1)");
        totalTrue++;
    }
    UNIT_ASSERT_VALUES_EQUAL_C(totalTrue, numKeys,
        "Total surviving rows " << totalTrue << " != " << numKeys);

    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        if (portions[idx]->GetPortionId() == newestBulkPid) {
            auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
            for (ui32 i = 0; i < f.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(f[i], true,
                    "Newest bulk portion row " << i << " must be kept");
            }
        }
    }
}

Y_UNIT_TEST(ManyUpdatesRealisticDeleteAll) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    const ui64 numKeys = 20;
    const ui64 updatesPerKey = 10;
    const ui64 numBulk = 5;

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;
    ui64 nextPortionId = 1;
    ui64 nextVersion = 10;

    for (ui64 k = 0; k < numKeys; ++k) {
        ui64 pid = nextPortionId++;
        portions.push_back(MakeTestPortion(pid, k, k, 1));
        RegisterColumnData(columnStore, tabletActorId, pid,
            {k}, {nextVersion}, {1}, {0});
    }
    nextVersion += 10;

    for (ui64 u = 0; u < updatesPerKey; ++u) {
        for (ui64 k = 0; k < numKeys; ++k) {
            ui64 pid = nextPortionId++;
            portions.push_back(MakeTestPortion(pid, k, k, 1));
            RegisterColumnData(columnStore, tabletActorId, pid,
                {k}, {nextVersion + u}, {1}, {0});
        }
    }
    nextVersion += updatesPerKey + 10;

    for (ui64 b = 0; b < numBulk; ++b) {
        ui64 pid = nextPortionId++;
        portions.push_back(MakeTestPortion(pid, 0, numKeys - 1, numKeys));
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 0; k < numKeys; ++k) {
            pk.push_back(k);
            ps.push_back(nextVersion + b);
            tx.push_back(1);
            wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, pid, pk, ps, tx, wr);
    }
    nextVersion += numBulk + 10;

    ui64 deletePid = nextPortionId++;
    portions.push_back(MakeTestPortion(deletePid, 0, numKeys - 1, numKeys));
    {
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 0; k < numKeys; ++k) {
            pk.push_back(k);
            ps.push_back(nextVersion);
            tx.push_back(1);
            wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, deletePid, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(1000, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs;
    for (const auto& p : portions) {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs.push_back(s);
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, p->GetPortionId(),
            p->GetRecordsCount(), s));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s->FilterReady && !s->Failed) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(30));

    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        UNIT_ASSERT_C(subs[idx]->FilterReady && !subs[idx]->Failed,
            "Portion " << portions[idx]->GetPortionId()
            << " failed: " << subs[idx]->FailureReason);

        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        ui64 expected = portions[idx]->GetRecordsCount();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), expected,
            "Portion " << portions[idx]->GetPortionId()
            << " filter must cover " << expected
            << " records but got " << f.size());
    }

    THashMap<ui64, ui32> trueCountPerKey;
    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        const auto& p = portions[idx];
        ui64 startKey = p->IndexKeyStart().GetValue<ui64>(0).value();
        for (ui32 i = 0; i < f.size(); ++i) {
            if (f[i]) {
                trueCountPerKey[startKey + i]++;
            }
        }
    }

    ui64 totalTrue = 0;
    for (const auto& [key, count] : trueCountPerKey) {
        UNIT_ASSERT_VALUES_EQUAL_C(count, 1u,
            "Key " << key << " has " << count
            << " surviving versions after delete-all (expected 1)");
        totalTrue++;
    }
    UNIT_ASSERT_VALUES_EQUAL_C(totalTrue, numKeys,
        "Total surviving rows " << totalTrue << " != " << numKeys);

    {
        ui32 delIdx = subs.size() - 1;
        UNIT_ASSERT_VALUES_EQUAL(portions[delIdx]->GetPortionId(), deletePid);
        auto f = subs[delIdx]->ReceivedFilter.BuildTrivialFilter();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), numKeys,
            "Delete portion filter size " << f.size());
        for (ui32 i = 0; i < f.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(f[i], true,
                "Delete portion row " << i << " must be kept (newest)");
        }
    }
}

Y_UNIT_TEST(StaggeredOverlapWithGaps) {
    // Portions that overlap in a staggered pattern with gaps:
    // P1=[1,5], P2=[4,8], P3=[7,12], P4=[11,15]
    // P5=[1,15] v=highest (delete all)
    // All requests at once.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;

    auto addPortion = [&](ui64 pid, ui64 start, ui64 end, ui64 planStep) {
        ui64 cnt = end - start + 1;
        portions.push_back(MakeTestPortion(pid, start, end, cnt));
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = start; k <= end; ++k) {
            pk.push_back(k); ps.push_back(planStep);
            tx.push_back(1); wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, pid, pk, ps, tx, wr);
    };

    addPortion(1, 1, 5, 10);
    addPortion(2, 4, 8, 15);
    addPortion(3, 7, 12, 20);
    addPortion(4, 11, 15, 25);
    addPortion(5, 1, 15, 50);

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs;
    for (const auto& p : portions) {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs.push_back(s);
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, p->GetPortionId(),
            p->GetRecordsCount(), s));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s->FilterReady && !s->Failed) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(15));

    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        UNIT_ASSERT_C(subs[idx]->FilterReady && !subs[idx]->Failed,
            "P" << portions[idx]->GetPortionId()
            << " failed: " << subs[idx]->FailureReason);

        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        ui64 expected = portions[idx]->GetRecordsCount();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), expected,
            "P" << portions[idx]->GetPortionId()
            << " filter size " << f.size() << " != " << expected);

        if (portions[idx]->GetPortionId() == 5) {
            for (ui32 i = 0; i < f.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(f[i], true,
                    "P5 (delete) row " << i << " must be kept");
            }
        } else {
            for (ui32 i = 0; i < f.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(f[i], false,
                    "P" << portions[idx]->GetPortionId()
                    << " row " << i << " must be deduped by P5");
            }
        }
    }
}

Y_UNIT_TEST(TwoConcurrentChainsPrematureDrain) {
    // Reproduces the bug: two TEvFilterRequestResourcesAllocated events create
    // two concurrent batch chains. Chain B's ReadyBorders at key6 matches
    // chain A's WaitingBorders, causing a premature drain of key6 before
    // P_old's data (in chain A's later batch) arrives in the Merger.
    // Result: both P_old and P_new get true at key6 → duplicate row.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;
    ui64 pid = 1;

    for (ui64 k = 0; k <= 9; ++k) {
        if (k == 6) continue;
        portions.push_back(MakeTestPortion(pid, k, k, 1));
        RegisterColumnData(columnStore, tabletActorId, pid, {k}, {10}, {1}, {0});
        ++pid;
    }

    ui64 pOldId = pid++;
    portions.push_back(MakeTestPortion(pOldId, 6, 6, 1));
    RegisterColumnData(columnStore, tabletActorId, pOldId, {6}, {50}, {1}, {0});

    ui64 pNewId = pid++;
    portions.push_back(MakeTestPortion(pNewId, 0, 9, 10));
    {
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 0; k <= 9; ++k) {
            pk.push_back(k);
            ps.push_back(100);
            tx.push_back(1);
            wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, pNewId, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(1000, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs(portions.size());

    // Send P_old's request FIRST (border=key6) to trigger chain A (borders 0-6)
    ui32 pOldIdx = Max<ui32>();
    ui32 pKey9Idx = Max<ui32>();
    for (ui32 i = 0; i < portions.size(); ++i) {
        if (portions[i]->GetPortionId() == pOldId) pOldIdx = i;
        if (portions[i]->IndexKeyStart().GetValue<ui64>(0).value() == 9 &&
            portions[i]->GetRecordsCount() == 1) pKey9Idx = i;
    }
    UNIT_ASSERT(pOldIdx != Max<ui32>());
    UNIT_ASSERT(pKey9Idx != Max<ui32>());

    {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs[pOldIdx] = s;
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, portions[pOldIdx]->GetPortionId(),
            portions[pOldIdx]->GetRecordsCount(), s));
    }
    {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs[pKey9Idx] = s;
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, portions[pKey9Idx]->GetPortionId(),
            portions[pKey9Idx]->GetRecordsCount(), s));
    }

    for (ui32 i = 0; i < portions.size(); ++i) {
        if (i == pOldIdx || i == pKey9Idx) continue;
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs[i] = s;
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, portions[i]->GetPortionId(),
            portions[i]->GetRecordsCount(), s));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s || (!s->FilterReady && !s->Failed)) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(30));

    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        UNIT_ASSERT_C(subs[idx]->FilterReady && !subs[idx]->Failed,
            "Portion " << portions[idx]->GetPortionId()
            << " failed: " << subs[idx]->FailureReason);

        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), portions[idx]->GetRecordsCount(),
            "Portion " << portions[idx]->GetPortionId()
            << " filter size mismatch");
    }

    THashMap<ui64, ui32> trueCountPerKey;
    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        ui64 startKey = portions[idx]->IndexKeyStart().GetValue<ui64>(0).value();
        for (ui32 i = 0; i < f.size(); ++i) {
            if (f[i]) {
                trueCountPerKey[startKey + i]++;
            }
        }
    }

    for (const auto& [key, count] : trueCountPerKey) {
        UNIT_ASSERT_VALUES_EQUAL_C(count, 1u,
            "Key " << key << " has " << count
            << " surviving versions (expected exactly 1, duplicate detected)");
    }
    UNIT_ASSERT_VALUES_EQUAL(trueCountPerKey.size(), 10u);
}

Y_UNIT_TEST(TwoConcurrentChainsManyPortions) {
    // Same bug but with realistic portion counts: many single-key updates + wide bulks.
    // P_old request (border=key6) arrives first, creating chain A for borders 0-6.
    // P_wide request (border=key19) arrives second, creating chain B for borders 7-19.
    // Chain B's ReadyBorders at key6 cause premature drain before chain A fetches
    // portions at key6.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    const ui64 numKeys = 10;

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;
    ui64 nextPid = 1;
    ui64 nextVersion = 10;

    for (ui64 k = 0; k < numKeys; ++k) {
        ui64 id = nextPid++;
        portions.push_back(MakeTestPortion(id, k, k, 1));
        RegisterColumnData(columnStore, tabletActorId, id, {k}, {nextVersion}, {1}, {0});
    }
    nextVersion += 10;

    for (ui64 u = 0; u < 10; ++u) {
        for (ui64 k = 0; k < numKeys; ++k) {
            ui64 id = nextPid++;
            portions.push_back(MakeTestPortion(id, k, k, 1));
            RegisterColumnData(columnStore, tabletActorId, id, {k}, {nextVersion + u}, {1}, {0});
        }
    }
    nextVersion += 20;

    for (ui64 b = 0; b < 5; ++b) {
        ui64 id = nextPid++;
        portions.push_back(MakeTestPortion(id, 0, numKeys - 1, numKeys));
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 0; k < numKeys; ++k) {
            pk.push_back(k);
            ps.push_back(nextVersion + b);
            tx.push_back(1);
            wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, id, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(2000, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs(portions.size());

    ui32 key6Idx = Max<ui32>();
    ui32 lastWideIdx = portions.size() - 1;
    for (ui32 i = 0; i < portions.size(); ++i) {
        auto startKey = portions[i]->IndexKeyStart().GetValue<ui64>(0).value();
        if (startKey == 6 && portions[i]->GetRecordsCount() == 1 && key6Idx == Max<ui32>()) {
            key6Idx = i;
        }
    }
    UNIT_ASSERT(key6Idx != Max<ui32>());

    {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs[key6Idx] = s;
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, portions[key6Idx]->GetPortionId(),
            portions[key6Idx]->GetRecordsCount(), s));
    }
    {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs[lastWideIdx] = s;
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, portions[lastWideIdx]->GetPortionId(),
            portions[lastWideIdx]->GetRecordsCount(), s));
    }

    for (ui32 i = 0; i < portions.size(); ++i) {
        if (i == key6Idx || i == lastWideIdx) continue;
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs[i] = s;
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, portions[i]->GetPortionId(),
            portions[i]->GetRecordsCount(), s));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s || (!s->FilterReady && !s->Failed)) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(30));

    THashMap<ui64, ui32> trueCountPerKey;
    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        UNIT_ASSERT_C(subs[idx]->FilterReady && !subs[idx]->Failed,
            "Portion " << portions[idx]->GetPortionId()
            << " failed: " << subs[idx]->FailureReason);

        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), portions[idx]->GetRecordsCount(),
            "Portion " << portions[idx]->GetPortionId()
            << " filter size mismatch");

        ui64 startKey = portions[idx]->IndexKeyStart().GetValue<ui64>(0).value();
        for (ui32 i = 0; i < f.size(); ++i) {
            if (f[i]) {
                trueCountPerKey[startKey + i]++;
            }
        }
    }

    for (const auto& [key, count] : trueCountPerKey) {
        UNIT_ASSERT_VALUES_EQUAL_C(count, 1u,
            "Key " << key << " has " << count
            << " surviving versions (expected 1, duplicate like assert 22==20)");
    }
    UNIT_ASSERT_VALUES_EQUAL(trueCountPerKey.size(), numKeys);
}

Y_UNIT_TEST(ManyUpdatesBulkFirstRequest) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    const ui64 numKeys = 20;
    const ui64 updatesPerKey = 10;
    const ui64 numBulk = 100;

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;
    ui64 nextPortionId = 1;
    ui64 nextVersion = 10;

    for (ui64 k = 0; k < numKeys; ++k) {
        ui64 pid = nextPortionId++;
        portions.push_back(MakeTestPortion(pid, k, k, 1));
        RegisterColumnData(columnStore, tabletActorId, pid,
            {k}, {nextVersion}, {1}, {0});
    }
    nextVersion += 10;

    for (ui64 u = 0; u < updatesPerKey; ++u) {
        for (ui64 k = 0; k < numKeys; ++k) {
            ui64 pid = nextPortionId++;
            portions.push_back(MakeTestPortion(pid, k, k, 1));
            RegisterColumnData(columnStore, tabletActorId, pid,
                {k}, {nextVersion + u}, {1}, {0});
        }
    }
    nextVersion += updatesPerKey + 10;

    for (ui64 b = 0; b < numBulk; ++b) {
        ui64 pid = nextPortionId++;
        portions.push_back(MakeTestPortion(pid, 0, numKeys - 1, numKeys));
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 0; k < numKeys; ++k) {
            pk.push_back(k);
            ps.push_back(nextVersion + b);
            tx.push_back(1);
            wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, pid, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(1000, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs(portions.size());

    ui32 lastBulkIdx = portions.size() - 1;
    {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs[lastBulkIdx] = s;
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, portions[lastBulkIdx]->GetPortionId(),
            portions[lastBulkIdx]->GetRecordsCount(), s));
    }

    for (ui32 i = 0; i < portions.size(); ++i) {
        if (i == lastBulkIdx) continue;
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs[i] = s;
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, portions[i]->GetPortionId(),
            portions[i]->GetRecordsCount(), s));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s || (!s->FilterReady && !s->Failed)) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(60));

    THashMap<ui64, ui32> trueCountPerKey;
    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        UNIT_ASSERT_C(subs[idx]->FilterReady && !subs[idx]->Failed,
            "Portion " << portions[idx]->GetPortionId()
            << " failed: " << subs[idx]->FailureReason);

        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        ui64 expected = portions[idx]->GetRecordsCount();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), expected,
            "Portion " << portions[idx]->GetPortionId()
            << " filter must cover " << expected
            << " records but got " << f.size());

        ui64 startKey = portions[idx]->IndexKeyStart().GetValue<ui64>(0).value();
        for (ui32 i = 0; i < f.size(); ++i) {
            if (f[i]) {
                trueCountPerKey[startKey + i]++;
            }
        }
    }

    for (const auto& [key, count] : trueCountPerKey) {
        UNIT_ASSERT_VALUES_EQUAL_C(count, 1u,
            "Key " << key << " has " << count
            << " surviving versions (expected exactly 1)");
    }
    UNIT_ASSERT_VALUES_EQUAL(trueCountPerKey.size(), numKeys);
}

Y_UNIT_TEST(ManyUpdatesExactFailingTestParams) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    const ui64 numKeys = 20;

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;
    ui64 nextPortionId = 1;
    ui64 nextVersion = 10;

    for (ui64 k = 0; k < numKeys; ++k) {
        ui64 pid = nextPortionId++;
        portions.push_back(MakeTestPortion(pid, k, k, 1));
        RegisterColumnData(columnStore, tabletActorId, pid,
            {k}, {nextVersion}, {1}, {0});
    }
    nextVersion += 10;

    for (ui64 u = 0; u < 10; ++u) {
        for (ui64 k = 0; k < numKeys; ++k) {
            ui64 pid = nextPortionId++;
            portions.push_back(MakeTestPortion(pid, k, k, 1));
            RegisterColumnData(columnStore, tabletActorId, pid,
                {k}, {nextVersion + u}, {1}, {0});
        }
    }
    nextVersion += 20;

    for (ui64 u = 0; u < 10; ++u) {
        for (ui64 k = 0; k < numKeys; ++k) {
            ui64 pid = nextPortionId++;
            portions.push_back(MakeTestPortion(pid, k, k, 1));
            RegisterColumnData(columnStore, tabletActorId, pid,
                {k}, {nextVersion + u}, {1}, {0});
        }
    }
    nextVersion += 20;

    for (ui64 b = 0; b < 100; ++b) {
        ui64 pid = nextPortionId++;
        portions.push_back(MakeTestPortion(pid, 0, numKeys - 1, numKeys));
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 0; k < numKeys; ++k) {
            pk.push_back(k);
            ps.push_back(nextVersion + b);
            tx.push_back(1);
            wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, pid, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(2000, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs;
    for (const auto& p : portions) {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs.push_back(s);
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, p->GetPortionId(),
            p->GetRecordsCount(), s));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s->FilterReady && !s->Failed) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(60));

    THashMap<ui64, ui32> trueCountPerKey;
    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        UNIT_ASSERT_C(subs[idx]->FilterReady && !subs[idx]->Failed,
            "Portion " << portions[idx]->GetPortionId()
            << " failed: " << subs[idx]->FailureReason);

        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        ui64 expected = portions[idx]->GetRecordsCount();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), expected,
            "Portion " << portions[idx]->GetPortionId()
            << " filter size " << f.size() << " != " << expected);

        ui64 startKey = portions[idx]->IndexKeyStart().GetValue<ui64>(0).value();
        for (ui32 i = 0; i < f.size(); ++i) {
            if (f[i]) {
                trueCountPerKey[startKey + i]++;
            }
        }
    }

    ui64 totalTrue = 0;
    for (const auto& [key, count] : trueCountPerKey) {
        UNIT_ASSERT_VALUES_EQUAL_C(count, 1u,
            "Key " << key << " has " << count
            << " surviving versions (expected exactly 1, got duplicates like assert 22==20)");
        totalTrue++;
    }
    UNIT_ASSERT_VALUES_EQUAL_C(totalTrue, numKeys,
        "Total surviving rows " << totalTrue << " != " << numKeys
        << " (duplicates detected)");
}

Y_UNIT_TEST(ManyUpdatesSingleKeyStorm) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    const ui64 numKeys = 20;
    const ui64 numPortionsPerKey = 15;

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;
    ui64 nextPortionId = 1;

    for (ui64 k = 0; k < numKeys; ++k) {
        for (ui64 v = 0; v < numPortionsPerKey; ++v) {
            ui64 pid = nextPortionId++;
            portions.push_back(MakeTestPortion(pid, k, k, 1));
            RegisterColumnData(columnStore, tabletActorId, pid,
                {k}, {10 + v}, {1}, {0});
        }
    }

    ui64 widePid = nextPortionId++;
    portions.push_back(MakeTestPortion(widePid, 0, numKeys - 1, numKeys));
    {
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 0; k < numKeys; ++k) {
            pk.push_back(k);
            ps.push_back(1000);
            tx.push_back(1);
            wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, widePid, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(2000, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs;
    for (const auto& p : portions) {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs.push_back(s);
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, p->GetPortionId(),
            p->GetRecordsCount(), s));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s->FilterReady && !s->Failed) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(30));

    THashMap<ui64, ui32> trueCountPerKey;
    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        UNIT_ASSERT_C(subs[idx]->FilterReady && !subs[idx]->Failed,
            "Portion " << portions[idx]->GetPortionId()
            << " failed: " << subs[idx]->FailureReason);

        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), portions[idx]->GetRecordsCount(),
            "Portion " << portions[idx]->GetPortionId()
            << " filter size mismatch");

        ui64 startKey = portions[idx]->IndexKeyStart().GetValue<ui64>(0).value();
        for (ui32 i = 0; i < f.size(); ++i) {
            if (f[i]) {
                trueCountPerKey[startKey + i]++;
            }
        }
    }

    for (const auto& [key, count] : trueCountPerKey) {
        UNIT_ASSERT_VALUES_EQUAL_C(count, 1u,
            "Key " << key << " has " << count
            << " surviving versions (expected exactly 1)");
    }
    UNIT_ASSERT_VALUES_EQUAL(trueCountPerKey.size(), numKeys);

    {
        auto wideF = subs.back()->ReceivedFilter.BuildTrivialFilter();
        for (ui32 i = 0; i < wideF.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(wideF[i], true,
                "Wide portion (newest) row " << i << " must be kept");
        }
    }
}

Y_UNIT_TEST(ManyUpdatesReverseRequestOrder) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    const ui64 numKeys = 20;
    const ui64 updatesPerKey = 10;
    const ui64 numBulk = 100;

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;
    ui64 nextPortionId = 1;
    ui64 nextVersion = 10;

    for (ui64 k = 0; k < numKeys; ++k) {
        ui64 pid = nextPortionId++;
        portions.push_back(MakeTestPortion(pid, k, k, 1));
        RegisterColumnData(columnStore, tabletActorId, pid,
            {k}, {nextVersion}, {1}, {0});
    }
    nextVersion += 10;

    for (ui64 u = 0; u < updatesPerKey; ++u) {
        for (ui64 k = 0; k < numKeys; ++k) {
            ui64 pid = nextPortionId++;
            portions.push_back(MakeTestPortion(pid, k, k, 1));
            RegisterColumnData(columnStore, tabletActorId, pid,
                {k}, {nextVersion + u}, {1}, {0});
        }
    }
    nextVersion += updatesPerKey + 10;

    for (ui64 b = 0; b < numBulk; ++b) {
        ui64 pid = nextPortionId++;
        portions.push_back(MakeTestPortion(pid, 0, numKeys - 1, numKeys));
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 0; k < numKeys; ++k) {
            pk.push_back(k);
            ps.push_back(nextVersion + b);
            tx.push_back(1);
            wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, pid, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(2000, 1), portions, dam, cdm, tabletActorId, setup);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs(portions.size());
    for (i64 i = portions.size() - 1; i >= 0; --i) {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs[i] = s;
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, portions[i]->GetPortionId(),
            portions[i]->GetRecordsCount(), s));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s->FilterReady && !s->Failed) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(60));

    THashMap<ui64, ui32> trueCountPerKey;
    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        UNIT_ASSERT_C(subs[idx]->FilterReady && !subs[idx]->Failed,
            "Portion " << portions[idx]->GetPortionId()
            << " failed: " << subs[idx]->FailureReason);

        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), portions[idx]->GetRecordsCount(),
            "Portion " << portions[idx]->GetPortionId()
            << " filter size mismatch");

        ui64 startKey = portions[idx]->IndexKeyStart().GetValue<ui64>(0).value();
        for (ui32 i = 0; i < f.size(); ++i) {
            if (f[i]) {
                trueCountPerKey[startKey + i]++;
            }
        }
    }

    for (const auto& [key, count] : trueCountPerKey) {
        UNIT_ASSERT_VALUES_EQUAL_C(count, 1u,
            "Key " << key << " has " << count
            << " surviving versions (expected exactly 1)");
    }
    UNIT_ASSERT_VALUES_EQUAL(trueCountPerKey.size(), numKeys);
}

Y_UNIT_TEST(DescSorted_SingleExclusivePortionGetsAllowAllFilter) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(edgeActor);
    auto cdm =
        std::make_shared<NColumnFetching::TColumnDataManager>(edgeActor);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 10, 20, 100));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(1, 1), portions, dam, cdm, edgeActor, setup, ERequestSorting::DESC);

    auto subscriber = std::make_shared<TTestFilterSubscriber>();
    runtime.Send(
        MakeFilterRequestHandle(actorId, edgeActor, 1, 100, subscriber));

    NActors::TDispatchOptions options;
    options.CustomFinalCondition =
        [&subscriber]() { return subscriber->FilterReady; };
    runtime.DispatchEvents(options, TDuration::Seconds(1));

    UNIT_ASSERT_C(subscriber->FilterReady,
                  "subscriber should have received the filter");
    UNIT_ASSERT(!subscriber->Failed);
    UNIT_ASSERT(subscriber->ReceivedFilter.IsTotalAllowFilter());
    UNIT_ASSERT_VALUES_EQUAL(
        subscriber->ReceivedFilter.GetRecordsCountVerified(), 100);
}

Y_UNIT_TEST(DescSorted_OverlappingPortionsMergeDeduplication) {
    // Test with DESC sorting: P1=[1,3] pk={1,2,3} version=10, P2=[2,4] pk={2,3,4} version=20
    // In DESC order, keys are processed from highest to lowest
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam =
        std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm =
        std::make_shared<NColumnFetching::TColumnDataManager>(
            tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 3, 3));
    portions.push_back(MakeTestPortion(2, 2, 4, 3));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId,
        1, {1, 2, 3}, {10, 10, 10}, {1, 1, 1}, {0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId,
        2, {2, 3, 4}, {20, 20, 20}, {1, 1, 1}, {0, 0, 0});

    NActors::TActorId columnCacheServiceId =
        NColumnFetching::TGeneralCache::MakeServiceId(
            runtime.GetNodeId(0));
    runtime.RegisterService(columnCacheServiceId,
        runtime.Register(
            new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm,
        tabletActorId, setup, ERequestSorting::DESC);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(
        MakeFilterRequestHandle(actorId, edgeActor, 1, 3, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition =
        [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    runtime.Send(
        MakeFilterRequestHandle(actorId, edgeActor, 2, 3, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition =
        [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady,
        "P1 filter should be ready; failure=" << sub1->FailureReason);
    UNIT_ASSERT_C(!sub1->Failed,
        "P1 should not fail: " << sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady,
        "P2 filter should be ready; failure=" << sub2->FailureReason);
    UNIT_ASSERT_C(!sub2->Failed,
        "P2 should not fail: " << sub2->FailureReason);

    auto p1Filter = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(p1Filter.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(p1Filter[0], true);   // key 1 unique
    UNIT_ASSERT_VALUES_EQUAL(p1Filter[1], false);  // key 2 deduped by P2
    UNIT_ASSERT_VALUES_EQUAL(p1Filter[2], false);  // key 3 deduped by P2

    auto p2Filter = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(p2Filter.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(p2Filter[0], true);   // key 2 kept (newer)
    UNIT_ASSERT_VALUES_EQUAL(p2Filter[1], true);   // key 3 kept (newer)
    UNIT_ASSERT_VALUES_EQUAL(p2Filter[2], true);   // key 4 unique
}

Y_UNIT_TEST(DescSorted_ThreePortionsChainOverlap) {
    // Test with DESC sorting: P1=[1,4], P2=[3,6], P3=[5,8]
    // Chain: P1-P2 overlap on 3,4; P2-P3 overlap on 5,6.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 4, 4));
    portions.push_back(MakeTestPortion(2, 3, 6, 4));
    portions.push_back(MakeTestPortion(3, 5, 8, 4));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1, 2, 3, 4}, {10, 10, 10, 10}, {1, 1, 1, 1}, {0, 0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {3, 4, 5, 6}, {20, 20, 20, 20}, {1, 1, 1, 1}, {0, 0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {5, 6, 7, 8}, {30, 30, 30, 30}, {1, 1, 1, 1}, {0, 0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup, ERequestSorting::DESC);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 4, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition = [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 4, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 4, sub3));
    NActors::TDispatchOptions opt3;
    opt3.CustomFinalCondition = [&]() { return sub3->FilterReady || sub3->Failed; };
    runtime.DispatchEvents(opt3, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);

    // P1: keys 1,2 unique; keys 3,4 deduped by P2
    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[3], false);

    // P2: keys 3,4 kept (newer than P1); keys 5,6 deduped by P3
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f2[3], false);

    // P3: all keys kept (newest or unique)
    auto f3 = sub3->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f3.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f3[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[2], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[3], true);
}

Y_UNIT_TEST(DescSorted_ManyUpdatesDeleteAllFilterCoversAllRows) {
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 10, 10));
    portions.push_back(MakeTestPortion(2, 1, 10, 10));
    portions.push_back(MakeTestPortion(3, 1, 10, 10));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1,2,3,4,5,6,7,8,9,10},
        {10,10,10,10,10,10,10,10,10,10},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {1,2,3,4,5,6,7,8,9,10},
        {20,20,20,20,20,20,20,20,20,20},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {1,2,3,4,5,6,7,8,9,10},
        {30,30,30,30,30,30,30,30,30,30},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup, ERequestSorting::DESC);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 10, sub1));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 10, sub2));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 10, sub3));

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        return (sub1->FilterReady || sub1->Failed) &&
               (sub2->FilterReady || sub2->Failed) &&
               (sub3->FilterReady || sub3->Failed);
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);

    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f1.size(), 10u,
        "P1 filter must cover all 10 records but got " << f1.size());
    for (ui32 i = 0; i < f1.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f1[i], false,
            "P1 row " << i << " must be deduped (newer P3 exists)");
    }

    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f2.size(), 10u,
        "P2 filter must cover all 10 records but got " << f2.size());
    for (ui32 i = 0; i < f2.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f2[i], false,
            "P2 row " << i << " must be deduped (newer P3 exists)");
    }

    auto f3 = sub3->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL_C(f3.size(), 10u,
        "P3 filter must cover all 10 records but got " << f3.size());
    for (ui32 i = 0; i < f3.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f3[i], true,
            "P3 row " << i << " must be kept (newest version)");
    }
}

Y_UNIT_TEST(DescSorted_MixedExclusiveAndOverlapping) {
    // P1=[1,3] exclusive, P2=[5,8] overlaps with P3=[7,10]
    // P1 should get allow-all. P2/P3 go through merge.
    // P2: pk={5,6,7,8} version=10
    // P3: pk={7,8,9,10} version=20
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 3, 2));
    portions.push_back(MakeTestPortion(2, 5, 8, 4));
    portions.push_back(MakeTestPortion(3, 7, 10, 4));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 2,
        {5, 6, 7, 8}, {10, 10, 10, 10}, {1, 1, 1, 1}, {0, 0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {7, 8, 9, 10}, {20, 20, 20, 20}, {1, 1, 1, 1}, {0, 0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup, ERequestSorting::DESC);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto subExcl = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    // P1 is exclusive — should get allow-all immediately
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 2, subExcl));
    NActors::TDispatchOptions optE;
    optE.CustomFinalCondition = [&]() { return subExcl->FilterReady; };
    runtime.DispatchEvents(optE, TDuration::Seconds(1));

    UNIT_ASSERT(subExcl->FilterReady);
    UNIT_ASSERT(subExcl->ReceivedFilter.IsTotalAllowFilter());
    UNIT_ASSERT_VALUES_EQUAL(subExcl->ReceivedFilter.GetRecordsCountVerified(), 2);

    // Now request overlapping portions
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 4, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 4, sub3));
    NActors::TDispatchOptions opt3;
    opt3.CustomFinalCondition = [&]() { return sub3->FilterReady || sub3->Failed; };
    runtime.DispatchEvents(opt3, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);

    // P2: keys 5,6 unique; keys 7,8 deduped by P3
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f2[3], false);

    // P3: all kept
    auto f3 = sub3->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f3.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f3[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[2], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[3], true);
}

Y_UNIT_TEST(DescSorted_ReverseRequestOrder) {
    // Same overlap as OverlappingPortionsMergeDeduplication, but request P2 first.
    // P1: pk={1,2,3} version=10 (older)
    // P2: pk={2,3,4} version=20 (newer)
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 3, 3));
    portions.push_back(MakeTestPortion(2, 2, 4, 3));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1, 2, 3}, {10, 10, 10}, {1, 1, 1}, {0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {2, 3, 4}, {20, 20, 20}, {1, 1, 1}, {0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup, ERequestSorting::DESC);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub1 = std::make_shared<TTestFilterSubscriber>();

    // Request P2 FIRST, then P1
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 3, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 3, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition = [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);

    // P1: key 1 unique, keys 2,3 deduped by newer P2
    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);

    // P2: all keys kept
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], true);
}

Y_UNIT_TEST(DescSorted_FullContainment) {
    // P1=[1,10] pk={1,3,5,7,10} version=10 (outer, older)
    // P2=[3,7]  pk={3,5,7}       version=20 (inner, newer)
    // Keys 3,5,7 overlap. P2 wins.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 10, 5));
    portions.push_back(MakeTestPortion(2, 3, 7, 3));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1, 3, 5, 7, 10}, {10, 10, 10, 10, 10}, {1, 1, 1, 1, 1}, {0, 0, 0, 0, 0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {3, 5, 7}, {20, 20, 20}, {1, 1, 1}, {0, 0, 0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup, ERequestSorting::DESC);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 5, sub1));
    NActors::TDispatchOptions opt1;
    opt1.CustomFinalCondition = [&]() { return sub1->FilterReady || sub1->Failed; };
    runtime.DispatchEvents(opt1, TDuration::Seconds(5));

    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 3, sub2));
    NActors::TDispatchOptions opt2;
    opt2.CustomFinalCondition = [&]() { return sub2->FilterReady || sub2->Failed; };
    runtime.DispatchEvents(opt2, TDuration::Seconds(5));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);

    // P1: keys 1,10 unique (kept); keys 3,5,7 deduped by newer P2
    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 5u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[3], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[4], true);

    // P2: all keys kept (newer)
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], true);
}

Y_UNIT_TEST(DescSorted_MultipleWideOverlappingPortions) {
    // 5 wide portions [1,10], each with all 10 keys, increasing versions.
    // All requests sent simultaneously. Tests many-to-many deduplication.
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    const ui32 portionCount = 5;
    const ui64 N = 10;
    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;

    for (ui32 p = 1; p <= portionCount; ++p) {
        portions.push_back(MakeTestPortion(p, 1, N, N));
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 1; k <= N; ++k) {
            pk.push_back(k);
            ps.push_back(p * 10);
            tx.push_back(1);
            wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, p, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup, ERequestSorting::DESC);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs(portionCount);
    for (ui32 p = 0; p < portionCount; ++p) {
        subs[p] = std::make_shared<TTestFilterSubscriber>();
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, p + 1, N, subs[p]));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s->FilterReady && !s->Failed) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));

    for (ui32 p = 0; p < portionCount; ++p) {
        UNIT_ASSERT_C(subs[p]->FilterReady && !subs[p]->Failed,
            "P" << (p+1) << " failed: " << subs[p]->FailureReason);
        auto f = subs[p]->ReceivedFilter.BuildTrivialFilter();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), N,
            "P" << (p+1) << " filter must cover " << N
            << " records but got " << f.size());

        bool isNewest = (p + 1 == portionCount);
        for (ui32 i = 0; i < f.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(f[i], isNewest,
                "P" << (p+1) << " row " << i
                << (isNewest ? " must be kept" : " must be deduped"));
        }
    }
}

Y_UNIT_TEST(DescSorted_BorderOrderingWithMultiplePortions) {
    // Test that borders are processed in correct order with DESC sorting
    // Portions: P1=[1,10], P2=[5,15], P3=[10,20]
    // In DESC order, borders should be processed from highest to lowest
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 10, 10));
    portions.push_back(MakeTestPortion(2, 5, 15, 11));
    portions.push_back(MakeTestPortion(3, 10, 20, 11));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1,2,3,4,5,6,7,8,9,10},
        {10,10,10,10,10,10,10,10,10,10},
        {1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {5,6,7,8,9,10,11,12,13,14,15},
        {20,20,20,20,20,20,20,20,20,20,20},
        {1,1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {10,11,12,13,14,15,16,17,18,19,20},
        {30,30,30,30,30,30,30,30,30,30,30},
        {1,1,1,1,1,1,1,1,1,1,1},
        {0,0,0,0,0,0,0,0,0,0,0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup, ERequestSorting::DESC);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    // Request in reverse order to test border processing
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 11, sub3));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 11, sub2));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 10, sub1));

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        return (sub1->FilterReady || sub1->Failed) &&
               (sub2->FilterReady || sub2->Failed) &&
               (sub3->FilterReady || sub3->Failed);
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);

    // P1: keys 1-4 unique, keys 5-10 deduped by P2 or P3
    auto f1 = sub1->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 10u);
    for (ui32 i = 0; i < 4; ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f1[i], true, i);
    }
    for (ui32 i = 4; i < 10; ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f1[i], false, i);
    }

    // P2: keys 5-9 deduped by P3, keys 10-15 deduped by P3
    auto f2 = sub2->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 11u);
    for (ui32 i = 0; i < 5; ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f2[i], true, i);
    }
    for (ui32 i = 5; i < 11; ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f2[i], false, i);
    }

    // P3: all kept (newest)
    auto f3 = sub3->ReceivedFilter.BuildTrivialFilter();
    UNIT_ASSERT_VALUES_EQUAL(f3.size(), 11u);
    for (ui32 i = 0; i < 11; ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(f3[i], true, i);
    }
}

Y_UNIT_TEST(DescSorted_ConcurrentChainsWithReverseOrder) {
    // Test concurrent batch chains with DESC sorting
    // Similar to TwoConcurrentChainsPrematureDrain but with DESC order
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    TColumnDataMap columnStore;
    ui64 pid = 1;

    for (ui64 k = 0; k <= 9; ++k) {
        if (k == 6) continue;
        portions.push_back(MakeTestPortion(pid, k, k, 1));
        RegisterColumnData(columnStore, tabletActorId, pid, {k}, {10}, {1}, {0});
        ++pid;
    }

    ui64 pOldId = pid++;
    portions.push_back(MakeTestPortion(pOldId, 6, 6, 1));
    RegisterColumnData(columnStore, tabletActorId, pOldId, {6}, {50}, {1}, {0});

    ui64 pNewId = pid++;
    portions.push_back(MakeTestPortion(pNewId, 0, 9, 10));
    {
        std::vector<ui64> pk, ps, tx, wr;
        for (ui64 k = 0; k <= 9; ++k) {
            pk.push_back(k);
            ps.push_back(100);
            tx.push_back(1);
            wr.push_back(0);
        }
        RegisterColumnData(columnStore, tabletActorId, pNewId, pk, ps, tx, wr);
    }

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(1000, 1), portions, dam, cdm, tabletActorId, setup, ERequestSorting::DESC);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    std::vector<std::shared_ptr<TTestFilterSubscriber>> subs(portions.size());

    // Send P_old's request FIRST (border=key6) to trigger chain A
    ui32 pOldIdx = Max<ui32>();
    ui32 pKey9Idx = Max<ui32>();
    for (ui32 i = 0; i < portions.size(); ++i) {
        if (portions[i]->GetPortionId() == pOldId) pOldIdx = i;
        if (portions[i]->IndexKeyStart().GetValue<ui64>(0).value() == 9 &&
            portions[i]->GetRecordsCount() == 1) pKey9Idx = i;
    }
    UNIT_ASSERT(pOldIdx != Max<ui32>());
    UNIT_ASSERT(pKey9Idx != Max<ui32>());

    {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs[pOldIdx] = s;
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, portions[pOldIdx]->GetPortionId(),
            portions[pOldIdx]->GetRecordsCount(), s));
    }
    {
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs[pKey9Idx] = s;
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, portions[pKey9Idx]->GetPortionId(),
            portions[pKey9Idx]->GetRecordsCount(), s));
    }

    for (ui32 i = 0; i < portions.size(); ++i) {
        if (i == pOldIdx || i == pKey9Idx) continue;
        auto s = std::make_shared<TTestFilterSubscriber>();
        subs[i] = s;
        runtime.Send(MakeFilterRequestHandle(
            actorId, edgeActor, portions[i]->GetPortionId(),
            portions[i]->GetRecordsCount(), s));
    }

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        for (auto& s : subs) {
            if (!s || (!s->FilterReady && !s->Failed)) return false;
        }
        return true;
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(30));

    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        UNIT_ASSERT_C(subs[idx]->FilterReady && !subs[idx]->Failed,
            "Portion " << portions[idx]->GetPortionId()
            << " failed: " << subs[idx]->FailureReason);

        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        UNIT_ASSERT_VALUES_EQUAL_C(f.size(), portions[idx]->GetRecordsCount(),
            "Portion " << portions[idx]->GetPortionId()
            << " filter size mismatch");
    }

    THashMap<ui64, ui32> trueCountPerKey;
    for (ui32 idx = 0; idx < subs.size(); ++idx) {
        auto f = subs[idx]->ReceivedFilter.BuildTrivialFilter();
        ui64 startKey = portions[idx]->IndexKeyStart().GetValue<ui64>(0).value();
        for (ui32 i = 0; i < f.size(); ++i) {
            if (f[i]) {
                trueCountPerKey[startKey + i]++;
            }
        }
    }

    for (const auto& [key, count] : trueCountPerKey) {
        UNIT_ASSERT_VALUES_EQUAL_C(count, 1u,
            "Key " << key << " has " << count
            << " surviving versions (expected exactly 1, duplicate detected)");
    }
    UNIT_ASSERT_VALUES_EQUAL(trueCountPerKey.size(), 10u);
}

Y_UNIT_TEST(DescSorted_NonSequentialBorderProcessing) {
    // Test that borders are processed correctly even when they don't arrive sequentially
    // Portions: P1=[1,5], P2=[10,15], P3=[20,25]
    // Request order: P2, P1, P3 (non-sequential)
    NActors::TTestActorRuntimeBase runtime(1, false);
    InitializeRuntimeWithLogging(runtime);
    NActors::TActorId tabletActorId = runtime.AllocateEdgeActor();

    auto dam = std::make_shared<TMockDataAccessorsManager>(tabletActorId);
    auto cdm = std::make_shared<NColumnFetching::TColumnDataManager>(tabletActorId);

    std::deque<std::shared_ptr<TPortionInfo>> portions;
    portions.push_back(MakeTestPortion(1, 1, 5, 5));
    portions.push_back(MakeTestPortion(2, 10, 15, 6));
    portions.push_back(MakeTestPortion(3, 20, 25, 6));

    TColumnDataMap columnStore;
    RegisterColumnData(columnStore, tabletActorId, 1,
        {1,2,3,4,5}, {10,10,10,10,10}, {1,1,1,1,1}, {0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 2,
        {10,11,12,13,14,15}, {20,20,20,20,20,20}, {1,1,1,1,1,1}, {0,0,0,0,0,0});
    RegisterColumnData(columnStore, tabletActorId, 3,
        {20,21,22,23,24,25}, {30,30,30,30,30,30}, {1,1,1,1,1,1}, {0,0,0,0,0,0});

    auto cacheId = NColumnFetching::TGeneralCache::MakeServiceId(runtime.GetNodeId(0));
    runtime.RegisterService(cacheId,
        runtime.Register(new TMockColumnDataCacheService(std::move(columnStore))));

    TManagerSetupResult setup;
    auto actorId = SetupDuplicateManager(
        runtime, TSnapshot(100, 1), portions, dam, cdm, tabletActorId, setup, ERequestSorting::DESC);
    NActors::TActorId edgeActor = runtime.AllocateEdgeActor();

    auto sub1 = std::make_shared<TTestFilterSubscriber>();
    auto sub2 = std::make_shared<TTestFilterSubscriber>();
    auto sub3 = std::make_shared<TTestFilterSubscriber>();

    // Request in non-sequential order: P2, P1, P3
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 2, 6, sub2));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 1, 5, sub1));
    runtime.Send(MakeFilterRequestHandle(actorId, edgeActor, 3, 6, sub3));

    NActors::TDispatchOptions opts;
    opts.CustomFinalCondition = [&]() {
        return (sub1->FilterReady || sub1->Failed) &&
               (sub2->FilterReady || sub2->Failed) &&
               (sub3->FilterReady || sub3->Failed);
    };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));

    UNIT_ASSERT_C(sub1->FilterReady && !sub1->Failed, sub1->FailureReason);
    UNIT_ASSERT_C(sub2->FilterReady && !sub2->Failed, sub2->FailureReason);
    UNIT_ASSERT_C(sub3->FilterReady && !sub3->Failed, sub3->FailureReason);

    // All portions are exclusive, so all should get allow-all
    UNIT_ASSERT(sub1->ReceivedFilter.IsTotalAllowFilter());
    UNIT_ASSERT(sub2->ReceivedFilter.IsTotalAllowFilter());
    UNIT_ASSERT(sub3->ReceivedFilter.IsTotalAllowFilter());
}

}
