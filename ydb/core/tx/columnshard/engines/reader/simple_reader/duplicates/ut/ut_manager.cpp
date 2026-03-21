#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/manager.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/events.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/filters.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/constructor/read_metadata.h>
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
#include <ydb/library/actors/testlib/test_runtime.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NReader;
using namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering;

namespace {

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
    const NActors::TActorId& scanActorId)
{
    auto indexInfoCache =
        std::make_shared<TObjectCache<TSchemaVersionId, TIndexInfo>>();
    auto entryGuard =
        indexInfoCache->Upsert(TSchemaVersionId(1, 1),
                               MakeTestIndexInfo());
    auto versionedIndex = std::make_shared<TVersionedIndex>();
    versionedIndex->AddIndex(TSnapshot(1, 1), std::move(entryGuard));

    TReadDescription readDesc(
        0, requestSnapshot, ERequestSorting::NONE);
    readDesc.SetScanCursor(nullptr);

    auto readMetadata = std::make_shared<NSimple::TReadMetadata>(
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
    std::shared_ptr<NSimple::TSpecialReadContext> Context;
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
            std::make_shared<NSimple::TSpecialReadContext>(ReadCtx);
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
    TManagerSetupResult& result)
{
    auto readContext = MakeTestReadContext(
        requestSnapshot, dataAccessorsManager,
        columnDataManager, scanActorId);
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
    runtime.Initialize();
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
    runtime.Initialize();
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
    runtime.Initialize();
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
    runtime.Initialize();
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
    runtime.Initialize();
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
    runtime.Initialize();
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

    auto p1Filter = sub1->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(p1Filter.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(p1Filter[0], true);
    UNIT_ASSERT_VALUES_EQUAL(p1Filter[1], false);
    UNIT_ASSERT_VALUES_EQUAL(p1Filter[2], false);

    auto p2Filter = sub2->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(p2Filter.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(p2Filter[0], true);
    UNIT_ASSERT_VALUES_EQUAL(p2Filter[1], true);
    UNIT_ASSERT_VALUES_EQUAL(p2Filter[2], true);
}

Y_UNIT_TEST(BoundaryOverlapSingleKey) {
    // P1=[1,5] pk={1,3,5} version=10  P2=[5,9] pk={5,7,9} version=20
    // Only key 5 overlaps. P2 wins on key 5.
    NActors::TTestActorRuntimeBase runtime(1, false);
    runtime.Initialize();
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
    auto f1 = sub1->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);

    // P2: all keys kept (key 5 is newer, keys 7,9 unique)
    auto f2 = sub2->ReceivedFilter.BuildSimpleFilter();
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
    runtime.Initialize();
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
    auto f1 = sub1->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);

    // P2: all keys kept
    auto f2 = sub2->ReceivedFilter.BuildSimpleFilter();
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
    runtime.Initialize();
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
    auto f1 = sub1->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 5u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[3], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[4], true);

    // P2: all keys kept (newer)
    auto f2 = sub2->ReceivedFilter.BuildSimpleFilter();
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
    runtime.Initialize();
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
    auto f1 = sub1->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);

    // P2: all kept (newer)
    auto f2 = sub2->ReceivedFilter.BuildSimpleFilter();
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
    runtime.Initialize();
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
    auto f1 = sub1->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[3], false);

    // P2: keys 3,4 kept (newer than P1); keys 5,6 deduped by P3
    auto f2 = sub2->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f2[3], false);

    // P3: all keys kept (newest or unique)
    auto f3 = sub3->ReceivedFilter.BuildSimpleFilter();
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
    runtime.Initialize();
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
    auto f1 = sub1->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[3], false);

    auto f2 = sub2->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f2[3], false);

    auto f3 = sub3->ReceivedFilter.BuildSimpleFilter();
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
    runtime.Initialize();
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
    auto f1 = sub1->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f1[1], false);
    UNIT_ASSERT_VALUES_EQUAL(f1[2], false);

    // P2: all kept (higher txId wins on overlap, key 4 unique)
    auto f2 = sub2->ReceivedFilter.BuildSimpleFilter();
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
    runtime.Initialize();
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
    auto f1 = sub1->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(f1.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(f1[0], false);

    // P2: key 5 kept (newer), keys 7,10 unique
    auto f2 = sub2->ReceivedFilter.BuildSimpleFilter();
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
    runtime.Initialize();
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
    auto f2 = sub2->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(f2.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f2[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f2[2], false);
    UNIT_ASSERT_VALUES_EQUAL(f2[3], false);

    // P3: all kept
    auto f3 = sub3->ReceivedFilter.BuildSimpleFilter();
    UNIT_ASSERT_VALUES_EQUAL(f3.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(f3[0], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[1], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[2], true);
    UNIT_ASSERT_VALUES_EQUAL(f3[3], true);
}

}
